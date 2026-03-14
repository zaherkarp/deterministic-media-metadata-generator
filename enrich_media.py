#!/usr/bin/env python3
"""
enrich_media.py — Deterministic Media Library Generator + Metadata Enricher
===========================================================================

PURPOSE:
    Parses an exported markdown list of media items (books/shows/movies/games),
    normalizes titles, deduplicates, enriches with Wikidata metadata (year, cover
    image, genres), and generates Obsidian-ready markdown notes with YAML frontmatter.

DESIGN PHILOSOPHY:
    - CONSERVATIVE: Prefer missing data over incorrect data.
    - DETERMINISTIC: Same input → same output (modulo timestamped folder names).
    - SAFE: No overwrites unless --overwrite is explicitly passed.
    - NO PLUGINS: Output works in vanilla Obsidian without Templater/Dataview.
    - NO HALLUCINATION: All metadata comes from verified Wikidata queries.
      Covers are real downloaded files or omitted entirely.

STREAMING AVAILABILITY:
    When --streaming is passed, the script adds platform/availability data:
    - Movies/Shows: Uses TMDb watch/providers API (requires --tmdb-key or
      TMDB_API_KEY env var) for streaming/rent/buy info per country.
    - Books: Fetches Open Library Work ID (P5331) from Wikidata — no key needed.
    - Games: Fetches Steam App ID (P1733) from Wikidata — no key needed.

USAGE:
    python3 enrich_media.py --input media_list.md --vault ~/my-vault
    python3 enrich_media.py --input media_list.md --vault ~/my-vault --only movies
    python3 enrich_media.py --input media_list.md --vault ~/my-vault --dry-run

REQUIRES:
    pip install requests   (only external dependency)

AUTHOR: Generated for Z's Obsidian media library workflow
"""

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import time
import unicodedata
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# OPTIONAL DEPENDENCY: requests (for Wikidata + image downloads)
# We import lazily so --dry-run and --no-enrich modes work without it.
# ---------------------------------------------------------------------------
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


# ===========================================================================
# SECTION 1: CONSTANTS AND CONFIGURATION
# ===========================================================================
# These govern the behavior of normalization, enrichment, and confidence gates.

# --- Media Type Mapping ---
# Maps the section headers in the input markdown to our canonical type strings.
# We match case-insensitively against these patterns.
SECTION_TYPE_MAP = {
    "books": "book",
    "book": "book",
    "shows": "show",
    "show": "show",
    "tv": "show",
    "tv shows": "show",
    "movies": "movie",
    "movie": "movie",
    "films": "movie",
    "film": "movie",
    "games": "game",
    "game": "game",
    "video games": "game",
    "videogames": "game",
}

# --- Wikidata Instance-Of (P31) QIDs ---
# Used in SPARQL queries to filter candidates by media type.
# Each type maps to one or more QIDs that Wikidata uses for classification.
WIKIDATA_TYPE_QIDS = {
    "book": [
        "Q571",       # book
        "Q7725634",   # literary work
        "Q1667921",   # novel series
        "Q8261",      # novel
        "Q49084",     # short story (some items are classified this way)
        "Q1004",      # comic book / bande dessinée
        "Q838795",    # comic book series
        "Q21198342",  # manga series
        "Q747381",    # graphic novel
        "Q104213567", # limited comic book series
        "Q149537",    # cookbook
    ],
    "movie": [
        "Q11424",     # film
        "Q24862",     # short film
        "Q336144",    # animated feature film (often used for anime films)
        "Q202866",    # animated film
        "Q20650540",  # anime film (WikiProject Anime standard)
        "Q29168811",  # animated feature film (alternate, widely used)
        "Q229390",    # 3D film
        "Q226730",    # OVA (original video animation — single-episode)
        "Q20667187",  # animated short film
        "Q506240",    # television film
        "Q98807719",  # animated television film
    ],
    "show": [
        "Q5398426",   # television series
        "Q581714",    # animated series
        "Q21191270",  # television series episode (rare but possible)
        "Q63952888",  # anime television series
        "Q11086742",  # anime television program (broader)
        "Q1259759",   # television special
        "Q123126551", # animated television special
        "Q526877",    # web series
        "Q21188109",  # web television series
        "Q1047299",   # ONA (original net animation)
        "Q113671041", # ONA series
        "Q220898",    # OVA (original video animation — multi-episode)
        "Q1366112",   # television miniseries
        "Q21664088",  # donghua (Chinese animated series)
    ],
    "game": [
        "Q7889",      # video game
        "Q21125433",  # video game remake (some newer entries)
        "Q56196027",  # mobile game
        "Q848991",    # browser game
    ],
}

# --- Parenthetical Patterns to Strip ---
# These are parenthetical suffixes on titles that should be removed from the
# display title (but may inform type assignment or be noted separately).
# We strip them during normalization. Order matters: more specific first.
STRIP_PARENS_PATTERNS = [
    r"\(series\)",
    r"\(show\)",
    r"\(tv\s*series\)",
    r"\(tv\s*show\)",
    r"\(film\)",
    r"\(\d{4}\s+film\)",        # e.g., "(2016 film)"
    r"\(video\s*game\)",
    r"\(game\)",
    r"\(novel\)",
    r"\(book\)",
    r"\(anime\)",
    r"\(manga\)",
    r"\(documentary\)",
    r"\(miniseries\)",
    r"\(mini-series\)",
]

# --- Trailing Junk Patterns ---
# Patterns to strip from the end of a title after parenthetical removal.
# e.g., "Severence, lol" → "Severence"
TRAILING_JUNK_PATTERNS = [
    r",?\s*lol\s*$",
    r",?\s*haha\s*$",
    r",?\s*maybe\s*$",
    r",?\s*\?\s*$",          # trailing question marks
    r"\s*\?\s*$",
]

# --- Search Title Cleanup Patterns ---
# These apply ONLY to the search_title (used for Wikidata queries),
# not the clean_title (used for display). More aggressive stripping
# is safe here because it won't affect the note heading or filename.
SEARCH_TITLE_JUNK_PATTERNS = [
    r"'s\s+series\s*$",     # "Fran Leiboweitz's series" → "Fran Leiboweitz"
    r"\s+series\s*$",       # trailing bare "series" (user annotation, not title)
]

# --- Characters Illegal in Filenames ---
# These characters are stripped from filenames to ensure cross-OS compatibility.
# macOS is generally lenient, but Windows compat and Obsidian expectations matter.
ILLEGAL_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

# --- Confidence Thresholds ---
# These control how aggressive or conservative we are in accepting Wikidata matches.
# Score components are additive; we require total >= ACCEPT_THRESHOLD.
CONFIDENCE = {
    "ACCEPT_THRESHOLD": 2,       # minimum score to accept a Wikidata match
    "EXACT_LABEL_BONUS": 3,      # exact label match (case-insensitive)
    "CLOSE_LABEL_BONUS": 2,      # label matches after normalization
    "YEAR_MATCH_BONUS": 2,       # year from Wikidata matches our year hint
    "HAS_IMAGE_BONUS": 1,        # candidate has a P18 image
    "AMBIGUITY_PENALTY": -2,     # if top two candidates are close in score
    "MIN_MARGIN": 1,             # minimum margin between #1 and #2 to accept
}

# --- Wikidata SPARQL Endpoint ---
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"

# --- Wikimedia Commons Thumb URL Template ---
# To get a usable image URL from a Wikimedia Commons filename, we use the
# Special:FilePath redirect which gives us the actual image. We request a
# reasonable thumbnail size (500px width) to avoid downloading massive files.
COMMONS_THUMB_TEMPLATE = (
    "https://commons.wikimedia.org/wiki/Special:FilePath/{filename}?width=500"
)

# --- HTTP Settings ---
DEFAULT_SLEEP = 0.3          # seconds between Wikidata requests (be polite)
REQUEST_TIMEOUT = 15         # seconds before giving up on a single request
MAX_RETRIES = 2              # retry failed requests this many times
USER_AGENT = (
    "ObsidianMediaEnricher/1.0 "
    "(https://github.com/example; media-library-tool) "
    "python-requests"
)

# --- TMDb API Settings ---
TMDB_API_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"
TMDB_WATCH_PROVIDER_TYPES = ["flatrate", "rent", "buy", "free", "ads"]
TMDB_WATCH_PROVIDER_TYPE_LABELS = {
    "flatrate": "subscription",
    "rent": "rent",
    "buy": "buy",
    "free": "free",
    "ads": "ads",
}

# --- Open Library API Settings ---
OPENLIBRARY_COVERS_BASE = "https://covers.openlibrary.org/b/olid"

# --- Logging Setup ---
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_DATE_FORMAT = "%H:%M:%S"


# ===========================================================================
# SECTION 2: INPUT PARSING
# ===========================================================================
# Responsible for reading the markdown input file and extracting structured
# entries grouped by media type.

def parse_input_file(filepath: str, only_type: Optional[str] = None) -> list[dict]:
    """
    Parse the input markdown file into a list of raw media entries.

    INPUT FORMAT EXPECTED:
        ## BOOKS
        Wave in the Mind (LeGuin).md
        Claim Your Inner Child (from Merija).md

        ## SHOWS
        The Mandalorian (from Anton).md
        ...

    Each entry is one line under a section header. Section headers are
    identified by '##' prefix and matched against SECTION_TYPE_MAP.

    RETURNS:
        List of dicts, each with:
        {
            "raw_line": str,         # original line from file
            "type": str,             # "book", "movie", "show", "game"
            "raw_title": str,        # line with .md stripped but otherwise raw
        }

    PARAMETERS:
        filepath:   Path to the input markdown file
        only_type:  If set, only return entries of this type (e.g., "movie")
    """
    entries = []
    current_type = None

    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            stripped = line.strip()

            # --- Skip empty lines and non-content ---
            if not stripped:
                continue

            # --- Detect Section Headers ---
            # Match lines like "## BOOKS", "## MOVIES", "# TV Shows", etc.
            # We're flexible: 1-6 '#' characters, optional space, then the name.
            header_match = re.match(r'^#{1,6}\s+(.+)$', stripped)
            if header_match:
                header_text = header_match.group(1).strip().lower()
                # Remove any trailing punctuation or decoration from header
                header_text = re.sub(r'[:\-–—]+$', '', header_text).strip()

                if header_text in SECTION_TYPE_MAP:
                    current_type = SECTION_TYPE_MAP[header_text]
                    logging.debug(f"Line {line_num}: Detected section '{header_text}' → type='{current_type}'")
                else:
                    # Unknown section header — might be a different part of the note.
                    # We stop collecting until we hit a known section.
                    logging.debug(f"Line {line_num}: Unknown section '{header_text}', pausing collection")
                    current_type = None
                continue

            # --- Skip lines if we haven't entered a known section yet ---
            if current_type is None:
                continue

            # --- Apply --only filter early (saves processing) ---
            if only_type and current_type != only_type:
                continue

            # --- Treat this line as a media entry ---
            # Strip markdown list prefixes if present (- or * or numbered)
            entry_text = re.sub(r'^[-*]\s+', '', stripped)
            entry_text = re.sub(r'^\d+\.\s+', '', entry_text)

            # Strip wikilinks if the input uses [[...]] notation
            entry_text = re.sub(r'\[\[([^\]]+)\]\]', r'\1', entry_text)

            entries.append({
                "raw_line": stripped,
                "type": current_type,
                "raw_title": entry_text,
                "line_num": line_num,
            })

    logging.info(f"Parsed {len(entries)} entries from '{filepath}'")
    if only_type:
        logging.info(f"  (filtered to type='{only_type}')")

    return entries


# ===========================================================================
# SECTION 3: TITLE NORMALIZATION AND METADATA EXTRACTION
# ===========================================================================
# This is the core normalization pipeline. Each entry goes through multiple
# stages of cleaning, extraction, and normalization to produce a clean title,
# a dedup key, and extracted metadata (year hints, source notes, etc.).

def extract_and_normalize(entry: dict) -> dict:
    """
    Take a raw parsed entry and produce a fully normalized record.

    PIPELINE:
        1. Strip .md extension
        2. Extract (from X) source notes
        3. Extract year hints from parentheticals or trailing patterns
        4. Strip parenthetical descriptors (series, show, film, etc.)
        5. Strip trailing junk (lol, ?, etc.)
        6. Normalize separator punctuation (- → : where appropriate)
        7. Collapse whitespace
        8. Generate dedup key (lowercase, stripped, no punctuation)
        9. Generate filesystem-safe filename

    RETURNS:
        Enriched dict with all original fields plus:
        {
            "clean_title": str,      # human-readable clean title
            "dedup_key": str,        # lowercase, stripped key for deduplication
            "year_hint": int|None,   # extracted year if present
            "source": str|None,      # e.g., "from Anton"
            "filename": str,         # filesystem-safe filename (no extension)
            "original_parens": list,  # all parenthetical content found
        }
    """
    title = entry["raw_title"]
    media_type = entry["type"]
    year_hint = None
    source = None
    original_parens = []

    # -----------------------------------------------------------------------
    # STEP 1: Strip .md extension
    # Some entries have it, some don't. Normalize by removing it.
    # -----------------------------------------------------------------------
    if title.lower().endswith(".md"):
        title = title[:-3]

    # -----------------------------------------------------------------------
    # STEP 2: Extract "(from X)" source annotations
    # Pattern: "(from SomeName)" at end of title, case-insensitive
    # We capture the full "from X" text and remove the parenthetical.
    # Must happen BEFORE year extraction to avoid "(from 2001)" confusion.
    # -----------------------------------------------------------------------
    source_match = re.search(r'\(from\s+([^)]+)\)\s*$', title, re.IGNORECASE)
    if source_match:
        source = f"from {source_match.group(1).strip()}"
        original_parens.append(source_match.group(0))
        title = title[:source_match.start()].strip()

    # -----------------------------------------------------------------------
    # STEP 3: Extract year hints
    # We look for years in several patterns:
    #   a) (YYYY) — parenthetical year: "Redline (2009)"
    #   b) (YYYY, ...) — year with extra info: "Come and See (1985, Иди и смотри)"
    #   c) Title, YYYY — trailing comma-year: "Little Murders, 1971"
    #   d) (YYYY film) — year+descriptor: "Inherit the Wind (1960 film)"
    #
    # IMPORTANT: We only extract 4-digit numbers in range 1880-2030 as years.
    # This avoids false positives like "(1984)" the novel title.
    # We use a heuristic: if the title IS a famous year-as-title work, skip.
    # -----------------------------------------------------------------------
    YEAR_RANGE = (1880, 2030)

    # Pattern (a): (YYYY) alone
    year_match = re.search(r'\((\d{4})\)\s*$', title)
    if year_match:
        candidate_year = int(year_match.group(1))
        if YEAR_RANGE[0] <= candidate_year <= YEAR_RANGE[1]:
            # Check: is the entire title just a year? e.g., "1984"
            # In that case, don't strip it — it's the actual title.
            title_without = title[:year_match.start()].strip()
            if title_without:  # there IS a title before the year
                year_hint = candidate_year
                original_parens.append(year_match.group(0))
                title = title_without

    # Pattern (b): (YYYY, extra stuff) — e.g., "(1985, Иди и смотри)"
    if year_hint is None:
        year_extra_match = re.search(r'\((\d{4}),\s*([^)]+)\)\s*$', title)
        if year_extra_match:
            candidate_year = int(year_extra_match.group(1))
            if YEAR_RANGE[0] <= candidate_year <= YEAR_RANGE[1]:
                year_hint = candidate_year
                # We note the extra info but strip the whole parenthetical
                original_parens.append(year_extra_match.group(0))
                title = title[:year_extra_match.start()].strip()

    # Pattern (d): (YYYY film) or (YYYY show) etc. — at end
    if year_hint is None:
        year_desc_match = re.search(
            r'\((\d{4})\s+(film|movie|show|series|game|novel|book|anime|documentary)\)\s*$',
            title, re.IGNORECASE
        )
        if year_desc_match:
            candidate_year = int(year_desc_match.group(1))
            if YEAR_RANGE[0] <= candidate_year <= YEAR_RANGE[1]:
                year_hint = candidate_year
                original_parens.append(year_desc_match.group(0))
                title = title[:year_desc_match.start()].strip()

    # Pattern (e): (YYYY) mid-title followed by user annotation
    # e.g., "Living (2022) - Ikiru remake" → year=2022, title="Living"
    # Only extract if followed by a separator (-, –, —) and more text.
    if year_hint is None:
        mid_year_match = re.search(r'\((\d{4})\)\s*[-–—]\s*', title)
        if mid_year_match:
            candidate_year = int(mid_year_match.group(1))
            if YEAR_RANGE[0] <= candidate_year <= YEAR_RANGE[1]:
                title_before = title[:mid_year_match.start()].strip()
                if title_before:
                    year_hint = candidate_year
                    original_parens.append(f"({candidate_year})")
                    # Keep the title before the year; the annotation after
                    # will be handled by separator normalization + annotation stripping
                    title = title_before

    # Pattern (c): Title, YYYY at end — e.g., "Little Murders, 1971"
    if year_hint is None:
        trailing_year_match = re.search(r',\s*(\d{4})\s*$', title)
        if trailing_year_match:
            candidate_year = int(trailing_year_match.group(1))
            if YEAR_RANGE[0] <= candidate_year <= YEAR_RANGE[1]:
                year_hint = candidate_year
                title = title[:trailing_year_match.start()].strip()

    # -----------------------------------------------------------------------
    # STEP 4: Strip parenthetical descriptors
    # Remove things like "(series)", "(show)", "(2016 film)" etc.
    # These are type hints, not part of the canonical title.
    # -----------------------------------------------------------------------
    for pattern in STRIP_PARENS_PATTERNS:
        paren_match = re.search(pattern, title, re.IGNORECASE)
        if paren_match:
            original_parens.append(paren_match.group(0))
            title = title[:paren_match.start()] + title[paren_match.end():]
            title = title.strip()

    # -----------------------------------------------------------------------
    # STEP 5: Strip trailing junk
    # Remove informal suffixes like ", lol", trailing "?", etc.
    # -----------------------------------------------------------------------
    for pattern in TRAILING_JUNK_PATTERNS:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE).strip()

    # -----------------------------------------------------------------------
    # STEP 6: Normalize separator punctuation
    # Convert "Title- Subtitle" or "Title - Subtitle" to "Title: Subtitle"
    # This handles the common pattern in the input where hyphens are used
    # as colon substitutes (likely because ':' is illegal in filenames).
    #
    # RULE: If a hyphen is surrounded by spaces (or preceded by a space and
    # followed by nothing/space), and the left side is ≥2 chars, treat it
    # as a title:subtitle separator and convert to colon.
    # -----------------------------------------------------------------------
    # Pattern: "Word- Subtitle" or "Word - Subtitle" or "Word -Subtitle"
    title = re.sub(r'\s*[-–—]\s+', ': ', title)
    # Clean up any resulting double-colons or colon-space issues
    title = re.sub(r':\s*:', ':', title)

    # -----------------------------------------------------------------------
    # STEP 7: Collapse whitespace
    # Multiple spaces (from the input or from our stripping) → single space.
    # -----------------------------------------------------------------------
    title = re.sub(r'\s+', ' ', title).strip()

    # -----------------------------------------------------------------------
    # STEP 8: Generate dedup key
    # This is a heavily normalized version used ONLY for deduplication.
    # It is NOT the display title — it strips everything down to bare minimum.
    #
    # PIPELINE:
    #   a) lowercase
    #   b) Unicode normalize (NFKD) to decompose accented characters
    #   c) Strip all non-alphanumeric-non-space characters
    #   d) Normalize unicode quotes/apostrophes to ASCII
    #   e) Collapse whitespace
    #   f) Prepend the media type to avoid cross-type collisions
    #      (a book "Safe" and a movie "Safe" are different items)
    # -----------------------------------------------------------------------
    dedup_key = title.lower()
    # Normalize unicode: é → e, ü → u, etc. for dedup purposes only
    dedup_key = unicodedata.normalize('NFKD', dedup_key)
    dedup_key = dedup_key.encode('ascii', 'ignore').decode('ascii')
    # Strip all punctuation and special chars, keep letters/digits/spaces
    dedup_key = re.sub(r'[^a-z0-9\s]', '', dedup_key)
    dedup_key = re.sub(r'\s+', ' ', dedup_key).strip()
    # Prefix with type to namespace the dedup
    dedup_key = f"{media_type}:{dedup_key}"

    # -----------------------------------------------------------------------
    # STEP 9: Generate filesystem-safe filename
    # The filename is derived from the clean title, with illegal characters
    # removed and spaces converted to hyphens for readability.
    # -----------------------------------------------------------------------
    filename = generate_safe_filename(title, year_hint)

    # -----------------------------------------------------------------------
    # STEP 10: Generate search title
    # This is the title used for Wikidata SPARQL queries.  We strip ALL
    # remaining parentheticals (e.g., "(LeGuin)", "(The Pixel Hunt)")
    # because Wikidata labels rarely contain them.  The clean_title is
    # kept intact for display / note headings.
    # -----------------------------------------------------------------------
    search_title = re.sub(r'\s*\([^)]*\)\s*', ' ', title).strip()
    search_title = re.sub(r'\s+', ' ', search_title)
    # Clean up orphaned colons left after parenthetical stripping
    # e.g. "Living (2022): Ikiru remake" → strip parens → "Living : Ikiru remake"
    search_title = re.sub(r'\s*:\s*', ': ', search_title).strip()
    search_title = re.sub(r'^:\s*', '', search_title)
    search_title = re.sub(r'\s*:$', '', search_title)
    if not search_title:
        search_title = title  # fallback if stripping removed everything

    # Strip search-specific junk patterns (more aggressive than clean_title)
    for pattern in SEARCH_TITLE_JUNK_PATTERNS:
        search_title = re.sub(pattern, '', search_title, flags=re.IGNORECASE).strip()

    # Also strip colon-suffixed user annotations from search_title.
    # Real subtitles typically have Title Case ("Frieren: Beyond Journey's End")
    # while user annotations are all lowercase ("Inherit the Wind: old drama").
    # Heuristic: if the part after the LAST colon starts with a lowercase letter,
    # it's likely a user comment, not a real subtitle.
    if ':' in search_title:
        before_colon, _, after_colon = search_title.rpartition(':')
        after_stripped = after_colon.strip()
        if before_colon.strip() and after_stripped and after_stripped[0].islower():
            search_title = before_colon.strip()

    # -----------------------------------------------------------------------
    # RETURN: Enriched entry with all extracted metadata
    # -----------------------------------------------------------------------
    entry.update({
        "clean_title": title,
        "search_title": search_title,
        "dedup_key": dedup_key,
        "year_hint": year_hint,
        "source": source,
        "filename": filename,
        "original_parens": original_parens,
    })
    return entry


def generate_safe_filename(title: str, year: Optional[int] = None) -> str:
    """
    Generate a filesystem-safe filename from a title.

    RULES:
        - Strip illegal characters for cross-OS compatibility
        - Replace spaces with hyphens
        - Collapse multiple hyphens
        - Lowercase for consistency
        - Append year if available (helps avoid collisions)
        - Truncate to 200 chars (filesystem limits)

    EXAMPLES:
        "Redline" + 2009 → "redline-2009"
        "Come and See" + 1985 → "come-and-see-1985"
        "The Mandalorian" + None → "the-mandalorian"
        "Arendt: The Origins of Totalitarianism" → "arendt-the-origins-of-totalitarianism"
    """
    slug = title.lower()
    # Remove illegal filename characters
    slug = ILLEGAL_FILENAME_CHARS.sub('', slug)
    # Replace colons, commas, and other separators with hyphens
    slug = re.sub(r'[:\.,;!\'"()\[\]{}]+', '', slug)
    # Replace spaces and underscores with hyphens
    slug = re.sub(r'[\s_]+', '-', slug)
    # Collapse multiple hyphens
    slug = re.sub(r'-{2,}', '-', slug)
    # Strip leading/trailing hyphens
    slug = slug.strip('-')

    # Append year if available
    if year:
        slug = f"{slug}-{year}"

    # Truncate to reasonable length
    if len(slug) > 200:
        slug = slug[:200].rstrip('-')

    # Fallback for empty slugs (shouldn't happen, but safety)
    if not slug:
        slug = "untitled"

    return slug


def generate_slug_for_cover(title: str, year: Optional[int] = None) -> str:
    """
    Generate a stable slug specifically for cover image filenames.
    Uses the same logic as generate_safe_filename but is a separate function
    so we can evolve them independently if needed.
    """
    return generate_safe_filename(title, year)


# ===========================================================================
# SECTION 4: DEDUPLICATION
# ===========================================================================

def deduplicate_entries(entries: list[dict]) -> list[dict]:
    """
    Deduplicate entries by (type, normalized_title_key).

    STRATEGY:
        - Group entries by dedup_key
        - For each group, keep the "best" entry (most metadata, first seen)
        - Log all duplicates found

    WHAT COUNTS AS DUPLICATE:
        - Same dedup_key (which includes type prefix)
        - e.g., "Heavenly delusion" and "Heavenly Delusion" both normalize to
          "show:heavenly delusion" → only one survives

    TIEBREAKING (when duplicates found, which one to keep):
        1. Entry with year_hint present wins
        2. Entry with source present wins
        3. Otherwise, first encountered wins
    """
    seen = {}       # dedup_key → entry
    dupes = []      # list of (kept_entry, dropped_entry) for logging

    for entry in entries:
        key = entry["dedup_key"]

        if key not in seen:
            seen[key] = entry
        else:
            existing = seen[key]
            # Decide which to keep
            new_score = _dedup_quality_score(entry)
            old_score = _dedup_quality_score(existing)

            if new_score > old_score:
                dupes.append((entry, existing))
                seen[key] = entry
            else:
                dupes.append((existing, entry))

    # Log duplicates
    for kept, dropped in dupes:
        logging.info(
            f"DEDUP: Kept '{kept['clean_title']}' (line {kept.get('line_num', '?')}), "
            f"dropped '{dropped['raw_title']}' (line {dropped.get('line_num', '?')})"
        )

    result = list(seen.values())
    logging.info(f"Deduplication: {len(entries)} → {len(result)} unique entries ({len(dupes)} duplicates removed)")
    return result


def _dedup_quality_score(entry: dict) -> int:
    """
    Score an entry's quality for dedup tiebreaking.
    Higher score = more metadata = prefer to keep.
    """
    score = 0
    if entry.get("year_hint"):
        score += 2  # year hints are valuable for disambiguation
    if entry.get("source"):
        score += 1  # source annotations are worth preserving
    return score


# ===========================================================================
# SECTION 5: COLLISION DETECTION
# ===========================================================================

def detect_filename_collisions(entries: list[dict]) -> list[dict]:
    """
    Detect and resolve filename collisions among deduplicated entries.

    Even after deduplication, two DIFFERENT items might normalize to the same
    filename. Example: a book "Safe" and a movie "Safe" would both get
    filename "safe" — but they have different dedup keys because type differs.

    RESOLUTION:
        - If collision detected, append a short hash suffix to disambiguate.
        - The hash is derived from the dedup_key for stability.
    """
    filename_map = {}  # filename → [entries]

    for entry in entries:
        fn = entry["filename"]
        filename_map.setdefault(fn, []).append(entry)

    for fn, collisions in filename_map.items():
        if len(collisions) <= 1:
            continue

        logging.warning(f"COLLISION: {len(collisions)} entries map to filename '{fn}'")
        for entry in collisions:
            # Generate a short hash from the dedup_key for stable disambiguation
            hash_suffix = hashlib.md5(
                entry["dedup_key"].encode("utf-8")
            ).hexdigest()[:6]
            new_fn = f"{fn}-{hash_suffix}"
            logging.warning(
                f"  Renamed '{entry['clean_title']}' ({entry['type']}) → '{new_fn}'"
            )
            entry["filename"] = new_fn

    return entries


# ===========================================================================
# SECTION 6: WIKIDATA ENRICHMENT
# ===========================================================================
# This section handles querying Wikidata's SPARQL endpoint for metadata,
# scoring candidates, and making conservative accept/reject decisions.

def _platform_optionals(media_type: str) -> str:
    """Return only the OPTIONAL platform-ID clauses relevant to *media_type*.

    Keeps the query plan small by not asking Wikidata to probe properties
    that can never match (e.g. Steam ID for a book).
    """
    clauses: list[str] = []
    if media_type == "movie":
        clauses.append("OPTIONAL { ?item wdt:P4983 ?tmdbMovieId . }")
    elif media_type == "show":
        clauses.append("OPTIONAL { ?item wdt:P4947 ?tmdbTvId . }")
    elif media_type == "book":
        clauses.append("OPTIONAL { ?item wdt:P5331 ?openLibraryId . }")
    elif media_type == "game":
        clauses.append("OPTIONAL { ?item wdt:P1733 ?steamId . }")
    return "\n      ".join(clauses)


def build_sparql_query(title: str, media_type: str) -> str:
    """
    Build a SPARQL query to search Wikidata for candidates matching a title.

    APPROACH:
        We use the MediaWiki full-text search API (mwapi:search) to find
        candidate items quickly via an indexed lookup, then narrow by
        instance-of type and exact label match.  This avoids a full scan
        of rdfs:label which causes timeouts on the public endpoint.

        We fetch:
        - P577 (publication date / release date)
        - P18 (image)
        - P136 (genre)

    We filter by P31 (instance of) to restrict to the right media type.
    We limit results to 10 candidates to avoid overwhelming the confidence gate.

    NOTE: SPARQL queries against Wikidata's public endpoint are rate-limited.
    We add a polite User-Agent and sleep between requests.
    """
    # Get the QIDs for this media type
    type_qids = WIKIDATA_TYPE_QIDS.get(media_type, [])
    if not type_qids:
        return ""

    # Build the VALUES clause for type filtering
    # This creates: VALUES ?type { wd:Q571 wd:Q7725634 ... }
    type_values = " ".join(f"wd:{qid}" for qid in type_qids)

    # Escape the title for use in a SPARQL string literal
    # Replace backslashes first, then quotes
    escaped_title = title.replace("\\", "\\\\").replace('"', '\\"')

    platform_clauses = _platform_optionals(media_type)

    # Build the query
    # STRATEGY: Use the MediaWiki API full-text search service to find items
    # by title (indexed, fast), then confirm the label matches exactly and
    # check the instance-of type.  Metadata is fetched in a small subquery
    # so OPTIONAL blocks don't create a cartesian explosion.
    query = f"""
    SELECT ?item ?itemLabel ?date ?image ?genreLabel ?description ?article
           ?altLabel ?startDate
           ?tmdbMovieId ?tmdbTvId ?openLibraryId ?steamId WHERE {{
      # --- Fast indexed lookup via MediaWiki search ---
      SERVICE wikibase:mwapi {{
        bd:serviceParam wikibase:endpoint "www.wikidata.org" ;
                        wikibase:api "EntitySearch" ;
                        mwapi:search "{escaped_title}" ;
                        mwapi:language "en" .
        ?item wikibase:apiOutputItem mwapi:item .
      }}

      # --- Filter by media type (instance-of) ---
      VALUES ?type {{ {type_values} }}
      ?item wdt:P31 ?type .

      # --- Optional: publication/release date (P577) ---
      OPTIONAL {{ ?item wdt:P577 ?date . }}

      # --- Optional: start date (P580) — used for TV series premiere dates ---
      OPTIONAL {{ ?item wdt:P580 ?startDate . }}

      # --- Optional: image (P18) ---
      OPTIONAL {{ ?item wdt:P18 ?image . }}

      # --- Optional: genre (P136) — use label service, not manual join ---
      OPTIONAL {{ ?item wdt:P136 ?genre . }}

      # --- Optional: alternative labels (skos:altLabel) ---
      OPTIONAL {{ ?item skos:altLabel ?altLabel .
                 FILTER(LANG(?altLabel) = "en") }}

      # --- Optional: description (schema:description) ---
      OPTIONAL {{ ?item schema:description ?description .
                 FILTER(LANG(?description) = "en") }}

      # --- Optional: English Wikipedia article link ---
      OPTIONAL {{ ?article schema:about ?item ;
                          schema:isPartOf <https://en.wikipedia.org/> . }}

      # --- Optional: platform IDs (only the ones relevant to this type) ---
      {platform_clauses}

      # --- Get English labels (also resolves ?genre → ?genreLabel) ---
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,mul". }}
    }}
    LIMIT 10
    """
    return query


def build_sparql_query_fuzzy(title: str, media_type: str) -> str:
    """
    Build a FALLBACK SPARQL query using the MediaWiki search service.

    This is used when the exact-match query returns no results.
    Instead of the old CONTAINS-over-all-labels approach (which caused
    timeouts), we reuse the indexed mwapi EntitySearch but drop the
    exact-label-match filter, so near-misses (e.g. "The Mandalorian"
    vs "Mandalorian") can still be found.

    CAUTION: Fuzzy queries return more false positives, so we apply stricter
    confidence gating on these results.
    """
    type_qids = WIKIDATA_TYPE_QIDS.get(media_type, [])
    if not type_qids:
        return ""

    type_values = " ".join(f"wd:{qid}" for qid in type_qids)
    escaped_title = title.replace("\\", "\\\\").replace('"', '\\"')

    platform_clauses = _platform_optionals(media_type)

    query = f"""
    SELECT ?item ?itemLabel ?date ?image ?genreLabel ?description ?article
           ?altLabel ?startDate
           ?tmdbMovieId ?tmdbTvId ?openLibraryId ?steamId WHERE {{
      # --- Fast indexed lookup (fuzzy — no exact-match filter) ---
      SERVICE wikibase:mwapi {{
        bd:serviceParam wikibase:endpoint "www.wikidata.org" ;
                        wikibase:api "EntitySearch" ;
                        mwapi:search "{escaped_title}" ;
                        mwapi:language "en" .
        ?item wikibase:apiOutputItem mwapi:item .
      }}

      # --- Filter by media type (instance-of) ---
      VALUES ?type {{ {type_values} }}
      ?item wdt:P31 ?type .

      OPTIONAL {{ ?item wdt:P577 ?date . }}
      OPTIONAL {{ ?item wdt:P580 ?startDate . }}
      OPTIONAL {{ ?item wdt:P18 ?image . }}
      OPTIONAL {{ ?item wdt:P136 ?genre . }}

      # --- Optional: alternative labels ---
      OPTIONAL {{ ?item skos:altLabel ?altLabel .
                 FILTER(LANG(?altLabel) = "en") }}

      # --- Optional: description ---
      OPTIONAL {{ ?item schema:description ?description .
                 FILTER(LANG(?description) = "en") }}

      # --- Optional: English Wikipedia article link ---
      OPTIONAL {{ ?article schema:about ?item ;
                          schema:isPartOf <https://en.wikipedia.org/> . }}

      # --- Optional: platform IDs (only relevant ones) ---
      {platform_clauses}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,mul". }}
    }}
    LIMIT 10
    """
    return query


# Global adaptive rate limiter — shared across all query_wikidata calls.
# When Wikidata pushes back (429, 5xx, timeouts), we increase the cooldown
# so subsequent queries (including fallback searches) don't pile on.
_rate_limiter = {
    "min_interval": DEFAULT_SLEEP,   # baseline from --sleep
    "current_interval": DEFAULT_SLEEP,
    "last_request_time": 0.0,
    "backoff_until": 0.0,            # don't send requests before this time
}


def _rate_limit_wait() -> None:
    """Wait if needed to respect the adaptive rate limit."""
    now = time.time()
    # If we're in a backoff period, wait it out
    if now < _rate_limiter["backoff_until"]:
        wait = _rate_limiter["backoff_until"] - now
        logging.debug(f"  Rate limiter: cooling down for {wait:.1f}s")
        time.sleep(wait)
    # Also respect minimum interval between requests
    elapsed = time.time() - _rate_limiter["last_request_time"]
    if elapsed < _rate_limiter["current_interval"]:
        time.sleep(_rate_limiter["current_interval"] - elapsed)
    _rate_limiter["last_request_time"] = time.time()


def _rate_limit_backoff(severity: str = "mild") -> None:
    """Increase the rate limit after a server pushback.

    severity:
        "mild"   — timeout or 5xx: double current interval, cooldown 3s
        "severe" — 429 rate limit: quadruple interval, cooldown 10s
    """
    if severity == "severe":
        _rate_limiter["current_interval"] = min(
            _rate_limiter["current_interval"] * 4, 10.0
        )
        _rate_limiter["backoff_until"] = time.time() + 10.0
        logging.info(
            f"  Rate limiter: 429 detected, cooling down 10s "
            f"(interval now {_rate_limiter['current_interval']:.1f}s)"
        )
    else:
        _rate_limiter["current_interval"] = min(
            _rate_limiter["current_interval"] * 2, 5.0
        )
        _rate_limiter["backoff_until"] = time.time() + 3.0


def _rate_limit_success() -> None:
    """Gradually reduce the rate limit after a successful request."""
    _rate_limiter["current_interval"] = max(
        _rate_limiter["current_interval"] * 0.8,
        _rate_limiter["min_interval"],
    )


def query_wikidata(sparql: str, session: "requests.Session") -> list[dict]:
    """
    Execute a SPARQL query against Wikidata and return parsed results.

    RETURNS:
        List of result binding dicts from the SPARQL JSON response.
        Empty list on any error.

    ERROR HANDLING:
        - Network errors: logged and return empty
        - Rate limiting (429): logged, backs off adaptively
        - Server errors (5xx): retried with exponential backoff
        - Timeouts: retried with backoff
        - Malformed response: logged and return empty
    """
    if not sparql:
        return []

    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": USER_AGENT,
    }
    params = {"query": sparql}

    for attempt in range(MAX_RETRIES + 1):
        _rate_limit_wait()

        try:
            resp = session.get(
                WIKIDATA_SPARQL_URL,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )

            if resp.status_code == 429:
                _rate_limit_backoff("severe")
                logging.warning(
                    f"Rate limited by Wikidata (429). "
                    f"Retry {attempt+1}/{MAX_RETRIES}..."
                )
                if attempt < MAX_RETRIES:
                    continue
                return []

            if resp.status_code in (500, 502, 503, 504):
                _rate_limit_backoff("mild")
                logging.warning(
                    f"Wikidata returned {resp.status_code}. "
                    f"Retry {attempt+1}/{MAX_RETRIES}..."
                )
                if attempt < MAX_RETRIES:
                    continue
                return []

            if resp.status_code == 403:
                logging.warning("Wikidata returned 403 Forbidden. User-Agent may need update.")
                return []

            resp.raise_for_status()
            data = resp.json()
            _rate_limit_success()
            return data.get("results", {}).get("bindings", [])

        except requests.exceptions.Timeout:
            _rate_limit_backoff("mild")
            logging.warning(
                f"Wikidata query timed out "
                f"(attempt {attempt+1}/{MAX_RETRIES+1})"
            )
            if attempt < MAX_RETRIES:
                continue
        except requests.exceptions.RequestException as e:
            logging.warning(f"Wikidata request failed: {e}")
            return []
        except json.JSONDecodeError:
            logging.warning("Wikidata returned non-JSON response")
            return []

    return []


def score_candidate(
    candidate: dict,
    title: str,
    year_hint: Optional[int],
    is_fuzzy: bool = False,
) -> dict:
    """
    Score a Wikidata candidate for confidence gating.

    SCORING COMPONENTS:
        +3  exact label match (case-insensitive)
        +2  close label match (after normalization)
        +2  year matches our year_hint
        +1  candidate has an image (P18)
        -2  penalty if this is from a fuzzy query (less reliable)

    RETURNS:
        Dict with:
        {
            "qid": str,              # Wikidata QID (e.g., "Q12345")
            "label": str,            # item label from Wikidata
            "year": int|None,        # extracted year from P577
            "image_filename": str|None,  # Commons filename from P18
            "genres": list[str],     # genre labels
            "score": int,            # total confidence score
            "score_breakdown": dict, # individual score components
        }
    """
    result = {
        "qid": None,
        "label": None,
        "year": None,
        "image_filename": None,
        "genres": [],
        "description": None,
        "source_url": None,
        "score": 0,
        "score_breakdown": {},
    }

    # --- Extract QID ---
    item_uri = candidate.get("item", {}).get("value", "")
    if item_uri:
        result["qid"] = item_uri.split("/")[-1]

    # --- Extract label ---
    result["label"] = candidate.get("itemLabel", {}).get("value", "")

    # --- Extract: alternative labels ---
    alt_label = candidate.get("altLabel", {}).get("value", "")
    if alt_label:
        result["alt_labels"] = [alt_label]
    else:
        result["alt_labels"] = []

    # --- Score: label match ---
    if result["label"].lower() == title.lower():
        result["score"] += CONFIDENCE["EXACT_LABEL_BONUS"]
        result["score_breakdown"]["exact_label"] = CONFIDENCE["EXACT_LABEL_BONUS"]
    else:
        # Check normalized match (strip punctuation, collapse whitespace)
        norm_label = _normalize_for_comparison(result["label"])
        norm_title = _normalize_for_comparison(title)
        if norm_label == norm_title:
            result["score"] += CONFIDENCE["CLOSE_LABEL_BONUS"]
            result["score_breakdown"]["close_label"] = CONFIDENCE["CLOSE_LABEL_BONUS"]
        elif alt_label:
            # Check if any alternative label matches
            if alt_label.lower() == title.lower():
                result["score"] += CONFIDENCE["CLOSE_LABEL_BONUS"]
                result["score_breakdown"]["alt_label_match"] = CONFIDENCE["CLOSE_LABEL_BONUS"]
            elif _normalize_for_comparison(alt_label) == norm_title:
                result["score"] += CONFIDENCE["CLOSE_LABEL_BONUS"]
                result["score_breakdown"]["alt_label_match"] = CONFIDENCE["CLOSE_LABEL_BONUS"]

    # --- Extract and score: year ---
    date_str = candidate.get("date", {}).get("value", "")
    if date_str:
        try:
            # Wikidata dates are ISO format: "1985-01-01T00:00:00Z"
            result["year"] = int(date_str[:4])
        except (ValueError, IndexError):
            pass

    # --- Extract: start date (P580) as fallback for shows ---
    start_date_str = candidate.get("startDate", {}).get("value", "")
    if start_date_str and not result["year"]:
        try:
            result["year"] = int(start_date_str[:4])
        except (ValueError, IndexError):
            pass

    if result["year"] and year_hint and result["year"] == year_hint:
        result["score"] += CONFIDENCE["YEAR_MATCH_BONUS"]
        result["score_breakdown"]["year_match"] = CONFIDENCE["YEAR_MATCH_BONUS"]

    # --- Extract and score: image ---
    image_uri = candidate.get("image", {}).get("value", "")
    if image_uri:
        # Extract the filename from the Commons URI
        # e.g., "http://commons.wikimedia.org/wiki/Special:FilePath/Example.jpg"
        # → "Example.jpg"
        result["image_filename"] = image_uri.split("/")[-1]
        # URL-decode the filename (spaces encoded as %20, etc.)
        from urllib.parse import unquote
        result["image_filename"] = unquote(result["image_filename"])
        result["score"] += CONFIDENCE["HAS_IMAGE_BONUS"]
        result["score_breakdown"]["has_image"] = CONFIDENCE["HAS_IMAGE_BONUS"]

    # --- Extract: genres ---
    genre_label = candidate.get("genreLabel", {}).get("value", "")
    if genre_label and genre_label not in result["genres"]:
        result["genres"].append(genre_label)

    # --- Extract: description (1-2 sentence Wikidata description) ---
    desc_val = candidate.get("description", {}).get("value", "")
    if desc_val:
        result["description"] = desc_val

    # --- Extract: source URL (English Wikipedia article) ---
    article_val = candidate.get("article", {}).get("value", "")
    if article_val:
        result["source_url"] = article_val

    # --- Extract: platform IDs (for streaming/availability) ---
    tmdb_movie_id = candidate.get("tmdbMovieId", {}).get("value", "")
    if tmdb_movie_id:
        result["tmdb_movie_id"] = tmdb_movie_id
    tmdb_tv_id = candidate.get("tmdbTvId", {}).get("value", "")
    if tmdb_tv_id:
        result["tmdb_tv_id"] = tmdb_tv_id
    open_library_id = candidate.get("openLibraryId", {}).get("value", "")
    if open_library_id:
        result["open_library_id"] = open_library_id
    steam_id = candidate.get("steamId", {}).get("value", "")
    if steam_id:
        result["steam_id"] = steam_id

    # --- Fuzzy query penalty ---
    if is_fuzzy:
        result["score"] += CONFIDENCE["AMBIGUITY_PENALTY"]
        result["score_breakdown"]["fuzzy_penalty"] = CONFIDENCE["AMBIGUITY_PENALTY"]

    return result


def _normalize_for_comparison(text: str) -> str:
    """
    Normalize a string for comparison purposes (not for display).
    Strips punctuation, lowercases, collapses whitespace.
    """
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def merge_candidates(raw_bindings: list[dict], title: str, year_hint: Optional[int], is_fuzzy: bool = False) -> list[dict]:
    """
    Merge multiple SPARQL result bindings into scored candidates.

    WHY MERGE? A single Wikidata item may appear in multiple result rows
    because of multiple genres (P136 is multi-valued). We group by QID
    and merge the genre lists, taking the max score components.

    RETURNS:
        List of scored candidate dicts, sorted by score descending.
    """
    by_qid = {}

    for binding in raw_bindings:
        scored = score_candidate(binding, title, year_hint, is_fuzzy)
        qid = scored["qid"]
        if not qid:
            continue

        if qid not in by_qid:
            by_qid[qid] = scored
        else:
            existing = by_qid[qid]
            # Merge genres
            for g in scored["genres"]:
                if g not in existing["genres"]:
                    existing["genres"].append(g)
            # Merge alt_labels
            for al in scored.get("alt_labels", []):
                if al not in existing.get("alt_labels", []):
                    existing.setdefault("alt_labels", []).append(al)
            # Take the higher score (shouldn't differ much, but safety)
            if scored["score"] > existing["score"]:
                existing["score"] = scored["score"]
                existing["score_breakdown"] = scored["score_breakdown"]
            # Take year/image/description/source_url if not yet present
            if not existing["year"] and scored["year"]:
                existing["year"] = scored["year"]
            if not existing["image_filename"] and scored["image_filename"]:
                existing["image_filename"] = scored["image_filename"]
            if not existing["description"] and scored["description"]:
                existing["description"] = scored["description"]
            if not existing["source_url"] and scored["source_url"]:
                existing["source_url"] = scored["source_url"]
            # Carry over platform IDs
            for pid_key in ("tmdb_movie_id", "tmdb_tv_id", "open_library_id", "steam_id"):
                if not existing.get(pid_key) and scored.get(pid_key):
                    existing[pid_key] = scored[pid_key]

    # Post-merge: re-check alt_labels for candidates that didn't get a label match.
    # A candidate may have gotten no label bonus from its primary label, but after
    # merging all alt_labels, one of them might match the search title.
    norm_title = _normalize_for_comparison(title)
    for cand in by_qid.values():
        has_label_bonus = any(
            k in cand["score_breakdown"]
            for k in ("exact_label", "close_label", "alt_label_match")
        )
        if has_label_bonus:
            continue
        for al in cand.get("alt_labels", []):
            if al.lower() == title.lower():
                cand["score"] += CONFIDENCE["CLOSE_LABEL_BONUS"]
                cand["score_breakdown"]["alt_label_match"] = CONFIDENCE["CLOSE_LABEL_BONUS"]
                break
            elif _normalize_for_comparison(al) == norm_title:
                cand["score"] += CONFIDENCE["CLOSE_LABEL_BONUS"]
                cand["score_breakdown"]["alt_label_match"] = CONFIDENCE["CLOSE_LABEL_BONUS"]
                break

    candidates = list(by_qid.values())
    candidates.sort(key=lambda c: c["score"], reverse=True)
    return candidates


def confidence_gate(candidates: list[dict]) -> Optional[dict]:
    """
    Apply confidence gating to select the best candidate (or reject all).

    RULES:
        1. If no candidates, return None.
        2. If top candidate score < ACCEPT_THRESHOLD, return None.
        3. If two candidates are close in score (margin < MIN_MARGIN),
           return None (ambiguous — we don't guess).
        4. Otherwise, return the top candidate.

    This is the CRITICAL function for avoiding hallucinated metadata.
    When in doubt, we return None and the item gets no enrichment.
    """
    if not candidates:
        return None

    top = candidates[0]

    # Rule 2: minimum score threshold
    if top["score"] < CONFIDENCE["ACCEPT_THRESHOLD"]:
        logging.debug(
            f"  Confidence too low: score={top['score']} < threshold={CONFIDENCE['ACCEPT_THRESHOLD']} "
            f"for '{top['label']}' ({top['qid']})"
        )
        return None

    # Rule 3: ambiguity check (if multiple candidates)
    if len(candidates) >= 2:
        second = candidates[1]
        margin = top["score"] - second["score"]
        if margin < CONFIDENCE["MIN_MARGIN"]:
            logging.debug(
                f"  Ambiguous: top='{top['label']}' (score={top['score']}) vs "
                f"'{second['label']}' (score={second['score']}), margin={margin}"
            )
            return None

    logging.debug(
        f"  Accepted: '{top['label']}' ({top['qid']}) score={top['score']} "
        f"breakdown={top['score_breakdown']}"
    )
    return top


def download_cover(
    image_filename: str,
    cover_slug: str,
    covers_dir: Path,
    session: "requests.Session",
    dry_run: bool = False,
) -> Optional[str]:
    """
    Download a cover image from Wikimedia Commons to the local vault.

    PARAMETERS:
        image_filename: The filename on Commons (e.g., "Redline_poster.jpg")
        cover_slug:     Our stable slug for the local filename (e.g., "redline-2009")
        covers_dir:     Path to the covers directory in the vault
        session:        requests.Session for HTTP
        dry_run:        If True, simulate but don't actually download

    RETURNS:
        Local filename (e.g., "redline-2009.jpg") if successful, None otherwise.

    NAMING:
        We use our own stable slug + the original extension.
        If the original has no extension or a weird one, we default to .jpg.
    """
    # Determine file extension from the Commons filename
    ext = Path(image_filename).suffix.lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"):
        ext = ".jpg"  # fallback to jpg

    local_filename = f"{cover_slug}{ext}"
    local_path = covers_dir / local_filename

    if dry_run:
        logging.info(f"  [DRY-RUN] Would download cover: {image_filename} → {local_filename}")
        return local_filename

    if local_path.exists():
        logging.info(f"  Cover already exists: {local_filename}")
        return local_filename

    # Build the download URL using Wikimedia's FilePath redirect
    # We URL-encode the filename for the request
    from urllib.parse import quote
    encoded_filename = quote(image_filename.replace(" ", "_"))
    url = COMMONS_THUMB_TEMPLATE.format(filename=encoded_filename)

    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, stream=True)
        resp.raise_for_status()

        # Check that we got an image (not an HTML error page)
        content_type = resp.headers.get("Content-Type", "")
        if "image" not in content_type and "octet-stream" not in content_type:
            logging.warning(f"  Cover download returned non-image content-type: {content_type}")
            return None

        # Write to a temp file first, then rename on success.
        # This prevents partial/corrupt files from blocking future retries.
        covers_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")
        try:
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            file_size = tmp_path.stat().st_size
            if file_size < 1000:
                # Suspiciously small — probably an error page
                logging.warning(f"  Cover file suspiciously small ({file_size} bytes): {local_filename}")
                tmp_path.unlink(missing_ok=True)
                return None

            # Atomic-ish rename from temp to final path
            tmp_path.rename(local_path)
            logging.info(f"  Downloaded cover: {local_filename} ({file_size:,} bytes)")
            return local_filename
        except OSError as e:
            logging.warning(f"  Cover file write failed for '{local_filename}': {e}")
            tmp_path.unlink(missing_ok=True)
            return None

    except requests.exceptions.RequestException as e:
        logging.warning(f"  Cover download failed for '{image_filename}': {e}")
        return None


def fetch_tmdb_poster(
    tmdb_id: str,
    media_type: str,
    tmdb_key: str,
    cover_slug: str,
    covers_dir: Path,
    session: "requests.Session",
    dry_run: bool = False,
) -> Optional[str]:
    """
    Fetch a poster image from TMDb as a cover fallback.

    Used when the Wikidata entry has no P18 image but does have a TMDb ID.
    TMDb has poster images for virtually every movie and TV show.

    PARAMETERS:
        tmdb_id:    TMDb numeric ID
        media_type: "movie" or "show"
        tmdb_key:   TMDb API v3 key
        cover_slug: Local filename slug
        covers_dir: Output directory for covers
        session:    requests.Session
        dry_run:    If True, simulate only

    RETURNS:
        Local filename if successful, None otherwise.
    """
    tmdb_type = "tv" if media_type == "show" else "movie"
    url = f"{TMDB_API_BASE}/{tmdb_type}/{tmdb_id}"

    try:
        resp = session.get(
            url,
            params={"api_key": tmdb_key},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            logging.debug(f"  TMDb details lookup failed: {resp.status_code}")
            return None

        data = resp.json()
        poster_path = data.get("poster_path")
        if not poster_path:
            logging.debug(f"  TMDb: no poster for {tmdb_type}/{tmdb_id}")
            return None

        # Download the poster image
        poster_url = f"{TMDB_IMAGE_BASE}{poster_path}"
        ext = Path(poster_path).suffix.lower() or ".jpg"
        local_filename = f"{cover_slug}{ext}"
        local_path = covers_dir / local_filename

        if dry_run:
            logging.info(f"  [DRY-RUN] Would download TMDb poster: {local_filename}")
            return local_filename

        if local_path.exists():
            logging.info(f"  TMDb poster already exists: {local_filename}")
            return local_filename

        covers_dir.mkdir(parents=True, exist_ok=True)
        img_resp = session.get(poster_url, timeout=REQUEST_TIMEOUT, stream=True)
        img_resp.raise_for_status()

        tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")
        try:
            with open(tmp_path, "wb") as f:
                for chunk in img_resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            file_size = tmp_path.stat().st_size
            if file_size < 1000:
                logging.warning(f"  TMDb poster suspiciously small ({file_size} bytes)")
                tmp_path.unlink(missing_ok=True)
                return None

            tmp_path.rename(local_path)
            logging.info(f"  Downloaded TMDb poster: {local_filename} ({file_size:,} bytes)")
            return local_filename
        except OSError as e:
            logging.warning(f"  TMDb poster write failed: {e}")
            tmp_path.unlink(missing_ok=True)
            return None

    except requests.exceptions.RequestException as e:
        logging.warning(f"  TMDb poster fetch failed: {e}")
        return None


def fetch_openlibrary_cover(
    ol_id: str,
    cover_slug: str,
    covers_dir: Path,
    session: "requests.Session",
    dry_run: bool = False,
) -> Optional[str]:
    """
    Fetch a book cover from Open Library as a cover fallback.

    Uses the Open Library Covers API which serves cover images by OLID.
    No API key required.

    PARAMETERS:
        ol_id:      Open Library work ID (e.g., "OL45883W")
        cover_slug: Local filename slug
        covers_dir: Output directory for covers
        session:    requests.Session
        dry_run:    If True, simulate only

    RETURNS:
        Local filename if successful, None otherwise.
    """
    # Open Library covers API: /b/olid/{OLID}-M.jpg  (M = medium size)
    cover_url = f"{OPENLIBRARY_COVERS_BASE}/{ol_id}-M.jpg"
    local_filename = f"{cover_slug}.jpg"
    local_path = covers_dir / local_filename

    if dry_run:
        logging.info(f"  [DRY-RUN] Would download Open Library cover: {local_filename}")
        return local_filename

    if local_path.exists():
        logging.info(f"  Open Library cover already exists: {local_filename}")
        return local_filename

    try:
        resp = session.get(cover_url, timeout=REQUEST_TIMEOUT, stream=True)
        if resp.status_code != 200:
            logging.debug(f"  Open Library cover not found for {ol_id}")
            return None

        content_type = resp.headers.get("Content-Type", "")
        if "image" not in content_type:
            logging.debug(f"  Open Library returned non-image: {content_type}")
            return None

        covers_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = local_path.with_suffix(".jpg.tmp")
        try:
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            file_size = tmp_path.stat().st_size
            if file_size < 1000:
                # Open Library returns a 1x1 pixel placeholder for missing covers
                logging.debug(f"  Open Library cover too small ({file_size} bytes), skipping")
                tmp_path.unlink(missing_ok=True)
                return None

            tmp_path.rename(local_path)
            logging.info(f"  Downloaded Open Library cover: {local_filename} ({file_size:,} bytes)")
            return local_filename
        except OSError as e:
            logging.warning(f"  Open Library cover write failed: {e}")
            tmp_path.unlink(missing_ok=True)
            return None

    except requests.exceptions.RequestException as e:
        logging.warning(f"  Open Library cover fetch failed: {e}")
        return None


def lookup_tmdb_streaming(
    tmdb_id: str,
    media_type: str,
    tmdb_key: str,
    country: str,
    session: "requests.Session",
) -> list[dict]:
    """
    Look up streaming/rent/buy availability via TMDb's watch/providers API.

    TMDb's watch/providers data is sourced from JustWatch and provides
    per-country availability across streaming platforms.

    PARAMETERS:
        tmdb_id:    The TMDb ID (numeric string)
        media_type: "movie" or "show" (show maps to TMDb's "tv" endpoint)
        tmdb_key:   TMDb API key (v3 auth)
        country:    ISO 3166-1 alpha-2 country code (e.g., "US", "GB")
        session:    requests.Session for HTTP

    RETURNS:
        List of dicts: [{"provider": "Netflix", "type": "subscription"}, ...]
        Empty list on any error or if no data for the given country.
    """
    # Map our type to TMDb endpoint path
    tmdb_type = "tv" if media_type == "show" else "movie"
    url = f"{TMDB_API_BASE}/{tmdb_type}/{tmdb_id}/watch/providers"

    try:
        resp = session.get(
            url,
            params={"api_key": tmdb_key},
            timeout=REQUEST_TIMEOUT,
        )

        if resp.status_code == 404:
            logging.debug(f"  TMDb: no watch/providers for {tmdb_type}/{tmdb_id}")
            return []

        if resp.status_code == 401:
            logging.warning("  TMDb: invalid API key (401)")
            return []

        resp.raise_for_status()
        data = resp.json()

    except requests.exceptions.RequestException as e:
        logging.warning(f"  TMDb watch/providers request failed: {e}")
        return []
    except json.JSONDecodeError:
        logging.warning("  TMDb returned non-JSON response")
        return []

    # Extract country-specific results
    country_data = data.get("results", {}).get(country.upper(), {})
    if not country_data:
        logging.debug(f"  TMDb: no watch/providers data for country={country}")
        return []

    providers = []
    seen = set()
    for provider_type in TMDB_WATCH_PROVIDER_TYPES:
        for entry in country_data.get(provider_type, []):
            name = entry.get("provider_name", "")
            if not name:
                continue
            # Deduplicate: same provider can appear in multiple types
            key = (name, provider_type)
            if key in seen:
                continue
            seen.add(key)
            providers.append({
                "provider": name,
                "type": TMDB_WATCH_PROVIDER_TYPE_LABELS.get(provider_type, provider_type),
            })

    return providers


def _generate_fallback_titles(search_title: str) -> list[str]:
    """
    Generate simplified title variants for fallback searches.

    When the primary EntitySearch fails to find candidates, we try
    progressively simpler forms of the title:
      1. Without leading "The " (Wikidata labels sometimes omit articles)
      2. Main title only (before colon/subtitle separator)
      3. First N significant words (for very long titles with author/publisher junk)
      4. Known proper title (for titles with embedded author names like "Arendt: ...")

    Returns a list of alternative search strings (may be empty if no
    meaningful simplification is possible).
    """
    variants = []

    # Variant 1: strip leading "The " — Wikidata labels sometimes omit articles
    if search_title.lower().startswith("the ") and len(search_title) > 6:
        without_the = search_title[4:].strip()
        if without_the not in variants:
            variants.append(without_the)

    # Variant 2: strip subtitle (part after colon)
    if ':' in search_title:
        main_part = search_title.split(':')[0].strip()
        if len(main_part) >= 3 and main_part not in variants:
            variants.append(main_part)
        # Also try the subtitle alone — handles "Author: Real Title" patterns
        after_colon = search_title.split(':', 1)[1].strip()
        if len(after_colon) >= 3 and after_colon not in variants:
            variants.append(after_colon)

    # Variant 3: for long titles (5+ words), try shorter prefixes
    # This helps with titles polluted by author names or annotations
    # e.g., "The Official Stardew Valley Cookbook ConcernedApe Ryan Novak"
    words = search_title.split()
    if len(words) >= 5:
        short = " ".join(words[:4])
        if short not in variants:
            variants.append(short)
        shorter = " ".join(words[:3])
        if shorter not in variants:
            variants.append(shorter)

    return variants


def enrich_entry(
    entry: dict,
    session: "requests.Session",
    covers_dir: Path,
    covers_rel_path: str,
    sleep_time: float,
    dry_run: bool = False,
    streaming: bool = False,
    tmdb_key: Optional[str] = None,
    country: str = "US",
) -> dict:
    """
    Enrich a single entry with Wikidata metadata.

    PIPELINE:
        1. Build SPARQL query using EntitySearch (indexed, fast)
        2. Execute query
        3. Score and merge candidates
        4. Apply confidence gate
        6. If accepted: extract year, download cover, extract genres
        7. If --streaming: extract platform links, query TMDb for watch providers
        8. Attach enrichment data to the entry

    RETURNS:
        The entry dict, updated with enrichment fields:
        {
            "enriched": bool,
            "wikidata_qid": str|None,
            "enriched_year": int|None,
            "cover_filename": str|None,
            "cover_source": str|None,
            "cover_confidence": str|None,
            "enriched_genres": list[str],
            "streaming_providers": list[dict],
            "open_library_url": str|None,
            "steam_url": str|None,
            "skip_reason": str|None,
        }
    """
    title = entry["clean_title"]
    search_title = entry.get("search_title", title)
    media_type = entry["type"]
    year_hint = entry.get("year_hint")

    # Initialize enrichment fields
    entry.update({
        "enriched": False,
        "wikidata_qid": None,
        "enriched_year": None,
        "cover_filename": None,
        "cover_source": None,
        "cover_confidence": None,
        "enriched_genres": [],
        "description": None,
        "source_url": None,
        "streaming_providers": [],
        "open_library_url": None,
        "steam_url": None,
        "skip_reason": None,
    })

    # --- Step 1-2: Query Wikidata via EntitySearch ---
    # Use search_title (parentheticals stripped) for SPARQL, but score
    # candidates against the search_title for confidence.
    # EntitySearch returns ranked results; scoring + confidence gate
    # handle exact vs near-match disambiguation.
    sparql = build_sparql_query(search_title, media_type)
    if not sparql:
        entry["skip_reason"] = "no SPARQL query (unknown type)"
        return entry

    logging.info(f"  Querying Wikidata: '{search_title}' [{media_type}]")
    bindings = query_wikidata(sparql, session)
    candidates = merge_candidates(bindings, search_title, year_hint, is_fuzzy=False)

    # --- Step 4-5: Confidence gate ---
    winner = confidence_gate(candidates)

    # --- Step 4b: Fallback searches ---
    # If no winner from exact search, try progressively:
    #   1. Year-qualified search ("Title Year") for disambiguation
    #   2. Simplified title variants (subtitle stripped, shortened)
    if winner is None:
        # Try year-qualified search first (helps disambiguate "Safe 1995", "Mirai 2018")
        if year_hint:
            year_title = f"{search_title} {year_hint}"
            logging.info(f"  Year-qualified search: '{year_title}' [{media_type}]")
            fb_sparql = build_sparql_query(year_title, media_type)
            if fb_sparql:
                fb_bindings = query_wikidata(fb_sparql, session)
                fb_candidates = merge_candidates(fb_bindings, search_title, year_hint, is_fuzzy=True)
                winner = confidence_gate(fb_candidates)
                if winner:
                    logging.info(f"  Year-qualified matched: '{winner['label']}' ({winner['qid']})")

    if winner is None:
        fallback_titles = _generate_fallback_titles(search_title)
        for fb_title in fallback_titles:
            if fb_title == search_title:
                continue  # already tried this one
            logging.info(f"  Fallback search: '{fb_title}' [{media_type}]")
            fb_sparql = build_sparql_query(fb_title, media_type)
            if not fb_sparql:
                continue
            fb_bindings = query_wikidata(fb_sparql, session)
            fb_candidates = merge_candidates(fb_bindings, fb_title, year_hint, is_fuzzy=True)
            winner = confidence_gate(fb_candidates)
            if winner:
                logging.info(f"  Fallback matched: '{winner['label']}' ({winner['qid']})")
                break

    if winner is None:
        reason = "no candidates" if not candidates else "ambiguous/low confidence"
        entry["skip_reason"] = reason
        logging.info(f"  SKIP: '{title}' — {reason}")
        return entry

    # --- Step 6: Extract data from the winner ---
    entry["enriched"] = True
    entry["wikidata_qid"] = winner["qid"]

    # Year: use Wikidata year, but only if it's consistent with our hint
    # (or if we have no hint)
    wd_year = winner.get("year")
    if wd_year:
        if year_hint is None or wd_year == year_hint:
            entry["enriched_year"] = wd_year
        elif year_hint and abs(wd_year - year_hint) <= 1:
            # Allow 1-year tolerance (release dates can vary by region)
            entry["enriched_year"] = wd_year
        else:
            # Year mismatch — trust our hint, don't use Wikidata's year
            logging.warning(
                f"  Year mismatch for '{title}': hint={year_hint}, Wikidata={wd_year}. "
                f"Using hint."
            )
            entry["enriched_year"] = year_hint

    # Cover: download if available (try Wikidata P18 first, then fallbacks)
    cover_slug = generate_slug_for_cover(title, entry["enriched_year"] or year_hint)

    # Try 1: Wikidata P18 image (highest quality, directly from Commons)
    if winner.get("image_filename"):
        cover_fn = download_cover(
            winner["image_filename"], cover_slug, covers_dir, session, dry_run
        )
        if cover_fn:
            entry["cover_filename"] = cover_fn
            entry["cover_source"] = "wikidata"
            if winner["score_breakdown"].get("exact_label"):
                entry["cover_confidence"] = "high"
            else:
                entry["cover_confidence"] = "medium"

    # Try 2: TMDb poster (movies/shows — requires API key)
    if not entry.get("cover_filename") and tmdb_key and media_type in ("movie", "show"):
        tmdb_id = winner.get("tmdb_movie_id") if media_type == "movie" else winner.get("tmdb_tv_id")
        if tmdb_id:
            time.sleep(sleep_time)
            cover_fn = fetch_tmdb_poster(
                tmdb_id, media_type, tmdb_key, cover_slug, covers_dir, session, dry_run
            )
            if cover_fn:
                entry["cover_filename"] = cover_fn
                entry["cover_source"] = "tmdb"
                entry["cover_confidence"] = "high"

    # Try 3: Open Library cover (books — no API key needed)
    if not entry.get("cover_filename") and media_type == "book":
        ol_id = winner.get("open_library_id")
        if ol_id:
            time.sleep(sleep_time)
            cover_fn = fetch_openlibrary_cover(
                ol_id, cover_slug, covers_dir, session, dry_run
            )
            if cover_fn:
                entry["cover_filename"] = cover_fn
                entry["cover_source"] = "openlibrary"
                entry["cover_confidence"] = "medium"

    # Genres: only 1-2, only if specific and unambiguous
    genres = winner.get("genres", [])
    # Filter out overly generic genres
    GENERIC_GENRES = {
        "fiction", "film", "video game", "television series",
        "entertainment", "media", "art", "culture",
    }
    filtered_genres = [
        g for g in genres
        if g.lower() not in GENERIC_GENRES and len(g) > 2
    ]
    entry["enriched_genres"] = filtered_genres[:2]  # max 2 genres

    # Description: short Wikidata description (if available)
    if winner.get("description"):
        entry["description"] = winner["description"]

    # Source URL: English Wikipedia link (if available)
    if winner.get("source_url"):
        entry["source_url"] = winner["source_url"]

    # --- Step 7: Streaming / platform availability ---
    if streaming:
        # Books: Open Library link (from Wikidata, no API key needed)
        if media_type == "book" and winner.get("open_library_id"):
            ol_id = winner["open_library_id"]
            entry["open_library_url"] = f"https://openlibrary.org/works/{ol_id}"
            logging.info(f"  Open Library: {entry['open_library_url']}")

        # Games: Steam link (from Wikidata, no API key needed)
        if media_type == "game" and winner.get("steam_id"):
            steam_id = winner["steam_id"]
            entry["steam_url"] = f"https://store.steampowered.com/app/{steam_id}"
            logging.info(f"  Steam: {entry['steam_url']}")

        # Movies/Shows: TMDb watch/providers (requires API key)
        if media_type in ("movie", "show") and tmdb_key:
            tmdb_id = winner.get("tmdb_movie_id") if media_type == "movie" else winner.get("tmdb_tv_id")
            if tmdb_id:
                time.sleep(sleep_time)  # throttle TMDb requests too
                providers = lookup_tmdb_streaming(
                    tmdb_id, media_type, tmdb_key, country, session
                )
                if providers:
                    entry["streaming_providers"] = providers
                    provider_names = [p["provider"] for p in providers[:3]]
                    logging.info(
                        f"  Streaming ({country}): {', '.join(provider_names)}"
                        f"{'...' if len(providers) > 3 else ''}"
                    )
                else:
                    logging.info(f"  No streaming data for {country}")
            else:
                logging.debug(f"  No TMDb ID found on Wikidata for '{title}'")

    logging.info(
        f"  ENRICHED: '{title}' → QID={winner['qid']}, "
        f"year={entry['enriched_year']}, cover={'yes' if entry['cover_filename'] else 'no'}, "
        f"genres={entry['enriched_genres']}"
    )

    return entry


# ===========================================================================
# SECTION 7: NOTE GENERATION
# ===========================================================================
# Generates Obsidian-compatible markdown notes with YAML frontmatter.

def generate_note_content(entry: dict, covers_rel_path: str) -> str:
    """
    Generate the full content of an Obsidian markdown note for a media entry.

    YAML FRONTMATTER FORMAT:
        ---
        type: movie
        status: backlog
        priority: someday
        year: 2009
        genre: [science fiction, action]
        mood: []
        source: from Anton
        cover: "[[media/covers/redline-2009.jpg]]"
        cover_source: wikidata
        cover_confidence: high
        description: "2009 Japanese animated science fiction film"
        source_url: https://en.wikipedia.org/wiki/Redline_(2009_film)
        wikidata: Q1234567
        ---
        # Redline

    RULES:
        - Required keys always present (type, status, priority, year, genre, mood)
        - Optional keys only if data is available and confident
        - year is blank (empty string) if unknown
        - genre and mood default to empty list []
        - Cover uses wikilink format for Obsidian rendering
        - Body is just the title as an H1 header
    """
    lines = ["---"]

    # --- Required YAML fields (always present) ---
    lines.append(f"type: {entry['type']}")
    lines.append("status: backlog")
    lines.append("priority: someday")

    # Year: use enriched year if available, else year_hint, else blank
    year = entry.get("enriched_year") or entry.get("year_hint")
    if year:
        lines.append(f"year: {year}")
    else:
        lines.append("year: ")

    # Genre: list format, or empty list
    genres = entry.get("enriched_genres", [])
    if genres:
        genre_str = ", ".join(genres)
        lines.append(f"genre: [{genre_str}]")
    else:
        lines.append("genre: []")

    # Mood: always empty (no enrichment source for this)
    lines.append("mood: []")

    # --- Optional YAML fields (only if data present) ---

    # Source: e.g., "from Anton"
    if entry.get("source"):
        lines.append(f"source: {entry['source']}")

    # Cover: wikilink to local image file
    if entry.get("cover_filename"):
        # Build the wikilink path relative to vault root
        cover_wikilink = f"{covers_rel_path}/{entry['cover_filename']}"
        lines.append(f'cover: "[[{cover_wikilink}]]"')

    # Cover source: where the cover came from
    if entry.get("cover_source"):
        lines.append(f"cover_source: {entry['cover_source']}")

    # Cover confidence
    if entry.get("cover_confidence"):
        lines.append(f"cover_confidence: {entry['cover_confidence']}")

    # Description: short Wikidata description
    if entry.get("description"):
        # Sanitize: strip newlines/carriage returns, escape for YAML
        desc = entry["description"].replace("\r", "").replace("\n", " ").strip()
        # Always quote descriptions — they often contain colons, commas, etc.
        # Escape internal double quotes per YAML spec
        desc = desc.replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'description: "{desc}"')

    # Source URL: link to English Wikipedia article (or Wikidata fallback)
    # Always quoted — URLs can contain # which YAML treats as comment
    if entry.get("source_url"):
        lines.append(f'source_url: "{entry["source_url"]}"')
    elif entry.get("wikidata_qid"):
        lines.append(f'source_url: "https://www.wikidata.org/wiki/{entry["wikidata_qid"]}"')

    # Wikidata QID: useful for manual verification
    if entry.get("wikidata_qid"):
        lines.append(f"wikidata: {entry['wikidata_qid']}")

    # Streaming availability (movies/shows via TMDb)
    if entry.get("streaming_providers"):
        lines.append("streaming:")
        for sp in entry["streaming_providers"]:
            provider = sp["provider"].replace('"', '\\"')
            lines.append(f'  - provider: "{provider}"')
            lines.append(f"    type: {sp['type']}")

    # Open Library URL (books)
    if entry.get("open_library_url"):
        lines.append(f'open_library_url: "{entry["open_library_url"]}"')

    # Steam URL (games)
    if entry.get("steam_url"):
        lines.append(f'steam_url: "{entry["steam_url"]}"')

    lines.append("---")
    lines.append("")
    lines.append(f"# {entry['clean_title']}")
    lines.append("")

    return "\n".join(lines)


def write_note(
    entry: dict,
    items_dir: Path,
    covers_rel_path: str,
    overwrite: bool = False,
    dry_run: bool = False,
) -> bool:
    """
    Write a single note file to disk.

    PARAMETERS:
        entry:          The enriched entry dict
        items_dir:      Path to the items output directory
        covers_rel_path: Relative path to covers from vault root (for wikilinks)
        overwrite:      If False, skip existing files
        dry_run:        If True, simulate but don't write

    RETURNS:
        True if the note was written (or would be in dry-run), False if skipped.

    CROSS-BATCH COLLISION HANDLING:
        If a file already exists and --overwrite is NOT set, we check whether
        the existing file belongs to a DIFFERENT media type. If so, we append
        the current entry's type to the filename to disambiguate (e.g.,
        "safe.md" exists as a movie → show version writes to "safe-show.md").
        This prevents silent data loss when batching by type.
    """
    filename = f"{entry['filename']}.md"
    filepath = items_dir / filename

    # --- Cross-batch collision detection ---
    # If file exists and we're NOT overwriting, check if it's a different item.
    # If same item (re-run), skip. If different item, add type suffix.
    if filepath.exists() and not overwrite:
        existing_type = _read_type_from_note(filepath)
        if existing_type and existing_type != entry["type"]:
            # Different type occupies this filename — add type suffix
            alt_filename = f"{entry['filename']}-{entry['type']}.md"
            alt_filepath = items_dir / alt_filename
            logging.info(
                f"  COLLISION (cross-type): '{filename}' exists as {existing_type}. "
                f"Writing as '{alt_filename}' instead."
            )
            filepath = alt_filepath
            filename = alt_filename
            # If even the alt exists, skip (true duplicate or triple collision)
            if filepath.exists() and not overwrite:
                logging.info(f"  SKIP (exists): {filepath}")
                return False
        else:
            # Same type or can't read — treat as same-item re-run, skip
            logging.info(f"  SKIP (exists): {filepath}")
            return False

    if dry_run:
        content = generate_note_content(entry, covers_rel_path)
        logging.info(f"  [DRY-RUN] Would write: {filepath}")
        logging.debug(f"  Content preview:\n{content[:200]}...")
        return True

    content = generate_note_content(entry, covers_rel_path)

    try:
        items_dir.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
    except OSError as e:
        logging.warning(f"  Failed to write note '{filepath}': {e}")
        return False

    logging.info(f"  Wrote: {filepath}")
    return True


def _read_type_from_note(filepath: Path) -> Optional[str]:
    """
    Read the 'type' field from an existing note's YAML frontmatter.

    Used for cross-batch collision detection: we need to know if an existing
    file with the same name belongs to a different media type.

    RETURNS:
        The type string ("book", "movie", "show", "game") or None if unreadable.
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            in_frontmatter = False
            for line in f:
                stripped = line.strip()
                if stripped == "---":
                    if not in_frontmatter:
                        in_frontmatter = True
                        continue
                    else:
                        break  # end of frontmatter
                if in_frontmatter:
                    match = re.match(r'^type:\s*(.+)$', stripped)
                    if match:
                        return match.group(1).strip()
    except (OSError, UnicodeDecodeError):
        pass
    return None


# ===========================================================================
# SECTION 8: CLI AND MAIN ORCHESTRATION
# ===========================================================================

def build_cli() -> argparse.ArgumentParser:
    """
    Build the command-line argument parser.

    REQUIRED ARGS:
        --input:    Path to the input markdown list file
        --vault:    Path to the Obsidian vault root directory

    OPTIONAL ARGS:
        --out-items:    Subdirectory for generated notes (relative to vault)
        --out-covers:   Subdirectory for downloaded covers (relative to vault)
        --overwrite:    Allow overwriting existing files
        --only:         Filter to a single media type
        --sleep:        Seconds to wait between Wikidata requests
        --dry-run:      Show what would be created without writing
        --no-enrich:    Skip Wikidata enrichment (just parse + generate stubs)
        --zip:          Package output as a zip file instead of writing to vault
        --verbose:      Enable debug-level logging
    """
    parser = argparse.ArgumentParser(
        description=(
            "Deterministic Media Library Generator: Parse a markdown media list, "
            "normalize titles, enrich with Wikidata metadata, and generate "
            "Obsidian-ready notes."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Process only movies, write to vault:
  python3 enrich_media.py --input media.md --vault ~/vault --only movies

  # Dry run to preview what would be created:
  python3 enrich_media.py --input media.md --vault ~/vault --dry-run

  # Full run with zip output:
  python3 enrich_media.py --input media.md --vault ~/vault --zip

  # Skip enrichment (just normalize + generate stub notes):
  python3 enrich_media.py --input media.md --vault ~/vault --no-enrich

  # Verbose mode with slow throttling:
  python3 enrich_media.py --input media.md --vault ~/vault --sleep 1.0 --verbose
        """,
    )

    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Path to the input markdown list file",
    )
    parser.add_argument(
        "--vault", "-v",
        required=True,
        help="Path to the Obsidian vault root directory",
    )
    parser.add_argument(
        "--out-items",
        default=None,
        help=(
            "Subdirectory for generated notes, relative to vault root. "
            "Default: media/items_enriched_<timestamp>"
        ),
    )
    parser.add_argument(
        "--out-covers",
        default="media/covers",
        help="Subdirectory for cover images, relative to vault root (default: media/covers)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Allow overwriting existing files (default: refuse and skip)",
    )
    parser.add_argument(
        "--only",
        choices=["movies", "shows", "books", "games",
                 "movie", "show", "book", "game"],
        default=None,
        help="Only process entries of this media type",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help=f"Seconds to wait between Wikidata requests (default: {DEFAULT_SLEEP})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Show what would be created without writing any files",
    )
    parser.add_argument(
        "--no-enrich",
        action="store_true",
        default=False,
        help="Skip Wikidata enrichment; generate stub notes with only parsed metadata",
    )
    parser.add_argument(
        "--zip",
        action="store_true",
        default=False,
        help="Package output as a zip file instead of writing directly to vault",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Enable debug-level logging",
    )
    parser.add_argument(
        "--streaming",
        action="store_true",
        default=False,
        help=(
            "Enable streaming/platform availability lookups. "
            "Movies/shows use TMDb (requires --tmdb-key). "
            "Books get Open Library links, games get Steam links (no key needed)."
        ),
    )
    parser.add_argument(
        "--tmdb-key",
        default=None,
        help=(
            "TMDb API key (v3) for poster covers and streaming lookups. "
            "Also reads from TMDB_API_KEY environment variable. "
            "When provided, TMDb posters are used as cover fallback for "
            "movies/shows that lack Wikidata images."
        ),
    )
    parser.add_argument(
        "--country",
        default="US",
        help=(
            "ISO 3166-1 alpha-2 country code for streaming availability "
            "(default: US). Examples: US, GB, DE, JP"
        ),
    )

    return parser


def normalize_only_type(only_arg: Optional[str]) -> Optional[str]:
    """
    Normalize the --only argument to our canonical type strings.
    Handles both singular and plural forms.
    """
    if only_arg is None:
        return None
    mapping = {
        "movies": "movie", "movie": "movie",
        "shows": "show", "show": "show",
        "books": "book", "book": "book",
        "games": "game", "game": "game",
    }
    return mapping.get(only_arg.lower())


def main():
    """
    Main orchestration function.

    PIPELINE:
        1. Parse CLI arguments
        2. Set up logging
        3. Parse input file into raw entries
        4. Normalize each entry (title cleaning, year extraction, etc.)
        5. Deduplicate
        6. Detect and resolve filename collisions
        7. (Optional) Enrich with Wikidata metadata
        8. Generate and write notes
        9. Print summary report
    """

    # -----------------------------------------------------------------------
    # STEP 1: Parse CLI arguments
    # -----------------------------------------------------------------------
    parser = build_cli()
    args = parser.parse_args()

    # -----------------------------------------------------------------------
    # STEP 2: Set up logging
    # -----------------------------------------------------------------------
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        stream=sys.stderr,
    )
    logging.info("=" * 60)
    logging.info("Media Library Generator + Metadata Enricher")
    logging.info("=" * 60)

    # -----------------------------------------------------------------------
    # STEP 3: Validate inputs
    # -----------------------------------------------------------------------
    input_path = Path(args.input).resolve()
    if not input_path.is_file():
        logging.error(f"Input file not found: {input_path}")
        sys.exit(1)

    vault_path = Path(args.vault).resolve()
    if not vault_path.is_dir() and not args.dry_run and not args.zip:
        logging.error(f"Vault directory not found: {vault_path}")
        sys.exit(1)

    only_type = normalize_only_type(args.only)

    # Determine output directories
    # If --zip mode, we write to a temp directory and zip it up at the end
    if args.zip:
        import tempfile
        work_dir = Path(tempfile.mkdtemp(prefix="media_enricher_"))
        logging.info(f"Zip mode: working in temp directory {work_dir}")
    else:
        work_dir = vault_path

    # Items output directory
    if args.out_items:
        items_rel = args.out_items
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        items_rel = f"media/items_enriched_{timestamp}"
    items_dir = work_dir / items_rel

    # Covers output directory
    covers_rel = args.out_covers
    covers_dir = work_dir / covers_rel

    logging.info(f"Input:      {input_path}")
    logging.info(f"Vault:      {vault_path}")
    logging.info(f"Items dir:  {items_rel}")
    logging.info(f"Covers dir: {covers_rel}")
    logging.info(f"Overwrite:  {args.overwrite}")
    logging.info(f"Dry run:    {args.dry_run}")
    logging.info(f"Enrich:     {not args.no_enrich}")

    # Resolve streaming settings
    streaming_enabled = args.streaming
    tmdb_key = args.tmdb_key or os.environ.get("TMDB_API_KEY")
    country = args.country.upper()

    if streaming_enabled:
        logging.info(f"Streaming:  enabled (country={country})")
        if tmdb_key:
            logging.info("TMDb key:   provided")
        else:
            logging.info(
                "TMDb key:   NOT provided — movie/show streaming will be skipped. "
                "Book/game platform links will still be fetched from Wikidata."
            )
    else:
        logging.info("Streaming:  disabled (use --streaming to enable)")

    if not args.no_enrich and not tmdb_key:
        logging.info(
            "TMDb key:   NOT provided — cover art limited to Wikidata Commons. "
            "Pass --tmdb-key or set TMDB_API_KEY for many more poster downloads."
        )

    if only_type:
        logging.info(f"Filter:     {only_type} only")

    # -----------------------------------------------------------------------
    # STEP 4: Parse input file
    # -----------------------------------------------------------------------
    raw_entries = parse_input_file(str(input_path), only_type=only_type)
    if not raw_entries:
        logging.warning("No entries found in input file. Check section headers.")
        if args.zip:
            shutil.rmtree(work_dir, ignore_errors=True)
        sys.exit(0)

    # Initialize variables used in the summary (in case of early exit)
    entries = []
    written = 0
    skipped_exists = 0
    zip_path = None

    # -----------------------------------------------------------------------
    # Wrap the pipeline in try/finally so that in --zip mode, the temp
    # directory is always cleaned up — even on crash or dry-run.
    # -----------------------------------------------------------------------
    try:
        # -----------------------------------------------------------------------
        # STEP 5: Normalize each entry
        # -----------------------------------------------------------------------
        logging.info("--- Normalizing entries ---")
        for entry in raw_entries:
            extract_and_normalize(entry)
            logging.debug(
                f"  '{entry['raw_title']}' → clean='{entry['clean_title']}' "
                f"year_hint={entry.get('year_hint')} source={entry.get('source')} "
                f"filename='{entry['filename']}'"
            )

        # -----------------------------------------------------------------------
        # STEP 6: Deduplicate
        # -----------------------------------------------------------------------
        logging.info("--- Deduplicating ---")
        entries = deduplicate_entries(raw_entries)

        # -----------------------------------------------------------------------
        # STEP 7: Detect filename collisions
        # -----------------------------------------------------------------------
        logging.info("--- Checking for filename collisions ---")
        entries = detect_filename_collisions(entries)

        # -----------------------------------------------------------------------
        # STEP 8: Enrich with Wikidata (optional)
        # -----------------------------------------------------------------------
        if not args.no_enrich:
            if not HAS_REQUESTS:
                logging.error(
                    "The 'requests' library is required for Wikidata enrichment. "
                    "Install it with: uv sync (or pip install requests)\n"
                    "Or use --no-enrich to skip enrichment."
                )
                sys.exit(1)

            logging.info("--- Enriching with Wikidata ---")
            session = requests.Session()
            session.headers.update({"User-Agent": USER_AGENT})

            # Initialize adaptive rate limiter from --sleep
            _rate_limiter["min_interval"] = args.sleep
            _rate_limiter["current_interval"] = args.sleep

            for i, entry in enumerate(entries, start=1):
                logging.info(f"[{i}/{len(entries)}] Processing: '{entry['clean_title']}' [{entry['type']}]")
                enrich_entry(
                    entry, session, covers_dir, covers_rel,
                    sleep_time=args.sleep, dry_run=args.dry_run,
                    streaming=streaming_enabled, tmdb_key=tmdb_key,
                    country=country,
                )
                # Throttle between items (rate limiter handles adaptive delays,
                # but we always wait at least --sleep between entries)
                if i < len(entries):
                    time.sleep(_rate_limiter["current_interval"])
        else:
            logging.info("--- Skipping enrichment (--no-enrich) ---")
            for entry in entries:
                entry.update({
                    "enriched": False,
                    "wikidata_qid": None,
                    "enriched_year": None,
                    "cover_filename": None,
                    "cover_source": None,
                    "cover_confidence": None,
                    "enriched_genres": [],
                    "description": None,
                    "source_url": None,
                    "streaming_providers": [],
                    "open_library_url": None,
                    "steam_url": None,
                    "skip_reason": "enrichment disabled",
                })

        # -----------------------------------------------------------------------
        # STEP 9: Generate and write notes
        # -----------------------------------------------------------------------
        logging.info("--- Generating notes ---")

        for entry in entries:
            result = write_note(
                entry, items_dir, covers_rel,
                overwrite=args.overwrite, dry_run=args.dry_run,
            )
            if result:
                written += 1
            else:
                skipped_exists += 1

        # -----------------------------------------------------------------------
        # STEP 9b: Clean up orphaned covers
        # -----------------------------------------------------------------------
        # A cover is orphaned if it was downloaded but the corresponding note
        # was not written (e.g., note already existed, or write failed).
        # We only clean up covers that THIS run downloaded but won't reference.
        if not args.dry_run and covers_dir.is_dir():
            referenced_covers = {
                e["cover_filename"] for e in entries
                if e.get("cover_filename")
            }
            for cover_file in covers_dir.iterdir():
                if cover_file.is_file() and cover_file.name not in referenced_covers:
                    # Only remove .tmp files left by interrupted downloads
                    if cover_file.suffix == ".tmp":
                        logging.info(f"  Cleaning up incomplete download: {cover_file.name}")
                        cover_file.unlink(missing_ok=True)

        # -----------------------------------------------------------------------
        # STEP 10: Create zip if requested
        # -----------------------------------------------------------------------
        if args.zip and not args.dry_run:
            zip_name = f"media_enriched_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            zip_path = Path.cwd() / zip_name

            logging.info(f"--- Creating zip: {zip_path} ---")
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for root, _dirs, files in os.walk(work_dir):
                    for file in files:
                        file_path = Path(root) / file
                        arcname = file_path.relative_to(work_dir)
                        zf.write(file_path, arcname)

            logging.info(f"Zip created: {zip_path}")

    finally:
        # Always clean up the temp directory in zip mode
        if args.zip:
            shutil.rmtree(work_dir, ignore_errors=True)

    # -----------------------------------------------------------------------
    # STEP 11: Summary report
    # -----------------------------------------------------------------------
    enriched_count = sum(1 for e in entries if e.get("enriched"))
    covers_count = sum(1 for e in entries if e.get("cover_filename"))
    skipped_ambiguous = sum(1 for e in entries if e.get("skip_reason") and "ambiguous" in e.get("skip_reason", ""))
    skipped_nocand = sum(1 for e in entries if e.get("skip_reason") and "no candidates" in e.get("skip_reason", ""))
    streaming_count = sum(1 for e in entries if e.get("streaming_providers"))
    platform_count = sum(1 for e in entries if e.get("open_library_url") or e.get("steam_url"))

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total entries parsed:     {len(raw_entries)}")
    print(f"After dedup:              {len(entries)}")
    print(f"Notes written:            {written}")
    print(f"Skipped (already exist):  {skipped_exists}")
    if not args.no_enrich:
        print(f"Enriched (Wikidata):      {enriched_count}")
        print(f"Covers downloaded:        {covers_count}")
        print(f"Skipped (ambiguous):      {skipped_ambiguous}")
        print(f"Skipped (no candidates):  {skipped_nocand}")
    if streaming_enabled:
        print(f"Streaming data found:     {streaming_count}")
        print(f"Platform links found:     {platform_count}")
    print("=" * 60)

    # Per-type breakdown
    type_counts = {}
    for e in entries:
        t = e["type"]
        type_counts.setdefault(t, {"total": 0, "enriched": 0, "covers": 0, "streaming": 0, "platforms": 0})
        type_counts[t]["total"] += 1
        if e.get("enriched"):
            type_counts[t]["enriched"] += 1
        if e.get("cover_filename"):
            type_counts[t]["covers"] += 1
        if e.get("streaming_providers"):
            type_counts[t]["streaming"] += 1
        if e.get("open_library_url") or e.get("steam_url"):
            type_counts[t]["platforms"] += 1

    if type_counts:
        print("\nPer-type breakdown:")
        for t, counts in sorted(type_counts.items()):
            line = f"  {t:8s}: {counts['total']} total, {counts['enriched']} enriched, {counts['covers']} covers"
            if streaming_enabled:
                if t in ("movie", "show"):
                    line += f", {counts['streaming']} streaming"
                elif t in ("book", "game"):
                    line += f", {counts['platforms']} platform links"
            print(line)

    if zip_path:
        print(f"\nOutput zip: {zip_path}")
    elif not args.dry_run:
        print(f"\nOutput dir: {items_dir}")

    print()


# ===========================================================================
# ENTRY POINT
# ===========================================================================
if __name__ == "__main__":
    main()
