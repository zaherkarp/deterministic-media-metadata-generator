# Media Library Generator + Metadata Enricher

A deterministic, script-driven tool for building an Obsidian media library from a markdown list. Parses, normalizes, deduplicates, and enriches media entries with Wikidata metadata — conservatively.

## Philosophy

**Conservative over comprehensive.** This tool will never insert metadata it isn't confident about. Missing data is always preferred over incorrect data. No hallucinated URLs, no placeholder images, no guessed years.

## Requirements

- Python 3.10+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (recommended) or pip
- macOS, Linux, or Windows (cross-platform filenames)
- An internet connection (for Wikidata enrichment; not needed with `--no-enrich`)

## Setup

```bash
# 1. Install uv (if you don't have it)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Clone the repository
git clone <repo-url>
cd deterministic-media-metadata-generator

# 3. Install dependencies (uv creates the venv automatically)
uv sync
```

That's it. `uv sync` reads `pyproject.toml`, creates a `.venv`, and installs `requests` into it.

## Running the Script

All commands use `uv run` — this ensures the correct Python and dependencies are used automatically. No need to manually activate a venv.

### Step-by-step: First run

```bash
# 1. Create your input file (see "Input Format" below for structure)
#    The file must have ## section headers for each media type.

# 2. Always start with a dry run to preview what will be created:
uv run python enrich_media.py --input my_media.md --vault ~/my-vault --dry-run

#    This prints what notes WOULD be written without touching the filesystem.
#    Review the output in your terminal. Check for title normalization issues.

# 3. Run for real, one type at a time (recommended):
uv run python enrich_media.py \
  --input my_media.md \
  --vault ~/my-vault \
  --only movies \
  --out-items media/items

#    This writes movie notes to ~/my-vault/media/items/
#    and cover images to ~/my-vault/media/covers/

# 4. Open your vault in Obsidian and review the generated notes.
#    Then repeat for other types:
uv run python enrich_media.py -i my_media.md -v ~/my-vault --only shows --out-items media/items
uv run python enrich_media.py -i my_media.md -v ~/my-vault --only books --out-items media/items
uv run python enrich_media.py -i my_media.md -v ~/my-vault --only games --out-items media/items
```

### Common run patterns

```bash
# Process all types at once (notes go to a timestamped folder):
uv run python enrich_media.py --input my_media.md --vault ~/my-vault

# Process all types into a specific folder:
uv run python enrich_media.py --input my_media.md --vault ~/my-vault --out-items media/items

# Skip Wikidata enrichment (just normalize + generate stub notes):
uv run python enrich_media.py --input my_media.md --vault ~/my-vault --no-enrich

# Package output as a zip instead of writing directly to vault:
uv run python enrich_media.py --input my_media.md --vault ~/my-vault --zip

# Overwrite existing notes (by default, existing notes are skipped):
uv run python enrich_media.py --input my_media.md --vault ~/my-vault --overwrite

# Verbose logging (shows SPARQL queries, confidence scores, etc.):
uv run python enrich_media.py --input my_media.md --vault ~/my-vault --verbose

# Slow down Wikidata requests if you're getting rate-limited:
uv run python enrich_media.py --input my_media.md --vault ~/my-vault --sleep 1.0
```

### Where output goes

| Output | Default path | Override with |
|---|---|---|
| Notes | `<vault>/media/items_enriched_<YYYYMMDD_HHMMSS>/` | `--out-items <relative-path>` |
| Cover images | `<vault>/media/covers/` | `--out-covers <relative-path>` |
| Zip (if `--zip`) | `./media_enriched_<YYYYMMDD_HHMMSS>.zip` | — |

Paths for `--out-items` and `--out-covers` are **relative to the vault root**, not absolute.

**Important:** If your vault path contains spaces (common with iCloud-synced vaults), wrap it in quotes:

```bash
uv run python enrich_media.py \
  --input media_list.md \
  --vault "/Users/you/Library/Mobile Documents/iCloud~md~obsidian/Documents/MyVault" \
  --out-items media/items
```

## CLI Reference

| Argument | Required | Default | Description |
|---|---|---|---|
| `--input`, `-i` | Yes | — | Path to input markdown list |
| `--vault`, `-v` | Yes | — | Path to Obsidian vault root |
| `--out-items` | No | `media/items_enriched_<timestamp>` | Subdirectory for notes |
| `--out-covers` | No | `media/covers` | Subdirectory for cover images |
| `--overwrite` | No | `false` | Allow overwriting existing files |
| `--only` | No | all | Filter: `movies`, `shows`, `books`, `games` |
| `--sleep` | No | `0.3` | Seconds between Wikidata requests |
| `--dry-run` | No | `false` | Preview without writing |
| `--no-enrich` | No | `false` | Skip Wikidata lookup entirely |
| `--zip` | No | `false` | Package output as zip |
| `--verbose` | No | `false` | Debug-level logging |
| `--streaming` | No | `false` | Enable streaming/platform availability lookups |
| `--tmdb-key` | No | — | TMDb API key (v3) for movie/show streaming. Also reads `TMDB_API_KEY` env var |
| `--country` | No | `US` | ISO 3166-1 country code for streaming availability (e.g., `US`, `GB`, `DE`) |

## Input Format

The input is a markdown file with section headers (`##`) for media types:

```markdown
## BOOKS
Wave in the Mind (LeGuin).md
Claim Your Inner Child (from Merija).md
1984

## SHOWS
The Mandalorian (from Anton).md

## MOVIES
Redline (2009).md
Come and See (1985, Иди и смотри).md
Little Murders, 1971

## GAMES
Sea  of Stars.md
```

**Recognized section headers:** BOOKS, SHOWS, MOVIES, GAMES, TV, TV SHOWS, FILMS, VIDEO GAMES (case-insensitive).

**Entry format:** One item per line. Markdown list prefixes (`-`, `*`, `1.`) are stripped. `.md` extensions are stripped. Wikilinks `[[...]]` are unwrapped.

## Output Format

Each note is a markdown file with YAML frontmatter:

```yaml
---
type: movie
status: backlog
priority: someday
year: 2009
genre: [action, science fiction]
mood: []
source: from Anton
cover: "[[media/covers/redline-2009.jpg]]"
cover_source: wikidata
cover_confidence: high
description: "2009 Japanese animated science fiction film"
source_url: "https://en.wikipedia.org/wiki/Redline_(2009_film)"
wikidata: Q1234567
---

# Redline
```

**Always-present keys:** `type`, `status`, `priority`, `year`, `genre`, `mood`

**Conditional keys (only when data is available):**

| Key | When present | Source |
|---|---|---|
| `source` | Entry had a `(from X)` annotation | Input file |
| `cover` | Wikidata P18 image downloaded successfully | Wikimedia Commons |
| `cover_source` | Cover was downloaded | Always `wikidata` |
| `cover_confidence` | Cover was downloaded | `high` (exact match) or `medium` |
| `description` | Wikidata has an English description | Wikidata `schema:description` |
| `source_url` | Wikipedia article or Wikidata page exists | English Wikipedia or Wikidata fallback |
| `wikidata` | Enrichment matched a Wikidata item | Wikidata QID |
| `streaming` | TMDb has watch/provider data for this country | TMDb (JustWatch) |
| `open_library_url` | Wikidata has Open Library Work ID (P5331) | Wikidata |
| `steam_url` | Wikidata has Steam App ID (P1733) | Wikidata |

## Title Normalization Rules

| Input | Output | Rule |
|---|---|---|
| `Frieren- beyond journeys end.md` | `Frieren: Beyond Journeys End` | `- ` → `: `, strip `.md` |
| `Sea  of Stars.md` | `Sea of Stars` | Collapse multiple spaces |
| `Redline (2009).md` | `Redline` | Extract year, strip `.md` |
| `The Mandalorian (from Anton).md` | `The Mandalorian` | Extract source |
| `Clipped (show)` | `Clipped` | Strip type descriptor |
| `Severence, lol` | `Severence` | Strip trailing junk |
| `Come and See (1985, Иди и смотри)` | `Come and See` | Extract year, strip non-English parenthetical |

**Important:** The script does NOT auto-correct spelling (e.g., "Severence" stays as-is). Spelling correction is too risky for deterministic output.

## Deduplication Rules

Entries are deduplicated by `(type, normalized_key)` where the key is:
- Lowercased
- Unicode-normalized (NFKD → ASCII)
- Punctuation stripped
- Whitespace collapsed
- Prefixed with media type

So `"Heavenly delusion"` (show) and `"Heavenly Delusion"` (show) both normalize to `show:heavenly delusion` → only one note is created.

When duplicates are found, the entry with more metadata (year hint, source annotation) is preferred.

## Confidence Rules

The enrichment engine uses a scoring system to decide whether to accept a Wikidata match:

| Component | Score | Condition |
|---|---|---|
| Exact label match | +3 | Wikidata label equals our title (case-insensitive) |
| Close label match | +2 | Labels match after normalization (strip punctuation) |
| Year match | +2 | Wikidata year matches our year hint |
| Has image | +1 | Candidate has a P18 image on Wikidata |
| Fuzzy query penalty | −2 | Result came from a CONTAINS query, not exact |

**Accept threshold: 3.** Candidates scoring below 3 are rejected.

**Ambiguity check:** If the top two candidates are within 1 point of each other, both are rejected — we can't distinguish them confidently.

**Result:** Common titles like "Safe" (which match dozens of Wikidata items) will be skipped. Specific titles like "Redline (2009)" will match confidently.

## Cover Images

Covers are downloaded from Wikimedia Commons (via Wikidata's P18 property):
- Stored locally in `media/covers/` inside the vault
- Named with stable slugs: `redline-2009.jpg`
- Thumbnail size: 500px width (not full resolution)
- Referenced in YAML via wikilink: `cover: "[[media/covers/redline-2009.jpg]]"`

**No external hotlinks.** Images are real files in your vault.

**No fake URLs.** If download fails, the cover field is omitted entirely.

## Streaming & Platform Availability

When `--streaming` is passed, the script adds "where to watch/play/read" data to notes. This is opt-in to preserve the no-API-key-by-default philosophy.

### Movies & Shows — TMDb Watch Providers

Uses TMDb's watch/providers API (powered by JustWatch data) to show streaming, rent, and buy options per country.

**Requirements:**
- A free TMDb API key (v3). Register at [themoviedb.org](https://www.themoviedb.org/settings/api).
- Pass via `--tmdb-key YOUR_KEY` or set the `TMDB_API_KEY` environment variable.

**How it works:**
1. During Wikidata enrichment, the script fetches the TMDb ID (P4983 for movies, P4947 for TV series).
2. If a TMDb ID is found and an API key is provided, it queries TMDb's `/movie/{id}/watch/providers` or `/tv/{id}/watch/providers` endpoint.
3. Results are filtered by `--country` (default `US`).

**Output in frontmatter:**
```yaml
streaming:
  - provider: "Netflix"
    type: subscription
  - provider: "Apple TV"
    type: rent
  - provider: "Amazon Video"
    type: buy
```

### Books — Open Library

Fetches the Open Library Work ID (P5331) from Wikidata — no API key needed.

```yaml
open_library_url: "https://openlibrary.org/works/OL45804W"
```

### Games — Steam

Fetches the Steam App ID (P1733) from Wikidata — no API key needed.

```yaml
steam_url: "https://store.steampowered.com/app/105600"
```

### Example Commands

```bash
# Enable streaming for movies (US availability):
uv run python enrich_media.py -i media.md -v ~/vault --only movies --streaming --tmdb-key YOUR_KEY

# Enable streaming with a different country:
uv run python enrich_media.py -i media.md -v ~/vault --streaming --tmdb-key YOUR_KEY --country GB

# Enable streaming via env var (no key on command line):
export TMDB_API_KEY=your_key_here
uv run python enrich_media.py -i media.md -v ~/vault --streaming

# Books/games get platform links without any API key:
uv run python enrich_media.py -i media.md -v ~/vault --only books --streaming
uv run python enrich_media.py -i media.md -v ~/vault --only games --streaming
```

**Note:** If `--streaming` is passed without a TMDb key, movie/show streaming data is skipped (with a log message), but book/game platform links still work since they come from Wikidata.

## Error Handling

The script is designed to fail gracefully at every stage:

- **Network errors** (timeout, connection refused): Logged as warnings, entry skipped, processing continues.
- **Wikidata rate limiting (429)**: Backs off 5 seconds and retries. If persistent, increase `--sleep`.
- **No Wikidata match**: Entry gets a stub note with no enrichment — never crashes.
- **Ambiguous match**: Both candidates rejected, entry gets a stub note.
- **Cover download failure**: Note is written without a cover field. No broken references.
- **Partial cover download**: Uses atomic temp-file-then-rename to prevent corrupt images on disk.
- **File write failure** (permissions, disk full): Logged as warning, entry skipped, processing continues.
- **Missing `requests` library**: Clear error message with install instructions. `--dry-run` and `--no-enrich` work without it.
- **Missing description or Wikipedia link**: Fields simply omitted from the note. Falls back to Wikidata URL when no Wikipedia article exists.

## Recommended Workflow

Process one type at a time for easier review:

```bash
# Step 1: Dry run to check normalization
uv run python enrich_media.py -i media.md -v ~/vault --dry-run

# Step 2: Movies first
uv run python enrich_media.py -i media.md -v ~/vault --only movies --out-items media/items

# Step 3: Review the movie notes in Obsidian, fix any issues

# Step 4: Shows
uv run python enrich_media.py -i media.md -v ~/vault --only shows --out-items media/items

# Step 5: Books
uv run python enrich_media.py -i media.md -v ~/vault --only books --out-items media/items

# Step 6: Games
uv run python enrich_media.py -i media.md -v ~/vault --only games --out-items media/items
```

Note: When using a fixed `--out-items` path across runs, existing notes are **skipped** (not overwritten) unless `--overwrite` is passed.

## Edge Cases Handled

- **Missing `.md` extension:** Stripped when present, no error when absent
- **Unicode titles:** Russian, Japanese, etc. preserved in display title; ASCII-normalized for dedup key only
- **Year-as-title:** "1984" is recognized as a title, not a year extraction target
- **Illegal filename characters:** `:*?"<>|/\` stripped from filenames
- **Filename collisions:** Resolved with short hash suffix when two different items produce the same slug
- **Multiple spaces:** Collapsed (`Sea  of Stars` → `Sea of Stars`)
- **Informal annotations:** `, lol`, `?`, etc. stripped from titles
- **Cross-type collisions:** If "Safe" exists as a movie note, the show version writes to `safe-show.md`

## Troubleshooting

**"Rate limited by Wikidata (429)"** — Increase `--sleep` to 1.0 or higher. Wikidata's public SPARQL endpoint has rate limits.

**"No candidates" for most items** — Check that section headers match expected formats (`## MOVIES`, not `### Movies List`).

**"requests not found"** — Run `uv sync` from the project directory. This installs all dependencies into the managed venv.

**Covers not rendering in Obsidian** — Ensure the cover path in YAML matches the actual file location relative to vault root. The wikilink format `"[[media/covers/file.jpg]]"` should work in most Obsidian setups.

**Notes written but empty metadata** — This means Wikidata enrichment was skipped (low confidence or no match). Check `--verbose` output for scoring details. Adding a year hint to your input (e.g., `Redline (2009)`) dramatically improves match accuracy.

**"Failed to write note"** — Check filesystem permissions and available disk space in your vault directory.
