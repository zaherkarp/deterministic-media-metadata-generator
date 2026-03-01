# Media Library Generator + Metadata Enricher

A deterministic, script-driven tool for building an Obsidian media library from a markdown list. Parses, normalizes, deduplicates, and enriches media entries with Wikidata metadata — conservatively.

## Philosophy

**Conservative over comprehensive.** This tool will never insert metadata it isn't confident about. Missing data is always preferred over incorrect data. No hallucinated URLs, no placeholder images, no guessed years.

## Requirements

- Python 3.10+
- `requests` library (`pip install requests`)
- macOS, Linux, or Windows (cross-platform filenames)

## Quick Start

```bash
# Install dependency
pip install requests

# Dry run first (see what would be created, no files written)
python3 enrich_media.py --input my_media.md --vault ~/my-vault --dry-run

# Process only movies
python3 enrich_media.py --input my_media.md --vault ~/my-vault --only movies

# Full run, all types, zip output
python3 enrich_media.py --input my_media.md --vault ~/my-vault --zip

# Without enrichment (just normalize + generate stubs)
python3 enrich_media.py --input my_media.md --vault ~/my-vault --no-enrich
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

## Input Format

The input is a markdown file with section headers (`##`) for media types:

```markdown
## BOOKS
Wave in the Mind (LeGuin).md
Claim Your Inner Child (from Merija).md
1984

## SHOWS
The Mandalorian (from Anton).md
Heavenly delusion
Heavenly Delusion
Frieren- beyond journeys end.md

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
wikidata: Q1234567
---

# Redline
```

**Always-present keys:** `type`, `status`, `priority`, `year`, `genre`, `mood`
**Conditional keys:** `source`, `cover`, `cover_source`, `cover_confidence`, `wikidata`

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

## Recommended Workflow

Process one type at a time for easier review:

```bash
# Step 1: Movies first
python3 enrich_media.py -i media.md -v ~/vault --only movies --out-items media/items

# Step 2: Review the movie notes, fix any issues

# Step 3: Shows
python3 enrich_media.py -i media.md -v ~/vault --only shows --out-items media/items

# Step 4: Books
python3 enrich_media.py -i media.md -v ~/vault --only books --out-items media/items

# Step 5: Games
python3 enrich_media.py -i media.md -v ~/vault --only games --out-items media/items
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

## Troubleshooting

**"Rate limited by Wikidata (429)"** — Increase `--sleep` to 1.0 or higher. Wikidata's public SPARQL endpoint has rate limits.

**"No candidates" for most items** — Check that section headers match expected formats (`## MOVIES`, not `### Movies List`).

**"requests not found"** — Run `pip install requests` (or `pip3 install requests`).

**Covers not rendering in Obsidian** — Ensure the cover path in YAML matches the actual file location relative to vault root. The wikilink format `"[[media/covers/file.jpg]]"` should work in most Obsidian setups.
