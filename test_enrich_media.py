#!/usr/bin/env python3
"""
test_enrich_media.py — Comprehensive test suite for enrich_media.py
====================================================================

Tests cover:
    - Input parsing (section headers, sub-headings, link format, edge cases)
    - Title normalization (year extraction, source notes, parenthetical stripping)
    - Deduplication and filename collision detection
    - Candidate scoring (exact, close, fuzzy, DeezyMatch signals)
    - Confidence gating (threshold, ambiguity, margin rules)
    - Cross-source validation logic
    - QA report generation
    - DeezyMatch training data generation
    - Note content generation (YAML frontmatter)
    - End-to-end dry-run pipeline

Run with:
    python -m pytest test_enrich_media.py -v
    python -m pytest test_enrich_media.py -v -k "test_parse"  # subset
"""

import os
import sys
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Import the module under test
import enrich_media as em


# ===========================================================================
# FIXTURES
# ===========================================================================

@pytest.fixture
def sample_input_file(tmp_path):
    """Create a sample input markdown file for parsing tests."""
    content = textwrap.dedent("""\
        ## BOOKS
        Wave in the Mind (LeGuin).md
        Claim Your Inner Child (from Merija).md
        Colorless Tsukuru.md
        Blood Meridian.md
        ### Software Engineering
        - [Clean Code: A Handbook of Agile Software Craftsmanship _(Robert C. Martin)_](__http://amzn.to/2iJgSbP__)
        - [Test Driven Development: By Example _(Kent Beck)_](__http://amzn.to/2iJcMAu__)
        ### Ruby
        - [Practical Object-Oriented Design in Ruby _(Sandy Metz)_](__http://amzn.to/2jzoq55__)
        ## SHOWS
        The Mandalorian (from Anton).md
        Frieren- beyond journeys end.md
        Severance, lol.md
        Assassination Classroom.md
        ## MOVIES
        Redline (2009).md
        Come and See (1985, Иди и смотри).md
        Living (2022) - Ikiru remake.md
        Inherit the Wind (1960).md
        Late Spring (1949).md
        ## GAMES
        Outer Wilds.md
        Sea  of Stars.md
    """)
    filepath = tmp_path / "test_input.md"
    filepath.write_text(content, encoding="utf-8")
    return str(filepath)


@pytest.fixture
def sample_entries(sample_input_file):
    """Parse sample input file and return raw entries."""
    return em.parse_input_file(sample_input_file)


@pytest.fixture
def normalized_entries(sample_entries):
    """Return normalized entries."""
    for e in sample_entries:
        em.extract_and_normalize(e)
    return sample_entries


@pytest.fixture
def mock_wikidata_binding():
    """Return a mock SPARQL binding for scoring tests."""
    return {
        "item": {"value": "http://www.wikidata.org/entity/Q123456"},
        "itemLabel": {"value": "Redline"},
        "date": {"value": "2009-10-09T00:00:00Z"},
        "image": {"value": "http://commons.wikimedia.org/wiki/Special:FilePath/Redline_poster.jpg"},
        "genreLabel": {"value": "science fiction"},
        "description": {"value": "2009 Japanese animated film"},
        "article": {"value": "https://en.wikipedia.org/wiki/Redline_(2009_film)"},
        "altLabel": {"value": "REDLINE"},
    }


# ===========================================================================
# SECTION 1: INPUT PARSING TESTS
# ===========================================================================

class TestParseInputFile:
    """Tests for parse_input_file()."""

    def test_parses_all_entries(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        assert len(entries) > 0

    def test_assigns_correct_types(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        types = {e["type"] for e in entries}
        assert types == {"book", "show", "movie", "game"}

    def test_book_count(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        books = [e for e in entries if e["type"] == "book"]
        # 4 plain + 2 software engineering + 1 ruby = 7
        assert len(books) == 7

    def test_movie_count(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        movies = [e for e in entries if e["type"] == "movie"]
        assert len(movies) == 5

    def test_show_count(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        shows = [e for e in entries if e["type"] == "show"]
        assert len(shows) == 4

    def test_game_count(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        games = [e for e in entries if e["type"] == "game"]
        assert len(games) == 2

    def test_only_filter(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file, only_type="movie")
        assert all(e["type"] == "movie" for e in entries)
        assert len(entries) == 5

    def test_subheading_topic_assigned(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        se_books = [e for e in entries if e.get("topic") == "Software Engineering"]
        assert len(se_books) == 2
        ruby_books = [e for e in entries if e.get("topic") == "Ruby"]
        assert len(ruby_books) == 1

    def test_no_topic_for_plain_entries(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        plain_books = [
            e for e in entries
            if e["type"] == "book" and e.get("topic") is None
        ]
        assert len(plain_books) == 4

    def test_topic_resets_on_new_section(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        shows = [e for e in entries if e["type"] == "show"]
        assert all(e.get("topic") is None for e in shows)

    def test_markdown_link_url_extracted(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        linked = [e for e in entries if e.get("url")]
        assert len(linked) == 3

    def test_markdown_link_url_stripped_underscores(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        linked = [e for e in entries if e.get("url")]
        for e in linked:
            assert not e["url"].startswith("__")
            assert not e["url"].endswith("__")

    def test_markdown_italic_stripped_from_link_titles(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        linked = [e for e in entries if e.get("url")]
        for e in linked:
            assert "_" not in e["raw_title"]

    def test_list_prefix_stripped(self, sample_input_file):
        entries = em.parse_input_file(sample_input_file)
        for e in entries:
            assert not e["raw_title"].startswith("- ")
            assert not e["raw_title"].startswith("* ")

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.md"
        f.write_text("", encoding="utf-8")
        entries = em.parse_input_file(str(f))
        assert entries == []

    def test_no_known_headers(self, tmp_path):
        f = tmp_path / "no_headers.md"
        f.write_text("## UNKNOWN\nSome item.md\n", encoding="utf-8")
        entries = em.parse_input_file(str(f))
        assert entries == []

    def test_wikilinks_stripped(self, tmp_path):
        f = tmp_path / "wikilinks.md"
        f.write_text("## MOVIES\n[[The Matrix]].md\n", encoding="utf-8")
        entries = em.parse_input_file(str(f))
        assert entries[0]["raw_title"] == "The Matrix.md"


# ===========================================================================
# SECTION 2: TITLE NORMALIZATION TESTS
# ===========================================================================

class TestExtractAndNormalize:
    """Tests for extract_and_normalize()."""

    def _make_entry(self, raw_title, media_type="movie"):
        return {"raw_title": raw_title, "type": media_type, "raw_line": raw_title, "line_num": 1}

    def test_strips_md_extension(self):
        e = self._make_entry("Redline.md")
        em.extract_and_normalize(e)
        assert e["clean_title"] == "Redline"

    def test_extracts_year_parenthetical(self):
        e = self._make_entry("Redline (2009).md")
        em.extract_and_normalize(e)
        assert e["year_hint"] == 2009
        assert "2009" not in e["clean_title"]

    def test_extracts_year_with_extra_info(self):
        e = self._make_entry("Come and See (1985, Иди и смотри).md")
        em.extract_and_normalize(e)
        assert e["year_hint"] == 1985

    def test_extracts_from_source(self):
        e = self._make_entry("The Mandalorian (from Anton).md", "show")
        em.extract_and_normalize(e)
        assert e["source"] == "from Anton"
        assert "from Anton" not in e["clean_title"]

    def test_strips_trailing_junk(self):
        e = self._make_entry("Severance, lol.md", "show")
        em.extract_and_normalize(e)
        assert "lol" not in e["clean_title"]

    def test_normalizes_separator(self):
        e = self._make_entry("Frieren- beyond journeys end.md", "show")
        em.extract_and_normalize(e)
        assert ":" in e["clean_title"]

    def test_generates_dedup_key(self):
        e = self._make_entry("Redline (2009).md")
        em.extract_and_normalize(e)
        assert e["dedup_key"].startswith("movie:")

    def test_generates_filename(self):
        e = self._make_entry("Redline (2009).md")
        em.extract_and_normalize(e)
        assert e["filename"] == "redline-2009"

    def test_filename_no_year(self):
        e = self._make_entry("The Mandalorian.md", "show")
        em.extract_and_normalize(e)
        assert e["filename"] == "the-mandalorian"

    def test_collapses_whitespace(self):
        e = self._make_entry("Sea  of Stars.md", "game")
        em.extract_and_normalize(e)
        assert "  " not in e["clean_title"]

    def test_strips_parenthetical_series(self):
        e = self._make_entry("Tekkonkinkreet (series).md", "show")
        em.extract_and_normalize(e)
        assert "(series)" not in e["clean_title"]

    def test_strips_year_film(self):
        e = self._make_entry("Silence (2016 film).md")
        em.extract_and_normalize(e)
        assert e["year_hint"] == 2016
        assert "film" not in e["clean_title"]

    def test_trailing_comma_year(self):
        e = self._make_entry("Little Murders, 1971.md")
        em.extract_and_normalize(e)
        assert e["year_hint"] == 1971

    def test_search_title_strips_all_parentheticals(self):
        e = self._make_entry(
            "Clean Code: A Handbook of Agile Software Craftsmanship (Robert C. Martin)",
            "book",
        )
        em.extract_and_normalize(e)
        assert "(Robert C. Martin)" not in e["search_title"]


# ===========================================================================
# SECTION 3: NORMALIZE FOR COMPARISON TESTS
# ===========================================================================

class TestNormalizeForComparison:
    """Tests for _normalize_for_comparison()."""

    def test_basic_normalization(self):
        assert em._normalize_for_comparison("Hello World") == "hello world"

    def test_strips_punctuation(self):
        assert em._normalize_for_comparison("Hello, World!") == "hello world"

    def test_handles_accents(self):
        result = em._normalize_for_comparison("Adèle Blanc-Sec")
        assert "e" in result  # è → e
        assert "blancsec" in result or "blanc sec" in result

    def test_collapses_whitespace(self):
        assert em._normalize_for_comparison("hello   world") == "hello world"

    def test_empty_string(self):
        assert em._normalize_for_comparison("") == ""


# ===========================================================================
# SECTION 4: CANDIDATE SCORING TESTS
# ===========================================================================

class TestScoreCandidate:
    """Tests for score_candidate()."""

    def test_exact_label_match(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "Redline", 2009)
        assert "exact_label" in result["score_breakdown"]
        assert result["score_breakdown"]["exact_label"] == em.CONFIDENCE["EXACT_LABEL_BONUS"]

    def test_close_label_match(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "redline", None)
        # "redline" vs "Redline" — exact (case-insensitive)
        assert "exact_label" in result["score_breakdown"]

    def test_year_match_bonus(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "Redline", 2009)
        assert "year_match" in result["score_breakdown"]

    def test_no_year_match_bonus_wrong_year(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "Redline", 2020)
        assert "year_match" not in result["score_breakdown"]

    def test_has_image_bonus(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "Redline", None)
        assert "has_image" in result["score_breakdown"]

    def test_fuzzy_penalty(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "Redline", 2009, is_fuzzy=True)
        assert "fuzzy_penalty" in result["score_breakdown"]
        assert result["score_breakdown"]["fuzzy_penalty"] < 0

    def test_extracts_qid(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "Redline", None)
        assert result["qid"] == "Q123456"

    def test_extracts_genres(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "Redline", None)
        assert "science fiction" in result["genres"]

    def test_extracts_description(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "Redline", None)
        assert result["description"] == "2009 Japanese animated film"

    def test_extracts_year(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "Redline", None)
        assert result["year"] == 2009

    def test_extracts_image_filename(self, mock_wikidata_binding):
        result = em.score_candidate(mock_wikidata_binding, "Redline", None)
        assert result["image_filename"] == "Redline_poster.jpg"

    def test_alt_label_match(self):
        binding = {
            "item": {"value": "http://www.wikidata.org/entity/Q999"},
            "itemLabel": {"value": "Some Different Name"},
            "altLabel": {"value": "Redline"},
        }
        result = em.score_candidate(binding, "Redline", None)
        assert "alt_label_match" in result["score_breakdown"]

    @pytest.mark.skipif(not em.HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
    def test_fuzzy_match_bonus_applied(self):
        """Test that rapidfuzz gives a bonus for near-matches."""
        binding = {
            "item": {"value": "http://www.wikidata.org/entity/Q555"},
            "itemLabel": {"value": "The Adventures of Augie March"},
            "altLabel": {"value": ""},
        }
        # Search title is slightly different
        result = em.score_candidate(binding, "Adventures of Augie March", None)
        assert result.get("fuzzy_score", 0) >= 85
        assert "fuzzy_match" in result["score_breakdown"]

    @pytest.mark.skipif(not em.HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
    def test_no_fuzzy_bonus_for_exact_match(self, mock_wikidata_binding):
        """If there's already an exact match, fuzzy bonus shouldn't apply."""
        result = em.score_candidate(mock_wikidata_binding, "Redline", 2009)
        assert "fuzzy_match" not in result["score_breakdown"]

    @pytest.mark.skipif(not em.HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
    def test_no_fuzzy_bonus_for_dissimilar(self):
        """Very different strings shouldn't get fuzzy bonus."""
        binding = {
            "item": {"value": "http://www.wikidata.org/entity/Q555"},
            "itemLabel": {"value": "Completely Different Title"},
            "altLabel": {"value": ""},
        }
        result = em.score_candidate(binding, "Blood Meridian", None)
        assert "fuzzy_match" not in result["score_breakdown"]


# ===========================================================================
# SECTION 5: MERGE CANDIDATES TESTS
# ===========================================================================

class TestMergeCandidates:
    """Tests for merge_candidates()."""

    def test_merges_genres(self, mock_wikidata_binding):
        binding2 = dict(mock_wikidata_binding)
        binding2["genreLabel"] = {"value": "action"}
        candidates = em.merge_candidates(
            [mock_wikidata_binding, binding2], "Redline", 2009
        )
        assert len(candidates) == 1
        assert "science fiction" in candidates[0]["genres"]
        assert "action" in candidates[0]["genres"]

    def test_deduplicates_by_qid(self):
        b1 = {
            "item": {"value": "http://www.wikidata.org/entity/Q111"},
            "itemLabel": {"value": "Foo"},
            "genreLabel": {"value": "drama"},
        }
        b2 = {
            "item": {"value": "http://www.wikidata.org/entity/Q222"},
            "itemLabel": {"value": "Bar"},
            "genreLabel": {"value": "comedy"},
        }
        candidates = em.merge_candidates([b1, b2], "Foo", None)
        assert len(candidates) == 2

    def test_sorted_by_score_descending(self, mock_wikidata_binding):
        b2 = {
            "item": {"value": "http://www.wikidata.org/entity/Q999"},
            "itemLabel": {"value": "Something Else"},
            "genreLabel": {"value": ""},
        }
        candidates = em.merge_candidates(
            [mock_wikidata_binding, b2], "Redline", 2009
        )
        assert candidates[0]["score"] >= candidates[-1]["score"]

    def test_empty_bindings(self):
        candidates = em.merge_candidates([], "Foo", None)
        assert candidates == []


# ===========================================================================
# SECTION 6: CONFIDENCE GATE TESTS
# ===========================================================================

class TestConfidenceGate:
    """Tests for confidence_gate()."""

    def test_returns_none_for_empty(self):
        assert em.confidence_gate([]) is None

    def test_accepts_high_score(self):
        candidates = [
            {"qid": "Q1", "label": "Foo", "score": 5, "score_breakdown": {}},
        ]
        result = em.confidence_gate(candidates)
        assert result is not None
        assert result["qid"] == "Q1"

    def test_rejects_below_threshold(self):
        candidates = [
            {"qid": "Q1", "label": "Foo", "score": 1, "score_breakdown": {}},
        ]
        result = em.confidence_gate(candidates)
        assert result is None

    def test_rejects_ambiguous(self):
        candidates = [
            {"qid": "Q1", "label": "Foo", "score": 3, "score_breakdown": {}},
            {"qid": "Q2", "label": "Bar", "score": 3, "score_breakdown": {}},
        ]
        result = em.confidence_gate(candidates)
        assert result is None

    def test_accepts_with_margin(self):
        candidates = [
            {"qid": "Q1", "label": "Foo", "score": 5, "score_breakdown": {}},
            {"qid": "Q2", "label": "Bar", "score": 2, "score_breakdown": {}},
        ]
        result = em.confidence_gate(candidates)
        assert result is not None
        assert result["qid"] == "Q1"


# ===========================================================================
# SECTION 7: DEDUPLICATION TESTS
# ===========================================================================

class TestDeduplication:
    """Tests for deduplicate_entries()."""

    def test_removes_exact_duplicates(self):
        entries = [
            {"dedup_key": "movie:redline", "clean_title": "Redline", "type": "movie",
             "enriched": False, "cover_filename": None, "filename": "redline",
             "raw_title": "Redline.md", "line_num": 1},
            {"dedup_key": "movie:redline", "clean_title": "Redline", "type": "movie",
             "enriched": False, "cover_filename": None, "filename": "redline",
             "raw_title": "Redline.md", "line_num": 2},
        ]
        result = em.deduplicate_entries(entries)
        assert len(result) == 1

    def test_keeps_different_types(self):
        entries = [
            {"dedup_key": "movie:safe", "clean_title": "Safe", "type": "movie",
             "enriched": False, "cover_filename": None, "filename": "safe",
             "raw_title": "Safe.md", "line_num": 1},
            {"dedup_key": "show:safe", "clean_title": "Safe", "type": "show",
             "enriched": False, "cover_filename": None, "filename": "safe",
             "raw_title": "Safe.md", "line_num": 2},
        ]
        result = em.deduplicate_entries(entries)
        assert len(result) == 2


# ===========================================================================
# SECTION 8: NOTE CONTENT GENERATION TESTS
# ===========================================================================

class TestGenerateNoteContent:
    """Tests for generate_note_content()."""

    def _make_enriched_entry(self, **overrides):
        base = {
            "type": "movie",
            "clean_title": "Redline",
            "year_hint": 2009,
            "enriched_year": 2009,
            "enriched_genres": ["science fiction", "action"],
            "source": None,
            "cover_filename": None,
            "cover_source": None,
            "cover_confidence": None,
            "description": "2009 Japanese animated film",
            "source_url": "https://en.wikipedia.org/wiki/Redline",
            "wikidata_qid": "Q123456",
            "streaming_providers": [],
            "open_library_url": None,
            "steam_url": None,
            "topic": None,
            "url": None,
        }
        base.update(overrides)
        return base

    def test_contains_yaml_frontmatter(self):
        e = self._make_enriched_entry()
        content = em.generate_note_content(e, "media/covers")
        assert content.startswith("---")
        assert content.count("---") >= 2

    def test_contains_type(self):
        e = self._make_enriched_entry()
        content = em.generate_note_content(e, "media/covers")
        assert "type: movie" in content

    def test_contains_year(self):
        e = self._make_enriched_entry()
        content = em.generate_note_content(e, "media/covers")
        assert "year: 2009" in content

    def test_contains_genres(self):
        e = self._make_enriched_entry()
        content = em.generate_note_content(e, "media/covers")
        assert "genre: [science fiction, action]" in content

    def test_contains_title_heading(self):
        e = self._make_enriched_entry()
        content = em.generate_note_content(e, "media/covers")
        assert "# Redline" in content

    def test_contains_topic_when_present(self):
        e = self._make_enriched_entry(topic="Software Engineering")
        content = em.generate_note_content(e, "media/covers")
        assert 'topic: "Software Engineering"' in content

    def test_no_topic_when_absent(self):
        e = self._make_enriched_entry()
        content = em.generate_note_content(e, "media/covers")
        assert "topic:" not in content

    def test_contains_url_when_present(self):
        e = self._make_enriched_entry(url="http://example.com")
        content = em.generate_note_content(e, "media/covers")
        assert 'url: "http://example.com"' in content

    def test_no_url_when_absent(self):
        e = self._make_enriched_entry()
        content = em.generate_note_content(e, "media/covers")
        # Check no standalone "url:" key (source_url is a different field)
        lines = content.split("\n")
        url_lines = [l for l in lines if l.startswith("url:")]
        assert len(url_lines) == 0

    def test_contains_source(self):
        e = self._make_enriched_entry(source="from Anton")
        content = em.generate_note_content(e, "media/covers")
        assert "source: from Anton" in content

    def test_description_quoted(self):
        e = self._make_enriched_entry()
        content = em.generate_note_content(e, "media/covers")
        assert 'description: "2009 Japanese animated film"' in content

    def test_cover_wikilink(self):
        e = self._make_enriched_entry(cover_filename="redline-2009.jpg")
        content = em.generate_note_content(e, "media/covers")
        assert 'cover: "[[media/covers/redline-2009.jpg]]"' in content

    def test_empty_year_when_unknown(self):
        e = self._make_enriched_entry(year_hint=None, enriched_year=None)
        content = em.generate_note_content(e, "media/covers")
        assert "year: \n" in content or "year: " in content


# ===========================================================================
# SECTION 9: FILENAME GENERATION TESTS
# ===========================================================================

class TestGenerateSafeFilename:
    """Tests for generate_safe_filename()."""

    def test_basic(self):
        assert em.generate_safe_filename("Redline", 2009) == "redline-2009"

    def test_colons_stripped(self):
        result = em.generate_safe_filename("Arendt: The Origins of Totalitarianism")
        assert ":" not in result

    def test_spaces_to_hyphens(self):
        assert "come-and-see" in em.generate_safe_filename("Come and See", 1985)

    def test_truncation(self):
        long_title = "A" * 300
        result = em.generate_safe_filename(long_title)
        assert len(result) <= 200

    def test_no_double_hyphens(self):
        result = em.generate_safe_filename("Hello -- World")
        assert "--" not in result


# ===========================================================================
# SECTION 10: QA REPORT TESTS
# ===========================================================================

class TestQAReport:
    """Tests for generate_qa_report()."""

    def test_generates_report_string(self):
        entries = [
            {
                "type": "movie", "clean_title": "Redline",
                "enriched": True, "match_score": 5,
                "match_breakdown": {"exact_label": 3, "year_match": 2},
                "wikidata_qid": "Q123456",
                "year_hint": 2009, "enriched_year": 2009,
                "cover_filename": "redline.jpg",
                "skip_reason": None,
            },
        ]
        report = em.generate_qa_report(entries)
        assert "QA REPORT" in report
        assert "OVERALL STATISTICS" in report

    def test_flags_low_confidence(self):
        entries = [
            {
                "type": "movie", "clean_title": "Safe",
                "enriched": True, "match_score": 2,
                "match_breakdown": {"close_label": 2},
                "wikidata_qid": "Q999",
                "year_hint": None, "enriched_year": None,
                "cover_filename": None,
                "skip_reason": None,
            },
        ]
        report = em.generate_qa_report(entries)
        assert "LOW CONFIDENCE" in report

    def test_flags_year_mismatch(self):
        entries = [
            {
                "type": "movie", "clean_title": "Test",
                "enriched": True, "match_score": 5,
                "match_breakdown": {},
                "wikidata_qid": "Q111",
                "year_hint": 2000, "enriched_year": 2010,
                "cover_filename": "test.jpg",
                "skip_reason": None,
            },
        ]
        report = em.generate_qa_report(entries)
        assert "YEAR MISMATCHES" in report

    def test_flags_missing_covers(self):
        entries = [
            {
                "type": "movie", "clean_title": "NoCover",
                "enriched": True, "match_score": 5,
                "match_breakdown": {},
                "wikidata_qid": "Q222",
                "year_hint": None, "enriched_year": None,
                "cover_filename": None,
                "skip_reason": None,
            },
        ]
        report = em.generate_qa_report(entries)
        assert "MISSING COVERS" in report

    def test_flags_duplicate_qids(self):
        entries = [
            {
                "type": "movie", "clean_title": "Foo",
                "enriched": True, "match_score": 5,
                "match_breakdown": {},
                "wikidata_qid": "Q111",
                "year_hint": None, "enriched_year": None,
                "cover_filename": None, "skip_reason": None,
            },
            {
                "type": "show", "clean_title": "Bar",
                "enriched": True, "match_score": 4,
                "match_breakdown": {},
                "wikidata_qid": "Q111",
                "year_hint": None, "enriched_year": None,
                "cover_filename": None, "skip_reason": None,
            },
        ]
        report = em.generate_qa_report(entries)
        assert "DUPLICATE WIKIDATA" in report

    def test_flags_failed_entries(self):
        entries = [
            {
                "type": "movie", "clean_title": "Unknown",
                "enriched": False, "skip_reason": "no candidates",
                "match_score": 0, "match_breakdown": {},
            },
        ]
        report = em.generate_qa_report(entries)
        assert "FAILED ENRICHMENTS" in report

    def test_writes_to_file(self, tmp_path):
        entries = [
            {
                "type": "movie", "clean_title": "Test",
                "enriched": True, "match_score": 5,
                "match_breakdown": {},
                "wikidata_qid": "Q123",
                "year_hint": None, "enriched_year": None,
                "cover_filename": None, "skip_reason": None,
            },
        ]
        out = str(tmp_path / "qa.txt")
        em.generate_qa_report(entries, output_path=out)
        assert Path(out).exists()
        assert "QA REPORT" in Path(out).read_text()

    @pytest.mark.skipif(not em.HAS_RAPIDFUZZ, reason="rapidfuzz not installed")
    def test_flags_fuzzy_only_matches(self):
        entries = [
            {
                "type": "movie", "clean_title": "FuzzyOnly",
                "enriched": True, "match_score": 3,
                "match_breakdown": {"fuzzy_match": 1, "year_match": 2},
                "wikidata_qid": "Q333",
                "wikidata_label": "FuzzyOnlyish",
                "fuzzy_score": 88.5,
                "year_hint": None, "enriched_year": None,
                "cover_filename": None, "skip_reason": None,
            },
        ]
        report = em.generate_qa_report(entries)
        assert "FUZZY-ONLY" in report


# ===========================================================================
# SECTION 11: DEEZYMATCH TRAINING DATA TESTS
# ===========================================================================

class TestDeezyMatchTrainingData:
    """Tests for generate_deezymatch_training_data()."""

    def test_generates_training_pairs(self, tmp_path):
        # Need >= 6 entries for negative pairs to be generated (threshold is > 5)
        entries = [
            {
                "enriched": True,
                "search_title": "Redline",
                "clean_title": "Redline",
                "wikidata_label": "Redline",
                "wikidata_alt_labels": ["REDLINE"],
            },
            {"enriched": False, "search_title": "Unknown Movie", "clean_title": "Unknown Movie"},
            {"enriched": False, "search_title": "Another Title", "clean_title": "Another Title"},
            {"enriched": False, "search_title": "Third Thing", "clean_title": "Third Thing"},
            {"enriched": False, "search_title": "Fourth Item", "clean_title": "Fourth Item"},
            {"enriched": False, "search_title": "Fifth Entry", "clean_title": "Fifth Entry"},
            {"enriched": False, "search_title": "Sixth One", "clean_title": "Sixth One"},
        ]
        out = str(tmp_path / "training.txt")
        n = em.generate_deezymatch_training_data(entries, out)
        assert n > 0
        content = Path(out).read_text()
        assert "TRUE" in content
        assert "FALSE" in content

    def test_tab_separated_format(self, tmp_path):
        entries = [
            {
                "enriched": True,
                "search_title": "Test Title",
                "clean_title": "Test Title",
                "wikidata_label": "Test Title",
                "wikidata_alt_labels": [],
            },
        ]
        out = str(tmp_path / "training.txt")
        em.generate_deezymatch_training_data(entries, out)
        for line in Path(out).read_text().strip().split("\n"):
            parts = line.split("\t")
            assert len(parts) == 3
            assert parts[2] in ("TRUE", "FALSE")


# ===========================================================================
# SECTION 12: DEEZYMATCH CONFIG GENERATION TESTS
# ===========================================================================

class TestDeezyMatchConfig:
    """Tests for generate_deezymatch_config()."""

    def test_generates_config_file(self, tmp_path):
        out = str(tmp_path / "config.yaml")
        em.generate_deezymatch_config(out)
        assert Path(out).exists()
        content = Path(out).read_text()
        assert "GRU" in content or "gru" in content
        assert "train" in content


# ===========================================================================
# SECTION 13: CROSS-SOURCE VALIDATION TESTS
# ===========================================================================

class TestCrossValidation:
    """Tests for cross_validate_match()."""

    def test_returns_false_no_ids(self):
        entry = {"type": "movie", "search_title": "Test", "clean_title": "Test"}
        winner = {"label": "Test"}
        session = MagicMock()
        result = em.cross_validate_match(entry, winner, session)
        assert result is False

    def test_tmdb_validation_success(self):
        entry = {"type": "movie", "search_title": "Redline", "clean_title": "Redline"}
        winner = {"label": "Redline", "tmdb_movie_id": "12345"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"title": "Redline"}

        session = MagicMock()
        session.get.return_value = mock_response

        result = em.cross_validate_match(
            entry, winner, session, tmdb_key="test_key"
        )
        assert result is True

    def test_tmdb_validation_mismatch(self):
        entry = {"type": "movie", "search_title": "Redline", "clean_title": "Redline"}
        winner = {"label": "Redline", "tmdb_movie_id": "12345"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"title": "Something Completely Different"}

        session = MagicMock()
        session.get.return_value = mock_response

        result = em.cross_validate_match(
            entry, winner, session, tmdb_key="test_key"
        )
        assert result is False

    def test_openlibrary_validation_success(self):
        entry = {"type": "book", "search_title": "Clean Code", "clean_title": "Clean Code"}
        winner = {"label": "Clean Code", "open_library_id": "OL123W"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"title": "Clean Code"}

        session = MagicMock()
        session.get.return_value = mock_response

        result = em.cross_validate_match(entry, winner, session)
        assert result is True

    def test_handles_api_error_gracefully(self):
        entry = {"type": "movie", "search_title": "Test", "clean_title": "Test"}
        winner = {"label": "Test", "tmdb_movie_id": "999"}

        session = MagicMock()
        session.get.side_effect = Exception("Network error")

        result = em.cross_validate_match(
            entry, winner, session, tmdb_key="test_key"
        )
        assert result is False


# ===========================================================================
# SECTION 14: FALLBACK TITLE GENERATION TESTS
# ===========================================================================

class TestGenerateFallbackTitles:
    """Tests for _generate_fallback_titles()."""

    def test_strips_leading_the(self):
        variants = em._generate_fallback_titles("The Mandalorian")
        assert "Mandalorian" in variants

    def test_strips_subtitle(self):
        variants = em._generate_fallback_titles("Clean Code: A Handbook")
        assert "Clean Code" in variants

    def test_tries_subtitle_alone(self):
        variants = em._generate_fallback_titles("Arendt: The Origins of Totalitarianism")
        assert "The Origins of Totalitarianism" in variants

    def test_shortens_long_titles(self):
        variants = em._generate_fallback_titles(
            "The Official Stardew Valley Cookbook ConcernedApe Ryan Novak"
        )
        # Should have shortened versions
        short = [v for v in variants if len(v.split()) <= 4]
        assert len(short) > 0

    def test_empty_for_short_simple_title(self):
        variants = em._generate_fallback_titles("Redline")
        # "Redline" is short and has no "The", no colon — limited variants
        assert "Redline" not in variants  # shouldn't include the original


# ===========================================================================
# SECTION 15: CONFIDENCE CONSTANT TESTS
# ===========================================================================

class TestConfidenceConstants:
    """Verify confidence constants are sane."""

    def test_threshold_positive(self):
        assert em.CONFIDENCE["ACCEPT_THRESHOLD"] > 0

    def test_exact_label_beats_threshold(self):
        assert em.CONFIDENCE["EXACT_LABEL_BONUS"] >= em.CONFIDENCE["ACCEPT_THRESHOLD"]

    def test_margin_positive(self):
        assert em.CONFIDENCE["MIN_MARGIN"] > 0

    def test_fuzzy_threshold_reasonable(self):
        assert 50 <= em.CONFIDENCE["FUZZY_MATCH_THRESHOLD"] <= 100

    def test_fuzzy_bonus_less_than_exact(self):
        assert em.CONFIDENCE["FUZZY_MATCH_BONUS"] < em.CONFIDENCE["EXACT_LABEL_BONUS"]

    def test_deezymatch_bonus_positive(self):
        assert em.CONFIDENCE["DEEZYMATCH_BONUS"] > 0

    def test_cross_source_bonus_positive(self):
        assert em.CONFIDENCE["CROSS_SOURCE_BONUS"] > 0


# ===========================================================================
# SECTION 16: END-TO-END DRY-RUN TEST
# ===========================================================================

class TestEndToEnd:
    """End-to-end test with --dry-run --no-enrich."""

    def test_dry_run_no_enrich(self, sample_input_file, tmp_path):
        """Full pipeline should complete without errors."""
        import subprocess
        result = subprocess.run(
            [
                sys.executable, "enrich_media.py",
                "--input", sample_input_file,
                "--vault", str(tmp_path),
                "--dry-run", "--no-enrich",
            ],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0
        assert "SUMMARY" in result.stdout
        assert "Total entries parsed:" in result.stdout

    def test_qa_report_with_no_enrich(self, sample_input_file, tmp_path):
        """QA report generation should not crash even with --no-enrich."""
        import subprocess
        qa_path = str(tmp_path / "qa_report.txt")
        result = subprocess.run(
            [
                sys.executable, "enrich_media.py",
                "--input", sample_input_file,
                "--vault", str(tmp_path),
                "--dry-run", "--no-enrich",
                "--qa-report", qa_path,
            ],
            capture_output=True, text=True, timeout=30,
        )
        # --qa-report with --no-enrich should not crash (report skipped)
        assert result.returncode == 0


# ===========================================================================
# SECTION 17: RAPIDFUZZ AVAILABILITY TESTS
# ===========================================================================

class TestRapidFuzzAvailability:
    """Tests that verify behavior with/without rapidfuzz."""

    def test_has_rapidfuzz_flag_consistent(self):
        """HAS_RAPIDFUZZ should match importability."""
        try:
            from rapidfuzz import fuzz
            assert em.HAS_RAPIDFUZZ is True
        except ImportError:
            assert em.HAS_RAPIDFUZZ is False

    def test_scoring_works_without_rapidfuzz(self, mock_wikidata_binding):
        """Scoring should work fine even without rapidfuzz."""
        with patch.object(em, 'HAS_RAPIDFUZZ', False):
            result = em.score_candidate(mock_wikidata_binding, "Redline", 2009)
            assert result["score"] > 0
            assert "fuzzy_match" not in result["score_breakdown"]


# ===========================================================================
# RUN
# ===========================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
