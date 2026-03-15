"""
tests/test_cleaner.py
Unit tests for pipeline/cleaner.py — no network calls, no external dependencies.
"""

import pytest
from pipeline.cleaner import (
    strip_html,
    normalize_whitespace,
    format_timestamp,
    clean_news_article,
    clean_filing_text,
    clean_xbrl_value,
)


# ── strip_html ────────────────────────────────────────────────────────────────

class TestStripHtml:
    def test_plain_text_unchanged(self):
        assert "hello world" in strip_html("hello world")

    def test_removes_p_tags(self):
        result = strip_html("<p>Hello world</p>")
        assert "<p>" not in result
        assert "Hello world" in result

    def test_removes_script_content(self):
        result = strip_html("<script>alert('xss')</script>Hello")
        assert "alert" not in result
        assert "Hello" in result

    def test_removes_style_content(self):
        result = strip_html("<style>body { color: red; }</style>Text")
        assert "color" not in result
        assert "Text" in result

    def test_br_becomes_newline(self):
        result = strip_html("Line one<br>Line two")
        assert "\n" in result

    def test_nested_tags(self):
        result = strip_html("<div><span><b>Bold text</b></span></div>")
        assert "<" not in result
        assert "Bold text" in result

    def test_empty_string(self):
        assert strip_html("") == ""

    def test_no_html(self):
        text = "Just plain text, no HTML."
        result = strip_html(text)
        assert "plain text" in result


# ── normalize_whitespace ──────────────────────────────────────────────────────

class TestNormalizeWhitespace:
    def test_collapses_multiple_spaces(self):
        assert normalize_whitespace("hello   world") == "hello world"

    def test_collapses_tabs(self):
        assert normalize_whitespace("hello\t\tworld") == "hello world"

    def test_three_or_more_newlines_become_two(self):
        result = normalize_whitespace("a\n\n\n\nb")
        assert result == "a\n\nb"

    def test_strips_leading_trailing(self):
        assert normalize_whitespace("  hello  ") == "hello"

    def test_empty_string(self):
        assert normalize_whitespace("") == ""

    def test_preserves_two_newlines(self):
        result = normalize_whitespace("para 1\n\npara 2")
        assert "para 1\n\npara 2" == result


# ── format_timestamp ──────────────────────────────────────────────────────────

class TestFormatTimestamp:
    def test_known_timestamp(self):
        # 2024-01-15 00:00:00 UTC
        ts = 1705276800
        result = format_timestamp(ts)
        assert result == "2024-01-15"

    def test_none_returns_today(self):
        result = format_timestamp(None)
        # just check it looks like a date
        assert len(result) == 10
        assert result[4] == "-" and result[7] == "-"

    def test_zero_returns_today(self):
        result = format_timestamp(0)
        # 0 is falsy → falls back to today
        assert len(result) == 10


# ── clean_news_article ────────────────────────────────────────────────────────

class TestCleanNewsArticle:
    def _make_article(self, headline="", summary="", source="Reuters",
                      url="http://example.com", datetime_ts=1705276800):
        return {
            "headline": headline,
            "summary": summary,
            "source": source,
            "url": url,
            "datetime": datetime_ts,
        }

    def test_returns_required_keys(self):
        article = self._make_article("Apple earnings beat", "AAPL reports record revenue")
        result = clean_news_article(article)
        assert set(result.keys()) == {"content", "title", "date", "source", "url"}

    def test_headline_and_summary_merged(self):
        article = self._make_article("Big headline", "Detail summary here")
        result = clean_news_article(article)
        assert "Big headline" in result["content"]
        assert "Detail summary here" in result["content"]

    def test_html_stripped_from_content(self):
        article = self._make_article("<b>Bold Title</b>", "<p>HTML summary</p>")
        result = clean_news_article(article)
        assert "<b>" not in result["content"]
        assert "<p>" not in result["content"]
        assert "Bold Title" in result["content"]
        assert "HTML summary" in result["content"]

    def test_date_formatted_correctly(self):
        article = self._make_article(datetime_ts=1705276800)
        result = clean_news_article(article)
        assert result["date"] == "2024-01-15"

    def test_source_preserved(self):
        article = self._make_article(source="Bloomberg")
        result = clean_news_article(article)
        assert result["source"] == "Bloomberg"

    def test_empty_summary(self):
        article = self._make_article(headline="Only headline", summary="")
        result = clean_news_article(article)
        assert "Only headline" in result["content"]

    def test_title_is_raw_headline(self):
        article = self._make_article(headline="Raw Headline <b>here</b>")
        result = clean_news_article(article)
        # title stays as original headline (for metadata)
        assert result["title"] == "Raw Headline <b>here</b>"


# ── clean_filing_text ─────────────────────────────────────────────────────────

class TestCleanFilingText:
    def test_strips_html(self):
        html = "<html><body><p>ITEM 1. BUSINESS</p><p>We operate stores.</p></body></html>"
        result = clean_filing_text(html)
        assert "<" not in result
        assert "ITEM 1. BUSINESS" in result
        assert "We operate stores" in result

    def test_removes_script_in_filing(self):
        html = "<script>var x = 1;</script><p>Risk factors.</p>"
        result = clean_filing_text(html)
        assert "var x" not in result
        assert "Risk factors" in result

    def test_plain_text_passthrough(self):
        text = "Annual report text with numbers 1000 and 2000."
        result = clean_filing_text(text)
        assert "Annual report" in result
        assert "1000" in result


# ── clean_xbrl_value ─────────────────────────────────────────────────────────

class TestCleanXbrlValue:
    def test_billions(self):
        assert clean_xbrl_value(1_234_567_890, "USD") == "$1.23B"

    def test_millions(self):
        assert clean_xbrl_value(23_400_000, "USD") == "$23.4M"

    def test_thousands(self):
        assert clean_xbrl_value(450_000, "USD") == "$450K"

    def test_small_value(self):
        assert clean_xbrl_value(999, "USD") == "$999"

    def test_negative_billions(self):
        result = clean_xbrl_value(-2_000_000_000, "USD")
        assert result == "-$2.00B"

    def test_non_usd_returns_raw(self):
        result = clean_xbrl_value(12345, "shares")
        assert result == "12345"

    def test_exactly_one_billion(self):
        result = clean_xbrl_value(1_000_000_000, "USD")
        assert result == "$1.00B"

    def test_exactly_one_million(self):
        result = clean_xbrl_value(1_000_000, "USD")
        assert result == "$1.0M"
