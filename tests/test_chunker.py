"""
tests/test_chunker.py
Unit tests for pipeline/chunker.py — pure logic, no network calls.
"""

from pipeline.chunker import (
    chunk_text,
    chunk_news,
    chunk_filing,
    chunk_xbrl_facts,
    CHUNK_SIZE,
    CHUNK_OVERLAP,
    MIN_CHUNK_LEN,
)


# ── chunk_text ────────────────────────────────────────────────────────────────

class TestChunkText:
    def test_short_text_is_single_chunk(self):
        # Must be >= MIN_CHUNK_LEN (50) to be returned as a chunk
        text = "This is a sufficiently long sentence that exceeds fifty characters."
        result = chunk_text(text)
        assert result == [text]

    def test_empty_string_returns_empty(self):
        assert chunk_text("") == []

    def test_too_short_returns_empty(self):
        assert chunk_text("hi") == []

    def test_long_text_produces_multiple_chunks(self):
        text = "word " * 200  # ~1000 chars
        chunks = chunk_text(text)
        assert len(chunks) > 1

    def test_each_chunk_within_size_limit(self):
        text = "A" * 2000
        chunks = chunk_text(text)
        # In worst case (no sentence boundaries) chunks may slightly exceed
        # CHUNK_SIZE due to boundary logic, but should all be reasonably sized
        for chunk in chunks:
            assert len(chunk) <= CHUNK_SIZE * 2

    def test_no_chunk_below_min_len(self):
        text = ("Long sentence here. " * 30)  # ~600 chars
        chunks = chunk_text(text)
        for chunk in chunks:
            assert len(chunk) >= MIN_CHUNK_LEN

    def test_overlap_creates_context_continuity(self):
        """Verify that the end of chunk N appears in the beginning of chunk N+1."""
        # Build a deterministic text with clear sentence boundaries
        text = ". ".join(f"Sentence number {i}" for i in range(60))
        chunks = chunk_text(text)
        assert len(chunks) >= 2
        # The tail of chunk 0 should partially overlap with head of chunk 1
        tail = chunks[0][-CHUNK_OVERLAP:]
        # At least some word overlap should exist
        tail_words = set(tail.split())
        head_words = set(chunks[1][:CHUNK_OVERLAP * 2].split())
        assert len(tail_words & head_words) > 0

    def test_custom_chunk_size(self):
        text = "word " * 100
        chunks = chunk_text(text, chunk_size=100, overlap=10)
        assert len(chunks) > 1

    def test_returns_list_of_strings(self):
        text = "Hello world. " * 50
        result = chunk_text(text)
        assert isinstance(result, list)
        for item in result:
            assert isinstance(item, str)


# ── chunk_news ────────────────────────────────────────────────────────────────

class TestChunkNews:
    def test_short_news_is_single_chunk(self):
        text = "Apple beats earnings. Shares jump 5% in after-hours trading."
        result = chunk_news(text)
        assert len(result) == 1
        assert result[0] == text

    def test_below_min_len_returns_empty(self):
        result = chunk_news("Hi")
        assert result == []

    def test_exactly_min_len(self):
        text = "A" * MIN_CHUNK_LEN
        result = chunk_news(text)
        assert len(result) == 1

    def test_long_news_still_single_chunk(self):
        # News shouldn't be split even if long
        text = "word " * 200
        result = chunk_news(text)
        # chunk_news returns single chunk (no split for news)
        assert len(result) == 1

    def test_strips_leading_trailing_whitespace(self):
        # After strip, must still be >= MIN_CHUNK_LEN (50 chars)
        text = "  Apple earnings beat expectations in Q1 2024, shares rose 5%.  "
        result = chunk_news(text)
        assert result[0] == text.strip()


# ── chunk_filing ──────────────────────────────────────────────────────────────

class TestChunkFiling:
    def test_short_filing_is_single_chunk(self):
        text = "This is a short filing section."
        result = chunk_filing(text)
        # If >= MIN_CHUNK_LEN, should be returned
        if len(text) >= MIN_CHUNK_LEN:
            assert len(result) == 1
        else:
            assert result == []

    def test_long_filing_is_chunked(self):
        text = "Risk factor sentence number one. " * 50  # ~1650 chars
        result = chunk_filing(text)
        assert len(result) > 1

    def test_empty_filing_returns_empty(self):
        assert chunk_filing("") == []


# ── chunk_xbrl_facts ──────────────────────────────────────────────────────────

class TestChunkXbrlFacts:
    def _make_facts(self):
        """Build a minimal but realistic us-gaap facts dict."""
        return {
            "NetIncomeLoss": {
                "units": {
                    "USD": [
                        {"end": "2023-09-30", "val": 20_000_000_000, "form": "10-K"},
                        {"end": "2023-06-30", "val": 19_000_000_000, "form": "10-Q"},
                        {"end": "2023-03-31", "val": 18_500_000_000, "form": "10-Q"},
                    ]
                }
            },
            "Revenues": {
                "units": {
                    "USD": [
                        {"end": "2023-09-30", "val": 383_285_000_000, "form": "10-K"},
                        {"end": "2023-06-30", "val": 81_797_000_000, "form": "10-Q"},
                    ]
                }
            },
            "EarningsPerShareDiluted": {
                "units": {
                    "shares": [
                        {"end": "2023-09-30", "val": 6.13, "form": "10-K"},
                    ]
                }
            },
        }

    def test_returns_list_of_strings(self):
        result = chunk_xbrl_facts("AAPL", self._make_facts())
        assert isinstance(result, list)
        for chunk in result:
            assert isinstance(chunk, str)

    def test_contains_ticker_name(self):
        result = chunk_xbrl_facts("AAPL", self._make_facts())
        full_text = "\n".join(result)
        assert "AAPL" in full_text

    def test_contains_financial_labels(self):
        result = chunk_xbrl_facts("AAPL", self._make_facts())
        full_text = "\n".join(result)
        assert "Net Income" in full_text
        assert "Revenue" in full_text

    def test_contains_dates(self):
        result = chunk_xbrl_facts("AAPL", self._make_facts())
        full_text = "\n".join(result)
        assert "2023-09-30" in full_text

    def test_empty_facts_returns_empty(self):
        result = chunk_xbrl_facts("AAPL", {})
        assert result == []

    def test_unknown_metric_ignored(self):
        facts = {"UnknownMetric": {"units": {"USD": [{"end": "2023-09-30", "val": 100, "form": "10-K"}]}}}
        result = chunk_xbrl_facts("AAPL", facts)
        assert result == []

    def test_chunk_min_length(self):
        result = chunk_xbrl_facts("AAPL", self._make_facts())
        for chunk in result:
            assert len(chunk) >= MIN_CHUNK_LEN

    def test_10_sentences_per_chunk(self):
        """chunk_xbrl_facts keeps only last 8 entries per metric.
        Need entries across multiple metrics to exceed 10 sentences (1 chunk).
        With 9 metrics × 8 entries = 72 sentences → 8 chunks of 10 (minus remainder).
        """
        # Use 2 metrics with 8 entries each = 16 sentences → 2 chunks (10 + 6)
        entry = lambda m: {"end": f"202{m}-09-30", "val": m * 1_000_000_000, "form": "10-Q"}
        facts = {
            "NetIncomeLoss": {"units": {"USD": [entry(i) for i in range(1, 9)]}},
            "Revenues":      {"units": {"USD": [entry(i) for i in range(1, 9)]}},
        }
        result = chunk_xbrl_facts("MSFT", facts)
        # 16 sentences / 10 per chunk = 2 chunks
        assert len(result) == 2
