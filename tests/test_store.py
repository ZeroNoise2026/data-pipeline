"""
tests/test_store.py
Unit tests for pipeline/store.py — mocked Supabase, no real DB calls.
"""

import hashlib
import os
from unittest.mock import MagicMock, patch

import pytest


# ── make_document_id ──────────────────────────────────────────────────────────

class TestMakeDocumentId:
    def test_returns_16_char_hex(self):
        from pipeline.store import make_document_id
        doc_id = make_document_id("AAPL", "2024-01-15", "Apple beats earnings.")
        assert len(doc_id) == 16
        assert all(c in "0123456789abcdef" for c in doc_id)

    def test_deterministic(self):
        from pipeline.store import make_document_id
        id1 = make_document_id("AAPL", "2024-01-15", "Some content here.")
        id2 = make_document_id("AAPL", "2024-01-15", "Some content here.")
        assert id1 == id2

    def test_different_ticker_different_id(self):
        from pipeline.store import make_document_id
        id1 = make_document_id("AAPL", "2024-01-15", "Content")
        id2 = make_document_id("MSFT", "2024-01-15", "Content")
        assert id1 != id2

    def test_different_date_different_id(self):
        from pipeline.store import make_document_id
        id1 = make_document_id("AAPL", "2024-01-15", "Content")
        id2 = make_document_id("AAPL", "2024-01-16", "Content")
        assert id1 != id2

    def test_different_content_prefix_different_id(self):
        from pipeline.store import make_document_id
        id1 = make_document_id("AAPL", "2024-01-15", "Content A" + "x" * 200)
        id2 = make_document_id("AAPL", "2024-01-15", "Content B" + "x" * 200)
        assert id1 != id2

    def test_only_first_100_chars_of_content_matter(self):
        """IDs should be equal when content[:100] is the same."""
        from pipeline.store import make_document_id
        prefix = "A" * 100
        id1 = make_document_id("AAPL", "2024-01-15", prefix + "suffix_one")
        id2 = make_document_id("AAPL", "2024-01-15", prefix + "suffix_two")
        assert id1 == id2

    def test_matches_sha256_manually(self):
        from pipeline.store import make_document_id
        ticker, date, content = "NVDA", "2024-03-01", "GPU demand surge."
        expected_raw = f"{ticker}{date}{content[:100]}"
        expected = hashlib.sha256(expected_raw.encode()).hexdigest()[:16]
        assert make_document_id(ticker, date, content) == expected


# ── dry_run mode (SUPABASE_URL not set) ──────────────────────────────────────

class TestDryRunMode:
    """When SUPABASE_URL is empty, all writes should log and return count."""

    @patch.dict(os.environ, {"SUPABASE_URL": "", "SUPABASE_KEY": ""})
    def test_upsert_documents_dry_run(self):
        # Force reload so config picks up empty env var
        import importlib, pipeline.config, pipeline.store
        importlib.reload(pipeline.config)
        importlib.reload(pipeline.store)
        from pipeline.store import upsert_documents

        rows = [
            {"id": "abc123", "ticker": "AAPL", "doc_type": "news",
             "date": "2024-01-15", "content": "x", "embedding": [0.0] * 384,
             "source": "finnhub"},
        ]
        result = upsert_documents(rows, dry_run=True)
        assert result == 1

    @patch.dict(os.environ, {"SUPABASE_URL": "", "SUPABASE_KEY": ""})
    def test_upsert_earnings_dry_run(self):
        import importlib, pipeline.config, pipeline.store
        importlib.reload(pipeline.config)
        importlib.reload(pipeline.store)
        from pipeline.store import upsert_earnings

        rows = [{"ticker": "AAPL", "quarter": "2024Q1", "eps": 1.5,
                 "revenue": 100_000_000, "net_income": None, "guidance": None,
                 "date": "2024-01-15"}]
        result = upsert_earnings(rows, dry_run=True)
        assert result == 1

    @patch.dict(os.environ, {"SUPABASE_URL": "", "SUPABASE_KEY": ""})
    def test_upsert_price_snapshot_dry_run(self):
        import importlib, pipeline.config, pipeline.store
        importlib.reload(pipeline.config)
        importlib.reload(pipeline.store)
        from pipeline.store import upsert_price_snapshot

        rows = [{"ticker": "VOO", "date": "2024-01-15",
                 "close_price": 450.0, "pe_ratio": None, "market_cap": None}]
        result = upsert_price_snapshot(rows, dry_run=True)
        assert result == 1

    @patch.dict(os.environ, {"SUPABASE_URL": "", "SUPABASE_KEY": ""})
    def test_get_tracked_tickers_fallback(self):
        import importlib, pipeline.config, pipeline.store
        importlib.reload(pipeline.config)
        importlib.reload(pipeline.store)
        from pipeline.store import get_tracked_tickers

        result = get_tracked_tickers()
        assert isinstance(result, list)
        assert len(result) > 0
        for row in result:
            assert "ticker" in row
            assert "ticker_type" in row


# ── explicit dry_run=True flag ────────────────────────────────────────────────

class TestExplicitDryRun:
    """dry_run=True should always bypass Supabase even if URL is set."""

    def test_upsert_documents_explicit_dry_run(self):
        from pipeline.store import upsert_documents
        rows = [
            {"id": "test001", "ticker": "TSLA", "doc_type": "news",
             "date": "2024-01-20", "content": "Tesla news", "embedding": [],
             "source": "reuters"},
        ]
        # dry_run=True means no Supabase write even if URL were set
        result = upsert_documents(rows, dry_run=True)
        assert result == 1

    def test_upsert_earnings_explicit_dry_run(self):
        from pipeline.store import upsert_earnings
        rows = [{"ticker": "TSLA", "quarter": "2024Q1", "eps": 0.71,
                 "revenue": 21_000_000_000, "net_income": None,
                 "guidance": None, "date": "2024-03-31"}]
        result = upsert_earnings(rows, dry_run=True)
        assert result == 1

    def test_upsert_price_snapshot_explicit_dry_run(self):
        from pipeline.store import upsert_price_snapshot
        rows = [{"ticker": "TSLA", "date": "2024-01-20",
                 "close_price": 200.0, "pe_ratio": 50.0, "market_cap": 640e9}]
        result = upsert_price_snapshot(rows, dry_run=True)
        assert result == 1

    def test_empty_rows_returns_zero(self):
        from pipeline.store import upsert_documents
        result = upsert_documents([], dry_run=True)
        assert result == 0
