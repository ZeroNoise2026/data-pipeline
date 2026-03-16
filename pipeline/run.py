"""
pipeline/run.py
Main pipeline orchestration: fetch → clean → chunk → embed → store

Usage:
  # Full run for all tracked tickers (GitHub Actions cron)
  python -m pipeline.run

  # Single ticker debug
  python -m pipeline.run --ticker AAPL

  # Print only, no DB writes (local testing)
  python -m pipeline.run --dry-run

  # Combine both
  python -m pipeline.run --ticker AAPL --dry-run

Architecture principles (Scale-friendly):
  - Each ticker is processed independently; one failure doesn't affect others
  - Sleep 1s after each request to data-fetchers, respecting Finnhub 60 req/min limit
  - Embeddings are batched (100 chunks per request, reducing round trips)
  - DB upsert is idempotent, safe to re-run
  - --dry-run mode: all write operations only log, no actual Supabase writes
  - Future scaling path: hand process_ticker() to Celery workers for concurrency
"""

import argparse
import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Optional, Union

import requests

from pipeline.cleaner import (
    clean_news_article,
    clean_filing_text,
    clean_xbrl_value,
    format_timestamp,
)
from pipeline.chunker import chunk_news, chunk_filing, chunk_xbrl_facts
from pipeline.store import (
    make_document_id,
    upsert_documents,
    upsert_earnings,
    upsert_price_snapshot,
    get_tracked_tickers,
    update_ticker_timestamps,
)
from pipeline.config import DATA_FETCHERS_URL, EMBEDDING_SERVICE_URL, FILING_REFRESH_DAYS, EDGAR_FACTS_MAX_MB, EMBED_BATCH

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────


TICKER_SLEEP = 1.0       # pause between tickers (seconds), respecting Finnhub 60 req/min
REQUEST_TIMEOUT = 30     # HTTP request timeout (seconds)
NEWS_LIMIT = 50          # news items per fetch (avoid overly large single requests)


# ── HTTP utility functions ──────────────────────────────────────────────────────

def _get(path: str, params: Optional[dict] = None) -> Optional[Union[dict, list]]:
    """Send a GET request to data-fetchers, return JSON. Returns None on failure."""
    url = f"{DATA_FETCHERS_URL}{path}"
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        mb = len(r.content) / 1024 / 1024
        if mb > EDGAR_FACTS_MAX_MB:
            logger.warning(f"Response {mb:.1f}MB > {EDGAR_FACTS_MAX_MB}MB limit, skipping: {url}")
            return None
        return r.json()
    except requests.HTTPError as e:
        logger.warning(f"HTTP {e.response.status_code} — {url}: {e.response.text[:200]}")
        return None
    except Exception as e:
        logger.warning(f"Request failed — {url}: {e}")
        return None


def _embed_batch(texts: List[str]) -> List[Optional[List[float]]]:
    """Send a batch of texts to the embedding-service, return a list of vectors.

    Sends in batches (EMBED_BATCH chunks/request), returns vectors in the same order as input.
    Failed batches are filled with None placeholders (no zero vectors written;
    callers should filter out None entries).
    """
    all_vectors: List[Optional[List[float]]] = []
    url = f"{EMBEDDING_SERVICE_URL}/api/encode"

    for i in range(0, len(texts), EMBED_BATCH):
        batch = texts[i: i + EMBED_BATCH]
        try:
            r = requests.post(url, json={"texts": batch}, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            all_vectors.extend(r.json()["embeddings"])
        except Exception as e:
            logger.error(f"embed batch {i // EMBED_BATCH + 1} failed: {e}")
            # None placeholders: callers filter these out, avoiding wasted Supabase quota
            all_vectors.extend([None] * len(batch))

    return all_vectors


def _today_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _latest_xbrl_date(facts_us_gaap: dict) -> Optional[str]:
    """Find the latest 'end' date from the XBRL us-gaap dict.

    Used to tag XBRL document chunks with the accurate data date,
    rather than the pipeline run date.
    """
    latest = ""
    for metric_data in facts_us_gaap.values():
        for unit_list in metric_data.get("units", {}).values():
            for entry in unit_list:
                end = entry.get("end", "")
                if end > latest:
                    latest = end
    return latest or None


def _parse_utc(ts: str) -> datetime:
    """Convert Supabase timestamp string → timezone-aware UTC datetime.

    Supabase timestamps may lack timezone info (naive).
    We always attach UTC to avoid errors when subtracting from datetime.now(timezone.utc).
    """
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _fmp_doc_type(period: str) -> str:
    """Convert FMP period string → doc_type, matching schema-defined valid values.

    FMP annual report period="FY" → "10-K"
    FMP quarterly report period="Q1/Q2/Q3/Q4" → "10-Q"
    """
    return "10-K" if period == "FY" else "10-Q"

# ── Single Ticker Processing ────────────────────────────────────────────────────────────

def process_ticker(
    ticker: str,
    ticker_type: str,
    last_news_fetch: Optional[str] = None,    # ISO date, incremental news start point
    last_filing_fetch: Optional[str] = None,  # ISO date, used for FMP/EDGAR skip logic
    dry_run: bool = False,
    initial: bool = False,                    # True = first-time deep fetch, ignore all skip logic
) -> None:
    """Full processing of one ticker: fetch → clean → chunk → embed → store.

    ticker_type: "stock" or "etf"
      - stock: news + earnings + EDGAR facts + price snapshot
      - etf:   news + price snapshot (no earnings/filings)
    initial=True: called when adding a new ticker for the first time,
                  forces full fetch, ignores timestamp-based skip logic
    """
    logger.info(f"▶ {ticker} ({ticker_type}){' [INITIAL]' if initial else ''}")
    doc_rows: List[dict] = []
    today = _today_iso()

    # Whether to re-fetch FMP/EDGAR (skip if already fetched within 30 days, respecting FMP 250 req/day limit)
    needs_filing_refresh = (
        initial
        or last_filing_fetch is None
        or (datetime.now(timezone.utc) - _parse_utc(last_filing_fetch)).days >= FILING_REFRESH_DAYS
    )

    # ── 2. News (fetched for all ticker types) ───────────────────────────────────────
    # Incremental fetch: if last fetch time exists, only get new content, saving Finnhub 60 req/min quota
    news_params: dict = {"limit": NEWS_LIMIT}
    if not initial and last_news_fetch:
        news_params["from"] = last_news_fetch
    news_data = _get(f"/api/finnhub/news/{ticker}", news_params) or []
    for article in news_data:
        try:
            cleaned = clean_news_article(article)
            chunks = chunk_news(cleaned["content"])
            if not chunks:
                continue
            vectors = _embed_batch(chunks)
            for chunk, vec in zip(chunks, vectors):
                if vec is None:
                    continue  # embedding failed, skip
                doc_rows.append({
                    "id":       make_document_id(ticker, cleaned["date"], chunk),
                    "content":  chunk,
                    "embedding": vec,
                    "ticker":   ticker,
                    "date":     cleaned["date"],
                    "source":   cleaned["source"],
                    "doc_type": "news",
                    "title":    cleaned.get("title", ""),
                })
        except Exception as e:
            logger.warning(f"{ticker} news article error: {e}")
    logger.info(f"  news → {len(news_data)} articles → {sum(1 for r in doc_rows if r['doc_type']=='news')} chunks")

    # ── 3. Stock-specific data ────────────────────────────────────────────────────────
    if ticker_type == "stock":

        # 3a. Earnings summary → earnings table + documents table (for semantic search)
        earnings_data = _get(f"/api/finnhub/earnings/{ticker}") or []
        earnings_rows: List[dict] = []
        earnings_doc_texts: List[str] = []   # collect all texts first
        earnings_doc_meta:  List[dict] = []  # corresponding metadata
        for e in earnings_data:
            try:
                quarter = e.get("period", "")
                if not quarter:
                    continue
                eps     = e.get("actual")        or e.get("estimate")        or 0.0
                revenue = e.get("revenueActual") or e.get("revenueEstimate") or 0.0
                earnings_rows.append({
                    "ticker":     ticker,
                    "quarter":    quarter,
                    "date":       quarter,
                    "eps":        float(eps or 0),
                    "revenue":    int(float(revenue or 0)),  # BIGINT column cannot store floats
                    "net_income": None,
                    "guidance":   None,
                })
                text = (
                    f"{ticker} Q earnings for {quarter}: "
                    f"EPS {eps}, revenue {clean_xbrl_value(revenue, 'USD')}."
                )
                earnings_doc_texts.append(text)
                earnings_doc_meta.append({"quarter": quarter, "text": text})
            except Exception as exc:
                logger.warning(f"{ticker} earnings row error: {exc}")

        # Batch vectorize all at once, instead of sending a separate request per quarter
        if earnings_doc_texts:
            earnings_vecs = _embed_batch(earnings_doc_texts)
            for meta, vec in zip(earnings_doc_meta, earnings_vecs):
                if vec is None:
                    continue  # embedding failed, skip this entry
                doc_rows.append({
                    "id":        make_document_id(ticker, meta["quarter"], meta["text"]),
                    "content":   meta["text"],
                    "embedding": vec,
                    "ticker":    ticker,
                    "date":      meta["quarter"],
                    "source":    "finnhub",
                    "doc_type":  "earnings",
                })
        upsert_earnings(earnings_rows, dry_run=dry_run)
        logger.info(f"  earnings → {len(earnings_rows)} quarters")

        # 3b. EDGAR XBRL facts (same as FMP, skip if already fetched within 30 days)
        if not needs_filing_refresh:
            logger.info(f"  edgar facts → skipped (last fetch: {last_filing_fetch})")
        else:
            cik_data = _get(f"/api/edgar/cik/{ticker}")
            if cik_data and "cik" in cik_data:
                cik = cik_data["cik"]
                facts_data = _get(f"/api/edgar/facts/{cik}")
                if facts_data and "facts" in facts_data:
                    us_gaap = facts_data["facts"].get("us-gaap", {})
                    xbrl_chunks = chunk_xbrl_facts(ticker, us_gaap)
                    if xbrl_chunks:
                        vectors = _embed_batch(xbrl_chunks)
                        xbrl_date = _latest_xbrl_date(us_gaap) or today
                        for chunk, vec in zip(xbrl_chunks, vectors):
                            if vec is None:
                                continue
                            doc_rows.append({
                                "id":       make_document_id(ticker, xbrl_date, chunk),
                                "content":  chunk,
                                "embedding": vec,
                                "ticker":   ticker,
                                "date":     xbrl_date,
                                "source":   "edgar",
                                "doc_type": "10-K",  # schema: "news"|"10-Q"|"10-K"|"earnings"
                            })
                        logger.info(f"  edgar facts → {len(xbrl_chunks)} chunks")

        # 3c. FMP financial statements (skip if already fetched within 30 days, respecting FMP 250 req/day quota)
        if not needs_filing_refresh:
            logger.info(f"  fmp statements → skipped (last fetch: {last_filing_fetch})")
        else:
            for stmt_type in ("income-statement", "balance-sheet"):
                stmt = _get(f"/api/fmp/{stmt_type}/{ticker}", {"limit": 4}) or []
                for period in stmt:
                    try:
                        date_str = period.get("date") or today
                        fmp_doc_type = _fmp_doc_type(period.get("period", ""))
                        lines = [f"{ticker} {stmt_type} for period {date_str}:"]
                        for k, v in period.items():
                            if k in ("date", "symbol", "reportedCurrency", "cik",
                                     "fillingDate", "acceptedDate", "calendarYear",
                                     "period", "link", "finalLink"):
                                continue
                            # FMP occasionally returns string-typed numbers, normalize them
                            try:
                                num = float(v) if not isinstance(v, (int, float)) else v
                            except (TypeError, ValueError):
                                continue
                            if num != 0:
                                lines.append(f"  {k}: {clean_xbrl_value(num, 'USD')}")
                        text = "\n".join(lines)
                        chunks = chunk_filing(text)
                        vectors = _embed_batch(chunks)
                        for chunk, vec in zip(chunks, vectors):
                            if vec is None:
                                continue
                            doc_rows.append({
                                "id":       make_document_id(ticker, date_str, chunk),
                                "content":  chunk,
                                "embedding": vec,
                                "ticker":   ticker,
                                "date":     date_str,
                                "source":   "fmp",
                                "doc_type": fmp_doc_type,  # "10-K" or "10-Q"
                            })
                    except Exception as exc:
                        logger.warning(f"{ticker} {stmt_type} period error: {exc}")
            logger.info(f"  fmp statements → done")

    # ── 4. Price snapshot (all tickers) ───────────────────────────────────────────
    quote = _get(f"/api/fmp/quote/{ticker}")
    if quote:
        try:
            price_rows = [{
                "ticker":     ticker,
                "date":       today,
                "close_price": quote.get("price") or quote.get("previousClose"),
                "pe_ratio":   None,
                "market_cap": int(float(quote.get("marketCap") or 0)) or None,
            }]
            if ticker_type == "stock":
                # Get PE ratio (Finnhub basic financials)
                fin = _get(f"/api/finnhub/financials/{ticker}", {"metric": "all"})
                if fin and "metric" in fin:
                    price_rows[0]["pe_ratio"] = fin["metric"].get("peNormalizedAnnual")
                    # marketCap already obtained from FMP quote, don't overwrite (avoid unit discrepancies)
            upsert_price_snapshot(price_rows, dry_run=dry_run)
        except Exception as e:
            logger.warning(f"{ticker} price snapshot error: {e}")

    # ── 5. Write to documents table ──────────────────────────────────────────────────
    upsert_documents(doc_rows, dry_run=dry_run)
    logger.info(f"✔ {ticker}: {len(doc_rows)} document chunks written")
    # ── 6. Update tracked_tickers timestamps (for Person B to display data freshness) ────────
    update_ticker_timestamps(
        ticker,
        news_fetch=today,
        filing_fetch=today if needs_filing_refresh else None,
        dry_run=dry_run,
    )

# ── Main entry point ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Run QuantAgent data pipeline")
    parser.add_argument("--ticker", type=str, default=None,
                        help="Process a single ticker (uppercase, e.g. AAPL)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log all operations but skip actual Supabase writes")
    parser.add_argument("--initial", action="store_true",
                        help="First-time deep fetch: ignore skip logic, pull all history")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("🔥 DRY RUN mode — no data will be written to Supabase")

    if args.ticker:
        ticker = args.ticker.upper()
        # Determine type via profile: ETFs return {} (no name field)
        profile = _get(f"/api/finnhub/profile/{ticker}") or {}
        ticker_type = "stock" if profile.get("name") else "etf"
        logger.info(f"  type detected: {ticker_type} (profile name: {profile.get('name', 'N/A')})")
        process_ticker(
            ticker, ticker_type,
            dry_run=args.dry_run,
            initial=args.initial,
        )
    else:
        tickers = get_tracked_tickers()
        logger.info(f"Processing {len(tickers)} tracked tickers")
        failed: List[str] = []
        for row in tickers:
            try:
                ticker_type = row.get("ticker_type")
                if not ticker_type:
                    # Fallback: determine type via profile (when Supabase is not configured or field is empty)
                    profile = _get(f"/api/finnhub/profile/{row['ticker']}") or {}
                    ticker_type = "stock" if profile.get("name") else "etf"
                process_ticker(
                    row["ticker"],
                    ticker_type,
                    last_news_fetch=row.get("last_news_fetch"),
                    last_filing_fetch=row.get("last_filing_fetch"),
                    dry_run=args.dry_run,
                    initial=args.initial,
                )
            except Exception as e:
                logger.error(f"FAILED {row['ticker']}: {e}", exc_info=True)
                failed.append(row["ticker"])
            finally:
                time.sleep(TICKER_SLEEP)  # respect Finnhub 60 req/min

        if failed:
            logger.warning(f"{len(failed)}/{len(tickers)} tickers failed: {failed}")
        if len(failed) == len(tickers):
            raise SystemExit(1)  # all failed → CI shows red


if __name__ == "__main__":
    main()
