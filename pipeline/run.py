"""
pipeline/run.py
主流程编排：fetch → clean → chunk → embed → store

用法:
  # 全量跑所有 tracked tickers（GitHub Actions cron）
  python -m pipeline.run

  # 单 ticker 调试
  python -m pipeline.run --ticker AAPL

  # 只打印不写库（本地测试）
  python -m pipeline.run --dry-run

  # 两者结合
  python -m pipeline.run --ticker AAPL --dry-run

架构原则（Scale 友好）:
  - 每个 ticker 独立处理，一个出错不影响其它
  - 对 data-fetchers 每次请求后 sleep 1s，守住 Finnhub 60 req/min 限制
  - embedding 批量打包发出（单次请求 100 块，减少往返次数）
  - 写库 upsert 幂等，重跑安全
  - --dry-run 模式：所有写操作只打日志，不真正写 Supabase
  - 未来扩容路径：把 process_ticker() 交给 Celery worker 即可并发
"""

import argparse
import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

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
)
from pipeline.config import DATA_FETCHERS_URL, EMBEDDING_SERVICE_URL

logger = logging.getLogger(__name__)

# ── 常量 ─────────────────────────────────────────────────────────────────────

EMBED_BATCH = 100        # 单次发给 embedding-service 的最大块数
TICKER_SLEEP = 1.0       # ticker 之间停顿（秒），守住 Finnhub 60 req/min
REQUEST_TIMEOUT = 30     # HTTP 请求超时（秒）
NEWS_LIMIT = 50          # 每次抓新闻条数（避免一次请求太大）


# ── HTTP 工具函数 ──────────────────────────────────────────────────────────────

def _get(path: str, params: Optional[dict] = None) -> dict | list:
    """向 data-fetchers 发 GET 请求，返回 JSON。失败时返回 None。"""
    url = f"{DATA_FETCHERS_URL}{path}"
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        logger.warning(f"HTTP {e.response.status_code} — {url}: {e.response.text[:200]}")
        return None
    except Exception as e:
        logger.warning(f"Request failed — {url}: {e}")
        return None


def _embed_batch(texts: List[str]) -> List[List[float]]:
    """向 embedding-service 发送一批文本，返回向量列表。

    分批发送（EMBED_BATCH 块/次），服务器返回顺序与输入一一对应。
    """
    all_vectors: List[List[float]] = []
    url = f"{EMBEDDING_SERVICE_URL}/api/encode"

    for i in range(0, len(texts), EMBED_BATCH):
        batch = texts[i: i + EMBED_BATCH]
        try:
            r = requests.post(url, json={"texts": batch}, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            all_vectors.extend(r.json()["embeddings"])
        except Exception as e:
            logger.error(f"embed batch {i // EMBED_BATCH + 1} failed: {e}")
            # 失败批次用全零向量占位（不跳过，保持索引对齐）
            all_vectors.extend([[0.0] * 384] * len(batch))

    return all_vectors


def _today_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ── 单 Ticker 处理 ─────────────────────────────────────────────────────────────

def process_ticker(ticker: str, ticker_type: str, dry_run: bool = False) -> None:
    """完整处理一个 ticker：fetch → clean → chunk → embed → store。

    ticker_type: "stock" 或 "etf"
      - stock: 新闻 + 盈利 + EDGAR facts + 价格快照
      - etf:   新闻 + 价格快照（无盈利/财报）
    """
    logger.info(f"▶ {ticker} ({ticker_type})")
    doc_rows: List[dict] = []
    today = _today_iso()

    # ── 1. 获取 ticker 类型（尊重传入值，减少 API 调用）──────────────────────
    # ticker_type 已由 get_tracked_tickers() 或 CLI 提供，直接使用

    # ── 2. 新闻（所有 ticker 类型都抓）───────────────────────────────────────
    news_data = _get(f"/api/finnhub/news/{ticker}", {"limit": NEWS_LIMIT}) or []
    for article in news_data:
        try:
            cleaned = clean_news_article(article)
            chunks = chunk_news(cleaned["content"])
            if not chunks:
                continue
            vectors = _embed_batch(chunks)
            for chunk, vec in zip(chunks, vectors):
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

    # ── 3. 股票专属数据 ────────────────────────────────────────────────────────
    if ticker_type == "stock":

        # 3a. 盈利摘要 → earnings 表 + documents 表（语义搜索用）
        earnings_data = _get(f"/api/finnhub/earnings/{ticker}") or []
        earnings_rows: List[dict] = []
        for e in earnings_data:
            try:
                quarter = e.get("period", "")
                if not quarter:
                    continue
                eps = e.get("actual") or e.get("estimate") or 0.0
                revenue = e.get("revenueActual") or e.get("revenueEstimate") or 0.0
                row = {
                    "ticker":     ticker,
                    "quarter":    quarter,
                    "date":       quarter,
                    "eps":        eps,
                    "revenue":    revenue,
                    "net_income": None,
                    "guidance":   None,
                }
                earnings_rows.append(row)
                # 也生成文档条目供语义搜索
                text = (
                    f"{ticker} Q earnings for {quarter}: "
                    f"EPS {eps}, revenue {clean_xbrl_value(revenue, 'USD')}."
                )
                vec = _embed_batch([text])[0]
                doc_rows.append({
                    "id":       make_document_id(ticker, quarter, text),
                    "content":  text,
                    "embedding": vec,
                    "ticker":   ticker,
                    "date":     quarter,
                    "source":   "finnhub",
                    "doc_type": "earnings",
                })
            except Exception as exc:
                logger.warning(f"{ticker} earnings row error: {exc}")
        upsert_earnings(earnings_rows, dry_run=dry_run)
        logger.info(f"  earnings → {len(earnings_rows)} quarters")

        # 3b. EDGAR XBRL facts → documents 表
        cik_data = _get(f"/api/edgar/cik/{ticker}")
        if cik_data and "cik" in cik_data:
            cik = cik_data["cik"]
            facts_data = _get(f"/api/edgar/facts/{cik}")
            if facts_data and "facts" in facts_data:
                us_gaap = facts_data["facts"].get("us-gaap", {})
                xbrl_chunks = chunk_xbrl_facts(ticker, us_gaap)
                if xbrl_chunks:
                    vectors = _embed_batch(xbrl_chunks)
                    year = today[:4]
                    for chunk, vec in zip(xbrl_chunks, vectors):
                        doc_rows.append({
                            "id":       make_document_id(ticker, year, chunk),
                            "content":  chunk,
                            "embedding": vec,
                            "ticker":   ticker,
                            "date":     today,
                            "source":   "edgar",
                            "doc_type": "xbrl_facts",
                        })
                    logger.info(f"  edgar facts → {len(xbrl_chunks)} chunks")

        # 3c. FMP 财务报表 → documents 表（income statement 最近 4 quarters）
        for stmt_type in ("income-statement", "balance-sheet"):
            stmt = _get(f"/api/fmp/{stmt_type}/{ticker}", {"limit": 4}) or []
            for period in stmt:
                try:
                    date_str = period.get("date") or today
                    lines = [
                        f"{ticker} {stmt_type} for period {date_str}:",
                    ]
                    for k, v in period.items():
                        if k in ("date", "symbol", "reportedCurrency", "cik",
                                 "fillingDate", "acceptedDate", "calendarYear",
                                 "period", "link", "finalLink"):
                            continue
                        if isinstance(v, (int, float)) and v != 0:
                            lines.append(f"  {k}: {clean_xbrl_value(v, 'USD')}")
                    text = "\n".join(lines)
                    chunks = chunk_filing(text)
                    vectors = _embed_batch(chunks)
                    for chunk, vec in zip(chunks, vectors):
                        doc_rows.append({
                            "id":       make_document_id(ticker, date_str, chunk),
                            "content":  chunk,
                            "embedding": vec,
                            "ticker":   ticker,
                            "date":     date_str,
                            "source":   "fmp",
                            "doc_type": stmt_type,
                        })
                except Exception as exc:
                    logger.warning(f"{ticker} {stmt_type} period error: {exc}")
        logger.info(f"  fmp statements → done")

    # ── 4. 价格快照（所有 ticker）───────────────────────────────────────────
    quote = _get(f"/api/finnhub/quote/{ticker}")
    if quote:
        try:
            price_rows = [{
                "ticker":     ticker,
                "date":       today,
                "close_price": quote.get("c") or quote.get("pc"),
                "pe_ratio":   None,
                "market_cap": None,
            }]
            if ticker_type == "stock":
                # 获取 PE ratio（Finnhub basic financials）
                fin = _get(f"/api/finnhub/financials/{ticker}", {"metric": "all"})
                if fin and "metric" in fin:
                    price_rows[0]["pe_ratio"] = fin["metric"].get("peNormalizedAnnual")
                    price_rows[0]["market_cap"] = fin["metric"].get("marketCapitalization")
            upsert_price_snapshot(price_rows, dry_run=dry_run)
        except Exception as e:
            logger.warning(f"{ticker} price snapshot error: {e}")

    # ── 5. 写入 documents 表 ──────────────────────────────────────────────────
    upsert_documents(doc_rows, dry_run=dry_run)
    logger.info(f"✔ {ticker}: {len(doc_rows)} document chunks written")


# ── 主入口 ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Run QuantAgent data pipeline")
    parser.add_argument("--ticker", type=str, default=None,
                        help="Process a single ticker (uppercase, e.g. AAPL)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log all operations but skip actual Supabase writes")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("🔥 DRY RUN mode — no data will be written to Supabase")

    if args.ticker:
        ticker = args.ticker.upper()
        # 对单 ticker 动态判断类型
        type_data = _get(f"/api/finnhub/type/{ticker}")
        if type_data is None:
            logger.error(f"Cannot determine type for {ticker}, aborting")
            return
        ticker_type = type_data.get("ticker_type", "stock")
        process_ticker(ticker, ticker_type, dry_run=args.dry_run)
    else:
        tickers = get_tracked_tickers()
        logger.info(f"Processing {len(tickers)} tracked tickers")
        for row in tickers:
            try:
                process_ticker(
                    row["ticker"],
                    row.get("ticker_type", "stock"),
                    dry_run=args.dry_run,
                )
            except Exception as e:
                logger.error(f"FAILED {row['ticker']}: {e}", exc_info=True)
            finally:
                time.sleep(TICKER_SLEEP)  # 守住 Finnhub 60 req/min


if __name__ == "__main__":
    main()
