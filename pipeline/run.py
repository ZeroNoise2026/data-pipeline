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

# ── 常量 ─────────────────────────────────────────────────────────────────────


TICKER_SLEEP = 1.0       # ticker 之间停顿（秒），守住 Finnhub 60 req/min
REQUEST_TIMEOUT = 30     # HTTP 请求超时（秒）
NEWS_LIMIT = 50          # 每次抓新闻条数（避免一次请求太大）


# ── HTTP 工具函数 ──────────────────────────────────────────────────────────────

def _get(path: str, params: Optional[dict] = None) -> Optional[Union[dict, list]]:
    """向 data-fetchers 发 GET 请求，返回 JSON。失败时返回 None。"""
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
    """向 embedding-service 发送一批文本，返回向量列表。

    分批发送（EMBED_BATCH 块/次），返回顺序与输入一一对应。
    失败的批次用 None 占位（不写入零向量，调用方应过滤掉 None 条目）。
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
            # None 占位：调用方过滤后不写入库，不浪费 Supabase 配额
            all_vectors.extend([None] * len(batch))

    return all_vectors


def _today_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _latest_xbrl_date(facts_us_gaap: dict) -> Optional[str]:
    """从 XBRL us-gaap dict 中找出最新的 'end' 日期。

    用来给 XBRL 文档块打上准确的数据日期，而非流水线运行日期。
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
    """Supabase timestamp 字符串 → timezone-aware UTC datetime。

    Supabase 返回的时间戳可能不含时区信息（naive），
    统一补上 UTC，避免与 datetime.now(timezone.utc) 相减时报错。
    """
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _fmp_doc_type(period: str) -> str:
    """FMP period 字符串 → doc_type，匹配 schema 约定的合法候选值。

    FMP 年报 period="FY" → "10-K"
    FMP 季报 period="Q1/Q2/Q3/Q4" → "10-Q"
    """
    return "10-K" if period == "FY" else "10-Q"

# ── 单 Ticker 处理 ────────────────────────────────────────────────────────────

def process_ticker(
    ticker: str,
    ticker_type: str,
    last_news_fetch: Optional[str] = None,    # ISO date，增量新闻起点
    last_filing_fetch: Optional[str] = None,  # ISO date，跳过 FMP/EDGAR 判断
    dry_run: bool = False,
    initial: bool = False,                    # True = 首次深度抓取，忽略所有跳过逻辑
) -> None:
    """完整处理一个 ticker：fetch → clean → chunk → embed → store。

    ticker_type: "stock" 或 "etf"
      - stock: 新闻 + 盈利 + EDGAR facts + 价格快照
      - etf:   新闻 + 价格快照（无盈利/财报）
    initial=True: 首次添加新 ticker 时调用，强制全量抓取，忽略时间戳跳过逻辑
    """
    logger.info(f"▶ {ticker} ({ticker_type}){' [INITIAL]' if initial else ''}")
    doc_rows: List[dict] = []
    today = _today_iso()

    # 是否需要重新抓取 FMP/EDGAR（30天内已抓过则跳过，守住 FMP 250 req/day）
    needs_filing_refresh = (
        initial
        or last_filing_fetch is None
        or (datetime.now(timezone.utc) - _parse_utc(last_filing_fetch)).days >= FILING_REFRESH_DAYS
    )

    # ── 2. 新闻（所有 ticker 类型都抓）───────────────────────────────────────
    # 增量抓取：有上次抓取时间则只取新内容，节省 Finnhub 60 req/min 配额
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
                    continue  # 向量化失败，跳过
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
        earnings_doc_texts: List[str] = []   # 先收集所有文本
        earnings_doc_meta:  List[dict] = []  # 对应元数据
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
                    "revenue":    int(float(revenue or 0)),  # BIGINT 列不能存浮点
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

        # 一次批量向量化，不再每个季度单独发请求
        if earnings_doc_texts:
            earnings_vecs = _embed_batch(earnings_doc_texts)
            for meta, vec in zip(earnings_doc_meta, earnings_vecs):
                if vec is None:
                    continue  # 向量化失败，跳过该条目
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

        # 3b. EDGAR XBRL facts（同 FMP，30天内已抓过则跳过）
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

        # 3c. FMP 财务报表（30天内已抓过则跳过，守住 FMP 250 req/day 配额）
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
                            # FMP 偶尔返回字符串型数字，统一转换
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
                                "doc_type": fmp_doc_type,  # "10-K" 或 "10-Q"
                            })
                    except Exception as exc:
                        logger.warning(f"{ticker} {stmt_type} period error: {exc}")
            logger.info(f"  fmp statements → done")

    # ── 4. 价格快照（所有 ticker）───────────────────────────────────────────
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
                # 获取 PE ratio（Finnhub basic financials）
                fin = _get(f"/api/finnhub/financials/{ticker}", {"metric": "all"})
                if fin and "metric" in fin:
                    price_rows[0]["pe_ratio"] = fin["metric"].get("peNormalizedAnnual")
                    # marketCap 已从 FMP quote 获取，不再覆盖（避免单位差异）
            upsert_price_snapshot(price_rows, dry_run=dry_run)
        except Exception as e:
            logger.warning(f"{ticker} price snapshot error: {e}")

    # ── 5. 写入 documents 表 ──────────────────────────────────────────────────
    upsert_documents(doc_rows, dry_run=dry_run)
    logger.info(f"✔ {ticker}: {len(doc_rows)} document chunks written")
    # ── 6. 更新 tracked_tickers 时间戳（供 Person B 显示数据新鲜度）────────
    update_ticker_timestamps(
        ticker,
        news_fetch=today,
        filing_fetch=today if needs_filing_refresh else None,
        dry_run=dry_run,
    )

# ── 主入口 ─────────────────────────────────────────────────────────────────────

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
        # 用 profile 判断类型：ETF 的 profile 返回 {}（无 name 字段）
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
                    # fallback：用 profile 判断（Supabase 未配置或字段为空时）
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
                time.sleep(TICKER_SLEEP)  # 守住 Finnhub 60 req/min

        if failed:
            logger.warning(f"{len(failed)}/{len(tickers)} tickers failed: {failed}")
        if len(failed) == len(tickers):
            raise SystemExit(1)  # 全部失败 → CI 显示红色


if __name__ == "__main__":
    main()
