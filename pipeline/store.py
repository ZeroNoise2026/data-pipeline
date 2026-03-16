"""
pipeline/store.py
Supabase 写入层：通过 supabase-py REST API（HTTPS）写入，无需直连 PostgreSQL。

设计原则：
  1. 幂等：document id = SHA256(ticker+date+content[:100])[:16]
  2. upsert = INSERT ... ON CONFLICT DO UPDATE，天天重跑不产生重复数据
  3. 批量 100 条/次，减少数据库往返次数
  4. SUPABASE_KEY 未配置时自动切到 dry_run 模式，方便本地调试
  5. 向量以字符串 '[x1,x2,...]' 形式传给 PostgREST，pgvector 自动转换
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import List, Optional

from supabase import create_client, Client

from pipeline.config import SUPABASE_URL, SUPABASE_KEY, DEFAULT_TICKERS

logger = logging.getLogger(__name__)

import os as _os
BATCH_SIZE = int(_os.getenv("BATCH_SIZE", "100"))
_client: Optional[Client] = None  # 模块级单例，避免重复初始化


def _get_client() -> Optional[Client]:
    """获取 Supabase client 单例。SUPABASE_KEY 未配置时返回 None。"""
    global _client
    if _client is not None:
        return _client
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        _client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized (REST API)")
        return _client
    except Exception as e:
        logger.warning(f"Supabase client init failed: {e}")
        return None


def _vec_str(v: list) -> str:
    """将 float list 转为 pgvector 格式字符串 '[x1,x2,...]'。"""
    return "[" + ",".join(str(x) for x in v) + "]"


def make_document_id(ticker: str, date: str, content: str) -> str:
    """生成确定性 document ID = SHA256(ticker+date+content[:100])[:16]。"""
    raw = f"{ticker}{date}{content[:100]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def upsert_documents(rows: List[dict], dry_run: bool = False) -> int:
    """批量 upsert 到 documents 表（含 embedding 向量）。"""
    if dry_run or not SUPABASE_KEY:
        logger.info(f"[dry_run] upsert {len(rows)} documents")
        for r in rows[:2]:
            logger.info(f"  {r['ticker']} | {r['doc_type']} | {r['date']} | id={r['id']}")
        return len(rows)

    client = _get_client()
    if not client:
        return 0

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i: i + BATCH_SIZE]
        try:
            formatted = []
            for r in batch:
                row = dict(r)
                # PostgREST 接受向量字符串 '[x1,x2,...]'，pgvector 自动转换
                if row.get("embedding") is not None:
                    row["embedding"] = _vec_str(row["embedding"])
                formatted.append(row)
            resp = client.table("documents").upsert(formatted).execute()
            total += len(resp.data)
            logger.info(f"Upserted {len(resp.data)} docs (batch {i // BATCH_SIZE + 1})")
        except Exception as e:
            logger.error(f"documents batch {i // BATCH_SIZE + 1} failed: {e}")
    return total


def upsert_earnings(rows: List[dict], dry_run: bool = False) -> int:
    """批量 upsert 到 earnings 表。conflict key：(ticker, quarter)"""
    if dry_run or not SUPABASE_KEY:
        logger.info(f"[dry_run] upsert {len(rows)} earnings rows")
        return len(rows)

    client = _get_client()
    if not client:
        return 0

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i: i + BATCH_SIZE]
        try:
            resp = client.table("earnings").upsert(batch).execute()
            total += len(resp.data)
        except Exception as e:
            logger.error(f"earnings batch failed: {e}")
    logger.info(f"upsert_earnings: {total}/{len(rows)} rows written")
    return total


def upsert_price_snapshot(rows: List[dict], dry_run: bool = False) -> int:
    """批量 upsert 到 price_snapshot 表。conflict key：(ticker, date)"""
    if dry_run or not SUPABASE_KEY:
        logger.info(f"[dry_run] upsert {len(rows)} price snapshots")
        return len(rows)

    client = _get_client()
    if not client:
        return 0

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i: i + BATCH_SIZE]
        try:
            resp = client.table("price_snapshot").upsert(batch).execute()
            total += len(resp.data)
        except Exception as e:
            logger.error(f"price_snapshot batch failed: {e}")
    logger.info(f"upsert_price_snapshot: {total}/{len(rows)} rows written")
    return total


def get_tracked_tickers() -> List[dict]:
    """从 Supabase 读取所有 is_active=true 的 ticker。

    返回: List[{"ticker": str, "ticker_type": str|None, ...}]
    如果 Supabase 未配置，回退到 DEFAULT_TICKERS。
    """
    client = _get_client()
    if not client:
        logger.warning("No Supabase client — using DEFAULT_TICKERS from config")
        return [{"ticker": t, "ticker_type": None} for t in DEFAULT_TICKERS]

    try:
        resp = (
            client.table("tracked_tickers")
            .select("ticker, ticker_type, last_news_fetch, last_filing_fetch")
            .eq("is_active", True)
            .order("ticker")
            .execute()
        )
        rows = resp.data
        if not rows:
            logger.warning("tracked_tickers is empty — using DEFAULT_TICKERS")
            return [{"ticker": t, "ticker_type": None} for t in DEFAULT_TICKERS]
        return list(rows)  # type: ignore[return-value]
    except Exception as e:
        logger.error(f"get_tracked_tickers failed: {e}")
        return [{"ticker": t, "ticker_type": None} for t in DEFAULT_TICKERS]


def update_ticker_timestamps(
    ticker: str,
    news_fetch: Optional[str] = None,
    filing_fetch: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """更新 tracked_tickers 时间戳，供 Person B 显示数据新鲜度。

    last_news_fetch     → 新闻最近成功抓取日期
    last_filing_fetch   → FMP/EDGAR 财报最近成功抓取日期
    last_successful_run → 最近一次完整处理时间（UTC）
    非致命操作，失败时只打 warning，不中断主流程。
    """
    if dry_run or not SUPABASE_KEY:
        logger.info(f"[dry_run] timestamps {ticker}: news={news_fetch}, filing={filing_fetch}")
        return

    client = _get_client()
    if not client:
        return

    updates: dict = {}
    if news_fetch:
        updates["last_news_fetch"] = news_fetch
    if filing_fetch:
        updates["last_filing_fetch"] = filing_fetch

    if not updates:
        return  # 没有要更新的字段，直接返回

    try:
        client.table("tracked_tickers").update(updates).eq("ticker", ticker).execute()
        logger.debug(f"Updated timestamps for {ticker}")
    except Exception as e:
        logger.warning(f"Failed to update timestamps for {ticker}: {e}")
