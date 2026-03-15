"""
pipeline/store.py
Supabase 写入层：封装 documents / earnings / price_snapshot 三张表的 upsert。

设计原则：
  1. 幂等：document id = SHA256(ticker+date+content[:100])[:16]
  2. upsert = INSERT ... ON CONFLICT DO UPDATE，天天重跑不产生重复数据
  3. 批量 100 条/次，减少 Supabase 免费版 API 请求次数
  4. SUPABASE_URL 未配置时自动切到 dry_run 模式，方便本地调试
"""

import hashlib
import logging
from typing import List

from pipeline.config import SUPABASE_URL, SUPABASE_KEY, DEFAULT_TICKERS

logger = logging.getLogger(__name__)

BATCH_SIZE = 100  # Supabase 单次请求建议上限


def _get_client():
    """延迟初始化 supabase client，导入时不强要求安装。"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        from supabase import create_client
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except ImportError:
        logger.warning("supabase-py not installed, running in dry_run mode")
        return None


def make_document_id(ticker: str, date: str, content: str) -> str:
    """生成确定性 document ID。

    ID = SHA256(ticker + date + content[:100])[:16]
    相同内容无论运行多少次，始终得到同一个 ID。
    """
    raw = f"{ticker}{date}{content[:100]}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def upsert_documents(rows: List[dict], dry_run: bool = False) -> int:
    """批量 upsert 到 documents 表（含 embedding 向量）。

    rows 必填字段：
        id, content, embedding, ticker, date, source, doc_type
    rows 可选字段：
        section, title
    """
    if dry_run or not SUPABASE_URL:
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
            client.table("documents").upsert(batch, on_conflict="id").execute()
            total += len(batch)
            logger.info(f"Upserted {len(batch)} docs (batch {i // BATCH_SIZE + 1})")
        except Exception as e:
            logger.error(f"documents batch {i // BATCH_SIZE + 1} failed: {e}")
    return total


def upsert_earnings(rows: List[dict], dry_run: bool = False) -> int:
    """批量 upsert 到 earnings 表。

    rows 字段：ticker, quarter, date, eps, revenue, net_income, guidance
    conflict key：(ticker, quarter)
    """
    if dry_run or not SUPABASE_URL:
        logger.info(f"[dry_run] upsert {len(rows)} earnings rows")
        return len(rows)

    client = _get_client()
    if not client:
        return 0

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i: i + BATCH_SIZE]
        try:
            client.table("earnings").upsert(
                batch, on_conflict="ticker,quarter"
            ).execute()
            total += len(batch)
        except Exception as e:
            logger.error(f"earnings batch failed: {e}")
    return total


def upsert_price_snapshot(rows: List[dict], dry_run: bool = False) -> int:
    """批量 upsert 到 price_snapshot 表。

    rows 字段：ticker, date, close_price, pe_ratio, market_cap
    conflict key：(ticker, date)
    """
    if dry_run or not SUPABASE_URL:
        logger.info(f"[dry_run] upsert {len(rows)} price snapshots")
        return len(rows)

    client = _get_client()
    if not client:
        return 0

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i: i + BATCH_SIZE]
        try:
            client.table("price_snapshot").upsert(
                batch, on_conflict="ticker,date"
            ).execute()
            total += len(batch)
        except Exception as e:
            logger.error(f"price_snapshot batch failed: {e}")
    return total


def get_tracked_tickers() -> List[dict]:
    """从 Supabase 读取所有 is_active=true 的 ticker。

    返回: List[{"ticker": str, "ticker_type": str}]
    如果 Supabase 未配置，回退到 DEFAULT_TICKERS。
    """
    client = _get_client()
    if not client:
        logger.warning("No Supabase — using DEFAULT_TICKERS from config")
        return [{"ticker": t, "ticker_type": "stock"} for t in DEFAULT_TICKERS]

    try:
        resp = (
            client.table("tracked_tickers")
            .select("ticker, ticker_type")
            .eq("is_active", True)
            .execute()
        )
        return resp.data or []
    except Exception as e:
        logger.error(f"get_tracked_tickers failed: {e}")
        return [{"ticker": t, "ticker_type": "stock"} for t in DEFAULT_TICKERS]
