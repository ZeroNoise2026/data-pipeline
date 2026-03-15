"""
pipeline/chunker.py
文本分块策略：
  - 新闻：不切（headline+summary 通常 < 300 chars）
  - 财报/EDGAR 长文本：512 chars + 64 chars overlap，在句子边界切割

设计考量（Scale）：
  - 512 chars ≈ 128 tokens，all-MiniLM-L6-v2 最大 256 tokens，安全边界
  - overlap 64 chars 保留跨块上下文，避免语义断层
  - 句子边界优先：避免在词中间切断，提升 embedding 质量
  - 最小块 50 chars：过短的块（如页眉页脚）无信息量，丢弃
"""

from typing import List

CHUNK_SIZE    = 512   # 每块最大字符数
CHUNK_OVERLAP = 64    # 相邻块重叠字符数
MIN_CHUNK_LEN = 50    # 小于此长度的块直接丢弃

# 句子边界标志（用于找切割点）
_SENTENCE_ENDS = ("。", ". ", "! ", "? ", ".\n", "!\n", "?\n", "\n\n")


def _find_sentence_boundary(text: str, start: int, end: int) -> int:
    """在 [start, end] 范围内，从 end 向前找最近的句子边界。
    找到返回边界位置，找不到返回 end（硬切）。
    """
    best = -1
    for sep in _SENTENCE_ENDS:
        pos = text.rfind(sep, start + CHUNK_SIZE // 2, end)
        if pos > best:
            best = pos + len(sep)  # 切在分隔符之后
    return best if best > start else end


def chunk_text(
    text: str,
    chunk_size: int = CHUNK_SIZE,
    overlap: int = CHUNK_OVERLAP,
) -> List[str]:
    """将任意长文本切成有 overlap 的块，优先在句子边界处切割。

    Args:
        text: 已经过 cleaner 清洗的纯文本
        chunk_size: 每块最大字符数，默认 512
        overlap: 相邻块重叠字符数，默认 64

    Returns:
        List[str]，每块 >= MIN_CHUNK_LEN 字符
    """
    text = text.strip()
    if not text:
        return []

    # 短文本不需要切割
    if len(text) <= chunk_size:
        return [text] if len(text) >= MIN_CHUNK_LEN else []

    chunks: List[str] = []
    start = 0

    while start < len(text):
        end = start + chunk_size

        if end >= len(text):
            # 最后一块，直接取剩余
            chunk = text[start:].strip()
        else:
            # 尝试在句子边界切割
            cut = _find_sentence_boundary(text, start, end)
            chunk = text[start:cut].strip()
            end = cut  # 下一轮从 cut 往前 overlap 开始

        if len(chunk) >= MIN_CHUNK_LEN:
            chunks.append(chunk)

        # 下一块从 (end - overlap) 开始，保留上下文
        next_start = end - overlap
        if next_start <= start:
            # 防止死循环（极端情况：整段无任何边界）
            next_start = start + chunk_size
        start = next_start

    return chunks


def chunk_news(text: str) -> List[str]:
    """新闻条目：不切割，整条作为一个 chunk。

    新闻 headline+summary 通常 100-400 chars，远小于 512，
    切割反而会破坏语义完整性。
    """
    text = text.strip()
    return [text] if len(text) >= MIN_CHUNK_LEN else []


def chunk_filing(text: str) -> List[str]:
    """SEC 财报全文：按 512 chars + 64 overlap 分块。

    财报全文可达数十万字符，必须分块才能向量化。
    """
    return chunk_text(text)


def chunk_xbrl_facts(ticker: str, facts_us_gaap: dict) -> List[str]:
    """将 EDGAR XBRL facts 转成可向量化的自然语言描述块。

    XBRL 是结构化数据，需要转成文本才能进入 RAG。
    每个核心财务指标生成一句话描述，若干句打包成一个 chunk。

    Args:
        ticker: 股票代码
        facts_us_gaap: edgar_client.get_company_facts() 返回的 us-gaap 字典

    Returns:
        List[str]，每块描述若干季度的财务数据
    """
    # 只提取对 RAG 有价值的核心指标
    METRICS = {
        "NetIncomeLoss":                          "Net Income",
        "Revenues":                               "Revenue",
        "RevenueFromContractWithCustomerExcludingAssessedTax": "Revenue",
        "EarningsPerShareBasic":                  "EPS (Basic)",
        "EarningsPerShareDiluted":                "EPS (Diluted)",
        "GrossProfit":                            "Gross Profit",
        "OperatingIncomeLoss":                    "Operating Income",
        "CashAndCashEquivalentsAtCarryingValue":  "Cash",
        "LongTermDebt":                           "Long-term Debt",
    }

    sentences: List[str] = []
    for xbrl_key, human_label in METRICS.items():
        if xbrl_key not in facts_us_gaap:
            continue
        units = facts_us_gaap[xbrl_key].get("units", {})
        values = units.get("USD", units.get("shares", []))
        # 只取最近 8 个季度（10-Q + 10-K）
        quarterly = [v for v in values if v.get("form") in ("10-Q", "10-K")][-8:]
        for v in quarterly:
            end_date = v.get("end", "")
            val      = v.get("val", 0)
            form     = v.get("form", "")
            sentences.append(
                f"{ticker} {human_label} as of {end_date} ({form}): {val:,}"
            )

    # 每 10 句打包成一个 chunk（约 400 chars），保持语义相关性
    chunks: List[str] = []
    for i in range(0, len(sentences), 10):
        chunk = "\n".join(sentences[i : i + 10])
        if len(chunk) >= MIN_CHUNK_LEN:
            chunks.append(chunk)

    return chunks
