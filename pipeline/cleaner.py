"""
pipeline/cleaner.py
文本清洗：HTML 去标签、空白标准化、新闻和财报格式化。
输入: 原始字符串（可能含 HTML）
输出: 供 RAG 分块使用的纯文本
"""

import re
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Optional


# ── 基础清洗函数 ─────────────────────────────────────

def strip_html(text: str) -> str:
    """移除 HTML 标签，保留纯文本。处理 <br> 为换行。"""
    soup = BeautifulSoup(text, "html.parser")
    # 移除不需要的块
    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()
    # <br> 转换为换行
    for br in soup.find_all("br"):
        br.replace_with("\n")
    return soup.get_text(separator=" ")


def normalize_whitespace(text: str) -> str:
    """合并多余空白和换行，移除首尾空白。"""
    text = re.sub(r'[ \t]+', ' ', text)          # 多个空格/tab 合并为一个
    text = re.sub(r'\n{3,}', '\n\n', text)       # 3+ 换行庋到 2 个
    text = re.sub(r' \n', '\n', text)             # 换行前空格
    return text.strip()


def format_timestamp(unix_ts: Optional[int]) -> str:
    """将 Unix timestamp 转成 ISO 日期字符串，供文档元数据使用。"""
    if not unix_ts:
        return datetime.now().strftime("%Y-%m-%d")
    return datetime.utcfromtimestamp(unix_ts).strftime("%Y-%m-%d")


# ── 业务清洗函数 ─────────────────────────────────────

def clean_news_article(article: dict) -> dict:
    """清洗 Finnhub 新闻小条，返回供 pipeline 使用的元数据字典。

    Returns:
        {
            "content": str,    # headline + summary 合并的纯文本
            "title":   str,    # 原始 headline
            "date":    str,    # ISO 日期
            "source":  str,    # Finnhub / SeekingAlpha 等
            "url":     str,
        }
    """
    headline = article.get("headline", "")
    summary  = article.get("summary", "")
    source   = article.get("source", "")
    url      = article.get("url", "")
    ts       = article.get("datetime")

    # 拼接标题 + 摘要，這两段内容共同构成 RAG chunk 的原料
    raw = f"{headline}. {summary}" if summary else headline
    content = normalize_whitespace(strip_html(raw))

    return {
        "content": content,
        "title":   headline,
        "date":    format_timestamp(ts),
        "source":  source,
        "url":     url,
    }


def clean_filing_text(raw_html: str) -> str:
    """清洗 SEC EDGAR 财报原文（HTML → 纯文本）。

    权衡报文通常有大量 HTML 表格标签包裹数字，
    BeautifulSoup 能正确提取表格内容。
    """
    text = strip_html(raw_html)
    text = normalize_whitespace(text)
    return text


def clean_xbrl_value(value: float, unit: str = "USD") -> str:
    """将 XBRL 数字转成可读的字符串（供 earnings 表格存储）。

    Examples:
        1_234_567_890 USD  →  "$1.23B"
        23_400_000 USD     →  "$23.4M"
        450_000 USD        →  "$450K"
    """
    if unit != "USD":
        return str(value)
    abs_val = abs(value)
    sign = "-" if value < 0 else ""
    if abs_val >= 1e9:
        return f"{sign}${abs_val/1e9:.2f}B"
    if abs_val >= 1e6:
        return f"{sign}${abs_val/1e6:.1f}M"
    if abs_val >= 1e3:
        return f"{sign}${abs_val/1e3:.0f}K"
    return f"{sign}${abs_val:.0f}"
