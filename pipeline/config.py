"""
pipeline/config.py
从 .env 读取所有服务地址和认证信息。
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

# 上游服务 URL
DATA_FETCHERS_URL     = os.getenv("DATA_FETCHERS_URL",     "http://localhost:8001")
EMBEDDING_SERVICE_URL = os.getenv("EMBEDDING_SERVICE_URL", "http://localhost:8002")

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "")   # https://xxx.supabase.co
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")   # service_role key（写库用）

# Supabase 未配置时的备用 ticker 列表
DEFAULT_TICKERS = os.getenv("DEFAULT_TICKERS", "AAPL,MSFT,NVDA").split(",")

# API 配额保护
FILING_REFRESH_DAYS = int(os.getenv("FILING_REFRESH_DAYS", "30"))   # FMP/EDGAR 重抓间隔（天）
EDGAR_FACTS_MAX_MB  = float(os.getenv("EDGAR_FACTS_MAX_MB", "15"))  # EDGAR facts JSON 大小上限（MB）

# 批量大小（随 Supabase/embedding-service 升级可调大）
EMBED_BATCH = int(os.getenv("EMBED_BATCH", "100"))   # 单次向量化最大块数
BATCH_SIZE  = int(os.getenv("BATCH_SIZE",  "100"))   # 单次写库最大行数

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
