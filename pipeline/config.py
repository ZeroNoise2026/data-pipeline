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

# Supabase（Person B 建表后提供）
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")  # service_role key

# Supabase 未配置时的备用 ticker 列表
DEFAULT_TICKERS = os.getenv("DEFAULT_TICKERS", "AAPL,MSFT,NVDA").split(",")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
