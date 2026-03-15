import os
from dotenv import load_dotenv

load_dotenv()

DATA_FETCHERS_URL = os.getenv("DATA_FETCHERS_URL", "http://localhost:8001")
EMBEDDING_SERVICE_URL = os.getenv("EMBEDDING_SERVICE_URL", "http://localhost:8002")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "")
DEFAULT_TICKERS = os.getenv("DEFAULT_TICKERS", "AAPL,MSFT,GOOGL").split(",")
