-- QuantAgent Database Schema
-- PostgreSQL + pgvector on Supabase
-- Run via: python -m pipeline.init_db

-- 1. Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Documents table (semantic search target)
CREATE TABLE IF NOT EXISTS documents (
    id          TEXT PRIMARY KEY,
    content     TEXT NOT NULL,
    embedding   VECTOR(384),
    ticker      VARCHAR(10),
    date        VARCHAR(10),          -- YYYY-MM-DD
    source      VARCHAR(30),          -- finnhub | fmp | edgar
    doc_type    VARCHAR(20),          -- news | 10-K | 10-Q | earnings
    section     VARCHAR(50),
    title       TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_documents_embedding
    ON documents USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE INDEX IF NOT EXISTS idx_documents_ticker   ON documents (ticker);
CREATE INDEX IF NOT EXISTS idx_documents_date     ON documents (date);
CREATE INDEX IF NOT EXISTS idx_documents_doc_type ON documents (doc_type);
CREATE INDEX IF NOT EXISTS idx_documents_ticker_date ON documents (ticker, date);

-- 3. Earnings table (structured financial data)
CREATE TABLE IF NOT EXISTS earnings (
    ticker      VARCHAR(10) NOT NULL,
    quarter     VARCHAR(20) NOT NULL,
    date        VARCHAR(10),
    eps         DOUBLE PRECISION,
    revenue     BIGINT,
    net_income  BIGINT,
    guidance    TEXT,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ticker, quarter)
);

CREATE INDEX IF NOT EXISTS idx_earnings_ticker ON earnings (ticker);

-- 4. Price snapshot table
CREATE TABLE IF NOT EXISTS price_snapshot (
    ticker      VARCHAR(10) NOT NULL,
    date        VARCHAR(10) NOT NULL,
    close_price DOUBLE PRECISION,
    pe_ratio    DOUBLE PRECISION,
    market_cap  BIGINT,
    PRIMARY KEY (ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_price_ticker ON price_snapshot (ticker);

-- 5. Tracked tickers (pipeline processes these every 6h)
CREATE TABLE IF NOT EXISTS tracked_tickers (
    ticker              VARCHAR(10) PRIMARY KEY,
    ticker_type         TEXT        DEFAULT 'stock',     -- stock | etf
    follower_count      INTEGER     DEFAULT 0,
    is_active           BOOLEAN     DEFAULT TRUE,
    last_news_fetch     TIMESTAMPTZ,
    last_filing_fetch   TIMESTAMPTZ,
    last_successful_run TIMESTAMPTZ
);

-- 6. User watchlist (linked to Supabase Auth)
CREATE TABLE IF NOT EXISTS user_watchlist (
    user_id     UUID        NOT NULL,
    ticker      VARCHAR(10) NOT NULL,
    added_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, ticker)
);

-- 7. Semantic search helper function (used by Person B's retriever)
CREATE OR REPLACE FUNCTION search_documents(
    query_embedding VECTOR(384),
    match_ticker    TEXT    DEFAULT NULL,
    match_doc_type  TEXT    DEFAULT NULL,
    match_count     INT     DEFAULT 5
)
RETURNS TABLE (
    id          TEXT,
    content     TEXT,
    ticker      VARCHAR(10),
    date        VARCHAR(10),
    source      VARCHAR(30),
    doc_type    VARCHAR(20),
    title       TEXT,
    similarity  FLOAT
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.id, d.content, d.ticker, d.date, d.source, d.doc_type, d.title,
        1 - (d.embedding <=> query_embedding) AS similarity
    FROM documents d
    WHERE
        (match_ticker   IS NULL OR d.ticker   = match_ticker)
        AND (match_doc_type IS NULL OR d.doc_type = match_doc_type)
    ORDER BY d.embedding <=> query_embedding
    LIMIT match_count;
END;
$$;

-- 8. Seed initial tracked tickers
INSERT INTO tracked_tickers (ticker, ticker_type) VALUES
    ('AAPL',  'stock'),
    ('MSFT',  'stock'),
    ('NVDA',  'stock'),
    ('GOOGL', 'stock'),
    ('AMZN',  'stock'),
    ('META',  'stock'),
    ('TSLA',  'stock'),
    ('VOO',   'etf'),
    ('SPY',   'etf'),
    ('QQQ',   'etf')
ON CONFLICT (ticker) DO NOTHING;
