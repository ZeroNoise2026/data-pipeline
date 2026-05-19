# data-pipeline

> QuantAgent Microservice — Data Ingestion, Cleaning, Embedding & Storage

Pulls raw financial data from Finnhub, SEC EDGAR, and FMP; cleans and
chunks it; calls `embedding-service` to vectorize; writes to Supabase.
Also exposes a small FastAPI layer for ad-hoc live lookups that the
other services hit directly.

> **For PMs / analysts:** This is the upstream pipeline that keeps
> Supabase fresh. Everything users see in chat, briefings, or research
> reports is fetched, cleaned, and indexed here first. If the chat
> says "no recent data for this ticker," the answer almost always
> lives in this repo (Was it added to `tracked_tickers`? Did the
> Finnhub/FMP quota run out? Did the embedder fail?).

> **For engineers:** Two artifacts: a CLI / Prefect flow
> (`pipeline/run.py`) for scheduled ingestion, and a FastAPI layer
> (`pipeline/api.py`) for sync live lookups used by Summarization's
> question-service. Owned tables: `documents`, `earnings`,
> `price_snapshot`, `tracked_tickers`, `summary_cache`. We are the
> only writer for these.

## Architecture

```
        ┌─ Finnhub API ─────────┐
        ├─ SEC EDGAR ───────────┤  pipeline/clients/*
        └─ FMP API ─────────────┘
                │
                ▼
       pipeline/cleaner.py        normalize, dedupe, strip boilerplate
                │
                ▼
       pipeline/chunker.py        512-char chunks, 64-char overlap, sentence-aware
                │
                ▼
       embedding-service /encode  384-dim all-MiniLM-L6-v2 vectors (batched)
                │
                ▼
       pipeline/store.py          upsert into Supabase (idempotent, by content hash)
                │
                ▼
    ┌─ documents ─ earnings ─ price_snapshot ─ tracked_tickers ─┐
    └────────────────── Supabase (pgvector) ────────────────────┘
                ▲
                │  read-only access from
       Summarization (CLI + question-service) + ChatbotUI
```

## Layout

```
data-pipeline/
├── pipeline/
│   ├── run.py                Prefect @flow entry — full ingestion pass
│   ├── api.py                FastAPI :8001 — live lookups (Finnhub/FMP passthroughs)
│   ├── config.py             env-driven config
│   ├── cleaner.py            normalize text, strip boilerplate, dedupe
│   ├── chunker.py            512-char chunks w/ overlap; sentence-boundary split
│   ├── store.py              Supabase writer (upserts by content SHA256)
│   ├── schema.sql            DDL for every table in the project (source of truth)
│   ├── clients/
│   │   ├── finnhub_client.py
│   │   ├── edgar_client.py
│   │   └── fmp_client.py
│   └── utils/
│       └── retry.py
├── Dockerfile.api             FastAPI image (Cloud Run-ready, $PORT)
├── Dockerfile.worker          Prefect worker image (long-running, pulls from work pool)
├── requirements.txt
├── .github/workflows/ingest.yml  GitHub Actions cron template (currently manual-only)
└── README.md
```

## Prerequisites

- Python 3.11+
- A Supabase project with `pgvector` enabled
- A reachable `embedding-service` (port 8002 by default)
- API keys for Finnhub, FMP, and an `EDGAR_USER_AGENT` for SEC EDGAR

## Setup

```bash
cd data-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env          # fill in keys

# One-time: create tables in Supabase
psql "$SUPABASE_DB_URL" -f pipeline/schema.sql
```

## Usage — ingestion

```bash
# Full run — every active ticker in tracked_tickers
python -m pipeline.run

# Single ticker, useful for debugging
python -m pipeline.run --ticker AAPL

# Dry-run: log everything but write nothing to Supabase
python -m pipeline.run --dry-run

# Backfill mode — uses initial-load fetch windows instead of incremental
python -m pipeline.run --initial
```

Per-ticker behavior:

- Errors on one ticker don't affect the others (isolated try/except).
- Sleep 1s between tickers to respect Finnhub's 60 req/min limit.
- Embeddings are batched at 100 chunks per request to `embedding-service`.
- All Supabase writes are upserts by content SHA256 — re-running is safe
  and won't create duplicates.

## Usage — FastAPI live-lookup layer

`pipeline/api.py` exposes a small read-only API consumed by other
services that need *live* data, not cached snapshots.

```bash
# Local dev
uvicorn pipeline.api:app --port 8001 --reload

# Docker
docker build -f Dockerfile.api -t qa-pipeline-api .
docker run -p 8001:8080 --env-file .env qa-pipeline-api
```

Endpoints:

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness check |
| `GET` | `/api/finnhub/news/{ticker}` | Live news from Finnhub |
| `GET` | `/api/fmp/quote/{ticker}` | Live quote from FMP |
| `GET` | `/api/finnhub/earnings/{ticker}` | Live earnings from Finnhub |

## Orchestration

The ingestion flow is wrapped in Prefect `@flow` / `@task` decorators
for retry, observability, and parallelism.

- **Local:** `python -m pipeline.run` runs without a Prefect server —
  the decorators just add logging and retries.
- **With Prefect server:** `prefect server start` then deploy via the
  Prefect UI at <http://localhost:4200>.
- **Production:** `Dockerfile.worker` runs `prefect worker start` and
  is intended to be deployed as a long-lived Cloud Run service
  (`min-instances=1`) pulling from a Prefect Cloud work pool. The
  worker image expects `PREFECT_API_URL`, `PREFECT_API_KEY`, and
  `PREFECT_WORK_POOL` (defaults to `default-pool`).

GitHub Actions (`.github/workflows/ingest.yml`) provides an alternative
cron path — currently `workflow_dispatch` only; flip the commented
`schedule: '0 */6 * * *'` to enable every-6-hours runs.

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL`, `SUPABASE_KEY` | yes | Supabase project credentials |
| `EMBEDDING_SERVICE_URL` | yes | e.g. `http://localhost:8002` |
| `FINNHUB_API_KEY` | yes | Finnhub Plus or higher (for news+earnings) |
| `FMP_API_KEY` | yes | Financial Modeling Prep API key |
| `EDGAR_USER_AGENT` | yes | SEC requires a contact email in UA |
| `PREFECT_API_URL`, `PREFECT_API_KEY`, `PREFECT_WORK_POOL` | worker only | Prefect Cloud config |

## Supabase schema

`pipeline/schema.sql` is the **single source of truth** for the DDL of
every table in the project (not just the ones we write). It defines:

- **Owned by this repo (writer = data-pipeline):**
  - `documents` — text chunks + embeddings + metadata
  - `earnings` — structured quarterly numbers
  - `price_snapshot` — daily price/PE/market cap
  - `tracked_tickers` — active ticker list driving ingestion
  - `summary_cache` — cached reports (written by Summarization, but
    DDL lives here)
- **Used by other services:** `user_watchlist`, `user_preferences`,
  `daily_briefings` (ChatbotUI is the writer; DDL here keeps the
  schema in one place).

`documents.id` is a SHA256 of the cleaned chunk text — so the same
content always maps to the same id, and the cache key in Summarization
can rely on stable ids without re-hashing bodies.

## Skills integration

This repo does **not** call into `skills/`. Data ingestion happens
upstream of any user-facing capability. The flow direction is strictly:

```
data-pipeline  →  Supabase  →  {Summarization, ChatbotUI, skills}
```

That said, the schema choices here (especially `documents.id =
SHA256(content)` and the cache lineage in `summary_cache`) are what
make skill behavior reproducible. If you change the DDL, expect
follow-up work in `Summarization/summary/cache.py`.

## Related services

| Service | Why this repo cares |
|---------|---------------------|
| `embedding-service` (:8002) | We POST to `/api/encode` for every batch of chunks |
| `Summarization` | Reads from `documents`, `earnings`, `price_snapshot`; writes only to `summary_cache` |
| `ChatbotUI` | Reads from the same tables; writes only to per-user tables |
