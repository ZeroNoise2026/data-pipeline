# 🔄 Data Pipeline

> QuantAgent Microservice — Data Processing & Storage

Fetches raw financial data from Finnhub, SEC EDGAR, and FMP directly via Python clients, cleans and chunks it, calls embedding-service for vectorization, and writes to Supabase.

## Architecture

```
Prefect Flow (scheduled or manual)
    │
    ▼
pipeline/run.py
    ├── pipeline/clients/finnhub_client.py   fetch news, earnings, financials
    ├── pipeline/clients/edgar_client.py     fetch XBRL facts, filings
    ├── pipeline/clients/fmp_client.py       fetch quotes, financial statements
    ├── pipeline/cleaner.py                  clean
    ├── pipeline/chunker.py                  chunk
    ├── POST embedding-service/api/encode    vectorize
    └── pipeline/store.py → Supabase         store
```

## Quick Start

```bash
cp .env.example .env   # fill in API keys and configuration
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Run the full pipeline
python -m pipeline.run

# Single ticker debug
python -m pipeline.run --ticker AAPL --dry-run
```

## Prefect

This pipeline uses [Prefect](https://www.prefect.io/) for orchestration. You can:

- Run locally with `python -m pipeline.run`
- Start a Prefect server with `prefect server start` and view flows at http://localhost:4200
- Deploy to Prefect Cloud for scheduled runs

## Related Services

| Service | Repo | How This Service Calls It |
|---------|------|--------------------------|
| embedding-service | [embedding-service](https://github.com/ZeroNoise2026/embedding-service) | HTTP POST to get vectors |
