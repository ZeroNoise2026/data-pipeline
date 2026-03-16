# 🔄 Data Pipeline

> QuantAgent Microservice — Data Processing & Storage

Calls data-fetchers to obtain raw data, cleans and chunks it, then calls embedding-service for vectorization, and finally writes to Supabase.

## Architecture

```
GitHub Actions (cron 6h)
    │
    ▼
pipeline/run.py
    ├── GET  data-fetchers/api/...        fetch raw data
    ├── pipeline/cleaner.py               clean
    ├── pipeline/chunker.py               chunk
    ├── POST embedding-service/api/encode vectorize
    └── pipeline/store.py → Supabase      store
```

## Quick Start

```bash
cp .env.example .env   # fill in configuration
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python -m pipeline.run
```

## Related Services

| Service | Repo | How This Service Calls It |
|---------|------|--------------------------|
| data-fetchers | [data-fetchers](https://github.com/ZeroNoise2026/data-fetchers) | HTTP GET to fetch data |
| embedding-service | [embedding-service](https://github.com/ZeroNoise2026/embedding-service) | HTTP POST to get vectors |
