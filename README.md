# 🔄 Data Pipeline

> QuantAgent 微服务 — 数据处理与写入

调用 data-fetchers 获取原始数据，清洗、分块后调用 embedding-service 向量化，最终写入 Supabase。

## 架构

```
GitHub Actions (cron 6h)
    │
    ▼
pipeline/run.py
    ├── GET  data-fetchers/api/...        拿原始数据
    ├── pipeline/cleaner.py               清洗
    ├── pipeline/chunker.py               分块
    ├── POST embedding-service/api/encode 向量化
    └── pipeline/store.py → Supabase      写入
```

## Quick Start

```bash
cp .env.example .env   # 填入配置
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python -m pipeline.run
```

## 关联服务

| Service | Repo | 本服务如何调用 |
|---------|------|--------------|
| data-fetchers | [data-fetchers](https://github.com/ZeroNoise2026/data-fetchers) | HTTP GET 拿数据 |
| embedding-service | [embedding-service](https://github.com/ZeroNoise2026/embedding-service) | HTTP POST 拿向量 |
