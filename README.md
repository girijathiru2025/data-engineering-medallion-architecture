# Medallion ETL Pipeline

A production-style data engineering portfolio project demonstrating **Medallion Architecture** (Bronze / Silver / Gold) for financial market data.

Built with: **PySpark · Apache Airflow · dbt · PostgreSQL · MinIO (S3-compatible) · Docker**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION LAYER                          │
│                    Apache Airflow (Daily DAG)                       │
└───────────┬────────────────┬──────────────────┬────────────────────┘
            │                │                  │
            ▼                ▼                  ▼
   ┌─────────────┐  ┌──────────────────┐  ┌──────────────┐
   │   BRONZE    │  │     SILVER       │  │     GOLD     │
   │             │  │                  │  │              │
   │ Raw Parquet │  │ Cleansed Parquet │  │  Star Schema │
   │  on MinIO   │─▶│   on MinIO       │─▶│ (PostgreSQL) │
   │             │  │                  │  │              │
   │ yfinance    │  │ PySpark job      │  │ dbt models   │
   │ → partitioned│  │ Quality checks   │  │ fact + dims  │
   │ by date     │  │ Rejected records │  │              │
   └─────────────┘  └──────────────────┘  └──────────────┘
```

### Layer Responsibilities

| Layer  | Storage        | Tool     | Purpose                                              |
|--------|----------------|----------|------------------------------------------------------|
| Bronze | MinIO (S3)     | Python   | Raw ingestion, append-only, partitioned by date      |
| Silver | MinIO (S3)     | PySpark  | Cleansed, validated, deduplicated, enriched          |
| Gold   | PostgreSQL     | dbt      | Star schema — analytics-ready for BI/reporting       |

---
