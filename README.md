# DE Medallion ETL Pipeline

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


## Tech Stack

| Component        | Technology                        |
|------------------|-----------------------------------|
| Orchestration    | Apache Airflow 2.8                |
| Processing       | PySpark 3.5                       |
| Transformation   | dbt-core 1.7 + dbt-postgres       |
| Storage (Bronze/Silver) | MinIO (S3-compatible)    |
| Storage (Gold)   | PostgreSQL 15                     |
| Data Source      | yfinance (Yahoo Finance API)      |
| Infrastructure   | Docker Compose                    |
| CI/CD            | GitHub Actions                    |
| Language         | Python 3.11                       |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- 4 GB RAM available for Docker

That's it — no AWS account, no cloud credentials needed.

---



## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/girijathiru2025/data-engineering-medallion-architecture.git
cd data-engineering-medallion-architecture

# 2. Start all services
cd docker
docker compose up -d

# Wait ~60 seconds for services to initialize, then:

# 3. Verify services are running
docker compose ps
```


---

## Tickers Covered

10 large-cap US stocks across sectors:

| Ticker | Company           | Sector             |
|--------|-------------------|--------------------|
| AAPL   | Apple             | Technology         |
| MSFT   | Microsoft         | Technology         |
| GOOGL  | Alphabet          | Technology         |
| AMZN   | Amazon            | Consumer Cyclical  |
| META   | Meta Platforms    | Technology         |
| NVDA   | NVIDIA            | Technology         |
| TSLA   | Tesla             | Consumer Cyclical  |
| JPM    | JPMorgan Chase    | Financial Services |
| JNJ    | Johnson & Johnson | Healthcare         |
| V      | Visa              | Financial Services |

Data range: 2020-01-01 to present

---