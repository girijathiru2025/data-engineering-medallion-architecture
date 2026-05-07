# DE Medallion ETL Pipeline

End-to-end data pipeline using Snowflake, dbt, and Airflow — implementing Medallion Architecture with incremental CDC via Snowflake Streams, automated data quality validation, and full pipeline observability.

Built with: **PySpark · Apache Airflow · dbt · PostgreSQL · MinIO (S3-compatible) · Docker**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                               ORCHESTRATION LAYER                                   │
│                           Apache Airflow (Mon–Fri 6 AM UTC)                         │
└───────────┬────────────────┬──────────────────┬──────────────┬──────────────────────┘
            │                │                  │              │
            ▼                ▼                  ▼              ▼
   ┌─────────────┐  ┌──────────────────┐  ┌──────────┐  ┌──────────────┐
   │   BRONZE    │  │     SILVER       │  │  LOAD    │  │     GOLD     │
   │             │  │                  │  │          │  │              │
   │ Raw Parquet │  │ Cleansed Parquet │  │ MinIO →  │  │  Star Schema │
   │  on MinIO   │─▶│   on MinIO       │─▶│ Postgres │─▶│ (PostgreSQL) │
   │             │  │                  │  │          │  │              │
   │ yfinance    │  │ PySpark job      │  │ Airflow  │  │ dbt models   │
   │ partitioned │  │ Quality checks   │  │ task     │  │ fact + dims  │
   │ by ticker/  │  │ Rejected records │  │          │  │              │
   │ date        │  │ → MinIO rejected │  │          │  │              │
   └─────────────┘  └──────────────────┘  └──────────┘  └──────────────┘
```

### Layer Responsibilities

| Layer  | Storage        | Tool     | Purpose                                                        |
|--------|----------------|----------|----------------------------------------------------------------|
| Bronze | MinIO (S3)     | Python   | Raw ingestion, append-only, partitioned by ticker/date         |
| Silver | MinIO (S3)     | PySpark  | Cleansed, validated, enriched; rejected records isolated       |
| Load   | PostgreSQL     | Airflow  | Bridges Silver Parquet → PostgreSQL for dbt to query           |
| Gold   | PostgreSQL     | dbt      | Star schema — analytics-ready for BI/reporting                 |

---

## Star Schema (Gold Layer)

```
              dim_date
             ┌─────────┐
             │ date_key│
             │ year    │
             │ quarter │
             │ month   │
             └────┬────┘
                  │
dim_ticker        │        fact_stock_prices
┌──────────┐      │       ┌──────────────────┐
│ticker_key├──────┼───────│ price_key        │
│symbol    │      └───────│ date_key         │
│company   │              │ ticker_key       │
│sector    │              │ open_price       │
│exchange  │              │ high_price       │
└──────────┘              │ low_price        │
                          │ close_price      │
                          │ adj_close_price  │
                          │ volume           │
                          │ daily_return     │
                          │ price_range      │
                          └──────────────────┘
```

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

## Data Quality Framework

The Silver layer applies the following checks to every record:

| Check                | Rule                                           | Action on Failure       |
|----------------------|------------------------------------------------|-------------------------|
| Null completeness    | No nulls in date, ticker, OHLCV               | Write to rejected path  |
| Positive prices      | open, high, low, close > 0                    | Write to rejected path  |
| Non-negative volume  | volume >= 0                                   | Write to rejected path  |
| OHLC consistency     | high >= low, high >= open, high >= close      | Write to rejected path  |

Rejected records are written to `s3://rejected/stock_prices/run_ts=<timestamp>/` with a `failure_reason` column for downstream analysis and remediation tracking.

Pipeline runs are logged to the `pipeline_audit_log` table in PostgreSQL.

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

# 4. Trigger a manual pipeline run
# Open Airflow UI: http://localhost:8080
# Login: admin / admin
# Enable and trigger: de_medallion_stock_pipeline DAG

# 5. Check MinIO (Bronze/Silver data)
# Open MinIO Console: http://localhost:9001
# Login: minioadmin / minioadmin

# 6. Query Gold layer (PostgreSQL)
docker compose exec postgres psql -U airflow -d gold -c "SELECT COUNT(*) FROM gold.fact_stock_prices;"
```

---

## Project Structure

```
data-engineering-medallion-architecture/
├── airflow/
│   └── dags/
│       └── de_medallion_stock_pipeline_dag.py   # Main Airflow DAG
├── spark/
│   └── jobs/
│       ├── bronze_ingest.py            # Bronze: yfinance → MinIO
│       └── silver_transform.py         # Silver: PySpark cleanse + validate
├── dbt/
│   ├── models/
│   │   └── gold/
│   │       ├── dim_date.sql
│   │       ├── dim_ticker.sql
│   │       ├── fact_stock_prices.sql
│   │       └── schema.yml              # dbt tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── scripts/
│   └── init_gold_db.sql               # Creates gold database and pipeline_audit_log table
├── tests/
│   └── test_silver_quality_checks.py  # PySpark unit tests
├── docker/
│   └── docker-compose.yml
├── .github/
│   └── workflows/ci.yml               # GitHub Actions CI
└── requirements.txt
```

---

## Running Tests Locally

```bash
pip install pyspark==3.5.1 pytest==8.1.1 pytest-cov==5.0.0
pytest tests/ -v
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

Schedule: Daily (Mon–Fri) at 6 AM UTC — fetches previous trading day's data

---

## Design Decisions

- **MinIO over real AWS S3** — same boto3/S3A API, zero cloud cost, runs fully local
- **PostgreSQL for Gold** — dbt native support, easy to query, no warehouse account needed
- **Incremental dbt models** — fact table uses `merge` strategy; reruns are safe and efficient
- **Rejected records pattern** — mirrors production data quality practices; failure_reason enables targeted remediation
- **Docker Compose** — single command to run the entire platform locally for reviewers

---

