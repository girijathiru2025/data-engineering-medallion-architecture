"""
DE Medallion Stock Pipeline DAG
-----------------------
Orchestrates the full Bronze → Silver → Gold ETL pipeline for stock price data.

Schedule: Daily at 6 AM UTC (after US markets close + data propagation window)
Catchup:  False (run latest only)

Tasks:
  1. bronze_ingest         — fetch raw data from Yahoo Finance → MinIO Bronze bucket (Parquet, partitioned by ticker/date)
  2. silver_transform      — PySpark cleanse/validate → MinIO Silver bucket (rejected records → MinIO rejected bucket)
  3. load_silver_to_postgres — load Silver Parquet from MinIO → PostgreSQL silver.stock_prices (for dbt)
  4. dbt_gold_run          — dbt models → PostgreSQL Gold star schema (dim_date, dim_ticker, fact_stock_prices)
  5. dbt_gold_test         — dbt data quality tests on Gold layer (18 tests)
  6. audit_log             — write pipeline run metrics to pipeline_audit_log table (runs even on failure)

"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)
logger.setLevel(
    logging.DEBUG
)  # By default, the DAG's logger is set to INFO level, so DEBUG messages are suppressed

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "JPM", "JNJ", "V"]


def run_bronze_ingest(**context):
    """Invoke the bronze ingestion job for today's date."""
    import sys

    sys.path.insert(0, "/opt/airflow/spark/jobs")
    from bronze_ingest import run

    execution_date = context["ds"]  # YYYY-MM-DD string
    # yfinance end_date is exclusive, so add 1 day to capture the execution date
    end_date = (
        datetime.strptime(execution_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")
    run_id = context["run_id"]
    logger.debug(
        f"run_bronze_ingest called | run_id={run_id} | execution_date={execution_date}"
    )
    logger.debug(f"Tickers to ingest: {TICKERS}")
    logger.info(f"Starting Bronze ingestion for date: {execution_date}")
    run(
        tickers=TICKERS,
        start_date=execution_date,
        end_date=end_date,
    )
    logger.info("Bronze ingestion complete.")
    logger.debug(f"run_bronze_ingest finished | run_id={run_id}")


def run_silver_transform(**context):
    """Invoke the silver transformation job directly (local[*] mode — no spark-submit needed)."""
    import sys

    sys.path.insert(0, "/opt/airflow/spark/jobs")
    from silver_transform import run

    run_id = context["run_id"]
    logger.debug(f"run_silver_transform called | run_id={run_id}")
    logger.info("Starting Silver transformation.")
    run()
    logger.info("Silver transformation complete.")
    logger.debug(f"run_silver_transform finished | run_id={run_id}")


def load_silver_to_postgres(**context):
    """Read silver Parquet files from MinIO and load into PostgreSQL silver.stock_prices."""
    import boto3
    import pyarrow.parquet as pq
    import pyarrow as pa
    import psycopg2
    from io import BytesIO

    run_id = context["run_id"]
    logger.info(f"Loading silver data to PostgreSQL | run_id={run_id}")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )

    bucket = os.getenv("SILVER_BUCKET", "silver")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix="stock_prices/")

    tables = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            table = pq.read_table(BytesIO(body))
            # PySpark partitionBy strips/nulls the partition column — always set from path
            ticker = next(
                (
                    part.split("=")[1]
                    for part in key.split("/")
                    if part.startswith("ticker=")
                ),
                None,
            )
            if ticker:
                ticker_col = pa.array([ticker] * len(table))
                if "ticker" in table.column_names:
                    idx = table.schema.get_field_index("ticker")
                    table = table.set_column(idx, "ticker", ticker_col)
                else:
                    table = table.append_column("ticker", ticker_col)
            tables.append(table)

    if not tables:
        raise RuntimeError("No silver Parquet files found in MinIO")

    df = pa.concat_tables(tables).to_pandas()
    logger.info(f"Loaded {len(df)} rows from silver MinIO")

    from psycopg2.extras import execute_values

    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    conn = psycopg2.connect(
        host=pg_host, dbname="gold", user="airflow", password="airflow"
    )
    try:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS silver")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS silver.stock_prices (
                date DATE, open DOUBLE PRECISION, high DOUBLE PRECISION,
                low DOUBLE PRECISION, close DOUBLE PRECISION, adj_close DOUBLE PRECISION,
                volume BIGINT, ticker VARCHAR(20), ingested_at TEXT, source TEXT,
                company_name TEXT, sector TEXT, exchange TEXT,
                daily_return DOUBLE PRECISION, price_range DOUBLE PRECISION,
                processed_at TEXT, layer TEXT
            )
        """
        )
        cur.execute("TRUNCATE TABLE silver.stock_prices")

        cols = [
            "date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "ticker",
            "ingested_at",
            "source",
            "company_name",
            "sector",
            "exchange",
            "daily_return",
            "price_range",
            "processed_at",
            "layer",
        ]
        df = df[[c for c in cols if c in df.columns]]

        execute_values(
            cur,
            f"INSERT INTO silver.stock_prices ({','.join(df.columns)}) VALUES %s",
            [tuple(row) for row in df.itertuples(index=False)],
        )
        conn.commit()
        cur.close()
        logger.info(f"Loaded {len(df)} rows into silver.stock_prices")
    finally:
        conn.close()


def write_audit_log(**context):
    """Write pipeline run summary to the audit log table in PostgreSQL."""
    import psycopg2

    run_id = context["run_id"]
    ds = context["ds"]
    logger.debug(f"write_audit_log called | run_id={run_id} | ds={ds}")

    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    logger.debug(f"Connecting to PostgreSQL | host={pg_host} | dbname=gold")

    conn = psycopg2.connect(
        host=pg_host,
        dbname="gold",
        user="airflow",
        password="airflow",
    )
    logger.debug("PostgreSQL connection established")

    cur = conn.cursor()
    values = (run_id, "all_layers", ds, "success")
    logger.debug(f"Inserting audit log row: {values}")

    cur.execute(
        """
        INSERT INTO pipeline_audit_log
            (run_id, layer, run_date, status, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        """,
        values,
    )
    conn.commit()
    logger.debug("Commit successful")
    cur.close()
    conn.close()
    logger.info(f"Audit log written for run_id={run_id}")
    logger.debug("PostgreSQL connection closed")


with DAG(
    dag_id="de_medallion_stock_pipeline",
    default_args=DEFAULT_ARGS,
    description="Bronze → Silver → Gold stock price ETL pipeline",
    schedule_interval="0 6 * * 1-5",  # Mon–Fri 6 AM UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "medallion", "finance", "stocks"],
    doc_md=__doc__,
) as dag:

    bronze_ingest = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze_ingest,
    )

    silver_transform = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver_transform,
    )

    # executes the SQL models and writes data into the tables (dim_date, dim_ticker, fact_stock_prices)
    dbt_gold_run = BashOperator(
        task_id="dbt_gold_run",
        bash_command=(
            "dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt "
            "--target dev"
        ),
    )

    # runs the test assertions from schema.yml and checks the data but writes nothing
    dbt_gold_test = BashOperator(
        task_id="dbt_gold_test",
        bash_command=(
            "dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt "
            "--target dev"
        ),
    )

    load_silver = PythonOperator(
        task_id="load_silver_to_postgres",
        python_callable=load_silver_to_postgres,
    )

    audit_log = PythonOperator(
        task_id="audit_log",
        python_callable=write_audit_log,
        trigger_rule="all_done",  # runs even if upstream fails, to log the outcome
    )

    (
        bronze_ingest
        >> silver_transform
        >> load_silver
        >> dbt_gold_run
        >> dbt_gold_test
        >> audit_log
    )
