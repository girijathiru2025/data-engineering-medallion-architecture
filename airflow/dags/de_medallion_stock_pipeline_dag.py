"""
DE Medallion Stock Pipeline DAG
-----------------------
Orchestrates the full Bronze → Silver → Gold ETL pipeline for stock price data.

Schedule: Daily at 6 AM UTC (after US markets close + data propagation window)
Catchup:  False (run latest only)

Tasks:
  1. bronze_ingest      — fetch raw data from Yahoo Finance → MinIO Bronze bucket
  2. silver_transform   — PySpark cleanse/validate → MinIO Silver bucket
  3. dbt_gold_run       — dbt models → PostgreSQL Gold star schema
  4. dbt_gold_test      — dbt data quality tests on Gold layer
  5. audit_log          — write pipeline run metrics to pipeline_audit_log table

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
    run_id = context["run_id"]
    logger.debug(
        f"run_bronze_ingest called | run_id={run_id} | execution_date={execution_date}"
    )
    logger.debug(f"Tickers to ingest: {TICKERS}")
    logger.info(f"Starting Bronze ingestion for date: {execution_date}")
    run(
        tickers=TICKERS,
        start_date=execution_date,
        end_date=execution_date,
    )
    logger.info("Bronze ingestion complete.")
    logger.debug(f"run_bronze_ingest finished | run_id={run_id}")


def run_silver_transform(**context):
    """Submit PySpark silver transformation job."""
    import subprocess

    run_id = context["run_id"]
    logger.debug(f"run_silver_transform called | run_id={run_id}")

    cmd = [
        "spark-submit",
        "--master",
        "local[*]",
        "--packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "/opt/airflow/spark/jobs/silver_transform.py",
    ]
    logger.debug(f"spark-submit command: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=1800,
    )
    logger.debug(f"spark-submit returncode={result.returncode}")
    logger.debug(f"spark-submit stdout:\n{result.stdout}")

    if result.returncode != 0:
        logger.error(f"Silver transform stderr:\n{result.stderr}")
        raise RuntimeError("Silver PySpark job failed.")
    logger.info("Silver transformation complete.")
    logger.debug(f"run_silver_transform finished | run_id={run_id}")


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

    dbt_gold_run = BashOperator(
        task_id="dbt_gold_run",
        bash_command=(
            "cd /opt/airflow && "
            "dbt run --project-dir spark/../dbt --profiles-dir spark/../dbt "
            "--target dev"
        ),
    )

    dbt_gold_test = BashOperator(
        task_id="dbt_gold_test",
        bash_command=(
            "cd /opt/airflow && "
            "dbt test --project-dir spark/../dbt --profiles-dir spark/../dbt "
            "--target dev"
        ),
    )

    audit_log = PythonOperator(
        task_id="audit_log",
        python_callable=write_audit_log,
        trigger_rule="all_done",  # runs even if upstream fails, to log the outcome
    )

    bronze_ingest >> silver_transform >> dbt_gold_run >> dbt_gold_test >> audit_log
