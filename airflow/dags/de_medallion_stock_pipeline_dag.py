"""
DE Medallion Stock Pipeline DAG
-----------------------

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

    bronze_ingest
