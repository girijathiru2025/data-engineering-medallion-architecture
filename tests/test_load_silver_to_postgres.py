"""
Unit tests for load_silver_to_postgres() in the Airflow DAG.
Airflow modules are stubbed so this runs in CI without apache-airflow installed.
boto3, psycopg2, and pyarrow I/O are all mocked.
"""

import sys
import os
import io
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date
from unittest.mock import MagicMock, patch

# Stub Airflow before importing the DAG file so CI doesn't need it installed.
for _mod in [
    "airflow",
    "airflow.models",
    "airflow.operators",
    "airflow.operators.python",
    "airflow.operators.bash",
    "airflow.utils",
    "airflow.utils.dates",
]:
    sys.modules.setdefault(_mod, MagicMock())

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))
from de_medallion_stock_pipeline_dag import load_silver_to_postgres


# ── helpers ────────────────────────────────────────────────────────────────


def _arrow_table(ticker="AAPL"):
    return pa.table(
        {
            "date": pa.array([date(2024, 1, 2)]),
            "open": pa.array([150.0]),
            "high": pa.array([155.0]),
            "low": pa.array([149.0]),
            "close": pa.array([152.0]),
            "adj_close": pa.array([151.5]),
            "volume": pa.array([5_000_000], type=pa.int64()),
            "ticker": pa.array([ticker]),
            "ingested_at": pa.array(["2024-01-02T00:00:00"]),
            "source": pa.array(["yahoo_finance"]),
            "company_name": pa.array(["Apple Inc."]),
            "sector": pa.array(["Technology"]),
            "exchange": pa.array(["NASDAQ"]),
            "daily_return": pa.array([0.01]),
            "price_range": pa.array([6.0]),
            "processed_at": pa.array(["2024-01-02T00:00:00"]),
            "layer": pa.array(["silver"]),
        }
    )


def _to_parquet_bytes(table):
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _build_s3_mock(key_table_pairs):
    """Return an s3 mock that serves parquet bytes for the given (key, table) pairs."""
    s3 = MagicMock()
    paginator = MagicMock()
    contents = [{"Key": k} for k, _ in key_table_pairs]
    paginator.paginate.return_value = [{"Contents": contents}]
    s3.get_paginator.return_value = paginator

    parquet_map = {k: _to_parquet_bytes(t) for k, t in key_table_pairs}

    def _get_object(Bucket, Key):
        body = MagicMock()
        body.read.return_value = parquet_map[Key]
        return {"Body": body}

    s3.get_object.side_effect = _get_object
    return s3


def _build_pg_mock():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def _ctx():
    return {"run_id": "test_run_001", "ds": "2024-01-02"}


# ── tests ──────────────────────────────────────────────────────────────────


class TestLoadSilverToPostgres:
    @patch("psycopg2.extras.execute_values")
    @patch("psycopg2.connect")
    @patch("boto3.client")
    def test_happy_path_commits_rows(self, mock_boto, mock_pg_connect, mock_exec_vals):
        key = "stock_prices/ticker=AAPL/year=2024/month=01/day=02/data.parquet"
        mock_boto.return_value = _build_s3_mock([(key, _arrow_table("AAPL"))])
        conn, _ = _build_pg_mock()
        mock_pg_connect.return_value = conn

        load_silver_to_postgres(**_ctx())

        mock_exec_vals.assert_called_once()
        conn.commit.assert_called_once()

    @patch("psycopg2.extras.execute_values")
    @patch("psycopg2.connect")
    @patch("boto3.client")
    def test_empty_bucket_raises_runtime_error(self, mock_boto, mock_pg_connect, mock_exec_vals):
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{}]  # no Contents key
        s3.get_paginator.return_value = paginator
        mock_boto.return_value = s3

        with pytest.raises(RuntimeError, match="No silver Parquet files found"):
            load_silver_to_postgres(**_ctx())

    @patch("psycopg2.extras.execute_values")
    @patch("psycopg2.connect")
    @patch("boto3.client")
    def test_ticker_extracted_from_key_path_when_column_missing(
        self, mock_boto, mock_pg_connect, mock_exec_vals
    ):
        # Parquet has no ticker column — must be derived from the S3 key
        table_no_ticker = pa.table(
            {"date": pa.array([date(2024, 1, 2)]), "close": pa.array([152.0])}
        )
        key = "stock_prices/ticker=MSFT/year=2024/month=01/day=02/data.parquet"
        mock_boto.return_value = _build_s3_mock([(key, table_no_ticker)])
        conn, _ = _build_pg_mock()
        mock_pg_connect.return_value = conn

        load_silver_to_postgres(**_ctx())

        insert_sql = mock_exec_vals.call_args[0][1]
        rows = mock_exec_vals.call_args[0][2]
        assert "ticker" in insert_sql
        assert any("MSFT" in str(row) for row in rows)

    @patch("psycopg2.extras.execute_values")
    @patch("psycopg2.connect")
    @patch("boto3.client")
    def test_connection_closed_even_on_db_exception(
        self, mock_boto, mock_pg_connect, mock_exec_vals
    ):
        key = "stock_prices/ticker=AAPL/year=2024/month=01/day=02/data.parquet"
        mock_boto.return_value = _build_s3_mock([(key, _arrow_table("AAPL"))])
        conn, cur = _build_pg_mock()
        cur.execute.side_effect = Exception("DB error")
        mock_pg_connect.return_value = conn

        with pytest.raises(Exception, match="DB error"):
            load_silver_to_postgres(**_ctx())

        conn.close.assert_called_once()
