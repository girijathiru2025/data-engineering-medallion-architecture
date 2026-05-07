"""
Unit tests for bronze_ingest.py.
All external I/O (yfinance, boto3/S3) is mocked — no network or MinIO needed.
"""

import sys
import os
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "jobs"))
import bronze_ingest


# ── helpers ────────────────────────────────────────────────────────────────


def _raw_multiindex(tickers):
    """Mimic yfinance batch download: MultiIndex columns (ticker, field)."""
    fields = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
    cols = pd.MultiIndex.from_tuples([(t, f) for t in tickers for f in fields])
    row = [150.0, 155.0, 149.0, 152.0, 151.5, 5_000_000] * len(tickers)
    df = pd.DataFrame([row], index=pd.to_datetime(["2024-01-02"]), columns=cols)
    df.index.name = "Date"
    return df


def _ticker_df(ticker="AAPL", dates=None):
    """Minimal valid DataFrame for write_to_bronze (date column as datetime)."""
    dates = dates or ["2024-01-02"]
    n = len(dates)
    return pd.DataFrame(
        {
            "date": pd.to_datetime(dates),
            "open": [150.0] * n,
            "high": [155.0] * n,
            "low": [149.0] * n,
            "close": [152.0] * n,
            "adj_close": [151.5] * n,
            "volume": [5_000_000] * n,
            "ticker": [ticker] * n,
            "ingested_at": ["2024-01-02T00:00:00"] * n,
            "source": ["yahoo_finance"] * n,
            "company_name": ["Apple Inc."] * n,
            "sector": ["Technology"] * n,
            "exchange": ["NASDAQ"] * n,
        }
    )


def _s3_404():
    """S3 mock where head_object always raises 404 (partition does not exist yet)."""
    s3 = MagicMock()
    s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
    )
    return s3


# ── fetch_all_tickers ──────────────────────────────────────────────────────


class TestFetchAllTickers:
    @patch("bronze_ingest.yf.download")
    def test_valid_ticker_returns_non_empty_dataframe(self, mock_dl):
        mock_dl.return_value = _raw_multiindex(["AAPL"])
        result = bronze_ingest.fetch_all_tickers(["AAPL"], "2024-01-02", "2024-01-03")
        assert not result["AAPL"].empty

    @patch("bronze_ingest.yf.download")
    def test_metadata_columns_are_populated(self, mock_dl):
        mock_dl.return_value = _raw_multiindex(["AAPL"])
        result = bronze_ingest.fetch_all_tickers(["AAPL"], "2024-01-02", "2024-01-03")
        df = result["AAPL"]
        assert df["company_name"].iloc[0] == "Apple Inc."
        assert df["sector"].iloc[0] == "Technology"
        assert df["source"].iloc[0] == "yahoo_finance"
        assert df["ticker"].iloc[0] == "AAPL"

    @patch("bronze_ingest.yf.download")
    def test_all_null_prices_returns_empty_dataframe(self, mock_dl):
        fields = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
        cols = pd.MultiIndex.from_tuples([("FAKE", f) for f in fields])
        raw = pd.DataFrame(
            [[None] * 6], index=pd.to_datetime(["2024-01-02"]), columns=cols
        )
        raw.index.name = "Date"
        mock_dl.return_value = raw
        result = bronze_ingest.fetch_all_tickers(["FAKE"], "2024-01-02", "2024-01-03")
        assert result["FAKE"].empty

    @patch("bronze_ingest.yf.download")
    def test_multiple_tickers_all_returned(self, mock_dl):
        tickers = ["AAPL", "MSFT"]
        mock_dl.return_value = _raw_multiindex(tickers)
        result = bronze_ingest.fetch_all_tickers(tickers, "2024-01-02", "2024-01-03")
        assert set(result.keys()) == {"AAPL", "MSFT"}
        assert not result["AAPL"].empty
        assert not result["MSFT"].empty


# ── write_to_bronze ────────────────────────────────────────────────────────


class TestWriteToBronze:
    def test_writes_parquet_to_expected_s3_key(self):
        s3 = _s3_404()
        bronze_ingest.write_to_bronze(s3, _ticker_df("AAPL"), "AAPL")
        s3.put_object.assert_called_once()
        key = s3.put_object.call_args[1]["Key"]
        assert key == "stock_prices/ticker=AAPL/year=2024/month=01/day=02/data.parquet"

    def test_skips_write_when_partition_already_exists(self):
        s3 = MagicMock()
        s3.head_object.return_value = {"ContentLength": 1024}  # object exists → no 404
        bronze_ingest.write_to_bronze(s3, _ticker_df("AAPL"), "AAPL")
        s3.put_object.assert_not_called()

    def test_empty_dataframe_writes_nothing(self):
        s3 = MagicMock()
        bronze_ingest.write_to_bronze(s3, pd.DataFrame(), "AAPL")
        s3.head_object.assert_not_called()
        s3.put_object.assert_not_called()

    def test_two_dates_produce_two_s3_writes(self):
        s3 = _s3_404()
        df = _ticker_df("AAPL", dates=["2024-01-02", "2024-01-03"])
        bronze_ingest.write_to_bronze(s3, df, "AAPL")
        assert s3.put_object.call_count == 2
