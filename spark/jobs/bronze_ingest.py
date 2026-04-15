"""
Bronze Layer Ingestion Job
--------------------------
Fetches raw stock price data from Yahoo Finance (yfinance) for a list of tickers
and writes it as partitioned Parquet files to the Bronze bucket (MinIO/S3).

Design principles:
- Append-only: existing partitions are never overwritten (idempotent on re-run for same date)
- No transformation: raw data is stored as-is to preserve source fidelity
- Partitioned by: ticker / year / month / day
"""

import os
import sys
import logging
from datetime import datetime, date
from typing import List

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("bronze_ingest")

# ── Config from environment ───────────────────────────────────────────────
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
AWS_ACCESS_KEY_ID   = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
BRONZE_BUCKET       = os.getenv("BRONZE_BUCKET", "bronze")

DEFAULT_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "NVDA", "TSLA", "JPM", "JNJ", "V",
]

TICKER_METADATA = {
    "AAPL":  {"company_name": "Apple Inc.",            "sector": "Technology",        "exchange": "NASDAQ"},
    "MSFT":  {"company_name": "Microsoft Corporation", "sector": "Technology",        "exchange": "NASDAQ"},
    "GOOGL": {"company_name": "Alphabet Inc.",         "sector": "Technology",        "exchange": "NASDAQ"},
    "AMZN":  {"company_name": "Amazon.com Inc.",       "sector": "Consumer Cyclical", "exchange": "NASDAQ"},
    "META":  {"company_name": "Meta Platforms Inc.",   "sector": "Technology",        "exchange": "NASDAQ"},
    "NVDA":  {"company_name": "NVIDIA Corporation",    "sector": "Technology",        "exchange": "NASDAQ"},
    "TSLA":  {"company_name": "Tesla Inc.",            "sector": "Consumer Cyclical", "exchange": "NASDAQ"},
    "JPM":   {"company_name": "JPMorgan Chase & Co.",  "sector": "Financial Services","exchange": "NYSE"},
    "JNJ":   {"company_name": "Johnson & Johnson",     "sector": "Healthcare",        "exchange": "NYSE"},
    "V":     {"company_name": "Visa Inc.",             "sector": "Financial Services","exchange": "NYSE"},
}


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def fetch_ticker_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Download OHLCV data from Yahoo Finance for a single ticker."""
    logger.info(f"Fetching {ticker} from {start_date} to {end_date}")
    df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)
    logger.debug(f"[{ticker}] Raw download shape: {df.shape}")
    logger.debug(f"[{ticker}] Raw columns: {list(df.columns)}")
    logger.debug(f"[{ticker}] Column type: {type(df.columns)}")

    if df.empty:
        logger.warning(f"No data returned for {ticker}")
        return pd.DataFrame()

    df = df.reset_index()
    logger.debug(f"[{ticker}] After reset_index — columns: {list(df.columns)}")

    # Flatten MultiIndex columns if present (yfinance >= 0.2 quirk)
    if isinstance(df.columns, pd.MultiIndex):
        logger.debug(f"[{ticker}] MultiIndex detected — flattening columns")
        flat_columns = []
        for col in df.columns:
            field_name = col[0]  # e.g. "Open", "Close" — always take the first level
            logger.debug(f"[{ticker}]   col tuple={col}  →  keeping '{field_name}'")
            flat_columns.append(field_name)
        df.columns = flat_columns
        logger.debug(f"[{ticker}] After flatten — columns: {flat_columns}")
    else:
        logger.debug(f"[{ticker}] No MultiIndex — columns unchanged")

    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    logger.debug(f"[{ticker}] After lowercasing — columns: {list(df.columns)}")

    df["ticker"]       = ticker
    df["ingested_at"]  = datetime.utcnow().isoformat()
    df["source"]       = "yahoo_finance"

    meta = TICKER_METADATA.get(ticker, {})
    logger.debug(f"[{ticker}] Metadata looked up: {meta}")
    df["company_name"] = meta.get("company_name", "")
    df["sector"]       = meta.get("sector", "")
    df["exchange"]     = meta.get("exchange", "")

    logger.debug(f"[{ticker}] Final DataFrame shape: {df.shape}")
    logger.debug(f"[{ticker}] Final columns: {list(df.columns)}")
    logger.debug(f"[{ticker}] First row:\n{df.iloc[0].to_dict()}")

    return df


def write_to_bronze(s3_client, df: pd.DataFrame, ticker: str):
    """Write DataFrame as Parquet to Bronze bucket, partitioned by ticker/year/month/day."""
    if df.empty:
        logger.debug(f"[{ticker}] DataFrame is empty — nothing to write")
        return

    unique_dates = df["date"].dt.date.unique()
    logger.debug(f"[{ticker}] Total rows to write: {len(df)} across {len(unique_dates)} date(s): {unique_dates}")

    for _, day_df in df.groupby(df["date"].dt.date):
        day_val: date = day_df["date"].iloc[0].date()
        logger.debug(f"[{ticker}] Processing partition for date: {day_val} ({len(day_df)} rows)")

        s3_key = (
            f"stock_prices/ticker={ticker}/"
            f"year={day_val.year}/"
            f"month={day_val.month:02d}/"
            f"day={day_val.day:02d}/"
            f"data.parquet"
        )
        logger.debug(f"[{ticker}] S3 key: {s3_key}")

        # Idempotency: skip if partition already exists
        try:
            s3_client.head_object(Bucket=BRONZE_BUCKET, Key=s3_key)
            logger.info(f"Partition already exists, skipping: s3://{BRONZE_BUCKET}/{s3_key}")
            continue
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

        table = pa.Table.from_pandas(day_df, preserve_index=False)
        logger.debug(f"[{ticker}] PyArrow table schema: {table.schema}")

        buf = pa.BufferOutputStream()
        pq.write_table(table, buf)
        parquet_bytes = buf.getvalue().to_pybytes()
        logger.debug(f"[{ticker}] Parquet buffer size: {len(parquet_bytes)} bytes")

        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=s3_key,
            Body=parquet_bytes,
            ContentType="application/octet-stream",
        )
        logger.info(f"Written {len(day_df)} rows -> s3://{BRONZE_BUCKET}/{s3_key}")


def run(
    tickers: List[str] = None,
    start_date: str = "2020-01-01",
    end_date: str = None,
):
    if tickers is None:
        tickers = DEFAULT_TICKERS
    if end_date is None:
        end_date = date.today().isoformat()

    logger.info(f"Bronze ingestion starting | tickers={tickers} | {start_date} to {end_date}")
    logger.debug(f"Config | MINIO_ENDPOINT={MINIO_ENDPOINT} | BRONZE_BUCKET={BRONZE_BUCKET}")

    s3 = get_s3_client()
    logger.debug("S3 client created successfully")

    success, failed = [], []
    for ticker in tickers:
        logger.debug(f"--- Starting ticker: {ticker} ---")
        try:
            df = fetch_ticker_data(ticker, start_date, end_date)
            write_to_bronze(s3, df, ticker)
            success.append(ticker)
            logger.debug(f"--- Finished ticker: {ticker} ✓ ---")
        except Exception as exc:
            logger.error(f"Failed to ingest {ticker}: {exc}", exc_info=True)
            failed.append(ticker)

    logger.info(f"Bronze ingestion complete | success={success} | failed={failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bronze layer stock data ingestion")
    parser.add_argument("--tickers",    nargs="+", default=None)
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date",   default=None)
    args = parser.parse_args()

    run(tickers=args.tickers, start_date=args.start_date, end_date=args.end_date)
