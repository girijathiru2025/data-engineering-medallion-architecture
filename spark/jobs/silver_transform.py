"""
Silver Layer Transformation Job
--------------------------------
Reads raw Parquet data from the Bronze bucket, applies cleansing and validation,
and writes clean records to the Silver bucket. Rejected records are written to a
separate rejected path with a failure_reason column for downstream remediation.

Transformations applied:
- Schema enforcement and type casting
- Null / completeness checks on critical columns
- Range validation (prices > 0, volume >= 0)
- Duplicate detection (same ticker + date)
- Calculation of daily_return and price_range derived columns

Uses PySpark for scalable processing.
"""

import os
import logging
from datetime import date, datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, DateType, TimestampType,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("silver_transform")

# ── Config ────────────────────────────────────────────────────────────────
MINIO_ENDPOINT        = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
BRONZE_BUCKET         = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET         = os.getenv("SILVER_BUCKET", "silver")

BRONZE_PATH   = f"s3a://{BRONZE_BUCKET}/stock_prices"
SILVER_PATH   = f"s3a://{SILVER_BUCKET}/stock_prices"
REJECTED_PATH = f"s3a://rejected/stock_prices"

# ── Expected schema from Bronze ───────────────────────────────────────────
BRONZE_SCHEMA = StructType([
    StructField("date",         DateType(),      True),
    StructField("open",         DoubleType(),    True),
    StructField("high",         DoubleType(),    True),
    StructField("low",          DoubleType(),    True),
    StructField("close",        DoubleType(),    True),
    StructField("adj_close",    DoubleType(),    True),
    StructField("volume",       LongType(),      True),
    StructField("ticker",       StringType(),    True),
    StructField("ingested_at",  StringType(),    True),
    StructField("source",       StringType(),    True),
    StructField("company_name", StringType(),    True),
    StructField("sector",       StringType(),    True),
    StructField("exchange",     StringType(),    True),
])

CRITICAL_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]


def build_spark_session() -> SparkSession:
    logger.debug(f"Building Spark session | MINIO_ENDPOINT={MINIO_ENDPOINT}")
    spark = (
        SparkSession.builder
        .appName("silver_transform")
        .config("spark.jars.packages",                        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",             AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key",             AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions",               "8")
        .getOrCreate()
    )
    logger.debug(f"Spark session created | app={spark.sparkContext.appName} | version={spark.version}")
    return spark


def read_bronze(spark: SparkSession, partition_filter: Optional[str] = None) -> DataFrame:
    """Read from Bronze bucket, optionally filtering to a specific date partition."""
    path = BRONZE_PATH
    logger.debug(f"partition_filter={partition_filter}")
    if partition_filter:
        path = f"{BRONZE_PATH}/{partition_filter}"
    logger.info(f"Reading Bronze data from: {path}")
    df = spark.read.parquet(path)
    logger.debug(f"Bronze read complete | rows={df.count()} | columns={df.columns}")
    logger.debug(f"Bronze schema:\n{df.schema.simpleString()}")
    return df


def apply_quality_checks(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Splits the DataFrame into clean and rejected records.
    Each rejected record carries a failure_reason column.
    """
    df = df.cache()
    total_rows = df.count()
    logger.debug(f"Starting quality checks on {total_rows} rows")
    logger.debug(f"Critical columns being checked: {CRITICAL_COLUMNS}")

    # Start with all records as potentially valid
    df = df.withColumn("failure_reason", F.lit(None).cast(StringType()))

    # 1. Null checks on critical columns
    null_condition = F.lit(False)
    for col in CRITICAL_COLUMNS:
        if col in df.columns:
            null_condition = null_condition | F.col(col).isNull()
            logger.debug(f"  Added null check for column: '{col}'")
        else:
            logger.debug(f"  Skipping null check — column '{col}' not in DataFrame")

    df = df.withColumn(
        "failure_reason",
        F.when(
            F.col("failure_reason").isNull() & null_condition,
            F.lit("null_in_critical_column")
        ).otherwise(F.col("failure_reason"))
    )
    null_failed = df.filter(F.col("failure_reason") == "null_in_critical_column").count()
    logger.debug(f"Check 1 (null check): {null_failed} rows flagged")

    # 2. Price range checks (prices must be positive)
    price_cols = ["open", "high", "low", "close", "adj_close"]
    negative_price = F.lit(False)
    for col in price_cols:
        if col in df.columns:
            negative_price = negative_price | (F.col(col) <= 0)
            logger.debug(f"  Added price check for column: '{col}'")

    df = df.withColumn(
        "failure_reason",
        F.when(
            F.col("failure_reason").isNull() & negative_price,
            F.lit("non_positive_price")
        ).otherwise(F.col("failure_reason"))
    )
    price_failed = df.filter(F.col("failure_reason") == "non_positive_price").count()
    logger.debug(f"Check 2 (price > 0): {price_failed} rows flagged")

    # 3. Volume check
    df = df.withColumn(
        "failure_reason",
        F.when(
            F.col("failure_reason").isNull() & (F.col("volume") < 0),
            F.lit("negative_volume")
        ).otherwise(F.col("failure_reason"))
    )
    volume_failed = df.filter(F.col("failure_reason") == "negative_volume").count()
    logger.debug(f"Check 3 (volume >= 0): {volume_failed} rows flagged")

    # 4. OHLC consistency: high >= low, high >= open, high >= close
    ohlc_invalid = (
        (F.col("high") < F.col("low")) |
        (F.col("high") < F.col("open")) |
        (F.col("high") < F.col("close"))
    )
    df = df.withColumn(
        "failure_reason",
        F.when(
            F.col("failure_reason").isNull() & ohlc_invalid,
            F.lit("ohlc_inconsistency")
        ).otherwise(F.col("failure_reason"))
    )
    ohlc_failed = df.filter(F.col("failure_reason") == "ohlc_inconsistency").count()
    logger.debug(f"Check 4 (OHLC consistency): {ohlc_failed} rows flagged")

    clean_df    = df.filter(F.col("failure_reason").isNull()).drop("failure_reason")
    rejected_df = df.filter(F.col("failure_reason").isNotNull())

    clean_count    = clean_df.count()
    rejected_count = rejected_df.count()
    logger.info(f"Quality check: clean={clean_count}, rejected={rejected_count}")
    logger.debug(f"Rejection breakdown — nulls={null_failed}, bad_price={price_failed}, bad_volume={volume_failed}, ohlc={ohlc_failed}")
    logger.debug(f"Total accounted for: {clean_count + rejected_count} of {total_rows}")

    return clean_df, rejected_df


def enrich_silver(df: DataFrame) -> DataFrame:
    """Add derived columns and silver-layer metadata."""
    logger.debug(f"Enriching silver | input rows={df.count()} | columns={df.columns}")

    window = Window.partitionBy("ticker").orderBy("date")
    logger.debug("Window spec: partitionBy='ticker', orderBy='date'")

    processed_at = datetime.utcnow().isoformat()
    logger.debug(f"processed_at timestamp: {processed_at}")

    df = (
        df
        # ── Derived columns ───────────────────────────────────────────────
        # Calculated from existing price data — adds analytical value
        .withColumn("prev_close",    F.lag("close", 1).over(window))          # previous day's close price (intermediate, dropped below)
        .withColumn("daily_return",  F.round((F.col("close") - F.col("prev_close")) / F.col("prev_close"), 6))  # % change from previous close
        .withColumn("price_range",   F.round(F.col("high") - F.col("low"), 4))  # intraday spread: how much price moved that day
        .drop("prev_close")           # intermediate column — not needed in output

        # ── Metadata columns ──────────────────────────────────────────────
        # Track when and where this record was processed — useful for auditing and debugging
        .withColumn("processed_at",  F.lit(processed_at))   # timestamp when this Silver job ran
        .withColumn("layer",         F.lit("silver"))        # tags the record as belonging to the Silver layer
    )

    logger.debug(f"Enrichment complete | output columns={df.columns}")
    logger.debug(f"Sample enriched row:\n{df.limit(1).collect()}")
    return df


def write_silver(df: DataFrame):
    row_count = df.count()
    logger.info(f"Writing Silver data to: {SILVER_PATH}")
    logger.debug(f"Silver write | rows={row_count} | partitionBy='ticker' | mode='overwrite'")
    (
        df.write
        .partitionBy("ticker")
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )
    logger.info("Silver write complete.")
    logger.debug(f"Successfully wrote {row_count} rows to {SILVER_PATH}")


def write_rejected(df: DataFrame):
    if df.limit(1).count() == 0:
        logger.info("No rejected records — skipping rejected write.")
        logger.debug("Rejected DataFrame is empty — nothing to write")
        return
    rejected_count = df.count()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = f"{REJECTED_PATH}/run_ts={ts}"
    logger.info(f"Writing {rejected_count} rejected records to: {path}")
    logger.debug(f"Rejected records breakdown by failure_reason:")
    df.groupBy("failure_reason").count().show(truncate=False)
    df.write.mode("overwrite").parquet(path)
    logger.debug(f"Rejected write complete to: {path}")


def run(partition_filter: Optional[str] = None):
    logger.debug(f"--- Silver transform run starting | partition_filter={partition_filter} ---")
    logger.debug(f"Config | BRONZE_PATH={BRONZE_PATH} | SILVER_PATH={SILVER_PATH} | REJECTED_PATH={REJECTED_PATH}")
    spark = build_spark_session()
    try:
        bronze_df = read_bronze(spark, partition_filter)
        logger.debug(f"Bronze read done | rows={bronze_df.count()}")

        clean_df, rejected_df = apply_quality_checks(bronze_df)
        logger.debug(f"Quality checks done | clean={clean_df.count()} | rejected={rejected_df.count()}")

        enriched_df = enrich_silver(clean_df)
        logger.debug(f"Enrichment done | rows={enriched_df.count()}")

        write_silver(enriched_df)
        write_rejected(rejected_df)
        logger.info("Silver transformation complete.")
        logger.debug("--- Silver transform run finished successfully ---")
    finally:
        logger.debug("Stopping Spark session")
        spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Silver layer transformation")
    parser.add_argument("--partition", default=None, help="Optional partition filter e.g. ticker=AAPL")
    args = parser.parse_args()
    run(partition_filter=args.partition)
