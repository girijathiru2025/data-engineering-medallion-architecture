"""
Unit tests for Silver layer data quality checks.
Uses PySpark local mode — no cluster or MinIO needed.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType
from datetime import date


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("test_silver_quality")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


SCHEMA = StructType([
    StructField("date",      DateType(),   True),
    StructField("open",      DoubleType(), True),
    StructField("high",      DoubleType(), True),
    StructField("low",       DoubleType(), True),
    StructField("close",     DoubleType(), True),
    StructField("adj_close", DoubleType(), True),
    StructField("volume",    LongType(),   True),
    StructField("ticker",    StringType(), True),
])


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)


def apply_checks(df):
    """Import and apply checks from the silver job."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark", "jobs"))
    from silver_transform import apply_quality_checks
    return apply_quality_checks(df)


class TestNullChecks:
    def test_null_ticker_is_rejected(self, spark):
        rows = [(date(2024, 1, 2), 150.0, 155.0, 149.0, 152.0, 152.0, 1000000, None)]
        df = make_df(spark, rows)
        clean, rejected = apply_checks(df)
        assert clean.count() == 0
        assert rejected.count() == 1
        assert rejected.collect()[0]["failure_reason"] == "null_in_critical_column"

    def test_null_close_is_rejected(self, spark):
        rows = [(date(2024, 1, 2), 150.0, 155.0, 149.0, None, None, 1000000, "AAPL")]
        df = make_df(spark, rows)
        clean, rejected = apply_checks(df)
        assert clean.count() == 0
        assert rejected.count() == 1


class TestPriceRangeChecks:
    def test_negative_price_is_rejected(self, spark):
        rows = [(date(2024, 1, 2), -1.0, 155.0, 149.0, 152.0, 152.0, 1000000, "AAPL")]
        df = make_df(spark, rows)
        clean, rejected = apply_checks(df)
        assert clean.count() == 0
        assert rejected.collect()[0]["failure_reason"] == "non_positive_price"

    def test_zero_close_is_rejected(self, spark):
        rows = [(date(2024, 1, 2), 150.0, 155.0, 149.0, 0.0, 0.0, 1000000, "AAPL")]
        df = make_df(spark, rows)
        clean, rejected = apply_checks(df)
        assert clean.count() == 0


class TestOHLCConsistency:
    def test_high_less_than_low_is_rejected(self, spark):
        rows = [(date(2024, 1, 2), 150.0, 140.0, 149.0, 152.0, 152.0, 1000000, "AAPL")]
        df = make_df(spark, rows)
        clean, rejected = apply_checks(df)
        assert clean.count() == 0
        assert rejected.collect()[0]["failure_reason"] == "ohlc_inconsistency"


class TestCleanPassthrough:
    def test_valid_record_passes(self, spark):
        rows = [(date(2024, 1, 2), 150.0, 155.0, 149.0, 152.0, 151.5, 5000000, "AAPL")]
        df = make_df(spark, rows)
        clean, rejected = apply_checks(df)
        assert clean.count() == 1
        assert rejected.count() == 0
