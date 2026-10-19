"""Load landed API records into bronze Delta tables."""

from __future__ import annotations

from pyspark.sql.types import DoubleType, StringType, StructField, StructType, LongType

from databricks.common import latest, table_name, upsert

# Explicit schemas keep Spark from changing column types between batches.
MARKET_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("trade_date", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
    StructField("fetched_at", StringType()),
])
FRED_SCHEMA = StructType([
    StructField("series_id", StringType()),
    StructField("observation_date", StringType()),
    StructField("value", DoubleType()),
    StructField("realtime_start", StringType()),
    StructField("realtime_end", StringType()),
    StructField("fetched_at", StringType()),
])


def run(spark, landing_root: str, schema: str) -> None:
    """Read both landing folders and retain the latest record per source key."""
    market = spark.read.schema(MARKET_SCHEMA).json(f"{landing_root}/market/*.jsonl")
    fred = spark.read.schema(FRED_SCHEMA).json(f"{landing_root}/fred/*.jsonl")
    upsert(spark, latest(market, ["symbol", "trade_date"]),
           table_name(schema, "bronze_market"), ["symbol", "trade_date"])
    upsert(spark, latest(fred, ["series_id", "observation_date"]),
           table_name(schema, "bronze_fred"), ["series_id", "observation_date"])
