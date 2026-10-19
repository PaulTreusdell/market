"""Validate raw observations and create typed silver tables."""

from __future__ import annotations

from pyspark.sql import functions as F

from databricks.common import table_name, upsert


def market_prices(df):
    """Type dates and reject invalid or internally inconsistent price rows."""
    return (df.select(
        F.upper(F.trim("symbol")).alias("symbol"),
        F.to_date("trade_date").alias("trade_date"),
        "open", "high", "low", "close", "volume",
        F.to_timestamp("fetched_at").alias("fetched_at"),
    ).where(
        F.col("symbol").isNotNull() & (F.col("symbol") != "") &
        F.col("trade_date").isNotNull() &
        (F.col("open") > 0) & (F.col("high") > 0) &
        (F.col("low") > 0) & (F.col("close") > 0) &
        (F.col("volume") >= 0) &
        (F.col("high") >= F.greatest("open", "close", "low")) &
        (F.col("low") <= F.least("open", "close", "high"))
    ))


def economic_observations(df):
    """Type FRED dates and keep observations with a usable value."""
    return (df.select(
        F.trim("series_id").alias("series_id"),
        F.to_date("observation_date").alias("observation_date"),
        "value",
        F.to_date("realtime_start").alias("realtime_start"),
        F.to_date("realtime_end").alias("realtime_end"),
        F.to_timestamp("fetched_at").alias("fetched_at"),
    ).where(
        F.col("series_id").isNotNull() & (F.col("series_id") != "") &
        F.col("observation_date").isNotNull() & F.col("value").isNotNull()
    ))


def run(spark, schema: str) -> None:
    """Transform bronze records and merge valid rows into silver tables."""
    market = market_prices(spark.table(table_name(schema, "bronze_market")))
    fred = economic_observations(spark.table(table_name(schema, "bronze_fred")))
    upsert(spark, market, table_name(schema, "silver_market"), ["symbol", "trade_date"])
    upsert(spark, fred, table_name(schema, "silver_fred"), ["series_id", "observation_date"])
