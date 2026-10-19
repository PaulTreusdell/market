"""Calculate daily returns and a trailing-history unusual-price flag."""

from __future__ import annotations

from pyspark.sql import Window, functions as F

from databricks.common import table_name

LOOKBACK_DAYS = 20
MIN_HISTORY_DAYS = 10
MIN_RETURN_THRESHOLD = 0.05
VOLATILITY_MULTIPLIER = 4.0


def market_anomalies(prices):
    """Compare each daily return with the symbol's preceding 20 returns."""
    by_date = Window.partitionBy("symbol").orderBy("trade_date")
    # End at -1 so today's jump never raises the baseline used to judge itself.
    history = by_date.rowsBetween(-LOOKBACK_DAYS, -1)
    with_returns = prices.withColumn("previous_close", F.lag("close").over(by_date))
    with_returns = with_returns.withColumn(
        "daily_return", F.col("close") / F.col("previous_close") - F.lit(1.0)
    )
    with_baseline = (with_returns
        .withColumn("prior_days", F.count("daily_return").over(history))
        .withColumn("baseline_volatility", F.stddev_samp("daily_return").over(history))
        .withColumn(
            "threshold",
            # Require a 5% move at minimum; volatile symbols need a larger move.
            F.greatest(F.lit(MIN_RETURN_THRESHOLD),
                       F.lit(VOLATILITY_MULTIPLIER) * F.coalesce("baseline_volatility", F.lit(0.0))),
        ))
    return with_baseline.select(
        "symbol", "trade_date", "close", "previous_close", "daily_return",
        "prior_days", "baseline_volatility", "threshold",
        # Early rows lack enough history and receive False, with prior_days visible.
        (F.coalesce(
            (F.col("prior_days") >= MIN_HISTORY_DAYS) &
            (F.abs("daily_return") > F.col("threshold")),
            F.lit(False),
        )).alias("is_unusual"),
    )


def run(spark, schema: str):
    """Refresh the gold table and return the two PostgreSQL datasets."""
    result = market_anomalies(spark.table(table_name(schema, "silver_market")))
    # A full refresh also corrects later rolling values after a historical edit.
    (result.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(table_name(schema, "gold_market_anomalies")))

    fred = spark.table(table_name(schema, "silver_fred")).select(
        "series_id", "observation_date", "value", "realtime_start", "realtime_end")
    return spark.table(table_name(schema, "gold_market_anomalies")), fred
