"""Shared Delta helpers for repeatable batch runs."""

from __future__ import annotations

import re

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window, functions as F


def table_name(schema: str, name: str) -> str:
    """Build a table name after checking the configured SQL identifier."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*(\.[A-Za-z_][A-Za-z_0-9]*){0,2}", schema):
        raise ValueError("Schema must be a one- to three-part SQL identifier")
    return f"{schema}.{name}"


def latest(df: DataFrame, keys: list[str]) -> DataFrame:
    """Keep the newest source record for each natural key."""
    # Multiple landing files may contain the same symbol/date or series/date.
    order = Window.partitionBy(*keys).orderBy(F.col("fetched_at").desc())
    return df.withColumn("_rank", F.row_number().over(order)).where("_rank = 1").drop("_rank")


def upsert(spark, df: DataFrame, name: str, keys: list[str]) -> None:
    """Create a Delta table or update rows by natural key."""
    if not spark.catalog.tableExists(name):
        df.write.format("delta").mode("overwrite").saveAsTable(name)
        return
    # MERGE makes repeated batch runs update existing records instead of adding copies.
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    (DeltaTable.forName(spark, name).alias("target")
     .merge(df.alias("source"), condition)
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
