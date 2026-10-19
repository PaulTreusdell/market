"""Run bronze, silver and gold in a Databricks Python task or notebook."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession

from databricks import bronze, gold, silver
from databricks.common import table_name


def main() -> None:
    """Run the medallion stages in dependency order for one batch."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing-root", required=True, help="Cloud/volume URI with market and fred folders")
    parser.add_argument("--schema", default="financial_market", help="Existing catalog.schema or schema")
    parser.add_argument("--skip-postgres", action="store_true",
                        help="Build Delta tables without loading PostgreSQL")
    args = parser.parse_args()
    dsn = os.environ.get("POSTGRES_DSN") if not args.skip_postgres else None
    if not args.skip_postgres and not dsn:
        parser.error("Set POSTGRES_DSN in the Databricks job environment")
    spark = SparkSession.builder.getOrCreate()
    table_name(args.schema, "bronze_market")  # Validate configured identifier.
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")
    bronze.run(spark, args.landing_root.rstrip("/"), args.schema)
    silver.run(spark, args.schema)
    market, economic = gold.run(spark, args.schema)
    if not args.skip_postgres:
        from databricks.postgres import load
        load(dsn, market, economic)


if __name__ == "__main__":
    main()
