"""Stream Databricks gold results into PostgreSQL with atomic upserts."""

from __future__ import annotations

import psycopg


MARKET_COLUMNS = (
    "symbol", "trade_date", "close", "previous_close", "daily_return",
    "prior_days", "baseline_volatility", "threshold", "is_unusual",
)
ECONOMIC_COLUMNS = (
    "series_id", "observation_date", "value", "realtime_start", "realtime_end",
)


def _copy_rows(cursor, frame, staging_table: str, columns: tuple[str, ...]) -> None:
    """Stream Spark partitions to a transaction-local PostgreSQL staging table."""
    selected = frame.select(*columns)
    column_list = ", ".join(columns)
    with cursor.copy(f"COPY {staging_table} ({column_list}) FROM STDIN") as copy:
        for row in selected.toLocalIterator():
            copy.write_row(tuple(row))


def load(dsn: str, market, economic) -> None:
    """Apply both complete recomputations atomically, keyed by their natural IDs.

    Existing rows absent from a batch are retained, matching the old MERGE
    behavior. A failed Spark read, COPY, or upsert rolls back both datasets.
    """
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE TEMP TABLE market_load "
                "(LIKE financial_market.market_anomalies) ON COMMIT DROP"
            )
            cursor.execute(
                "CREATE TEMP TABLE economic_load "
                "(LIKE financial_market.economic_observations) ON COMMIT DROP"
            )
            _copy_rows(cursor, market, "market_load", MARKET_COLUMNS)
            _copy_rows(cursor, economic, "economic_load", ECONOMIC_COLUMNS)
            cursor.execute("""
                INSERT INTO financial_market.market_anomalies (
                    symbol, trade_date, close, previous_close, daily_return,
                    prior_days, baseline_volatility, threshold, is_unusual
                )
                SELECT symbol, trade_date, close, previous_close, daily_return,
                       prior_days, baseline_volatility, threshold, is_unusual
                FROM market_load WHERE true
                ON CONFLICT (symbol, trade_date) DO UPDATE SET
                    close = EXCLUDED.close,
                    previous_close = EXCLUDED.previous_close,
                    daily_return = EXCLUDED.daily_return,
                    prior_days = EXCLUDED.prior_days,
                    baseline_volatility = EXCLUDED.baseline_volatility,
                    threshold = EXCLUDED.threshold,
                    is_unusual = EXCLUDED.is_unusual
            """)
            cursor.execute("""
                INSERT INTO financial_market.economic_observations (
                    series_id, observation_date, value, realtime_start, realtime_end
                )
                SELECT series_id, observation_date, value, realtime_start, realtime_end
                FROM economic_load WHERE true
                ON CONFLICT (series_id, observation_date) DO UPDATE SET
                    value = EXCLUDED.value,
                    realtime_start = EXCLUDED.realtime_start,
                    realtime_end = EXCLUDED.realtime_end
            """)
