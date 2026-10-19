"""Check the database handoff without requiring a running PostgreSQL server."""

import unittest
from unittest.mock import MagicMock, patch

from databricks.postgres import load


class Frame:
    def __init__(self, rows):
        self.rows = rows
        self.columns = None

    def select(self, *columns):
        self.columns = columns
        return self

    def toLocalIterator(self):
        return iter(self.rows)


class PostgresLoadTests(unittest.TestCase):
    @patch("databricks.postgres.psycopg.connect")
    def test_streams_both_datasets_and_upserts_in_one_connection(self, connect):
        market = Frame([("IBM", "2026-01-01", 100.0, None, None, 0, None, 0.05, False)])
        economic = Frame([("DGS10", "2026-01-01", 4.0, None, None)])
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        copy = cursor.copy.return_value.__enter__.return_value

        load("postgresql://example", market, economic)

        connect.assert_called_once_with("postgresql://example")
        self.assertEqual(cursor.copy.call_count, 2)
        self.assertEqual(copy.write_row.call_count, 2)
        self.assertEqual(market.columns[0:2], ("symbol", "trade_date"))
        self.assertEqual(economic.columns[0:2], ("series_id", "observation_date"))
        sql = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertIn("ON CONFLICT (symbol, trade_date)", sql)
        self.assertIn("ON CONFLICT (series_id, observation_date)", sql)

    @patch("databricks.postgres.psycopg.connect")
    def test_spark_failure_prevents_either_upsert(self, connect):
        market = Frame([])
        economic = Frame([])
        economic.toLocalIterator = MagicMock(side_effect=RuntimeError("Spark failed"))
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value

        with self.assertRaisesRegex(RuntimeError, "Spark failed"):
            load("postgresql://example", market, economic)

        self.assertEqual(cursor.execute.call_count, 2)  # Temporary tables only.


if __name__ == "__main__":
    unittest.main()
