"""Spark test for a sudden jump after a stable price history."""

import unittest
from datetime import date, timedelta

try:
    from pyspark.sql import SparkSession
    from databricks.gold import market_anomalies
except ImportError:
    SparkSession = None


@unittest.skipIf(SparkSession is None, "Install pyspark to run Spark tests")
class GoldTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("market-pipeline-test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_eighty_percent_jump_is_unusual_after_stable_history(self):
        start = date(2026, 1, 1)
        rows = [("IBM", start + timedelta(days=i), 100.0 + (i % 2)) for i in range(25)]
        rows.append(("IBM", start + timedelta(days=25), 180.0))
        prices = self.spark.createDataFrame(rows, "symbol string, trade_date date, close double")
        result = market_anomalies(prices).orderBy("trade_date").collect()
        self.assertFalse(result[0]["is_unusual"])
        self.assertTrue(result[-1]["is_unusual"])
        self.assertEqual(result[-1]["prior_days"], 20)


if __name__ == "__main__":
    unittest.main()
