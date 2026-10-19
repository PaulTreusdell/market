"""Source adapters and landing format, without live API calls."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ingestion.fred_data import parse_observations
from ingestion.market_data import AlphaVantageRateLimitError, parse_daily_prices
from ingestion.run import main, write_batch


class IngestionTests(unittest.TestCase):
    def test_fred_missing_values_are_skipped(self):
        payload = {"observations": [
            {"date": "2026-01-01", "value": ".", "realtime_start": "2026-01-02", "realtime_end": "2026-01-02"},
            {"date": "2026-01-02", "value": "4.25", "realtime_start": "2026-01-03", "realtime_end": "2026-01-03"},
        ]}
        rows = parse_observations("DGS10", payload)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["value"], 4.25)

    def test_market_parses_numeric_ohlcv(self):
        payload = {"Time Series (Daily)": {
            "2026-01-02": {"1. open": "100", "2. high": "102", "3. low": "99", "4. close": "101", "5. volume": "1234"}
        }}
        rows = parse_daily_prices("ibm", payload)
        self.assertEqual(rows[0]["symbol"], "IBM")
        self.assertEqual(rows[0]["close"], 101.0)
        self.assertEqual(rows[0]["volume"], 1234)

    def test_rate_limit_is_reported(self):
        with self.assertRaisesRegex(AlphaVantageRateLimitError, "rate limit"):
            parse_daily_prices("IBM", {"Note": "rate limit"})

    def test_landing_is_json_lines(self):
        with tempfile.TemporaryDirectory() as directory:
            path = write_batch(Path(directory), "market", "IBM", [{"symbol": "IBM"}])
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"symbol": "IBM"})

    def test_market_requests_are_spaced_apart(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch("sys.argv", ["run.py", "--symbols", "IBM", "MSFT", "--series", "DGS10", "--output", directory]), \
             patch.dict("os.environ", {"FRED_API_KEY": "test", "ALPHA_VANTAGE_API_KEY": "test"}), \
             patch("ingestion.run.load_dotenv"), \
             patch("ingestion.run.time.monotonic", side_effect=[100.0, 100.2, 101.1]), \
             patch("ingestion.run.time.sleep") as sleep, \
             patch("ingestion.run.fetch_daily_prices", return_value=[]) as fetch_market, \
             patch("ingestion.run.fetch_observations", return_value=[]):
            main()

        self.assertEqual(fetch_market.call_count, 2)
        sleep.assert_called_once()
        self.assertAlmostEqual(sleep.call_args.args[0], 0.9)

    def test_market_limit_does_not_block_fred_or_repeat_saved_symbol(self):
        with tempfile.TemporaryDirectory() as directory:
            market_dir = Path(directory) / "market"
            market_dir.mkdir()
            (market_dir / "IBM_saved.jsonl").write_text('{"symbol": "IBM"}\n', encoding="utf-8")
            with patch("sys.argv", ["run.py", "--symbols", "IBM", "MSFT", "--series", "DGS10",
                                    "--output", directory, "--skip-existing-market"]), \
                 patch.dict("os.environ", {"FRED_API_KEY": "test", "ALPHA_VANTAGE_API_KEY": "test"}), \
                 patch("ingestion.run.load_dotenv"), \
                 patch("ingestion.run.fetch_observations", return_value=[{"series_id": "DGS10"}]), \
                 patch("ingestion.run.fetch_daily_prices",
                       side_effect=AlphaVantageRateLimitError("Alpha Vantage rate limit")) as fetch_market:
                with self.assertRaises(SystemExit) as exit_result:
                    main()

            self.assertEqual(exit_result.exception.code, 2)
            fetch_market.assert_called_once_with("MSFT", "test")
            self.assertEqual(len(list((Path(directory) / "fred").glob("DGS10_*.jsonl"))), 1)


if __name__ == "__main__":
    unittest.main()
