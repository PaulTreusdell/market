"""CLI: fetch source data and write portable JSON Lines landing files."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv

from ingestion.fred_data import fetch_observations
from ingestion.market_data import AlphaVantageRateLimitError, fetch_daily_prices

ALPHA_VANTAGE_INTERVAL_SECONDS = 1.1


def write_batch(root: Path, source: str, name: str, rows: list[dict]) -> Path:
    """Write one fetch to its own JSON Lines file for Spark to read later."""
    destination = root / source / f"{name}_{uuid4().hex}.jsonl"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, allow_nan=False) + "\n")
    return destination


def main() -> None:
    """Parse CLI settings, fetch each requested source, and land its rows."""
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", nargs="+", default=["IBM"])
    parser.add_argument("--series", nargs="+", default=["DGS10", "FEDFUNDS"])
    parser.add_argument("--fred-start", default="2024-01-01")
    parser.add_argument("--output", type=Path, default=Path("data/landing"))
    parser.add_argument("--skip-existing-market", action="store_true",
                        help="Reuse market JSONL files already saved for requested symbols")
    args = parser.parse_args()

    fred_key = os.environ.get("FRED_API_KEY")
    market_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    if not fred_key or not market_key:
        parser.error("Set FRED_API_KEY and ALPHA_VANTAGE_API_KEY in the project .env or environment")

    # Fetch FRED first so an Alpha Vantage rejection cannot block its files.
    for series_id in dict.fromkeys(s.upper() for s in args.series):
        rows = fetch_observations(series_id, fred_key, args.fred_start)
        path = write_batch(args.output, "fred", series_id, rows)
        print(f"{series_id}: {len(rows)} observations -> {path}")

    # Preserve input order while avoiding duplicate API calls.
    last_market_request = None
    for symbol in dict.fromkeys(s.upper() for s in args.symbols):
        if args.skip_existing_market and any(
            path.stat().st_size > 0 for path in (args.output / "market").glob(f"{symbol}_*.jsonl")
        ):
            print(f"{symbol}: using existing market landing file")
            continue
        now = time.monotonic()
        if last_market_request is not None:
            wait = ALPHA_VANTAGE_INTERVAL_SECONDS - (now - last_market_request)
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
        last_market_request = now
        try:
            rows = fetch_daily_prices(symbol, market_key)
        except AlphaVantageRateLimitError as error:
            parser.exit(2, f"error: {error} Saved landing files remain available. "
                           "If the daily quota is exhausted, wait for it to reset before retrying.\n")
        path = write_batch(args.output, "market", symbol, rows)
        print(f"{symbol}: {len(rows)} prices -> {path}")


if __name__ == "__main__":
    main()
