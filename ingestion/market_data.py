"""Alpha Vantage daily, as-traded OHLCV prices."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ingestion.http import get_json

MARKET_URL = "https://www.alphavantage.co/query"
TIME_SERIES_KEY = "Time Series (Daily)"


class AlphaVantageRateLimitError(ValueError):
    """Alpha Vantage declined a request because of an API rate limit."""


def parse_daily_prices(symbol: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten Alpha Vantage's date-keyed response into daily price rows."""
    # API errors and rate limits often arrive as JSON with HTTP status 200.
    for key in ("Error Message", "Note", "Information"):
        if key in payload:
            message = str(payload[key])
            if key in ("Note", "Information") and any(
                phrase in message.lower()
                for phrase in ("rate limit", "requests per day", "requests per second", "call frequency")
            ):
                raise AlphaVantageRateLimitError(
                    f"Alpha Vantage rate limit reached while fetching {symbol}. "
                    "The free key may have hit its burst or daily quota."
                )
            raise ValueError(f"Alpha Vantage rejected {symbol}: {payload[key]}")
    daily = payload.get(TIME_SERIES_KEY)
    if not isinstance(daily, dict):
        raise ValueError(f"Alpha Vantage returned no daily prices for {symbol}")

    fetched_at = datetime.now(timezone.utc).isoformat()
    return [
        {
            "symbol": symbol.upper(),
            "trade_date": date,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"]),
            "fetched_at": fetched_at,
        }
        for date, values in sorted(daily.items())
    ]


def fetch_daily_prices(symbol: str, api_key: str) -> list[dict[str, Any]]:
    """Fetch the latest daily prices for one symbol using the compact API."""
    payload = get_json(MARKET_URL, {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": api_key,
    })
    return parse_daily_prices(symbol, payload)
