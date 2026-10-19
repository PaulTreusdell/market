"""FRED series observations, normalized to one record per date."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from ingestion.http import get_json

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"


def parse_observations(series_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert FRED observations to numeric rows, skipping missing values."""
    if "error_message" in payload:
        raise ValueError(f"FRED rejected series {series_id}: {payload['error_message']}")
    observations = payload.get("observations")
    if not isinstance(observations, list):
        raise ValueError(f"FRED returned no observations for {series_id}")

    # A shared fetch time lets bronze pick the newest version on later runs.
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for item in observations:
        value = item["value"]
        if value == ".":  # FRED's marker for missing observations
            continue
        rows.append({
            "series_id": series_id,
            "observation_date": item["date"],
            "value": float(value),
            "realtime_start": item["realtime_start"],
            "realtime_end": item["realtime_end"],
            "fetched_at": fetched_at,
        })
    return rows


def fetch_observations(series_id: str, api_key: str, start_date: str) -> list[dict[str, Any]]:
    """Request one FRED series from start_date onward and normalize it."""
    payload = get_json(FRED_URL, {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
    })
    return parse_observations(series_id, payload)
