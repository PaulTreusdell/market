"""Small HTTP client shared by both API adapters."""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def session() -> requests.Session:
    """Create a client that retries temporary server and rate-limit failures."""
    client = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    client.mount("https://", HTTPAdapter(max_retries=retry))
    return client


def get_json(url: str, params: dict[str, str], client: requests.Session | None = None) -> dict:
    """Fetch a JSON object while keeping API keys out of error messages."""
    requester = client or session()
    try:
        response = requester.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        # Requests exceptions can contain the URL, including its API key.
        raise RuntimeError(f"Request to {url} failed") from None
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object from {url}")
    return payload
