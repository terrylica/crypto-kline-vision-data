"""OKX test fixtures and shared utilities.

This module provides shared fixtures and utility functions for OKX integration tests.

ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
DRY Consolidation: Extracted retry_request from 7 OKX test files.
DRY Consolidation: Added timestamp helpers and assertion utilities.
"""

import time
from datetime import datetime, timedelta, timezone

import httpx
import pytest

# =============================================================================
# OKX API Constants
# =============================================================================

OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# Test instruments
SPOT_INSTRUMENT = "BTC-USDT"
SWAP_INSTRUMENT = "BTC-USD-SWAP"

# Supported intervals
INTERVALS = [
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1H",
    "2H",
    "4H",
    "6H",
    "12H",
    "1D",
    "1W",
    "1M",
]


# =============================================================================
# Shared Utility Functions
# =============================================================================


def retry_request(url: str, params: dict | None = None, max_retries: int = MAX_RETRIES) -> dict:
    """Make HTTP request with retry logic.

    Args:
        url: The API endpoint URL.
        params: Query parameters for the request.
        max_retries: Maximum number of retry attempts.

    Returns:
        JSON response data from the API.

    Raises:
        httpx.HTTPStatusError: If all retry attempts fail.
    """
    for attempt in range(max_retries):
        try:
            response = httpx.get(url, params=params, timeout=10.0)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                raise e
    return {}


def retry_request_with_status(url: str, params: dict | None = None, max_retries: int = MAX_RETRIES) -> dict:
    """Make HTTP request with retry logic, returning status code.

    Args:
        url: The API endpoint URL.
        params: Query parameters for the request.
        max_retries: Maximum number of retry attempts.

    Returns:
        Dictionary with status_code and data/error fields.
    """
    for attempt in range(max_retries):
        try:
            response = httpx.get(url, params=params, timeout=10.0)
            response.raise_for_status()
            return {
                "status_code": response.status_code,
                "data": response.json(),
            }
        except httpx.HTTPStatusError as e:
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                return {
                    "status_code": e.response.status_code if e.response else -1,
                    "error": str(e),
                }
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RequestError) as e:
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                return {
                    "status_code": -1,
                    "error": str(e),
                }
    return {"status_code": -1, "error": "Unknown error"}


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="function")
def instrument():
    """Provide the trading instrument for OKX tests.

    Returns a default value of BTC-USDT for spot markets.
    """
    return SPOT_INSTRUMENT


@pytest.fixture(scope="function")
def interval():
    """Provide the trading interval for OKX tests.

    Returns a default value of 1m (1 minute).
    """
    return "1m"


# =============================================================================
# Timestamp Helpers
# =============================================================================


def ms_timestamp_days_ago(days: int) -> int:
    """Get Unix timestamp in milliseconds for N days ago.

    Args:
        days: Number of days to go back.

    Returns:
        Unix timestamp in milliseconds.

    Example:
        >>> ts = ms_timestamp_days_ago(30)  # 30 days ago
    """
    return int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)


def ms_timestamp_hours_ago(hours: int) -> int:
    """Get Unix timestamp in milliseconds for N hours ago.

    Args:
        hours: Number of hours to go back.

    Returns:
        Unix timestamp in milliseconds.
    """
    return int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp() * 1000)


def ms_timestamp_minutes_ago(minutes: int) -> int:
    """Get Unix timestamp in milliseconds for N minutes ago.

    Args:
        minutes: Number of minutes to go back.

    Returns:
        Unix timestamp in milliseconds.
    """
    return int((datetime.now(timezone.utc) - timedelta(minutes=minutes)).timestamp() * 1000)


def ms_timestamp_now() -> int:
    """Get current Unix timestamp in milliseconds.

    Returns:
        Current Unix timestamp in milliseconds.
    """
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# =============================================================================
# Assertion Helpers
# =============================================================================


def assert_okx_success(data: dict, context: str = "") -> None:
    """Assert OKX API response indicates success (code == '0').

    Args:
        data: OKX API response dictionary.
        context: Optional context string for error message.

    Raises:
        AssertionError: If response code is not '0'.
    """
    msg = data.get("msg", "")
    code = data.get("code")
    ctx = f" for {context}" if context else ""
    assert code == "0", f"Expected code '0'{ctx}, got {code}: {msg}"


def assert_okx_has_data(data: dict, context: str = "") -> list:
    """Assert OKX API response has data and return it.

    Args:
        data: OKX API response dictionary.
        context: Optional context string for error message.

    Returns:
        The 'data' list from the response.

    Raises:
        AssertionError: If response code is not '0' or data is empty.
    """
    assert_okx_success(data, context)
    result = data.get("data", [])
    ctx = f" for {context}" if context else ""
    assert len(result) > 0, f"Expected non-empty data{ctx}"
    return result
