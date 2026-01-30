#!/usr/bin/env python3
"""
OKX API endpoint tests.

These integration tests verify that the OKX candles and history-candles endpoints
work correctly for spot and swap instruments across all supported intervals.
"""

import time
from datetime import datetime, timedelta

import httpx
import pytest

OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"

# Test parameters
SPOT_INSTRUMENT = "BTC-USDT"
SWAP_INSTRUMENT = "BTC-USD-SWAP"
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
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds


def retry_request(url: str, params: dict | None = None, max_retries: int = MAX_RETRIES) -> dict:
    """
    Make HTTP request with retry logic.

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


@pytest.mark.integration
@pytest.mark.okx
class TestCandlesEndpoint:
    """Tests for the OKX /market/candles endpoint."""

    @pytest.mark.parametrize("interval", INTERVALS)
    def test_spot_candles_returns_data(self, interval: str) -> None:
        """
        Verify the candles endpoint returns data for spot BTC-USDT across all intervals.

        Validates:
        - API returns code "0" (success)
        - Response contains candle data (count > 0)
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": interval, "limit": 100}
        data = retry_request(CANDLES_ENDPOINT, params)

        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert "data" in data, "Response missing 'data' field"
        assert len(data["data"]) > 0, f"No candle data returned for interval {interval}"

    @pytest.mark.parametrize("interval", INTERVALS)
    def test_swap_candles_returns_data(self, interval: str) -> None:
        """
        Verify the candles endpoint returns data for swap BTC-USD-SWAP across all intervals.

        Validates:
        - API returns code "0" (success)
        - Response contains candle data (count > 0)
        """
        params = {"instId": SWAP_INSTRUMENT, "bar": interval, "limit": 100}
        data = retry_request(CANDLES_ENDPOINT, params)

        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert "data" in data, "Response missing 'data' field"
        assert len(data["data"]) > 0, f"No candle data returned for interval {interval}"


@pytest.mark.integration
@pytest.mark.okx
class TestHistoryCandlesEndpoint:
    """Tests for the OKX /market/history-candles endpoint."""

    @pytest.mark.parametrize("inst", [SPOT_INSTRUMENT, SWAP_INSTRUMENT])
    def test_history_candles_returns_data(self, inst: str) -> None:
        """
        Verify the history-candles endpoint returns data for both spot and swap instruments.

        Uses a timestamp from 30 days ago to ensure data availability.

        Validates:
        - API returns code "0" (success)
        - Response contains candle data (count > 0)
        """
        timestamp = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        params = {"instId": inst, "bar": "1D", "limit": 100, "after": timestamp}
        data = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert "data" in data, "Response missing 'data' field"
        assert len(data["data"]) > 0, f"No history candle data returned for {inst}"


@pytest.mark.integration
@pytest.mark.okx
class TestMaxLimit:
    """Tests for the maximum limit parameter behavior."""

    @pytest.mark.parametrize("limit", [100, 200, 300])
    def test_limit_returns_expected_count(self, limit: int) -> None:
        """
        Verify the candles endpoint respects the limit parameter.

        Validates:
        - API returns code "0" (success)
        - Number of returned candles matches requested limit (or available data)
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": limit}
        data = retry_request(CANDLES_ENDPOINT, params)

        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert "data" in data, "Response missing 'data' field"
        # API should return at most the requested limit
        assert len(data["data"]) <= limit, f"Got more data ({len(data['data'])}) than limit ({limit})"


@pytest.mark.integration
@pytest.mark.okx
class TestDataFormat:
    """Tests for the candle data format and structure."""

    def test_candle_data_structure(self) -> None:
        """
        Verify the candle data has the expected OHLCV structure.

        Each candle should contain 9 fields:
        [timestamp, open, high, low, close, volume, volCcy, volCcyQuote, confirm]

        Validates:
        - API returns code "0" (success)
        - Data contains at least one candle
        - Each candle has exactly 9 fields
        - Timestamp is a valid integer (in milliseconds)
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": 1}
        data = retry_request(CANDLES_ENDPOINT, params)

        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert "data" in data, "Response missing 'data' field"
        assert len(data["data"]) > 0, "No candle data returned"

        candle = data["data"][0]
        assert len(candle) == 9, f"Expected 9 fields per candle, got {len(candle)}"

        # Verify timestamp is a valid millisecond timestamp
        timestamp = int(candle[0])
        assert timestamp > 0, "Timestamp should be positive"
        assert len(str(timestamp)) == 13, "Timestamp should be in milliseconds (13 digits)"
