#!/usr/bin/env python3
"""
OKX API edge case tests.

These integration tests verify the OKX API behavior with edge cases including
invalid parameters, limit constraints, and timestamp handling.
"""

import time
from datetime import datetime, timedelta

import httpx
import pytest

# API constants
OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"

# Test parameters
SPOT_INSTRUMENT = "BTC-USDT"
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


@pytest.mark.integration
@pytest.mark.okx
class TestLimitConstraints:
    """Tests for the limit parameter constraints on OKX endpoints."""

    @pytest.mark.parametrize("limit", [1, 50, 100])
    def test_candles_valid_limits(self, limit: int) -> None:
        """
        Verify the candles endpoint accepts valid limit values.

        The candles endpoint supports up to 300 records per request.

        Validates:
        - API returns code "0" (success)
        - Records returned match the requested limit (or available data)
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": limit}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert len(data.get("data", [])) <= limit, "Returned more records than requested"

    @pytest.mark.parametrize("limit", [1, 50, 100])
    def test_history_candles_valid_limits(self, limit: int) -> None:
        """
        Verify the history-candles endpoint accepts valid limit values.

        The history-candles endpoint supports up to 100 records per request.

        Validates:
        - API returns code "0" (success)
        - Records returned match the requested limit (or available data)
        """
        timestamp = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1m",
            "limit": limit,
            "after": timestamp,
        }
        response = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert len(data.get("data", [])) <= limit, "Returned more records than requested"


@pytest.mark.integration
@pytest.mark.okx
class TestInvalidInstruments:
    """Tests for API behavior with invalid instrument IDs."""

    @pytest.mark.parametrize(
        "instrument,description",
        [
            ("BTCUSDT", "No hyphen separator"),
            ("BTC-INVALID", "Invalid quote currency"),
            ("INVALID-USDT", "Invalid base currency"),
        ],
    )
    def test_candles_invalid_instrument_returns_error(self, instrument: str, description: str) -> None:
        """
        Verify the candles endpoint returns an error for invalid instruments.

        OKX requires specific instrument format (e.g., BTC-USDT for spot).

        Validates:
        - API returns an error code (not "0") or empty data
        """
        params = {"instId": instrument, "bar": "1m", "limit": 10}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        # Invalid instruments should return error code or empty data
        is_error = data.get("code") != "0" or len(data.get("data", [])) == 0
        assert is_error, f"Expected error for invalid instrument '{instrument}' ({description})"

    @pytest.mark.parametrize(
        "instrument,description",
        [
            ("BTCUSDT", "No hyphen separator"),
            ("BTC-INVALID", "Invalid quote currency"),
            ("INVALID-USDT", "Invalid base currency"),
        ],
    )
    def test_history_candles_invalid_instrument_returns_error(self, instrument: str, description: str) -> None:
        """
        Verify the history-candles endpoint returns an error for invalid instruments.

        Validates:
        - API returns an error code (not "0") or empty data
        """
        timestamp = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        params = {"instId": instrument, "bar": "1m", "limit": 10, "after": timestamp}
        response = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        is_error = data.get("code") != "0" or len(data.get("data", [])) == 0
        assert is_error, f"Expected error for invalid instrument '{instrument}' ({description})"


@pytest.mark.integration
@pytest.mark.okx
class TestInvalidIntervals:
    """Tests for API behavior with invalid interval values."""

    @pytest.mark.parametrize(
        "interval,description",
        [
            ("2h", "Lowercase hour (should be 2H)"),
            ("1d", "Lowercase day (should be 1D)"),
            ("5M", "5 months (doesn't exist)"),
            ("invalid", "Gibberish interval"),
        ],
    )
    def test_candles_invalid_interval_returns_error(self, interval: str, description: str) -> None:
        """
        Verify the candles endpoint returns an error for invalid intervals.

        OKX uses specific case-sensitive interval format:
        - Lowercase for minutes (1m, 3m, 5m, etc.)
        - Uppercase for hours/days/weeks/months (1H, 4H, 1D, 1W, 1M)

        Validates:
        - API returns an error code (not "0") or empty data
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": interval, "limit": 10}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        is_error = data.get("code") != "0" or len(data.get("data", [])) == 0
        assert is_error, f"Expected error for invalid interval '{interval}' ({description})"


@pytest.mark.integration
@pytest.mark.okx
class TestTimestampEdgeCases:
    """Tests for timestamp parameter edge cases."""

    def test_future_timestamp_returns_no_data(self) -> None:
        """
        Verify requesting data with a future timestamp returns no data.

        Validates:
        - API returns code "0" (success)
        - Data array is empty (no future candles exist)
        """
        now_ms = int(datetime.now().timestamp() * 1000)
        far_future_ms = now_ms + (365 * 24 * 60 * 60 * 1000)  # 1 year in the future

        params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": 10, "after": far_future_ms}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}"
        assert len(data.get("data", [])) == 0, "Expected no data for future timestamp"

    def test_very_old_timestamp_returns_data(self) -> None:
        """
        Verify the history-candles endpoint returns data for old timestamps.

        Uses January 1, 2018 which should have historical data available.

        Validates:
        - API returns code "0" (success)
        - Data is returned for historical date
        """
        far_past_ms = int(datetime(2018, 1, 1).timestamp() * 1000)

        params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1D",
            "limit": 10,
            "after": far_past_ms,
        }
        response = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        # Historical data should be available for this date
        assert len(data.get("data", [])) > 0, "Expected historical data for 2018"


@pytest.mark.integration
@pytest.mark.okx
class TestMissingRequiredParameters:
    """Tests for API behavior when required parameters are missing."""

    def test_candles_missing_instid_returns_error(self) -> None:
        """
        Verify the candles endpoint returns an error when instId is missing.

        Validates:
        - API returns an error code (not "0")
        """
        params = {"bar": "1m", "limit": 10}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") != "0", "Expected error when instId is missing"

    def test_candles_missing_bar_uses_default(self) -> None:
        """
        Verify the candles endpoint uses default interval when bar is missing.

        According to OKX docs, 'bar' defaults to '1m' if not specified.

        Validates:
        - API returns code "0" (success)
        - Data is returned (using default interval)
        """
        params = {"instId": SPOT_INSTRUMENT, "limit": 10}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        # Bar parameter is optional with default '1m'
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert len(data.get("data", [])) > 0, "Expected data with default bar parameter"

    def test_candles_empty_instid_returns_error(self) -> None:
        """
        Verify the candles endpoint returns an error when instId is empty.

        Validates:
        - API returns an error code (not "0") or empty data
        """
        params = {"instId": "", "bar": "1m", "limit": 10}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        is_error = data.get("code") != "0" or len(data.get("data", [])) == 0
        assert is_error, "Expected error when instId is empty"
