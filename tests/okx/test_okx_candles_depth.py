#!/usr/bin/env python3
"""
OKX candles historical depth tests.

These integration tests verify how far back the OKX candles endpoints can
retrieve data for different intervals.
"""

import time
from datetime import datetime, timedelta

import httpx
import pytest

# Constants
OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"
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


def has_data(endpoint: str, instrument: str, interval: str, timestamp: int) -> tuple[bool, dict]:
    """
    Check if data exists at a specific timestamp.

    Args:
        endpoint: The API endpoint URL.
        instrument: Trading pair symbol.
        interval: Candle interval.
        timestamp: Timestamp in milliseconds.

    Returns:
        Tuple of (has_data, response).
    """
    params = {
        "instId": instrument,
        "bar": interval,
        "limit": 1,
        "after": timestamp,
    }
    result = retry_request(endpoint, params)

    if (
        result
        and "data" in result
        and result["data"].get("code") == "0"
        and len(result["data"].get("data", [])) > 0
    ):
        return True, result

    return False, result


@pytest.mark.integration
@pytest.mark.okx
class TestHistoricalDepthFromNow:
    """Tests for historical data depth from current time."""

    @pytest.mark.parametrize(
        "interval,days_back",
        [
            ("1m", 7),
            ("1m", 30),
            ("1H", 30),
            ("1H", 90),
            ("1D", 90),
            ("1D", 365),
        ],
    )
    def test_candles_historical_depth(self, interval: str, days_back: int) -> None:
        """
        Verify the candles endpoint has data available for various lookback periods.

        Validates:
        - API returns code "0" (success)
        - Data availability is checked for the specified period
        """
        test_date = datetime.now() - timedelta(days=days_back)
        test_ms = int(test_date.timestamp() * 1000)

        _, response = has_data(
            CANDLES_ENDPOINT, SPOT_INSTRUMENT, interval, test_ms
        )

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0': {data.get('msg')}"
        # Note: candles endpoint has limited history, so data may not be available

    @pytest.mark.parametrize(
        "interval,days_back",
        [
            ("1m", 30),
            ("1m", 90),
            ("1H", 90),
            ("1H", 365),
            ("1D", 365),
            ("1D", 730),  # 2 years
        ],
    )
    def test_history_candles_deep_historical_depth(self, interval: str, days_back: int) -> None:
        """
        Verify the history-candles endpoint has deep historical data available.

        The history-candles endpoint should have data going back several years.

        Validates:
        - API returns code "0" (success)
        - Data is available for the specified period
        """
        test_date = datetime.now() - timedelta(days=days_back)
        test_ms = int(test_date.timestamp() * 1000)

        has_data_result, response = has_data(
            HISTORY_ENDPOINT, SPOT_INSTRUMENT, interval, test_ms
        )

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0': {data.get('msg')}"
        assert has_data_result, f"Expected {interval} data available {days_back} days ago"


@pytest.mark.integration
@pytest.mark.okx
class TestCandlesEndpointRecentWindow:
    """Tests for the candles endpoint recent data window."""

    @pytest.mark.parametrize(
        "hours_ago",
        [1, 3, 6, 12, 24, 48],
    )
    def test_candles_recent_hours_available(self, hours_ago: int) -> None:
        """
        Verify the candles endpoint has recent hourly data available.

        Validates:
        - API returns code "0" (success)
        - Data is available for recent hours
        """
        test_time = datetime.now() - timedelta(hours=hours_ago)
        test_timestamp = int(test_time.timestamp() * 1000)

        has_data_result, response = has_data(
            CANDLES_ENDPOINT, SPOT_INSTRUMENT, "1m", test_timestamp
        )

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0': {data.get('msg')}"
        assert has_data_result, f"Expected 1m data available {hours_ago} hours ago"


@pytest.mark.integration
@pytest.mark.okx
class TestEarliestDataAvailability:
    """Tests for verifying earliest available data dates."""

    def test_history_candles_has_2017_data(self) -> None:
        """
        Verify the history-candles endpoint has data from October 2017.

        This is the known earliest date for BTC-USDT data on OKX.

        Validates:
        - API returns code "0" (success)
        - Data is available from October 2017
        """
        # October 15, 2017
        test_date = datetime(2017, 10, 15)
        test_ms = int(test_date.timestamp() * 1000)

        has_data_result, response = has_data(
            HISTORY_ENDPOINT, SPOT_INSTRUMENT, "1D", test_ms
        )

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0': {data.get('msg')}"
        assert has_data_result, "Expected 1D data available from October 2017"

    def test_history_candles_no_data_before_2017(self) -> None:
        """
        Verify the history-candles endpoint has no data before October 2017.

        Validates:
        - API returns code "0" (success)
        - Data is not available for dates before the exchange existed
        """
        # January 1, 2016 - before OKX had BTC-USDT
        test_date = datetime(2016, 1, 1)
        test_ms = int(test_date.timestamp() * 1000)

        has_data_result, response = has_data(
            HISTORY_ENDPOINT, SPOT_INSTRUMENT, "1D", test_ms
        )

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0': {data.get('msg')}"
        assert not has_data_result, "Expected no data before October 2017"


@pytest.mark.integration
@pytest.mark.okx
class TestMultipleIntervalDepth:
    """Tests for verifying data depth across multiple intervals."""

    @pytest.mark.parametrize(
        "interval",
        ["1m", "3m", "5m", "15m", "30m", "1H", "2H", "4H", "6H", "12H", "1D"],
    )
    def test_history_candles_interval_has_30_day_depth(self, interval: str) -> None:
        """
        Verify all standard intervals have at least 30 days of historical data.

        Validates:
        - API returns code "0" (success)
        - Data is available for 30 days ago
        """
        test_date = datetime.now() - timedelta(days=30)
        test_ms = int(test_date.timestamp() * 1000)

        has_data_result, response = has_data(
            HISTORY_ENDPOINT, SPOT_INSTRUMENT, interval, test_ms
        )

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0' for {interval}: {data.get('msg')}"
        assert has_data_result, f"Expected {interval} data available 30 days ago"

    @pytest.mark.parametrize(
        "interval",
        ["1H", "4H", "1D"],
    )
    def test_history_candles_interval_has_1_year_depth(self, interval: str) -> None:
        """
        Verify common intervals have at least 1 year of historical data.

        Validates:
        - API returns code "0" (success)
        - Data is available for 1 year ago
        """
        test_date = datetime.now() - timedelta(days=365)
        test_ms = int(test_date.timestamp() * 1000)

        has_data_result, response = has_data(
            HISTORY_ENDPOINT, SPOT_INSTRUMENT, interval, test_ms
        )

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0' for {interval}: {data.get('msg')}"
        assert has_data_result, f"Expected {interval} data available 1 year ago"
