#!/usr/bin/env python3
"""
OKX API discrepancy tests.

These integration tests compare behavior between the candles and history-candles
endpoints to identify any discrepancies in data availability, latency, and consistency.
"""

import time
from datetime import datetime, timedelta

import httpx
import pytest

from data_source_manager.utils.config import SECONDS_IN_HOUR

# Constants
OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"
SPOT_INSTRUMENT = "BTC-USDT"
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

MS_IN_HOUR = SECONDS_IN_HOUR * 1000  # Milliseconds in an hour
MS_IN_MINUTE = 60000  # Milliseconds in a minute


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
class TestBarParameterRequirement:
    """Tests for the 'bar' parameter requirement across endpoints."""

    def test_candles_without_bar_uses_default(self) -> None:
        """
        Verify the candles endpoint works without the 'bar' parameter.

        OKX documentation indicates 'bar' is optional and defaults to '1m'.

        Validates:
        - API returns code "0" (success)
        - Data is returned with default interval
        """
        params = {"instId": SPOT_INSTRUMENT, "limit": 5}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert len(data.get("data", [])) > 0, "Expected data with default bar parameter"

    def test_history_candles_without_bar_uses_default(self) -> None:
        """
        Verify the history-candles endpoint works without the 'bar' parameter.

        Validates:
        - API returns code "0" (success)
        - Data is returned with default interval
        """
        timestamp = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)
        params = {"instId": SPOT_INSTRUMENT, "limit": 5, "after": timestamp}
        response = retry_request(HISTORY_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert len(data.get("data", [])) > 0, "Expected data with default bar parameter"


@pytest.mark.integration
@pytest.mark.okx
class TestOneSecondIntervalAvailability:
    """Tests for 1-second interval data availability."""

    def test_candles_1s_interval_available(self) -> None:
        """
        Verify the candles endpoint supports 1-second interval for recent data.

        Validates:
        - API returns code "0" (success)
        - Data is returned for 1s interval
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": "1s", "limit": 5}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert len(data.get("data", [])) > 0, "Expected 1s interval data"

    @pytest.mark.parametrize("days_back", [1, 7, 14])
    def test_history_candles_1s_interval_at_various_dates(self, days_back: int) -> None:
        """
        Verify 1-second interval data availability at different historical points.

        The 1s interval typically has limited historical depth (around 20-30 days).

        Validates:
        - API returns code "0" (success)
        - Data availability is checked at different time points
        """
        test_time = datetime.now() - timedelta(days=days_back)
        test_timestamp = int(test_time.timestamp() * 1000)

        params = {"instId": SPOT_INSTRUMENT, "bar": "1s", "limit": 5, "after": test_timestamp}
        response = retry_request(HISTORY_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        # Note: 1s data may not be available for older dates
        # We just verify the API responds correctly


@pytest.mark.integration
@pytest.mark.okx
class TestHistoricalDataAvailability:
    """Tests for historical 1D data availability across endpoints."""

    def test_candles_has_recent_1d_data(self) -> None:
        """
        Verify the candles endpoint has recent 1D data available.

        Validates:
        - API returns code "0" (success)
        - Data is returned for 1D interval
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": "1D", "limit": 5}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert len(data.get("data", [])) > 0, "Expected 1D interval data"

    def test_history_candles_has_old_1d_data(self) -> None:
        """
        Verify the history-candles endpoint has data from 2017.

        OKX history-candles endpoint should have data going back to October 2017.

        Validates:
        - API returns code "0" (success)
        - Data is returned for historical date
        """
        test_date = datetime(2017, 10, 15)
        test_timestamp = int(test_date.timestamp() * 1000)

        params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1D",
            "limit": 5,
            "after": test_timestamp,
        }
        response = retry_request(HISTORY_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        assert len(data.get("data", [])) > 0, "Expected historical 1D data from 2017"


@pytest.mark.integration
@pytest.mark.okx
class TestEndpointRecordLimits:
    """Tests for verifying the record limits per request for each endpoint."""

    @pytest.mark.parametrize("limit", [100, 200, 300])
    def test_candles_respects_limit(self, limit: int) -> None:
        """
        Verify the candles endpoint respects the limit parameter.

        According to docs, candles endpoint supports up to 300 records.

        Validates:
        - API returns code "0" (success)
        - Number of records returned does not exceed limit
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": limit}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        records_returned = len(data.get("data", []))
        assert records_returned <= limit, f"Got {records_returned} records, expected <= {limit}"

    @pytest.mark.parametrize("limit", [50, 100])
    def test_history_candles_respects_limit(self, limit: int) -> None:
        """
        Verify the history-candles endpoint respects the limit parameter.

        According to docs, history-candles endpoint supports up to 100 records.

        Validates:
        - API returns code "0" (success)
        - Number of records returned does not exceed limit
        """
        timestamp = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)
        params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1m",
            "limit": limit,
            "after": timestamp,
        }
        response = retry_request(HISTORY_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0', got {data.get('code')}: {data.get('msg')}"
        records_returned = len(data.get("data", []))
        assert records_returned <= limit, f"Got {records_returned} records, expected <= {limit}"
