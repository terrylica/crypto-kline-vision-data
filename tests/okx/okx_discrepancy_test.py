#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""OKX API discrepancy tests.

These integration tests compare behavior between the candles and history-candles
endpoints to identify any discrepancies in data availability, latency, and consistency.
"""

from datetime import datetime, timedelta

import pytest

from ckvd.utils.config import SECONDS_IN_HOUR
from tests.okx.conftest import (
    CANDLES_ENDPOINT,
    HISTORY_CANDLES_ENDPOINT as HISTORY_ENDPOINT,
    SPOT_INSTRUMENT,
    retry_request_with_status as retry_request,
)

MS_IN_HOUR = SECONDS_IN_HOUR * 1000  # Milliseconds in an hour
MS_IN_MINUTE = 60000  # Milliseconds in a minute


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
class TestOneSecondIntervalRejection:
    """Tests verifying that 1-second interval is NOT supported by OKX REST API v5.

    NOTE: OKX REST API v5 does NOT support 1-second intervals.
    Supported intervals are: 1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W, 1M
    and their UTC variants (1Hutc, 4Hutc, etc.)

    Error 51000 = "Parameter bar error" is returned for unsupported intervals.
    """

    def test_candles_rejects_1s_interval(self) -> None:
        """
        Verify the candles endpoint rejects 1-second interval.

        OKX REST API v5 does not support 1s interval. The minimum supported
        interval is 1m (1 minute).

        Validates:
        - API returns error code "51000" (Parameter bar error)
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": "1s", "limit": 5}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        # OKX returns code "51000" for "Parameter bar error"
        assert data.get("code") == "51000", (
            f"Expected error code '51000' for unsupported 1s interval, got code='{data.get('code')}', msg='{data.get('msg')}'"
        )

    def test_history_candles_rejects_1s_interval(self) -> None:
        """
        Verify the history-candles endpoint rejects 1-second interval.

        Validates:
        - API returns error code "51000" (Parameter bar error)
        """
        test_time = datetime.now() - timedelta(days=1)
        test_timestamp = int(test_time.timestamp() * 1000)

        params = {"instId": SPOT_INSTRUMENT, "bar": "1s", "limit": 5, "after": test_timestamp}
        response = retry_request(HISTORY_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        # OKX returns code "51000" for "Parameter bar error"
        assert data.get("code") == "51000", (
            f"Expected error code '51000' for unsupported 1s interval, got code='{data.get('code')}', msg='{data.get('msg')}'"
        )


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
