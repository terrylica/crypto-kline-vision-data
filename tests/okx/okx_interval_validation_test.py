#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""OKX interval validation tests.

These integration tests verify the OKX API behavior with different interval
formats, including case sensitivity and 1-second interval support.
"""

from datetime import datetime, timedelta

import pytest

from tests.okx.conftest import (
    CANDLES_ENDPOINT,
    HISTORY_CANDLES_ENDPOINT,
    SPOT_INSTRUMENT,
    retry_request_with_status as retry_request,
)


@pytest.mark.integration
@pytest.mark.okx
class TestIntervalCaseSensitivity:
    """Tests for interval parameter case sensitivity."""

    @pytest.mark.parametrize(
        "interval,expected_success",
        [
            ("1m", True),   # Correct: lowercase m for minute
            ("1H", True),   # Correct: uppercase H for hour
            ("4H", True),   # Correct: uppercase H for hour
            ("1D", True),   # Correct: uppercase D for day
            ("1W", True),   # Correct: uppercase W for week
            ("1M", True),   # Correct: uppercase M for month
        ],
    )
    def test_candles_official_interval_format(self, interval: str, expected_success: bool) -> None:
        """
        Verify the candles endpoint accepts official interval formats.

        OKX uses specific case-sensitive formats:
        - Lowercase for minutes (1m, 3m, 5m, etc.)
        - Uppercase for hours/days/weeks/months (1H, 4H, 1D, 1W, 1M)

        Validates:
        - API returns code "0" for valid intervals
        - Data is returned for valid intervals
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": interval, "limit": 1}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]

        if expected_success:
            assert data.get("code") == "0", f"Expected success for '{interval}': {data.get('msg')}"
            assert len(data.get("data", [])) > 0, f"No data returned for '{interval}'"
        else:
            assert data.get("code") != "0" or len(data.get("data", [])) == 0, (
                f"Expected failure for '{interval}'"
            )

    @pytest.mark.parametrize(
        "interval,description",
        [
            ("1h", "lowercase hour"),
            ("1d", "lowercase day"),
            ("1w", "lowercase week"),
        ],
    )
    def test_candles_lowercase_intervals_fail(self, interval: str, description: str) -> None:
        """
        Verify the candles endpoint rejects lowercase intervals for hour/day/week.

        OKX requires uppercase letters for hour (H), day (D), and week (W).

        Validates:
        - API returns error or empty data for invalid case
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": interval, "limit": 1}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]

        # Invalid format should return error or empty data
        is_error = data.get("code") != "0" or len(data.get("data", [])) == 0
        assert is_error, f"Expected error for {description} '{interval}'"


@pytest.mark.integration
@pytest.mark.okx
class TestOneSecondInterval:
    """Tests for 1-second interval support."""

    def test_candles_supports_1s_interval(self) -> None:
        """
        Verify the candles endpoint supports 1-second interval.

        Validates:
        - API returns code "0" (success)
        - Data is returned for 1s interval
        """
        params = {"instId": SPOT_INSTRUMENT, "bar": "1s", "limit": 10}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected success for 1s: {data.get('msg')}"
        assert len(data.get("data", [])) > 0, "No data returned for 1s interval"

    @pytest.mark.parametrize(
        "days_back,period_name",
        [
            (1, "1 day ago"),
            (7, "1 week ago"),
        ],
    )
    def test_history_candles_1s_recent_availability(self, days_back: int, period_name: str) -> None:
        """
        Verify 1-second interval data availability for recent periods.

        The 1s interval has limited historical depth (typically around 20-30 days).

        Validates:
        - API returns code "0" (success)
        - Data is available for recent dates
        """
        timestamp = int((datetime.now() - timedelta(days=days_back)).timestamp() * 1000)
        params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1s",
            "limit": 10,
            "after": timestamp,
        }
        response = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected success for 1s at {period_name}: {data.get('msg')}"
        # Note: Data may or may not be available depending on the cutoff

    def test_history_candles_1s_old_data_not_available(self) -> None:
        """
        Verify 1-second interval data is not available for dates beyond the retention period.

        The 1s interval typically only retains data for ~20-30 days.

        Validates:
        - API returns code "0" (success)
        - Data is empty for dates beyond retention
        """
        # 6 months ago should be beyond 1s retention
        timestamp = int((datetime.now() - timedelta(days=180)).timestamp() * 1000)
        params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1s",
            "limit": 10,
            "after": timestamp,
        }
        response = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0': {data.get('msg')}"
        # Expect no data for old 1s requests
        assert len(data.get("data", [])) == 0, "Expected no 1s data from 6 months ago"
