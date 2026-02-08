#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""OKX interval validation tests.

These integration tests verify the OKX API behavior with different interval
formats, including case sensitivity and 1-second interval support.
"""

import pytest

from tests.okx.conftest import (
    CANDLES_ENDPOINT,
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
            ("1m", True),  # Correct: lowercase m for minute
            ("1H", True),  # Correct: uppercase H for hour
            ("4H", True),  # Correct: uppercase H for hour
            ("1D", True),  # Correct: uppercase D for day
            ("1W", True),  # Correct: uppercase W for week
            ("1M", True),  # Correct: uppercase M for month
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
            assert data.get("code") != "0" or len(data.get("data", [])) == 0, f"Expected failure for '{interval}'"

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
    """Tests for 1-second interval behavior.

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
        params = {"instId": SPOT_INSTRUMENT, "bar": "1s", "limit": 10}
        response = retry_request(CANDLES_ENDPOINT, params)

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        # OKX returns code "51000" for "Parameter bar error"
        assert data.get("code") == "51000", (
            f"Expected error code '51000' for unsupported 1s interval, got code='{data.get('code')}', msg='{data.get('msg')}'"
        )
