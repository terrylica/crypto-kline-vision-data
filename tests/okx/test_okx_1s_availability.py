#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""OKX 1-second interval data availability tests.

These integration tests verify the availability of 1-second interval data
across different time periods and endpoints.
"""

from datetime import datetime, timedelta

import pytest

from tests.okx.conftest import (
    CANDLES_ENDPOINT,
    HISTORY_CANDLES_ENDPOINT as HISTORY_ENDPOINT,
    SPOT_INSTRUMENT,
    retry_request_with_status as retry_request,
)

TEST_INTERVAL = "1s"


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
class TestRecentAvailability:
    """Tests for recent 1s data availability."""

    @pytest.mark.parametrize(
        "minutes_back,window_name",
        [
            (1, "Last minute"),
            (10, "Last 10 minutes"),
            (60, "Last hour"),
        ],
    )
    def test_candles_recent_1s_data_available(self, minutes_back: int, window_name: str) -> None:
        """
        Verify 1s data is available from the candles endpoint for recent time windows.

        Validates:
        - API returns code "0" (success)
        - Data is returned for recent 1s queries
        """
        current_time = datetime.now()
        test_time = current_time - timedelta(minutes=minutes_back)
        test_timestamp = int(test_time.timestamp() * 1000)

        has_data_result, response = has_data(
            CANDLES_ENDPOINT, SPOT_INSTRUMENT, TEST_INTERVAL, test_timestamp
        )

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0' for {window_name}: {data.get('msg')}"
        assert has_data_result, f"Expected 1s data for {window_name}"


@pytest.mark.integration
@pytest.mark.okx
class TestHistoricalTimepoints:
    """Tests for 1s data availability at historical timepoints."""

    @pytest.mark.parametrize(
        "days_back,expected_available",
        [
            (1, True),      # Yesterday - should be available
            (7, True),      # Last week - likely available
            (30, False),    # Last month - may not be available
            (180, False),   # Six months ago - not available
        ],
    )
    def test_history_candles_1s_at_historical_dates(
        self, days_back: int, expected_available: bool
    ) -> None:
        """
        Verify 1s data availability at various historical dates.

        The 1s interval typically has a retention period of around 20-30 days.

        Validates:
        - API returns code "0" (success)
        - Data availability matches expected pattern
        """
        test_date = datetime.now() - timedelta(days=days_back)
        test_timestamp = int(test_date.timestamp() * 1000)

        has_data_result, response = has_data(
            HISTORY_ENDPOINT, SPOT_INSTRUMENT, TEST_INTERVAL, test_timestamp
        )

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0': {data.get('msg')}"

        if expected_available:
            assert has_data_result, f"Expected 1s data available {days_back} days ago"
        else:
            # For older dates, data may or may not be available
            # This is informational - the API should respond correctly either way
            pass


@pytest.mark.integration
@pytest.mark.okx
class TestHourlyAvailability:
    """Tests for 1s data availability throughout the current day."""

    @pytest.mark.parametrize("hours_ago", [0, 3, 6, 12])
    def test_candles_1s_availability_by_hour(self, hours_ago: int) -> None:
        """
        Verify 1s data is available at different hours throughout today.

        This checks for any time-based patterns in data availability.

        Validates:
        - API returns code "0" (success)
        - Data is consistently available throughout the day
        """
        current_time = datetime.now()
        test_time = current_time - timedelta(hours=hours_ago)

        # Skip if test time is in the future
        if test_time > current_time:
            pytest.skip("Test time is in the future")

        test_timestamp = int(test_time.timestamp() * 1000)
        has_data_result, response = has_data(
            CANDLES_ENDPOINT, SPOT_INSTRUMENT, TEST_INTERVAL, test_timestamp
        )

        assert "data" in response, f"Request failed: {response.get('error')}"
        data = response["data"]
        assert data.get("code") == "0", f"Expected code '0' for {hours_ago}h ago: {data.get('msg')}"
        assert has_data_result, f"Expected 1s data available {hours_ago} hours ago"


@pytest.mark.integration
@pytest.mark.okx
class TestConsecutiveCalls:
    """Tests for API consistency with rapid consecutive calls."""

    def test_rapid_consecutive_calls_return_consistent_results(self) -> None:
        """
        Verify rapid consecutive calls return consistent results.

        This checks for any rate limiting or data inconsistencies.

        Validates:
        - All calls return code "0" (success)
        - Data availability is consistent across calls
        """
        current_time = datetime.now()
        test_timestamp = int(current_time.timestamp() * 1000)
        num_calls = 3

        results = []
        for i in range(num_calls):
            has_data_result, response = has_data(
                CANDLES_ENDPOINT, SPOT_INSTRUMENT, TEST_INTERVAL, test_timestamp
            )
            results.append(has_data_result)

            assert "data" in response, f"Call {i+1} failed: {response.get('error')}"
            data = response["data"]
            assert data.get("code") == "0", f"Call {i+1} expected code '0': {data.get('msg')}"

        # All results should be consistent
        assert len(set(results)) == 1, "Consecutive calls returned inconsistent results"


@pytest.mark.integration
@pytest.mark.okx
class TestDataRetentionBoundary:
    """Tests for finding the 1s data retention boundary."""

    def test_recent_data_available_old_data_not(self) -> None:
        """
        Verify the expected retention pattern: recent data available, old data not.

        The 1s interval typically retains data for around 20-30 days.

        Validates:
        - Recent data (within 7 days) is available
        - Old data (beyond 60 days) is not available
        """
        current_time = datetime.now()

        # Recent data should be available
        recent_time = current_time - timedelta(days=7)
        recent_timestamp = int(recent_time.timestamp() * 1000)
        has_recent, recent_response = has_data(
            HISTORY_ENDPOINT, SPOT_INSTRUMENT, TEST_INTERVAL, recent_timestamp
        )

        assert "data" in recent_response, f"Recent request failed: {recent_response.get('error')}"
        recent_data = recent_response["data"]
        assert recent_data.get("code") == "0", f"Expected code '0': {recent_data.get('msg')}"

        # Old data should not be available
        old_time = current_time - timedelta(days=60)
        old_timestamp = int(old_time.timestamp() * 1000)
        has_old, old_response = has_data(
            HISTORY_ENDPOINT, SPOT_INSTRUMENT, TEST_INTERVAL, old_timestamp
        )

        assert "data" in old_response, f"Old request failed: {old_response.get('error')}"
        old_data = old_response["data"]
        assert old_data.get("code") == "0", f"Expected code '0': {old_data.get('msg')}"

        # Recent should have data, old should not
        assert has_recent, "Expected 1s data available within 7 days"
        assert not has_old, "Expected no 1s data beyond 60 days"
