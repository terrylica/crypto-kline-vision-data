#!/usr/bin/env python
"""Tests for the ApiBoundaryValidator class.

These tests validate the functionality of the ApiBoundaryValidator against actual
Binance API responses, ensuring that our validation logic correctly reflects the
real API behavior. All tests use real API calls rather than mocks.
"""

import asyncio
import pytest
import pandas as pd
from datetime import datetime, timedelta, timezone
import httpx

from utils.api_boundary_validator import ApiBoundaryValidator
from utils.market_constraints import Interval, MarketType
from utils.logger_setup import get_logger

# Configure logger
logger = get_logger(__name__)

# Test symbol - using a common symbol with liquidity
TEST_SYMBOL = "BTCUSDT"

# Configure pytest-asyncio default event loop scope
pytestmark = pytest.mark.asyncio(loop_scope="function")


@pytest.fixture
async def api_validator():
    """Fixture for ApiBoundaryValidator with proper resource management."""
    validator = ApiBoundaryValidator(MarketType.SPOT)
    yield validator
    await validator.close()


@pytest.fixture
async def direct_api_client():
    """Fixture for direct API access using httpx for verification."""
    client = httpx.AsyncClient(timeout=10.0)
    yield client
    await client.aclose()


@pytest.fixture
def recent_time_range():
    """Fixture providing a recent time range for testing.

    Returns a tuple of (start_time, end_time) with times in UTC.
    """
    # Use a recent time range 2 days ago to ensure data availability
    end_time = datetime.now(timezone.utc) - timedelta(days=2)
    # Round to nearest hour to make tests more deterministic
    end_time = end_time.replace(minute=0, second=0, microsecond=0)
    # Start time 1 hour before end time
    start_time = end_time - timedelta(hours=1)
    return start_time, end_time


async def test_is_valid_time_range_valid_period(
    api_validator, recent_time_range, caplog
):
    """Test is_valid_time_range with a valid time period."""
    start_time, end_time = recent_time_range
    interval = Interval.MINUTE_1

    logger.info(
        f"Testing valid time range: {start_time} to {end_time} with interval {interval}"
    )

    # Direct API call to verify data existence
    api_endpoint = f"https://api.binance.com/api/v3/klines"
    params = {
        "symbol": TEST_SYMBOL,
        "interval": interval.value,
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(end_time.timestamp() * 1000),
        "limit": 1,
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(api_endpoint, params=params)
        response.raise_for_status()
        api_data_exists = len(response.json()) > 0

    # Test our validator
    result = await api_validator.is_valid_time_range(start_time, end_time, interval)

    # Assert validator result matches direct API call
    assert (
        result == api_data_exists
    ), f"Validator returned {result}, but direct API call indicates data exists: {api_data_exists}"

    logger.info(f"is_valid_time_range returned {result} for {start_time} to {end_time}")


async def test_is_valid_time_range_future_period(api_validator, caplog):
    """Test is_valid_time_range with a future time period that shouldn't have data."""
    # Time range in the future
    future_time = datetime.now(timezone.utc) + timedelta(days=30)
    start_time = future_time
    end_time = future_time + timedelta(hours=1)
    interval = Interval.MINUTE_1

    logger.info(
        f"Testing future time range: {start_time} to {end_time} with interval {interval}"
    )

    # Direct API call to verify data absence
    api_endpoint = f"https://api.binance.com/api/v3/klines"
    params = {
        "symbol": TEST_SYMBOL,
        "interval": interval.value,
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(end_time.timestamp() * 1000),
        "limit": 1,
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(api_endpoint, params=params)
        response.raise_for_status()
        api_data_exists = len(response.json()) > 0

    # Test our validator
    result = await api_validator.is_valid_time_range(start_time, end_time, interval)

    # Assert validator result matches direct API call
    assert (
        result == api_data_exists
    ), f"Validator returned {result}, but direct API call indicates data exists: {api_data_exists}"
    assert result is False, "Future time range should not be valid"

    logger.info(
        f"is_valid_time_range correctly returned {result} for future time range"
    )


async def test_get_api_boundaries(api_validator, recent_time_range, caplog):
    """Test get_api_boundaries returns correct boundary information."""
    start_time, end_time = recent_time_range
    interval = Interval.MINUTE_1

    logger.info(
        f"Testing API boundaries for: {start_time} to {end_time} with interval {interval}"
    )

    # Direct API call to get boundary data
    api_endpoint = f"https://api.binance.com/api/v3/klines"
    params = {
        "symbol": TEST_SYMBOL,
        "interval": interval.value,
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(end_time.timestamp() * 1000),
        "limit": 1000,
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(api_endpoint, params=params)
        response.raise_for_status()
        api_data = response.json()

    if api_data:
        direct_first_timestamp = datetime.fromtimestamp(
            api_data[0][0] / 1000, tz=timezone.utc
        )
        direct_last_timestamp = datetime.fromtimestamp(
            api_data[-1][0] / 1000, tz=timezone.utc
        )
        direct_record_count = len(api_data)
    else:
        direct_first_timestamp = None
        direct_last_timestamp = None
        direct_record_count = 0

    # Test our validator
    boundaries = await api_validator.get_api_boundaries(start_time, end_time, interval)

    # Log the boundaries for debugging
    logger.info(f"API boundaries: {boundaries}")

    # Assert validator results match direct API call
    if direct_record_count > 0:
        assert (
            boundaries["api_start_time"] is not None
        ), "Start time should not be None for valid data"
        assert (
            boundaries["api_end_time"] is not None
        ), "End time should not be None for valid data"
        assert (
            boundaries["record_count"] == direct_record_count
        ), f"Record count mismatch: {boundaries['record_count']} vs {direct_record_count}"

        # Check timestamps
        assert (
            abs((boundaries["api_start_time"] - direct_first_timestamp).total_seconds())
            < 0.001
        ), f"Start time mismatch: {boundaries['api_start_time']} vs {direct_first_timestamp}"
        assert (
            abs((boundaries["api_end_time"] - direct_last_timestamp).total_seconds())
            < 0.001
        ), f"End time mismatch: {boundaries['api_end_time']} vs {direct_last_timestamp}"
    else:
        assert (
            boundaries["record_count"] == 0
        ), "Record count should be 0 for empty results"


async def test_does_data_range_match_api_response(
    api_validator, recent_time_range, caplog
):
    """Test does_data_range_match_api_response correctly validates DataFrame against API."""
    start_time, end_time = recent_time_range
    interval = Interval.MINUTE_1

    logger.info(
        f"Testing data range matching for: {start_time} to {end_time} with interval {interval}"
    )

    # First get the actual API data
    api_data = await api_validator.get_api_response(start_time, end_time, interval)

    if not api_data.empty:
        # Test with exact same data - should match
        result = await api_validator.does_data_range_match_api_response(
            api_data, start_time, end_time, interval
        )
        assert result is True, "Data directly from API should match"

        # Test with modified DataFrame - should not match
        modified_df = api_data.copy()
        if len(modified_df) > 1:
            # Drop first row to create a mismatch
            modified_df = modified_df.iloc[1:]
            result = await api_validator.does_data_range_match_api_response(
                modified_df, start_time, end_time, interval
            )
            assert result is False, "Modified DataFrame should not match API response"
    else:
        logger.warning(
            "No API data available for test_does_data_range_match_api_response"
        )
        # Skip test if no data available
        pytest.skip("No API data available for the specified time range")


async def test_empty_dataframe_validation(api_validator, caplog):
    """Test validator behavior with empty DataFrame."""
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=1)
    end_time = now
    interval = Interval.MINUTE_1

    # Create empty DataFrame
    empty_df = pd.DataFrame([], columns=["open", "high", "low", "close"])
    empty_df.index = pd.DatetimeIndex([], name="open_time")

    result = await api_validator.does_data_range_match_api_response(
        empty_df, start_time, end_time, interval
    )

    assert result is False, "Empty DataFrame should never match API response"


async def test_millisecond_precision(api_validator, recent_time_range, caplog):
    """Test validator handles millisecond precision in timestamps correctly."""
    start_time, end_time = recent_time_range

    # Add millisecond precision
    start_time = start_time.replace(microsecond=123000)  # 123 milliseconds
    end_time = end_time.replace(microsecond=456000)  # 456 milliseconds

    interval = Interval.MINUTE_1

    logger.info(f"Testing millisecond precision: {start_time} to {end_time}")

    # Direct API call with millisecond precision
    api_endpoint = f"https://api.binance.com/api/v3/klines"
    params = {
        "symbol": TEST_SYMBOL,
        "interval": interval.value,
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(end_time.timestamp() * 1000),
        "limit": 1000,
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(api_endpoint, params=params)
        response.raise_for_status()
        api_data = response.json()

    # Test our validator
    boundaries = await api_validator.get_api_boundaries(start_time, end_time, interval)

    logger.info(f"API boundaries with millisecond precision: {boundaries}")

    # Verify record count
    assert boundaries["record_count"] == len(
        api_data
    ), f"Record count mismatch: {boundaries['record_count']} vs {len(api_data)}"


async def test_cross_day_boundary(api_validator, caplog):
    """Test validator handles time ranges that cross day boundaries."""
    # Use a time range that crosses a day boundary
    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) - timedelta(
        days=3
    )
    next_day = day_start + timedelta(days=1, hours=1)  # Cross into next day

    interval = Interval.HOUR_1

    logger.info(f"Testing cross-day boundary: {day_start} to {next_day}")

    # Direct API call for cross-day boundary
    api_endpoint = f"https://api.binance.com/api/v3/klines"
    params = {
        "symbol": TEST_SYMBOL,
        "interval": interval.value,
        "startTime": int(day_start.timestamp() * 1000),
        "endTime": int(next_day.timestamp() * 1000),
        "limit": 1000,
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(api_endpoint, params=params)
        response.raise_for_status()
        api_data = response.json()

    # Test our validator
    boundaries = await api_validator.get_api_boundaries(day_start, next_day, interval)

    logger.info(f"API boundaries for cross-day period: {boundaries}")

    # Verify record count
    assert boundaries["record_count"] == len(
        api_data
    ), f"Record count mismatch: {boundaries['record_count']} vs {len(api_data)}"

    # Verify data exists
    assert boundaries["record_count"] > 0, "Should return data for cross-day boundary"


async def test_different_intervals(api_validator, recent_time_range, caplog):
    """Test validator with different intervals."""
    start_time, end_time = recent_time_range

    # Test intervals
    intervals = [Interval.MINUTE_1, Interval.MINUTE_5, Interval.HOUR_1]

    for interval in intervals:
        logger.info(f"Testing interval: {interval}")

        # Direct API call
        api_endpoint = f"https://api.binance.com/api/v3/klines"
        params = {
            "symbol": TEST_SYMBOL,
            "interval": interval.value,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": 1000,
        }

        async with httpx.AsyncClient() as client:
            response = await client.get(api_endpoint, params=params)
            response.raise_for_status()
            api_data = response.json()

        # Test our validator
        boundaries = await api_validator.get_api_boundaries(
            start_time, end_time, interval
        )

        logger.info(f"API boundaries for interval {interval}: {boundaries}")

        # Verify record count
        assert boundaries["record_count"] == len(
            api_data
        ), f"Record count mismatch for {interval}: {boundaries['record_count']} vs {len(api_data)}"


async def test_context_manager_usage(recent_time_range, caplog):
    """Test using ApiBoundaryValidator as a context manager."""
    start_time, end_time = recent_time_range
    interval = Interval.MINUTE_1

    async with ApiBoundaryValidator() as validator:
        # Perform an operation
        result = await validator.is_valid_time_range(start_time, end_time, interval)
        assert isinstance(result, bool), "is_valid_time_range should return a boolean"

    # Client should be closed after context manager exits
    assert validator.http_client.is_closed, "HTTP client should be closed"


async def test_error_handling(api_validator, caplog):
    """Test error handling in the validator."""
    # Invalid symbol to trigger an error
    invalid_symbol = "INVALIDPAIRNAMENOTEXIST"

    # Get current time
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=1)
    end_time = now
    interval = Interval.MINUTE_1

    # Test private _call_api method with invalid symbol
    try:
        await api_validator._call_api(
            start_time, end_time, interval, symbol=invalid_symbol
        )
        assert False, "Should have raised an exception for invalid symbol"
    except httpx.HTTPStatusError as e:
        logger.info(f"Expected error: {e}")
        assert e.response.status_code in [
            400,
            404,
        ], "Expected 400 or 404 status code for invalid symbol"

    # Test is_valid_time_range with invalid symbol
    result = await api_validator.is_valid_time_range(
        start_time, end_time, interval, symbol=invalid_symbol
    )
    assert (
        result is False
    ), "is_valid_time_range should handle errors gracefully and return False"

    # Test get_api_boundaries with invalid symbol
    boundaries = await api_validator.get_api_boundaries(
        start_time, end_time, interval, symbol=invalid_symbol
    )
    assert (
        boundaries["record_count"] == 0
    ), "get_api_boundaries should handle errors gracefully"
    assert "error" in boundaries, "get_api_boundaries should include error information"

    # Test get_api_response with invalid symbol
    df = await api_validator.get_api_response(
        start_time, end_time, interval, symbol=invalid_symbol
    )
    assert df.empty, "get_api_response should return empty DataFrame on error"
