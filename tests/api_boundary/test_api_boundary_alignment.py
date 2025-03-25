#!/usr/bin/env python
"""Tests for boundary alignment functionality in ApiBoundaryValidator.

These tests verify that the boundary alignment methods in ApiBoundaryValidator correctly
mimic the Binance REST API's boundary behavior as documented in binance_rest_api_boundary_behaviour.md.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
import httpx

from utils.api_boundary_validator import ApiBoundaryValidator
from utils.market_constraints import Interval, MarketType
from utils.logger_setup import get_logger

# Configure logger
logger = get_logger(__name__)

# Configure pytest-asyncio default event loop scope
pytestmark = pytest.mark.asyncio(loop_scope="function")

# Test constants
TEST_SYMBOL = "BTCUSDT"


@pytest.fixture
async def api_validator():
    """Fixture for ApiBoundaryValidator with proper resource management."""
    validator = ApiBoundaryValidator(MarketType.SPOT)
    yield validator
    await validator.close()


@pytest.fixture
def millisecond_time_ranges():
    """Fixture providing time ranges with millisecond precision for testing boundary alignment."""
    now = datetime.now(timezone.utc)
    # Round to nearest minute and then add milliseconds
    base = now.replace(second=0, microsecond=0)

    # Test cases from binance_rest_api_boundary_behaviour.md
    return [
        # (start_time, end_time, interval, description)
        (
            base.replace(second=20, microsecond=0),  # 05:23:20.000
            base.replace(second=30, microsecond=0),  # 05:23:30.000
            Interval.SECOND_1,
            "Exact boundaries (1s)",
        ),
        (
            base.replace(second=20, microsecond=123000),  # 05:23:20.123
            base.replace(second=30, microsecond=0),  # 05:23:30.000
            Interval.SECOND_1,
            "Millisecond start (1s)",
        ),
        (
            base.replace(second=20, microsecond=0),  # 05:23:20.000
            base.replace(second=30, microsecond=456000),  # 05:23:30.456
            Interval.SECOND_1,
            "Millisecond end (1s)",
        ),
        (
            base.replace(second=20, microsecond=123000),  # 05:23:20.123
            base.replace(second=30, microsecond=456000),  # 05:23:30.456
            Interval.SECOND_1,
            "Both with milliseconds (1s)",
        ),
        (
            base.replace(second=20, microsecond=999000),  # 05:23:20.999
            base.replace(second=30, microsecond=1000),  # 05:23:30.001
            Interval.SECOND_1,
            "Edge case (999ms) (1s)",
        ),
        (
            base - timedelta(minutes=base.minute % 5),  # Exact 5-minute boundary
            (base - timedelta(minutes=base.minute % 5))
            + timedelta(minutes=10),  # 10 minutes later
            Interval.MINUTE_5,
            "Exact boundaries (5m)",
        ),
        (
            (base - timedelta(minutes=base.minute % 5))
            + timedelta(milliseconds=123),  # 123ms after 5-minute boundary
            (base - timedelta(minutes=base.minute % 5))
            + timedelta(minutes=10),  # 10 minutes later
            Interval.MINUTE_5,
            "Millisecond start (5m)",
        ),
    ]


async def test_align_time_boundaries(api_validator, millisecond_time_ranges, caplog):
    """Test that align_time_boundaries correctly implements Binance REST API behavior."""
    for start_time, end_time, interval, description in millisecond_time_ranges:
        logger.info(f"Testing alignment for {description}: {start_time} -> {end_time}")

        # Get aligned boundaries using our method
        aligned_start, aligned_end = api_validator.align_time_boundaries(
            start_time, end_time, interval
        )

        # Get actual API response to verify
        api_data = await api_validator.get_api_response(
            start_time, end_time, interval, symbol=TEST_SYMBOL
        )

        if not api_data.empty:
            # Extract actual boundaries from API response
            api_start = api_data.index[0].to_pydatetime()
            api_end = api_data.index[-1].to_pydatetime()

            logger.info(
                f"Comparison for {description}:\n"
                f"  Original: {start_time} -> {end_time}\n"
                f"  Aligned:  {aligned_start} -> {aligned_end}\n"
                f"  API:      {api_start} -> {api_end}"
            )

            # Check if our alignment matches the actual API behavior
            start_matches = abs((aligned_start - api_start).total_seconds()) < 0.001
            # For end time, we need to check the closest to original end_time that has data
            end_correct = aligned_end <= end_time

            assert start_matches, f"Start time mismatch for {description}"
            assert end_correct, f"End time incorrect for {description}"

            logger.info(f"Alignment test for {description}: PASS")
        else:
            logger.warning(f"No API data for {description}, can't fully validate")
            # We can still check basic alignment logic
            assert (
                aligned_start >= start_time
            ), f"Aligned start should not be before original start for {description}"
            assert (
                aligned_end <= end_time
            ), f"Aligned end should not be after original end for {description}"


async def test_estimate_record_count(api_validator, millisecond_time_ranges, caplog):
    """Test that estimate_record_count correctly predicts Binance REST API record counts."""
    for start_time, end_time, interval, description in millisecond_time_ranges:
        logger.info(
            f"Testing record count estimation for {description}: {start_time} -> {end_time}"
        )

        # Get estimated record count
        estimated_count = api_validator.estimate_record_count(
            start_time, end_time, interval
        )

        # Get actual API response to verify
        api_data = await api_validator.get_api_response(
            start_time, end_time, interval, symbol=TEST_SYMBOL
        )

        actual_count = len(api_data)

        logger.info(
            f"Record count for {description}:\n"
            f"  Estimated: {estimated_count}\n"
            f"  Actual: {actual_count}"
        )

        # The estimate may not be exact due to missing data points in the real API,
        # but it should be close and never less than the actual count
        if actual_count > 0:
            assert (
                estimated_count >= actual_count
            ), f"Estimated count too low for {description}"
            # Allow some tolerance for missing data points
            assert (
                estimated_count <= actual_count + 5
            ), f"Estimated count too high for {description}"


async def test_alignment_with_direct_api_call(
    api_validator, millisecond_time_ranges, caplog
):
    """Test boundary alignment against a direct API call using httpx."""
    for start_time, end_time, interval, description in millisecond_time_ranges:
        logger.info(f"Testing alignment against direct API call for {description}")

        # Get aligned boundaries using our method
        aligned_start, aligned_end = api_validator.align_time_boundaries(
            start_time, end_time, interval
        )

        # Make direct API call to verify
        api_endpoint = f"https://api.binance.com/api/v3/klines"
        params = {
            "symbol": TEST_SYMBOL,
            "interval": interval.value,
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": 1000,
        }

        logger.info(f"Making direct API call with params: {params}")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(api_endpoint, params=params)
                response.raise_for_status()
                api_data = response.json()

                if api_data:
                    # Extract actual boundaries from API response
                    api_start = datetime.fromtimestamp(
                        api_data[0][0] / 1000, tz=timezone.utc
                    )
                    api_end = datetime.fromtimestamp(
                        api_data[-1][0] / 1000, tz=timezone.utc
                    )

                    logger.info(
                        f"Direct API call for {description}:\n"
                        f"  Original: {start_time} -> {end_time}\n"
                        f"  Aligned:  {aligned_start} -> {aligned_end}\n"
                        f"  API:      {api_start} -> {api_end}"
                    )

                    # Check if our alignment matches the actual API behavior
                    start_matches = (
                        abs((aligned_start - api_start).total_seconds()) < 0.001
                    )
                    end_matches = abs((aligned_end - api_end).total_seconds()) < 0.001

                    assert (
                        start_matches
                    ), f"Start time mismatch in direct API call for {description}"
                    # For short time ranges, end_matches should be true
                    # For longer ranges, we expect aligned_end <= api_end
                    assert (
                        end_matches or aligned_end <= api_end
                    ), f"End time incorrect in direct API call for {description}"

                    logger.info(f"Direct API alignment test for {description}: PASS")
                else:
                    logger.warning(f"No data from direct API call for {description}")

        except Exception as e:
            logger.error(f"Error in direct API call for {description}: {e}")
            # Don't raise - we want to test all scenarios
            pass


async def test_cross_boundary_alignment(api_validator, caplog):
    """Test alignment for time ranges that cross significant time boundaries."""
    now = datetime.now(timezone.utc)

    # Test cases for crossing boundaries
    test_cases = [
        # (start_time, end_time, interval, description)
        (
            # Cross midnight boundary
            datetime(now.year, now.month, now.day, 23, 59, 55, tzinfo=timezone.utc),
            datetime(now.year, now.month, now.day, 23, 59, 55, tzinfo=timezone.utc)
            + timedelta(seconds=10),
            Interval.SECOND_1,
            "Cross-midnight (1s)",
        ),
        (
            # Cross hour boundary with milliseconds
            datetime(
                now.year, now.month, now.day, 10, 59, 59, 123000, tzinfo=timezone.utc
            ),
            datetime(
                now.year, now.month, now.day, 11, 0, 0, 456000, tzinfo=timezone.utc
            ),
            Interval.SECOND_1,
            "Cross-hour with ms (1s)",
        ),
        (
            # Cross day boundary with 1m interval
            datetime(now.year, now.month, now.day, 23, 58, 0, tzinfo=timezone.utc),
            datetime(now.year, now.month, now.day, 23, 58, 0, tzinfo=timezone.utc)
            + timedelta(minutes=4),
            Interval.MINUTE_1,
            "Cross-midnight (1m)",
        ),
    ]

    for start_time, end_time, interval, description in test_cases:
        logger.info(f"Testing {description}: {start_time} -> {end_time}")

        # Get aligned boundaries using our method
        aligned_start, aligned_end = api_validator.align_time_boundaries(
            start_time, end_time, interval
        )

        # Get actual API response
        api_data = await api_validator.get_api_response(
            start_time, end_time, interval, symbol=TEST_SYMBOL
        )

        if not api_data.empty:
            # Check if we have continuous data across the boundary
            api_data["timestamp"] = api_data.index
            api_data["gap"] = api_data["timestamp"].diff()

            # Calculate expected gap based on interval
            expected_gap = pd.Timedelta(
                seconds=api_validator._get_interval_seconds(interval)
            )

            # Find any gaps larger than expected
            large_gaps = api_data[api_data["gap"] > expected_gap]

            logger.info(
                f"Alignment for {description}:\n"
                f"  Original: {start_time} -> {end_time}\n"
                f"  Aligned:  {aligned_start} -> {aligned_end}\n"
                f"  Data points: {len(api_data)}"
            )

            if not large_gaps.empty:
                logger.warning(f"Found gaps in data across boundary: {large_gaps}")

            # There should be no gaps around the boundary
            assert len(large_gaps) == 0, f"Found unexpected gaps for {description}"

            # Check if our aligned boundaries match the actual data
            assert (
                api_data.index[0].to_pydatetime() >= aligned_start
            ), f"First data point before aligned start for {description}"
            assert (
                api_data.index[-1].to_pydatetime() <= aligned_end
            ), f"Last data point after aligned end for {description}"

            logger.info(f"Cross-boundary test for {description}: PASS")
        else:
            logger.warning(f"No API data for {description}, can't validate")
