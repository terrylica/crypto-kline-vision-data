#!/usr/bin/env python
"""Edge case tests for the ApiBoundaryValidator.

These tests verify that the boundary alignment methods in ApiBoundaryValidator correctly
mimic the Binance REST API's boundary behavior as documented in binance_rest_api_boundary_behaviour.md.
"""

import pytest
import pandas as pd
import asyncio
from datetime import datetime, timezone, timedelta
import httpx

from utils.api_boundary_validator import ApiBoundaryValidator
from utils.market_constraints import Interval, MarketType
from utils.logger_setup import get_logger

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
def cross_month_time():
    """Fixture providing a time range that crosses a month boundary."""
    # Use a recent month boundary to ensure data availability
    now = datetime.now(timezone.utc)
    # Get the first day of current month
    first_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Set end time to 1 hour after month start
    end_time = first_of_month + timedelta(hours=1)
    # Set start time to 1 hour before month start
    start_time = first_of_month - timedelta(hours=1)
    return start_time, end_time


@pytest.fixture
def cross_year_time():
    """Fixture providing a time range that crosses a year boundary."""
    # Use the most recent year boundary to ensure data availability
    now = datetime.now(timezone.utc)
    # Get January 1st of current year
    new_year = datetime(now.year, 1, 1, tzinfo=timezone.utc)
    # Set end time to 1 hour after year start
    end_time = new_year + timedelta(hours=1)
    # Set start time to 1 hour before year start
    start_time = new_year - timedelta(hours=1)
    return start_time, end_time


async def test_cross_month_boundary(api_validator, cross_month_time, caplog):
    """Test API boundary validation across month boundaries."""
    start_time, end_time = cross_month_time
    interval = Interval.MINUTE_1

    logger.info(
        f"Testing cross-month boundary: {start_time} to {end_time} with interval {interval}"
    )

    # Get API boundaries
    boundaries = await api_validator.get_api_boundaries(start_time, end_time, interval)

    # Log the actual boundaries for debugging
    logger.info(f"API boundaries response: {boundaries}")

    assert boundaries["record_count"] > 0, "Should have data across month boundary"
    assert boundaries["api_start_time"] is not None, "Should have valid start time"
    assert boundaries["api_end_time"] is not None, "Should have valid end time"

    # Verify data continuity across boundary
    df = await api_validator.get_api_response(start_time, end_time, interval)

    # Check for gaps around month boundary
    df["timestamp"] = df.index
    df["gap"] = df["timestamp"].diff()
    expected_gap = pd.Timedelta(minutes=1)  # For 1m interval

    # Find any gaps larger than expected
    large_gaps = df[df["gap"] > expected_gap]

    # Log any gaps found for debugging
    if not large_gaps.empty:
        logger.warning(f"Found gaps at month boundary: {large_gaps}")

    assert (
        len(large_gaps) == 0
    ), f"Found unexpected gaps at month boundary: {large_gaps}"


async def test_cross_year_boundary(api_validator, cross_year_time, caplog):
    """Test API boundary validation across year boundaries."""
    start_time, end_time = cross_year_time
    interval = Interval.MINUTE_1

    logger.info(
        f"Testing cross-year boundary: {start_time} to {end_time} with interval {interval}"
    )

    # Get API boundaries
    boundaries = await api_validator.get_api_boundaries(start_time, end_time, interval)

    # Log the actual boundaries for debugging
    logger.info(f"API boundaries response: {boundaries}")

    assert boundaries["record_count"] > 0, "Should have data across year boundary"
    assert boundaries["api_start_time"] is not None, "Should have valid start time"
    assert boundaries["api_end_time"] is not None, "Should have valid end time"

    # Verify data continuity across boundary
    df = await api_validator.get_api_response(start_time, end_time, interval)

    # Check for gaps around year boundary
    df["timestamp"] = df.index
    df["gap"] = df["timestamp"].diff()
    expected_gap = pd.Timedelta(minutes=1)  # For 1m interval

    # Find any gaps larger than expected
    large_gaps = df[df["gap"] > expected_gap]

    # Log any gaps found for debugging
    if not large_gaps.empty:
        logger.warning(f"Found gaps at year boundary: {large_gaps}")

    assert len(large_gaps) == 0, f"Found unexpected gaps at year boundary: {large_gaps}"


async def test_market_specific_boundaries(api_validator, caplog):
    """Test API boundary validation for specific market type."""
    # Test time range (recent to ensure data availability)
    end_time = datetime.now(timezone.utc) - timedelta(days=1)
    start_time = end_time - timedelta(hours=1)
    interval = Interval.MINUTE_1

    # Test SPOT market type
    logger.info(f"Testing SPOT market: {start_time} to {end_time}")

    symbol = "BTCUSDT"  # Standard symbol for SPOT

    boundaries = await api_validator.get_api_boundaries(
        start_time, end_time, interval, symbol=symbol
    )

    # Log the actual boundaries for debugging
    logger.info(f"API boundaries for SPOT: {boundaries}")

    assert boundaries["record_count"] > 0, "Should have data for SPOT market"
    assert (
        boundaries["api_start_time"] is not None
    ), "Should have valid start time for SPOT"
    assert boundaries["api_end_time"] is not None, "Should have valid end time for SPOT"


async def test_rate_limiting_retry(api_validator, caplog):
    """Test rate limiting handling and retry behavior."""
    # Make multiple rapid requests to trigger rate limiting
    end_time = datetime.now(timezone.utc) - timedelta(days=1)
    start_time = end_time - timedelta(minutes=5)
    interval = Interval.MINUTE_1

    logger.info(f"Testing rate limiting with time range: {start_time} to {end_time}")

    # Make 10 rapid requests
    tasks = []
    for i in range(10):
        task = api_validator.get_api_boundaries(start_time, end_time, interval)
        tasks.append(task)

    # Execute all requests concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Log results for debugging
    success_count = sum(
        1 for r in results if isinstance(r, dict) and not r.get("error")
    )
    error_count = len(results) - success_count
    logger.info(
        f"Rate limit test results - Successes: {success_count}, Errors: {error_count}"
    )

    # Check results - we expect some successes
    assert success_count > 0, "At least some requests should succeed"

    # Check successful responses have data
    for result in results:
        if isinstance(result, dict) and not result.get("error"):
            assert result["record_count"] > 0, "Successful requests should return data"
            logger.info(f"Successful request returned {result['record_count']} records")
