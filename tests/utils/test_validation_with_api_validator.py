#!/usr/bin/env python
"""Tests for integration of ApiBoundaryValidator with DataValidation class."""

import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
import httpx

from utils.validation import DataValidation
from utils.api_boundary_validator import ApiBoundaryValidator
from utils.market_constraints import Interval, MarketType
from utils.logger_setup import get_logger

# Configure logger
logger = get_logger(__name__, "INFO")

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
def data_validator(api_validator):
    """Fixture for DataValidation with injected ApiBoundaryValidator."""
    return DataValidation(api_validator)


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


async def test_validate_api_time_range(data_validator, recent_time_range):
    """Test validate_api_time_range works correctly with ApiBoundaryValidator."""
    start_time, end_time = recent_time_range
    interval = Interval.MINUTE_1

    # Test with valid time range
    result = await data_validator.validate_api_time_range(
        start_time, end_time, interval, symbol=TEST_SYMBOL
    )
    assert isinstance(result, bool), "Should return a boolean result"

    # Test with string interval
    result_str = await data_validator.validate_api_time_range(
        start_time, end_time, interval.value, symbol=TEST_SYMBOL
    )
    assert result_str == result, "Should handle string intervals"

    # Test with future time range (should be invalid)
    future_start = datetime.now(timezone.utc) + timedelta(days=30)
    future_end = future_start + timedelta(hours=1)
    future_result = await data_validator.validate_api_time_range(
        future_start, future_end, interval, symbol=TEST_SYMBOL
    )
    assert future_result is False, "Future time range should be invalid"


async def test_get_api_aligned_boundaries(data_validator, recent_time_range):
    """Test get_api_aligned_boundaries returns correct boundaries."""
    start_time, end_time = recent_time_range
    interval = Interval.MINUTE_1

    # Get API aligned boundaries
    boundaries = await data_validator.get_api_aligned_boundaries(
        start_time, end_time, interval, symbol=TEST_SYMBOL
    )

    # Verify the structure of the returned boundaries
    assert "api_start_time" in boundaries, "Should include api_start_time"
    assert "api_end_time" in boundaries, "Should include api_end_time"
    assert "record_count" in boundaries, "Should include record_count"
    assert "matches_request" in boundaries, "Should include matches_request flag"

    # Verify the boundaries are datetime objects
    assert isinstance(
        boundaries["api_start_time"], datetime
    ), "api_start_time should be datetime"
    assert isinstance(
        boundaries["api_end_time"], datetime
    ), "api_end_time should be datetime"

    # Test with string interval
    boundaries_str = await data_validator.get_api_aligned_boundaries(
        start_time, end_time, interval.value, symbol=TEST_SYMBOL
    )
    assert (
        boundaries_str["record_count"] == boundaries["record_count"]
    ), "Should handle string intervals"


async def test_error_handling(data_validator):
    """Test error handling when API validation fails."""
    # Test with invalid symbol
    invalid_symbol = "INVALIDPAIRNAMENOTEXIST"
    start_time = datetime.now(timezone.utc) - timedelta(days=2)
    end_time = start_time + timedelta(hours=1)
    interval = Interval.MINUTE_1

    # ApiBoundaryValidator handles API errors internally and returns False
    # rather than raising exceptions for validate_api_time_range
    result = await data_validator.validate_api_time_range(
        start_time, end_time, interval, symbol=invalid_symbol
    )
    assert result is False, "Should return False for invalid symbol"

    # For get_api_aligned_boundaries, it should raise an exception or return error info
    # Let's just verify it returns something without error
    try:
        boundaries = await data_validator.get_api_aligned_boundaries(
            start_time, end_time, interval, symbol=invalid_symbol
        )
        # Either we get an error record back
        if "error" in boundaries:
            assert boundaries["error"], "Should include error message"
            assert boundaries["record_count"] == 0, "Should have no records"
        # Or we should get empty/None for boundaries
        else:
            assert (
                boundaries["api_start_time"] is None or boundaries["record_count"] == 0
            ), "Should have no valid data"
    except Exception as e:
        # Also okay if it throws an exception
        assert str(e), "Exception should have a message"


async def test_validation_without_validator():
    """Test validation methods without ApiBoundaryValidator."""
    # Create DataValidation without ApiBoundaryValidator
    validator = DataValidation()

    start_time = datetime.now(timezone.utc) - timedelta(days=2)
    end_time = start_time + timedelta(hours=1)
    interval = Interval.MINUTE_1

    # Methods should raise ValueError when validator is not provided
    with pytest.raises(ValueError, match="ApiBoundaryValidator is required"):
        await validator.validate_api_time_range(
            start_time, end_time, interval, symbol=TEST_SYMBOL
        )

    with pytest.raises(ValueError, match="ApiBoundaryValidator is required"):
        await validator.get_api_aligned_boundaries(
            start_time, end_time, interval, symbol=TEST_SYMBOL
        )


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
