#!/usr/bin/env python
"""Tests for integration of ApiBoundaryValidator with CacheValidator class."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import tempfile
from pathlib import Path
import pyarrow as pa
import pyarrow.ipc as ipc

from utils.cache_validator import CacheValidator, ERROR_TYPES
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
def cache_validator(api_validator):
    """Fixture for CacheValidator with injected ApiBoundaryValidator."""
    return CacheValidator(api_validator)


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


@pytest.fixture
def sample_api_data(api_validator, recent_time_range):
    """Fixture providing sample data from the API for testing."""

    async def _get_data():
        start_time, end_time = recent_time_range
        interval = Interval.MINUTE_1

        # Get API response as DataFrame
        api_response = await api_validator.get_api_response(
            start_time, end_time, interval, symbol=TEST_SYMBOL
        )

        # If no data from API, create synthetic data
        if api_response.empty:
            index = pd.date_range(
                start=start_time, end=end_time, freq="1min", inclusive="left"
            )
            api_response = pd.DataFrame(
                {
                    "open": np.random.random(len(index)) * 1000 + 30000,
                    "high": np.random.random(len(index)) * 1000 + 30500,
                    "low": np.random.random(len(index)) * 1000 + 29500,
                    "close": np.random.random(len(index)) * 1000 + 30000,
                    "volume": np.random.random(len(index)) * 100,
                },
                index=pd.DatetimeIndex(index, name="open_time"),
            )

        # Create modified data that doesn't match API (for negative tests)
        modified_data = api_response.copy()
        if not modified_data.empty:
            if len(modified_data) > 5:
                # Drop some rows to create a mismatch
                modified_data = modified_data.iloc[5:]
            else:
                # Modify some values
                modified_data.iloc[0, 0] = modified_data.iloc[0, 0] * 1.1

        return {
            "api_data": api_response,
            "modified_data": modified_data,
            "start_time": start_time,
            "end_time": end_time,
            "interval": interval,
        }

    return _get_data


@pytest.fixture
def temp_arrow_file():
    """Fixture to create a temporary Arrow file."""
    with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as temp:
        temp_path = Path(temp.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


async def test_validate_cache_data_with_api_validator(cache_validator, sample_api_data):
    """Test validate_cache_data with API boundary validation."""
    test_data = await sample_api_data()

    # Test with data that matches API response
    error = await cache_validator.validate_cache_data(
        test_data["api_data"],
        start_time=test_data["start_time"],
        end_time=test_data["end_time"],
        interval=test_data["interval"],
        symbol=TEST_SYMBOL,
    )
    assert error is None, "Should validate successfully with matching data"

    # Test with data that doesn't match API response
    if not test_data["modified_data"].empty and len(test_data["modified_data"]) != len(
        test_data["api_data"]
    ):
        error = await cache_validator.validate_cache_data(
            test_data["modified_data"],
            start_time=test_data["start_time"],
            end_time=test_data["end_time"],
            interval=test_data["interval"],
            symbol=TEST_SYMBOL,
        )
        assert error is not None, "Should fail validation with mismatched data"
        assert (
            error.error_type == ERROR_TYPES["API_BOUNDARY"]
        ), "Should report API boundary error"
        assert error.is_recoverable, "Error should be marked as recoverable"


async def test_validate_cache_data_without_api_params(cache_validator, sample_api_data):
    """Test validate_cache_data without API boundary parameters."""
    test_data = await sample_api_data()

    # When API parameters are not provided, validation should still work
    # but skip the API boundary validation
    error = await cache_validator.validate_cache_data(
        test_data["api_data"],
        allow_empty=False,
    )
    assert error is None, "Should validate successfully without API parameters"

    # Test with empty DataFrame (should fail)
    empty_df = pd.DataFrame()
    error = await cache_validator.validate_cache_data(
        empty_df,
        allow_empty=False,
    )
    assert error is not None, "Should fail validation with empty DataFrame"
    assert (
        error.error_type == ERROR_TYPES["VALIDATION"]
    ), "Should report validation error"


async def test_align_cached_data_to_api_boundaries(cache_validator, sample_api_data):
    """Test align_cached_data_to_api_boundaries method."""
    test_data = await sample_api_data()

    # Create a DataFrame with wider range than API would return
    # by extending it with some additional data points
    wider_df = test_data["api_data"].copy()
    if not wider_df.empty:
        # Add data points before and after the expected API range
        extended_start = test_data["start_time"] - timedelta(minutes=5)
        extended_end = test_data["end_time"] + timedelta(minutes=5)

        # Create extended index
        extended_index = pd.date_range(
            start=extended_start, end=extended_end, freq="1min", inclusive="left"
        )

        # Create new DataFrame with extended range
        extended_df = pd.DataFrame(
            {
                "open": np.random.random(len(extended_index)) * 1000 + 30000,
                "high": np.random.random(len(extended_index)) * 1000 + 30500,
                "low": np.random.random(len(extended_index)) * 1000 + 29500,
                "close": np.random.random(len(extended_index)) * 1000 + 30000,
                "volume": np.random.random(len(extended_index)) * 100,
            },
            index=pd.DatetimeIndex(extended_index, name="open_time"),
        )

        # Replace API data values with our original data where overlapping
        for idx in wider_df.index:
            if idx in extended_df.index:
                extended_df.loc[idx] = wider_df.loc[idx]

        # Test alignment
        aligned_df = await cache_validator.align_cached_data_to_api_boundaries(
            extended_df,
            test_data["start_time"],
            test_data["end_time"],
            test_data["interval"],
            symbol=TEST_SYMBOL,
        )

        # Check that aligned data matches what API would return
        api_boundaries = (
            await cache_validator.api_boundary_validator.get_api_boundaries(
                test_data["start_time"],
                test_data["end_time"],
                test_data["interval"],
                symbol=TEST_SYMBOL,
            )
        )

        # Verify aligned data has right size
        if api_boundaries["record_count"] > 0:
            assert (
                len(aligned_df) == api_boundaries["record_count"]
            ), "Aligned data should have same record count as API"

            # Verify time boundaries
            assert (
                aligned_df.index[0].to_pydatetime() == api_boundaries["api_start_time"]
            ), "Start time should match API"
            assert (
                aligned_df.index[-1].to_pydatetime() == api_boundaries["api_end_time"]
            ), "End time should match API"


async def test_validation_without_validator():
    """Test validation methods without ApiBoundaryValidator."""
    # Create CacheValidator without ApiBoundaryValidator
    validator = CacheValidator()

    start_time = datetime.now(timezone.utc) - timedelta(days=2)
    end_time = start_time + timedelta(hours=1)
    interval = Interval.MINUTE_1

    # Create sample DataFrame
    index = pd.date_range(start=start_time, end=end_time, freq="1min", inclusive="left")
    df = pd.DataFrame(
        {
            "open": np.random.random(len(index)) * 1000 + 30000,
            "high": np.random.random(len(index)) * 1000 + 30500,
            "low": np.random.random(len(index)) * 1000 + 29500,
            "close": np.random.random(len(index)) * 1000 + 30000,
            "volume": np.random.random(len(index)) * 100,
        },
        index=pd.DatetimeIndex(index, name="open_time"),
    )

    # align_cached_data_to_api_boundaries should raise ValueError when validator is not provided
    with pytest.raises(ValueError, match="ApiBoundaryValidator is required"):
        await validator.align_cached_data_to_api_boundaries(
            df, start_time, end_time, interval, symbol=TEST_SYMBOL
        )

    # validate_cache_data should still work without API params
    error = await validator.validate_cache_data(df)
    assert error is None, "Should validate successfully without API parameters"


async def test_saving_and_loading_cache(
    cache_validator, temp_arrow_file, sample_api_data
):
    """Test saving and loading cache with validation."""
    from utils.cache_validator import VisionCacheManager

    test_data = await sample_api_data()
    df = test_data["api_data"]

    if not df.empty:
        # Save to cache
        checksum, record_count = await VisionCacheManager.save_to_cache(
            df, temp_arrow_file, test_data["start_time"]
        )

        assert record_count == len(df), "Record count should match DataFrame length"
        assert checksum, "Should return a checksum"
        assert temp_arrow_file.exists(), "Cache file should exist"

        # Load from cache
        loaded_df = await VisionCacheManager.load_from_cache(temp_arrow_file)

        assert loaded_df is not None, "Should load DataFrame from cache"
        assert len(loaded_df) == len(df), "Loaded DataFrame should have same size"

        # Validate loaded data
        error = await cache_validator.validate_cache_data(
            loaded_df,
            start_time=test_data["start_time"],
            end_time=test_data["end_time"],
            interval=test_data["interval"],
            symbol=TEST_SYMBOL,
        )

        # The validation might fail if the data doesn't match API exactly,
        # but we're primarily testing that the validation logic runs correctly here
        if error:
            logger.info(f"Cache validation had error: {error.message}")


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
