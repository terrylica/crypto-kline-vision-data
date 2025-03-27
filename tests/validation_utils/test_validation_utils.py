#!/usr/bin/env python
"""Tests for validation_utils module."""

import os
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
import tempfile
import pytest
import pandas as pd
import numpy as np

from utils.validation_utils import (
    validate_dates,
    validate_time_window,
    validate_time_range,
    validate_interval,
    validate_symbol_format,
    validate_data_availability,
    is_data_likely_available,
    validate_dataframe,
    format_dataframe,
    validate_cache_integrity,
    calculate_checksum,
    ValidationOptions,
    CacheValidationError,
    ERROR_TYPES,
    ApiValidator,
    DataValidator,
)
from utils.market_constraints import Interval, MarketType
from utils.api_boundary_validator import ApiBoundaryValidator


class TestValidationFunctions:
    """Tests for basic validation functions."""

    def test_validate_dates_valid(self):
        """Test validate_dates with valid dates."""
        start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 2, tzinfo=timezone.utc)
        # Should not raise
        validate_dates(start_time, end_time)

    def test_validate_dates_invalid_order(self):
        """Test validate_dates with invalid date order."""
        start_time = datetime(2023, 1, 2, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="Start time must be before end time"):
            validate_dates(start_time, end_time)

    def test_validate_dates_timezone_awareness(self):
        """Test validate_dates with timezone-naive dates."""
        start_time = datetime(2023, 1, 1)  # naive
        end_time = datetime(2023, 1, 2, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="must be timezone-aware"):
            validate_dates(start_time, end_time)

    def test_validate_time_window_valid(self):
        """Test validate_time_window with valid time window."""
        start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 2, tzinfo=timezone.utc)
        # Should not raise
        validate_time_window(start_time, end_time)

    def test_validate_time_window_too_large(self):
        """Test validate_time_window with too large window."""
        from utils.config import MAX_TIME_RANGE

        start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end_time = start_time + MAX_TIME_RANGE + timedelta(seconds=1)
        with pytest.raises(ValueError, match="Time range exceeds maximum allowed"):
            validate_time_window(start_time, end_time)

    def test_validate_time_range(self):
        """Test validate_time_range normalization."""
        # Test with timezone-naive datetimes
        start_time = datetime(2023, 1, 1)
        end_time = datetime(2023, 1, 2)
        normalized_start, normalized_end = validate_time_range(start_time, end_time)
        assert normalized_start.tzinfo is not None
        assert normalized_end.tzinfo is not None

    def test_validate_interval_valid(self):
        """Test validate_interval with valid intervals."""
        # Valid spot intervals
        validate_interval("1s", "SPOT")
        validate_interval("1m", "SPOT")
        validate_interval("1h", "SPOT")
        validate_interval("1d", "SPOT")

        # Valid futures intervals
        validate_interval("1m", "FUTURES")
        validate_interval("1h", "FUTURES")

    def test_validate_interval_invalid(self):
        """Test validate_interval with invalid intervals."""
        # Invalid interval for spot
        with pytest.raises(ValueError, match="Invalid interval"):
            validate_interval("2m", "SPOT")

        # 1s not available for futures
        with pytest.raises(ValueError, match="Invalid interval"):
            validate_interval("1s", "FUTURES")

    def test_validate_symbol_format_valid(self):
        """Test validate_symbol_format with valid symbols."""
        validate_symbol_format("BTCUSDT")
        validate_symbol_format("ETHBTC")
        validate_symbol_format("XRPBNB")

    def test_validate_symbol_format_invalid(self):
        """Test validate_symbol_format with invalid symbols."""
        # Lowercase
        with pytest.raises(ValueError, match="should be uppercase"):
            validate_symbol_format("btcusdt")

        # Empty
        with pytest.raises(ValueError, match="must be a non-empty string"):
            validate_symbol_format("")

    def test_validate_data_availability(self, caplog):
        """Test validate_data_availability warning for recent data."""
        start_time = datetime.now(timezone.utc) - timedelta(days=10)
        end_time = datetime.now(timezone.utc) - timedelta(hours=1)
        validate_data_availability(start_time, end_time)
        assert "may not be fully consolidated" in caplog.text

    def test_is_data_likely_available(self):
        """Test is_data_likely_available function."""
        # Old data should be available
        old_date = datetime.now(timezone.utc) - timedelta(days=10)
        assert is_data_likely_available(old_date) is True

        # Very recent data may not be available
        recent_date = datetime.now(timezone.utc) - timedelta(minutes=10)
        assert is_data_likely_available(recent_date) is False


class TestDataFrameValidation:
    """Tests for DataFrame validation functions."""

    @pytest.fixture
    def sample_df(self):
        """Create a valid sample DataFrame."""
        index = pd.date_range(
            start=datetime(2023, 1, 1, tzinfo=timezone.utc), periods=10, freq="1h"
        )
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
        return df

    def test_validate_dataframe_valid(self, sample_df):
        """Test validate_dataframe with valid DataFrame."""
        # Should not raise
        validate_dataframe(sample_df)

    def test_validate_dataframe_empty(self):
        """Test validate_dataframe with empty DataFrame."""
        df = pd.DataFrame()
        # Should not raise for empty DataFrame
        validate_dataframe(df)

    def test_validate_dataframe_non_datetime_index(self, sample_df):
        """Test validate_dataframe with non-DatetimeIndex."""
        df = sample_df.reset_index()
        with pytest.raises(ValueError, match="index must be DatetimeIndex"):
            validate_dataframe(df)

    def test_validate_dataframe_naive_timezone(self, sample_df):
        """Test validate_dataframe with timezone-naive index."""
        df = sample_df.copy()
        df.index = df.index.tz_localize(None)
        with pytest.raises(ValueError, match="index must be timezone-aware"):
            validate_dataframe(df)

    def test_validate_dataframe_wrong_index_name(self, sample_df):
        """Test validate_dataframe with wrong index name."""
        df = sample_df.copy()
        df.index.name = "timestamp"
        with pytest.raises(ValueError, match="index must be named"):
            validate_dataframe(df)

    def test_validate_dataframe_duplicate_indices(self, sample_df):
        """Test validate_dataframe with duplicate indices."""
        df = pd.concat([sample_df, sample_df.iloc[0:1]])
        with pytest.raises(ValueError, match="duplicate timestamps"):
            validate_dataframe(df)

    def test_validate_dataframe_unsorted_index(self, sample_df):
        """Test validate_dataframe with unsorted index."""
        df = sample_df.iloc[::-1]  # Reverse order
        with pytest.raises(ValueError, match="monotonically increasing"):
            validate_dataframe(df)

    def test_validate_dataframe_missing_columns(self, sample_df):
        """Test validate_dataframe with missing required columns."""
        df = sample_df.drop(columns=["volume"])
        with pytest.raises(ValueError, match="missing required columns"):
            validate_dataframe(df)

    def test_format_dataframe_empty(self):
        """Test format_dataframe with empty DataFrame."""
        df = pd.DataFrame()
        formatted = format_dataframe(df)
        assert formatted.empty
        assert isinstance(formatted.index, pd.DatetimeIndex)
        assert formatted.index.name == "open_time"
        assert formatted.index.tz == timezone.utc

    def test_format_dataframe_timezone_conversion(self, sample_df):
        """Test format_dataframe timezone conversion."""
        df = sample_df.copy()
        df.index = df.index.tz_convert("US/Eastern")
        formatted = format_dataframe(df)
        assert formatted.index.tz == timezone.utc

    def test_format_dataframe_fixes_duplicates(self, sample_df):
        """Test format_dataframe fixes duplicate indices."""
        df = pd.concat([sample_df, sample_df.iloc[0:1]])
        formatted = format_dataframe(df)
        assert not formatted.index.has_duplicates

    def test_format_dataframe_sorts_index(self, sample_df):
        """Test format_dataframe sorts index."""
        df = sample_df.iloc[::-1]  # Reverse order
        formatted = format_dataframe(df)
        assert formatted.index.is_monotonic_increasing


class TestFileValidation:
    """Tests for file validation functions."""

    def test_validate_cache_integrity_missing_file(self):
        """Test validate_cache_integrity with missing file."""
        result = validate_cache_integrity("/path/to/nonexistent/file")
        assert result is not None
        assert result["error_type"] == "file_missing"
        assert "does not exist" in result["message"]

    def test_validate_cache_integrity_file_too_small(self):
        """Test validate_cache_integrity with file too small."""
        with tempfile.NamedTemporaryFile() as temp_file:
            # Write a small amount of data
            temp_file.write(b"test")
            temp_file.flush()
            result = validate_cache_integrity(temp_file.name, min_size=1000)
            assert result is not None
            assert result["error_type"] == "file_too_small"
            assert "too small" in result["message"]

    def test_validate_cache_integrity_file_valid(self):
        """Test validate_cache_integrity with valid file."""
        with tempfile.NamedTemporaryFile() as temp_file:
            # Write enough data
            temp_file.write(b"x" * 2000)
            temp_file.flush()
            result = validate_cache_integrity(temp_file.name, min_size=1000)
            assert result is None

    def test_calculate_checksum(self):
        """Test calculate_checksum function."""
        with tempfile.NamedTemporaryFile() as temp_file:
            data = b"test data for checksum"
            temp_file.write(data)
            temp_file.flush()

            # Calculate expected checksum
            expected = hashlib.sha256(data).hexdigest()

            # Test our function
            result = calculate_checksum(Path(temp_file.name))
            assert result == expected


@pytest.fixture
async def api_boundary_validator():
    """Fixture for ApiBoundaryValidator with proper resource management."""
    validator = ApiBoundaryValidator(MarketType.SPOT)
    yield validator
    await validator.close()


@pytest.fixture
def api_validator(api_boundary_validator):
    """Fixture for ApiValidator with injected ApiBoundaryValidator."""
    return ApiValidator(api_boundary_validator)


@pytest.fixture
def data_validator(api_validator):
    """Fixture for DataValidator with injected ApiValidator."""
    return DataValidator(api_validator)


@pytest.fixture
async def sample_api_data(api_boundary_validator):
    """Generate sample data that matches API response."""
    # Recent time range for actual API data
    start_time = datetime.now(timezone.utc) - timedelta(days=1)
    start_time = start_time.replace(minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(hours=1)
    interval = Interval.MINUTE_1
    symbol = "BTCUSDT"

    # Get actual data from API
    api_response = await api_boundary_validator._call_api(
        start_time, end_time, interval, symbol=symbol
    )

    # Convert to DataFrame
    api_data = pd.DataFrame(
        api_response,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_volume",
            "taker_buy_quote_volume",
            "ignored",
        ],
    )

    # Convert timestamp to datetime and set as index
    api_data["open_time"] = pd.to_datetime(api_data["open_time"], unit="ms", utc=True)
    api_data = api_data.set_index("open_time")

    # Create modified version for validation testing
    modified_data = api_data.copy()
    if not modified_data.empty:
        # Drop the last row to make it different
        modified_data = modified_data.iloc[:-1]

    return {
        "api_data": api_data,
        "modified_data": modified_data,
        "start_time": start_time,
        "end_time": end_time,
        "interval": interval,
        "symbol": symbol,
    }


class TestValidatorClasses:
    """Tests for validator classes."""

    async def test_api_validator_validate_api_time_range(self, api_validator):
        """Test ApiValidator.validate_api_time_range."""
        # Use recent time range that should have data
        start_time = datetime.now(timezone.utc) - timedelta(days=1)
        end_time = start_time + timedelta(hours=1)
        interval = Interval.MINUTE_1

        result = await api_validator.validate_api_time_range(
            start_time, end_time, interval
        )
        assert isinstance(result, bool)

    async def test_api_validator_get_api_aligned_boundaries(self, api_validator):
        """Test ApiValidator.get_api_aligned_boundaries."""
        start_time = datetime.now(timezone.utc) - timedelta(days=1)
        end_time = start_time + timedelta(hours=1)
        interval = Interval.MINUTE_1

        boundaries = await api_validator.get_api_aligned_boundaries(
            start_time, end_time, interval
        )
        assert "api_start_time" in boundaries
        assert "api_end_time" in boundaries
        assert "record_count" in boundaries

    async def test_api_validator_does_data_range_match_api_response(
        self, api_validator, sample_api_data
    ):
        """Test ApiValidator.does_data_range_match_api_response."""
        # Test with data that matches API response
        result = await api_validator.does_data_range_match_api_response(
            sample_api_data["api_data"],
            sample_api_data["start_time"],
            sample_api_data["end_time"],
            sample_api_data["interval"],
            sample_api_data["symbol"],
        )
        assert result is True

        # Test with modified data if it's different
        if not sample_api_data["modified_data"].empty and len(
            sample_api_data["modified_data"]
        ) != len(sample_api_data["api_data"]):
            result = await api_validator.does_data_range_match_api_response(
                sample_api_data["modified_data"],
                sample_api_data["start_time"],
                sample_api_data["end_time"],
                sample_api_data["interval"],
                sample_api_data["symbol"],
            )
            assert result is False

    async def test_data_validator_validate_data(self, data_validator, sample_api_data):
        """Test DataValidator.validate_data."""
        # Test with valid data
        options = ValidationOptions(
            allow_empty=False,
            start_time=sample_api_data["start_time"],
            end_time=sample_api_data["end_time"],
            interval=sample_api_data["interval"],
            symbol=sample_api_data["symbol"],
        )

        error = await data_validator.validate_data(sample_api_data["api_data"], options)
        assert error is None

        # Test with empty DataFrame
        empty_df = pd.DataFrame()
        error = await data_validator.validate_data(empty_df, options)
        assert error is not None
        assert error.error_type == ERROR_TYPES["VALIDATION"]
        assert "empty" in error.message

    async def test_data_validator_align_data_to_api_boundaries(
        self, data_validator, sample_api_data
    ):
        """Test DataValidator.align_data_to_api_boundaries."""
        # Create a larger DataFrame that needs alignment
        larger_start = sample_api_data["start_time"] - timedelta(minutes=30)
        larger_end = sample_api_data["end_time"] + timedelta(minutes=30)

        # Create a DataFrame with expanded time range
        larger_index = pd.date_range(
            start=larger_start,
            end=larger_end,
            freq="1min",
            name="open_time",
            tz=timezone.utc,
        )

        # Create expanded DataFrame
        expanded_df = pd.DataFrame(
            {
                "open": np.random.random(len(larger_index)) * 1000 + 30000,
                "high": np.random.random(len(larger_index)) * 1000 + 30500,
                "low": np.random.random(len(larger_index)) * 1000 + 29500,
                "close": np.random.random(len(larger_index)) * 1000 + 30000,
                "volume": np.random.random(len(larger_index)) * 100,
            },
            index=larger_index,
        )

        # Align data to API boundaries
        aligned_df = await data_validator.align_data_to_api_boundaries(
            expanded_df,
            sample_api_data["start_time"],
            sample_api_data["end_time"],
            sample_api_data["interval"],
            sample_api_data["symbol"],
        )

        # Get API boundaries to verify alignment
        api_boundaries = await data_validator.api_validator.get_api_aligned_boundaries(
            sample_api_data["start_time"],
            sample_api_data["end_time"],
            sample_api_data["interval"],
            sample_api_data["symbol"],
        )

        if api_boundaries["record_count"] > 0:
            # Verify aligned data has right boundaries
            assert len(aligned_df) <= api_boundaries["record_count"]
            if not aligned_df.empty:
                assert aligned_df.index.min() >= api_boundaries["api_start_time"]
                assert aligned_df.index.max() <= api_boundaries["api_end_time"]
