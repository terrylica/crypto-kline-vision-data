#!/usr/bin/env python
"""Tests for the TimeRangeManager class in utils/time_alignment.py."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta

from utils.time_alignment import TimeRangeManager
from utils.market_constraints import Interval
from utils.logger_setup import get_logger

# Configure logger
logger = get_logger(__name__, "INFO")


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame with datetime index for testing."""
    dates = pd.date_range(
        start="2023-01-01 12:00:00",
        end="2023-01-01 13:00:00",
        freq="10min",
        tz=timezone.utc,
    )
    df = pd.DataFrame({"value": np.random.rand(len(dates))}, index=dates)
    return df


@pytest.fixture
def sample_dataframe_with_column():
    """Create a sample DataFrame with open_time column for testing."""
    dates = pd.date_range(
        start="2023-01-01 12:00:00",
        end="2023-01-01 13:00:00",
        freq="10min",
        tz=timezone.utc,
    )
    df = pd.DataFrame({"open_time": dates, "value": np.random.rand(len(dates))})
    return df


def test_validate_boundaries_with_datetime_index(sample_dataframe):
    """Test validation of boundaries with a DataFrame having a datetime index."""
    df = sample_dataframe

    # Case 1: Valid time range within data boundaries
    start_time = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 13, 0, tzinfo=timezone.utc)
    # Should not raise an error
    TimeRangeManager.validate_boundaries(df, start_time, end_time)

    # Case 2: Valid - Start time before data start (Binance API behavior)
    start_time = datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 13, 0, tzinfo=timezone.utc)
    # Should not raise an error
    TimeRangeManager.validate_boundaries(df, start_time, end_time)

    # Case 3: Valid - End time after data end (Binance API behavior)
    start_time = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 14, 0, tzinfo=timezone.utc)
    # Should not raise an error
    TimeRangeManager.validate_boundaries(df, start_time, end_time)

    # Case 4: Valid - Both start before data start and end after data end
    start_time = datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 14, 0, tzinfo=timezone.utc)
    # Should not raise an error
    TimeRangeManager.validate_boundaries(df, start_time, end_time)


def test_validate_boundaries_with_open_time_column(sample_dataframe_with_column):
    """Test validation of boundaries with a DataFrame having an open_time column."""
    df = sample_dataframe_with_column

    # Case 1: Valid time range within data boundaries
    start_time = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 13, 0, tzinfo=timezone.utc)
    # Should not raise an error
    TimeRangeManager.validate_boundaries(df, start_time, end_time)

    # Case 2: Valid - Start time before data start (Binance API behavior)
    start_time = datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 13, 0, tzinfo=timezone.utc)
    # Should not raise an error
    TimeRangeManager.validate_boundaries(df, start_time, end_time)

    # Case 3: Valid - End time after data end (Binance API behavior)
    start_time = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 14, 0, tzinfo=timezone.utc)
    # Should not raise an error
    TimeRangeManager.validate_boundaries(df, start_time, end_time)


def test_validate_boundaries_failure_cases(sample_dataframe):
    """Test validation of boundaries failure cases."""
    df = sample_dataframe

    # Case 1: Invalid - Start time after data start (data starts too late)
    start_time = datetime(2023, 1, 1, 12, 30, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 13, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="Data starts too late"):
        TimeRangeManager.validate_boundaries(df, start_time, end_time)

    # Case 2: Empty DataFrame
    empty_df = pd.DataFrame()
    start_time = datetime(2023, 1, 1, 12, 0, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 13, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="DataFrame is empty"):
        TimeRangeManager.validate_boundaries(empty_df, start_time, end_time)

    # Case 3: No datetime index or open_time column
    invalid_df = pd.DataFrame({"value": [1, 2, 3]})
    with pytest.raises(ValueError, match="No datetime index or open_time column found"):
        TimeRangeManager.validate_boundaries(invalid_df, start_time, end_time)


def test_filter_dataframe(sample_dataframe):
    """Test filtering DataFrame based on time range."""
    df = sample_dataframe

    # Filter within data boundaries
    start_time = datetime(2023, 1, 1, 12, 10, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 1, 12, 40, tzinfo=timezone.utc)

    filtered_df = TimeRangeManager.filter_dataframe(df, start_time, end_time)

    # Check that the filtered dataframe has the expected range
    assert filtered_df.index.min() >= start_time
    assert filtered_df.index.max() < end_time

    # Check number of entries (should be 3 entries: 12:10, 12:20, 12:30)
    assert len(filtered_df) == 3


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
