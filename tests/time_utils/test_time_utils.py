#!/usr/bin/env python
"""Unit tests for the time_utils module."""

import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta

from utils.time_utils import (
    enforce_utc_timezone,
    validate_time_window,
    get_interval_micros,
    get_interval_seconds,
    get_interval_timedelta,
    get_interval_floor,
    get_interval_ceiling,
    get_bar_close_time,
    is_bar_complete,
    filter_dataframe_by_time,
    align_time_boundaries,
    estimate_record_count,
    vision_api_time_window_alignment,
    align_vision_api_to_rest,
)
from utils.market_constraints import Interval


class TestTimeUtils:
    """Test cases for time_utils module."""

    def test_enforce_utc_timezone(self):
        """Test enforce_utc_timezone function."""
        # Test with naive datetime
        naive_dt = datetime(2023, 1, 1, 12, 0, 0)
        result = enforce_utc_timezone(naive_dt)
        assert result.tzinfo is not None
        assert result.tzinfo == timezone.utc

        # Test with non-UTC timezone
        est = timezone(timedelta(hours=-5))
        est_dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=est)
        result = enforce_utc_timezone(est_dt)
        assert result.tzinfo == timezone.utc
        assert result.hour == 17  # 12 EST = 17 UTC

        # Test with UTC timezone
        utc_dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = enforce_utc_timezone(utc_dt)
        assert result is not utc_dt  # Should be a new object
        assert result.tzinfo == timezone.utc
        assert result.hour == 12

    def test_validate_time_window_valid(self):
        """Test validate_time_window with valid inputs."""
        start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end = datetime(2023, 1, 2, tzinfo=timezone.utc)
        # Should not raise exception
        validate_time_window(start, end)

    def test_validate_time_window_invalid_order(self):
        """Test validate_time_window with start after end."""
        start = datetime(2023, 1, 2, tzinfo=timezone.utc)
        end = datetime(2023, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="Start time .* must be before end time"):
            validate_time_window(start, end)

    def test_validate_time_window_too_large(self):
        """Test validate_time_window with range too large."""
        start = datetime(2023, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 2, 1, tzinfo=timezone.utc)  # More than 1 year
        with pytest.raises(ValueError, match="Time range too large"):
            validate_time_window(start, end)

    def test_get_interval_micros(self):
        """Test get_interval_micros function."""
        # Test with different intervals
        assert get_interval_micros(Interval.SECOND_1) == 1_000_000  # 1 second
        assert get_interval_micros(Interval.MINUTE_1) == 60_000_000  # 1 minute
        assert get_interval_micros(Interval.HOUR_1) == 3_600_000_000  # 1 hour
        assert get_interval_micros(Interval.DAY_1) == 86_400_000_000  # 1 day

    def test_get_interval_seconds(self):
        """Test get_interval_seconds function."""
        # Test with different intervals
        assert get_interval_seconds(Interval.SECOND_1) == 1
        assert get_interval_seconds(Interval.MINUTE_1) == 60
        assert get_interval_seconds(Interval.HOUR_1) == 3600
        assert get_interval_seconds(Interval.DAY_1) == 86400

    def test_get_interval_timedelta(self):
        """Test get_interval_timedelta function."""
        # Test with different intervals
        assert get_interval_timedelta(Interval.SECOND_1) == timedelta(seconds=1)
        assert get_interval_timedelta(Interval.MINUTE_1) == timedelta(minutes=1)
        assert get_interval_timedelta(Interval.HOUR_1) == timedelta(hours=1)
        assert get_interval_timedelta(Interval.DAY_1) == timedelta(days=1)

    def test_get_interval_floor(self):
        """Test get_interval_floor function."""
        # Test with 1-minute interval
        dt = datetime(2023, 1, 1, 12, 30, 45, tzinfo=timezone.utc)
        result = get_interval_floor(dt, Interval.MINUTE_1)
        expected = datetime(2023, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
        assert result == expected

        # Test with 1-hour interval
        result = get_interval_floor(dt, Interval.HOUR_1)
        expected = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert result == expected

    def test_get_interval_ceiling(self):
        """Test get_interval_ceiling function."""
        # Test with 1-minute interval
        dt = datetime(2023, 1, 1, 12, 30, 45, tzinfo=timezone.utc)
        result = get_interval_ceiling(dt, Interval.MINUTE_1)
        expected = datetime(2023, 1, 1, 12, 31, 0, tzinfo=timezone.utc)
        assert result == expected

        # Test with 1-hour interval
        result = get_interval_ceiling(dt, Interval.HOUR_1)
        expected = datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
        assert result == expected

        # Test with timestamp exactly on interval boundary
        dt = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = get_interval_ceiling(dt, Interval.HOUR_1)
        assert result == dt  # Should return same time if already on boundary

    def test_get_bar_close_time(self):
        """Test get_bar_close_time function."""
        # Test with 1-minute interval
        open_time = datetime(2023, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
        result = get_bar_close_time(open_time, Interval.MINUTE_1)
        expected = datetime(2023, 1, 1, 12, 30, 59, 999999, tzinfo=timezone.utc)
        assert result == expected

        # Test with 1-hour interval
        open_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = get_bar_close_time(open_time, Interval.HOUR_1)
        expected = datetime(2023, 1, 1, 12, 59, 59, 999999, tzinfo=timezone.utc)
        assert result == expected

    def test_is_bar_complete(self):
        """Test is_bar_complete function."""
        # Test with bar that should be complete
        bar_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        current_time = datetime(2023, 1, 1, 12, 1, 30, tzinfo=timezone.utc)
        assert is_bar_complete(bar_time, Interval.MINUTE_1, current_time)

        # Test with bar that should not be complete
        bar_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        current_time = datetime(2023, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
        assert not is_bar_complete(bar_time, Interval.MINUTE_1, current_time)

    def test_filter_dataframe_by_time(self):
        """Test filter_dataframe_by_time function."""
        # Create test dataframe
        dates = pd.date_range(
            start="2023-01-01", end="2023-01-10", freq="D", tz=timezone.utc
        )
        df = pd.DataFrame({"value": range(len(dates))}, index=dates)

        # Test filtering
        start = datetime(2023, 1, 3, tzinfo=timezone.utc)
        end = datetime(2023, 1, 7, tzinfo=timezone.utc)
        result = filter_dataframe_by_time(df, start, end)

        # Expected: dates from Jan 3 to Jan 6 (end is exclusive)
        assert len(result) == 4
        assert result.index[0].day == 3
        assert result.index[-1].day == 6

    def test_align_time_boundaries(self):
        """Test align_time_boundaries function."""
        # Test with 1-minute interval, both times on boundary
        start = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2023, 1, 1, 12, 5, 0, tzinfo=timezone.utc)
        aligned_start, aligned_end = align_time_boundaries(
            start, end, Interval.MINUTE_1
        )
        assert aligned_start == start
        assert aligned_end == end

        # Test with 1-minute interval, start off boundary
        start = datetime(2023, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
        aligned_start, aligned_end = align_time_boundaries(
            start, end, Interval.MINUTE_1
        )
        assert aligned_start == datetime(2023, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
        assert aligned_end == end

        # Test with 1-minute interval, end off boundary
        start = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2023, 1, 1, 12, 5, 30, tzinfo=timezone.utc)
        aligned_start, aligned_end = align_time_boundaries(
            start, end, Interval.MINUTE_1
        )
        assert aligned_start == start
        assert aligned_end == datetime(2023, 1, 1, 12, 5, 0, tzinfo=timezone.utc)

    def test_estimate_record_count(self):
        """Test estimate_record_count function."""
        # Test with 1-minute interval over 5 minutes
        start = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        end = datetime(2023, 1, 1, 12, 5, 0, tzinfo=timezone.utc)
        count = estimate_record_count(start, end, Interval.MINUTE_1)
        assert count == 6  # 12:00, 12:01, 12:02, 12:03, 12:04, 12:05

        # Test with 1-hour interval over 1 day
        start = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2023, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        count = estimate_record_count(start, end, Interval.HOUR_1)
        assert count == 25  # 24 hours plus the 00:00 of day 2

    def test_vision_api_time_window_alignment(self):
        """Test vision_api_time_window_alignment function."""
        # Test with 1-minute interval
        start = datetime(2023, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
        end = datetime(2023, 1, 1, 12, 5, 30, tzinfo=timezone.utc)
        aligned_start, aligned_end = vision_api_time_window_alignment(
            start, end, Interval.MINUTE_1
        )
        assert aligned_start == datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert aligned_end == datetime(2023, 1, 1, 12, 5, 0, tzinfo=timezone.utc)

    def test_align_vision_api_to_rest(self):
        """Test align_vision_api_to_rest function."""
        # Test with 1-minute interval
        start = datetime(2023, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
        end = datetime(2023, 1, 1, 12, 5, 30, tzinfo=timezone.utc)
        result = align_vision_api_to_rest(start, end, Interval.MINUTE_1)

        # Check result structure and values
        assert isinstance(result, dict)
        assert "original_start" in result
        assert "original_end" in result
        assert "adjusted_start" in result
        assert "adjusted_end" in result
        assert "interval" in result
        assert "interval_micros" in result

        assert result["adjusted_start"] == datetime(
            2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc
        )
        assert result["adjusted_end"] == datetime(
            2023, 1, 1, 12, 6, 0, tzinfo=timezone.utc
        )
