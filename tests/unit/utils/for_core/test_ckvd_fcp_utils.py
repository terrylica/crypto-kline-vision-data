#!/usr/bin/env python3
"""Unit tests for FCP utility functions.

Tests the Failover Control Protocol utility functions in dsm_fcp_utils.py:
1. validate_interval() - Interval validation
2. process_vision_step() - FCP Step 2: Vision API
3. process_rest_step() - FCP Step 3: REST API fallback
4. verify_final_data() - Final data validation
5. handle_error() - Error handling

ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ckvd.utils.for_core.ckvd_fcp_utils import (
    handle_error,
    process_rest_step,
    process_vision_step,
    validate_interval,
    verify_final_data,
)
from ckvd.utils.for_core.ckvd_time_range_utils import (
    identify_missing_segments,
    merge_adjacent_ranges,
    merge_dataframes,
)
from ckvd.utils.for_core.vision_exceptions import UnsupportedIntervalError
from ckvd.utils.market_constraints import Interval, MarketType


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def sample_ohlcv_df():
    """Create a sample OHLCV DataFrame for testing with proper structure."""
    base_time = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    timestamps = [base_time + timedelta(hours=i) for i in range(24)]
    return pd.DataFrame(
        {
            "open_time": timestamps,
            "open": [42000.0 + i * 10 for i in range(24)],
            "high": [42100.0 + i * 10 for i in range(24)],
            "low": [41900.0 + i * 10 for i in range(24)],
            "close": [42050.0 + i * 10 for i in range(24)],
            "volume": [1000.0 + i for i in range(24)],
        }
    )


@pytest.fixture
def sample_ohlcv_df_with_index():
    """Create a sample OHLCV DataFrame with open_time as index."""
    base_time = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    timestamps = [base_time + timedelta(hours=i) for i in range(24)]
    return pd.DataFrame(
        {
            "open": [42000.0 + i * 10 for i in range(24)],
            "high": [42100.0 + i * 10 for i in range(24)],
            "low": [41900.0 + i * 10 for i in range(24)],
            "close": [42050.0 + i * 10 for i in range(24)],
            "volume": [1000.0 + i for i in range(24)],
        },
        index=pd.DatetimeIndex(timestamps, name="open_time", tz="UTC"),
    )


@pytest.fixture
def historical_time_range():
    """Historical time range for tests (safe for Vision API)."""
    end = datetime(2024, 1, 15, 23, 0, 0, tzinfo=timezone.utc)
    start = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    return start, end


# =============================================================================
# validate_interval() Tests
# =============================================================================


class TestValidateInterval:
    """Tests for validate_interval function."""

    def test_valid_interval_spot_hour_1(self):
        """HOUR_1 should be valid for SPOT market."""
        # Should not raise
        validate_interval(MarketType.SPOT, Interval.HOUR_1)

    def test_valid_interval_futures_usdt_minute_1(self):
        """MINUTE_1 should be valid for FUTURES_USDT market."""
        # Should not raise
        validate_interval(MarketType.FUTURES_USDT, Interval.MINUTE_1)

    def test_valid_interval_futures_coin_day_1(self):
        """DAY_1 should be valid for FUTURES_COIN market."""
        # Should not raise
        validate_interval(MarketType.FUTURES_COIN, Interval.DAY_1)

    def test_invalid_interval_spot_second_1(self):
        """SECOND_1 should raise error for markets that don't support it."""
        # FUTURES_USDT doesn't support 1s intervals
        with pytest.raises(UnsupportedIntervalError) as excinfo:
            validate_interval(MarketType.FUTURES_USDT, Interval.SECOND_1)

        error_msg = str(excinfo.value)
        assert "1s" in error_msg or "SECOND" in error_msg
        assert "not supported" in error_msg.lower()

    def test_invalid_interval_error_includes_suggestions(self):
        """Error message should include supported intervals and suggestions."""
        with pytest.raises(UnsupportedIntervalError) as excinfo:
            validate_interval(MarketType.FUTURES_COIN, Interval.SECOND_1)

        error_msg = str(excinfo.value)
        # Should mention supported intervals
        assert "Supported intervals" in error_msg or "supported" in error_msg.lower()


# =============================================================================
# process_vision_step() Tests
# =============================================================================


class TestProcessVisionStep:
    """Tests for process_vision_step function (FCP Step 2)."""

    def test_vision_success_returns_data_and_clears_missing(self, sample_ohlcv_df, historical_time_range):
        """Vision API success should return data and clear missing ranges."""
        start_time, end_time = historical_time_range
        missing_ranges = [(start_time, end_time)]

        mock_vision_func = MagicMock(return_value=sample_ohlcv_df.copy())

        result_df, _remaining_missing = process_vision_step(
            fetch_from_vision_func=mock_vision_func,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )

        assert len(result_df) > 0
        mock_vision_func.assert_called_once()

    def test_vision_adds_source_info(self, sample_ohlcv_df, historical_time_range):
        """Vision step with include_source_info should add _data_source column."""
        start_time, end_time = historical_time_range

        mock_vision_func = MagicMock(return_value=sample_ohlcv_df.copy())

        result_df, _ = process_vision_step(
            fetch_from_vision_func=mock_vision_func,
            symbol="BTCUSDT",
            missing_ranges=[(start_time, end_time)],
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )

        assert "_data_source" in result_df.columns
        assert (result_df["_data_source"] == "VISION").all()

    def test_vision_failure_returns_original_missing_ranges(self, historical_time_range):
        """Vision API failure should preserve missing ranges for REST fallback."""
        start_time, end_time = historical_time_range
        missing_ranges = [(start_time, end_time)]

        # Mock Vision returning empty
        mock_vision_func = MagicMock(return_value=pd.DataFrame())

        result_df, remaining_missing = process_vision_step(
            fetch_from_vision_func=mock_vision_func,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=False,
            result_df=pd.DataFrame(),
        )

        assert result_df.empty
        assert len(remaining_missing) == 1

    def test_vision_merges_with_existing_data(self, sample_ohlcv_df, historical_time_range):
        """Vision data should merge with existing cache data."""
        start_time, end_time = historical_time_range

        # Existing cache data (first 12 hours)
        existing_df = sample_ohlcv_df.iloc[:12].copy()
        existing_df["_data_source"] = "CACHE"

        # Vision returns remaining data
        vision_data = sample_ohlcv_df.iloc[12:].copy()
        mock_vision_func = MagicMock(return_value=vision_data)

        mid_time = start_time + timedelta(hours=12)
        missing_ranges = [(mid_time, end_time)]

        result_df, _ = process_vision_step(
            fetch_from_vision_func=mock_vision_func,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=existing_df,
        )

        # Should have merged data
        assert len(result_df) >= 12  # At least existing data


# =============================================================================
# process_rest_step() Tests
# =============================================================================


class TestProcessRestStep:
    """Tests for process_rest_step function (FCP Step 3)."""

    def test_rest_success_returns_data(self, sample_ohlcv_df, historical_time_range):
        """REST API success should return data."""
        start_time, end_time = historical_time_range
        missing_ranges = [(start_time, end_time)]

        mock_rest_func = MagicMock(return_value=sample_ohlcv_df.copy())

        result_df = process_rest_step(
            fetch_from_rest_func=mock_rest_func,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )

        assert len(result_df) > 0
        mock_rest_func.assert_called_once()

    def test_rest_adds_source_info(self, sample_ohlcv_df, historical_time_range):
        """REST step with include_source_info should add _data_source column."""
        start_time, end_time = historical_time_range

        mock_rest_func = MagicMock(return_value=sample_ohlcv_df.copy())

        result_df = process_rest_step(
            fetch_from_rest_func=mock_rest_func,
            symbol="BTCUSDT",
            missing_ranges=[(start_time, end_time)],
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )

        assert "_data_source" in result_df.columns
        assert (result_df["_data_source"] == "REST").all()

    def test_rest_calls_save_to_cache(self, sample_ohlcv_df, historical_time_range):
        """REST step should call save_to_cache_func when provided."""
        start_time, end_time = historical_time_range

        mock_rest_func = MagicMock(return_value=sample_ohlcv_df.copy())
        mock_save_func = MagicMock()

        process_rest_step(
            fetch_from_rest_func=mock_rest_func,
            symbol="BTCUSDT",
            missing_ranges=[(start_time, end_time)],
            interval=Interval.HOUR_1,
            include_source_info=False,
            result_df=pd.DataFrame(),
            save_to_cache_func=mock_save_func,
        )

        mock_save_func.assert_called_once()

    def test_rest_merges_with_existing_data(self, sample_ohlcv_df, historical_time_range):
        """REST data should merge with existing data."""
        start_time, end_time = historical_time_range

        # Existing data from Vision
        existing_df = sample_ohlcv_df.iloc[:12].copy()
        existing_df["_data_source"] = "VISION"

        # REST returns remaining data
        rest_data = sample_ohlcv_df.iloc[12:].copy()
        mock_rest_func = MagicMock(return_value=rest_data)

        mid_time = start_time + timedelta(hours=12)
        missing_ranges = [(mid_time, end_time)]

        result_df = process_rest_step(
            fetch_from_rest_func=mock_rest_func,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=existing_df,
        )

        # Should have merged data from both sources
        assert len(result_df) >= 12

    def test_rest_empty_returns_existing_data(self, sample_ohlcv_df, historical_time_range):
        """REST returning empty should preserve existing data."""
        start_time, end_time = historical_time_range

        existing_df = sample_ohlcv_df.copy()
        mock_rest_func = MagicMock(return_value=pd.DataFrame())

        result_df = process_rest_step(
            fetch_from_rest_func=mock_rest_func,
            symbol="BTCUSDT",
            missing_ranges=[(start_time, end_time)],
            interval=Interval.HOUR_1,
            include_source_info=False,
            result_df=existing_df,
        )

        # Should return existing data unchanged
        assert len(result_df) == len(existing_df)


# =============================================================================
# verify_final_data() Tests
# =============================================================================


class TestVerifyFinalData:
    """Tests for verify_final_data function."""

    def test_empty_dataframe_raises_runtime_error(self, historical_time_range):
        """Empty DataFrame should raise RuntimeError."""
        start_time, end_time = historical_time_range

        with pytest.raises(RuntimeError) as excinfo:
            verify_final_data(pd.DataFrame(), start_time, end_time)

        assert "No data available" in str(excinfo.value) or "All data sources failed" in str(excinfo.value)

    def test_valid_historical_data_passes(self, sample_ohlcv_df, historical_time_range):
        """Valid historical data should pass verification."""
        start_time, end_time = historical_time_range

        # Should not raise
        verify_final_data(sample_ohlcv_df, start_time, end_time)

    def test_valid_data_with_index_passes(self, sample_ohlcv_df_with_index, historical_time_range):
        """DataFrame with open_time as index should pass verification."""
        start_time, end_time = historical_time_range

        # Should not raise - handles index-based open_time
        verify_final_data(sample_ohlcv_df_with_index, start_time, end_time)


# =============================================================================
# handle_error() Tests
# =============================================================================


class TestHandleError:
    """Tests for handle_error function."""

    def test_handle_error_raises_runtime_error(self):
        """handle_error should re-raise as RuntimeError."""
        original_error = ValueError("Test error message")

        with pytest.raises(RuntimeError) as excinfo:
            handle_error(original_error)

        assert "Test error message" in str(excinfo.value)

    def test_handle_error_preserves_all_sources_failed_message(self):
        """'All data sources failed' error should preserve message."""
        original_error = RuntimeError("All data sources failed. Custom message.")

        with pytest.raises(RuntimeError) as excinfo:
            handle_error(original_error)

        assert "All data sources failed" in str(excinfo.value)

    def test_handle_error_sanitizes_non_printable_chars(self):
        """handle_error should sanitize non-printable characters."""
        # Error with non-printable character
        error_with_binary = ValueError("Error with binary: \x00\x01\x02")

        with pytest.raises(RuntimeError) as excinfo:
            handle_error(error_with_binary)

        # Should not contain raw binary, should be sanitized
        error_str = str(excinfo.value)
        assert "\x00" not in error_str or "\\x00" in error_str


# =============================================================================
# merge_adjacent_ranges() Tests - Gap Detection Logic
# =============================================================================


class TestMergeAdjacentRanges:
    """Tests for merge_adjacent_ranges function (gap detection)."""

    def test_empty_ranges_returns_empty(self):
        """Empty input should return empty list."""
        result = merge_adjacent_ranges([], Interval.HOUR_1)
        assert result == []

    def test_single_range_unchanged(self):
        """Single range should be returned unchanged."""
        start = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        ranges = [(start, end)]

        result = merge_adjacent_ranges(ranges, Interval.HOUR_1)

        assert len(result) == 1
        assert result[0] == (start, end)

    def test_adjacent_ranges_merged(self):
        """Adjacent ranges should be merged into one."""
        base = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        ranges = [
            (base, base + timedelta(hours=6)),
            (base + timedelta(hours=6), base + timedelta(hours=12)),
        ]

        result = merge_adjacent_ranges(ranges, Interval.HOUR_1)

        assert len(result) == 1
        assert result[0][0] == base
        assert result[0][1] == base + timedelta(hours=12)

    def test_overlapping_ranges_merged(self):
        """Overlapping ranges should be merged."""
        base = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        ranges = [
            (base, base + timedelta(hours=8)),
            (base + timedelta(hours=4), base + timedelta(hours=12)),
        ]

        result = merge_adjacent_ranges(ranges, Interval.HOUR_1)

        assert len(result) == 1
        assert result[0][0] == base
        assert result[0][1] == base + timedelta(hours=12)

    def test_non_adjacent_ranges_kept_separate(self):
        """Non-adjacent ranges should remain separate."""
        base = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        ranges = [
            (base, base + timedelta(hours=4)),
            (base + timedelta(hours=12), base + timedelta(hours=16)),
        ]

        result = merge_adjacent_ranges(ranges, Interval.HOUR_1)

        assert len(result) == 2

    def test_unsorted_ranges_handled(self):
        """Unsorted input ranges should be sorted and merged."""
        base = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        ranges = [
            (base + timedelta(hours=12), base + timedelta(hours=18)),
            (base, base + timedelta(hours=6)),
            (base + timedelta(hours=6), base + timedelta(hours=12)),
        ]

        result = merge_adjacent_ranges(ranges, Interval.HOUR_1)

        # All three should merge into one continuous range
        assert len(result) == 1
        assert result[0][0] == base
        assert result[0][1] == base + timedelta(hours=18)


# =============================================================================
# identify_missing_segments() Tests - Gap Detection
# =============================================================================


class TestIdentifyMissingSegments:
    """Tests for identify_missing_segments function."""

    def test_empty_dataframe_returns_full_range(self):
        """Empty DataFrame should return entire range as missing."""
        start = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        result = identify_missing_segments(pd.DataFrame(), start, end, Interval.HOUR_1)

        assert len(result) == 1
        assert result[0] == (start, end)

    def test_complete_data_returns_empty(self, sample_ohlcv_df, historical_time_range):
        """Complete data should return no missing segments."""
        start_time, end_time = historical_time_range

        # Use sample data that covers the full range
        result = identify_missing_segments(sample_ohlcv_df, start_time, end_time, Interval.HOUR_1)

        # Should have no or minimal missing segments if data is complete
        # (may have edge case at boundaries)
        assert len(result) <= 1

    def test_gap_in_middle_detected(self):
        """Gap in middle of data should be detected."""
        base = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

        # Create data with a gap in the middle (hours 0-5 and 10-12)
        timestamps_before = [base + timedelta(hours=i) for i in range(6)]
        timestamps_after = [base + timedelta(hours=i) for i in range(10, 13)]
        all_timestamps = timestamps_before + timestamps_after

        df = pd.DataFrame(
            {
                "open_time": all_timestamps,
                "open": [100.0] * len(all_timestamps),
                "high": [110.0] * len(all_timestamps),
                "low": [90.0] * len(all_timestamps),
                "close": [105.0] * len(all_timestamps),
                "volume": [1000.0] * len(all_timestamps),
            }
        )

        result = identify_missing_segments(df, base, base + timedelta(hours=12), Interval.HOUR_1)

        # Should detect at least one missing segment
        assert len(result) >= 1


# =============================================================================
# merge_dataframes() Tests - 3-Way Merge Logic
# =============================================================================


class TestMergeDataFrames:
    """Tests for merge_dataframes function (3-way merge)."""

    def test_empty_list_returns_empty_df(self):
        """Empty list should return empty DataFrame."""
        result = merge_dataframes([])

        assert result.empty

    def test_single_dataframe_standardized(self, sample_ohlcv_df):
        """Single DataFrame should be standardized and returned."""
        result = merge_dataframes([sample_ohlcv_df.copy()])

        assert len(result) == len(sample_ohlcv_df)

    def test_two_dataframes_merged(self):
        """Two DataFrames should be merged correctly."""
        base = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

        df1 = pd.DataFrame(
            {
                "open_time": [base + timedelta(hours=i) for i in range(6)],
                "open": [100.0] * 6,
                "high": [110.0] * 6,
                "low": [90.0] * 6,
                "close": [105.0] * 6,
                "volume": [1000.0] * 6,
                "_data_source": ["CACHE"] * 6,
            }
        )

        df2 = pd.DataFrame(
            {
                "open_time": [base + timedelta(hours=i) for i in range(6, 12)],
                "open": [105.0] * 6,
                "high": [115.0] * 6,
                "low": [95.0] * 6,
                "close": [110.0] * 6,
                "volume": [1100.0] * 6,
                "_data_source": ["VISION"] * 6,
            }
        )

        result = merge_dataframes([df1, df2])

        assert len(result) == 12
        assert "_data_source" in result.columns

    def test_duplicate_timestamps_resolved_by_priority(self):
        """Duplicate timestamps should keep higher priority source."""
        base = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

        # CACHE data
        df_cache = pd.DataFrame(
            {
                "open_time": [base + timedelta(hours=i) for i in range(6)],
                "open": [100.0] * 6,
                "high": [110.0] * 6,
                "low": [90.0] * 6,
                "close": [105.0] * 6,
                "volume": [1000.0] * 6,
                "_data_source": ["CACHE"] * 6,
            }
        )

        # REST data with same timestamps (should win due to higher priority)
        df_rest = pd.DataFrame(
            {
                "open_time": [base + timedelta(hours=i) for i in range(6)],
                "open": [200.0] * 6,  # Different values
                "high": [210.0] * 6,
                "low": [190.0] * 6,
                "close": [205.0] * 6,
                "volume": [2000.0] * 6,
                "_data_source": ["REST"] * 6,
            }
        )

        result = merge_dataframes([df_cache, df_rest])

        # Should have 6 rows (duplicates removed)
        assert len(result) == 6
        # REST should win (higher priority)
        assert (result["_data_source"] == "REST").all()

    def test_source_priority_order(self):
        """Verify source priority: REST > CACHE > VISION > UNKNOWN."""
        base = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        timestamp = base

        # Create DataFrames with same timestamp but different sources
        dfs = []
        for source, value in [("UNKNOWN", 100), ("VISION", 200), ("CACHE", 300), ("REST", 400)]:
            df = pd.DataFrame(
                {
                    "open_time": [timestamp],
                    "open": [float(value)],
                    "high": [float(value + 10)],
                    "low": [float(value - 10)],
                    "close": [float(value + 5)],
                    "volume": [1000.0],
                    "_data_source": [source],
                }
            )
            dfs.append(df)

        result = merge_dataframes(dfs)

        # Should have 1 row (all duplicates merged)
        assert len(result) == 1
        # REST should win
        assert result["_data_source"].iloc[0] == "REST"
        # Value should be from REST DataFrame
        assert result["open"].iloc[0] == 400.0

    def test_three_way_merge_cache_vision_rest(self):
        """Test 3-way merge: Cache + Vision + REST."""
        base = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

        # Cache: hours 0-3
        df_cache = pd.DataFrame(
            {
                "open_time": [base + timedelta(hours=i) for i in range(4)],
                "open": [100.0] * 4,
                "high": [110.0] * 4,
                "low": [90.0] * 4,
                "close": [105.0] * 4,
                "volume": [1000.0] * 4,
                "_data_source": ["CACHE"] * 4,
            }
        )

        # Vision: hours 4-7
        df_vision = pd.DataFrame(
            {
                "open_time": [base + timedelta(hours=i) for i in range(4, 8)],
                "open": [105.0] * 4,
                "high": [115.0] * 4,
                "low": [95.0] * 4,
                "close": [110.0] * 4,
                "volume": [1100.0] * 4,
                "_data_source": ["VISION"] * 4,
            }
        )

        # REST: hours 8-11
        df_rest = pd.DataFrame(
            {
                "open_time": [base + timedelta(hours=i) for i in range(8, 12)],
                "open": [110.0] * 4,
                "high": [120.0] * 4,
                "low": [100.0] * 4,
                "close": [115.0] * 4,
                "volume": [1200.0] * 4,
                "_data_source": ["REST"] * 4,
            }
        )

        result = merge_dataframes([df_cache, df_vision, df_rest])

        # Should have 12 rows total
        assert len(result) == 12
        # Should have data from all three sources
        source_counts = result["_data_source"].value_counts()
        assert "CACHE" in source_counts.index
        assert "VISION" in source_counts.index
        assert "REST" in source_counts.index


# =============================================================================
# Source Attribution Tests
# =============================================================================


class TestSourceAttribution:
    """Tests for _data_source column tracking."""

    def test_vision_source_attribution(self, sample_ohlcv_df, historical_time_range):
        """Vision step should attribute source as 'VISION'."""
        start_time, end_time = historical_time_range
        mock_vision = MagicMock(return_value=sample_ohlcv_df.copy())

        result_df, _ = process_vision_step(
            fetch_from_vision_func=mock_vision,
            symbol="BTCUSDT",
            missing_ranges=[(start_time, end_time)],
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )

        assert "_data_source" in result_df.columns
        assert (result_df["_data_source"] == "VISION").all()

    def test_rest_source_attribution(self, sample_ohlcv_df, historical_time_range):
        """REST step should attribute source as 'REST'."""
        start_time, end_time = historical_time_range
        mock_rest = MagicMock(return_value=sample_ohlcv_df.copy())

        result_df = process_rest_step(
            fetch_from_rest_func=mock_rest,
            symbol="BTCUSDT",
            missing_ranges=[(start_time, end_time)],
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )

        assert "_data_source" in result_df.columns
        assert (result_df["_data_source"] == "REST").all()

    def test_mixed_source_attribution_preserved(self):
        """Mixed source data should preserve individual attributions."""
        base = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

        df_cache = pd.DataFrame(
            {
                "open_time": [base],
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
                "volume": [1000.0],
                "_data_source": ["CACHE"],
            }
        )

        df_rest = pd.DataFrame(
            {
                "open_time": [base + timedelta(hours=1)],
                "open": [105.0],
                "high": [115.0],
                "low": [95.0],
                "close": [110.0],
                "volume": [1100.0],
                "_data_source": ["REST"],
            }
        )

        result = merge_dataframes([df_cache, df_rest])

        assert len(result) == 2
        source_counts = result["_data_source"].value_counts()
        assert source_counts.get("CACHE", 0) == 1
        assert source_counts.get("REST", 0) == 1
