#!/usr/bin/env python3
"""Unit tests for FCP utility functions.

Tests the Failover Control Protocol utility functions in dsm_fcp_utils.py:
1. validate_interval() - Interval validation
2. process_cache_step() - FCP Step 1: Cache lookup
3. process_vision_step() - FCP Step 2: Vision API
4. process_rest_step() - FCP Step 3: REST API fallback
5. verify_final_data() - Final data validation
6. handle_error() - Error handling

ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from data_source_manager.utils.for_core.dsm_fcp_utils import (
    handle_error,
    process_cache_step,
    process_rest_step,
    process_vision_step,
    validate_interval,
    verify_final_data,
)
from data_source_manager.utils.for_core.vision_exceptions import UnsupportedIntervalError
from data_source_manager.utils.market_constraints import Interval, MarketType


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
# process_cache_step() Tests
# =============================================================================


class TestProcessCacheStep:
    """Tests for process_cache_step function (FCP Step 1)."""

    def test_cache_hit_returns_data_and_empty_missing_ranges(
        self, sample_ohlcv_df, historical_time_range
    ):
        """Cache hit should return data with no missing ranges."""
        start_time, end_time = historical_time_range

        # Mock cache function that returns data
        mock_cache_func = MagicMock(return_value=(sample_ohlcv_df, []))

        result_df, missing_ranges = process_cache_step(
            use_cache=True,
            get_from_cache_func=mock_cache_func,
            symbol="BTCUSDT",
            aligned_start=start_time,
            aligned_end=end_time,
            interval=Interval.HOUR_1,
            include_source_info=True,
        )

        # Assertions
        assert len(result_df) == len(sample_ohlcv_df)
        assert len(missing_ranges) == 0
        mock_cache_func.assert_called_once()

    def test_cache_hit_adds_source_info(self, sample_ohlcv_df, historical_time_range):
        """Cache hit with include_source_info should add _data_source column."""
        start_time, end_time = historical_time_range

        mock_cache_func = MagicMock(return_value=(sample_ohlcv_df.copy(), []))

        result_df, _ = process_cache_step(
            use_cache=True,
            get_from_cache_func=mock_cache_func,
            symbol="BTCUSDT",
            aligned_start=start_time,
            aligned_end=end_time,
            interval=Interval.HOUR_1,
            include_source_info=True,
        )

        assert "_data_source" in result_df.columns
        assert (result_df["_data_source"] == "CACHE").all()

    def test_cache_miss_returns_empty_df_with_full_range_missing(
        self, historical_time_range
    ):
        """Cache miss should return empty DataFrame with full range as missing."""
        start_time, end_time = historical_time_range

        # Mock cache function that returns empty
        mock_cache_func = MagicMock(return_value=(pd.DataFrame(), []))

        result_df, missing_ranges = process_cache_step(
            use_cache=True,
            get_from_cache_func=mock_cache_func,
            symbol="BTCUSDT",
            aligned_start=start_time,
            aligned_end=end_time,
            interval=Interval.HOUR_1,
            include_source_info=False,
        )

        assert result_df.empty
        assert len(missing_ranges) == 1
        assert missing_ranges[0] == (start_time, end_time)

    def test_cache_partial_hit_returns_data_with_missing_ranges(
        self, sample_ohlcv_df, historical_time_range
    ):
        """Partial cache hit should return data with remaining missing ranges."""
        start_time, end_time = historical_time_range
        mid_time = start_time + timedelta(hours=12)

        # Mock returns partial data with gap
        mock_cache_func = MagicMock(
            return_value=(sample_ohlcv_df.iloc[:12], [(mid_time, end_time)])
        )

        result_df, missing_ranges = process_cache_step(
            use_cache=True,
            get_from_cache_func=mock_cache_func,
            symbol="BTCUSDT",
            aligned_start=start_time,
            aligned_end=end_time,
            interval=Interval.HOUR_1,
            include_source_info=False,
        )

        assert len(result_df) == 12
        assert len(missing_ranges) == 1
        assert missing_ranges[0] == (mid_time, end_time)


# =============================================================================
# process_vision_step() Tests
# =============================================================================


class TestProcessVisionStep:
    """Tests for process_vision_step function (FCP Step 2)."""

    def test_vision_success_returns_data_and_clears_missing(
        self, sample_ohlcv_df, historical_time_range
    ):
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

    def test_vision_merges_with_existing_data(
        self, sample_ohlcv_df, historical_time_range
    ):
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

    def test_rest_merges_with_existing_data(
        self, sample_ohlcv_df, historical_time_range
    ):
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

    def test_rest_empty_returns_existing_data(
        self, sample_ohlcv_df, historical_time_range
    ):
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

        assert "No data available" in str(excinfo.value) or "All data sources failed" in str(
            excinfo.value
        )

    def test_valid_historical_data_passes(
        self, sample_ohlcv_df, historical_time_range
    ):
        """Valid historical data should pass verification."""
        start_time, end_time = historical_time_range

        # Should not raise
        verify_final_data(sample_ohlcv_df, start_time, end_time)

    def test_valid_data_with_index_passes(
        self, sample_ohlcv_df_with_index, historical_time_range
    ):
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
# Integration-like Tests (FCP Flow)
# =============================================================================


class TestFCPFlowIntegration:
    """Tests for complete FCP flow through utility functions."""

    def test_cache_hit_short_circuits_flow(
        self, sample_ohlcv_df, historical_time_range
    ):
        """Cache hit should provide complete data without needing Vision/REST."""
        start_time, end_time = historical_time_range

        # Cache returns complete data
        mock_cache = MagicMock(return_value=(sample_ohlcv_df.copy(), []))

        result_df, missing_ranges = process_cache_step(
            use_cache=True,
            get_from_cache_func=mock_cache,
            symbol="BTCUSDT",
            aligned_start=start_time,
            aligned_end=end_time,
            interval=Interval.HOUR_1,
            include_source_info=True,
        )

        # No missing ranges means Vision/REST not needed
        assert len(missing_ranges) == 0
        assert len(result_df) == len(sample_ohlcv_df)

    def test_cache_miss_vision_hit_flow(
        self, sample_ohlcv_df, historical_time_range
    ):
        """Cache miss followed by Vision hit should complete without REST."""
        start_time, end_time = historical_time_range

        # Step 1: Cache miss
        mock_cache = MagicMock(return_value=(pd.DataFrame(), []))
        cache_df, missing_ranges = process_cache_step(
            use_cache=True,
            get_from_cache_func=mock_cache,
            symbol="BTCUSDT",
            aligned_start=start_time,
            aligned_end=end_time,
            interval=Interval.HOUR_1,
            include_source_info=False,
        )

        assert cache_df.empty
        assert missing_ranges == [(start_time, end_time)]

        # Step 2: Vision hit
        mock_vision = MagicMock(return_value=sample_ohlcv_df.copy())
        result_df, _remaining = process_vision_step(
            fetch_from_vision_func=mock_vision,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=cache_df,
        )

        assert len(result_df) > 0
        assert "_data_source" in result_df.columns

    def test_full_fallback_chain_flow(
        self, sample_ohlcv_df, historical_time_range
    ):
        """Full fallback: Cache miss -> Vision miss -> REST success."""
        start_time, end_time = historical_time_range

        # Step 1: Cache miss
        mock_cache = MagicMock(return_value=(pd.DataFrame(), []))
        cache_df, missing_ranges = process_cache_step(
            use_cache=True,
            get_from_cache_func=mock_cache,
            symbol="BTCUSDT",
            aligned_start=start_time,
            aligned_end=end_time,
            interval=Interval.HOUR_1,
            include_source_info=False,
        )

        # Step 2: Vision miss
        mock_vision = MagicMock(return_value=pd.DataFrame())
        vision_df, remaining = process_vision_step(
            fetch_from_vision_func=mock_vision,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=False,
            result_df=cache_df,
        )

        assert vision_df.empty
        assert len(remaining) > 0

        # Step 3: REST success
        mock_rest = MagicMock(return_value=sample_ohlcv_df.copy())
        result_df = process_rest_step(
            fetch_from_rest_func=mock_rest,
            symbol="BTCUSDT",
            missing_ranges=remaining,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=vision_df,
        )

        assert len(result_df) > 0
        assert "_data_source" in result_df.columns
        assert (result_df["_data_source"] == "REST").all()
