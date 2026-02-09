#!/usr/bin/env python3
"""Unit tests for fetch_market_data() high-level API.

Tests validate that the rewritten fetch_market_data() correctly:
1. Accepts datetime objects (the root cause bug fix)
2. Accepts string times and None+days
3. Passes return_polars through to get_data() for zero-copy
4. Returns the correct tuple structure
5. Raises UnsupportedIntervalError (not SystemExit)

Copy from: tests/unit/core/sync/test_return_type_overloads.py
Plan: /Users/terryli/.claude/plans/gleaming-frolicking-engelbart.md
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest

from ckvd.core.sync.ckvd_lib import fetch_market_data
from ckvd.utils.for_core.vision_exceptions import UnsupportedIntervalError
from ckvd.utils.market_constraints import (
    ChartType,
    DataProvider,
    Interval,
    MarketType,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_ckvd():
    """Mock CryptoKlineVisionData to avoid network calls."""
    with patch("ckvd.core.sync.ckvd_lib.CryptoKlineVisionData") as mock_cls:
        mock_manager = MagicMock()
        mock_cls.return_value = mock_manager
        # Context manager protocol
        mock_manager.__enter__ = MagicMock(return_value=mock_manager)
        mock_manager.__exit__ = MagicMock(return_value=False)
        yield mock_cls, mock_manager


@pytest.fixture
def sample_pandas_df():
    """Sample pandas DataFrame mimicking OHLCV data."""
    base_time = datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    timestamps = [base_time + timedelta(hours=i) for i in range(6)]
    return pd.DataFrame(
        {
            "open": [42000.0 + i * 10 for i in range(6)],
            "high": [42100.0 + i * 10 for i in range(6)],
            "low": [41900.0 + i * 10 for i in range(6)],
            "close": [42050.0 + i * 10 for i in range(6)],
            "volume": [1000.0 + i for i in range(6)],
        },
        index=pd.DatetimeIndex(timestamps, name="open_time", tz="UTC"),
    )


@pytest.fixture
def sample_polars_df():
    """Sample Polars DataFrame mimicking OHLCV data."""
    return pl.DataFrame(
        {
            "open_time": [datetime(2024, 1, 15, i, 0, 0, tzinfo=timezone.utc) for i in range(6)],
            "open": [42000.0 + i * 10 for i in range(6)],
            "high": [42100.0 + i * 10 for i in range(6)],
            "low": [41900.0 + i * 10 for i in range(6)],
            "close": [42050.0 + i * 10 for i in range(6)],
            "volume": [1000.0 + i for i in range(6)],
        }
    )


# =============================================================================
# Test 1-3: Time Input Acceptance
# =============================================================================


class TestFetchMarketDataTimeInputs:
    """Tests that fetch_market_data accepts various time input types."""

    @patch("ckvd.core.sync.ckvd_lib.validate_interval")
    @patch("ckvd.core.sync.ckvd_lib.calculate_date_range")
    def test_accepts_datetime_objects(self, mock_date_range, mock_validate, mock_ckvd, sample_pandas_df):
        """fetch_market_data() should accept plain datetime objects without crashing.

        This is the root cause bug: before the fix, passing datetime objects
        caused a crash in calculate_date_range() which expected strings only.
        """
        _mock_cls, mock_manager = mock_ckvd
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 10, tzinfo=timezone.utc)
        mock_date_range.return_value = (start, end)
        mock_manager.get_data.return_value = sample_pandas_df

        df, elapsed, count = fetch_market_data(
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
            chart_type=ChartType.KLINES,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start,
            end_time=end,
        )

        assert isinstance(df, pd.DataFrame)
        assert count == 6
        assert elapsed > 0
        mock_date_range.assert_called_once_with(start, end, 3, Interval.HOUR_1)

    @patch("ckvd.core.sync.ckvd_lib.validate_interval")
    @patch("ckvd.core.sync.ckvd_lib.calculate_date_range")
    def test_accepts_string_times(self, mock_date_range, mock_validate, mock_ckvd, sample_pandas_df):
        """fetch_market_data() should accept ISO format string times."""
        _mock_cls, mock_manager = mock_ckvd
        start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_dt = datetime(2024, 1, 10, tzinfo=timezone.utc)
        mock_date_range.return_value = (start_dt, end_dt)
        mock_manager.get_data.return_value = sample_pandas_df

        df, _elapsed, count = fetch_market_data(
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
            chart_type=ChartType.KLINES,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-10T00:00:00Z",
        )

        assert isinstance(df, pd.DataFrame)
        assert count == 6
        mock_date_range.assert_called_once_with("2024-01-01T00:00:00Z", "2024-01-10T00:00:00Z", 3, Interval.HOUR_1)

    @patch("ckvd.core.sync.ckvd_lib.validate_interval")
    @patch("ckvd.core.sync.ckvd_lib.calculate_date_range")
    def test_accepts_none_with_days(self, mock_date_range, mock_validate, mock_ckvd, sample_pandas_df):
        """fetch_market_data() should accept None times with days parameter."""
        _mock_cls, mock_manager = mock_ckvd
        start_dt = datetime(2024, 1, 7, tzinfo=timezone.utc)
        end_dt = datetime(2024, 1, 10, tzinfo=timezone.utc)
        mock_date_range.return_value = (start_dt, end_dt)
        mock_manager.get_data.return_value = sample_pandas_df

        df, _elapsed, _count = fetch_market_data(
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
            chart_type=ChartType.KLINES,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            days=7,
        )

        assert isinstance(df, pd.DataFrame)
        mock_date_range.assert_called_once_with(None, None, 7, Interval.HOUR_1)

    @patch("ckvd.core.sync.ckvd_lib.validate_interval")
    @patch("ckvd.core.sync.ckvd_lib.calculate_date_range")
    def test_accepts_pendulum_datetime(self, mock_date_range, mock_validate, mock_ckvd, sample_pandas_df):
        """fetch_market_data() should accept pendulum DateTime objects."""
        import pendulum

        _mock_cls, mock_manager = mock_ckvd
        start = pendulum.datetime(2024, 1, 1, tz="UTC")
        end = pendulum.datetime(2024, 1, 10, tz="UTC")
        mock_date_range.return_value = (start, end)
        mock_manager.get_data.return_value = sample_pandas_df

        df, _elapsed, count = fetch_market_data(
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
            chart_type=ChartType.KLINES,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start,
            end_time=end,
        )

        assert isinstance(df, pd.DataFrame)
        assert count == 6


# =============================================================================
# Test 5-6: Return Type Tests
# =============================================================================


class TestFetchMarketDataReturnTypes:
    """Tests that return_polars flows through to get_data() correctly."""

    @patch("ckvd.core.sync.ckvd_lib.validate_interval")
    @patch("ckvd.core.sync.ckvd_lib.calculate_date_range")
    def test_return_polars_true(self, mock_date_range, mock_validate, mock_ckvd, sample_polars_df):
        """return_polars=True should pass through and return pl.DataFrame."""
        _mock_cls, mock_manager = mock_ckvd
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 10, tzinfo=timezone.utc)
        mock_date_range.return_value = (start, end)
        mock_manager.get_data.return_value = sample_polars_df

        df, _elapsed, count = fetch_market_data(
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
            chart_type=ChartType.KLINES,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start,
            end_time=end,
            return_polars=True,
        )

        assert isinstance(df, pl.DataFrame)
        assert count == 6
        # Verify return_polars was passed through to get_data()
        call_kwargs = mock_manager.get_data.call_args[1]
        assert call_kwargs["return_polars"] is True

    @patch("ckvd.core.sync.ckvd_lib.validate_interval")
    @patch("ckvd.core.sync.ckvd_lib.calculate_date_range")
    def test_return_polars_false(self, mock_date_range, mock_validate, mock_ckvd, sample_pandas_df):
        """return_polars=False (default) should return pd.DataFrame."""
        _mock_cls, mock_manager = mock_ckvd
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 10, tzinfo=timezone.utc)
        mock_date_range.return_value = (start, end)
        mock_manager.get_data.return_value = sample_pandas_df

        df, _elapsed, count = fetch_market_data(
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
            chart_type=ChartType.KLINES,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start,
            end_time=end,
        )

        assert isinstance(df, pd.DataFrame)
        assert count == 6
        # Verify return_polars defaults to False
        call_kwargs = mock_manager.get_data.call_args[1]
        assert call_kwargs["return_polars"] is False


# =============================================================================
# Test 7: Error Handling
# =============================================================================


class TestFetchMarketDataErrors:
    """Tests for error handling in fetch_market_data."""

    @patch("ckvd.core.sync.ckvd_lib.validate_interval")
    def test_invalid_interval_raises_unsupported_error(self, mock_validate):
        """Invalid interval should raise UnsupportedIntervalError, not SystemExit."""
        mock_validate.side_effect = UnsupportedIntervalError("SECOND_1 is not supported for FUTURES_USDT")

        with pytest.raises(UnsupportedIntervalError, match="SECOND_1"):
            fetch_market_data(
                provider=DataProvider.BINANCE,
                market_type=MarketType.FUTURES_USDT,
                chart_type=ChartType.KLINES,
                symbol="BTCUSDT",
                interval=Interval.SECOND_1,
                days=1,
            )


# =============================================================================
# Test 8: Return Tuple Structure
# =============================================================================


class TestFetchMarketDataTupleStructure:
    """Tests for the return tuple structure (df, float, int)."""

    @patch("ckvd.core.sync.ckvd_lib.validate_interval")
    @patch("ckvd.core.sync.ckvd_lib.calculate_date_range")
    def test_return_tuple_structure(self, mock_date_range, mock_validate, mock_ckvd, sample_pandas_df):
        """Return value should be (DataFrame, float, int) tuple."""
        _mock_cls, mock_manager = mock_ckvd
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 10, tzinfo=timezone.utc)
        mock_date_range.return_value = (start, end)
        mock_manager.get_data.return_value = sample_pandas_df

        result = fetch_market_data(
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
            chart_type=ChartType.KLINES,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start,
            end_time=end,
        )

        assert isinstance(result, tuple)
        assert len(result) == 3
        df, elapsed, count = result
        assert isinstance(df, (pd.DataFrame, pl.DataFrame, type(None)))
        assert isinstance(elapsed, float)
        assert isinstance(count, int)

    @patch("ckvd.core.sync.ckvd_lib.validate_interval")
    @patch("ckvd.core.sync.ckvd_lib.calculate_date_range")
    def test_return_tuple_none_df(self, mock_date_range, mock_validate, mock_ckvd):
        """When get_data returns None, count should be 0."""
        _mock_cls, mock_manager = mock_ckvd
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 10, tzinfo=timezone.utc)
        mock_date_range.return_value = (start, end)
        mock_manager.get_data.return_value = None

        df, elapsed, count = fetch_market_data(
            provider=DataProvider.BINANCE,
            market_type=MarketType.SPOT,
            chart_type=ChartType.KLINES,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start,
            end_time=end,
        )

        assert df is None
        assert count == 0
        assert elapsed > 0
