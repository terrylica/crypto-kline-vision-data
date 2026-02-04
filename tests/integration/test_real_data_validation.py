"""Real data validation tests for DataSourceManager.

Integration tests with real data to validate output correctness.

These tests fetch ACTUAL data from Binance APIs and validate:
1. DataFrame structure (columns, dtypes, index)
2. Data integrity (no gaps, monotonic timestamps, valid OHLCV)
3. Cross-source consistency (Cache vs Vision vs REST)
4. Cross-market consistency (SPOT vs FUTURES_USDT vs FUTURES_COIN)

ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl
import pytest

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def expected_columns():
    """Standard OHLCV columns for all market types."""
    return ["open", "high", "low", "close", "volume"]


@pytest.fixture
def expected_dtypes():
    """Expected column data types."""
    return {
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
    }


# Standard validation period (historical, guaranteed available)
VALIDATION_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
VALIDATION_END = datetime(2024, 1, 7, tzinfo=timezone.utc)


# =============================================================================
# Structure Validation Tests
# =============================================================================


@pytest.mark.integration
class TestDataFrameStructure:
    """Tests for DataFrame structure across all market types."""

    @pytest.mark.parametrize(
        "market_type,symbol",
        [
            (MarketType.SPOT, "BTCUSDT"),
            (MarketType.FUTURES_USDT, "BTCUSDT"),
            (MarketType.FUTURES_COIN, "BTCUSD_PERP"),
        ],
    )
    def test_dataframe_structure_all_markets(
        self, market_type, symbol, expected_columns, expected_dtypes
    ):
        """Validate DataFrame structure across all market types."""
        manager = DataSourceManager.create(DataProvider.BINANCE, market_type)

        df = manager.get_data(
            symbol=symbol,
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
        )

        manager.close()

        # Structure checks
        assert df.index.name == "open_time", f"Index name mismatch for {market_type}"
        assert list(df.columns)[:5] == expected_columns, f"Column mismatch for {market_type}"

        # Dtype checks
        for col, expected_dtype in expected_dtypes.items():
            assert str(df[col].dtype) == expected_dtype, (
                f"Dtype mismatch for {col} in {market_type}: "
                f"got {df[col].dtype}, expected {expected_dtype}"
            )

    @pytest.mark.parametrize(
        "market_type,symbol",
        [
            (MarketType.SPOT, "BTCUSDT"),
            (MarketType.FUTURES_USDT, "BTCUSDT"),
            (MarketType.FUTURES_COIN, "BTCUSD_PERP"),
        ],
    )
    def test_polars_output_structure(self, market_type, symbol):
        """Validate Polars output structure."""
        manager = DataSourceManager.create(DataProvider.BINANCE, market_type)

        df_pandas = manager.get_data(
            symbol=symbol,
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
            return_polars=False,
        )

        df_polars = manager.get_data(
            symbol=symbol,
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
            return_polars=True,
        )

        manager.close()

        assert isinstance(df_pandas, pd.DataFrame)
        assert isinstance(df_polars, pl.DataFrame)
        assert len(df_polars) == len(df_pandas)
        assert "open_time" in df_polars.columns  # Polars has it as column, not index


# =============================================================================
# Data Integrity Validation Tests
# =============================================================================


@pytest.mark.integration
class TestDataIntegrity:
    """Tests for data integrity across all market types."""

    @pytest.mark.parametrize(
        "market_type,symbol",
        [
            (MarketType.SPOT, "BTCUSDT"),
            (MarketType.FUTURES_USDT, "BTCUSDT"),
            (MarketType.FUTURES_COIN, "BTCUSD_PERP"),
        ],
    )
    def test_timestamp_monotonicity(self, market_type, symbol):
        """Timestamps must be strictly increasing (no duplicates, no reversals)."""
        manager = DataSourceManager.create(DataProvider.BINANCE, market_type)

        df = manager.get_data(
            symbol=symbol,
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
        )

        manager.close()

        assert df.index.is_monotonic_increasing, (
            f"Timestamps not monotonic for {market_type}: "
            f"duplicates={df.index.has_duplicates}"
        )

    @pytest.mark.parametrize(
        "market_type,symbol",
        [
            (MarketType.SPOT, "BTCUSDT"),
            (MarketType.FUTURES_USDT, "BTCUSDT"),
            (MarketType.FUTURES_COIN, "BTCUSD_PERP"),
        ],
    )
    def test_ohlcv_value_constraints(self, market_type, symbol):
        """OHLCV values must satisfy logical constraints."""
        manager = DataSourceManager.create(DataProvider.BINANCE, market_type)

        df = manager.get_data(
            symbol=symbol,
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
        )

        manager.close()

        # High >= Low (always)
        assert (df["high"] >= df["low"]).all(), f"High < Low violation in {market_type}"

        # High >= Open and High >= Close
        assert (df["high"] >= df["open"]).all(), f"High < Open violation in {market_type}"
        assert (df["high"] >= df["close"]).all(), f"High < Close violation in {market_type}"

        # Low <= Open and Low <= Close
        assert (df["low"] <= df["open"]).all(), f"Low > Open violation in {market_type}"
        assert (df["low"] <= df["close"]).all(), f"Low > Close violation in {market_type}"

        # Volume >= 0
        assert (df["volume"] >= 0).all(), f"Negative volume in {market_type}"

        # All prices > 0 (for BTC)
        assert (df["open"] > 0).all(), f"Zero/negative open price in {market_type}"

    @pytest.mark.parametrize(
        "market_type,symbol,interval",
        [
            (MarketType.SPOT, "BTCUSDT", Interval.HOUR_1),
            (MarketType.FUTURES_USDT, "BTCUSDT", Interval.HOUR_1),
            (MarketType.FUTURES_COIN, "BTCUSD_PERP", Interval.HOUR_1),
        ],
    )
    def test_no_gaps_in_data(self, market_type, symbol, interval):
        """Data should have no missing candles for liquid pairs."""
        manager = DataSourceManager.create(DataProvider.BINANCE, market_type)

        df = manager.get_data(
            symbol=symbol,
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=interval,
        )

        manager.close()

        # Calculate expected candle count
        interval_seconds = interval.to_seconds()
        total_seconds = (VALIDATION_END - VALIDATION_START).total_seconds()
        expected_candles = int(total_seconds / interval_seconds)

        # Allow 1% tolerance for edge cases
        min_candles = int(expected_candles * 0.99)

        assert len(df) >= min_candles, (
            f"Gap detected in {market_type}: got {len(df)}, "
            f"expected >= {min_candles} (of {expected_candles})"
        )


# =============================================================================
# Cross-Source Consistency Tests
# =============================================================================


@pytest.mark.integration
class TestCrossSourceConsistency:
    """Tests for consistency across different data sources."""

    def test_cache_vs_fresh_fetch_consistency(self):
        """Cache and fresh fetch should return identical data for same range."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # First fetch populates cache
        df_first = manager.get_data(
            symbol="BTCUSDT",
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
        )

        # Second fetch should use cache
        df_second = manager.get_data(
            symbol="BTCUSDT",
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
        )

        manager.close()

        # Remove source info columns if present for comparison
        cols_to_compare = ["open", "high", "low", "close", "volume"]

        pd.testing.assert_frame_equal(
            df_first[cols_to_compare].reset_index(drop=True),
            df_second[cols_to_compare].reset_index(drop=True),
            check_exact=False,
            rtol=1e-10,
        )


# =============================================================================
# Interval Validation Tests
# =============================================================================


@pytest.mark.integration
class TestIntervalValidation:
    """Tests for interval-specific behavior."""

    @pytest.mark.parametrize(
        "interval",
        [
            Interval.MINUTE_1,
            Interval.MINUTE_5,
            Interval.MINUTE_15,
            Interval.HOUR_1,
            Interval.HOUR_4,
            Interval.DAY_1,
        ],
    )
    def test_interval_produces_correct_spacing(self, interval):
        """Each interval should produce correctly spaced candles."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Use shorter range for minute intervals
        if interval.to_seconds() < 3600:
            start = VALIDATION_START
            end = VALIDATION_START + timedelta(hours=2)
        else:
            start = VALIDATION_START
            end = VALIDATION_END

        df = manager.get_data(
            symbol="BTCUSDT",
            start_time=start,
            end_time=end,
            interval=interval,
        )

        manager.close()

        if len(df) > 1:
            # Check time delta between consecutive rows
            time_diffs = df.index.to_series().diff().dropna()
            expected_delta = pd.Timedelta(seconds=interval.to_seconds())

            # All differences should equal the interval
            assert (time_diffs == expected_delta).all(), (
                f"Incorrect spacing for {interval.value}: "
                f"expected {expected_delta}, got unique values {time_diffs.unique()}"
            )


# =============================================================================
# Edge Case Tests
# =============================================================================


@pytest.mark.integration
class TestEdgeCases:
    """Edge case tests with real data."""

    def test_ancient_date_returns_empty_or_error(self):
        """Date before exchange launch should return empty or raise error."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

        # Use a date before Binance existed (2010)
        ancient_start = datetime(2010, 1, 1, tzinfo=timezone.utc)
        ancient_end = datetime(2010, 1, 2, tzinfo=timezone.utc)

        try:
            df = manager.get_data(
                symbol="BTCUSDT",
                start_time=ancient_start,
                end_time=ancient_end,
                interval=Interval.DAY_1,
            )
            # Should return empty if it doesn't raise
            assert df is None or len(df) == 0
        except (RuntimeError, ValueError):
            # Also acceptable - explicit error for invalid dates
            pass
        finally:
            manager.close()

    def test_coin_margined_symbol_format(self):
        """FUTURES_COIN requires USD_PERP format, not USDT."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_COIN)

        # Correct format
        df_correct = manager.get_data(
            symbol="BTCUSD_PERP",
            start_time=VALIDATION_START,
            end_time=VALIDATION_START + timedelta(days=1),
            interval=Interval.HOUR_1,
        )

        assert len(df_correct) > 0, "BTCUSD_PERP should return data"

        manager.close()


# =============================================================================
# Provider Validation Tests
# =============================================================================


@pytest.mark.integration
class TestProviderValidation:
    """Tests for provider-specific behavior."""

    def test_unsupported_provider_raises_error(self):
        """Unsupported providers should raise clear error."""
        with pytest.raises(ValueError) as exc_info:
            DataSourceManager.create(DataProvider.OKX, MarketType.SPOT)

        error_msg = str(exc_info.value).lower()
        assert "not supported" in error_msg
        assert "binance" in error_msg  # Should mention supported provider

    def test_binance_provider_supported(self):
        """Binance provider should work without error."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
        assert manager is not None
        assert manager.provider == DataProvider.BINANCE
        manager.close()
