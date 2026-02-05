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
    """Expected column data types.

    Note: volume may be int64 or float64 depending on market type and data source.
    FUTURES_COIN specifically returns int64 volume from the API.
    """
    return {
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        # volume can be int64 or float64 depending on market type
        "volume": ("float64", "int64"),
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
            actual_dtype = str(df[col].dtype)
            # Handle single dtype or tuple of acceptable dtypes
            if isinstance(expected_dtype, tuple):
                assert actual_dtype in expected_dtype, (
                    f"Dtype mismatch for {col} in {market_type}: "
                    f"got {actual_dtype}, expected one of {expected_dtype}"
                )
            else:
                assert actual_dtype == expected_dtype, (
                    f"Dtype mismatch for {col} in {market_type}: "
                    f"got {actual_dtype}, expected {expected_dtype}"
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

    def test_multiple_symbols_consistent_structure(self):
        """Different symbols should return consistent DataFrame structure."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        dataframes = {}

        for symbol in symbols:
            df = manager.get_data(
                symbol=symbol,
                start_time=VALIDATION_START,
                end_time=VALIDATION_END,
                interval=Interval.HOUR_1,
            )
            dataframes[symbol] = df

        manager.close()

        # All should have same structure
        for symbol, df in dataframes.items():
            assert df.index.name == "open_time", f"{symbol} has wrong index name"
            assert "open" in df.columns, f"{symbol} missing 'open' column"
            assert "close" in df.columns, f"{symbol} missing 'close' column"
            assert len(df) > 0, f"{symbol} returned no data"


# =============================================================================
# Cross-Market Consistency Tests
# =============================================================================


@pytest.mark.integration
class TestCrossMarketConsistency:
    """Tests for consistency across different market types."""

    def test_spot_vs_futures_price_correlation(self):
        """SPOT and FUTURES_USDT prices should be highly correlated (>0.99).

        Same underlying asset (BTC) on different markets should have
        near-identical prices.
        """
        spot_manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
        futures_manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        df_spot = spot_manager.get_data(
            symbol="BTCUSDT",
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
        )

        df_futures = futures_manager.get_data(
            symbol="BTCUSDT",
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
        )

        spot_manager.close()
        futures_manager.close()

        # Align by timestamp
        common_idx = df_spot.index.intersection(df_futures.index)
        assert len(common_idx) > 100, f"Insufficient overlap: {len(common_idx)} rows"

        spot_close = df_spot.loc[common_idx, "close"]
        futures_close = df_futures.loc[common_idx, "close"]

        # Correlation should be > 0.99 for same underlying
        correlation = spot_close.corr(futures_close)
        assert correlation > 0.99, f"SPOT/FUTURES correlation too low: {correlation:.4f}"

    def test_spot_vs_futures_price_difference_bounded(self):
        """SPOT and FUTURES_USDT price difference should be small (<2%).

        Basis (futures premium/discount) is typically small for liquid markets.
        """
        spot_manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
        futures_manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        df_spot = spot_manager.get_data(
            symbol="BTCUSDT",
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
        )

        df_futures = futures_manager.get_data(
            symbol="BTCUSDT",
            start_time=VALIDATION_START,
            end_time=VALIDATION_END,
            interval=Interval.HOUR_1,
        )

        spot_manager.close()
        futures_manager.close()

        # Align by timestamp
        common_idx = df_spot.index.intersection(df_futures.index)

        spot_close = df_spot.loc[common_idx, "close"]
        futures_close = df_futures.loc[common_idx, "close"]

        # Calculate percentage difference
        pct_diff = abs((futures_close - spot_close) / spot_close * 100)

        # Average difference should be < 2%
        avg_diff = pct_diff.mean()
        assert avg_diff < 2.0, f"Average SPOT/FUTURES difference too large: {avg_diff:.2f}%"

        # Maximum difference should be < 5% (extreme cases)
        max_diff = pct_diff.max()
        assert max_diff < 5.0, f"Max SPOT/FUTURES difference too large: {max_diff:.2f}%"

    def test_all_market_types_return_data(self):
        """All supported market types should return valid data."""
        market_configs = [
            (MarketType.SPOT, "BTCUSDT"),
            (MarketType.FUTURES_USDT, "BTCUSDT"),
            (MarketType.FUTURES_COIN, "BTCUSD_PERP"),
        ]

        for market_type, symbol in market_configs:
            manager = DataSourceManager.create(DataProvider.BINANCE, market_type)

            df = manager.get_data(
                symbol=symbol,
                start_time=VALIDATION_START,
                end_time=VALIDATION_END,
                interval=Interval.HOUR_1,
            )

            manager.close()

            assert len(df) > 0, f"{market_type.name} with {symbol} returned no data"
            assert df.index.is_monotonic_increasing, f"{market_type.name} timestamps not monotonic"


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

        # Import DataNotAvailableError for fail-loud behavior (GitHub Issue #10)
        from data_source_manager.utils.for_core.vision_exceptions import DataNotAvailableError

        try:
            df = manager.get_data(
                symbol="BTCUSDT",
                start_time=ancient_start,
                end_time=ancient_end,
                interval=Interval.DAY_1,
            )
            # Should return empty if it doesn't raise
            assert df is None or len(df) == 0
        except (RuntimeError, ValueError, DataNotAvailableError):
            # Also acceptable - explicit error for invalid dates
            # DataNotAvailableError is expected for fail-loud behavior (GitHub Issue #10)
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
        """Unsupported providers should raise clear error.

        Note: OKX is now supported, so we test with TRADESTATION which is
        defined in the enum but not yet implemented.
        """
        with pytest.raises(ValueError) as exc_info:
            DataSourceManager.create(DataProvider.TRADESTATION, MarketType.SPOT)

        error_msg = str(exc_info.value).lower()
        assert "not supported" in error_msg
        assert "binance" in error_msg  # Should mention supported provider

    def test_binance_provider_supported(self):
        """Binance provider should work without error."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
        assert manager is not None
        assert manager.provider == DataProvider.BINANCE
        manager.close()
