"""Memory pressure stress tests.

Tests that verify memory efficiency under load:
- Large historical fetches
- Mixed source FCP chain
- Polars vs Pandas output comparison
"""

import gc
import tracemalloc
from datetime import datetime, timezone

import pytest

from ckvd import DataProvider, CryptoKlineVisionData, Interval, MarketType


@pytest.mark.stress
class TestLargeHistoricalFetch:
    """Tests for large data fetches."""

    def test_30_day_1h_fetch_memory_bounded(self, memory_tracker):
        """30-day 1h fetch must stay under 20MB peak memory."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 31, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)

        manager.close()

        # 30 days * 24 hours = 720 rows expected
        assert len(df) >= 700, f"Expected ~720 rows, got {len(df)}"

        # Memory threshold based on baseline: 6.21 MB for 720 rows
        # Allow 3x overhead: 20MB
        assert tracker.peak_mb < 20, f"Peak {tracker.peak_mb:.1f}MB exceeds 20MB limit"

    def test_7_day_1m_fetch_memory_bounded(self, memory_tracker):
        """7-day 1m fetch must stay under 25MB peak memory."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, Interval.MINUTE_1)

        manager.close()

        # 7 days * 24 hours * 60 min = 10,080 rows expected
        assert len(df) >= 10000, f"Expected ~10,080 rows, got {len(df)}"

        # Memory threshold based on baseline: 12.49 MB for 10,080 rows
        # Allow 2x overhead: 25MB
        assert tracker.peak_mb < 25, f"Peak {tracker.peak_mb:.1f}MB exceeds 25MB limit"

    def test_memory_efficiency_ratio(self, memory_tracker):
        """Peak memory should be < 5x final DataFrame size."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)

        manager.close()

        if df.empty:
            pytest.skip("No data returned - likely network issue")

        # Calculate efficiency ratio
        final_size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        efficiency_ratio = tracker.peak_mb / final_size_mb if final_size_mb > 0 else float("inf")

        # Target: < 20x overhead (baseline measured at ~15x for small DataFrames)
        # High ratios are expected for small DataFrames due to fixed Python/Polars overhead
        assert efficiency_ratio < 20.0, f"Efficiency ratio {efficiency_ratio:.1f}x exceeds 20x limit"


@pytest.mark.stress
class TestPolarsVsPandasMemory:
    """Compare memory efficiency of Pandas vs Polars output."""

    def test_polars_output_memory_comparison(self, memory_tracker):
        """Polars output should use similar or less memory than Pandas."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        # Measure Pandas path
        gc.collect()
        tracemalloc.start()
        df_pandas = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1, return_polars=False)
        _, peak_pandas = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        del df_pandas
        gc.collect()

        # Measure Polars path
        gc.collect()
        tracemalloc.start()
        manager.get_data("BTCUSDT", start, end, Interval.HOUR_1, return_polars=True)
        _, peak_polars = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        manager.close()

        # Log comparison (Polars may be similar due to internal Pandas processing)
        peak_pandas_mb = peak_pandas / (1024 * 1024)
        peak_polars_mb = peak_polars / (1024 * 1024)

        # Polars path shouldn't be significantly worse than Pandas
        # (Currently they're similar because Polars output still goes through Pandas internally)
        assert peak_polars_mb <= peak_pandas_mb * 1.5, (
            f"Polars path ({peak_polars_mb:.1f}MB) significantly worse than Pandas ({peak_pandas_mb:.1f}MB)"
        )


@pytest.mark.stress
class TestMixedSourceMerge:
    """Tests for FCP chain with multiple data sources."""

    def test_merge_memory_efficiency(self, memory_tracker):
        """FCP merge from multiple sources should be memory efficient."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Use a range that likely spans cache + Vision + REST
        end = datetime(2024, 1, 15, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1, include_source_info=True)

        manager.close()

        if df.empty:
            pytest.skip("No data returned - likely network issue")

        # Check data sources used (if tracking is enabled)
        if "_data_source" in df.columns:
            df["_data_source"].unique()
            # Log which sources were used
            source_counts = df["_data_source"].value_counts()
            print(f"Sources used: {source_counts.to_dict()}")

        # Memory efficiency during merge should be < 15x final size
        # Issue #1: Baseline measured at ~12x due to FCP chain overhead
        final_size_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        if final_size_mb > 0:
            efficiency_ratio = tracker.peak_mb / final_size_mb
            assert efficiency_ratio < 15.0, f"Merge efficiency {efficiency_ratio:.1f}x exceeds 15x limit"


@pytest.mark.stress
@pytest.mark.integration
class TestPolarsFeatureFlagIntegration:
    """Tests that verify Polars pipeline with real market data.

    The Polars pipeline is always active (USE_POLARS_PIPELINE removed in v3.1.0).
    These tests ensure:
    1. Memory stays within bounds for standard fetches
    2. Zero-copy output (USE_POLARS_OUTPUT) works correctly
    3. Data integrity across multiple symbols and intervals
    """

    @pytest.fixture
    def env_without_polars(self, monkeypatch):
        """Environment with Polars zero-copy output disabled."""
        monkeypatch.setenv("CKVD_USE_POLARS_OUTPUT", "false")

    @pytest.fixture
    def env_with_full_polars(self, monkeypatch):
        """Environment with full Polars optimization (zero-copy output)."""
        monkeypatch.setenv("CKVD_USE_POLARS_OUTPUT", "true")

    def test_polars_pipeline_memory_bounded(self, memory_tracker):
        """Verify Polars pipeline memory stays within bounds.

        The Polars pipeline is always active (USE_POLARS_PIPELINE removed in v3.1.0).
        This test verifies memory usage is reasonable for a standard fetch.
        """
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)

        manager.close()

        if df.empty:
            pytest.skip("No data returned - likely network issue")

        # 7 days * 24 hours = 168 rows expected
        assert len(df) >= 160, f"Expected ~168 rows, got {len(df)}"

        # Memory should be reasonable (< 20MB for 1 week hourly)
        assert tracker.peak_mb < 20, f"Peak {tracker.peak_mb:.1f}MB exceeds 20MB limit"

        print(f"\nPolars pipeline: {len(df)} rows, {tracker.peak_mb:.1f}MB peak")

    def test_zero_copy_output_memory(self, memory_tracker):
        """Verify zero-copy Polars output uses less memory than pandas conversion.

        When USE_POLARS_OUTPUT=true and return_polars=True, the output should
        skip pandas conversion entirely, reducing memory usage.
        """
        import os

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        # Measure WITH conversion (return_polars=True but output flag disabled)
        os.environ["CKVD_USE_POLARS_OUTPUT"] = "false"

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        gc.collect()
        tracemalloc.start()
        manager.get_data("BTCUSDT", start, end, Interval.HOUR_1, return_polars=True)
        _, peak_with_conversion = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        manager.close()

        gc.collect()

        # Measure WITHOUT conversion (zero-copy path)
        os.environ["CKVD_USE_POLARS_OUTPUT"] = "true"

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        gc.collect()
        tracemalloc.start()
        manager.get_data("BTCUSDT", start, end, Interval.HOUR_1, return_polars=True)
        _, peak_zero_copy = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        manager.close()

        peak_conversion_mb = peak_with_conversion / (1024 * 1024)
        peak_zero_copy_mb = peak_zero_copy / (1024 * 1024)

        print("\nZero-copy comparison:")
        print(f"  With conversion: {peak_conversion_mb:.2f} MB")
        print(f"  Zero-copy:       {peak_zero_copy_mb:.2f} MB")

        # Zero-copy should be similar or better (not significantly worse)
        assert peak_zero_copy_mb <= peak_conversion_mb * 1.2, (
            f"Zero-copy ({peak_zero_copy_mb:.1f}MB) worse than conversion ({peak_conversion_mb:.1f}MB)"
        )

    @pytest.mark.parametrize(
        "symbol,market_type",
        [
            ("BTCUSDT", MarketType.FUTURES_USDT),
            ("ETHUSDT", MarketType.FUTURES_USDT),
            ("BTCUSDT", MarketType.SPOT),
            ("BTCUSD_PERP", MarketType.FUTURES_COIN),
        ],
    )
    def test_polars_pipeline_multi_market(self, symbol, market_type, memory_tracker):
        """Test Polars pipeline works correctly across different market types.

        The Polars pipeline is always active (USE_POLARS_PIPELINE removed in v3.1.0).
        This test verifies data integrity and memory bounds for:
        - FUTURES_USDT (BTCUSDT, ETHUSDT)
        - SPOT (BTCUSDT)
        - FUTURES_COIN (BTCUSD_PERP)
        """

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, market_type)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data(symbol, start, end, Interval.HOUR_1)

        manager.close()

        if df.empty:
            pytest.skip(f"No data returned for {symbol} on {market_type.name}")

        # Verify data integrity
        assert len(df) >= 100, f"Expected 100+ rows for 7 days, got {len(df)}"
        assert df.index.is_monotonic_increasing, f"Timestamps not sorted for {symbol}"
        assert not df.index.has_duplicates, f"Duplicate timestamps for {symbol}"

        # Memory should be reasonable (< 50MB for 1 week of hourly data)
        assert tracker.peak_mb < 50, f"{symbol}/{market_type.name}: Peak {tracker.peak_mb:.1f}MB exceeds 50MB"

        print(f"\n{symbol}/{market_type.name}: {len(df)} rows, {tracker.peak_mb:.1f}MB peak")

    @pytest.mark.parametrize(
        "interval,expected_min_rows",
        [
            (Interval.MINUTE_1, 10000),  # 7 days * 24 * 60 = 10,080
            (Interval.MINUTE_5, 2000),  # 7 days * 24 * 12 = 2,016
            (Interval.HOUR_1, 160),  # 7 days * 24 = 168
            (Interval.HOUR_4, 40),  # 7 days * 6 = 42
            (Interval.DAY_1, 6),  # 7 days
        ],
    )
    def test_polars_pipeline_multi_interval(self, interval, expected_min_rows, memory_tracker):
        """Test Polars pipeline works correctly across different intervals.

        The Polars pipeline is always active (USE_POLARS_PIPELINE removed in v3.1.0).
        This test verifies data integrity and memory efficiency for:
        - High frequency: 1m, 5m
        - Medium frequency: 1h, 4h
        - Low frequency: 1d
        """

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, interval)

        manager.close()

        if df.empty:
            pytest.skip(f"No data returned for interval {interval.value}")

        # Verify row count meets expectation
        assert len(df) >= expected_min_rows, f"Interval {interval.value}: Expected {expected_min_rows}+ rows, got {len(df)}"

        # Verify data integrity
        assert df.index.is_monotonic_increasing, f"Timestamps not sorted for {interval.value}"
        assert not df.index.has_duplicates, f"Duplicate timestamps for {interval.value}"

        # OHLCV integrity checks
        assert (df["high"] >= df["low"]).all(), f"high < low detected for {interval.value}"
        assert (df["volume"] >= 0).all(), f"Negative volume for {interval.value}"

        print(f"\n{interval.value}: {len(df)} rows, {tracker.peak_mb:.1f}MB peak")
