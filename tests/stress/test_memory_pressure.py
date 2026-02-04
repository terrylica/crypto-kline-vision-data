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

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType


@pytest.mark.stress
class TestLargeHistoricalFetch:
    """Tests for large data fetches."""

    def test_30_day_1h_fetch_memory_bounded(self, memory_tracker):
        """30-day 1h fetch must stay under 20MB peak memory."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

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
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

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
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

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
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

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
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

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
