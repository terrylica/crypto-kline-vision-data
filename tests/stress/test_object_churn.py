"""Object churn stress tests.

Tests that verify memory stability across repeated operations:
- Sequential fetches
- Memory leak detection
- Resource cleanup
"""

import gc
import tracemalloc
from datetime import datetime, timezone

import pytest

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType


@pytest.mark.stress
class TestSequentialFetches:
    """Tests for memory stability with repeated operations."""

    def test_sequential_fetches_no_memory_leak(self, test_symbols, memory_tracker):
        """Multiple sequential fetches should not leak memory.

        This tests for resource cleanup and GC pressure.
        """
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        gc.collect()
        tracemalloc.start()
        baseline_snapshot = tracemalloc.take_snapshot()

        for symbol in test_symbols:
            df = manager.get_data(symbol, start, end, Interval.HOUR_1)
            del df
            gc.collect()

        final_snapshot = tracemalloc.take_snapshot()
        tracemalloc.stop()

        manager.close()

        # Calculate memory delta
        diff = final_snapshot.compare_to(baseline_snapshot, "lineno")
        total_delta_mb = sum(stat.size_diff for stat in diff) / (1024 * 1024)

        # Allow up to 10MB growth across 10 symbols
        assert total_delta_mb < 10, f"Memory leak: {total_delta_mb:.1f}MB growth"

    def test_repeated_same_symbol_stable(self, memory_tracker):
        """Repeated fetches of same symbol should have stable memory."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        gc.collect()
        tracemalloc.start()

        memory_readings = []
        for _i in range(5):
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
            current, _ = tracemalloc.get_traced_memory()
            memory_readings.append(current / (1024 * 1024))
            del df
            gc.collect()

        tracemalloc.stop()
        manager.close()

        # Memory should stabilize after first few fetches
        # Last reading shouldn't be more than 2x first reading
        if memory_readings[0] > 0:
            growth_ratio = memory_readings[-1] / memory_readings[0]
            assert growth_ratio < 2.0, f"Memory grew {growth_ratio:.1f}x across repeated fetches"

    def test_manager_reuse_efficient(self, memory_tracker):
        """Reusing manager should be more efficient than creating new ones."""
        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        # Test 1: Reusing manager
        gc.collect()
        tracemalloc.start()
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
        for _ in range(3):
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
            del df
            gc.collect()
        manager.close()
        _, peak_reuse = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        gc.collect()

        # Test 2: Creating new managers
        gc.collect()
        tracemalloc.start()
        for _ in range(3):
            m = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
            df = m.get_data("BTCUSDT", start, end, Interval.HOUR_1)
            m.close()
            del df
            gc.collect()
        _, peak_new = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_reuse_mb = peak_reuse / (1024 * 1024)
        peak_new_mb = peak_new / (1024 * 1024)

        # Reusing manager shouldn't be worse than creating new ones
        # (might be similar due to caching)
        assert peak_reuse_mb <= peak_new_mb * 1.5, (
            f"Reusing manager ({peak_reuse_mb:.1f}MB) worse than new ({peak_new_mb:.1f}MB)"
        )


@pytest.mark.stress
class TestResourceCleanup:
    """Tests for proper resource cleanup."""

    def test_manager_close_releases_memory(self, memory_tracker):
        """Closing manager should release resources."""
        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        gc.collect()
        tracemalloc.start()
        tracemalloc.get_traced_memory()[0]

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
        df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
        after_fetch = tracemalloc.get_traced_memory()[0]

        del df
        manager.close()
        gc.collect()

        after_close = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()

        # Memory after close should be less than peak (some resources released)
        assert after_close < after_fetch, "Memory not released after close()"

    def test_dataframe_deletion_frees_memory(self, memory_tracker):
        """Deleting DataFrame should free memory."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        gc.collect()
        tracemalloc.start()

        df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
        with_df = tracemalloc.get_traced_memory()[0]

        del df
        gc.collect()

        without_df = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()

        manager.close()

        # Memory should decrease after deleting DataFrame
        assert without_df < with_df, "DataFrame deletion didn't free memory"
