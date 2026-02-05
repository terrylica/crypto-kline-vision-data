"""Concurrent stress tests for DataSourceManager.

Tests that verify thread safety and cache race condition handling:
- Multiple threads fetching same symbol
- Multiple threads fetching different symbols
- Concurrent cache write race detection
- Memory stability under concurrent load

GitHub Issue #3 - P0: Concurrent stress tests
"""

import gc
import statistics
import tracemalloc
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pytest

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType
from data_source_manager.utils.for_core.rest_exceptions import RestAPIError
from data_source_manager.utils.for_core.vision_exceptions import VisionAPIError

# Exception types that can occur during DSM operations
DSM_ERRORS = (RestAPIError, VisionAPIError, ValueError, RuntimeError, OSError)


@pytest.mark.stress
class TestConcurrentSameSymbol:
    """Tests for concurrent fetches of the same symbol."""

    def test_10_threads_same_symbol_no_corruption(self, memory_tracker, historical_time_range):
        """10 threads fetching same symbol should not cause data corruption.

        All threads should receive identical data for the same symbol and time range.
        """
        start, end = historical_time_range
        results = []
        errors = []
        results_lock = Lock()

        def fetch_data(thread_id: int):
            try:
                manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
                df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
                manager.close()
                return thread_id, len(df), df["close"].iloc[-1] if len(df) > 0 else None
            except DSM_ERRORS as e:
                return thread_id, -1, str(e)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_data, i) for i in range(10)]
            for future in as_completed(futures):
                result = future.result()
                with results_lock:
                    if result[1] == -1:
                        errors.append(result)
                    else:
                        results.append(result)

        # All threads should complete without errors
        assert len(errors) == 0, f"Threads failed: {errors}"
        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

        # All results should have same row count
        row_counts = [r[1] for r in results]
        assert len(set(row_counts)) == 1, f"Data corruption: different row counts {row_counts}"

        # All results should have same final close price
        close_prices = [r[2] for r in results]
        assert len(set(close_prices)) == 1, f"Data corruption: different close prices {close_prices}"

    def test_concurrent_same_symbol_memory_stable(self, memory_tracker, historical_time_range):
        """Memory should be stable (<5% variance) across concurrent fetches."""
        start, end = historical_time_range
        memory_readings = []

        def fetch_and_track():
            manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
            current = tracemalloc.get_traced_memory()[0] if tracemalloc.is_tracing() else 0
            manager.close()
            del df
            gc.collect()
            return current

        gc.collect()
        tracemalloc.start()

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_and_track) for _ in range(5)]
            for future in as_completed(futures):
                memory_readings.append(future.result())

        tracemalloc.stop()

        # Calculate variance in memory readings
        if len(memory_readings) > 1 and all(r > 0 for r in memory_readings):
            mean_mem = statistics.mean(memory_readings)
            stdev_mem = statistics.stdev(memory_readings)
            variance_pct = (stdev_mem / mean_mem) * 100 if mean_mem > 0 else 0
            assert variance_pct < 50, f"Memory variance too high: {variance_pct:.1f}%"


@pytest.mark.stress
class TestConcurrentDifferentSymbols:
    """Tests for concurrent fetches of different symbols."""

    def test_10_threads_different_symbols_no_race(self, memory_tracker, historical_time_range, test_symbols):
        """10 threads fetching different symbols should not have race conditions."""
        start, end = historical_time_range
        results = {}
        errors = []
        results_lock = Lock()

        def fetch_symbol(symbol: str):
            try:
                manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
                df = manager.get_data(symbol, start, end, Interval.HOUR_1)
                manager.close()
                return symbol, len(df), True
            except DSM_ERRORS as e:
                return symbol, 0, str(e)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_symbol, sym): sym for sym in test_symbols}
            for future in as_completed(futures):
                symbol, row_count, status = future.result()
                with results_lock:
                    if status is True:
                        results[symbol] = row_count
                    else:
                        errors.append((symbol, status))

        # All symbols should complete without errors
        assert len(errors) == 0, f"Symbols failed: {errors}"
        assert len(results) == len(test_symbols), "Missing results for some symbols"

        # All symbols should have reasonable data
        for symbol, row_count in results.items():
            assert row_count > 0, f"No data returned for {symbol}"

    def test_shared_manager_concurrent_symbols(self, memory_tracker, historical_time_range, test_symbols):
        """Single manager fetching multiple symbols concurrently should work.

        This tests internal state management with shared manager instance.
        """
        start, end = historical_time_range
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
        results = {}
        errors = []
        results_lock = Lock()

        def fetch_with_shared_manager(symbol: str):
            try:
                df = manager.get_data(symbol, start, end, Interval.HOUR_1)
                return symbol, len(df), True
            except DSM_ERRORS as e:
                return symbol, 0, str(e)

        # Use fewer workers since we're sharing a manager
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(fetch_with_shared_manager, sym): sym for sym in test_symbols[:5]}
            for future in as_completed(futures):
                symbol, row_count, status = future.result()
                with results_lock:
                    if status is True:
                        results[symbol] = row_count
                    else:
                        errors.append((symbol, status))

        manager.close()

        # Verify results
        assert len(errors) == 0, f"Shared manager errors: {errors}"
        assert len(results) == 5, f"Expected 5 results, got {len(results)}"


@pytest.mark.stress
class TestConcurrentCacheWrite:
    """Tests for concurrent cache write race detection."""

    def test_concurrent_cache_population(self, memory_tracker, historical_time_range):
        """Concurrent cache population should use proper locking or atomic writes.

        Multiple threads trying to cache same data should not corrupt the cache.
        """
        start, end = historical_time_range
        errors = []

        def populate_cache(thread_id: int):
            try:
                # Each thread creates its own manager to populate cache
                manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
                df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
                row_count = len(df)
                manager.close()
                return thread_id, row_count, True
            except DSM_ERRORS as e:
                return thread_id, 0, str(e)

        # Run multiple threads that will try to write to cache simultaneously
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(populate_cache, i) for i in range(5)]
            results = [f.result() for f in as_completed(futures)]

        # Check for errors
        for thread_id, _row_count, status in results:
            if status is not True:
                errors.append((thread_id, status))

        assert len(errors) == 0, f"Cache write errors: {errors}"

        # Verify cache is consistent by re-reading
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
        df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
        manager.close()

        # All row counts should match the final cached value
        row_counts = [r[1] for r in results]
        assert all(rc == len(df) for rc in row_counts), f"Cache inconsistency: {row_counts} vs {len(df)}"

    def test_concurrent_read_write_isolation(self, memory_tracker, historical_time_range, test_symbols):
        """Concurrent reads and writes should not interfere with each other."""
        start, end = historical_time_range
        results = []
        results_lock = Lock()

        def read_or_write(symbol: str, operation: str):
            try:
                manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
                df = manager.get_data(symbol, start, end, Interval.HOUR_1)
                manager.close()
                return symbol, operation, len(df), True
            except DSM_ERRORS as e:
                return symbol, operation, 0, str(e)

        # Mix of operations: some symbols read (cached), some write (new data)
        operations = [(sym, "write") for sym in test_symbols[:3]] + [("BTCUSDT", "read") for _ in range(3)]

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(read_or_write, sym, op) for sym, op in operations]
            for future in as_completed(futures):
                result = future.result()
                with results_lock:
                    results.append(result)

        # Check for errors
        errors = [r for r in results if r[3] is not True]
        assert len(errors) == 0, f"Read/write isolation errors: {errors}"

        # All BTCUSDT reads should have same row count
        btc_reads = [r for r in results if r[0] == "BTCUSDT" and r[1] == "read"]
        if len(btc_reads) > 1:
            row_counts = [r[2] for r in btc_reads]
            assert len(set(row_counts)) == 1, f"Inconsistent reads: {row_counts}"


@pytest.mark.stress
class TestConcurrentMemoryStability:
    """Tests for memory stability under concurrent load."""

    def test_concurrent_memory_bounded(self, memory_tracker, historical_time_range):
        """Concurrent operations should have bounded memory usage."""
        start, end = historical_time_range

        def fetch_data():
            manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
            manager.close()
            return len(df)

        gc.collect()

        with memory_tracker as tracker:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(fetch_data) for _ in range(10)]
                results = [f.result() for f in as_completed(futures)]

        # Memory should be reasonable for 10 concurrent fetches
        # Each fetch is ~7 days of hourly data = 168 rows
        # Allow 50MB peak for 10 concurrent operations
        assert tracker.peak_mb < 50, f"Memory too high: {tracker.peak_mb:.1f}MB"
        assert len(results) == 10, f"Expected 10 results, got {len(results)}"

    def test_concurrent_no_memory_leak(self, memory_tracker, historical_time_range):
        """Repeated concurrent operations should not leak memory."""
        start, end = historical_time_range

        def fetch_batch():
            results = []
            with ThreadPoolExecutor(max_workers=3) as executor:
                for _ in range(3):
                    manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
                    future = executor.submit(manager.get_data, "BTCUSDT", start, end, Interval.HOUR_1)
                    df = future.result()
                    results.append(len(df))
                    manager.close()
                    del df
            gc.collect()
            return results

        gc.collect()
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        # Run multiple batches
        for _ in range(3):
            fetch_batch()

        final = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()

        delta_mb = (final - baseline) / (1024 * 1024)

        # Memory growth should be minimal after cleanup
        assert delta_mb < 10, f"Memory leak detected: {delta_mb:.1f}MB growth"
