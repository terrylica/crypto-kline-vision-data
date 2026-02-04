"""Fault tolerance stress tests.

Tests that verify graceful handling of edge cases:
- Empty results
- Invalid symbols
- Error recovery without memory leaks
"""

import contextlib
import gc
import tracemalloc
from datetime import datetime, timezone

import pytest
from tenacity import RetryError

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType


@pytest.mark.stress
class TestEmptyResults:
    """Tests for handling empty/missing data gracefully."""

    def test_empty_result_no_memory_leak(self, memory_tracker):
        """Empty result should not allocate unnecessary memory."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

        # Date before BTC existed on Binance
        ancient_start = datetime(2010, 1, 1, tzinfo=timezone.utc)
        ancient_end = datetime(2010, 1, 2, tzinfo=timezone.utc)

        gc.collect()
        tracemalloc.start()
        tracemalloc.get_traced_memory()[0]

        # Expect error or empty result - use contextlib.suppress for expected errors
        with contextlib.suppress(RuntimeError, ValueError, RetryError):
            df = manager.get_data("BTCUSDT", ancient_start, ancient_end, Interval.DAY_1)
            # If we get here, result should be empty
            assert df is None or len(df) == 0, "Expected empty result for ancient dates"

        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        manager.close()

        # Empty result should use minimal memory (<50MB)
        peak_mb = peak / (1024 * 1024)
        assert peak_mb < 50, f"Empty result used {peak_mb:.1f}MB"

    def test_invalid_symbol_no_memory_leak(self, memory_tracker):
        """Invalid symbol should fail without memory leak."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        gc.collect()
        tracemalloc.start()
        tracemalloc.get_traced_memory()[0]

        # Invalid symbol - expect error (suppress known error types)
        with contextlib.suppress(RuntimeError, ValueError, RetryError):
            manager.get_data("NOTAREALSYMBOL", start, end, Interval.HOUR_1)

        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        manager.close()

        # Error handling should use minimal memory
        peak_mb = peak / (1024 * 1024)
        assert peak_mb < 50, f"Invalid symbol used {peak_mb:.1f}MB"


@pytest.mark.stress
class TestErrorRecovery:
    """Tests for recovery after errors."""

    def test_recovery_after_error_no_leak(self, memory_tracker):
        """Manager should recover cleanly after an error."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        gc.collect()
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        # First: cause an error (invalid request)
        with contextlib.suppress(RuntimeError, ValueError, RetryError):
            manager.get_data("NOTAREALSYMBOL", start, end, Interval.HOUR_1)

        gc.collect()
        after_error = tracemalloc.get_traced_memory()[0]

        # Then: make a valid request
        df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)

        tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()

        manager.close()
        del df
        gc.collect()

        # Error shouldn't have caused significant memory growth
        error_growth_mb = (after_error - baseline) / (1024 * 1024)
        assert error_growth_mb < 20, f"Error handling leaked {error_growth_mb:.1f}MB"

    def test_multiple_errors_stable_memory(self, memory_tracker):
        """Multiple errors should not accumulate memory."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        gc.collect()
        tracemalloc.start()
        baseline_snapshot = tracemalloc.take_snapshot()

        # Cause 5 errors
        for i in range(5):
            with contextlib.suppress(RuntimeError, ValueError, RetryError):
                manager.get_data(f"INVALID{i}", start, end, Interval.HOUR_1)
            gc.collect()

        final_snapshot = tracemalloc.take_snapshot()
        tracemalloc.stop()
        manager.close()

        # Calculate memory delta
        diff = final_snapshot.compare_to(baseline_snapshot, "lineno")
        total_delta_mb = sum(stat.size_diff for stat in diff) / (1024 * 1024)

        # Multiple errors shouldn't accumulate significant memory
        assert total_delta_mb < 10, f"Error handling accumulated {total_delta_mb:.1f}MB"


@pytest.mark.stress
class TestSymbolValidation:
    """Tests for symbol format validation."""

    def test_wrong_symbol_format_for_market(self, memory_tracker):
        """Wrong symbol format should handle gracefully (either data, empty, or error).

        Note: DSM may return data for "wrong" symbol formats if the underlying
        Vision API has data available. This test verifies memory bounds regardless
        of the outcome.
        """
        # FUTURES_COIN typically expects USD_PERP format, but BTCUSDT may exist
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_COIN)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        gc.collect()
        tracemalloc.start()

        # BTCUSDT may or may not work for FUTURES_COIN depending on Vision data
        # Any outcome is acceptable - we're testing memory bounds
        with contextlib.suppress(ValueError, RuntimeError, RetryError):
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
            _ = df  # Use the variable

        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        manager.close()

        # Symbol handling should use bounded memory regardless of outcome
        peak_mb = peak / (1024 * 1024)
        assert peak_mb < 50, f"Symbol handling used {peak_mb:.1f}MB"

    def test_correct_coin_margined_symbol(self, memory_tracker):
        """Correct coin-margined symbol should work."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_COIN)

        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSD_PERP", start, end, Interval.HOUR_1)

        manager.close()

        # Should return data for correct format
        if df is not None and len(df) > 0:
            # Memory should be reasonable
            assert tracker.peak_mb < 50, f"Correct symbol used {tracker.peak_mb:.1f}MB"
