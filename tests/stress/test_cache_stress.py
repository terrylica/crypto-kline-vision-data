"""Cache resilience stress tests for DataSourceManager.

Tests that verify graceful handling of cache issues:
- Corrupted Arrow file recovery
- Partial write handling
- Cache permission errors
- Cache directory issues

GitHub Issue #8 - P3: Cache resilience tests
"""

import contextlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType

# Known DSM errors that can occur during cache recovery
CACHE_RECOVERY_ERRORS = (RuntimeError, ValueError, OSError)


@pytest.mark.stress
class TestCorruptedCacheRecovery:
    """Tests for graceful fallback when cache is corrupted."""

    def test_corrupted_arrow_file_graceful_fallback(self, memory_tracker):
        """Corrupted Arrow file should trigger graceful fallback to REST.

        This test creates a corrupted cache file and verifies DSM
        handles it gracefully without crashing.
        """
        # Use temp directory for cache
        with tempfile.TemporaryDirectory() as temp_cache:
            # Create a corrupted "cache" file
            cache_path = Path(temp_cache) / "binance" / "futures_usdt" / "klines" / "daily" / "BTCUSDT" / "1h"
            cache_path.mkdir(parents=True, exist_ok=True)

            # Write garbage data to simulate corruption
            corrupted_file = cache_path / "2024-01-01.arrow"
            corrupted_file.write_bytes(b"THIS IS NOT A VALID ARROW FILE - CORRUPTED DATA")

            # DSM should handle this gracefully (may log warning, fallback to REST)
            manager = DataSourceManager.create(
                DataProvider.BINANCE,
                MarketType.FUTURES_USDT,
                cache_dir=temp_cache,
            )

            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 2, tzinfo=timezone.utc)

            # Should not crash - may return data from REST or raise recoverable error
            with contextlib.suppress(*CACHE_RECOVERY_ERRORS):
                df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
                # If we get data, it should be valid
                if df is not None and len(df) > 0:
                    assert df.index.is_monotonic_increasing

            manager.close()

    def test_empty_arrow_file_handled(self, memory_tracker):
        """Empty Arrow file should be handled gracefully."""
        with tempfile.TemporaryDirectory() as temp_cache:
            cache_path = Path(temp_cache) / "binance" / "futures_usdt" / "klines" / "daily" / "BTCUSDT" / "1h"
            cache_path.mkdir(parents=True, exist_ok=True)

            # Write empty file
            empty_file = cache_path / "2024-01-01.arrow"
            empty_file.write_bytes(b"")

            manager = DataSourceManager.create(
                DataProvider.BINANCE,
                MarketType.FUTURES_USDT,
                cache_dir=temp_cache,
            )

            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 2, tzinfo=timezone.utc)

            # Should not crash
            with contextlib.suppress(*CACHE_RECOVERY_ERRORS):
                df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
                _ = df

            manager.close()


@pytest.mark.stress
class TestPartialWriteRecovery:
    """Tests for handling incomplete/partial cache writes."""

    def test_truncated_arrow_file_handled(self, memory_tracker):
        """Truncated Arrow file should be handled gracefully.

        Simulates a partial write where the file was cut off mid-write.
        """
        with tempfile.TemporaryDirectory() as temp_cache:
            cache_path = Path(temp_cache) / "binance" / "futures_usdt" / "klines" / "daily" / "BTCUSDT" / "1h"
            cache_path.mkdir(parents=True, exist_ok=True)

            # Write truncated Arrow header (valid magic bytes but incomplete)
            # Arrow files start with "ARROW1" magic bytes
            truncated_file = cache_path / "2024-01-01.arrow"
            truncated_file.write_bytes(b"ARROW1\x00\x00")  # Incomplete header

            manager = DataSourceManager.create(
                DataProvider.BINANCE,
                MarketType.FUTURES_USDT,
                cache_dir=temp_cache,
            )

            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 2, tzinfo=timezone.utc)

            # Should not crash - will fallback or error gracefully
            with contextlib.suppress(*CACHE_RECOVERY_ERRORS):
                df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
                _ = df

            manager.close()

    def test_multiple_corrupted_days_handled(self, memory_tracker):
        """Multiple corrupted cache files should be handled gracefully."""
        with tempfile.TemporaryDirectory() as temp_cache:
            cache_path = Path(temp_cache) / "binance" / "futures_usdt" / "klines" / "daily" / "BTCUSDT" / "1h"
            cache_path.mkdir(parents=True, exist_ok=True)

            # Create multiple corrupted files
            for day in range(1, 4):
                corrupted_file = cache_path / f"2024-01-0{day}.arrow"
                corrupted_file.write_bytes(b"CORRUPTED DATA")

            manager = DataSourceManager.create(
                DataProvider.BINANCE,
                MarketType.FUTURES_USDT,
                cache_dir=temp_cache,
            )

            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 4, tzinfo=timezone.utc)

            # Should not crash
            with contextlib.suppress(*CACHE_RECOVERY_ERRORS):
                df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
                _ = df

            manager.close()


@pytest.mark.stress
class TestCacheDirectoryIssues:
    """Tests for cache directory edge cases."""

    def test_nonexistent_cache_dir_created(self, memory_tracker):
        """Non-existent cache directory should be created automatically."""
        with tempfile.TemporaryDirectory() as temp_base:
            # Use a path that doesn't exist yet
            cache_dir = Path(temp_base) / "new_cache_dir" / "nested"

            manager = DataSourceManager.create(
                DataProvider.BINANCE,
                MarketType.FUTURES_USDT,
                cache_dir=str(cache_dir),
            )

            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 2, tzinfo=timezone.utc)

            # Should work - directory created as needed
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
            assert len(df) > 0, "Should return data"
            manager.close()

    def test_cache_with_special_characters_in_path(self, memory_tracker):
        """Cache path with special characters should be handled."""
        with tempfile.TemporaryDirectory() as temp_base:
            # Path with spaces and special chars (common on user systems)
            cache_dir = Path(temp_base) / "cache dir with spaces"
            cache_dir.mkdir(parents=True, exist_ok=True)

            manager = DataSourceManager.create(
                DataProvider.BINANCE,
                MarketType.FUTURES_USDT,
                cache_dir=str(cache_dir),
            )

            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 2, tzinfo=timezone.utc)

            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
            assert len(df) > 0, "Should return data"
            manager.close()


@pytest.mark.stress
class TestCacheRecoveryMemory:
    """Tests for memory stability during cache recovery."""

    def test_cache_recovery_memory_bounded(self, memory_tracker):
        """Cache recovery operations should have bounded memory usage."""
        with tempfile.TemporaryDirectory() as temp_cache:
            cache_path = Path(temp_cache) / "binance" / "futures_usdt" / "klines" / "daily" / "BTCUSDT" / "1h"
            cache_path.mkdir(parents=True, exist_ok=True)

            # Create corrupted file
            corrupted_file = cache_path / "2024-01-01.arrow"
            corrupted_file.write_bytes(b"CORRUPTED")

            manager = DataSourceManager.create(
                DataProvider.BINANCE,
                MarketType.FUTURES_USDT,
                cache_dir=temp_cache,
            )

            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 2, tzinfo=timezone.utc)

            with memory_tracker as tracker:
                with contextlib.suppress(*CACHE_RECOVERY_ERRORS):
                    df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
                    _ = df

            manager.close()

            # Recovery should use bounded memory
            assert tracker.peak_mb < 50, f"Cache recovery used {tracker.peak_mb:.1f}MB"

    def test_repeated_cache_failures_no_leak(self, memory_tracker):
        """Repeated cache failures should not leak memory."""
        import gc
        import tracemalloc

        with tempfile.TemporaryDirectory() as temp_cache:
            cache_path = Path(temp_cache) / "binance" / "futures_usdt" / "klines" / "daily" / "BTCUSDT" / "1h"
            cache_path.mkdir(parents=True, exist_ok=True)

            manager = DataSourceManager.create(
                DataProvider.BINANCE,
                MarketType.FUTURES_USDT,
                cache_dir=temp_cache,
            )

            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 2, tzinfo=timezone.utc)

            gc.collect()
            tracemalloc.start()
            baseline = tracemalloc.get_traced_memory()[0]

            # Create corrupted file and try to read multiple times
            for i in range(3):
                corrupted_file = cache_path / "2024-01-01.arrow"
                corrupted_file.write_bytes(f"CORRUPTED_{i}".encode())

                with contextlib.suppress(*CACHE_RECOVERY_ERRORS):
                    df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
                    del df
                gc.collect()

            final = tracemalloc.get_traced_memory()[0]
            tracemalloc.stop()
            manager.close()

            delta_mb = (final - baseline) / (1024 * 1024)
            assert delta_mb < 10, f"Repeated cache failures leaked {delta_mb:.1f}MB"
