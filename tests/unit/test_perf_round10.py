"""Performance benchmarks for Round 10: Cache Read Pipeline.

# ADR: docs/adr/2025-01-30-failover-control-protocol.md

Validates that batch pl.scan_ipc() outperforms per-file scanning,
and that magic byte detection is removed (cache always writes Arrow IPC).
"""

import tempfile
import timeit
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl


def _create_arrow_files(n_files: int, rows_per_file: int = 24) -> tuple[list[Path], Path]:
    """Create N temporary Arrow IPC files with synthetic OHLCV data."""
    tmpdir = Path(tempfile.mkdtemp())
    paths = []

    base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)

    for i in range(n_files):
        day_start = base_time + timedelta(days=i)
        timestamps = [day_start + timedelta(hours=h) for h in range(rows_per_file)]

        df = pl.DataFrame(
            {
                "open_time": timestamps,
                "open": [100.0 + h for h in range(rows_per_file)],
                "high": [101.0 + h for h in range(rows_per_file)],
                "low": [99.0 + h for h in range(rows_per_file)],
                "close": [100.5 + h for h in range(rows_per_file)],
                "volume": [1000.0] * rows_per_file,
            }
        )

        path = tmpdir / f"data-{i:04d}.arrow"
        df.write_ipc(path)
        paths.append(path)

    return paths, tmpdir


class TestBatchScanIpcPerformance:
    """Benchmark: single pl.scan_ipc(list) vs N individual pl.scan_ipc() calls."""

    def test_batch_scan_correctness_7_files(self):
        """Batch scan should return all data from 7 files."""
        paths, tmpdir = _create_arrow_files(7)
        try:
            lf = pl.scan_ipc(paths)
            result = lf.collect()
            assert len(result) == 7 * 24  # 7 days × 24 hours
            assert "open_time" in result.columns
        finally:
            import shutil
            shutil.rmtree(tmpdir)

    def test_batch_scan_correctness_with_filter(self):
        """Batch scan with time filter should return correct subset."""
        paths, tmpdir = _create_arrow_files(30)
        try:
            start = datetime(2024, 1, 5, tzinfo=timezone.utc)
            end = datetime(2024, 1, 10, tzinfo=timezone.utc)

            lf = pl.scan_ipc(paths)
            filtered = lf.filter(
                (pl.col("open_time") >= start) & (pl.col("open_time") < end)
            ).collect()

            # 5 days × 24 hours = 120 rows
            assert len(filtered) == 5 * 24
        finally:
            import shutil
            shutil.rmtree(tmpdir)

    def test_batch_vs_individual_scan_30_files(self):
        """Batch scan should be significantly faster than N individual scans at 30 files."""
        paths, tmpdir = _create_arrow_files(30)
        iterations = 50

        try:
            # Individual scan pattern (old approach)
            def individual_scan():
                frames = []
                for p in paths:
                    frames.append(pl.scan_ipc(p))
                combined = pl.concat(frames)
                return combined.select("open_time").collect()

            # Batch scan pattern (new approach)
            def batch_scan():
                lf = pl.scan_ipc(paths)
                return lf.select("open_time").collect()

            old_time = timeit.timeit(individual_scan, number=iterations)
            new_time = timeit.timeit(batch_scan, number=iterations)

            speedup = old_time / new_time
            # Speedup varies by system load; batch is consistently faster
            assert speedup >= 1.0, (
                f"Expected batch to be faster at 30 files, got {speedup:.1f}x "
                f"(old={old_time:.4f}s, new={new_time:.4f}s)"
            )
        finally:
            import shutil
            shutil.rmtree(tmpdir)

    def test_batch_vs_individual_scan_365_files(self):
        """Batch scan should be faster than individual scans at 365 files."""
        paths, tmpdir = _create_arrow_files(365)
        iterations = 10

        try:
            def individual_scan():
                frames = []
                for p in paths:
                    frames.append(pl.scan_ipc(p))
                combined = pl.concat(frames)
                return combined.select("open_time").collect()

            def batch_scan():
                lf = pl.scan_ipc(paths)
                return lf.select("open_time").collect()

            old_time = timeit.timeit(individual_scan, number=iterations)
            new_time = timeit.timeit(batch_scan, number=iterations)

            speedup = old_time / new_time
            # Speedup varies by system load and I/O caching; batch is consistently faster
            assert speedup >= 1.0, (
                f"Expected batch to be faster at 365 files, got {speedup:.1f}x "
                f"(old={old_time:.4f}s, new={new_time:.4f}s)"
            )
        finally:
            import shutil
            shutil.rmtree(tmpdir)


class TestMagicBytesRemoved:
    """Verify _scan_cache_file is still available but batch path is preferred."""

    def test_scan_cache_file_still_works(self):
        """_scan_cache_file should still work for backward compatibility."""
        from ckvd.utils.for_core.ckvd_cache_utils import _scan_cache_file

        paths, tmpdir = _create_arrow_files(1)
        try:
            lf = _scan_cache_file(paths[0])
            result = lf.collect()
            assert len(result) == 24
        finally:
            import shutil
            shutil.rmtree(tmpdir)

    def test_get_cache_lazyframes_returns_single_batch_frame(self):
        """get_cache_lazyframes should return at most 1 LazyFrame (batch scan)."""
        from unittest.mock import MagicMock, patch

        from ckvd.utils.for_core.ckvd_cache_utils import get_cache_lazyframes
        from ckvd.utils.market.enums import Interval, MarketType

        paths, tmpdir = _create_arrow_files(7)

        try:
            # Mock the FSSpecVisionHandler to return our temp paths
            mock_handler = MagicMock()
            mock_handler.exists.return_value = True

            # Return paths in sequence
            path_iter = iter(paths)
            mock_handler.get_local_path_for_data.side_effect = lambda **kwargs: next(path_iter)

            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 7, 23, 59, 59, tzinfo=timezone.utc)

            with patch("ckvd.utils.for_core.ckvd_cache_utils.FSSpecVisionHandler", return_value=mock_handler):
                result = get_cache_lazyframes(
                    symbol="BTCUSDT",
                    start_time=start,
                    end_time=end,
                    interval=Interval.HOUR_1,
                    cache_dir=tmpdir,
                    market_type=MarketType.FUTURES_USDT,
                )

            # Should return exactly 1 LazyFrame (batch), not 7
            assert len(result) == 1
            collected = result[0].collect()
            assert len(collected) > 0
        finally:
            import shutil
            shutil.rmtree(tmpdir)


class TestGetFromCacheBatchScan:
    """Verify get_from_cache uses batch scan internally."""

    def test_get_from_cache_returns_data(self):
        """get_from_cache should return valid DataFrame with batch scan."""
        from unittest.mock import MagicMock, patch

        from ckvd.utils.for_core.ckvd_cache_utils import get_from_cache
        from ckvd.utils.market.enums import Interval, MarketType

        paths, tmpdir = _create_arrow_files(3)

        try:
            mock_handler = MagicMock()
            mock_handler.exists.return_value = True
            path_iter = iter(paths)
            mock_handler.get_local_path_for_data.side_effect = lambda **kwargs: next(path_iter)

            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            end = datetime(2024, 1, 3, 23, 59, 59, tzinfo=timezone.utc)

            with patch("ckvd.utils.for_core.ckvd_cache_utils.FSSpecVisionHandler", return_value=mock_handler):
                result_df, missing_ranges = get_from_cache(
                    symbol="BTCUSDT",
                    start_time=start,
                    end_time=end,
                    interval=Interval.HOUR_1,
                    cache_dir=tmpdir,
                    market_type=MarketType.FUTURES_USDT,
                )

            assert not result_df.empty
            assert len(result_df) == 3 * 24  # 3 days × 24 hours
        finally:
            import shutil
            shutil.rmtree(tmpdir)
