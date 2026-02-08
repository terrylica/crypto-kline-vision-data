"""Extreme volume stress tests for CryptoKlineVisionData.

Tests that verify handling of very large datasets:
- Year-long data (365 days x 1440 min = 525,600 rows)
- Memory scaling linearly with row count
- Memory returns to baseline after cleanup

GitHub Issue #9 - P3: Extreme volume tests
"""

import gc
import tracemalloc
from datetime import datetime, timezone

import pytest

from ckvd import DataProvider, CryptoKlineVisionData, Interval, MarketType


@pytest.mark.stress
class TestYearLongData:
    """Tests for year-long data fetches (365 days)."""

    def test_1y_1h_data_completes(self, memory_tracker):
        """365 days of 1h data (~8,760 rows) should complete without OOM.

        This is a baseline test for year-long data at hourly resolution.
        """
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Full year of 2024
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)

        manager.close()

        # 365 days * 24 hours = 8,760 rows expected
        assert len(df) >= 8700, f"Expected ~8,760 rows, got {len(df)}"

        # Memory bound: <50MB for ~9k rows (generous for year-long fetch)
        assert tracker.peak_mb < 50, f"Peak {tracker.peak_mb:.1f}MB exceeds 50MB limit"

    def test_6_month_1m_data_completes(self, memory_tracker):
        """6 months of 1m data (~262,800 rows) should complete without OOM.

        Note: Full year of 1m data (525,600 rows) may take too long for CI.
        This tests half-year as a practical extreme volume test.
        """
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # 6 months of data
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 7, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, Interval.MINUTE_1)

        manager.close()

        # 182 days * 24 hours * 60 min = 262,080 rows expected
        assert len(df) >= 260000, f"Expected ~262,080 rows, got {len(df)}"

        # Memory bound: <300MB for ~260k rows
        # Actual measurement shows ~207MB baseline, ~265MB with Polars pipeline
        # Allow headroom for variation and Polars schema standardization overhead
        assert tracker.peak_mb < 300, f"Peak {tracker.peak_mb:.1f}MB exceeds 300MB limit"

    def test_3_month_1m_data_quality(self, memory_tracker):
        """3 months of 1m data should have consistent quality."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # 3 months of data
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 4, 1, tzinfo=timezone.utc)

        df = manager.get_data("BTCUSDT", start, end, Interval.MINUTE_1)
        manager.close()

        # 90 days * 24 hours * 60 min = 129,600 rows expected
        assert len(df) >= 125000, f"Expected ~129,600 rows, got {len(df)}"

        # Data quality checks
        assert df.index.is_monotonic_increasing, "Timestamps not monotonic"
        assert not df.index.has_duplicates, "Found duplicate timestamps"

        # Check high >= low (allow for minor data anomalies from exchange)
        high_low_violations = (df["high"] < df["low"]).sum()
        violation_rate = high_low_violations / len(df) * 100
        assert violation_rate < 0.1, f"Too many High < Low violations: {violation_rate:.2f}%"

        # Check volume (dropna to handle reindexed missing rows)
        valid_volume = df["volume"].dropna()
        assert (valid_volume >= 0).all(), "Negative volume found"


@pytest.mark.stress
class TestMemoryScaling:
    """Tests that verify memory scales linearly with row count."""

    def test_memory_scales_linearly(self, memory_tracker):
        """Memory should scale linearly, not quadratically, with row count."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Measure memory for 1 week
        start_1w = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_1w = datetime(2024, 1, 8, tzinfo=timezone.utc)

        with memory_tracker as tracker1:
            df_1w = manager.get_data("BTCUSDT", start_1w, end_1w, Interval.HOUR_1)
        peak_1w = tracker1.peak_mb
        rows_1w = len(df_1w)

        # Measure memory for 1 month (4x data)
        start_1m = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_1m = datetime(2024, 2, 1, tzinfo=timezone.utc)

        with memory_tracker as tracker2:
            df_1m = manager.get_data("BTCUSDT", start_1m, end_1m, Interval.HOUR_1)
        peak_1m = tracker2.peak_mb
        rows_1m = len(df_1m)

        manager.close()

        # Calculate scaling factor
        row_ratio = rows_1m / rows_1w if rows_1w > 0 else 1
        memory_ratio = peak_1m / peak_1w if peak_1w > 0 else 1

        # Memory should scale roughly linearly with rows
        # Allow 2x tolerance for fixed overhead
        # If row_ratio is ~4x, memory_ratio should be < 8x
        assert memory_ratio < row_ratio * 2, f"Memory scaling non-linear: rows grew {row_ratio:.1f}x but memory grew {memory_ratio:.1f}x"

    def test_incremental_fetch_memory_stable(self, memory_tracker):
        """Fetching data in increments should not accumulate memory."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        gc.collect()
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        # Fetch 4 weeks incrementally
        for week in range(4):
            start = datetime(2024, 1, 1 + week * 7, tzinfo=timezone.utc)
            end = datetime(2024, 1, 8 + week * 7, tzinfo=timezone.utc)

            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
            del df
            gc.collect()

        final = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()

        manager.close()

        delta_mb = (final - baseline) / (1024 * 1024)

        # Memory growth should be minimal after cleanup
        assert delta_mb < 10, f"Incremental fetch leaked {delta_mb:.1f}MB"


@pytest.mark.stress
class TestMemoryCleanup:
    """Tests that verify memory returns to baseline after operations."""

    def test_large_fetch_cleanup_returns_to_baseline(self, memory_tracker):
        """Memory should return close to baseline after large fetch and cleanup."""
        gc.collect()
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Fetch 30 days of 1m data (~43,200 rows)
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 2, 1, tzinfo=timezone.utc)

        df = manager.get_data("BTCUSDT", start, end, Interval.MINUTE_1)

        # Capture peak after fetch
        _, peak_during = tracemalloc.get_traced_memory()

        # Cleanup
        del df
        manager.close()
        gc.collect()

        # Measure after cleanup
        final = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()

        delta_mb = (final - baseline) / (1024 * 1024)
        peak_mb = peak_during / (1024 * 1024)

        # Memory should return close to baseline (within 15MB)
        assert delta_mb < 15, (
            f"Memory not released: baseline={baseline / 1024 / 1024:.1f}MB, "
            f"peak={peak_mb:.1f}MB, final={final / 1024 / 1024:.1f}MB, delta={delta_mb:.1f}MB"
        )

    def test_repeated_large_fetches_no_accumulation(self, memory_tracker):
        """Repeated large fetches should not accumulate memory."""
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        gc.collect()
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        # Perform 3 large fetches
        for month in range(1, 4):
            start = datetime(2024, month, 1, tzinfo=timezone.utc)
            end = datetime(2024, month + 1, 1, tzinfo=timezone.utc)

            df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
            del df
            gc.collect()

        final = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()

        manager.close()

        delta_mb = (final - baseline) / (1024 * 1024)

        # Should not accumulate memory across fetches
        assert delta_mb < 10, f"Memory accumulated over fetches: {delta_mb:.1f}MB"
