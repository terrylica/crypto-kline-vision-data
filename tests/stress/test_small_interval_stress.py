"""Small interval stress tests for DataSourceManager.

Tests that verify high-frequency data handling:
- 1s interval (SPOT only) with large row counts
- 1m interval with multi-day ranges
- Memory bounds for small interval data

GitHub Issue #6 - P1: Small interval stress tests

Note: 1s interval tests use enforce_source=REST because:
1. 1s data is not available on Vision API (only klines files, not 1s)
2. There's a known Vision API interface issue (fetch vs fetch_data)
"""

from datetime import datetime, timedelta, timezone

import pytest

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType
from data_source_manager.core.sync.dsm_types import DataSource


@pytest.mark.stress
class TestOneSecondInterval:
    """1s interval stress tests (SPOT market only).

    Note: 1s interval is only available for SPOT market.
    Uses REST API directly since Vision API doesn't have 1s data.
    """

    def test_1s_spot_2_hours_completes(self, memory_tracker):
        """2 hours of 1s data (~7,200 rows) should complete without OOM.

        Uses REST API directly since Vision API doesn't support 1s interval.
        Reduced from 4 hours to minimize REST API load.
        """
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

        # Use recent data for REST API (within last 30 days)
        end = datetime.now(timezone.utc) - timedelta(hours=2)
        start = end - timedelta(hours=2)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, Interval.SECOND_1, enforce_source=DataSource.REST)

        manager.close()

        # Should return substantial data
        # 2 hours = 7,200 rows expected (may be less due to REST API pagination)
        assert len(df) >= 5000, f"Expected ~7,200 rows, got {len(df)}"

        # Memory bound: <25MB for 7k rows
        assert tracker.peak_mb < 25, f"Peak {tracker.peak_mb:.1f}MB exceeds 25MB limit"

    def test_1s_data_timestamps_monotonic(self, memory_tracker):
        """1s data should have monotonic, no-duplicate timestamps."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

        # Use recent data for REST API
        end = datetime.now(timezone.utc) - timedelta(hours=1)
        start = end - timedelta(minutes=30)

        df = manager.get_data("BTCUSDT", start, end, Interval.SECOND_1, enforce_source=DataSource.REST)
        manager.close()

        if len(df) == 0:
            pytest.skip("No 1s data returned - REST API may be unavailable")

        # Timestamps must be monotonic increasing
        assert df.index.is_monotonic_increasing, "Timestamps not monotonic"

        # No duplicates allowed
        assert not df.index.has_duplicates, "Found duplicate timestamps in 1s data"


@pytest.mark.stress
class TestOneMinuteInterval:
    """1m interval stress tests."""

    def test_1m_7_days_completes(self, memory_tracker):
        """7 days of 1m data (~10,080 rows) should complete without OOM."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # 7 days * 24 hours * 60 minutes = 10,080 rows expected
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 8, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, Interval.MINUTE_1)

        manager.close()

        # Should return substantial data
        assert len(df) >= 10000, f"Expected ~10,080 rows, got {len(df)}"

        # Memory bound: <25MB for 10k rows
        assert tracker.peak_mb < 25, f"Peak {tracker.peak_mb:.1f}MB exceeds 25MB limit"

    def test_1m_large_row_count_validates(self, memory_tracker):
        """Verify 1m data with large row count has valid structure.

        Uses same 7-day range as test_1m_7_days_completes but validates
        data quality for 10k+ rows.
        """
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # 7 days * 24 hours * 60 minutes = 10,080 rows expected
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 8, tzinfo=timezone.utc)

        with memory_tracker as tracker:
            df = manager.get_data("BTCUSDT", start, end, Interval.MINUTE_1)

        manager.close()

        # Should return substantial data
        assert len(df) >= 10000, f"Expected ~10,080 rows, got {len(df)}"

        # Memory bound: <25MB for 10k rows
        assert tracker.peak_mb < 25, f"Peak {tracker.peak_mb:.1f}MB exceeds 25MB limit"

        # Data quality checks for large datasets
        assert df.index.is_monotonic_increasing, "Timestamps not monotonic"
        assert not df.index.has_duplicates, "Found duplicate timestamps"
        assert (df["high"] >= df["low"]).all(), "High < Low violation"
        assert (df["volume"] >= 0).all(), "Negative volume"

    def test_1m_data_timestamps_monotonic(self, memory_tracker):
        """1m data should have monotonic, no-duplicate timestamps."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 3, tzinfo=timezone.utc)

        df = manager.get_data("BTCUSDT", start, end, Interval.MINUTE_1)
        manager.close()

        # Timestamps must be monotonic increasing
        assert df.index.is_monotonic_increasing, "Timestamps not monotonic"

        # No duplicates allowed
        assert not df.index.has_duplicates, "Found duplicate timestamps in 1m data"

    def test_1m_ohlcv_constraints_valid(self, memory_tracker):
        """1m OHLCV data should satisfy logical constraints."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        df = manager.get_data("BTCUSDT", start, end, Interval.MINUTE_1)
        manager.close()

        assert len(df) > 0, "No data returned"

        # OHLCV constraints
        assert (df["high"] >= df["low"]).all(), "High < Low violation"
        assert (df["high"] >= df["open"]).all(), "High < Open violation"
        assert (df["high"] >= df["close"]).all(), "High < Close violation"
        assert (df["low"] <= df["open"]).all(), "Low > Open violation"
        assert (df["low"] <= df["close"]).all(), "Low > Close violation"
        assert (df["volume"] >= 0).all(), "Negative volume"


@pytest.mark.stress
class TestSmallIntervalMemory:
    """Memory bounds for small interval data."""

    def test_memory_scales_linearly_with_rows(self, memory_tracker):
        """Memory should scale roughly linearly with row count."""
        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        # Fetch 1 day (1440 rows)
        start_1d = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_1d = datetime(2024, 1, 2, tzinfo=timezone.utc)

        with memory_tracker as tracker1:
            df_1d = manager.get_data("BTCUSDT", start_1d, end_1d, Interval.MINUTE_1)
        peak_1d = tracker1.peak_mb
        rows_1d = len(df_1d)

        # Fetch 3 days (4320 rows)
        start_3d = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_3d = datetime(2024, 1, 4, tzinfo=timezone.utc)

        with memory_tracker as tracker3:
            df_3d = manager.get_data("BTCUSDT", start_3d, end_3d, Interval.MINUTE_1)
        peak_3d = tracker3.peak_mb
        rows_3d = len(df_3d)

        manager.close()

        # Memory per row shouldn't increase drastically (allow 2x variance)
        if rows_1d > 0 and rows_3d > 0 and peak_1d > 0:
            mb_per_row_1d = peak_1d / rows_1d
            mb_per_row_3d = peak_3d / rows_3d
            ratio = mb_per_row_3d / mb_per_row_1d if mb_per_row_1d > 0 else float("inf")

            # 3x data shouldn't have worse than 2x memory per row
            assert ratio < 2.0, (
                f"Memory scaling not linear: "
                f"1d={mb_per_row_1d:.4f}MB/row, 3d={mb_per_row_3d:.4f}MB/row, ratio={ratio:.2f}x"
            )

    def test_small_interval_cleanup_no_leak(self, memory_tracker):
        """Repeated small interval fetches should not leak memory."""
        import gc
        import tracemalloc

        manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)

        gc.collect()
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        # Fetch 3 times
        for _ in range(3):
            df = manager.get_data("BTCUSDT", start, end, Interval.MINUTE_1)
            del df
            gc.collect()

        final = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()

        manager.close()

        delta_mb = (final - baseline) / (1024 * 1024)

        # Memory growth should be minimal after cleanup
        assert delta_mb < 5, f"Memory leak detected: {delta_mb:.1f}MB growth"
