"""Performance benchmarks for Round 15: FCP Concurrency.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md

Validates:
1. Vision step parallelism via ThreadPoolExecutor (≥3x speedup for 5 ranges)
2. Data integrity — merged result has no duplicates
3. Error isolation — failed range doesn't break other fetches
4. REST step stays sequential (rate-limit safe)
"""

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd

from ckvd.utils.for_core.ckvd_fcp_utils import process_rest_step, process_vision_step
from ckvd.utils.market.enums import Interval


def _make_ohlcv_df(start: datetime, hours: int, source: str = "VISION") -> pd.DataFrame:
    """Create a synthetic OHLCV DataFrame for testing."""
    timestamps = [start + timedelta(hours=h) for h in range(hours)]
    df = pd.DataFrame(
        {
            "open_time": timestamps,
            "open": [100.0 + h for h in range(hours)],
            "high": [101.0 + h for h in range(hours)],
            "low": [99.0 + h for h in range(hours)],
            "close": [100.5 + h for h in range(hours)],
            "volume": [1000.0] * hours,
            "_data_source": [source] * hours,
        }
    )
    return df


class TestVisionParallelism:
    """Benchmark: parallel Vision fetch vs sequential baseline."""

    def test_parallel_faster_than_sequential_5_ranges(self):
        """Parallel Vision fetching should be ≥3x faster than sequential for 5 ranges."""
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        missing_ranges = [
            (base + timedelta(days=i), base + timedelta(days=i, hours=23))
            for i in range(5)
        ]
        latency = 0.1  # 100ms simulated latency per range

        def slow_vision_fetch(symbol, start, end, interval):
            time.sleep(latency)
            return _make_ohlcv_df(start, 24)

        # Sequential baseline: call fetch 5 times serially
        seq_start = time.perf_counter()
        for ms, me in missing_ranges:
            slow_vision_fetch("BTCUSDT", ms, me, Interval.HOUR_1)
        seq_elapsed = time.perf_counter() - seq_start

        # Parallel: use process_vision_step
        par_start = time.perf_counter()
        result_df, remaining = process_vision_step(
            fetch_from_vision_func=slow_vision_fetch,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )
        par_elapsed = time.perf_counter() - par_start

        speedup = seq_elapsed / par_elapsed
        assert speedup >= 3.0, (
            f"Expected ≥3x speedup, got {speedup:.1f}x "
            f"(seq={seq_elapsed:.3f}s, par={par_elapsed:.3f}s)"
        )

    def test_single_range_skips_threadpool(self):
        """Single range should skip ThreadPoolExecutor overhead."""
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        missing_ranges = [(base, base + timedelta(hours=23))]

        def fast_fetch(symbol, start, end, interval):
            return _make_ohlcv_df(start, 24)

        with patch("concurrent.futures.ThreadPoolExecutor") as mock_pool:
            result_df, remaining = process_vision_step(
                fetch_from_vision_func=fast_fetch,
                symbol="BTCUSDT",
                missing_ranges=missing_ranges,
                interval=Interval.HOUR_1,
                include_source_info=False,
                result_df=pd.DataFrame(),
            )

            # ThreadPoolExecutor should NOT be used for single range
            mock_pool.assert_not_called()
            assert not result_df.empty


class TestVisionDataIntegrity:
    """Verify parallel Vision fetching preserves data integrity."""

    def test_merged_result_no_duplicates(self):
        """Parallel fetch of 3 overlapping ranges should produce no duplicate timestamps."""
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        # Three ranges with potential overlap (same day)
        missing_ranges = [
            (base, base + timedelta(hours=12)),
            (base + timedelta(hours=6), base + timedelta(hours=18)),
            (base + timedelta(hours=12), base + timedelta(hours=23)),
        ]

        def vision_fetch(symbol, start, end, interval):
            hours = int((end - start).total_seconds() / 3600)
            return _make_ohlcv_df(start, hours)

        result_df, remaining = process_vision_step(
            fetch_from_vision_func=vision_fetch,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )

        assert not result_df.empty
        # merge_dataframes sets open_time as index — check index for duplicates
        if "open_time" in result_df.columns:
            assert result_df["open_time"].is_unique, "Duplicate timestamps in column"
        else:
            assert not result_df.index.has_duplicates, "Duplicate timestamps in index"

    def test_source_column_set_to_vision(self):
        """All Vision data should have _data_source = VISION."""
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        missing_ranges = [
            (base, base + timedelta(hours=23)),
            (base + timedelta(days=1), base + timedelta(days=1, hours=23)),
        ]

        def vision_fetch(symbol, start, end, interval):
            hours = int((end - start).total_seconds() / 3600)
            # Return without _data_source — process_vision_step should add it
            timestamps = [start + timedelta(hours=h) for h in range(hours)]
            return pd.DataFrame(
                {
                    "open_time": timestamps,
                    "open": [100.0] * hours,
                    "high": [101.0] * hours,
                    "low": [99.0] * hours,
                    "close": [100.5] * hours,
                    "volume": [1000.0] * hours,
                }
            )

        result_df, _ = process_vision_step(
            fetch_from_vision_func=vision_fetch,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )

        assert "_data_source" in result_df.columns
        assert (result_df["_data_source"] == "VISION").all()

    def test_existing_data_merged_with_vision(self):
        """Vision results should merge with pre-existing result_df."""
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)

        # Pre-existing data: day 1
        existing_df = _make_ohlcv_df(base, 24, source="CACHE")

        # Missing: day 2
        day2_start = base + timedelta(days=1)
        missing_ranges = [(day2_start, day2_start + timedelta(hours=23))]

        def vision_fetch(symbol, start, end, interval):
            return _make_ohlcv_df(start, 24)

        result_df, _ = process_vision_step(
            fetch_from_vision_func=vision_fetch,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=existing_df,
        )

        # Should have data from both days
        assert len(result_df) == 48


class TestVisionErrorIsolation:
    """Verify failed Vision ranges don't break other fetches."""

    def test_one_failed_range_others_succeed(self):
        """If one Vision range fails, others should still succeed."""
        from ckvd.utils.for_core.vision_exceptions import VisionAPIError

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        missing_ranges = [
            (base, base + timedelta(hours=23)),
            (base + timedelta(days=1), base + timedelta(days=1, hours=23)),
            (base + timedelta(days=2), base + timedelta(days=2, hours=23)),
        ]

        call_count = 0

        def flaky_vision_fetch(symbol, start, end, interval):
            nonlocal call_count
            call_count += 1
            # Fail on the second range
            if start == base + timedelta(days=1):
                raise VisionAPIError("Simulated failure")
            return _make_ohlcv_df(start, 24)

        result_df, remaining = process_vision_step(
            fetch_from_vision_func=flaky_vision_fetch,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )

        # Should have data from 2 successful ranges
        assert not result_df.empty
        assert len(result_df) == 48  # 2 × 24 hours

        # Failed range should be in remaining_missing
        assert len(remaining) >= 1

    def test_empty_vision_result_tracked(self):
        """Empty Vision result should add range to remaining_missing."""
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        missing_ranges = [
            (base, base + timedelta(hours=23)),
            (base + timedelta(days=1), base + timedelta(days=1, hours=23)),
        ]

        def partial_vision_fetch(symbol, start, end, interval):
            # Return empty for second range
            if start == base + timedelta(days=1):
                return pd.DataFrame()
            return _make_ohlcv_df(start, 24)

        result_df, remaining = process_vision_step(
            fetch_from_vision_func=partial_vision_fetch,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=False,
            result_df=pd.DataFrame(),
        )

        assert len(result_df) == 24  # Only first range data
        assert len(remaining) >= 1  # Second range still missing

    def test_all_vision_fail_returns_empty_with_all_ranges(self):
        """If all Vision fetches fail, return empty df and all ranges as remaining."""
        from ckvd.utils.for_core.vision_exceptions import VisionAPIError

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        missing_ranges = [
            (base, base + timedelta(hours=23)),
            (base + timedelta(days=1), base + timedelta(days=1, hours=23)),
        ]

        def failing_fetch(symbol, start, end, interval):
            raise VisionAPIError("All fail")

        result_df, remaining = process_vision_step(
            fetch_from_vision_func=failing_fetch,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=False,
            result_df=pd.DataFrame(),
        )

        assert result_df.empty
        # Adjacent failed ranges may be merged by merge_adjacent_ranges
        assert len(remaining) >= 1


class TestVisionEmptyRanges:
    """Edge cases for empty input."""

    def test_no_missing_ranges_returns_immediately(self):
        """Empty missing_ranges should return immediately."""
        fetch_mock = MagicMock()

        result_df, remaining = process_vision_step(
            fetch_from_vision_func=fetch_mock,
            symbol="BTCUSDT",
            missing_ranges=[],
            interval=Interval.HOUR_1,
            include_source_info=False,
            result_df=pd.DataFrame(),
        )

        fetch_mock.assert_not_called()
        assert result_df.empty
        assert remaining == []


class TestRestSequentialFallback:
    """Verify REST step is sequential when no rest_client or single range."""

    def test_rest_sequential_without_rest_client(self):
        """Without rest_client, process_rest_step fetches ranges sequentially."""
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        missing_ranges = [
            (base + timedelta(days=i), base + timedelta(days=i, hours=23))
            for i in range(3)
        ]

        call_order: list[datetime] = []

        def tracking_rest_fetch(symbol, start, end, interval):
            call_order.append(start)
            return _make_ohlcv_df(start, 24, source="REST")

        result_df = process_rest_step(
            fetch_from_rest_func=tracking_rest_fetch,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
        )

        assert not result_df.empty
        # Verify sequential execution order
        assert call_order == sorted(call_order), "REST calls should be in order"

    def test_rest_single_range_stays_sequential(self):
        """Single range should use sequential path even with rest_client."""
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        missing_ranges = [(base, base + timedelta(hours=23))]

        mock_client = MagicMock()

        def fast_fetch(symbol, start, end, interval):
            return _make_ohlcv_df(start, 24, source="REST")

        result_df = process_rest_step(
            fetch_from_rest_func=fast_fetch,
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
            rest_client=mock_client,
        )

        assert not result_df.empty
        # fetch_klines_parallel should NOT be called for single range
        mock_client.fetch_klines_parallel.assert_not_called()


class TestRestParallelWiring:
    """Verify REST parallel path is wired via fetch_klines_parallel."""

    def test_multiple_ranges_uses_parallel(self):
        """Multiple merged ranges with rest_client should use fetch_klines_parallel."""
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        # Gap of >1 interval between ranges so merge_adjacent_ranges keeps them separate
        missing_ranges = [
            (base, base + timedelta(hours=12)),
            (base + timedelta(days=5), base + timedelta(days=5, hours=12)),
        ]

        mock_client = MagicMock()
        # Return 2 DataFrames (one per range)
        mock_client.fetch_klines_parallel.return_value = [
            _make_ohlcv_df(base, 24, source="REST"),
            _make_ohlcv_df(base + timedelta(days=2), 24, source="REST"),
        ]

        result_df = process_rest_step(
            fetch_from_rest_func=MagicMock(),  # Should not be called
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=pd.DataFrame(),
            rest_client=mock_client,
        )

        # fetch_klines_parallel should have been called
        mock_client.fetch_klines_parallel.assert_called_once()
        assert not result_df.empty
        assert len(result_df) == 48  # 2 × 24 hours

    def test_parallel_rate_limit_returns_partial(self):
        """Rate limit during parallel fetch should return existing data."""
        from ckvd.utils.for_core.rest_exceptions import RateLimitError

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        existing_df = _make_ohlcv_df(base, 24, source="CACHE")
        # Gap of >1 interval between ranges so merge_adjacent_ranges keeps them separate
        missing_ranges = [
            (base + timedelta(days=1), base + timedelta(days=1, hours=23)),
            (base + timedelta(days=7), base + timedelta(days=7, hours=23)),
        ]

        mock_client = MagicMock()
        mock_client.fetch_klines_parallel.side_effect = RateLimitError(
            retry_after=60, message="Rate limited"
        )

        result_df = process_rest_step(
            fetch_from_rest_func=MagicMock(),
            symbol="BTCUSDT",
            missing_ranges=missing_ranges,
            interval=Interval.HOUR_1,
            include_source_info=True,
            result_df=existing_df,
            rest_client=mock_client,
        )

        # Should return existing data with rate-limit markers
        assert len(result_df) == 24
        assert result_df.attrs.get("_rate_limited") is True
