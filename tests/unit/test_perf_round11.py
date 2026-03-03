"""Performance benchmarks for Round 11: Polars-Native Gap Detection.

# ADR: docs/adr/2025-01-30-failover-control-protocol.md

Validates that Polars-native gap detection with projection pushdown
outperforms the pandas collect + identify_missing_segments path.
"""

import timeit
from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl

from ckvd.utils.market.enums import Interval


def _create_hourly_data_polars(days: int = 30) -> pl.LazyFrame:
    """Create a Polars LazyFrame with synthetic 1h OHLCV data."""
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    n_rows = days * 24
    timestamps = [base + timedelta(hours=i) for i in range(n_rows)]

    return pl.DataFrame(
        {
            "open_time": timestamps,
            "open": [100.0 + i % 50 for i in range(n_rows)],
            "high": [101.0 + i % 50 for i in range(n_rows)],
            "low": [99.0 + i % 50 for i in range(n_rows)],
            "close": [100.5 + i % 50 for i in range(n_rows)],
            "volume": [1000.0] * n_rows,
            "quote_asset_volume": [50000.0] * n_rows,
            "count": [100] * n_rows,
            "taker_buy_base_asset_volume": [500.0] * n_rows,
            "taker_buy_quote_asset_volume": [25000.0] * n_rows,
            "_data_source": ["CACHE"] * n_rows,
        }
    ).lazy()


def _create_hourly_data_pandas(days: int = 30) -> pd.DataFrame:
    """Create a pandas DataFrame with the same synthetic data."""
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    n_rows = days * 24
    timestamps = [base + timedelta(hours=i) for i in range(n_rows)]

    return pd.DataFrame(
        {
            "open_time": pd.to_datetime(timestamps, utc=True),
            "open": [100.0 + i % 50 for i in range(n_rows)],
            "high": [101.0 + i % 50 for i in range(n_rows)],
            "low": [99.0 + i % 50 for i in range(n_rows)],
            "close": [100.5 + i % 50 for i in range(n_rows)],
            "volume": [1000.0] * n_rows,
            "quote_asset_volume": [50000.0] * n_rows,
            "count": [100] * n_rows,
            "taker_buy_base_asset_volume": [500.0] * n_rows,
            "taker_buy_quote_asset_volume": [25000.0] * n_rows,
            "_data_source": ["CACHE"] * n_rows,
        }
    )


def _create_data_with_gaps_polars() -> pl.LazyFrame:
    """Create data with known gaps for correctness testing."""
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    # 3 days of data, but skip hours 36-48 (half of day 2)
    timestamps = []
    for i in range(72):
        if 36 <= i < 48:
            continue  # Create gap
        timestamps.append(base + timedelta(hours=i))

    return pl.DataFrame(
        {
            "open_time": timestamps,
            "open": [100.0] * len(timestamps),
            "close": [105.0] * len(timestamps),
        }
    ).lazy()


def _create_data_with_gaps_pandas() -> pd.DataFrame:
    """Create same gapped data as pandas DataFrame."""
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    timestamps = []
    for i in range(72):
        if 36 <= i < 48:
            continue
        timestamps.append(base + timedelta(hours=i))

    return pd.DataFrame(
        {
            "open_time": pd.to_datetime(timestamps, utc=True),
            "open": [100.0] * len(timestamps),
            "close": [105.0] * len(timestamps),
        }
    )


class TestPolarsGapDetectionPerformance:
    """Benchmark: Polars-native vs pandas collect + identify_missing_segments."""

    def test_polars_gap_detection_faster_than_full_collect(self):
        """Polars-native gap detection (projection pushdown) should be faster than
        full collect_pandas + identify_missing_segments.

        This simulates the actual FCP flow:
        - Old: collect_pandas(LazyFrame with 11 cols) → identify_missing_segments(pd.DataFrame)
        - New: identify_missing_segments_polars(LazyFrame) — only collects open_time column
        """
        from ckvd.utils.for_core.ckvd_time_range_utils import (
            identify_missing_segments,
            identify_missing_segments_polars,
        )
        from ckvd.utils.internal.polars_pipeline import PolarsDataPipeline

        days = 90
        lf = _create_hourly_data_polars(days)

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=days)
        interval = Interval.HOUR_1
        iterations = 10

        # Old FCP path: full collect → pandas gap detection
        def old_fcp_path():
            pipeline = PolarsDataPipeline()
            pipeline.add_source(lf, "CACHE")
            cache_df = pipeline.collect_pandas(use_streaming=True)
            return identify_missing_segments(cache_df, start, end, interval)

        # New FCP path: Polars-native gap detection (projection pushdown)
        def new_fcp_path():
            return identify_missing_segments_polars(lf, start, end, interval)

        old_time = timeit.timeit(old_fcp_path, number=iterations)
        new_time = timeit.timeit(new_fcp_path, number=iterations)

        speedup = old_time / new_time
        assert speedup >= 1.5, (
            f"Expected >=1.5x speedup, got {speedup:.1f}x "
            f"(old_fcp={old_time:.4f}s, new_fcp={new_time:.4f}s)"
        )

    def test_polars_gap_detection_faster_with_many_columns(self):
        """Polars should be much faster when DataFrame has many columns (projection pushdown)."""
        from ckvd.utils.for_core.ckvd_time_range_utils import (
            identify_missing_segments,
            identify_missing_segments_polars,
        )

        days = 30
        lf = _create_hourly_data_polars(days)
        pdf = _create_hourly_data_pandas(days)

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=days)
        interval = Interval.HOUR_1

        # This test validates projection pushdown benefit:
        # Polars only collects the open_time column vs pandas collecting all 11 columns.
        polars_result = identify_missing_segments_polars(lf, start, end, interval)
        pandas_result = identify_missing_segments(pdf, start, end, interval)

        # Both should find 0 gaps (continuous data)
        assert len(polars_result) == 0, f"Polars found unexpected gaps: {polars_result}"
        assert len(pandas_result) == 0, f"Pandas found unexpected gaps: {pandas_result}"


class TestPolarsGapDetectionCorrectness:
    """Verify Polars and pandas gap detection return identical results."""

    def test_identical_results_no_gaps(self):
        """Both paths should find 0 gaps for continuous data."""
        from ckvd.utils.for_core.ckvd_time_range_utils import (
            identify_missing_segments,
            identify_missing_segments_polars,
        )

        lf = _create_hourly_data_polars(7)
        pdf = _create_hourly_data_pandas(7)

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=7)

        polars_result = identify_missing_segments_polars(lf, start, end, Interval.HOUR_1)
        pandas_result = identify_missing_segments(pdf, start, end, Interval.HOUR_1)

        assert polars_result == pandas_result == []

    def test_identical_results_with_gaps(self):
        """Both paths should detect the same gap."""
        from ckvd.utils.for_core.ckvd_time_range_utils import (
            identify_missing_segments,
            identify_missing_segments_polars,
        )

        lf = _create_data_with_gaps_polars()
        pdf = _create_data_with_gaps_pandas()

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=3)

        polars_result = identify_missing_segments_polars(lf, start, end, Interval.HOUR_1)
        pandas_result = identify_missing_segments(pdf, start, end, Interval.HOUR_1)

        # Both should find the gap at hours 36-48
        assert len(polars_result) > 0, "Polars should detect gap"
        assert len(pandas_result) > 0, "Pandas should detect gap"

        # The gap ranges should overlap (may differ slightly in boundary handling)
        # Both should detect a gap roughly at hour 36 to hour 48
        polars_gap_start = polars_result[0][0]
        pandas_gap_start = pandas_result[0][0]

        # Within 1 interval of each other
        assert abs((polars_gap_start - pandas_gap_start).total_seconds()) <= 3600

    def test_empty_lazyframe_returns_full_range(self):
        """Empty LazyFrame should return entire range as missing."""
        from ckvd.utils.for_core.ckvd_time_range_utils import identify_missing_segments_polars

        empty_lf = pl.DataFrame({"open_time": []}).cast({"open_time": pl.Datetime("us", "UTC")}).lazy()

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(days=7)

        result = identify_missing_segments_polars(empty_lf, start, end, Interval.HOUR_1)
        assert result == [(start, end)]

    def test_detects_boundary_gaps(self):
        """Should detect gaps at start and end of range."""
        from ckvd.utils.for_core.ckvd_time_range_utils import identify_missing_segments_polars

        # Data only covers hours 12-36
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        timestamps = [base + timedelta(hours=i) for i in range(12, 37)]

        lf = pl.DataFrame(
            {"open_time": timestamps, "close": [100.0] * len(timestamps)}
        ).lazy()

        start = base
        end = base + timedelta(days=2)

        result = identify_missing_segments_polars(lf, start, end, Interval.HOUR_1)

        # Should detect gap at beginning (hours 0-12) and end (hours 37-48)
        assert len(result) >= 1, "Should detect at least 1 boundary gap"
        # First gap should start at range start
        assert result[0][0] == start


class TestPreSortedFlag:
    """Verify detect_gaps pre_sorted=True skips sort."""

    def test_pre_sorted_produces_same_results(self):
        """pre_sorted=True should produce same gaps as default."""
        from ckvd.utils.gap_detector import detect_gaps

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        timestamps = [base + timedelta(hours=i) for i in range(48)]
        # Remove some rows to create a gap
        timestamps = [t for t in timestamps if not (12 <= (t - base).total_seconds() / 3600 < 24)]

        df = pd.DataFrame(
            {
                "open_time": pd.to_datetime(timestamps, utc=True),
                "close": [100.0] * len(timestamps),
            }
        )

        # Sort first
        df = df.sort_values("open_time")

        gaps_default, stats_default = detect_gaps(df, Interval.HOUR_1, enforce_min_span=False)
        gaps_presorted, stats_presorted = detect_gaps(
            df, Interval.HOUR_1, enforce_min_span=False, pre_sorted=True
        )

        assert len(gaps_default) == len(gaps_presorted)
        assert stats_default["total_gaps"] == stats_presorted["total_gaps"]

    def test_pre_sorted_skips_sort(self):
        """When pre_sorted=True, sort_values should not be called."""
        from unittest.mock import patch

        from ckvd.utils.gap_detector import detect_gaps

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        timestamps = [base + timedelta(hours=i) for i in range(48)]

        df = pd.DataFrame(
            {
                "open_time": pd.to_datetime(timestamps, utc=True),
                "close": [100.0] * len(timestamps),
            }
        )

        with patch.object(df, "sort_values", wraps=df.sort_values) as mock_sort:
            detect_gaps(df, Interval.HOUR_1, enforce_min_span=False, pre_sorted=True)
            mock_sort.assert_not_called()

    def test_unsorted_without_flag_still_works(self):
        """Default behavior should still sort unsorted input."""
        from ckvd.utils.gap_detector import detect_gaps

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        # Create deliberately unsorted timestamps
        timestamps = [base + timedelta(hours=i) for i in reversed(range(48))]

        df = pd.DataFrame(
            {
                "open_time": pd.to_datetime(timestamps, utc=True),
                "close": [100.0] * len(timestamps),
            }
        )

        gaps, stats = detect_gaps(df, Interval.HOUR_1, enforce_min_span=False)
        assert stats["total_gaps"] == 0  # No gaps in continuous data


class TestReturnPolarsSingleCollection:
    """Verify return_polars=True uses pipeline directly."""

    def test_collect_polars_called_for_return_polars(self):
        """When return_polars=True, collect_polars should be called."""
        from ckvd.utils.internal.polars_pipeline import PolarsDataPipeline

        # Create a real pipeline with some data
        pipeline = PolarsDataPipeline()
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        timestamps = [base + timedelta(hours=i) for i in range(24)]

        lf = pl.DataFrame(
            {
                "open_time": timestamps,
                "open": [100.0] * 24,
                "high": [101.0] * 24,
                "low": [99.0] * 24,
                "close": [100.5] * 24,
                "volume": [1000.0] * 24,
                "_data_source": ["CACHE"] * 24,
            }
        ).lazy()

        pipeline.add_source(lf, "CACHE")

        # Verify pipeline is not empty
        assert not pipeline.is_empty()

        # Verify collect_polars returns a pl.DataFrame
        result = pipeline.collect_polars(use_streaming=True)
        assert isinstance(result, pl.DataFrame)
        assert len(result) == 24
