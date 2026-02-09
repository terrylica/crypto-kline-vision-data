#!/usr/bin/env python3
"""Unit tests for Polars merge equivalence validation.

Tests validate that the FCP merge_dataframes() behavior is correctly preserved
when migrating to Polars. These tests serve as a regression safety net for
Phase 2 Polars pipeline migration.

Copy from: tests/unit/utils/for_core/test_ckvd_fcp_utils.py
Task: #73 - Create test_merge_dataframes_polars.py (12 tests)

ADR: docs/adr/2025-01-30-failover-control-protocol.md
Plan: /Users/terryli/.claude/plans/gleaming-frolicking-engelbart.md
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def base_time():
    """Fixed base time for reproducible tests."""
    return datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def ohlcv_columns():
    """Standard OHLCV columns for market data."""
    return ["open_time", "open", "high", "low", "close", "volume"]


def make_ohlcv_df(
    base_time: datetime,
    hours: int,
    source: str | None = None,
    offset_hours: int = 0,
    open_base: float = 100.0,
) -> pd.DataFrame:
    """Create a standard OHLCV DataFrame for testing.

    Args:
        base_time: Starting timestamp.
        hours: Number of hourly candles to create.
        source: Optional _data_source value.
        offset_hours: Offset from base_time in hours.
        open_base: Base value for open price.

    Returns:
        pd.DataFrame: OHLCV data with optional _data_source column.
    """
    start = base_time + timedelta(hours=offset_hours)
    timestamps = [start + timedelta(hours=i) for i in range(hours)]

    data = {
        "open_time": timestamps,
        "open": [open_base + i * 10 for i in range(hours)],
        "high": [open_base + 100 + i * 10 for i in range(hours)],
        "low": [open_base - 100 + i * 10 for i in range(hours)],
        "close": [open_base + 50 + i * 10 for i in range(hours)],
        "volume": [1000.0 + i * 100 for i in range(hours)],
    }

    if source is not None:
        data["_data_source"] = [source] * hours

    return pd.DataFrame(data)


# =============================================================================
# Test 1: Empty list returns empty DataFrame
# =============================================================================


class TestPolarsEquivalence:
    """Tests for merge_dataframes() Polars migration equivalence."""

    def test_polars_merge_empty_list_returns_empty_df(self):
        """Empty list should return empty DataFrame.

        Polars equivalent: pl.concat([]) should handle empty list gracefully.
        Migration risk: LOW - Both libraries handle empty lists.
        """
        result = merge_dataframes([])

        assert isinstance(result, pd.DataFrame)
        assert result.empty
        assert len(result) == 0

    def test_polars_merge_single_df_standardized(self, base_time):
        """Single DataFrame should be standardized and returned.

        Polars equivalent: pl.concat([lf]).collect() should return same data.
        Migration risk: LOW - Single frame passthrough.
        """
        input_df = make_ohlcv_df(base_time, hours=6, source="CACHE")
        result = merge_dataframes([input_df])

        assert len(result) == 6
        assert "_data_source" in result.columns
        # Data should be preserved
        assert result["open"].iloc[0] == 100.0

    def test_polars_merge_two_sources_priority_resolution(self, base_time):
        """Two non-overlapping sources should be merged correctly.

        Polars equivalent: pl.concat([lf1, lf2]).sort("open_time")
        Migration risk: MEDIUM - Ensure sort order preserved.
        """
        df_cache = make_ohlcv_df(base_time, hours=6, source="CACHE")
        df_vision = make_ohlcv_df(base_time, hours=6, source="VISION", offset_hours=6)

        result = merge_dataframes([df_cache, df_vision])

        assert len(result) == 12
        assert "_data_source" in result.columns
        # Check both sources present
        sources = result["_data_source"].unique()
        assert "CACHE" in sources
        assert "VISION" in sources

    def test_polars_three_way_merge_matches_pandas(self, base_time):
        """Three-way merge should produce consistent results.

        This is the critical FCP test: Cache + Vision + REST merge.
        Polars equivalent: pl.concat([cache, vision, rest]).unique(keep="last")
        Migration risk: HIGH - Core FCP logic must be preserved.
        """
        df_cache = make_ohlcv_df(base_time, hours=4, source="CACHE")
        df_vision = make_ohlcv_df(base_time, hours=4, source="VISION", offset_hours=4)
        df_rest = make_ohlcv_df(base_time, hours=4, source="REST", offset_hours=8)

        result = merge_dataframes([df_cache, df_vision, df_rest])

        # Should have 12 unique timestamps
        assert len(result) == 12
        # All three sources should be present
        source_counts = result["_data_source"].value_counts()
        assert source_counts.get("CACHE", 0) == 4
        assert source_counts.get("VISION", 0) == 4
        assert source_counts.get("REST", 0) == 4

    def test_polars_merge_overlapping_timestamps_keeps_highest_priority(self, base_time):
        """Overlapping timestamps should keep highest priority source.

        FCP Priority: REST(3) > CACHE(2) > VISION(1) > UNKNOWN(0)
        Polars equivalent: .unique(keep="last") after sort by [open_time, priority]
        Migration risk: HIGH - Priority resolution is critical.
        """
        # Same timestamps, different sources
        df_vision = make_ohlcv_df(base_time, hours=6, source="VISION", open_base=100.0)
        df_rest = make_ohlcv_df(base_time, hours=6, source="REST", open_base=200.0)

        result = merge_dataframes([df_vision, df_rest])

        # Should have 6 rows (duplicates removed)
        assert len(result) == 6
        # REST should win due to higher priority
        assert (result["_data_source"] == "REST").all()
        # Values should be from REST DataFrame
        assert result["open"].iloc[0] == 200.0

    def test_polars_merge_rest_higher_priority_than_cache(self, base_time):
        """REST should override CACHE for duplicate timestamps.

        Polars equivalent: priority mapping via .replace() + sort + unique.
        Migration risk: MEDIUM - Priority mapping must match.
        """
        df_cache = make_ohlcv_df(base_time, hours=6, source="CACHE", open_base=100.0)
        df_rest = make_ohlcv_df(base_time, hours=6, source="REST", open_base=300.0)

        result = merge_dataframes([df_cache, df_rest])

        assert len(result) == 6
        assert (result["_data_source"] == "REST").all()
        assert result["open"].iloc[0] == 300.0

    def test_polars_merge_cache_higher_priority_than_vision(self, base_time):
        """CACHE should override VISION for duplicate timestamps.

        Polars equivalent: Same priority mapping logic.
        Migration risk: MEDIUM - Priority order must be preserved.
        """
        df_vision = make_ohlcv_df(base_time, hours=6, source="VISION", open_base=100.0)
        df_cache = make_ohlcv_df(base_time, hours=6, source="CACHE", open_base=200.0)

        result = merge_dataframes([df_vision, df_cache])

        assert len(result) == 6
        assert (result["_data_source"] == "CACHE").all()
        assert result["open"].iloc[0] == 200.0

    def test_source_priority_mapping_correct(self, base_time):
        """Verify exact source priority: REST(3) > CACHE(2) > VISION(1) > UNKNOWN(0).

        This test verifies the priority mapping used in merge_dataframes().
        Polars equivalent: pl.col("_data_source").replace({"REST": 3, "CACHE": 2, ...})
        Migration risk: HIGH - Must match exactly for FCP correctness.
        """
        # Create single-timestamp DataFrames with all priority levels
        timestamp = base_time

        dfs = []
        for source, value in [
            ("UNKNOWN", 100),
            ("VISION", 200),
            ("CACHE", 300),
            ("REST", 400),
        ]:
            df = pd.DataFrame(
                {
                    "open_time": [timestamp],
                    "open": [float(value)],
                    "high": [float(value + 10)],
                    "low": [float(value - 10)],
                    "close": [float(value + 5)],
                    "volume": [1000.0],
                    "_data_source": [source],
                }
            )
            dfs.append(df)

        result = merge_dataframes(dfs)

        # Should have 1 row (all duplicates merged)
        assert len(result) == 1
        # REST should win (highest priority)
        assert result["_data_source"].iloc[0] == "REST"
        # Value should be from REST DataFrame
        assert result["open"].iloc[0] == 400.0

    def test_polars_merge_preserves_ohlcv_columns(self, base_time):
        """Merge should preserve all OHLCV columns.

        Polars equivalent: Schema consistency via pl.concat with how="diagonal".
        Migration risk: LOW - Column preservation is automatic.

        Note: open_time becomes the index after standardize_columns().
        """
        df = make_ohlcv_df(base_time, hours=6, source="CACHE")

        result = merge_dataframes([df])

        # open_time is set as index by standardize_columns()
        assert result.index.name == "open_time"
        # OHLCV columns plus _data_source should be preserved
        expected_columns = {"open", "high", "low", "close", "volume", "_data_source"}
        assert set(result.columns) == expected_columns

    def test_polars_merge_preserves_data_source_column(self, base_time):
        """_data_source column should be preserved through merge.

        Polars equivalent: .with_columns(pl.lit(source).alias("_data_source"))
        Migration risk: LOW - Column addition is straightforward.
        """
        df_cache = make_ohlcv_df(base_time, hours=4, source="CACHE")
        df_rest = make_ohlcv_df(base_time, hours=4, source="REST", offset_hours=4)

        result = merge_dataframes([df_cache, df_rest])

        assert "_data_source" in result.columns
        # Should have both sources
        source_counts = result["_data_source"].value_counts()
        assert source_counts["CACHE"] == 4
        assert source_counts["REST"] == 4

    def test_polars_merge_sorts_by_open_time_ascending(self, base_time):
        """Result should be sorted by open_time ascending.

        Polars equivalent: .sort("open_time")
        Migration risk: MEDIUM - Ensure consistent sort order.

        Note: open_time becomes the index after standardize_columns().
        """
        # Create DataFrames in reverse order
        df_late = make_ohlcv_df(base_time, hours=6, source="REST", offset_hours=6)
        df_early = make_ohlcv_df(base_time, hours=6, source="CACHE")

        result = merge_dataframes([df_late, df_early])

        # open_time is set as index by standardize_columns()
        assert result.index.name == "open_time"
        assert result.index.is_monotonic_increasing, "open_time should be sorted ascending"

    def test_polars_merge_removes_duplicates_keeps_last(self, base_time):
        """Duplicate timestamps should keep last occurrence after priority sort.

        Pandas: df.drop_duplicates(subset=["open_time"], keep="last")
        Polars: lf.unique(subset=["open_time"], keep="last")

        CRITICAL: Polars unique(keep="last") semantics differ from pandas!
        In pandas, after sort by [open_time, priority], keep="last" keeps highest priority.
        In Polars, keep="last" keeps the last in input order unless explicitly sorted.

        Migration risk: HIGH - Semantic difference must be handled.
        """
        # Create overlapping data with different priorities
        df_vision = make_ohlcv_df(base_time, hours=6, source="VISION", open_base=100.0)
        df_cache = make_ohlcv_df(base_time, hours=6, source="CACHE", open_base=200.0)
        df_rest = make_ohlcv_df(base_time, hours=6, source="REST", open_base=300.0)

        result = merge_dataframes([df_vision, df_cache, df_rest])

        # After priority sort, "last" should be highest priority
        assert len(result) == 6
        # REST wins for all timestamps
        assert (result["_data_source"] == "REST").all()
        # Values from REST
        assert result["open"].iloc[0] == 300.0


# =============================================================================
# Additional Edge Cases for Migration Safety
# =============================================================================


class TestPolarsMigrationEdgeCases:
    """Additional edge cases identified during 9-agent audit."""

    def test_merge_with_no_data_source_column(self, base_time):
        """DataFrames without _data_source column should be handled.

        Note: For single DataFrame, merge_dataframes() just calls standardize_columns()
        which does NOT add _data_source. Only the multi-df merge path adds "UNKNOWN".

        For multiple DataFrames, the merge loop adds _data_source="UNKNOWN" if missing.
        Polars equivalent: .with_columns(pl.lit("UNKNOWN").alias("_data_source"))
        """
        # Create two DataFrames without _data_source to trigger multi-df merge path
        df1 = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(3)],
                "open": [100.0, 110.0, 120.0],
                "high": [110.0, 120.0, 130.0],
                "low": [90.0, 100.0, 110.0],
                "close": [105.0, 115.0, 125.0],
                "volume": [1000.0, 1100.0, 1200.0],
            }
        )
        df2 = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(3, 6)],
                "open": [130.0, 140.0, 150.0],
                "high": [140.0, 150.0, 160.0],
                "low": [120.0, 130.0, 140.0],
                "close": [135.0, 145.0, 155.0],
                "volume": [1300.0, 1400.0, 1500.0],
            }
        )

        result = merge_dataframes([df1, df2])

        assert "_data_source" in result.columns
        # Both should get "UNKNOWN" since neither had _data_source
        assert (result["_data_source"] == "UNKNOWN").all()

    def test_merge_with_index_based_open_time(self, base_time):
        """DataFrame with open_time as index should be handled.

        Polars has no index concept - explicit column conversion needed.
        Migration risk: MEDIUM - Index handling must be explicit.
        """
        timestamps = [base_time + timedelta(hours=i) for i in range(6)]
        df = pd.DataFrame(
            {
                "open": [100.0] * 6,
                "high": [110.0] * 6,
                "low": [90.0] * 6,
                "close": [105.0] * 6,
                "volume": [1000.0] * 6,
                "_data_source": ["CACHE"] * 6,
            },
            index=pd.DatetimeIndex(timestamps, name="open_time", tz="UTC"),
        )

        result = merge_dataframes([df])

        # open_time should be accessible (either as column or index)
        assert len(result) == 6
        assert "_data_source" in result.columns
