#!/usr/bin/env python3
"""Unit tests for empty DataFrame and null handling edge cases.

Tests validate proper handling of empty DataFrames and null values
during FCP operations and Polars migration.

These tests serve as a regression safety net for Phase 2 Polars pipeline migration.

Copy from: tests/unit/utils/for_core/test_ckvd_fcp_utils.py
Task: #75 - Create test_empty_dataframe_edge_cases.py (10 tests)

ADR: docs/adr/2025-01-30-failover-control-protocol.md
Plan: /Users/terryli/.claude/plans/gleaming-frolicking-engelbart.md
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl
import pytest

from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def base_time():
    """Fixed base time for reproducible tests."""
    return datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)


def make_empty_pandas_df() -> pd.DataFrame:
    """Create an empty pandas DataFrame with standard OHLCV schema."""
    return pd.DataFrame(
        {
            "open_time": pd.Series([], dtype="datetime64[ns, UTC]"),
            "open": pd.Series([], dtype="float64"),
            "high": pd.Series([], dtype="float64"),
            "low": pd.Series([], dtype="float64"),
            "close": pd.Series([], dtype="float64"),
            "volume": pd.Series([], dtype="float64"),
            "_data_source": pd.Series([], dtype="object"),
        }
    )


def make_empty_polars_df() -> pl.DataFrame:
    """Create an empty Polars DataFrame with standard OHLCV schema."""
    return pl.DataFrame(
        {
            "open_time": pl.Series([], dtype=pl.Datetime("us", "UTC")),
            "open": pl.Series([], dtype=pl.Float64),
            "high": pl.Series([], dtype=pl.Float64),
            "low": pl.Series([], dtype=pl.Float64),
            "close": pl.Series([], dtype=pl.Float64),
            "volume": pl.Series([], dtype=pl.Float64),
            "_data_source": pl.Series([], dtype=pl.String),
        }
    )


def make_ohlcv_df(
    base_time: datetime,
    hours: int,
    source: str | None = None,
    offset_hours: int = 0,
) -> pd.DataFrame:
    """Create a standard OHLCV DataFrame for testing."""
    start = base_time + timedelta(hours=offset_hours)
    timestamps = [start + timedelta(hours=i) for i in range(hours)]

    data = {
        "open_time": timestamps,
        "open": [100.0 + i * 10 for i in range(hours)],
        "high": [150.0 + i * 10 for i in range(hours)],
        "low": [50.0 + i * 10 for i in range(hours)],
        "close": [120.0 + i * 10 for i in range(hours)],
        "volume": [1000.0 + i * 100 for i in range(hours)],
    }

    if source is not None:
        data["_data_source"] = [source] * hours

    return pd.DataFrame(data)


# =============================================================================
# Test 1-4: Empty DataFrame Handling in merge_dataframes
# =============================================================================


class TestEmptyDataFrameMerge:
    """Tests for empty DataFrame handling in merge_dataframes()."""

    def test_merge_empty_list_returns_empty(self):
        """Merging empty list should return empty DataFrame.

        FCP behavior: No data sources available.
        """
        result = merge_dataframes([])

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_merge_single_empty_df_returns_empty(self):
        """Merging single empty DataFrame should return empty.

        FCP behavior: Single source has no data.
        """
        empty_df = make_empty_pandas_df()
        result = merge_dataframes([empty_df])

        assert len(result) == 0

    def test_merge_all_empty_dfs_returns_empty(self):
        """Merging multiple empty DataFrames should return empty.

        FCP behavior: All sources have no data.
        """
        empty1 = make_empty_pandas_df()
        empty2 = make_empty_pandas_df()
        empty3 = make_empty_pandas_df()

        result = merge_dataframes([empty1, empty2, empty3])

        assert len(result) == 0

    def test_merge_empty_with_non_empty_preserves_data(self, base_time):
        """Merging empty with non-empty should preserve non-empty data.

        FCP behavior: One source has data, others are empty.
        """
        empty_df = make_empty_pandas_df()
        valid_df = make_ohlcv_df(base_time, hours=6, source="REST")

        result = merge_dataframes([empty_df, valid_df])

        assert len(result) == 6
        assert "_data_source" in result.columns


# =============================================================================
# Test 5-7: Null Value Handling
# =============================================================================


class TestNullValueHandling:
    """Tests for null/NaN value handling."""

    def test_merge_df_with_null_values(self, base_time):
        """DataFrames with null values should be handled correctly."""
        df = make_ohlcv_df(base_time, hours=6, source="CACHE")
        # Introduce some nulls
        df.loc[2, "open"] = None
        df.loc[4, "volume"] = None

        result = merge_dataframes([df])

        assert len(result) == 6
        # Nulls should be preserved (or handled appropriately)
        assert pd.isna(result.loc[result.index[2], "open"]) or result.iloc[2]["open"] is None

    def test_polars_null_to_pandas_nan(self):
        """Polars null should convert to pandas NaN correctly."""
        pl_df = pl.DataFrame(
            {
                "open_time": [datetime(2024, 1, 15, tzinfo=timezone.utc)],
                "open": [None],
                "high": [100.0],
                "low": [90.0],
                "close": [95.0],
                "volume": [1000.0],
            }
        )

        pd_df = pl_df.to_pandas()

        assert pd.isna(pd_df["open"].iloc[0])

    def test_pandas_nan_to_polars_null(self):
        """Pandas NaN should convert to Polars null correctly."""
        pd_df = pd.DataFrame(
            {
                "open_time": [datetime(2024, 1, 15, tzinfo=timezone.utc)],
                "open": [float("nan")],
                "high": [100.0],
                "low": [90.0],
                "close": [95.0],
                "volume": [1000.0],
            }
        )

        pl_df = pl.from_pandas(pd_df)

        assert pl_df["open"].null_count() == 1


# =============================================================================
# Test 8-10: Edge Cases for Polars Empty LazyFrame
# =============================================================================


class TestPolarsEmptyLazyFrame:
    """Tests for Polars empty LazyFrame edge cases."""

    def test_empty_lazyframe_filter_returns_empty(self):
        """Filtering empty LazyFrame should return empty DataFrame."""
        empty = make_empty_polars_df()
        lf = empty.lazy()

        filtered = lf.filter(pl.col("open") > 100).collect()

        assert len(filtered) == 0
        # Schema should be preserved
        assert "open_time" in filtered.columns

    def test_empty_lazyframe_concat_with_non_empty(self, base_time):
        """Concatenating empty with non-empty LazyFrame should work."""
        empty = make_empty_polars_df()
        non_empty = pl.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(3)],
                "open": [100.0, 110.0, 120.0],
                "high": [110.0, 120.0, 130.0],
                "low": [90.0, 100.0, 110.0],
                "close": [105.0, 115.0, 125.0],
                "volume": [1000.0, 1100.0, 1200.0],
                "_data_source": ["CACHE", "CACHE", "CACHE"],
            }
        )

        # Concat as LazyFrames
        result = pl.concat([empty.lazy(), non_empty.lazy()]).collect()

        assert len(result) == 3

    def test_empty_lazyframe_to_pandas(self):
        """Empty LazyFrame should convert to empty pandas DataFrame."""
        empty = make_empty_polars_df()
        lf = empty.lazy()

        pd_df = lf.collect().to_pandas()

        assert isinstance(pd_df, pd.DataFrame)
        assert len(pd_df) == 0
        # Schema should be preserved
        assert "open_time" in pd_df.columns
        assert "open" in pd_df.columns
