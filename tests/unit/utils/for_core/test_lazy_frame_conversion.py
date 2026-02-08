#!/usr/bin/env python3
"""Unit tests for Polars LazyFrame conversion patterns.

Tests validate that LazyFrame operations correctly produce expected outputs
when transitioning between Polars lazy/eager modes and Pandas DataFrames.

These tests serve as a regression safety net for Phase 2 Polars pipeline migration.

Copy from: tests/unit/utils/for_core/test_dsm_fcp_utils.py
Task: #74 - Create test_lazy_frame_conversion.py (15 tests)

ADR: docs/adr/2025-01-30-failover-control-protocol.md
Plan: /Users/terryli/.claude/plans/gleaming-frolicking-engelbart.md
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl
import pytest


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


def make_polars_df(
    base_time: datetime,
    hours: int,
    source: str | None = None,
    offset_hours: int = 0,
    open_base: float = 100.0,
) -> pl.DataFrame:
    """Create a standard OHLCV Polars DataFrame for testing.

    Args:
        base_time: Starting timestamp.
        hours: Number of hourly candles to create.
        source: Optional _data_source value.
        offset_hours: Offset from base_time in hours.
        open_base: Base value for open price.

    Returns:
        pl.DataFrame: OHLCV data with optional _data_source column.
    """
    start = base_time + timedelta(hours=offset_hours)
    timestamps = [start + timedelta(hours=i) for i in range(hours)]

    data = {
        "open_time": timestamps,
        "open": [open_base + i * 10 for i in range(hours)],
        "high": [open_base + 100 + i * 10 for i in range(hours)],
        "low": [open_base - 50 + i * 10 for i in range(hours)],
        "close": [open_base + 50 + i * 10 for i in range(hours)],
        "volume": [1000.0 + i * 100 for i in range(hours)],
    }

    if source is not None:
        data["_data_source"] = [source] * hours

    return pl.DataFrame(data)


def make_pandas_df(
    base_time: datetime,
    hours: int,
    source: str | None = None,
    offset_hours: int = 0,
    open_base: float = 100.0,
) -> pd.DataFrame:
    """Create a standard OHLCV Pandas DataFrame for testing.

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
        "low": [open_base - 50 + i * 10 for i in range(hours)],
        "close": [open_base + 50 + i * 10 for i in range(hours)],
        "volume": [1000.0 + i * 100 for i in range(hours)],
    }

    if source is not None:
        data["_data_source"] = [source] * hours

    return pd.DataFrame(data)


# =============================================================================
# Test 1-5: LazyFrame Creation and Schema
# =============================================================================


class TestLazyFrameCreation:
    """Tests for LazyFrame creation and schema validation."""

    def test_lazyframe_from_dataframe(self, base_time):
        """LazyFrame created from DataFrame should have same schema.

        Pattern: pl.DataFrame(...).lazy()
        """
        df = make_polars_df(base_time, hours=6, source="CACHE")
        lf = df.lazy()

        assert isinstance(lf, pl.LazyFrame)
        # Schema should match
        assert lf.collect_schema() == df.schema

    def test_lazyframe_schema_preserved_after_collect(self, base_time):
        """Schema should be preserved after collect().

        Pattern: lf.collect() returns DataFrame with same schema
        """
        df = make_polars_df(base_time, hours=6, source="CACHE")
        lf = df.lazy()

        collected = lf.collect()

        assert collected.schema == df.schema
        assert len(collected) == len(df)

    def test_lazyframe_has_correct_column_types(self, base_time):
        """LazyFrame should preserve column types.

        OHLCV columns should be Float64, open_time should be Datetime.
        """
        df = make_polars_df(base_time, hours=6, source="CACHE")
        lf = df.lazy()

        schema = lf.collect_schema()

        # Check OHLCV types
        assert schema["open"] == pl.Float64
        assert schema["high"] == pl.Float64
        assert schema["low"] == pl.Float64
        assert schema["close"] == pl.Float64
        assert schema["volume"] == pl.Float64
        # open_time should be datetime
        assert schema["open_time"] == pl.Datetime("us", "UTC")

    def test_lazyframe_preserves_data_source_column(self, base_time):
        """_data_source column should be preserved in LazyFrame."""
        df = make_polars_df(base_time, hours=6, source="VISION")
        lf = df.lazy()

        schema = lf.collect_schema()
        assert "_data_source" in schema
        assert schema["_data_source"] == pl.String

    def test_empty_lazyframe_schema(self):
        """Empty LazyFrame should still have correct schema."""
        df = pl.DataFrame(
            {
                "open_time": pl.Series([], dtype=pl.Datetime("us", "UTC")),
                "open": pl.Series([], dtype=pl.Float64),
                "high": pl.Series([], dtype=pl.Float64),
                "low": pl.Series([], dtype=pl.Float64),
                "close": pl.Series([], dtype=pl.Float64),
                "volume": pl.Series([], dtype=pl.Float64),
            }
        )
        lf = df.lazy()

        assert isinstance(lf, pl.LazyFrame)
        collected = lf.collect()
        assert len(collected) == 0
        # Schema still correct
        assert collected.schema["open"] == pl.Float64


# =============================================================================
# Test 6-10: LazyFrame Operations and Filters
# =============================================================================


class TestLazyFrameOperations:
    """Tests for LazyFrame operations and filters."""

    def test_lazyframe_filter_by_time_range(self, base_time):
        """LazyFrame filter should correctly filter by time range.

        Pattern: lf.filter((pl.col("open_time") >= start) & (pl.col("open_time") <= end))
        """
        df = make_polars_df(base_time, hours=24, source="CACHE")
        lf = df.lazy()

        # Filter to first 12 hours
        start = base_time
        end = base_time + timedelta(hours=11)

        filtered = lf.filter((pl.col("open_time") >= start) & (pl.col("open_time") <= end)).collect()

        assert len(filtered) == 12  # Hours 0-11 inclusive

    def test_lazyframe_sort_by_open_time(self, base_time):
        """LazyFrame sort should produce correct order.

        Pattern: lf.sort("open_time")
        """
        # Create out-of-order data
        df1 = make_polars_df(base_time, hours=3, source="REST", offset_hours=3)
        df2 = make_polars_df(base_time, hours=3, source="CACHE")

        combined = pl.concat([df1, df2]).lazy()
        sorted_lf = combined.sort("open_time")

        result = sorted_lf.collect()

        # Check monotonic
        open_times = result["open_time"].to_list()
        assert open_times == sorted(open_times)

    def test_lazyframe_unique_keeps_last(self, base_time):
        """LazyFrame unique(keep='last') should keep last occurrence.

        CRITICAL: Polars unique semantics for FCP priority resolution.
        """
        # Create overlapping data with different sources
        df1 = make_polars_df(base_time, hours=3, source="VISION", open_base=100.0)
        df2 = make_polars_df(base_time, hours=3, source="REST", open_base=200.0)

        # Sort by priority then unique(keep="last")
        combined = pl.concat([df1, df2]).lazy()
        priority_map = {"VISION": 1, "REST": 3}

        result = (
            combined.with_columns(pl.col("_data_source").replace(priority_map).alias("_priority"))
            .sort(["open_time", "_priority"])
            .unique(subset=["open_time"], keep="last")
            .drop("_priority")
            .collect()
        )

        # REST should win (higher priority, last after sort)
        assert len(result) == 3
        assert (result["_data_source"] == "REST").all()
        assert result["open"][0] == 200.0

    def test_lazyframe_with_columns_adds_source(self, base_time):
        """with_columns should correctly add _data_source.

        Pattern: lf.with_columns(pl.lit("CACHE").alias("_data_source"))
        """
        df = pl.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(3)],
                "open": [100.0, 110.0, 120.0],
                "high": [110.0, 120.0, 130.0],
                "low": [90.0, 100.0, 110.0],
                "close": [105.0, 115.0, 125.0],
                "volume": [1000.0, 1100.0, 1200.0],
            }
        )
        lf = df.lazy()

        result = lf.with_columns(pl.lit("CACHE").alias("_data_source")).collect()

        assert "_data_source" in result.columns
        assert (result["_data_source"] == "CACHE").all()

    def test_lazyframe_concat_multiple_sources(self, base_time):
        """pl.concat should correctly combine LazyFrames.

        Pattern: pl.concat([lf1, lf2, lf3])
        """
        lf1 = make_polars_df(base_time, hours=3, source="CACHE").lazy()
        lf2 = make_polars_df(base_time, hours=3, source="VISION", offset_hours=3).lazy()
        lf3 = make_polars_df(base_time, hours=3, source="REST", offset_hours=6).lazy()

        combined = pl.concat([lf1, lf2, lf3]).collect()

        assert len(combined) == 9
        source_counts = combined["_data_source"].value_counts()
        assert len(source_counts) == 3


# =============================================================================
# Test 11-15: Conversion Between Polars and Pandas
# =============================================================================


class TestPolarsToAndFromPandas:
    """Tests for conversion between Polars and Pandas DataFrames."""

    def test_polars_to_pandas_preserves_values(self, base_time):
        """to_pandas() should preserve all values.

        Pattern: df_polars.to_pandas()
        """
        pl_df = make_polars_df(base_time, hours=6, source="CACHE")

        pd_df = pl_df.to_pandas()

        assert isinstance(pd_df, pd.DataFrame)
        assert len(pd_df) == 6
        assert pd_df["open"].iloc[0] == 100.0
        assert pd_df["_data_source"].iloc[0] == "CACHE"

    def test_polars_to_pandas_preserves_types(self, base_time):
        """to_pandas() should preserve numeric types as float64."""
        pl_df = make_polars_df(base_time, hours=6, source="CACHE")

        pd_df = pl_df.to_pandas()

        assert pd_df["open"].dtype == "float64"
        assert pd_df["high"].dtype == "float64"
        assert pd_df["low"].dtype == "float64"
        assert pd_df["close"].dtype == "float64"
        assert pd_df["volume"].dtype == "float64"

    def test_pandas_to_polars_preserves_values(self, base_time):
        """pl.from_pandas() should preserve all values.

        Pattern: pl.from_pandas(df_pandas)
        """
        pd_df = make_pandas_df(base_time, hours=6, source="REST")

        pl_df = pl.from_pandas(pd_df)

        assert isinstance(pl_df, pl.DataFrame)
        assert len(pl_df) == 6
        assert pl_df["open"][0] == 100.0
        assert pl_df["_data_source"][0] == "REST"

    def test_lazyframe_collect_then_to_pandas(self, base_time):
        """LazyFrame collect then to_pandas should work correctly.

        Pattern: lf.collect().to_pandas()
        This is the current CKVD pattern for returning pandas from Polars pipeline.
        """
        pl_df = make_polars_df(base_time, hours=6, source="CACHE")
        lf = pl_df.lazy()

        # Apply some operations
        filtered_lf = lf.filter(pl.col("open") >= 110.0)
        result = filtered_lf.collect().to_pandas()

        assert isinstance(result, pd.DataFrame)
        # Should have rows where open >= 110 (indices 1-5)
        assert len(result) == 5
        assert result["open"].min() >= 110.0

    def test_pandas_roundtrip_preserves_data_source(self, base_time):
        """pandas → polars → pandas roundtrip should preserve _data_source.

        This validates the zero-copy path doesn't lose source attribution.
        """
        pd_df = make_pandas_df(base_time, hours=6, source="VISION")

        # Convert to Polars and back
        pl_df = pl.from_pandas(pd_df)
        pd_df_roundtrip = pl_df.to_pandas()

        assert "_data_source" in pd_df_roundtrip.columns
        assert (pd_df_roundtrip["_data_source"] == "VISION").all()
        pd.testing.assert_series_equal(pd_df["open"], pd_df_roundtrip["open"], check_names=False)
