#!/usr/bin/env python3
"""Unit tests for PolarsDataPipeline class.

Tests validate the internal Polars pipeline implementation for FCP merge operations.
These tests focus on:
1. Pipeline initialization and state management
2. Source addition with FCP priority tagging
3. Priority-based merge resolution
4. Schema standardization for cache files
5. Streaming collection behavior

Task: #101 - Add unit tests for PolarsDataPipeline to existing test structure
ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl
import pytest

from ckvd.utils.internal.polars_pipeline import (
    SOURCE_PRIORITY,
    PolarsDataPipeline,
)


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def base_time():
    """Fixed base time for reproducible tests."""
    return datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def sample_polars_df(base_time):
    """Create a sample Polars DataFrame for testing."""
    timestamps = [base_time + timedelta(hours=i) for i in range(6)]
    return pl.DataFrame(
        {
            "open_time": timestamps,
            "open": [42000.0 + i * 10 for i in range(6)],
            "high": [42100.0 + i * 10 for i in range(6)],
            "low": [41900.0 + i * 10 for i in range(6)],
            "close": [42050.0 + i * 10 for i in range(6)],
            "volume": [1000.0 + i for i in range(6)],
        }
    ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))


@pytest.fixture
def sample_pandas_df(base_time):
    """Create a sample pandas DataFrame for testing.

    Note: The DataFrame has open_time as the index (standard CKVD format),
    not as both index and column.
    """
    timestamps = [base_time + timedelta(hours=i) for i in range(6)]
    return pd.DataFrame(
        {
            "open": [42000.0 + i * 10 for i in range(6)],
            "high": [42100.0 + i * 10 for i in range(6)],
            "low": [41900.0 + i * 10 for i in range(6)],
            "close": [42050.0 + i * 10 for i in range(6)],
            "volume": [1000.0 + i for i in range(6)],
        },
        index=pd.DatetimeIndex(timestamps, name="open_time", tz="UTC"),
    )


# =============================================================================
# Test Class: Pipeline Initialization
# =============================================================================


class TestPipelineInitialization:
    """Tests for pipeline initialization and state."""

    def test_init_creates_empty_pipeline(self):
        """New pipeline should have no data sources."""
        pipeline = PolarsDataPipeline()

        assert pipeline.is_empty()
        assert len(pipeline._lazy_frames) == 0

    def test_is_empty_returns_true_for_new_pipeline(self):
        """is_empty() should return True for new pipeline."""
        pipeline = PolarsDataPipeline()

        assert pipeline.is_empty() is True

    def test_is_empty_returns_false_after_add_source(self, sample_polars_df):
        """is_empty() should return False after adding data."""
        pipeline = PolarsDataPipeline()
        pipeline.add_source(sample_polars_df, "CACHE")

        assert pipeline.is_empty() is False


# =============================================================================
# Test Class: Adding Sources
# =============================================================================


class TestAddSource:
    """Tests for add_source() method."""

    def test_add_source_accepts_polars_dataframe(self, sample_polars_df):
        """add_source() should accept Polars DataFrame."""
        pipeline = PolarsDataPipeline()

        result = pipeline.add_source(sample_polars_df, "CACHE")

        assert result is pipeline  # Returns self for chaining
        assert len(pipeline._lazy_frames) == 1

    def test_add_source_accepts_polars_lazyframe(self, sample_polars_df):
        """add_source() should accept Polars LazyFrame."""
        pipeline = PolarsDataPipeline()
        lf = sample_polars_df.lazy()

        result = pipeline.add_source(lf, "VISION")

        assert result is pipeline
        assert len(pipeline._lazy_frames) == 1

    def test_add_source_adds_data_source_column(self, sample_polars_df):
        """add_source() should add _data_source column."""
        pipeline = PolarsDataPipeline()
        pipeline.add_source(sample_polars_df, "CACHE")

        result = pipeline.collect_polars()

        assert "_data_source" in result.columns
        assert result["_data_source"][0] == "CACHE"

    def test_add_source_preserves_existing_data_source(self, sample_polars_df):
        """add_source() should not overwrite existing _data_source."""
        df_with_source = sample_polars_df.with_columns(pl.lit("EXISTING").alias("_data_source"))
        pipeline = PolarsDataPipeline()

        pipeline.add_source(df_with_source, "NEW")
        result = pipeline.collect_polars()

        # Should keep existing value, not overwrite with "NEW"
        assert result["_data_source"][0] == "EXISTING"

    def test_add_source_chaining(self, sample_polars_df):
        """add_source() should support method chaining."""
        pipeline = PolarsDataPipeline()

        result = pipeline.add_source(sample_polars_df, "CACHE").add_source(sample_polars_df, "VISION").add_source(sample_polars_df, "REST")

        assert result is pipeline
        assert len(pipeline._lazy_frames) == 3


class TestAddPandas:
    """Tests for add_pandas() method."""

    def test_add_pandas_accepts_pandas_dataframe(self, sample_pandas_df):
        """add_pandas() should accept pandas DataFrame."""
        pipeline = PolarsDataPipeline()

        result = pipeline.add_pandas(sample_pandas_df, "CACHE")

        assert result is pipeline
        assert len(pipeline._lazy_frames) == 1

    def test_add_pandas_skips_empty_dataframe(self):
        """add_pandas() should skip empty DataFrames."""
        pipeline = PolarsDataPipeline()
        empty_df = pd.DataFrame()

        result = pipeline.add_pandas(empty_df, "CACHE")

        assert result is pipeline
        assert pipeline.is_empty()

    def test_add_pandas_resets_index_named_open_time(self, sample_pandas_df):
        """add_pandas() should handle open_time as index."""
        pipeline = PolarsDataPipeline()

        pipeline.add_pandas(sample_pandas_df, "CACHE")
        result = pipeline.collect_polars()

        assert "open_time" in result.columns
        assert len(result) == 6


# =============================================================================
# Test Class: Priority Resolution
# =============================================================================


class TestPriorityResolution:
    """Tests for FCP priority resolution during merge."""

    def test_source_priority_constants_correct(self):
        """SOURCE_PRIORITY should match FCP specification."""
        assert SOURCE_PRIORITY["UNKNOWN"] == 0
        assert SOURCE_PRIORITY["VISION"] == 1
        assert SOURCE_PRIORITY["CACHE"] == 2
        assert SOURCE_PRIORITY["REST"] == 3

    def test_rest_priority_wins_over_cache(self, base_time):
        """REST should take priority over CACHE for same timestamp."""
        pipeline = PolarsDataPipeline()

        # CACHE data
        cache_df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
                "volume": [1000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        # REST data (higher priority) - same timestamp, different values
        rest_df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [200.0],  # Different value
                "high": [210.0],
                "low": [190.0],
                "close": [205.0],
                "volume": [2000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        pipeline.add_source(cache_df, "CACHE").add_source(rest_df, "REST")
        result = pipeline.collect_polars()

        # REST value should win
        assert len(result) == 1
        assert result["open"][0] == 200.0
        assert result["_data_source"][0] == "REST"

    def test_cache_priority_wins_over_vision(self, base_time):
        """CACHE should take priority over VISION for same timestamp."""
        pipeline = PolarsDataPipeline()

        # VISION data
        vision_df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
                "volume": [1000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        # CACHE data (higher priority)
        cache_df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [200.0],
                "high": [210.0],
                "low": [190.0],
                "close": [205.0],
                "volume": [2000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        pipeline.add_source(vision_df, "VISION").add_source(cache_df, "CACHE")
        result = pipeline.collect_polars()

        # CACHE value should win
        assert len(result) == 1
        assert result["open"][0] == 200.0
        assert result["_data_source"][0] == "CACHE"

    def test_merge_preserves_unique_timestamps(self, base_time):
        """Merge should preserve records with unique timestamps."""
        pipeline = PolarsDataPipeline()

        # Different timestamps from different sources
        cache_df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
                "volume": [1000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        rest_df = pl.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=1)],
                "open": [200.0],
                "high": [210.0],
                "low": [190.0],
                "close": [205.0],
                "volume": [2000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        pipeline.add_source(cache_df, "CACHE").add_source(rest_df, "REST")
        result = pipeline.collect_polars()

        # Both records should be preserved
        assert len(result) == 2

    def test_output_sorted_by_open_time(self, base_time):
        """Output should be sorted by open_time ascending."""
        pipeline = PolarsDataPipeline()

        # Add out-of-order data
        late_df = pl.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=2)],
                "open": [300.0],
                "high": [310.0],
                "low": [290.0],
                "close": [305.0],
                "volume": [3000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        early_df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
                "volume": [1000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        pipeline.add_source(late_df, "CACHE").add_source(early_df, "REST")
        result = pipeline.collect_polars()

        # Should be sorted by open_time
        assert result["open_time"][0] == base_time
        assert result["open_time"][1] == base_time + timedelta(hours=2)


# =============================================================================
# Test Class: Schema Standardization
# =============================================================================


class TestSchemaStandardization:
    """Tests for _standardize_schema() behavior."""

    def test_standardize_casts_volume_to_float64(self, base_time):
        """Schema standardization should cast volume to Float64."""
        pipeline = PolarsDataPipeline()

        # Create DataFrame with Int64 volume
        df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
                "volume": [1000],  # Int64
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        pipeline.add_source(df, "CACHE")
        result = pipeline.collect_polars()

        assert result["volume"].dtype == pl.Float64

    def test_standardize_handles_datetime_nanoseconds(self, base_time):
        """Schema standardization should handle Datetime with nanoseconds."""
        pipeline = PolarsDataPipeline()

        # Create DataFrame with nanosecond timestamps
        df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
                "volume": [1000.0],
            }
        ).with_columns(pl.col("open_time").cast(pl.Datetime("ns", "UTC")))

        pipeline.add_source(df, "CACHE")
        result = pipeline.collect_polars()

        # Should be standardized to microseconds UTC
        assert result["open_time"].dtype == pl.Datetime("us", "UTC")

    def test_standardize_handles_missing_columns(self, base_time):
        """Schema standardization should handle missing optional columns."""
        pipeline = PolarsDataPipeline()

        # Create minimal DataFrame without optional columns
        df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
                "volume": [1000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        # Should not raise even without quote_volume, count, etc.
        pipeline.add_source(df, "CACHE")
        result = pipeline.collect_polars()

        assert len(result) == 1


# =============================================================================
# Test Class: Collection Methods
# =============================================================================


class TestCollectionMethods:
    """Tests for collect_polars() and collect_pandas() methods."""

    def test_collect_polars_returns_polars_dataframe(self, sample_polars_df):
        """collect_polars() should return Polars DataFrame."""
        pipeline = PolarsDataPipeline()
        pipeline.add_source(sample_polars_df, "CACHE")

        result = pipeline.collect_polars()

        assert isinstance(result, pl.DataFrame)

    def test_collect_pandas_returns_pandas_dataframe(self, sample_polars_df):
        """collect_pandas() should return pandas DataFrame."""
        pipeline = PolarsDataPipeline()
        pipeline.add_source(sample_polars_df, "CACHE")

        result = pipeline.collect_pandas()

        assert isinstance(result, pd.DataFrame)

    def test_collect_pandas_sets_open_time_as_index(self, sample_polars_df):
        """collect_pandas() should set open_time as index."""
        pipeline = PolarsDataPipeline()
        pipeline.add_source(sample_polars_df, "CACHE")

        result = pipeline.collect_pandas()

        assert result.index.name == "open_time"

    def test_collect_pandas_empty_returns_empty_dataframe(self):
        """collect_pandas() on empty pipeline should return empty DataFrame."""
        pipeline = PolarsDataPipeline()

        result = pipeline.collect_pandas()

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_collect_polars_empty_returns_empty_dataframe(self):
        """collect_polars() on empty pipeline should return empty DataFrame."""
        pipeline = PolarsDataPipeline()

        result = pipeline.collect_polars()

        assert isinstance(result, pl.DataFrame)
        assert result.is_empty()

    def test_collect_polars_with_streaming(self, sample_polars_df):
        """collect_polars(use_streaming=True) should use streaming engine."""
        pipeline = PolarsDataPipeline()
        pipeline.add_source(sample_polars_df, "CACHE")

        # Should not raise - streaming mode
        result = pipeline.collect_polars(use_streaming=True)

        assert len(result) == 6

    def test_collect_polars_without_streaming(self, sample_polars_df):
        """collect_polars(use_streaming=False) should use in-memory engine."""
        pipeline = PolarsDataPipeline()
        pipeline.add_source(sample_polars_df, "CACHE")

        # Should not raise - in-memory mode
        result = pipeline.collect_polars(use_streaming=False)

        assert len(result) == 6


# =============================================================================
# Test Class: Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_single_source_no_merge_needed(self, sample_polars_df):
        """Single source should return data without merge overhead."""
        pipeline = PolarsDataPipeline()
        pipeline.add_source(sample_polars_df, "CACHE")

        result = pipeline.collect_polars()

        assert len(result) == 6
        assert "_data_source" in result.columns

    def test_multiple_sources_same_data(self, sample_polars_df):
        """Multiple sources with identical data should deduplicate."""
        pipeline = PolarsDataPipeline()

        # Add same data from different sources
        pipeline.add_source(sample_polars_df, "CACHE")
        pipeline.add_source(sample_polars_df, "VISION")
        pipeline.add_source(sample_polars_df, "REST")

        result = pipeline.collect_polars()

        # Should deduplicate - REST wins for all timestamps
        assert len(result) == 6
        assert all(result["_data_source"] == "REST")

    def test_unknown_source_has_lowest_priority(self, base_time):
        """UNKNOWN source should have lowest priority."""
        pipeline = PolarsDataPipeline()

        unknown_df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [100.0],
                "high": [110.0],
                "low": [90.0],
                "close": [105.0],
                "volume": [1000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        vision_df = pl.DataFrame(
            {
                "open_time": [base_time],
                "open": [200.0],
                "high": [210.0],
                "low": [190.0],
                "close": [205.0],
                "volume": [2000.0],
            }
        ).with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        pipeline.add_source(unknown_df, "UNKNOWN").add_source(vision_df, "VISION")
        result = pipeline.collect_polars()

        # VISION should win over UNKNOWN
        assert result["open"][0] == 200.0
        assert result["_data_source"][0] == "VISION"
