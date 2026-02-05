#!/usr/bin/env python
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Polars-native FCP data pipeline with streaming support.

This module provides a Polars-based implementation of the Failover Control Protocol
data merging logic. It serves as an internal optimization for Phase 2 of the memory
efficiency refactoring.

The pipeline:
1. Accepts LazyFrames from cache, Vision, and REST sources
2. Merges with priority resolution (REST > CACHE > VISION > UNKNOWN)
3. Uses streaming engine for memory-efficient collection
4. Returns either pandas DataFrame (backward compatible) or Polars DataFrame

Usage:
    pipeline = PolarsDataPipeline()
    pipeline.add_source(cache_lf, "CACHE")
    pipeline.add_source(vision_lf, "VISION")
    pipeline.add_source(rest_lf, "REST")
    df = pipeline.collect_pandas()  # Or collect_polars() for zero-copy output
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from data_source_manager.utils.loguru_setup import logger

if TYPE_CHECKING:
    import pandas as pd


# Source priority for FCP conflict resolution
# Higher number = higher priority (kept when duplicates exist)
# CRITICAL: This order must match dsm_time_range_utils.py merge_dataframes()
SOURCE_PRIORITY = {
    "UNKNOWN": 0,
    "VISION": 1,
    "CACHE": 2,
    "REST": 3,
}


class PolarsDataPipeline:
    """Polars-native FCP data pipeline with streaming support.

    This class implements the same merge logic as merge_dataframes() in
    dsm_time_range_utils.py but using Polars LazyFrame operations for
    better memory efficiency and predicate pushdown.

    The pipeline is designed to be used internally by DataSourceManager
    when the USE_POLARS_PIPELINE feature flag is enabled.
    """

    def __init__(self) -> None:
        """Initialize empty pipeline."""
        self._lazy_frames: list[pl.LazyFrame] = []

    def add_source(
        self,
        lf: pl.LazyFrame | pl.DataFrame,
        source: str,
    ) -> PolarsDataPipeline:
        """Add data source with FCP priority tag.

        Args:
            lf: LazyFrame or DataFrame containing OHLCV data.
                Must have 'open_time' column with UTC timezone.
            source: Source identifier ("CACHE", "VISION", "REST", or "UNKNOWN")

        Returns:
            Self for method chaining.
        """
        # Convert DataFrame to LazyFrame if needed
        if isinstance(lf, pl.DataFrame):
            lf = lf.lazy()

        # Add _data_source column if not present
        if "_data_source" not in lf.collect_schema():
            lf = lf.with_columns(pl.lit(source).alias("_data_source"))

        self._lazy_frames.append(lf)
        logger.debug(f"Added {source} source to pipeline")
        return self

    def add_pandas(
        self,
        df: pd.DataFrame,
        source: str,
    ) -> PolarsDataPipeline:
        """Add pandas DataFrame source with FCP priority tag.

        Convenience method for adding pandas DataFrames. Converts to
        Polars LazyFrame internally.

        Args:
            df: Pandas DataFrame containing OHLCV data.
            source: Source identifier ("CACHE", "VISION", "REST", or "UNKNOWN")

        Returns:
            Self for method chaining.
        """
        if df.empty:
            logger.debug(f"Skipping empty pandas DataFrame from {source}")
            return self

        # Handle index - if open_time is index, reset it
        if df.index.name == "open_time":
            df = df.reset_index()

        pl_df = pl.from_pandas(df)
        return self.add_source(pl_df.lazy(), source)

    def is_empty(self) -> bool:
        """Check if pipeline has no data sources."""
        return len(self._lazy_frames) == 0

    def _standardize_schema(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Standardize LazyFrame schema to canonical OHLCV types.

        Cache files may have inconsistent schemas due to different write paths:
        - open_time: Datetime(Nanoseconds) vs Datetime(Milliseconds)
        - volume: Int64 vs Float64
        - Extra columns in some files

        This method ensures all LazyFrames have consistent types before concat.

        Args:
            lf: LazyFrame to standardize

        Returns:
            LazyFrame with standardized schema
        """
        schema = lf.collect_schema()

        # Cast timestamp columns to consistent resolution (Microseconds UTC)
        time_cols = ["open_time", "close_time"]
        casts = []

        for col in time_cols:
            if col in schema:
                # Cast to Datetime with microseconds and UTC timezone
                casts.append(
                    pl.col(col).cast(pl.Datetime("us", "UTC")).alias(col)
                )

        # Cast numeric columns to Float64 for consistency
        numeric_cols = ["open", "high", "low", "close", "volume",
                       "quote_volume", "quote_asset_volume",
                       "taker_buy_volume", "taker_buy_quote_volume"]

        for col in numeric_cols:
            if col in schema:
                casts.append(pl.col(col).cast(pl.Float64).alias(col))

        # Cast count to Int64
        if "count" in schema:
            casts.append(pl.col("count").cast(pl.Int64).alias("count"))

        if casts:
            lf = lf.with_columns(casts)

        return lf

    def _merge_with_priority(self) -> pl.LazyFrame:
        """Merge all sources with REST > CACHE > VISION priority resolution.

        This implements the same logic as merge_dataframes() but using Polars:
        1. Standardize schemas across all LazyFrames
        2. Concatenate all LazyFrames
        3. Add priority column based on _data_source
        4. Sort by [open_time, _priority] ascending
        5. Keep last occurrence (highest priority) using unique(keep="last")
        6. Drop priority column

        Returns:
            Merged LazyFrame with duplicates resolved by priority.
        """
        if not self._lazy_frames:
            logger.warning("No data sources in pipeline")
            return pl.LazyFrame()

        if len(self._lazy_frames) == 1:
            logger.debug("Single source in pipeline, returning directly")
            return self._standardize_schema(self._lazy_frames[0])

        logger.debug(f"Merging {len(self._lazy_frames)} sources with priority resolution")

        # Standardize schemas before concat to avoid type mismatches
        standardized = [self._standardize_schema(lf) for lf in self._lazy_frames]

        # Concatenate all LazyFrames (diagonal handles missing columns)
        combined = pl.concat(standardized, how="diagonal")

        # Add priority column and resolve duplicates
        return (
            combined.with_columns(
                pl.col("_data_source")
                .replace(SOURCE_PRIORITY, default=0)
                .alias("_priority")
            )
            .sort(["open_time", "_priority"])
            .unique(subset=["open_time"], keep="last")
            .drop("_priority")
            .sort("open_time")
        )


    def collect_polars(self, use_streaming: bool = True) -> pl.DataFrame:
        """Collect merged data as Polars DataFrame.

        Uses the new streaming engine (Polars 1.31+) for better memory
        efficiency when use_streaming=True.

        Args:
            use_streaming: Whether to use streaming engine for collection.
                          Defaults to True for memory efficiency.

        Returns:
            Merged Polars DataFrame with duplicates resolved by priority.
        """
        lf = self._merge_with_priority()

        if use_streaming:
            logger.debug("Collecting with streaming engine")
            return lf.collect(engine="streaming")
        logger.debug("Collecting with in-memory engine")
        return lf.collect()

    def collect_pandas(self, use_streaming: bool = True) -> pd.DataFrame:
        """Collect merged data as pandas DataFrame.

        This is the backward-compatible output format for existing consumers.
        The data flows through Polars for efficient merging, then converts
        to pandas at the API boundary.

        Args:
            use_streaming: Whether to use streaming engine for collection.
                          Defaults to True for memory efficiency.

        Returns:
            Merged pandas DataFrame with duplicates resolved by priority.
        """
        if self.is_empty():
            logger.warning("Pipeline is empty, returning empty pandas DataFrame")
            from data_source_manager.utils.config import create_empty_dataframe

            return create_empty_dataframe()

        pl_df = self.collect_polars(use_streaming=use_streaming)

        if pl_df.is_empty():
            from data_source_manager.utils.config import create_empty_dataframe

            return create_empty_dataframe()

        # Convert to pandas
        pd_df = pl_df.to_pandas()

        # Set open_time as index (standard DSM format)
        if "open_time" in pd_df.columns:
            pd_df = pd_df.set_index("open_time")

        # Log merge statistics
        if "_data_source" in pd_df.columns and not pd_df.empty:
            source_counts = pd_df["_data_source"].value_counts()
            for source, count in source_counts.items():
                percentage = (count / len(pd_df)) * 100
                logger.debug(
                    f"Polars pipeline result: {count} records ({percentage:.1f}%) from {source}"
                )

        return pd_df

    def sink_to_ipc(self, path: str) -> None:
        """Sink merged data directly to IPC file without memory materialization.

        For very large results, this avoids holding the entire result in memory.
        Uses Polars sink_ipc() for streaming writes.

        Args:
            path: Path to write the IPC (Arrow) file.
        """
        logger.debug(f"Sinking merged data to {path}")
        lf = self._merge_with_priority()
        lf.sink_ipc(path)
        logger.debug(f"Successfully wrote merged data to {path}")
