#!/usr/bin/env python
"""Utility functions for DataSourceManager time range and data segment operations."""

from datetime import datetime, timedelta

import pandas as pd

from data_source_manager.utils.config import REST_IS_STANDARD
from data_source_manager.utils.dataframe_utils import ensure_open_time_as_column, standardize_dataframe
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import Interval
from data_source_manager.utils.time_utils import standardize_timestamp_precision


def merge_adjacent_ranges(ranges: list[tuple[datetime, datetime]], interval: Interval) -> list[tuple[datetime, datetime]]:
    """Merge adjacent or overlapping time ranges to minimize API calls.

    Args:
        ranges: List of (start, end) tuples representing time ranges
        interval: Time interval to determine adjacency threshold

    Returns:
        List of merged (start, end) tuples
    """
    if not ranges:
        return []

    # Sort ranges by start time
    sorted_ranges = sorted(ranges, key=lambda x: x[0])

    # Determine the threshold for what's considered "adjacent"
    # (allow for a small gap, typically 1-2x the interval)
    adjacency_threshold = timedelta(seconds=interval.to_seconds() * 2)

    merged = []
    current_start, current_end = sorted_ranges[0]

    for next_start, next_end in sorted_ranges[1:]:
        # If ranges overlap or are adjacent, extend the current range
        if next_start <= current_end + adjacency_threshold:
            current_end = max(current_end, next_end)
        else:
            # Otherwise, add the current range and start a new one
            merged.append((current_start, current_end))
            current_start, current_end = next_start, next_end

    # Add the final range
    merged.append((current_start, current_end))

    return merged


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names and data types to ensure consistency.

    This method ensures:
    1. Standardized column names (mapping variant names to canonical names)
    2. Consistent data types for all columns
    3. Timestamp precision standardization (to milliseconds, matching REST API)
    4. Proper handling of all timestamp-related columns
    5. Consistent timezone-aware datetime handling in UTC

    Args:
        df: DataFrame to standardize

    Returns:
        Standardized DataFrame following REST API format
    """
    if df.empty:
        return df

    # Standardize column names
    column_map = {
        # Common name variations
        "open_time_ms": "open_time",
        "openTime": "open_time",
        "close_time_ms": "close_time",
        "closeTime": "close_time",
        # Volume variants
        "volume_base": "volume",
        "baseVolume": "volume",
        "volume_quote": "quote_asset_volume",
        "quoteVolume": "quote_asset_volume",
        # Other variants
        "trades": "count",
        "numberOfTrades": "count",
    }

    # Apply column mapping
    for old_name, new_name in column_map.items():
        if old_name in df.columns and new_name not in df.columns:
            df = df.rename(columns={old_name: new_name})

    # First apply the centralized standardize_dataframe function
    # This function ensures proper column structure and data types
    df = standardize_dataframe(df)

    # Ensure open_time is a proper datetime column with UTC timezone
    if "open_time" in df.columns:
        # Convert integer/float timestamps to datetime if needed
        if pd.api.types.is_numeric_dtype(df["open_time"]):
            logger.debug("Converting numeric open_time to datetime64[ns, UTC]")
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        # Ensure timezone awareness for datetime columns
        elif pd.api.types.is_datetime64_dtype(df["open_time"]):
            if df["open_time"].dt.tz is None:
                logger.debug("Localizing naive datetime open_time to UTC")
                df["open_time"] = df["open_time"].dt.tz_localize("UTC")
            elif df["open_time"].dt.tz.zone != "UTC":
                logger.debug(f"Converting open_time timezone from {df['open_time'].dt.tz.zone} to UTC")
                df["open_time"] = df["open_time"].dt.tz_convert("UTC")

    # Apply the same timezone standardization to close_time
    if "close_time" in df.columns:
        if pd.api.types.is_numeric_dtype(df["close_time"]):
            logger.debug("Converting numeric close_time to datetime64[ns, UTC]")
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        elif pd.api.types.is_datetime64_dtype(df["close_time"]):
            if df["close_time"].dt.tz is None:
                logger.debug("Localizing naive datetime close_time to UTC")
                df["close_time"] = df["close_time"].dt.tz_localize("UTC")
            elif df["close_time"].dt.tz.zone != "UTC":
                logger.debug(f"Converting close_time timezone from {df['close_time'].dt.tz.zone} to UTC")
                df["close_time"] = df["close_time"].dt.tz_convert("UTC")

    # Set the index to open_time for standard representation
    if "open_time" in df.columns and df.index.name != "open_time":
        logger.debug("Setting index to open_time for standard representation")
        df = df.set_index("open_time")

    # Reset the index to keep open_time as a column too
    # This ensures both index and column are available to consumers
    if df.index.name == "open_time" and "open_time" not in df.columns:
        logger.debug("Adding open_time as column while keeping it as index")
        df = df.reset_index()
        # And set it back as index to have both
        df = df.set_index("open_time")

    # Then standardize timestamp precision to align with REST API format
    # This ensures Vision API data (which may use microsecond precision in 2025+)
    # is converted to millisecond precision to match REST API format
    if REST_IS_STANDARD:
        logger.debug("Standardizing timestamp precision to match REST API format")
        df = standardize_timestamp_precision(df)

    return df


def identify_missing_segments(
    df: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
) -> list[tuple[datetime, datetime]]:
    """Identify missing segments in the data using gap_detector.

    Args:
        df: DataFrame containing time series data
        start_time: Expected start time of the data
        end_time: Expected end time of the data
        interval: Time interval between data points

    Returns:
        List of (start, end) tuples representing missing segments
    """
    logger.debug(f"Identifying missing segments between {start_time} and {end_time}")
    if df.empty:
        logger.debug("DataFrame is empty, entire range is missing")
        return [(start_time, end_time)]

    df = ensure_open_time_as_column(df)
    if not pd.api.types.is_datetime64_any_dtype(df["open_time"]):
        logger.warning("open_time not datetime, converting...")
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.sort_values("open_time")

    min_time, max_time = df["open_time"].min(), df["open_time"].max()
    logger.debug(f"Data spans from {min_time} to {max_time}")

    from data_source_manager.utils.gap_detector import detect_gaps

    gaps, stats = detect_gaps(
        df=df,
        interval=interval,
        time_column="open_time",
        gap_threshold=0.1,
        day_boundary_threshold=1.0,
        enforce_min_span=False,
    )
    logger.debug(f"Gap detector found {stats['total_gaps']} gaps")

    missing_segments: list[tuple[datetime, datetime]] = []
    for gap in gaps:
        start = gap.start_time + timedelta(seconds=interval.to_seconds())
        end = gap.end_time
        missing_segments.append((start, end))

    if min_time > start_time:
        missing_segments.insert(0, (start_time, min_time))
    if max_time < end_time:
        boundary_end = max_time + timedelta(seconds=interval.to_seconds())
        if boundary_end < end_time:
            missing_segments.append((boundary_end, end_time))

    if missing_segments:
        missing_segments = merge_adjacent_ranges(missing_segments, interval)
    logger.debug(f"Final missing segments count: {len(missing_segments)}")
    return missing_segments


def merge_dataframes(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Merge multiple DataFrames into one, handling overlaps.

    This function is a critical part of the FCP mechanism that ensures:
    1. Each DataFrame has consistent open_time formatting
    2. Source information is preserved during merging
    3. When duplicate timestamps exist, higher priority sources are preferred
       (REST > VISION > CACHE, unless the data came from recent updates)
    4. Columns are consistently named, typed, and aligned
    5. The resulting DataFrame maintains 1-minute granularity

    Args:
        dfs: List of DataFrames to merge

    Returns:
        Merged DataFrame with consistent schema
    """
    if not dfs:
        logger.warning("Empty list of DataFrames to merge")
        from data_source_manager.utils.config import create_empty_dataframe

        return create_empty_dataframe()

    if len(dfs) == 1:
        logger.debug("Only one DataFrame to merge, standardizing and returning")
        # Ensure consistent formatting even for single DataFrame
        result = dfs[0].copy()
        return standardize_columns(result)

    # Log information about DataFrames to be merged
    logger.debug(f"Merging {len(dfs)} DataFrames")

    # Ensure all DataFrames have open_time as a column, not just an index
    for i, df in enumerate(dfs):
        if df.empty:
            logger.warning(f"DataFrame {i} is empty, skipping")
            continue

        if "open_time" not in df.columns:
            logger.debug(f"Converting index to open_time column in DataFrame {i}")
            if df.index.name == "open_time":
                df_reset = df.reset_index()
                dfs[i] = df_reset  # Update the DataFrame in the list
            else:
                logger.warning(f"DataFrame {i} has no open_time column or index")
                continue  # Skip this DataFrame as we can't process it

        # Ensure open_time is a datetime column
        if not pd.api.types.is_datetime64_any_dtype(dfs[i]["open_time"]):
            logger.debug(f"Converting open_time to datetime in DataFrame {i}")
            dfs[i]["open_time"] = pd.to_datetime(dfs[i]["open_time"], utc=True)

        # Add data source information if missing
        if "_data_source" not in dfs[i].columns:
            logger.debug(f"Adding unknown source tag to DataFrame {i}")
            dfs[i]["_data_source"] = "UNKNOWN"

    # Log source counts before merging
    for i, df in enumerate(dfs):
        if not df.empty and "_data_source" in df.columns:
            source_counts = df["_data_source"].value_counts()
            for source, count in source_counts.items():
                logger.debug(f"DataFrame {i} contains {count} records from source={source}")

    # Concatenate all DataFrames
    logger.debug(f"Concatenating {len(dfs)} DataFrames")
    merged = pd.concat(dfs, ignore_index=True)

    # Set source priority for resolving duplicates (higher number = higher priority)
    # IMPORTANT: This priority order is critical for the FCP mechanism:
    # - REST (3): Highest priority as it always has the most up-to-date data from the exchange
    # - CACHE (2): Second priority to prefer local data over network calls when available
    # - VISION (1): Lower priority than CACHE to minimize unnecessary network calls
    # - UNKNOWN (0): Lowest priority for data with unknown origin
    #
    # How conflict resolution works:
    # 1. When multiple data sources have records for the same timestamp, these priority values
    #    are used to determine which one to keep
    # 2. The DataFrame is sorted by open_time and then by _source_priority (ascending order)
    # 3. drop_duplicates(subset=["open_time"], keep="last") is called, which keeps the LAST
    #    occurrence of each timestamp - which will be the one with the HIGHEST priority value
    #    because of the sorting order
    #
    # Do not change this ordering without careful consideration of the FCP workflow.
    # Reversing CACHE and VISION would result in always preferring network calls over local cache.
    source_priority = {
        "UNKNOWN": 0,
        "VISION": 1,
        "CACHE": 2,
        "REST": 3,
    }

    # Add a numeric priority column based on data source
    if "_data_source" in merged.columns:
        merged["_source_priority"] = merged["_data_source"].map(source_priority)
    else:
        merged["_source_priority"] = 0

    # Sort by open_time and source priority (high priority last to keep in drop_duplicates)
    logger.debug("Sorting merged DataFrame by open_time and source priority")
    merged = merged.sort_values(["open_time", "_source_priority"])

    # Remove duplicates, keeping the highest priority source for each timestamp
    if "open_time" in merged.columns:
        before_count = len(merged)
        merged = merged.drop_duplicates(subset=["open_time"], keep="last")
        after_count = len(merged)

        if before_count > after_count:
            logger.debug(f"Removed {before_count - after_count} duplicate timestamps, keeping highest priority source")

    # Remove the temporary source priority column
    if "_source_priority" in merged.columns:
        merged = merged.drop(columns=["_source_priority"])

    # Sort by open_time to ensure chronological order
    merged = merged.sort_values("open_time").reset_index(drop=True)

    # Final standardization to ensure consistency across all columns
    merged = standardize_columns(merged)

    # Log statistics about the merged result
    if "_data_source" in merged.columns and not merged.empty:
        source_counts = merged["_data_source"].value_counts()
        for source, count in source_counts.items():
            percentage = (count / len(merged)) * 100
            logger.debug(f"Final merged DataFrame contains {count} records ({percentage:.1f}%) from {source}")

    logger.debug(f"Successfully merged {len(dfs)} DataFrames into one with {len(merged)} rows")
    return merged
