#!/usr/bin/env python
"""DataFrame time-based filtering utilities for market data.

This module provides functions for filtering DataFrames by time ranges
with robust handling of different timestamp formats.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from time_utils.py for modularity
# polars-exception: extracted from existing pandas-based time_utils.py - migration is separate task
"""

from datetime import datetime

import pandas as pd

from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.time.conversion import enforce_utc_timezone

__all__ = [
    "filter_dataframe_by_time",
]


def filter_dataframe_by_time(
    df: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
    time_column: str = "open_time",
    *,
    copy: bool = False,
) -> pd.DataFrame:
    """Filter a DataFrame by time range with robust handling of different timestamp formats.

    This function provides a consistent way to filter market data by a time range,
    handling various timestamp formats and edge cases:

    1. Both DatetimeIndex and regular column-based filtering
    2. Different timestamp precisions (milliseconds/microseconds)
    3. Different timestamp formats (datetime objects, epoch timestamps)
    4. Proper timezone handling (converting all to UTC)

    Args:
        df: DataFrame to filter, can have DatetimeIndex or time column
        start_time: Start of time range (inclusive)
        end_time: End of time range (inclusive)
        time_column: Name of timestamp column to filter by (defaults to "open_time")
        copy: If True, return a copy of the filtered data. If False (default),
              return a view for memory efficiency. Set to True only if you need
              to modify the result without affecting the original.

    Returns:
        pd.DataFrame: Filtered DataFrame containing only rows within the specified time range

    Example:
        >>> import pandas as pd
        >>> from datetime import datetime, timezone, timedelta
        >>> from data_source_manager.utils.time.filtering import filter_dataframe_by_time
        >>>
        >>> # Create sample data
        >>> now = datetime.now(timezone.utc)
        >>> dates = [now - timedelta(minutes=i) for i in range(10)]
        >>> df = pd.DataFrame({
        ...     'open_time': dates,
        ...     'value': range(10)
        ... })
        >>>
        >>> # Filter for last 5 minutes
        >>> start = now - timedelta(minutes=5)
        >>> filtered_df = filter_dataframe_by_time(df, start, now)
        >>>
        >>> print(f"Original rows: {len(df)}")
        >>> print(f"Filtered rows: {len(filtered_df)}")

    Note:
        The function handles both the case where the timestamp is the index
        and where it's a regular column. It prioritizes using DatetimeIndex
        filtering for better performance when available.
    """
    if df.empty:
        # MEMORY OPTIMIZATION: Empty DataFrame has no data to copy
        # Return as-is unless explicit copy requested
        return df.copy() if copy else df

    # Ensure times are timezone-aware
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    logger.debug(f"Filtering DataFrame by time: {start_time} to {end_time}")
    logger.debug(f"Before filtering: {len(df)} rows")

    # FAIL-FAST: Timezone-aware timestamp debugging with rich exception context
    from data_source_manager.utils_for_debug.timestamp_debug import (
        analyze_filter_conditions,
        compare_filtered_results,
        trace_dataframe_timestamps,
    )

    # Rich timezone-aware debugging - fails fast on timezone issues
    trace_dataframe_timestamps(df, time_column, start_time, end_time)
    analyze_filter_conditions(df, start_time, end_time, time_column)

    # Check if the time column exists
    if time_column not in df.columns:
        if df.index.name == time_column and isinstance(df.index, pd.DatetimeIndex):
            # Reset index to make the time column available for filtering
            df_with_column = df.reset_index()

            # IMPORTANT: Use >= for start_time and <= for end_time to ensure
            # exact interval boundaries are included correctly
            logger.debug(
                f"Filtering on index reset as column, using criteria: {time_column} >= {start_time} AND {time_column} <= {end_time}"
            )

            # MEMORY OPTIMIZATION: Boolean indexing returns view, copy only if requested
            filtered_df = df_with_column[(df_with_column[time_column] >= start_time) & (df_with_column[time_column] <= end_time)]
            if copy:
                filtered_df = filtered_df.copy()

            # Set index back
            if not filtered_df.empty:
                filtered_df = filtered_df.set_index(time_column)
        else:
            logger.warning(f"Time column '{time_column}' not found in DataFrame")
            # MEMORY OPTIMIZATION: Return as-is unless explicit copy requested
            return df.copy() if copy else df
    else:
        # Filter dataframe using the time column, preserving exact timestamps
        # IMPORTANT: Use >= for start_time and <= for end_time to include timestamps
        # exactly at the interval boundaries
        logger.debug(f"Filtering on column, using criteria: {time_column} >= {start_time} AND {time_column} <= {end_time}")

        # MEMORY OPTIMIZATION: Boolean indexing returns view, copy only if requested
        filtered_df = df[(df[time_column] >= start_time) & (df[time_column] <= end_time)]
        if copy:
            filtered_df = filtered_df.copy()

    # Reset index if it's not already the time column
    if filtered_df.index.name != time_column:
        filtered_df = filtered_df.reset_index(drop=True)

    if len(filtered_df) == 0:
        logger.warning(f"No data within time range {start_time} to {end_time}")
    else:
        logger.debug(f"After filtering: {len(filtered_df)} rows")
        if len(filtered_df) > 0:
            # Log the min and max timestamps in the filtered data
            if time_column in filtered_df.columns:
                min_ts = filtered_df[time_column].min()
                max_ts = filtered_df[time_column].max()
                logger.debug(f"First timestamp: {min_ts} (represents BEGINNING of candle)")
                logger.debug(f"Last timestamp: {max_ts} (represents BEGINNING of candle)")

                # Check if the first expected timestamp is present
                if min_ts > start_time:
                    time_diff = (min_ts - start_time).total_seconds()
                    logger.debug(f"First timestamp ({min_ts}) is later than requested start time ({start_time}), diff: {time_diff} seconds")
                    logger.debug("First candle is missing from result! This may indicate a timestamp interpretation issue.")

                # Check if the last expected timestamp is present
                if max_ts < end_time:
                    logger.debug(f"Last timestamp ({max_ts}) is earlier than requested end time ({end_time})")
            elif isinstance(filtered_df.index, pd.DatetimeIndex):
                min_ts = filtered_df.index.min()
                max_ts = filtered_df.index.max()
                logger.debug(f"First timestamp: {min_ts} (represents BEGINNING of candle)")
                logger.debug(f"Last timestamp: {max_ts} (represents BEGINNING of candle)")

    # FAIL-FAST: Timezone-aware validation of filtering results
    compare_filtered_results(df, filtered_df, start_time, end_time, time_column)

    return filtered_df
