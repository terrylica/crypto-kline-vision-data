#!/usr/bin/env python
"""Utility functions for working with DataSourceManager data.

This module provides helper functions for:
1. Reindexing DataFrames across different data sources
2. Verifying data completeness
3. Safely handling timezone-aware and naive datetimes
4. Standardizing timestamp formats
"""

from datetime import datetime, timezone

import pandas as pd

from utils.config import CANONICAL_INDEX_NAME
from utils.dataframe_utils import ensure_open_time_as_index
from utils.loguru_setup import logger
from utils.market_constraints import Interval


def safely_reindex_dataframe(
    df: pd.DataFrame,
    start_time: datetime | str | pd.Timestamp,
    end_time: datetime | str | pd.Timestamp,
    interval: Interval | str,
    fill_method: str | None = None,
) -> pd.DataFrame:
    """
    Safely reindex a DataFrame to a complete time range with the given interval.

    This function creates a complete, regular DatetimeIndex from start_time to end_time
    with the specified interval, then reindexes the given DataFrame to match this
    complete index. This is useful for ensuring all expected timestamps exist,
    even if some data is missing.

    Args:
        df: DataFrame to reindex
        start_time: Start time of the range (can be datetime, string, or Timestamp)
        end_time: End time of the range (can be datetime, string, or Timestamp)
        interval: Interval specification (e.g., "1m", "1h", Interval.MINUTE_1)
        fill_method: Method to fill missing values (e.g., "ffill", "bfill")

    Returns:
        DataFrame reindexed to the complete time range
    """
    if df.empty:
        logger.debug("Empty DataFrame passed to safely_reindex_dataframe")
        # For empty DataFrames, just create a complete range and return an empty DataFrame with that index
        complete_index = pd.date_range(
            start=start_time,
            end=end_time,
            freq="1min",  # Default to 1-minute intervals
            inclusive="left",
            name=CANONICAL_INDEX_NAME,
        )
        return pd.DataFrame(index=complete_index)

    # Handle string timestamps
    if isinstance(start_time, str):
        start_time = pd.to_datetime(start_time, utc=True)
    if isinstance(end_time, str):
        end_time = pd.to_datetime(end_time, utc=True)

    # Convert interval to string if it's an enum
    interval_str = interval.value if hasattr(interval, "value") else interval

    # Determine pandas frequency string based on interval
    freq = None
    if interval_str.endswith("s"):
        freq = f"{interval_str[:-1]}s"  # seconds - use lowercase 's' for seconds
    elif interval_str.endswith("m"):
        freq = f"{interval_str[:-1]}min"  # minutes (updated from 'T' to 'min')
    elif interval_str.endswith("h"):
        freq = f"{interval_str[:-1]}h"  # hours - use lowercase 'h' instead of 'H'
    elif interval_str.endswith("d"):
        freq = f"{interval_str[:-1]}D"  # days
    elif interval_str.endswith("w"):
        freq = f"{interval_str[:-1]}W"  # weeks

    if not freq:
        logger.error(f"Unrecognized interval format: {interval_str}")
        return df

    # Create a complete DatetimeIndex
    complete_index = pd.date_range(
        start=start_time,
        end=end_time,
        freq=freq,
        inclusive="left",  # Include start but not end
        name=CANONICAL_INDEX_NAME,
    )

    # Ensure df has open_time as index
    df = ensure_open_time_as_index(df)

    # Create a new DataFrame with the complete index
    try:
        # Handle index differently based on whether open_time is present in columns
        if CANONICAL_INDEX_NAME in df.columns:
            # Keep both index and column
            result_df = df.reindex(complete_index)
            # Add the index as a column too
            result_df = result_df.reset_index()
        else:
            # Only keep as index
            result_df = df.reindex(complete_index)

        # Apply fill method if specified
        if fill_method and not result_df.empty:
            # Use explicit methods to avoid deprecation warnings
            if fill_method == "ffill":
                # Set option to avoid downcasting warning
                with pd.option_context("future.no_silent_downcasting", True):
                    result_df = result_df.ffill()
            elif fill_method == "bfill":
                # Set option to avoid downcasting warning
                with pd.option_context("future.no_silent_downcasting", True):
                    result_df = result_df.bfill()
            else:
                logger.warning(f"Unsupported fill method: {fill_method}, no filling applied")

        logger.debug(f"Reindexed DataFrame from {len(df)} to {len(result_df)} rows using {interval_str} interval")

        # Log information about missing data
        if not df.empty and not result_df.empty:
            missing_count = result_df.isna().any(axis=1).sum()
            if missing_count > 0:
                missing_pct = (missing_count / len(result_df)) * 100
                logger.warning(f"Reindexed DataFrame contains {missing_count}/{len(result_df)} rows ({missing_pct:.2f}%) with missing data")

        return result_df

    except Exception as e:
        logger.error(f"Error reindexing DataFrame: {e}")
        return df


def ensure_consistent_timezone(dt: datetime | pd.Timestamp | str | None) -> datetime | None:
    """
    Ensure a datetime object has a consistent timezone (UTC).

    This function standardizes timezone handling by:
    1. Converting string datetimes to datetime objects
    2. Adding UTC timezone to naive datetimes
    3. Converting any non-UTC timezone to UTC

    Args:
        dt: Datetime object, string, or None

    Returns:
        Timezone-aware datetime object in UTC, or None if input is None

    Example:
        >>> from datetime import datetime
        >>> naive_dt = datetime(2023, 1, 1)
        >>> aware_dt = ensure_consistent_timezone(naive_dt)
        >>> print(aware_dt.tzinfo)
        UTC
    """
    if dt is None:
        return None

    # Convert string to datetime if needed
    if isinstance(dt, str):
        try:
            dt = pd.to_datetime(dt)
        except Exception as e:
            logger.error(f"Error converting string to datetime: {e}")
            # Return a default datetime if conversion fails
            return datetime.now(timezone.utc)

    # Convert pandas Timestamp to datetime if needed
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()

    # Add UTC timezone if naive
    if dt.tzinfo is None:
        logger.debug(f"Adding UTC timezone to naive datetime: {dt}")
        dt = dt.replace(tzinfo=timezone.utc)
    # Convert to UTC if not already
    elif dt.tzinfo != timezone.utc:
        logger.debug(f"Converting timezone from {dt.tzinfo} to UTC")
        dt = dt.astimezone(timezone.utc)

    return dt


def safe_timestamp_comparison(ts1: int | float | datetime | pd.Timestamp | str, ts2: int | float | datetime | pd.Timestamp | str) -> int:
    """
    Safely compare two timestamps of potentially different types.

    This handles the common issue where timestamps may be represented as:
    - Integer milliseconds
    - Datetime objects (naive or timezone-aware)
    - Pandas Timestamps
    - ISO format strings

    Args:
        ts1: First timestamp (any supported format)
        ts2: Second timestamp (any supported format)

    Returns:
        -1 if ts1 < ts2, 0 if ts1 == ts2, 1 if ts1 > ts2

    Example:
        >>> from datetime import datetime, timezone
        >>> # Compare millisecond timestamp to datetime
        >>> result = safe_timestamp_comparison(1640995200000, datetime(2022, 1, 1, tzinfo=timezone.utc))
        >>> print(result)
        0
    """
    # Convert both timestamps to datetime objects for comparison
    dt1 = _convert_to_datetime(ts1)
    dt2 = _convert_to_datetime(ts2)

    # Ensure both have UTC timezone
    dt1 = ensure_consistent_timezone(dt1)
    dt2 = ensure_consistent_timezone(dt2)

    # Compare the standardized datetimes
    if dt1 < dt2:
        return -1
    if dt1 > dt2:
        return 1
    return 0


def _convert_to_datetime(ts: int | float | datetime | pd.Timestamp | str) -> datetime:
    """
    Convert various timestamp formats to a standard datetime object.

    Args:
        ts: Timestamp in any supported format

    Returns:
        datetime object
    """
    if isinstance(ts, int | float):
        # Determine if milliseconds or seconds based on magnitude
        if ts > 1e11:  # Likely milliseconds (13 digits for recent years)
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        # Likely seconds
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    if isinstance(ts, str):
        # Try to parse ISO format string
        try:
            dt = pd.to_datetime(ts)
            if isinstance(dt, pd.Timestamp):
                return dt.to_pydatetime()
            return dt
        except Exception as e:
            logger.error(f"Error parsing datetime string '{ts}': {e}")
            # Return current time as fallback
            return datetime.now(timezone.utc)

    elif isinstance(ts, pd.Timestamp):
        return ts.to_pydatetime()

    elif isinstance(ts, datetime):
        return ts

    else:
        logger.error(f"Unsupported timestamp type: {type(ts)}")
        # Return current time as fallback
        return datetime.now(timezone.utc)


def get_data_source_info(df: pd.DataFrame) -> dict:
    """
    Get information about data sources used in the DataFrame.

    This is useful for understanding what data sources were used in a merged DataFrame.

    Args:
        df: DataFrame to analyze

    Returns:
        Dictionary with information about data sources

    Example:
        >>> # For a DataFrame with data from multiple sources
        >>> info = get_data_source_info(df)
        >>> print(info)
        {'sources': ['CACHE', 'VISION', 'REST'], 'source_counts': {'CACHE': 120, 'VISION': 240, 'REST': 60}}
    """
    if df.empty:
        return {"sources": [], "source_counts": {}}

    # Check if the DataFrame has source information
    if "_data_source" in df.columns:
        sources = df["_data_source"].unique().tolist()
        source_counts = df["_data_source"].value_counts().to_dict()

        return {"sources": sources, "source_counts": source_counts}
    return {"sources": ["UNKNOWN"], "source_counts": {"UNKNOWN": len(df)}}


def check_window_data_completeness(df: pd.DataFrame, window_size: int, min_required_pct: float = 80.0) -> tuple[bool, float]:
    """
    Check if a DataFrame has enough data for window-based calculations.

    This is useful for applications that need to perform window-based calculations
    (like moving averages) and need to ensure enough data is available.

    Args:
        df: DataFrame to check
        window_size: Size of the window for calculations
        min_required_pct: Minimum percentage of data points required (default: 80%)

    Returns:
        Tuple containing:
            - bool: True if enough data is available, False otherwise
            - float: Percentage of available data points

    Example:
        >>> # Check if enough data for a 24-period calculation
        >>> is_complete, available_pct = check_window_data_completeness(df, 24, 90.0)
        >>> if is_complete:
        ...     print(f"Data is {available_pct:.1f}% complete - proceeding with calculation")
        ... else:
        ...     print(f"Only {available_pct:.1f}% data available - skipping calculation")
    """
    if df.empty:
        return False, 0.0

    # Count non-NaN values in relevant columns
    non_nan_counts = {}
    for col in df.columns:
        if col not in (CANONICAL_INDEX_NAME, "_data_source"):
            non_nan_counts[col] = df[col].notna().sum()

    # If no relevant columns found, return False
    if not non_nan_counts:
        return False, 0.0

    # Calculate average completeness across columns
    avg_non_nan = sum(non_nan_counts.values()) / len(non_nan_counts)
    completeness_pct = (avg_non_nan / len(df)) * 100

    # Check if we have enough data for the window
    has_enough_data = avg_non_nan >= (window_size * min_required_pct / 100)

    return has_enough_data, completeness_pct
