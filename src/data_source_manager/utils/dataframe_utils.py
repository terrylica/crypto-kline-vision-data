#!/usr/bin/env python
# polars-exception: Core pandas DataFrame utility functions - provides
# index/column manipulation, timezone handling, and standardization for pd.DataFrame
"""DataFrame utilities for consistent handling of pandas DataFrames.

This module centralizes common DataFrame operations to ensure consistent behavior
across the codebase, particularly for:
1. Handling open_time as both column and index
2. Standardizing DataFrame formats
3. Converting between different DataFrame representations
4. Ensuring proper index configuration

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""

import contextlib
import traceback
from datetime import datetime, timedelta, timezone

import pandas as pd

from data_source_manager.utils.config import (
    CANONICAL_INDEX_NAME,
    DEFAULT_COLUMN_ORDER,
    FUNDING_RATE_DTYPES,
    OUTPUT_DTYPES,
)
from data_source_manager.utils.loguru_setup import logger


def ensure_open_time_as_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure that open_time exists as a column in the DataFrame.

    This handles the common cases:
    1. DataFrame has open_time as an index but not as a column
    2. DataFrame has both open_time as index and column (resolves ambiguity)
    3. DataFrame is missing open_time completely (attempts recovery)

    Args:
        df: Input DataFrame to process

    Returns:
        DataFrame with open_time guaranteed to be present as a column
    """
    if df.empty:
        logger.debug("Empty DataFrame passed to ensure_open_time_as_column")
        return df

    # Log initial state
    logger.debug(f"DataFrame columns: {list(df.columns)}")
    logger.debug(f"DataFrame index name: {df.index.name}")
    logger.debug(f"DataFrame index type: {type(df.index)}")

    # Case 1: open_time exists as both index and column
    if hasattr(df, "index") and hasattr(df.index, "name") and df.index.name == CANONICAL_INDEX_NAME and CANONICAL_INDEX_NAME in df.columns:
        logger.debug("Resolving ambiguity: open_time exists as both index and column")
        df = df.reset_index(drop=True)  # Keep only the column version

    # Case 2: open_time exists only as index
    elif (
        hasattr(df, "index")
        and hasattr(df.index, "name")
        and df.index.name == CANONICAL_INDEX_NAME
        and CANONICAL_INDEX_NAME not in df.columns
    ):
        logger.debug("Converting open_time from index to column")
        df = df.reset_index()  # Convert index to column

    # Case 3: open_time missing completely
    elif CANONICAL_INDEX_NAME not in df.columns:
        logger.warning("open_time missing as both column and index - attempting recovery")

        # Try to find any time-related columns that could serve as open_time
        for col in df.columns:
            if "time" in col.lower() and pd.api.types.is_datetime64_any_dtype(df[col]):
                logger.debug(f"Using {col} as open_time")
                df[CANONICAL_INDEX_NAME] = df[col]
                break
        else:
            # If the index is a DatetimeIndex, use it
            if isinstance(df.index, pd.DatetimeIndex):
                logger.debug("Using DatetimeIndex as open_time")
                df[CANONICAL_INDEX_NAME] = df.index
            else:
                logger.error("Cannot find suitable column to use as open_time")
                # Create a placeholder column to prevent errors
                # This is a last resort and should be handled by the caller
                df[CANONICAL_INDEX_NAME] = pd.Series(dtype="datetime64[ns, UTC]")

    # Ensure the column is datetime type with UTC timezone
    if CANONICAL_INDEX_NAME in df.columns:
        # Handle non-datetime columns
        if not pd.api.types.is_datetime64_any_dtype(df[CANONICAL_INDEX_NAME]):
            try:
                logger.debug(f"Converting {CANONICAL_INDEX_NAME} to datetime")
                df[CANONICAL_INDEX_NAME] = pd.to_datetime(df[CANONICAL_INDEX_NAME], utc=True)
            except (ValueError, TypeError) as e:
                logger.error(f"Error converting {CANONICAL_INDEX_NAME} to datetime: {e}")

        # Ensure timezone is UTC
        if hasattr(df[CANONICAL_INDEX_NAME], "dt") and hasattr(df[CANONICAL_INDEX_NAME].dt, "tz"):
            if df[CANONICAL_INDEX_NAME].dt.tz is None:
                logger.debug(f"Localizing {CANONICAL_INDEX_NAME} to UTC")
                df[CANONICAL_INDEX_NAME] = df[CANONICAL_INDEX_NAME].dt.tz_localize(timezone.utc)
            elif df[CANONICAL_INDEX_NAME].dt.tz != timezone.utc:
                logger.debug(f"Converting {CANONICAL_INDEX_NAME} timezone to UTC")
                df[CANONICAL_INDEX_NAME] = df[CANONICAL_INDEX_NAME].dt.tz_convert(timezone.utc)

    logger.debug(f"Final DataFrame columns: {list(df.columns)}")
    return df


def ensure_open_time_as_index(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure that open_time exists as the DataFrame index.

    This handles the common cases:
    1. DataFrame has open_time as a column but not as the index
    2. DataFrame has both open_time as index and column (resolves ambiguity)
    3. DataFrame is missing open_time completely (attempts recovery)

    Args:
        df: Input DataFrame to process

    Returns:
        DataFrame with open_time as the index
    """
    if df.empty:
        logger.debug("Empty DataFrame passed to ensure_open_time_as_index")
        # Create empty DatetimeIndex with proper name and timezone
        df.index = pd.DatetimeIndex([], name=CANONICAL_INDEX_NAME, tz=timezone.utc)
        return df

    # Log initial state
    logger.debug(f"DataFrame columns: {list(df.columns)}")
    logger.debug(f"DataFrame index name: {df.index.name}")
    logger.debug(f"DataFrame index type: {type(df.index)}")

    try:
        # Case 1: open_time is already properly set as index
        if isinstance(df.index, pd.DatetimeIndex) and df.index.name == CANONICAL_INDEX_NAME:
            logger.debug("open_time already properly set as index")

            # Case 1a: index is timezone-naive - localize to UTC
            if df.index.tz is None:
                logger.debug("Localizing timezone-naive DatetimeIndex to UTC")
                df.index = df.index.tz_localize(timezone.utc)
            # Case 1b: index has non-UTC timezone - convert to UTC
            elif df.index.tz != timezone.utc:
                logger.debug(f"Converting DatetimeIndex timezone from {df.index.tz} to UTC")
                df.index = df.index.tz_convert(timezone.utc)

            return df

        # Case 2: open_time exists as a column - use it as index
        if CANONICAL_INDEX_NAME in df.columns:
            logger.debug("Setting open_time column as index")

            # Ensure the column is properly timezone-aware
            if (
                pd.api.types.is_datetime64_dtype(df[CANONICAL_INDEX_NAME])
                and hasattr(df[CANONICAL_INDEX_NAME], "dt")
                and hasattr(df[CANONICAL_INDEX_NAME].dt, "tz")
            ):
                if df[CANONICAL_INDEX_NAME].dt.tz is None:
                    logger.debug("Localizing open_time column to UTC before setting as index")
                    df[CANONICAL_INDEX_NAME] = df[CANONICAL_INDEX_NAME].dt.tz_localize(timezone.utc)
                elif df[CANONICAL_INDEX_NAME].dt.tz != timezone.utc:
                    logger.debug(f"Converting open_time column timezone from {df[CANONICAL_INDEX_NAME].dt.tz} to UTC")
                    df[CANONICAL_INDEX_NAME] = df[CANONICAL_INDEX_NAME].dt.tz_convert(timezone.utc)

            try:
                return df.set_index(CANONICAL_INDEX_NAME)
            except (ValueError, KeyError) as e:
                logger.error(f"Error setting {CANONICAL_INDEX_NAME} as index: {e}")
                # Continue to recovery options

            # Case 4: index is unnamed or has a different name
            else:
                logger.warning("No suitable open_time found - attempting recovery")

                # If the index is a DatetimeIndex but has the wrong name
                if isinstance(df.index, pd.DatetimeIndex):
                    logger.debug("Renaming existing DatetimeIndex to open_time")
                    df.index.name = CANONICAL_INDEX_NAME
                else:
                    # Try to find any time-related columns
                    time_col = None
                    for col in df.columns:
                        if "time" in col.lower() and pd.api.types.is_datetime64_any_dtype(df[col]):
                            time_col = col
                            break

                    if time_col:
                        logger.debug(f"Using {time_col} as open_time index")
                        try:
                            df = df.set_index(time_col)
                            df.index.name = CANONICAL_INDEX_NAME
                        except (ValueError, KeyError) as e:
                            logger.error(f"Error setting {time_col} as index: {e}")
                            # Create a copy of the column first before setting as index
                            df[CANONICAL_INDEX_NAME] = df[time_col]
                            df = df.set_index(CANONICAL_INDEX_NAME)
                    else:
                        # Last resort - try to create an open_time column based on row number
                        logger.warning("Creating synthetic open_time based on row number")
                        now = datetime.now(timezone.utc)
                        base_time = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

                        # Create evenly spaced timestamps - Using minute intervals
                        df[CANONICAL_INDEX_NAME] = [base_time + timedelta(minutes=i) for i in range(len(df))]
                        df = df.set_index(CANONICAL_INDEX_NAME)

                        logger.warning("Created synthetic timestamps. This is a fallback and may not represent real data timestamps.")

                # Ensure index is datetime type with UTC timezone
                if not isinstance(df.index, pd.DatetimeIndex):
                    logger.debug("Converting index to DatetimeIndex")
                    # Try to convert the index to datetime (if it's not already)
                    try:
                        df.index = pd.to_datetime(df.index, utc=True)
                        df.index.name = CANONICAL_INDEX_NAME
                    except (ValueError, TypeError) as e:
                        logger.error(f"Error converting index to datetime: {e}")
                        # Create a new DatetimeIndex if conversion fails
                        old_index = df.index.copy()
                        df = df.reset_index()
                        now = datetime.now(timezone.utc)
                        base_time = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
                        df[CANONICAL_INDEX_NAME] = [base_time + timedelta(minutes=i) for i in range(len(df))]
                        # Add the old index as a column with a different name for reference
                        df["original_index"] = old_index
                        df = df.set_index(CANONICAL_INDEX_NAME)
                        logger.warning("Created synthetic index. Original index preserved in 'original_index' column.")

                # Ensure timezone is UTC
                if isinstance(df.index, pd.DatetimeIndex):
                    if df.index.tz is None:
                        logger.debug("Localizing index to UTC")
                        df.index = df.index.tz_localize(timezone.utc)
                    elif df.index.tz != timezone.utc:
                        logger.debug("Converting index timezone to UTC")
                        df.index = df.index.tz_convert(timezone.utc)

            # Ensure index is sorted
            if not df.index.is_monotonic_increasing:
                logger.debug("Sorting DataFrame by index")
                df = df.sort_index()

            # Remove duplicates from index
            if df.index.has_duplicates:
                logger.warning("Removing duplicate indices")
                df = df[~df.index.duplicated(keep="first")]

    except (ValueError, TypeError, KeyError) as e:
        logger.error(f"Error in ensure_open_time_as_index: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")

        # If all else fails, force a minimal working solution
        if CANONICAL_INDEX_NAME in df.columns:
            logger.warning("Fallback: Using existing open_time column as index despite error")
            # Last resort, try a simple set_index operation
            with contextlib.suppress(Exception):
                df = df.set_index(CANONICAL_INDEX_NAME)
        else:
            logger.warning("Fallback: Creating a new synthetic index despite error")
            # Last resort, create a new empty index
            df.index = pd.DatetimeIndex([datetime.now(timezone.utc)] * len(df), name=CANONICAL_INDEX_NAME)

    logger.debug(f"Final DataFrame index name: {df.index.name}")
    logger.debug(f"Final DataFrame index type: {type(df.index)}")
    return df


def verify_data_completeness(
    df: pd.DataFrame, start_time: datetime, end_time: datetime, interval: str
) -> tuple[bool, list[tuple[datetime, datetime]]]:
    """Verify data completeness and identify any gaps in the time series.

    This function checks if the provided DataFrame contains all expected data points
    within the given time range at the specified interval. It identifies and returns
    any gaps in the data.

    Args:
        df: DataFrame to check for completeness
        start_time: Start time of the expected range (timezone-aware)
        end_time: End time of the expected range (timezone-aware)
        interval: Interval string (e.g., "1m", "1h", "1d")

    Returns:
        tuple containing:
            - bool: True if data is complete, False if gaps exist
            - list of (start, end) tuples representing gaps in the data
    """
    if df.empty:
        logger.warning("Empty DataFrame - entire requested range is missing")
        return False, [(start_time, end_time)]

    # Ensure we have open_time as index
    df = ensure_open_time_as_index(df)

    # Create a complete time index at the specified interval
    # Parse the interval string to create a proper frequency string for pandas
    freq = None
    if interval.endswith("s"):
        freq = f"{interval[:-1]}s"  # seconds - use lowercase 's' for seconds
    elif interval.endswith("m"):
        freq = f"{interval[:-1]}min"  # minutes (updated from 'T' to 'min')
    elif interval.endswith("h"):
        freq = f"{interval[:-1]}h"  # hours - use lowercase 'h' instead of 'H'
    elif interval.endswith("d"):
        freq = f"{interval[:-1]}D"  # days
    elif interval.endswith("w"):
        freq = f"{interval[:-1]}W"  # weeks

    if not freq:
        logger.error(f"Unrecognized interval format: {interval}")
        return False, [(start_time, end_time)]

    # Create the expected index
    expected_index = pd.date_range(
        start=start_time,
        end=end_time,
        freq=freq,
        inclusive="left",  # Include start_time but not end_time
        name=CANONICAL_INDEX_NAME,
    )

    # Find missing timestamps
    missing_timestamps = expected_index.difference(df.index)

    if len(missing_timestamps) == 0:
        logger.info("Data is complete - no gaps detected")
        return True, []

    # Convert missing timestamps to ranges
    gaps = []
    if len(missing_timestamps) > 0:
        # Sort missing timestamps
        missing_timestamps = missing_timestamps.sort_values()

        # Group consecutive missing timestamps into ranges
        gap_start = missing_timestamps[0]
        prev_ts = missing_timestamps[0]

        for ts in missing_timestamps[1:]:
            # If this timestamp is not right after the previous one, we have a new gap
            expected_next = prev_ts + pd.Timedelta(freq)
            if ts != expected_next:
                # Add the previous gap to our list
                gaps.append((gap_start, prev_ts + pd.Timedelta(freq)))
                # Start a new gap
                gap_start = ts
            prev_ts = ts

        # Add the last gap
        gaps.append((gap_start, prev_ts + pd.Timedelta(freq)))

    # Log information about gaps
    total_missing = len(missing_timestamps)
    total_expected = len(expected_index)
    missing_pct = (total_missing / total_expected) * 100 if total_expected > 0 else 0

    logger.warning(
        f"Data incomplete: {total_missing}/{total_expected} timestamps missing ({missing_pct:.2f}%). Found {len(gaps)} gaps in the data."
    )

    for i, (gap_start, gap_end) in enumerate(gaps):
        gap_duration = (gap_end - gap_start).total_seconds()
        logger.debug(f"Gap {i + 1}: {gap_start} to {gap_end} ({gap_duration}s)")

    return False, gaps


def standardize_dataframe(df: pd.DataFrame, keep_as_column: bool = True) -> pd.DataFrame:
    """Standardize a DataFrame for consistent use throughout the system.

    This is a comprehensive function that ensures:
    1. open_time is properly handled (as index, column, or both)
    2. All columns have the correct data types
    3. The DataFrame is properly sorted
    4. No duplicate indices exist

    Args:
        df: Input DataFrame to standardize
        keep_as_column: Whether to keep open_time as a column (in addition to index)
                       Set to True for REST API and Vision API compatibility
                       Set to False for compact storage (cache files)

    Returns:
        Standardized DataFrame
    """
    if df.empty:
        logger.debug("Empty DataFrame passed to standardize_dataframe")
        return df

    # Ensure open_time is available as a column (needed for most operations)
    df = ensure_open_time_as_column(df)

    # Set index to open_time for proper sorting and lookup
    df = ensure_open_time_as_index(df)

    # If requested, keep open_time as a column too
    if keep_as_column and CANONICAL_INDEX_NAME not in df.columns:
        logger.debug("Adding open_time as column from index")
        df = df.reset_index()

    # Fix: Handle the case where quote_volume exists but quote_asset_volume doesn't
    if "quote_volume" in df.columns:
        if "quote_asset_volume" not in df.columns:
            logger.debug("Mapping 'quote_volume' to 'quote_asset_volume' to match standard column naming")
            df["quote_asset_volume"] = df["quote_volume"]
        elif "quote_asset_volume" in df.columns and df["quote_asset_volume"].isna().any() and not df["quote_volume"].isna().all():
            # If quote_asset_volume has NaN values but quote_volume has values, use those
            logger.debug("Filling NaN values in 'quote_asset_volume' with values from 'quote_volume'")
            mask = df["quote_asset_volume"].isna()
            df.loc[mask, "quote_asset_volume"] = df.loc[mask, "quote_volume"]

    # Use centralized DEFAULT_COLUMN_ORDER from config.py instead of duplicating
    standard_columns = DEFAULT_COLUMN_ORDER.copy()

    # Add open_time to front of column list if we're keeping it as a column
    if keep_as_column:
        standard_columns = [CANONICAL_INDEX_NAME, *standard_columns]

    # Add data source info if present
    if "_data_source" in df.columns:
        standard_columns.append("_data_source")

    # Create a new DataFrame with only the standard columns that exist
    result_columns = [col for col in standard_columns if col in df.columns]

    # If any standard columns are missing, log a warning
    missing_columns = [col for col in standard_columns if col not in df.columns]
    if missing_columns:
        logger.warning(f"Missing standard columns in output: {missing_columns}")

    # Return DataFrame with standardized columns
    return df[result_columns]


def merge_dataframes(dfs: list[pd.DataFrame], priority_sources: list[str] | None = None) -> pd.DataFrame:
    """Merge multiple DataFrames into one, handling overlaps by data source priority.

    Args:
        dfs: List of DataFrames to merge
        priority_sources: List of data source names in descending priority order
                         (highest priority first)
                         Default: ["CACHE", "VISION", "REST"]

    Returns:
        Merged DataFrame
    """
    if not dfs:
        logger.warning("No DataFrames provided to merge")
        return pd.DataFrame()

    # Use default priority if not specified
    if priority_sources is None:
        priority_sources = ["CACHE", "VISION", "REST"]

    # Filter out empty DataFrames
    dfs = [df for df in dfs if not df.empty]

    if not dfs:
        logger.warning("All provided DataFrames were empty")
        return pd.DataFrame()

    if len(dfs) == 1:
        logger.debug("Only one non-empty DataFrame provided, no merging needed")
        return standardize_dataframe(dfs[0])

    # Ensure each DataFrame has open_time as a column
    normalized_dfs = [ensure_open_time_as_column(df) for df in dfs]

    # Concatenate all DataFrames
    merged = pd.concat(normalized_dfs, ignore_index=True)

    # If _data_source column exists, use it for prioritization
    if "_data_source" in merged.columns:
        # Create a priority order for data sources
        source_priorities = {source: i for i, source in enumerate(priority_sources)}
        default_priority = len(priority_sources)  # Lowest priority for unknown sources

        # Add a priority column for sorting
        merged["_source_priority"] = merged["_data_source"].apply(lambda source: source_priorities.get(source, default_priority))

        # Sort by open_time first, then by source priority
        merged = merged.sort_values(["open_time", "_source_priority"])

        # Drop the temporary priority column
        merged = merged.drop(columns=["_source_priority"])
    else:
        # If no source information, just sort by open_time
        merged = merged.sort_values("open_time")

    # Remove duplicates based on open_time, keeping the first one (highest priority)
    merged = merged.drop_duplicates(subset=["open_time"], keep="first")

    # Sort one final time and reset index
    merged = merged.sort_values("open_time").reset_index(drop=True)

    # Log data source statistics if available
    if "_data_source" in merged.columns:
        source_counts = merged["_data_source"].value_counts()
        logger.info(f"Merged data source statistics: {source_counts.to_dict()}")

    # Standardize the final DataFrame
    return standardize_dataframe(merged)


def convert_to_standardized_formats(df: pd.DataFrame, output_format: str = "default", chart_type: str = "klines") -> pd.DataFrame:
    """Convert a DataFrame to a standardized format based on the specified output format.

    Args:
        df: Input DataFrame to convert
        output_format: Output format (default, column_only, index_only)
        chart_type: Chart type (klines, funding_rate)

    Returns:
        Converted DataFrame
    """
    if df.empty:
        logger.debug("Empty DataFrame passed to convert_to_standardized_formats")
        return df

    # Select appropriate dtype mapping based on chart type
    dtypes = FUNDING_RATE_DTYPES if chart_type.lower() == "funding_rate" else OUTPUT_DTYPES

    # Ensure correct data types
    for col, dtype in dtypes.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to convert {col} to {dtype}: {e}")

    # Perform the conversion based on the requested format
    if output_format == "column_only":
        # Ensure open_time is a column but not an index
        if df.index.name == CANONICAL_INDEX_NAME and CANONICAL_INDEX_NAME not in df.columns:
            df = df.reset_index()
        elif df.index.name == CANONICAL_INDEX_NAME and CANONICAL_INDEX_NAME in df.columns:
            df = df.reset_index(drop=True)  # Drop the index version to avoid ambiguity

    elif output_format == "index_only":
        # Ensure open_time is the index but not a column
        df = ensure_open_time_as_index(df)
        if CANONICAL_INDEX_NAME in df.columns:
            df = df.drop(columns=[CANONICAL_INDEX_NAME])

    else:  # default - keep both
        # Ensure open_time is both an index and a column
        df = ensure_open_time_as_index(df)
        if CANONICAL_INDEX_NAME not in df.columns:
            # Add it as a column too
            df = df.reset_index()
            df = df.set_index(CANONICAL_INDEX_NAME)

    return df


def format_dataframe_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Format DataFrame for display with better readability.

    Args:
        df: DataFrame to format

    Returns:
        Formatted DataFrame
    """
    # Make a copy to avoid modifying the original
    display_df = df.copy()

    # Format timestamp columns for better readability
    datetime_cols = display_df.select_dtypes(include=["datetime64"]).columns
    for col in datetime_cols:
        display_df[col] = display_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Format float columns to reduce decimal places
    float_cols = display_df.select_dtypes(include=["float"]).columns
    for col in float_cols:
        display_df[col] = display_df[col].round(4)

    return display_df
