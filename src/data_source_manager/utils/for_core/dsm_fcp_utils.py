#!/usr/bin/env python
# polars-exception: FCP utilities process pandas DataFrames from DSM pipeline
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""Utility functions for Failover Control Protocol (FCP) implementation."""

import traceback
from datetime import datetime, timezone

import pandas as pd

from data_source_manager.utils.config import create_empty_dataframe
from data_source_manager.utils.for_core.dsm_time_range_utils import (
    identify_missing_segments,
    merge_adjacent_ranges,
    merge_dataframes,
)
from data_source_manager.utils.for_core.vision_exceptions import UnsupportedIntervalError
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import (
    Interval,
    MarketType,
    get_market_capabilities,
    is_interval_supported,
)


def validate_interval(market_type: MarketType, interval: Interval) -> None:
    """Validate that the interval is supported by the market type.

    Args:
        market_type: Market type to validate against
        interval: Interval to validate

    Raises:
        UnsupportedIntervalError: If interval isn't supported
    """
    if not is_interval_supported(market_type, interval):
        capabilities = get_market_capabilities(market_type)
        supported_intervals = [i.value for i in capabilities.supported_intervals]

        # Find the minimum supported interval for suggestion
        min_interval = min(capabilities.supported_intervals, key=lambda x: x.to_seconds())

        error_msg = (
            f"Interval {interval.value} is not supported by {market_type.name} market. "
            f"Supported intervals: {supported_intervals}. "
            f"Consider using {min_interval.value} (minimum supported interval) "
            f"or another interval from the list."
        )

        logger.error(error_msg)
        raise UnsupportedIntervalError(error_msg)


def process_cache_step(
    use_cache: bool,
    get_from_cache_func,
    symbol: str,
    aligned_start: datetime,
    aligned_end: datetime,
    interval: Interval,
    include_source_info: bool,
) -> tuple[pd.DataFrame, list[tuple[datetime, datetime]]]:
    """Process the cache step (Step 1) of the FCP mechanism.

    Args:
        use_cache: Whether cache is enabled
        get_from_cache_func: Function to retrieve data from cache
        symbol: Symbol to retrieve data for
        aligned_start: Aligned start time
        aligned_end: Aligned end time
        interval: Interval for data points
        include_source_info: Whether to include source info in the DataFrame

    Returns:
        Tuple of (result_df, missing_ranges)
    """
    logger.info(f"[FCP] STEP 1: Checking local cache for {symbol}")

    # Get data from cache
    cache_df, missing_ranges = get_from_cache_func(symbol, aligned_start, aligned_end, interval)

    if not cache_df.empty:
        # Add source info if requested
        if include_source_info and "_data_source" not in cache_df.columns:
            cache_df["_data_source"] = "CACHE"

        # Log the time range of the cache data
        min_time = cache_df["open_time"].min()
        max_time = cache_df["open_time"].max()
        logger.debug(f"[FCP] Cache data provides records from {min_time} to {max_time}")

        logger.info(f"[FCP] Cache contributed {len(cache_df)} records")
        return cache_df, missing_ranges
    # If cache is empty, treat entire range as missing
    missing_ranges = [(aligned_start, aligned_end)]
    logger.debug(f"[FCP] No cache data available, entire range marked as missing: {aligned_start} to {aligned_end}")
    return create_empty_dataframe(), missing_ranges


def process_vision_step(
    fetch_from_vision_func,
    symbol: str,
    missing_ranges: list[tuple[datetime, datetime]],
    interval: Interval,
    include_source_info: bool,
    result_df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[tuple[datetime, datetime]]]:
    """Process the Vision API step (Step 2) of the FCP mechanism.

    Args:
        fetch_from_vision_func: Function to fetch data from Vision API
        symbol: Symbol to retrieve data for
        missing_ranges: List of missing time ranges
        interval: Interval for data points
        include_source_info: Whether to include source info in the DataFrame
        result_df: Existing results DataFrame to merge with

    Returns:
        Tuple of (updated_result_df, remaining_missing_ranges)
    """
    logger.info("[FCP] STEP 2: Checking Vision API for missing data")

    # Process each missing range (no copy needed - list is only iterated, not modified)
    remaining_ranges = []

    for range_idx, (miss_start, miss_end) in enumerate(missing_ranges):
        logger.debug(f"[FCP] Fetching from Vision API range {range_idx + 1}/{len(missing_ranges)}: {miss_start} to {miss_end}")

        range_df = fetch_from_vision_func(symbol, miss_start, miss_end, interval)

        if not range_df.empty:
            # Add source info
            if include_source_info and "_data_source" not in range_df.columns:
                range_df["_data_source"] = "VISION"

            # If we already have data, merge with the new data
            if not result_df.empty:
                logger.debug(f"[FCP] Merging {len(range_df)} Vision records with existing {len(result_df)} records")
                result_df = merge_dataframes([result_df, range_df])
            else:
                # Otherwise just use the Vision data
                result_df = range_df

            # Check if Vision API returned all expected records or if there are gaps
            if not result_df.empty:
                # Identify any remaining missing segments from Vision API
                missing_segments = identify_missing_segments(result_df, miss_start, miss_end, interval)

                if missing_segments:
                    logger.debug(f"[FCP] Vision API left {len(missing_segments)} missing segments")
                    remaining_ranges.extend(missing_segments)
                else:
                    logger.debug("[FCP] Vision API provided complete coverage for this range")
        else:
            # Vision API returned no data for this range
            logger.debug("[FCP] Vision API returned no data for range")
            remaining_ranges.append((miss_start, miss_end))

    # Update missing_ranges to only include what's still missing after Vision API
    if remaining_ranges:
        # Merge adjacent or overlapping ranges
        updated_missing_ranges = merge_adjacent_ranges(remaining_ranges, interval)
        logger.debug(f"[FCP] After Vision API, still have {len(updated_missing_ranges)} missing ranges")
    else:
        updated_missing_ranges = []
        logger.debug("[FCP] No missing ranges after Vision API")

    return result_df, updated_missing_ranges


def process_rest_step(
    fetch_from_rest_func,
    symbol: str,
    missing_ranges: list[tuple[datetime, datetime]],
    interval: Interval,
    include_source_info: bool,
    result_df: pd.DataFrame,
    save_to_cache_func=None,
) -> pd.DataFrame:
    """Process the REST API step (Step 3) of the FCP mechanism.

    Args:
        fetch_from_rest_func: Function to fetch data from REST API
        symbol: Symbol to retrieve data for
        missing_ranges: List of missing time ranges
        interval: Interval for data points
        include_source_info: Whether to include source info in the DataFrame
        result_df: Existing results DataFrame to merge with
        save_to_cache_func: Function to save data to cache (optional)

    Returns:
        Updated result DataFrame
    """
    logger.info(f"[FCP] STEP 3: Using REST API for {len(missing_ranges)} remaining missing ranges")

    # Merge adjacent ranges to minimize API calls
    merged_rest_ranges = merge_adjacent_ranges(missing_ranges, interval)

    for range_idx, (miss_start, miss_end) in enumerate(merged_rest_ranges):
        logger.debug(f"[FCP] Fetching from REST API range {range_idx + 1}/{len(merged_rest_ranges)}: {miss_start} to {miss_end}")

        rest_df = fetch_from_rest_func(symbol, miss_start, miss_end, interval)

        if not rest_df.empty:
            # Add source info
            if include_source_info and "_data_source" not in rest_df.columns:
                rest_df["_data_source"] = "REST"

            # If we already have data, merge with the new data
            if not result_df.empty:
                logger.debug(f"[FCP] Merging {len(rest_df)} REST records with existing {len(result_df)} records")
                result_df = merge_dataframes([result_df, rest_df])
            else:
                # Otherwise just use the REST data
                result_df = rest_df

            # Save to cache if enabled
            if save_to_cache_func:
                logger.debug("[FCP] Auto-saving REST data to cache")
                save_to_cache_func(rest_df, symbol, interval, source="REST")

    return result_df


def verify_final_data(
    result_df: pd.DataFrame,
    aligned_start: datetime,
    aligned_end: datetime,
) -> None:
    """Verify final data and log any incomplete data warnings.

    For historical data (>24 hours old), there should be NO validation warnings
    as long as we have some data. The system correctly aligns user timestamps
    to interval boundaries, and data sources return available data for those
    boundaries.

    Args:
        result_df: Result DataFrame to verify
        aligned_start: Aligned start time of requested range
        aligned_end: Aligned end time of requested range

    Raises:
        RuntimeError: If result_df is empty
    """
    if result_df.empty:
        logger.critical("[FCP] CRITICAL ERROR: No data available from any source")
        raise RuntimeError("All data sources failed. Unable to retrieve data for the requested time range.")

    # Extract time range from actual data
    if "open_time" in result_df.columns:
        min_time = result_df["open_time"].min()
        max_time = result_df["open_time"].max()
    elif hasattr(result_df, "index") and hasattr(result_df.index, "name") and result_df.index.name == "open_time":
        min_time = result_df.index.min()
        max_time = result_df.index.max()
    else:
        # If open_time is not available, log a warning and return without validation
        logger.warning("[FCP] Cannot verify time range: open_time not found in columns or index")
        return

    # Determine if this is historical data (more than 24 hours old)
    now = datetime.now(timezone.utc)
    data_age_hours = (now - max_time).total_seconds() / 3600
    is_historical_data = data_age_hours > 24

    logger.debug(f"[FCP] Final result spans from {min_time} to {max_time} with {len(result_df)} records")
    logger.debug(f"[FCP] Data age: {data_age_hours:.1f} hours, is_historical: {is_historical_data}")

    # CORRECTED VALIDATION LOGIC:
    # For historical data, if we have data, it should be considered successful.
    # The system correctly aligned timestamps and fetched available data.
    # Any "missing" data compared to theoretical boundaries is normal.

    if is_historical_data:
        # Historical data - just log success
        time_span = max_time - min_time
        hours_covered = time_span.total_seconds() / 3600
        logger.info(
            f"[FCP] Historical data retrieved successfully: {len(result_df)} records "
            f"covering {hours_covered:.1f} hours from {min_time} to {max_time}"
        )
        return

    # ONLY for recent data (< 24 hours old), check for potential gaps
    # that might indicate real data availability issues

    # Calculate theoretical range vs actual range
    total_range_seconds = (aligned_end - aligned_start).total_seconds()

    # Check if we got reasonable coverage for recent data
    missing_start_seconds = (min_time - aligned_start).total_seconds() if min_time > aligned_start else 0
    missing_end_seconds = (aligned_end - max_time).total_seconds() if max_time < aligned_end else 0
    total_missing_seconds = missing_start_seconds + missing_end_seconds
    missing_percentage = (total_missing_seconds / total_range_seconds) * 100 if total_range_seconds > 0 else 0

    # For recent data, only warn about significant gaps that might indicate real issues
    if missing_percentage > 10.0:  # Only warn if more than 10% missing for recent data
        logger.warning(
            f"[FCP] Recent data coverage concern: {100 - missing_percentage:.1f}% complete. "
            f"This might indicate data availability issues for recent time periods."
        )

        # Log specific gaps only for recent data with significant issues
        if missing_start_seconds > 0 and missing_start_seconds > 300:  # > 5 minutes
            logger.warning(f"[FCP] Significant gap at start: {aligned_start} to {min_time}")
        if missing_end_seconds > 0 and missing_end_seconds > 300:  # > 5 minutes
            logger.warning(f"[FCP] Significant gap at end: {max_time} to {aligned_end}")
    else:
        # Recent data with good coverage
        logger.info(
            f"[FCP] Recent data coverage: {100 - missing_percentage:.1f}% complete ({len(result_df)} records from {min_time} to {max_time})"
        )


def handle_error(e: Exception) -> None:
    """Handle errors with improved error handling.

    Args:
        e: Exception to handle

    Raises:
        DataNotAvailableError: Re-raised directly for fail-loud behavior (GitHub Issue #10)
        RuntimeError: For other errors, re-raises with sanitized error message
    """
    # Import here to avoid circular imports
    from data_source_manager.utils.for_core.vision_exceptions import DataNotAvailableError

    # DataNotAvailableError should be re-raised directly for fail-loud behavior
    # This allows callers to catch and handle this specific exception
    if isinstance(e, DataNotAvailableError):
        raise e

    safe_error_message = ""
    try:
        # Sanitize error message to prevent binary data from causing rich formatting issues
        error_message = str(e)
        # Replace any non-printable characters
        safe_error_message = "".join(c if c.isprintable() else f"\\x{ord(c):02x}" for c in error_message)

        logger.critical(f"Error in get_data: {safe_error_message}")
        logger.critical(f"Error type: {type(e).__name__}")

        # More controlled traceback handling
        tb_string = traceback.format_exc()
        # Sanitize the traceback
        safe_tb = "".join(c if c.isprintable() else f"\\x{ord(c):02x}" for c in tb_string)
        tb_lines = safe_tb.splitlines()

        logger.critical("Traceback summary:")
        for line in tb_lines[:3]:
            logger.critical(line)
        logger.critical("...")
        for line in tb_lines[-3:]:
            logger.critical(line)
    except (ValueError, TypeError, AttributeError, UnicodeDecodeError) as nested_error:
        # If even our error handling fails, log a simpler message
        logger.critical(f"Critical error in get_data: {type(e).__name__}")
        logger.critical(f"Error handling also failed: {type(nested_error).__name__}")

    # Re-raise the exception to properly exit with error
    if "All data sources failed" in str(e):
        raise RuntimeError("All data sources failed. Unable to retrieve data for the requested time range.")
    raise RuntimeError(f"Failed to retrieve data from all sources: {safe_error_message}")
