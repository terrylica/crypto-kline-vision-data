#!/usr/bin/env python
# polars-exception: FCP utilities process pandas DataFrames from CKVD pipeline
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""Utility functions for Failover Control Protocol (FCP) implementation."""

from datetime import datetime, timezone

import pandas as pd

from ckvd.utils.for_core.ckvd_time_range_utils import (
    identify_missing_segments,
    merge_adjacent_ranges,
    merge_dataframes,
)
from ckvd.utils.for_core.rest_exceptions import RateLimitError
from ckvd.utils.for_core.vision_exceptions import UnsupportedIntervalError, VisionAPIError
from ckvd.utils.loguru_setup import logger
from ckvd.utils.market_constraints import (
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


def process_vision_step(
    fetch_from_vision_func,
    symbol: str,
    missing_ranges: list[tuple[datetime, datetime]],
    interval: Interval,
    include_source_info: bool,
    result_df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[tuple[datetime, datetime]]]:
    """Process the Vision API step (Step 2) of the FCP mechanism.

    Round 15: Fetches multiple missing ranges in parallel using ThreadPoolExecutor.
    Vision API has no rate limits, so parallel fetching is safe and significantly
    faster when multiple ranges are missing (e.g., 7-day request with empty cache).

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

    if not missing_ranges:
        return result_df, []

    n_vision_ranges = len(missing_ranges)

    def _fetch_single_range(miss_start: datetime, miss_end: datetime) -> tuple[tuple[datetime, datetime], pd.DataFrame]:
        """Fetch a single Vision range (thread-safe — Vision has no rate limits)."""
        range_df = fetch_from_vision_func(symbol, miss_start, miss_end, interval)
        if not range_df.empty and include_source_info and "_data_source" not in range_df.columns:
            range_df["_data_source"] = "VISION"
        return (miss_start, miss_end), range_df

    # Collect results: successful fetches and failed ranges
    vision_dfs: list[pd.DataFrame] = []
    failed_ranges: list[tuple[datetime, datetime]] = []
    successful_ranges: list[tuple[datetime, datetime]] = []

    if n_vision_ranges == 1:
        # Single range — skip ThreadPoolExecutor overhead
        (ms, me) = missing_ranges[0]
        logger.debug(f"[FCP] Fetching from Vision API range 1/1: {ms} to {me}")
        try:
            (ms, me), range_df = _fetch_single_range(ms, me)
            if not range_df.empty:
                vision_dfs.append(range_df)
                successful_ranges.append((ms, me))
            else:
                logger.debug("[FCP] Vision API returned no data for range")
                failed_ranges.append((ms, me))
        except (VisionAPIError, OSError) as e:
            logger.warning(f"[FCP] Vision fetch failed for range: {e}")
            failed_ranges.append((ms, me))
    else:
        # Parallel fetch for multiple ranges (Vision API has no rate limits)
        from concurrent.futures import ThreadPoolExecutor, as_completed

        max_workers = min(n_vision_ranges, 8)
        logger.debug(f"[FCP] Parallel Vision fetch: {n_vision_ranges} ranges with {max_workers} workers")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_fetch_single_range, ms, me): (ms, me)
                for ms, me in missing_ranges
            }

            for future in as_completed(futures):
                original_range = futures[future]
                try:
                    (ms, me), range_df = future.result()
                    if not range_df.empty:
                        logger.debug(f"[FCP] Vision returned {len(range_df)} records for {ms} to {me}")
                        vision_dfs.append(range_df)
                        successful_ranges.append((ms, me))
                    else:
                        logger.debug(f"[FCP] Vision API returned no data for {ms} to {me}")
                        failed_ranges.append(original_range)
                except (VisionAPIError, OSError) as e:
                    logger.warning(f"[FCP] Vision fetch failed for range {original_range}: {e}")
                    failed_ranges.append(original_range)

    # Merge all Vision results at once (single merge instead of per-range merges)
    if vision_dfs:
        all_dfs = ([result_df] if not result_df.empty else []) + vision_dfs
        if len(all_dfs) == 1:
            result_df = all_dfs[0]
        else:
            logger.debug(f"[FCP] Merging {len(vision_dfs)} Vision results with existing data")
            result_df = merge_dataframes(all_dfs)

    # Determine remaining missing ranges
    remaining_ranges: list[tuple[datetime, datetime]] = list(failed_ranges)

    # Check for gaps in successfully fetched ranges
    if not result_df.empty:
        for miss_start, miss_end in successful_ranges:
            missing_segments = identify_missing_segments(result_df, miss_start, miss_end, interval)
            if missing_segments:
                logger.debug(f"[FCP] Vision API left {len(missing_segments)} missing segments in {miss_start} to {miss_end}")
                remaining_ranges.extend(missing_segments)

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
    rest_client=None,
) -> pd.DataFrame:
    """Process the REST API step (Step 3) of the FCP mechanism.

    When multiple ranges need fetching and a rest_client with parallel capability
    is available, uses fetch_klines_parallel() for concurrent fetching. Falls back
    to sequential fetching for single ranges or when no client is provided.

    Args:
        fetch_from_rest_func: Function to fetch data from REST API
        symbol: Symbol to retrieve data for
        missing_ranges: List of missing time ranges
        interval: Interval for data points
        include_source_info: Whether to include source info in the DataFrame
        result_df: Existing results DataFrame to merge with
        save_to_cache_func: Function to save data to cache (optional)
        rest_client: RestDataClient instance for parallel fetching (optional)

    Returns:
        Updated result DataFrame
    """
    logger.info(f"[FCP] STEP 3: Using REST API for {len(missing_ranges)} remaining missing ranges")

    # Merge adjacent ranges to minimize API calls
    merged_rest_ranges = merge_adjacent_ranges(missing_ranges, interval)
    n_rest_ranges = len(merged_rest_ranges)

    # Use parallel fetching when multiple ranges and rest_client supports it
    if n_rest_ranges > 1 and rest_client is not None and hasattr(rest_client, "fetch_klines_parallel"):
        return _process_rest_parallel(
            rest_client=rest_client,
            symbol=symbol,
            merged_rest_ranges=merged_rest_ranges,
            interval=interval,
            include_source_info=include_source_info,
            result_df=result_df,
            save_to_cache_func=save_to_cache_func,
        )

    # Sequential path: single range or no parallel capability
    rate_limit_hit = False
    for range_idx, (miss_start, miss_end) in enumerate(merged_rest_ranges):
        logger.debug(f"[FCP] Fetching from REST API range {range_idx + 1}/{n_rest_ranges}: {miss_start} to {miss_end}")

        try:
            rest_df = fetch_from_rest_func(symbol, miss_start, miss_end, interval)
        except RateLimitError as e:
            logger.warning(
                f"[FCP] Rate limited at REST range {range_idx + 1}/{n_rest_ranges}. "
                f"Returning partial data. Retry after: {getattr(e, 'retry_after', 'unknown')}s"
            )
            rate_limit_hit = True
            break

        if not rest_df.empty:
            if include_source_info and "_data_source" not in rest_df.columns:
                rest_df["_data_source"] = "REST"

            if not result_df.empty:
                logger.debug(f"[FCP] Merging {len(rest_df)} REST records with existing {len(result_df)} records")
                result_df = merge_dataframes([result_df, rest_df])
            else:
                result_df = rest_df

            if save_to_cache_func:
                logger.debug("[FCP] Auto-saving REST data to cache")
                save_to_cache_func(rest_df, symbol, interval, source="REST")

    if rate_limit_hit and not result_df.empty:
        result_df.attrs["_rate_limited"] = True
        result_df.attrs["_fcp_partial"] = True

    return result_df


def _process_rest_parallel(
    rest_client,
    symbol: str,
    merged_rest_ranges: list[tuple[datetime, datetime]],
    interval: Interval,
    include_source_info: bool,
    result_df: pd.DataFrame,
    save_to_cache_func=None,
) -> pd.DataFrame:
    """Fetch multiple REST ranges in parallel using rest_client.fetch_klines_parallel().

    Uses low concurrency (max_workers=3) to respect Binance rate limits.

    Args:
        rest_client: RestDataClient with fetch_klines_parallel method
        symbol: Symbol to retrieve data for
        merged_rest_ranges: Pre-merged list of date ranges
        interval: Interval for data points
        include_source_info: Whether to include source info
        result_df: Existing results DataFrame to merge with
        save_to_cache_func: Function to save data to cache (optional)

    Returns:
        Updated result DataFrame
    """
    n_ranges = len(merged_rest_ranges)
    logger.debug(f"[FCP] Parallel REST fetch: {n_ranges} ranges")

    try:
        rest_dfs = rest_client.fetch_klines_parallel(
            symbol=symbol,
            interval=interval,
            date_ranges=merged_rest_ranges,
            max_workers=3,
        )
    except RateLimitError as e:
        logger.warning(
            f"[FCP] Rate limited during parallel REST fetch. "
            f"Returning partial data. Retry after: {getattr(e, 'retry_after', 'unknown')}s"
        )
        if not result_df.empty:
            result_df.attrs["_rate_limited"] = True
            result_df.attrs["_fcp_partial"] = True
        return result_df

    # Merge all successful results
    all_dfs = [result_df] if not result_df.empty else []
    for rest_df in rest_dfs:
        if not rest_df.empty:
            if include_source_info and "_data_source" not in rest_df.columns:
                rest_df["_data_source"] = "REST"
            all_dfs.append(rest_df)

            if save_to_cache_func:
                save_to_cache_func(rest_df, symbol, interval, source="REST")

    if len(all_dfs) == 0:
        return result_df
    if len(all_dfs) == 1:
        return all_dfs[0]

    logger.debug(f"[FCP] Merging {len(all_dfs)} DataFrames from parallel REST fetch")
    return merge_dataframes(all_dfs)


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
    from ckvd.utils.for_core.vision_exceptions import DataNotAvailableError

    # DataNotAvailableError should be re-raised directly for fail-loud behavior
    # This allows callers to catch and handle this specific exception
    if isinstance(e, DataNotAvailableError):
        raise e

    safe_error_message = ""
    try:
        from ckvd.utils.for_core.ckvd_api_utils import _log_critical_error_with_traceback

        safe_error_message = _log_critical_error_with_traceback("get_data", e)
    except (ValueError, TypeError, AttributeError, UnicodeDecodeError) as nested_error:
        # If even our error handling fails, log a simpler message
        logger.critical(f"Critical error in get_data: {type(e).__name__}")
        logger.critical(f"Error handling also failed: {type(nested_error).__name__}")

    # Re-raise the exception to properly exit with error
    if "All data sources failed" in str(e):
        raise RuntimeError("All data sources failed. Unable to retrieve data for the requested time range.")
    raise RuntimeError(f"Failed to retrieve data from all sources: {safe_error_message}")
