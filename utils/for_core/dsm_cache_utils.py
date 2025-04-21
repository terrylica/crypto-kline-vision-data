#!/usr/bin/env python
"""Utility functions for DataSourceManager cache operations."""

from datetime import datetime, timezone, timedelta
from typing import List, Tuple
import pandas as pd

from utils.logger_setup import logger
from utils.market_constraints import Interval
from utils.time_utils import filter_dataframe_by_time, align_time_boundaries
from utils.config import create_empty_dataframe, FEATURE_FLAGS


def save_to_cache(
    df: pd.DataFrame,
    symbol: str,
    interval: Interval,
    cache_manager,
    provider: str,
    chart_type: str,
    market_type: str,
    source: str = None,
) -> None:
    """Save data to cache.

    Args:
        df: DataFrame to cache
        symbol: Symbol the data is for
        interval: Time interval of the data
        cache_manager: UnifiedCacheManager instance
        provider: Data provider name
        chart_type: Chart type name
        market_type: Market type string
        source: Data source (VISION, REST, etc.) - used to prioritize Vision API data for caching

    Note:
        Following the FCP mechanism requirements, Vision data is delivered in daily packs.
        When Vision data is requested for any part of a day, the entire day's data is
        downloaded and cached. This ensures complete daily data availability in the cache
        regardless of the specific time range requested.
    """
    if cache_manager is None:
        logger.debug("Cache manager is None - skipping cache save")
        return

    if df.empty:
        logger.error(f"Empty DataFrame for {symbol} - skipping cache save")
        return

    # Enhanced debug info about incoming data
    logger.debug(f"save_to_cache called for {symbol} with {len(df)} records")
    logger.debug(f"DataFrame columns: {list(df.columns)}")
    logger.debug(f"DataFrame dtypes: {df.dtypes}")

    try:
        # Ensure data is sorted by open_time before caching to prevent unsorted cache entries
        if "open_time" in df.columns and not df["open_time"].is_monotonic_increasing:
            logger.debug(f"Sorting data by open_time before caching for {symbol}")
            df = df.sort_values("open_time").reset_index(drop=True)

        # Group data by date
        if "open_time" not in df.columns:
            logger.error(f"DataFrame missing open_time column: {list(df.columns)}")
            return

        logger.debug(f"Creating date column from open_time for grouping")
        df["date"] = df["open_time"].dt.date

        logger.debug(f"Grouping {len(df)} records by date")
        date_groups = df.groupby("date")
        logger.debug(f"Found {len(date_groups)} date groups")

        for date, group in date_groups:
            logger.debug(f"Processing group for date {date} with {len(group)} records")
            # Remove the date column
            group = group.drop(columns=["date"])

            # Convert date to datetime at midnight
            cache_date = datetime.combine(date, datetime.min.time()).replace(
                tzinfo=timezone.utc
            )

            logger.debug(
                f"Saving {len(group)} records for {symbol} on {cache_date.date()} to cache"
            )

            # Always save data directly to Arrow cache
            success = cache_manager.save_to_cache(
                df=group,
                symbol=symbol,
                interval=interval.value,
                date=cache_date,
                provider=provider,
                chart_type=chart_type,
                market_type=market_type,
            )

            if success:
                logger.debug(
                    f"Successfully saved cache data for {symbol} on {cache_date.date()}"
                )
            else:
                logger.warning(
                    f"Failed to save cache data for {symbol} on {cache_date.date()}"
                )
    except Exception as e:
        logger.error(f"Error in save_to_cache: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")


def get_from_cache(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
    cache_manager,
    provider: str,
    chart_type: str,
    market_type: str,
) -> Tuple[pd.DataFrame, List[Tuple[datetime, datetime]]]:
    """Retrieve data from cache and identify missing ranges.

    Args:
        symbol: Symbol to retrieve data for
        start_time: Start time for data retrieval
        end_time: End time for data retrieval
        interval: Time interval between data points
        cache_manager: UnifiedCacheManager instance
        provider: Data provider name
        chart_type: Chart type name
        market_type: Market type string

    Returns:
        Tuple of (cached DataFrame, list of missing date ranges)
    """
    if cache_manager is None:
        # Return empty DataFrame and the entire date range as missing
        logger.debug(
            f"Cache manager is None. Returning entire range as missing: {start_time} to {end_time}"
        )
        return create_empty_dataframe(), [(start_time, end_time)]

    # Align time boundaries
    aligned_start, aligned_end = align_time_boundaries(start_time, end_time, interval)

    logger.debug(
        f"[FCP] Cache retrieval with aligned boundaries: {aligned_start} to {aligned_end}"
    )

    # Generate list of dates in the range
    dates = []
    current_date = aligned_start.replace(hour=0, minute=0, second=0, microsecond=0)
    while current_date <= aligned_end:
        dates.append(current_date)
        current_date += timedelta(days=1)

    logger.debug(
        f"[FCP] Checking cache for {len(dates)} dates from {dates[0].date()} to {dates[-1].date()}"
    )

    # Try to load each date from cache
    cached_dfs = []
    missing_ranges = []
    last_missing_start = None
    incomplete_days = []
    all_empty = True

    for date in dates:
        df = cache_manager.load_from_cache(
            symbol=symbol,
            interval=interval.value,
            date=date,
            provider=provider,
            chart_type=chart_type,
            market_type=market_type,
        )

        if df is not None and not df.empty:
            all_empty = False
            # Add source information
            df["_data_source"] = "CACHE"

            # Check if this day has complete data (1440 minutes for a full day)
            expected_records = 1440  # Full day of 1-minute data
            if len(df) < expected_records:
                incomplete_days.append((date, len(df)))
                logger.debug(
                    f"[FCP] Day {date.date()} has incomplete data: {len(df)}/{expected_records} records"
                )
            else:
                logger.debug(
                    f"[FCP] Loaded {len(df)} records from cache for {date.date()}"
                )

            cached_dfs.append(df)
            # If we were tracking a missing range, close it
            if last_missing_start is not None:
                missing_end = date - timedelta(microseconds=1)
                missing_ranges.append((last_missing_start, missing_end))
                logger.debug(
                    f"[FCP] Identified missing range: {last_missing_start} to {missing_end}"
                )
                last_missing_start = None
        else:
            # Start tracking a missing range if we haven't already
            if last_missing_start is None:
                last_missing_start = date
                logger.debug(f"[FCP] Started tracking missing range from {date.date()}")

    # Close any open missing range
    if last_missing_start is not None:
        missing_end = aligned_end
        missing_ranges.append((last_missing_start, missing_end))
        logger.debug(
            f"[FCP] Closing final missing range: {last_missing_start} to {missing_end}"
        )

    # If we have no cached data, return empty DataFrame and the entire range as missing
    if all_empty or not cached_dfs:
        logger.debug(
            f"[FCP] No cached data found for entire range. Missing: {aligned_start} to {aligned_end}"
        )
        return create_empty_dataframe(), [(aligned_start, aligned_end)]

    # Combine cached DataFrames
    combined_df = pd.concat(cached_dfs, ignore_index=True)
    logger.debug(
        f"[FCP] Combined {len(cached_dfs)} cache dataframes with total {len(combined_df)} records"
    )

    # Remove duplicates and sort by open_time
    if not combined_df.empty:
        combined_df = combined_df.drop_duplicates(subset=["open_time"])
        combined_df = combined_df.sort_values("open_time").reset_index(drop=True)
        logger.debug(
            f"[FCP] After deduplication: {len(combined_df)} records from cache"
        )

        # Filter to requested time range
        before_filter_len = len(combined_df)
        combined_df = filter_dataframe_by_time(
            combined_df, aligned_start, aligned_end, "open_time"
        )
        logger.debug(
            f"[FCP] After time filtering: {len(combined_df)} records (removed {before_filter_len - len(combined_df)})"
        )

        # Check time bounds of the filtered data
        if not combined_df.empty:
            min_time = combined_df["open_time"].min()
            max_time = combined_df["open_time"].max()
            logger.debug(f"[FCP] Cache data spans from {min_time} to {max_time}")

            # Check for gaps at the beginning or end of the range
            if min_time > aligned_start:
                logger.debug(
                    f"[FCP] Missing data at beginning: {aligned_start} to {min_time}"
                )
                missing_ranges.append((aligned_start, min_time - timedelta(seconds=1)))

            if max_time < aligned_end:
                # This is the critical fix - detect missing data at the end!
                logger.debug(f"[FCP] Missing data at end: {max_time} to {aligned_end}")
                missing_ranges.append((max_time + timedelta(minutes=1), aligned_end))

            # Now check for incomplete days and add them to missing ranges
            # This ensures that days with just a few records get fully refreshed
            for date, record_count in incomplete_days:
                # If this day is within our aligned range and significantly incomplete
                if (
                    date.date() >= aligned_start.date()
                    and date.date() <= aligned_end.date()
                    and record_count < 1440 * 0.9
                ):  # If less than 90% complete

                    # Create a range for this day
                    day_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
                    day_end = date.replace(
                        hour=23, minute=59, second=59, microsecond=999999
                    )

                    # If this is the first day, adjust start time
                    if day_start.date() == aligned_start.date():
                        day_start = aligned_start

                    # If this is the last day, adjust end time
                    if day_end.date() == aligned_end.date():
                        day_end = aligned_end

                    # @critical_optimization: TEST_CASE_ID:CACHE-OPT-001
                    # BUSINESS REQUIREMENT: This block prevents wasteful API calls
                    # DO NOT REMOVE OR MODIFY without updating unit tests.
                    # This ensures partial days aren't refetched when they already contain all required data.
                    if FEATURE_FLAGS.get("OPTIMIZE_CACHE_PARTIAL_DAYS", True):
                        day_data = filter_dataframe_by_time(
                            combined_df, day_start, day_end, "open_time"
                        )

                        # Calculate expected records for this time range based on interval
                        expected_intervals = (
                            int(
                                (day_end - day_start).total_seconds()
                                / interval.to_seconds()
                            )
                            + 1
                        )

                        # Only add to missing ranges if we have actual gaps
                        if len(day_data) < expected_intervals:
                            logger.debug(
                                f"[FCP] Adding incomplete day to missing ranges: {day_start} to {day_end} "
                                f"(missing {expected_intervals - len(day_data)} out of {expected_intervals} expected records)"
                            )
                            missing_ranges.append((day_start, day_end))
                        else:
                            logger.debug(
                                f"[FCP] Day {date.date()} has all {len(day_data)}/{expected_intervals} records needed "
                                f"for requested time range - no need to refetch"
                            )
                    else:
                        # Legacy behavior - always refetch incomplete days
                        logger.debug(
                            f"[FCP] Using legacy behavior (OPTIMIZE_CACHE_PARTIAL_DAYS=False): "
                            f"Refetching incomplete day {date.date()} with {record_count}/1440 records"
                        )
                        missing_ranges.append((day_start, day_end))

    # Merge overlapping or adjacent ranges
    if missing_ranges:
        from utils.for_core.dsm_time_range_utils import merge_adjacent_ranges

        merged_ranges = merge_adjacent_ranges(missing_ranges, interval)
        logger.debug(
            f"[FCP] Merged {len(missing_ranges)} missing ranges into {len(merged_ranges)} ranges"
        )
        missing_ranges = merged_ranges

    # Log the missing ranges in detail
    if missing_ranges:
        for i, (miss_start, miss_end) in enumerate(missing_ranges):
            logger.debug(
                f"[FCP] Missing range {i+1}/{len(missing_ranges)}: {miss_start} to {miss_end}"
            )

    return combined_df, missing_ranges
