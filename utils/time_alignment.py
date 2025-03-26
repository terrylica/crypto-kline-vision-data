#!/usr/bin/env python
"""Time alignment utilities for Binance API requests.

This module provides utilities for handling time alignment in API requests
and data processing. It includes functions for:

1. Time zone conversion and normalization
2. Validating time windows for API requests
3. Filtering data based on time boundaries
4. Enforcing consistent time formats throughout the application

These utilities ensure consistent data handling across different data sources.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any
import logging
import re

import pandas as pd

from utils.market_constraints import Interval
from utils.deprecation_rules import TimeUnit

# Configure module logger
logger = logging.getLogger(__name__)

# Remove any imported TimeUnit implementation for direct alignment calculation
# All alignment is now handled by ApiBoundaryValidator


def enforce_utc_timezone(dt: datetime) -> datetime:
    """Ensure datetime is in UTC timezone.

    Args:
        dt: Input datetime, potentially with or without timezone

    Returns:
        Datetime object guaranteed to have UTC timezone
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_interval_micros(interval: Interval) -> int:
    """Convert interval to microseconds.

    Args:
        interval: The interval specification

    Returns:
        int: Interval duration in microseconds
    """
    # Parse interval value and unit
    match = re.match(r"(\d+)([a-zA-Z]+)", interval.value)
    if not match:
        raise ValueError(f"Invalid interval format: {interval.value}")

    value, unit_symbol = match.groups()
    value = int(value)

    # Find matching unit
    unit = next((u for u in TimeUnit.get_all_units() if u.symbol == unit_symbol), None)
    if unit is None:
        raise ValueError(f"Unknown unit symbol: {unit_symbol}")

    return value * unit.micros


def get_interval_timedelta(interval: Interval) -> timedelta:
    """Convert interval to timedelta.

    Args:
        interval: The interval specification

    Returns:
        timedelta: Interval duration
    """
    return timedelta(microseconds=get_interval_micros(interval))


def get_smaller_units(interval: Interval) -> List[TimeUnit]:
    """Get all units smaller than this interval.

    Args:
        interval: The interval specification

    Returns:
        List[TimeUnit]: Units smaller than the interval
    """
    interval_micros = get_interval_micros(interval)
    return [unit for unit in TimeUnit.get_all_units() if unit.micros < interval_micros]


def get_interval_floor(timestamp: datetime, interval: Interval) -> datetime:
    """Floor timestamp to interval boundary, removing all smaller units.

    Args:
        timestamp: The timestamp to floor
        interval: The interval specification

    Returns:
        datetime: Floor time with sub-interval units removed
    """
    interval_micros = get_interval_micros(interval)
    timestamp_micros = int(timestamp.timestamp() * 1_000_000)
    floored_micros = (timestamp_micros // interval_micros) * interval_micros
    return datetime.fromtimestamp(floored_micros / 1_000_000, timezone.utc)


def get_interval_ceiling(timestamp: datetime, interval: Interval) -> datetime:
    """Ceil timestamp to next interval boundary.

    Args:
        timestamp: The timestamp to ceiling
        interval: The interval specification

    Returns:
        datetime: Ceiling time (next interval with sub-interval units removed)
    """
    floor = get_interval_floor(timestamp, interval)
    if timestamp == floor:
        return floor
    return floor + get_interval_timedelta(interval)


def get_bar_close_time(open_time: datetime, interval: Interval) -> datetime:
    """Get the close time for a bar given its open time.

    Args:
        open_time: The bar's open time
        interval: The interval specification

    Returns:
        datetime: Close time (interval - 1 microsecond after open time)
    """
    interval_delta = get_interval_timedelta(interval)
    close_time = open_time + interval_delta - timedelta(microseconds=1)
    return close_time


# DEPRECATED: This function should not be used for REST API alignment
# Only kept for Vision API and cache to match REST API behavior
def _vision_api_time_window_alignment(
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
) -> Tuple[datetime, datetime]:
    """Align time window for Vision API to match REST API behavior.

    This is specifically for Vision API and cache alignment to match REST API behavior.
    DO NOT use for REST API calls - pass timestamps directly to REST API.

    Args:
        start_time: Start time
        end_time: End time
        interval: The interval specification

    Returns:
        Tuple of aligned start and end times that match REST API behavior
    """
    # Ensure using exact timezone.utc object using centralized utility
    start_time = TimeRangeManager.enforce_utc_timezone(start_time)
    end_time = TimeRangeManager.enforce_utc_timezone(end_time)

    # Get floor times - always floor the start and end time to match REST API behavior
    start_floor = get_interval_floor(start_time, interval)
    end_floor = get_interval_floor(end_time, interval)

    # For start time: ALWAYS use floor time (inclusive)
    adjusted_start = start_floor

    # For end time: Use floor time (exclusive boundary)
    adjusted_end = end_floor

    return adjusted_start, adjusted_end


# DEPRECATED: Do not use for REST API - kept for backward compatibility
# and will emit a warning if used
def adjust_time_window(
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
    current_time: Optional[datetime] = None,
) -> Tuple[datetime, datetime]:
    """DEPRECATED: Do not use for REST API calls.

    Use _vision_api_time_window_alignment for Vision API and cache instead.
    This function will be removed in a future version.

    Args:
        start_time: Start time
        end_time: End time
        interval: The interval specification
        current_time: Optional current time for testing

    Returns:
        Tuple of adjusted start and end times
    """
    logger.warning(
        "adjust_time_window() is deprecated. "
        "For REST API: Do not manually align timestamps. "
        "For Vision API/cache: Use _vision_api_time_window_alignment()."
    )
    return _vision_api_time_window_alignment(start_time, end_time, interval)


def is_bar_complete(
    timestamp: datetime, interval: Interval, current_time: Optional[datetime] = None
) -> bool:
    """Check if a bar is complete based on the current time.

    Args:
        timestamp: The bar's timestamp
        interval: The interval specification
        current_time: Optional current time for testing or comparison.
                     If None, uses the current UTC time.

    Returns:
        bool: True if the bar is complete
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc)

    # Calculate interval timedelta based on interval seconds
    interval_td = get_interval_timedelta(interval)

    # A bar is complete if current time is at least one interval after its start
    return current_time >= (timestamp + interval_td)


class TimeRangeManager:
    """Centralized manager for handling time ranges and alignment.

    IMPORTANT: Do not use get_time_boundaries() or get_adjusted_boundaries()
    for REST API calls - pass timestamps directly to REST API.
    These methods are kept for Vision API and cache alignment only.
    """

    @staticmethod
    def validate_time_window(start_time: datetime, end_time: datetime) -> None:
        """Validate the time window for an API request.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval

        Raises:
            ValueError: If start_time is after end_time or time window is invalid
        """
        # Ensure datetimes are timezone aware and in UTC
        start_time = TimeRangeManager.enforce_utc_timezone(start_time)
        end_time = TimeRangeManager.enforce_utc_timezone(end_time)

        # Basic validation - start time must be before end time
        if start_time >= end_time:
            raise ValueError(
                f"Start time ({start_time.isoformat()}) must be before end time ({end_time.isoformat()})"
            )

        # Check if time range is within reasonable limits
        time_diff = end_time - start_time
        if time_diff > timedelta(days=365):
            raise ValueError(
                f"Time range too large: {time_diff.days} days. "
                "Consider breaking into smaller requests."
            )

    @staticmethod
    def enforce_utc_timezone(dt: datetime) -> datetime:
        """Ensures datetime object is timezone aware and in UTC."""
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def filter_dataframe(
        df: pd.DataFrame, start_time: datetime, end_time: datetime
    ) -> pd.DataFrame:
        """Filter a dataframe based on time boundaries.

        Args:
            df: Dataframe to filter
            start_time: Start time boundary (inclusive)
            end_time: End time boundary (exclusive)

        Returns:
            Filtered dataframe
        """
        if df.empty:
            return df

        # Assert UTC timezone
        start_time = TimeRangeManager.enforce_utc_timezone(start_time)
        end_time = TimeRangeManager.enforce_utc_timezone(end_time)

        # First check if 'timestamp' or 'open_time' is in columns
        if "timestamp" in df.columns:
            time_col = "timestamp"
            filtered_df = df[(df[time_col] >= start_time) & (df[time_col] < end_time)]
        elif "open_time" in df.columns:
            time_col = "open_time"
            filtered_df = df[(df[time_col] >= start_time) & (df[time_col] < end_time)]
        else:
            # If neither in columns, assume the index is the time
            # This handles cases where 'open_time' is already set as the index
            filtered_df = df[(df.index >= start_time) & (df.index < end_time)]

        return filtered_df

    @staticmethod
    def align_vision_api_to_rest(
        start_time: datetime, end_time: datetime, interval: Interval
    ) -> Dict[str, Any]:
        """Apply alignment to Vision API requests that matches REST API's natural boundary behavior.

        This function should be used ONLY for Vision API requests and cache operations
        to ensure compatibility with REST API behavior.

        Args:
            start_time: Start time for the request
            end_time: End time for the request
            interval: The interval object representing data granularity

        Returns:
            Dictionary containing adjusted start/end times and metadata
        """
        # First, ensure times are in UTC
        start_time = TimeRangeManager.enforce_utc_timezone(start_time)
        end_time = TimeRangeManager.enforce_utc_timezone(end_time)

        # Get interval in microseconds
        interval_micros = get_interval_micros(interval)

        # Round start time DOWN to interval boundary
        start_micros = int(start_time.timestamp() * 1_000_000)
        aligned_start_micros = (start_micros // interval_micros) * interval_micros
        aligned_start = datetime.fromtimestamp(
            aligned_start_micros / 1_000_000, tz=timezone.utc
        )

        # Round end time DOWN to interval boundary
        end_micros = int(end_time.timestamp() * 1_000_000)
        aligned_end_micros = (end_micros // interval_micros) * interval_micros
        aligned_end = datetime.fromtimestamp(
            aligned_end_micros / 1_000_000, tz=timezone.utc
        )

        # If end time was exactly on a boundary, we don't want to exclude it
        # So we only adjust if the original wasn't already aligned
        if end_micros != aligned_end_micros and aligned_end < end_time:
            # Add one interval to include the partial interval at the end
            aligned_end = datetime.fromtimestamp(
                (aligned_end_micros + interval_micros) / 1_000_000, tz=timezone.utc
            )

        # Create result with metadata
        result = {
            "original_start": start_time,
            "original_end": end_time,
            "adjusted_start": aligned_start,
            "adjusted_end": aligned_end,
            "interval": interval,
            "interval_micros": interval_micros,
        }

        return result
