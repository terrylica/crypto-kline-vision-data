#!/usr/bin/env python
"""Time utilities for handling time alignment and boundaries in Binance API requests.

This module centralizes all time-related functionality, providing a single source of truth for:
1. Time zone conversion and normalization
2. Interval calculations and manipulations
3. Time boundary alignment for API requests
4. Time window validation

The module combines functionality previously scattered across time_alignment.py and
api_boundary_validator.py to ensure consistent behavior throughout the application.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any
import re

import pandas as pd

from utils.market_constraints import Interval as MarketInterval
from utils.deprecation_rules import TimeUnit
from utils.logger_setup import logger

# Configure module logger


def enforce_utc_timezone(dt: datetime) -> datetime:
    """Ensure datetime is in UTC timezone.

    Args:
        dt: Input datetime, potentially with or without timezone

    Returns:
        Datetime object guaranteed to have UTC timezone
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    # Create a new object to ensure we don't return the same instance
    if dt.tzinfo == timezone.utc:
        return datetime(
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            dt.microsecond,
            tzinfo=timezone.utc,
        )
    return dt.astimezone(timezone.utc)


def validate_time_window(start_time: datetime, end_time: datetime) -> None:
    """Validate the time window for an API request.

    Args:
        start_time: Start time for data retrieval
        end_time: End time for data retrieval

    Raises:
        ValueError: If start_time is after end_time or time window is invalid
    """
    # Ensure datetimes are timezone aware and in UTC
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

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


def get_interval_micros(interval: MarketInterval) -> int:
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

    # Map market interval units to TimeUnit units
    unit_mapping = {
        "s": "s",  # seconds
        "m": "min",  # minutes
        "h": "h",  # hours
        "d": "D",  # days
        "w": "W",  # weeks
        "M": "M",  # months
    }

    if unit_symbol not in unit_mapping:
        raise ValueError(f"Unsupported interval unit: {unit_symbol}")

    time_unit_symbol = unit_mapping[unit_symbol]

    # Find matching TimeUnit
    unit = next(
        (u for u in TimeUnit.get_all_units() if u.value == time_unit_symbol), None
    )
    if unit is None:
        raise ValueError(f"Unknown TimeUnit symbol: {time_unit_symbol}")

    return value * unit.micros


def get_interval_seconds(interval: MarketInterval) -> int:
    """Get interval duration in seconds.

    Args:
        interval: The interval to convert

    Returns:
        Number of seconds in the interval
    """
    return get_interval_micros(interval) // 1_000_000


def get_interval_timedelta(interval: MarketInterval) -> timedelta:
    """Convert interval to timedelta.

    Args:
        interval: The interval specification

    Returns:
        timedelta: Interval duration
    """
    return timedelta(microseconds=get_interval_micros(interval))


def get_smaller_units(interval: MarketInterval) -> List[TimeUnit]:
    """Get all units smaller than this interval.

    Args:
        interval: The interval specification

    Returns:
        List[TimeUnit]: Units smaller than the interval
    """
    interval_micros = get_interval_micros(interval)
    return [unit for unit in TimeUnit.get_all_units() if unit.micros < interval_micros]


def get_interval_floor(timestamp: datetime, interval: MarketInterval) -> datetime:
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


def get_interval_ceiling(timestamp: datetime, interval: MarketInterval) -> datetime:
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


def get_bar_close_time(open_time: datetime, interval: MarketInterval) -> datetime:
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


def is_bar_complete(
    timestamp: datetime,
    interval: MarketInterval,
    current_time: Optional[datetime] = None,
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


def filter_dataframe_by_time(
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
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

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


def align_time_boundaries(
    start_time: datetime, end_time: datetime, interval: MarketInterval
) -> Tuple[datetime, datetime]:
    """Align time boundaries according to Binance REST API behavior.

    This method implements the exact boundary alignment behavior of the Binance REST API:
    - startTime: Rounds UP to the next interval boundary if not exactly on a boundary
    - endTime: Rounds DOWN to the previous interval boundary if not exactly on a boundary

    Args:
        start_time: User-provided start time
        end_time: User-provided end time
        interval: Data interval

    Returns:
        Tuple of (aligned_start_time, aligned_end_time) mimicking Binance API behavior
    """
    logger.info(
        f"Aligning time boundaries: {start_time} -> {end_time} for interval {interval}"
    )

    # Ensure timezone awareness
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    # Get interval in microseconds for precise calculations
    interval_microseconds = get_interval_micros(interval)

    # Extract microseconds since epoch for calculations
    start_microseconds = int(start_time.timestamp() * 1_000_000)
    end_microseconds = int(end_time.timestamp() * 1_000_000)

    # Calculate floor of each timestamp to interval boundary
    start_floor = start_microseconds - (start_microseconds % interval_microseconds)
    end_floor = end_microseconds - (end_microseconds % interval_microseconds)

    # Apply Binance API boundary rules:
    # - startTime: Round UP to next interval boundary if not exactly on boundary
    # - endTime: Round DOWN to previous interval boundary if not exactly on boundary
    aligned_start_microseconds = (
        start_floor
        if start_microseconds == start_floor
        else start_floor + interval_microseconds
    )
    aligned_end_microseconds = end_floor

    # Convert back to datetime
    aligned_start = datetime.fromtimestamp(
        aligned_start_microseconds / 1_000_000, tz=timezone.utc
    )
    aligned_end = datetime.fromtimestamp(
        aligned_end_microseconds / 1_000_000, tz=timezone.utc
    )

    logger.info(
        f"Aligned boundaries: {aligned_start} -> {aligned_end} for interval {interval}"
    )

    return aligned_start, aligned_end


def estimate_record_count(
    start_time: datetime, end_time: datetime, interval: MarketInterval
) -> int:
    """Estimate number of records between two timestamps for a given interval.

    The Binance API uses specific boundary treatment:
    1. For exact boundaries: inclusive-inclusive (returns both start and end timestamps)
    2. For timestamps with milliseconds:
       - startTime: rounds UP to next interval boundary
       - endTime: rounds DOWN to previous interval boundary

    Args:
        start_time: Start time
        end_time: End time
        interval: Data interval

    Returns:
        Estimated number of records
    """
    # Ensure timezone awareness
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    # Check if we're dealing with future dates (no data yet)
    now = datetime.now(timezone.utc)
    if start_time > now:
        return 0  # No records for future dates

    # Limit end_time to current time as future data isn't available
    if end_time > now:
        end_time = now

    # Get aligned boundaries
    aligned_start, aligned_end = align_time_boundaries(start_time, end_time, interval)

    # If end is before start after alignment, return 0
    if aligned_end < aligned_start:
        return 0

    # Calculate interval in seconds
    interval_secs = get_interval_seconds(interval)

    # The formula accounts for inclusive-inclusive behavior:
    # For a 1-second interval from 05:23:20 to 05:23:30:
    # (30 - 20) / 1 + 1 = 11 records
    # This formula works for all cases when boundaries are properly aligned
    time_diff_secs = int((aligned_end - aligned_start).total_seconds())
    record_count = (time_diff_secs // interval_secs) + 1

    return record_count


def vision_api_time_window_alignment(
    start_time: datetime,
    end_time: datetime,
    interval: MarketInterval,
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
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    # Get floor times - always floor the start and end time to match REST API behavior
    start_floor = get_interval_floor(start_time, interval)
    end_floor = get_interval_floor(end_time, interval)

    # For start time: ALWAYS use floor time (inclusive)
    adjusted_start = start_floor

    # For end time: Use floor time (exclusive boundary)
    adjusted_end = end_floor

    return adjusted_start, adjusted_end


def align_vision_api_to_rest(
    start_time: datetime, end_time: datetime, interval: MarketInterval
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
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

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
