#!/usr/bin/env python
"""Utility module for handling time alignment and incomplete bars.

Key behaviors:
1. All units smaller than the interval are removed (e.g., for 1m, all seconds and microseconds are removed)
2. The current incomplete interval is removed for safety
3. Start times are ALWAYS rounded DOWN to include the full interval
   (e.g., 08:37:25.528448 gets rounded DOWN to 08:37:25.000000 for 1-second intervals)
4. End times are rounded DOWN to current interval boundary and are treated as EXCLUSIVE
   (e.g., 08:37:30.056345 gets rounded DOWN to 08:37:30.000000 for 1-second intervals,
   and data is returned up to but NOT including 08:37:30.000000)
5. Start timestamp is inclusive, end timestamp is exclusive
6. This ensures consistent behavior regardless of microsecond precision

Example:
For a time window from 2025-03-17 08:37:25.528448 to 2025-03-17 08:37:30.056345 with 1-second intervals:
- Adjusted start: 2025-03-17 08:37:25.000000 (rounded DOWN from 25.528448)
- Adjusted end: 2025-03-17 08:37:30.000000 (rounded DOWN from 30.056345)
- Expected records: Records for exactly 5 seconds (25, 26, 27, 28, 29), NOT including 30

Important implementation detail:
- When filtering data after fetching, the comparison `df.index < end_time` is used
  to enforce the exclusive end time boundary

IMPORTANT: When counting expected records for a time range:
1. Start times are INCLUSIVE, end times are EXCLUSIVE after alignment
2. Start times with microseconds are rounded DOWN to include the full interval
3. End times with microseconds are rounded DOWN to the current second and treated as exclusive
4. The number of records is the integer difference in seconds between adjusted start and end
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any
import re
import pandas as pd

from utils.logger_setup import get_logger
from utils.market_constraints import Interval

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


@dataclass(frozen=True)
class TimeUnit:
    """Represents a time unit with conversion to microseconds."""

    name: str
    micros: int
    symbol: str

    @classmethod
    def MICRO(cls) -> "TimeUnit":
        return cls("microsecond", 1, "us")

    @classmethod
    def MILLI(cls) -> "TimeUnit":
        return cls("millisecond", 1_000, "ms")

    @classmethod
    def SECOND(cls) -> "TimeUnit":
        return cls("second", 1_000_000, "s")

    @classmethod
    def MINUTE(cls) -> "TimeUnit":
        return cls("minute", 60 * 1_000_000, "m")

    @classmethod
    def HOUR(cls) -> "TimeUnit":
        return cls("hour", 3600 * 1_000_000, "h")

    @classmethod
    def DAY(cls) -> "TimeUnit":
        return cls("day", 86400 * 1_000_000, "d")

    @classmethod
    def WEEK(cls) -> "TimeUnit":
        return cls("week", 7 * 86400 * 1_000_000, "w")

    @classmethod
    def get_all_units(cls) -> List["TimeUnit"]:
        """Get all available units in descending order of size."""
        return [
            cls.WEEK(),
            cls.DAY(),
            cls.HOUR(),
            cls.MINUTE(),
            cls.SECOND(),
            cls.MILLI(),
            cls.MICRO(),
        ]


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
    logger.debug("\n=== Bar Close Time Calculation ===")
    logger.debug(f"Input open_time: {open_time}")
    logger.debug(f"Input interval: {interval}")

    interval_delta = get_interval_timedelta(interval)
    logger.debug(f"Interval timedelta: {interval_delta}")

    close_time = open_time + interval_delta - timedelta(microseconds=1)
    logger.debug(f"Calculated close_time: {close_time}")
    logger.debug(f"Close time microseconds: {close_time.microsecond}")

    return close_time


def adjust_time_window(
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
    current_time: Optional[datetime] = None,
) -> Tuple[datetime, datetime]:
    """Adjust time window for data retrieval.

    Key behaviors:
    1. All units smaller than the interval are removed
    2. The current incomplete interval is removed for safety
    3. Start times are ALWAYS rounded DOWN to include the full interval
    4. End times are rounded DOWN to the current interval (exclusive - the end interval is not included)
    5. Start timestamp is inclusive, end timestamp is exclusive

    Args:
        start_time: Start time
        end_time: End time
        interval: The interval specification
        current_time: Optional current time for testing

    Returns:
        Tuple of adjusted start and end times
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc)

    # Ensure UTC timezone
    start_time = start_time.astimezone(timezone.utc)
    end_time = end_time.astimezone(timezone.utc)
    current_time = current_time.astimezone(timezone.utc)

    # Get floor times - always floor the start time
    start_floor = get_interval_floor(start_time, interval)
    end_floor = get_interval_floor(end_time, interval)

    # For start time: ALWAYS use floor time
    adjusted_start = start_floor

    # Check if we're in an incomplete interval
    interval_td = get_interval_timedelta(interval)
    time_since_floor = current_time - end_floor
    if time_since_floor < interval_td:
        # We're in an incomplete interval, move back one interval
        end_floor = end_floor - interval_td

    # End time is exclusive (the end interval is not included)
    # So we use the floor time exactly (not the close time of the interval)
    adjusted_end = end_floor

    # Calculate expected number of records
    expected_records = (
        int((adjusted_end - adjusted_start).total_seconds()) // interval.to_seconds()
    )

    # Create detailed debug information
    logger.debug("\n=== Time Window Adjustment Details ===")
    logger.debug(
        f"Original Start: {start_time.isoformat()} (microseconds: {start_time.microsecond})"
    )
    logger.debug(
        f"Original End: {end_time.isoformat()} (microseconds: {end_time.microsecond})"
    )
    logger.debug(f"Floored Start: {start_floor.isoformat()}")
    logger.debug(f"Floored End: {end_floor.isoformat()}")
    logger.debug(f"Interval: {interval.value} ({interval.to_seconds()} seconds)")
    logger.debug(f"Current Time: {current_time.isoformat()}")
    logger.debug(f"Is end interval incomplete? {time_since_floor < interval_td}")
    logger.debug(f"Adjusted Start: {adjusted_start.isoformat()} (INCLUSIVE)")
    logger.debug(f"Adjusted End: {adjusted_end.isoformat()} (EXCLUSIVE)")
    logger.debug(f"Expected Records: {expected_records}")
    logger.debug(
        f"Time Span (seconds): {int((adjusted_end - adjusted_start).total_seconds())}"
    )
    logger.debug(
        f"Boundary Behavior: Start timestamp is INCLUSIVE, end timestamp is EXCLUSIVE"
    )

    # Log adjustments if they were made
    if adjusted_start != start_time or adjusted_end != end_time:
        logger.debug(
            "Time window adjusted:"
            f"\nOriginal:  {start_time.isoformat()} -> {end_time.isoformat()}"
            f"\nAdjusted:  {adjusted_start.isoformat()} -> {adjusted_end.isoformat()}"
        )

    # Calculate and log the expected number of records
    if interval == Interval.SECOND_1:
        # For 1-second intervals, calculate seconds difference (exclusive end)
        seconds_diff = int((adjusted_end - adjusted_start).total_seconds())
        expected_records = seconds_diff  # Exclusive end boundary
        logger.debug(
            f"Expected records with exclusive end boundary: {expected_records}"
        )
        logger.debug(f"Time span in seconds: {seconds_diff} seconds")
        logger.debug(
            f"Boundaries: Start timestamp is INCLUSIVE, end timestamp is EXCLUSIVE"
        )

    return adjusted_start, adjusted_end


def get_time_boundaries(
    start_time: datetime, end_time: datetime, interval: Interval
) -> Dict[str, Any]:
    """Get detailed time boundary information for consistent handling across the codebase.

    Args:
        start_time: Original start time
        end_time: Original end time
        interval: Time interval

    Returns:
        Dictionary with time boundary details for use in multiple contexts:
        - adjusted_start: Adjusted start time (inclusive)
        - adjusted_end: Adjusted end time (exclusive)
        - start_ms: Start time in milliseconds for API calls
        - end_ms: End time in milliseconds for API calls
        - expected_records: Expected number of records
        - interval_ms: Interval in milliseconds
        - interval_micros: Interval in microseconds
    """
    # Apply time window adjustment
    adjusted_start, adjusted_end = adjust_time_window(start_time, end_time, interval)

    # Calculate timestamps in milliseconds (for API calls)
    start_ms = int(adjusted_start.timestamp() * 1000)
    end_ms = int(adjusted_end.timestamp() * 1000)

    # Calculate interval in various units
    interval_seconds = interval.to_seconds()
    interval_ms = interval_seconds * 1000
    interval_micros = interval_seconds * 1_000_000

    # Calculate expected records
    expected_records = (
        int((adjusted_end - adjusted_start).total_seconds()) // interval_seconds
    )

    return {
        "adjusted_start": adjusted_start,
        "adjusted_end": adjusted_end,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "expected_records": expected_records,
        "interval_ms": interval_ms,
        "interval_micros": interval_micros,
        "boundary_type": "inclusive_start_exclusive_end",
    }


def filter_time_range(
    df: pd.DataFrame, start_time: datetime, end_time: datetime
) -> pd.DataFrame:
    """
    Filter a DataFrame to a specific time range.
    Implements inclusive start time (>=) and exclusive end time (<) behavior.

    This function will handle DataFrames with either:
    1. A DatetimeIndex (preferred)
    2. An 'open_time' column containing datetime values

    Args:
        df: DataFrame to filter
        start_time: Inclusive start time
        end_time: Exclusive end time

    Returns:
        Filtered DataFrame
    """
    if df.empty:
        return df

    # Ensure both timestamps are in UTC
    start_time = start_time.astimezone(timezone.utc)
    end_time = end_time.astimezone(timezone.utc)

    # Debug information about DataFrame
    logger.debug(f"DataFrame index type: {type(df.index)}")
    logger.debug(f"DataFrame index dtype: {df.index.dtype}")
    logger.debug(f"DataFrame shape: {df.shape}")

    # Check if index is datetime type
    if pd.api.types.is_datetime64_any_dtype(df.index):
        # Case 1: DataFrame has a datetime index
        logger.debug("Filtering with datetime index")
        mask = (df.index >= start_time) & (df.index < end_time)
        return df[mask]

    # Case 2: Try to use open_time column if it exists
    elif "open_time" in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df["open_time"]):
            logger.debug("Filtering with open_time column")
            mask = (df["open_time"] >= start_time) & (df["open_time"] < end_time)
            return df[mask]
        else:
            logger.warning(
                f"open_time column exists but is not datetime type: {df['open_time'].dtype}"
            )

    # Case 3: No valid datetime index or column found
    logger.warning(
        f"Cannot filter DataFrame: no valid datetime index or open_time column found. "
        f"Index type: {type(df.index)}, columns: {df.columns}"
    )

    # In case of failure, we should return an empty DataFrame of the same structure
    return df.iloc[0:0]


def is_bar_complete(
    bar_time: datetime, current_time: datetime, interval: Interval = Interval.SECOND_1
) -> bool:
    """Check if a bar is complete based on its timestamp.

    A bar is considered complete when:
    1. It's at least one interval old
    2. The bar's close time has passed

    Args:
        bar_time: The bar's open time
        current_time: Current time to compare against
        interval: The interval specification (default: 1 second)

    Returns:
        bool: True if the bar is complete, False otherwise
    """
    # Ensure UTC timezone
    bar_time = bar_time.astimezone(timezone.utc)
    current_time = current_time.astimezone(timezone.utc)

    # Get close time
    close_time = get_bar_close_time(bar_time, interval)

    # Bar is complete if we're past its close time
    return current_time > close_time
