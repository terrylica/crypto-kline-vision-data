#!/usr/bin/env python
"""Utility module for handling time alignment and incomplete bars.

IMPORTANT: This module has been refactored to remove manual time alignment for REST API calls
while keeping necessary alignment for Vision API and cache to match REST API behavior.

Key behaviors:
1. REST API: No manual alignment is performed - timestamps are passed directly to the API
2. Vision API & Cache: Manual alignment is applied to match REST API's natural boundary behavior

For Vision API and Cache (manual alignment to match REST API behavior):
1. All units smaller than the interval are removed (e.g., for 1m, all seconds and microseconds are removed)
2. Start times are rounded DOWN to include the full interval
   (e.g., 08:37:25.528448 gets rounded DOWN to 08:37:25.000000 for 1-second intervals)
3. End times are rounded DOWN to current interval boundary
   (e.g., 08:37:30.056345 gets rounded DOWN to 08:37:30.000000 for 1-second intervals)
4. Start timestamp is inclusive, end timestamp is exclusive after alignment

These alignment utilities should NOT be used for REST API calls, but are retained for:
1. Vision API alignment (to match REST API behavior)
2. Cache key generation (to match REST API behavior)
3. General time utility functions not related to alignment
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any
import re
import pandas as pd
import numpy as np
import logging

from utils.logger_setup import get_logger
from utils.market_constraints import Interval

# Import the canonical TimeUnit implementation to highlight we should use this one instead
from utils.deprecation_rules import TimeUnit as DeprecationTimeUnit

# Ensure we use a consistent logger with INFO level
logger = get_logger(__name__, "INFO", show_path=False)


# DEPRECATION WARNING: This TimeUnit implementation is deprecated
# Use utils.deprecation_rules.TimeUnit instead
@dataclass(frozen=True)
class TimeUnit:
    """Represents a time unit with conversion to microseconds.

    DEPRECATED: Use utils.deprecation_rules.TimeUnit instead
    """

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
            cls.MICRO(),
            cls.MILLI(),
            cls.SECOND(),
            cls.MINUTE(),
            cls.HOUR(),
            cls.DAY(),
            cls.WEEK(),
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
        current_time: Optional current time for testing

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
    def enforce_utc_timezone(dt: datetime) -> datetime:
        """Ensures datetime object is timezone aware and in UTC."""
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def align_vision_api_to_rest(
        start_time: datetime, end_time: datetime, interval: Interval
    ) -> Dict[str, Any]:
        """Align Vision API time boundaries to match REST API behavior.

        This method should be used to manually align Vision API and cache operations
        to match REST API's natural boundary behavior.

        Args:
            start_time: Start time
            end_time: End time
            interval: The interval specification

        Returns:
            Dictionary with aligned start and end times
        """
        # Apply alignment to match REST API behavior
        adjusted_start, adjusted_end = _vision_api_time_window_alignment(
            start_time, end_time, interval
        )

        # Calculate other useful properties
        interval_seconds = interval.to_seconds()
        interval_ms = interval_seconds * 1000
        interval_micros = interval_seconds * 1_000_000

        # Calculate timestamps for API calls
        start_ms = int(adjusted_start.timestamp() * 1000)
        end_ms = int(adjusted_end.timestamp() * 1000)

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

    @staticmethod
    def filter_dataframe(
        df: pd.DataFrame, start_time: datetime, end_time: datetime
    ) -> pd.DataFrame:
        """Filter DataFrame by time range with boundary handling.

        This method applies consistent filtering based on the boundary definitions:
        - start_time: inclusive
        - end_time: exclusive

        Args:
            df: Input DataFrame with DatetimeIndex
            start_time: Start time (inclusive)
            end_time: End time (exclusive)

        Returns:
            Filtered DataFrame
        """
        if df.empty:
            return df

        # Ensure proper timezone
        start_time = TimeRangeManager.enforce_utc_timezone(start_time)
        end_time = TimeRangeManager.enforce_utc_timezone(end_time)

        # Apply filtering with explicit boundary handling
        filtered_df = df.loc[(df.index >= start_time) & (df.index < end_time)]

        return filtered_df

    # DEPRECATED: Do not use for REST API - kept for backward compatibility only
    @staticmethod
    def get_adjusted_boundaries(
        start_time: datetime, end_time: datetime, interval: Interval
    ) -> Dict[str, datetime]:
        """DEPRECATED: Do not use for REST API calls.

        Use align_vision_api_to_rest for Vision API and cache alignment instead.
        This method will be removed in a future version.

        Args:
            start_time: Start time
            end_time: End time
            interval: The interval specification

        Returns:
            Dictionary with adjusted start and end times
        """
        logger.warning(
            "get_adjusted_boundaries() is deprecated. "
            "For REST API: Do not manually align timestamps. "
            "For Vision API/cache: Use align_vision_api_to_rest()."
        )

        adjusted_start, adjusted_end = _vision_api_time_window_alignment(
            start_time, end_time, interval
        )

        return {
            "adjusted_start": adjusted_start,
            "adjusted_end": adjusted_end,
        }

    # DEPRECATED: Do not use for REST API - kept for backward compatibility only
    @staticmethod
    def get_time_boundaries(
        start_time: datetime, end_time: datetime, interval: Interval
    ) -> Dict[str, Any]:
        """DEPRECATED: Do not use for REST API calls.

        Use align_vision_api_to_rest for Vision API and cache alignment instead.
        This method will be removed in a future version.

        Args:
            start_time: Start time
            end_time: End time
            interval: The interval specification

        Returns:
            Dictionary with time boundary information
        """
        logger.warning(
            "get_time_boundaries() is deprecated. "
            "For REST API: Do not manually align timestamps. "
            "For Vision API/cache: Use align_vision_api_to_rest()."
        )

        return TimeRangeManager.align_vision_api_to_rest(start_time, end_time, interval)
