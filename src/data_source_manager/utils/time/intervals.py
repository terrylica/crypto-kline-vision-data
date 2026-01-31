#!/usr/bin/env python
"""Interval calculation utilities for market data.

This module provides functions for converting market intervals to various
time units (microseconds, seconds, timedelta) and calculating interval
boundaries.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from time_utils.py for modularity
# polars-exception: extracted from existing pandas-based time_utils.py - migration is separate task
"""

import re
from datetime import datetime, timedelta, timezone

from data_source_manager.utils.deprecation_rules import TimeUnit
from data_source_manager.utils.market_constraints import Interval as MarketInterval

__all__ = [
    "align_time_boundaries",
    "estimate_record_count",
    "get_interval_ceiling",
    "get_interval_floor",
    "get_interval_micros",
    "get_interval_seconds",
    "get_interval_timedelta",
    "get_smaller_units",
]


def get_interval_micros(interval: MarketInterval) -> int:
    """Convert market interval to microseconds for precise time calculations.

    This function provides a standardized way to convert any market interval
    (1s, 1m, 1h, 1d, etc.) to its equivalent duration in microseconds.
    It's a foundational utility that enables precise time calculations across
    the application.

    Args:
        interval: The market interval to convert (e.g., MINUTE_1, HOUR_1)

    Returns:
        int: Interval duration in microseconds

    Raises:
        ValueError: If the interval format is invalid or unsupported

    Example:
        >>> from data_source_manager.utils.market_constraints import Interval
        >>> from data_source_manager.utils.time.intervals import get_interval_micros
        >>>
        >>> # Convert different intervals to microseconds
        >>> minute_micros = get_interval_micros(Interval.MINUTE_1)
        >>> hour_micros = get_interval_micros(Interval.HOUR_1)
        >>> day_micros = get_interval_micros(Interval.DAY_1)
        >>>
        >>> print(f"1 minute = {minute_micros} microseconds")
        >>> print(f"1 hour = {hour_micros} microseconds")
        >>> print(f"1 day = {day_micros} microseconds")
        1 minute = 60000000 microseconds
        1 hour = 3600000000 microseconds
        1 day = 86400000000 microseconds
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
    unit = next((u for u in TimeUnit.get_all_units() if u.value == time_unit_symbol), None)
    if unit is None:
        raise ValueError(f"Unknown TimeUnit symbol: {time_unit_symbol}")

    return value * unit.micros


def get_interval_seconds(interval: MarketInterval) -> int:
    """Convert market interval to seconds for standard time calculations.

    This function provides a more convenient alternative to get_interval_micros
    when working with seconds is more appropriate (e.g., for API requests,
    human-readable durations, or compatibility with libraries that use seconds).

    Args:
        interval: The market interval to convert (e.g., MINUTE_1, HOUR_1)

    Returns:
        int: Interval duration in seconds

    Example:
        >>> from data_source_manager.utils.market_constraints import Interval
        >>> from data_source_manager.utils.time.intervals import get_interval_seconds
        >>>
        >>> # Compare interval durations in seconds
        >>> intervals = [Interval.MINUTE_1, Interval.MINUTE_5, Interval.HOUR_1, Interval.DAY_1]
        >>> for interval in intervals:
        ...     seconds = get_interval_seconds(interval)
        ...     print(f"{interval.value} = {seconds} seconds")
        1m = 60 seconds
        5m = 300 seconds
        1h = 3600 seconds
        1d = 86400 seconds
    """
    return get_interval_micros(interval) // 1_000_000


def get_interval_timedelta(interval: MarketInterval) -> timedelta:
    """Convert market interval to timedelta for datetime arithmetic.

    This function is particularly useful for datetime calculations where you need
    to add or subtract a market interval from a datetime object. Using timedelta
    objects ensures proper handling of calendar rules (month/year boundaries, DST changes).

    Args:
        interval: The market interval to convert (e.g., MINUTE_1, HOUR_1)

    Returns:
        timedelta: Interval as a Python timedelta object

    Example:
        >>> from datetime import datetime, timezone
        >>> from data_source_manager.utils.market_constraints import Interval
        >>> from data_source_manager.utils.time.intervals import get_interval_timedelta
        >>>
        >>> # Add different intervals to a datetime
        >>> now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        >>> intervals = [Interval.MINUTE_1, Interval.HOUR_1, Interval.DAY_1]
        >>>
        >>> for interval in intervals:
        ...     delta = get_interval_timedelta(interval)
        ...     future = now + delta
        ...     print(f"{now} + {interval.value} = {future}")
        2023-01-01 12:00:00+00:00 + 1m = 2023-01-01 12:01:00+00:00
        2023-01-01 12:00:00+00:00 + 1h = 2023-01-01 13:00:00+00:00
        2023-01-01 12:00:00+00:00 + 1d = 2023-01-02 12:00:00+00:00
    """
    return timedelta(microseconds=get_interval_micros(interval))


def get_smaller_units(interval: MarketInterval) -> list[TimeUnit]:
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


def align_time_boundaries(start_time: datetime, end_time: datetime, interval: MarketInterval) -> tuple[datetime, datetime]:
    """Align time boundaries to exact interval points for precise data retrieval.

    This function ensures that start and end times align perfectly with interval boundaries.
    For example, for a 1-minute interval:
    - 2023-01-01 12:34:56 would be aligned to 2023-01-01 12:34:00
    - 2023-01-01 12:34:56 would be aligned to 2023-01-01 12:35:00 for the end time

    Proper alignment is critical for:
    1. Ensuring data completeness (no partial bars)
    2. Avoiding off-by-one errors in record counts
    3. Consistent handling across different data sources
    4. Accurate gap detection between time periods

    Args:
        start_time: Start datetime to align (naive or timezone-aware)
        end_time: End datetime to align (naive or timezone-aware)
        interval: Market interval to align to (e.g., MINUTE_1, HOUR_1)

    Returns:
        tuple: (aligned_start, aligned_end) as timezone-aware datetime objects
            - aligned_start: Floored to the nearest interval boundary
            - aligned_end: Ceiled to the nearest interval boundary

    Example:
        >>> from datetime import datetime, timezone
        >>> from data_source_manager.utils.market_constraints import Interval
        >>> from data_source_manager.utils.time.intervals import align_time_boundaries
        >>>
        >>> # Align a 1-hour interval request
        >>> start = datetime(2023, 1, 1, 14, 30, 0, tzinfo=timezone.utc)
        >>> end = datetime(2023, 1, 1, 16, 45, 0, tzinfo=timezone.utc)
        >>>
        >>> aligned_start, aligned_end = align_time_boundaries(start, end, Interval.HOUR_1)
        >>> print(f"Original: {start.isoformat()} to {end.isoformat()}")
        >>> print(f"Aligned: {aligned_start.isoformat()} to {aligned_end.isoformat()}")
        Original: 2023-01-01T14:30:00+00:00 to 2023-01-01T16:45:00+00:00
        Aligned: 2023-01-01T14:00:00+00:00 to 2023-01-01T17:00:00+00:00
    """
    from data_source_manager.utils.loguru_setup import logger
    from data_source_manager.utils.time.conversion import enforce_utc_timezone

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

    # Apply corrected boundary alignment rules:
    # - startTime: Round DOWN to interval boundary (floor)
    # - endTime: Round DOWN to interval boundary (floor)
    # This ensures aligned_start <= aligned_end always
    aligned_start_microseconds = start_floor
    aligned_end_microseconds = end_floor

    # Ensure aligned_end is not before aligned_start
    # If they're equal, we need at least one interval
    if aligned_end_microseconds <= aligned_start_microseconds:
        aligned_end_microseconds = aligned_start_microseconds + interval_microseconds

    # Convert back to datetime
    aligned_start = datetime.fromtimestamp(aligned_start_microseconds / 1_000_000, tz=timezone.utc)
    aligned_end = datetime.fromtimestamp(aligned_end_microseconds / 1_000_000, tz=timezone.utc)

    # Log with explicit semantic meaning of timestamps
    logger.debug(
        f"Aligned boundaries: {start_time} → {aligned_start} (BEGINNING of first candle), "
        f"{end_time} → {aligned_end} (BEGINNING of last candle) "
        f"(interval: {interval.value})"
    )

    # Calculate and log the actual data range
    actual_end_time = datetime.fromtimestamp(
        (aligned_end_microseconds + interval_microseconds - 1) / 1_000_000,
        tz=timezone.utc,
    )
    logger.debug(
        f"Complete data range after alignment: {aligned_start} to {actual_end_time} (from BEGINNING of first candle to END of last candle)"
    )

    return aligned_start, aligned_end


def estimate_record_count(start_time: datetime, end_time: datetime, interval: MarketInterval) -> int:
    """Estimate the number of data points between two timestamps for capacity planning.

    This function calculates how many data points (candles/bars) would exist between
    two timestamps for a given interval. It's useful for:

    1. Capacity planning before data retrieval
    2. Validating that retrieved data is complete
    3. Pre-allocating arrays or dataframes of appropriate size
    4. Detecting missing data by comparing actual vs. expected record counts

    The calculation accounts for:
    - Proper time boundary alignment
    - Future date handling (returns 0 for future dates)
    - Current time limitations (no data exists beyond now)

    Args:
        start_time: Start time for data range
        end_time: End time for data range
        interval: Market interval (e.g., MINUTE_1, HOUR_1)

    Returns:
        int: Estimated number of data points/records between start and end time

    Example:
        >>> from datetime import datetime, timezone, timedelta
        >>> from data_source_manager.utils.market_constraints import Interval
        >>> from data_source_manager.utils.time.intervals import estimate_record_count
        >>>
        >>> # Calculate records for different time spans and intervals
        >>> now = datetime.now(timezone.utc)
        >>> yesterday = now - timedelta(days=1)
        >>> last_week = now - timedelta(days=7)
        >>>
        >>> # Estimate records for different intervals
        >>> minute_records = estimate_record_count(yesterday, now, Interval.MINUTE_1)
        >>> hour_records = estimate_record_count(last_week, now, Interval.HOUR_1)
        >>>
        >>> print(f"Expected 1-minute bars for last day: {minute_records}")
        >>> print(f"Expected 1-hour bars for last week: {hour_records}")
        >>> print(f"For 1-minute data, 1 day should have exactly 1440 records: {minute_records == 1440}")

    Note:
        When calculating storage requirements, remember that each record typically
        has multiple fields (open, high, low, close, volume, etc.), so the total
        storage needed will be: record_count * fields_per_record * bytes_per_field.
    """
    from data_source_manager.utils.time.conversion import enforce_utc_timezone

    # Ensure timezone awareness
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    # Check if we're dealing with future dates (no data yet)
    now = datetime.now(timezone.utc)
    if start_time > now:
        return 0  # No records for future dates

    # Limit end_time to current time as future data isn't available
    end_time = min(end_time, now)

    # Get aligned boundaries
    aligned_start, aligned_end = align_time_boundaries(start_time, end_time, interval)

    # If end is before start after alignment, return 0
    if aligned_end < aligned_start:
        return 0

    time_diff_secs = (end_time - start_time).total_seconds()
    interval_secs = interval.to_seconds()

    # Calculate the number of intervals, add 1 to include the end time point
    return int((time_diff_secs // interval_secs) + 1)
