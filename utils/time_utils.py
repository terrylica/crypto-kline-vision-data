#!/usr/bin/env python
"""Time utilities for handling time alignment and boundaries in market data operations.

This module centralizes all time-related functionality for working with financial market data,
providing a single source of truth for timestamp handling, interval calculations, and time
boundary management. It ensures consistent time handling across the entire application.

Key functionality:
1. Time zone conversion and normalization
2. Interval calculations and boundary alignment
3. Timestamp precision management (milliseconds/microseconds)
4. Time window validation and filtering
5. Bar/candle completion detection
6. Record count estimation

The module is particularly important for:
- Aligning time boundaries for API requests to Binance and other providers
- Ensuring consistent timestamp handling across different data sources
- Calculating precise time intervals for market data analysis
- Properly handling timezone information for global market data

Example:
    >>> from utils.time_utils import align_time_boundaries, datetime_to_milliseconds
    >>> from utils.market_constraints import Interval
    >>> from datetime import datetime, timezone
    >>>
    >>> # Align time boundaries for a 1-minute interval request
    >>> start = datetime(2023, 1, 1, 12, 34, 56, tzinfo=timezone.utc)
    >>> end = datetime(2023, 1, 1, 15, 45, 23, tzinfo=timezone.utc)
    >>> aligned_start, aligned_end = align_time_boundaries(start, end, Interval.MINUTE_1)
    >>>
    >>> print(f"Original: {start} to {end}")
    >>> print(f"Aligned: {aligned_start} to {aligned_end}")
    >>>
    >>> # Convert datetime to milliseconds for API requests
    >>> ms_timestamp = datetime_to_milliseconds(aligned_start)
    >>> print(f"Millisecond timestamp: {ms_timestamp}")
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Literal

import numpy as np
import pandas as pd

from utils.config import (
    CANONICAL_CLOSE_TIME,
    CANONICAL_INDEX_NAME,
    MILLISECOND_DIGITS,
    TIMESTAMP_PRECISION,
)
from utils.deprecation_rules import TimeUnit
from utils.logger_setup import logger
from utils.market_constraints import Interval as MarketInterval

# Re-export the get_interval_micros function at the module level for direct import
__all__ = [
    "TimeseriesDataProcessor",
    "align_time_boundaries",
    "datetime_to_milliseconds",
    "detect_timestamp_unit",
    "enforce_utc_timezone",
    "estimate_record_count",
    "filter_dataframe_by_time",
    "get_bar_close_time",
    "get_interval_ceiling",
    "get_interval_floor",
    "get_interval_micros",
    "get_interval_seconds",
    "get_interval_timedelta",
    "get_smaller_units",
    "is_bar_complete",
    "milliseconds_to_datetime",
    "standardize_timestamp_precision",
    "validate_timestamp_unit",
]

# Constants for timestamp format detection
MICROSECOND_DIGITS = 16
TIMESTAMP_UNIT = "us"  # Default unit for timestamp parsing

# Timestamp unit type
TimestampUnit = Literal["ms", "us"]  # Supported timestamp units

# Configure module logger


def detect_timestamp_unit(sample_ts: int | str) -> TimestampUnit:
    """Detect timestamp unit based on number of digits.

    Args:
        sample_ts: Sample timestamp value

    Returns:
        "us" for microseconds (16 digits)
        "ms" for milliseconds (13 digits)

    Raises:
        ValueError: If timestamp format is not recognized

    Note:
        This is a core architectural feature to handle Binance Vision's
        evolution of timestamp formats:
        - Pre-2025: Millisecond timestamps (13 digits)
        - 2025 onwards: Microsecond timestamps (16 digits)
    """
    digits = len(str(int(sample_ts)))

    if digits == MICROSECOND_DIGITS:
        return "us"
    if digits == MILLISECOND_DIGITS:
        return "ms"
    raise ValueError(
        f"Unrecognized timestamp format with {digits} digits. "
        f"Expected {MILLISECOND_DIGITS} for milliseconds or "
        f"{MICROSECOND_DIGITS} for microseconds."
    )


def standardize_timestamp_precision(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize timestamp precision to match the system standard (REST API format).

    This function ensures all timestamps in a DataFrame conform to the standard precision
    defined in utils/config.py (now set to milliseconds to match REST API format).

    It handles:
    1. DatetimeIndex standardization
    2. Timestamp-type columns (open_time, close_time)
    3. Conversion between microsecond and millisecond precision

    Args:
        df: DataFrame with timestamp columns or DatetimeIndex

    Returns:
        DataFrame with standardized timestamp precision

    Note:
        - The current system standard is millisecond precision as used by the REST API
        - Vision API data from 2025+ uses microsecond precision that needs to be standardized
        - This function is crucial for ensuring that data from different sources is comparable
    """
    if df.empty:
        logger.debug("Empty DataFrame in standardize_timestamp_precision")
        return df

    # Make a copy to avoid modifying the original
    result_df = df.copy()

    # Process DatetimeIndex if present
    if isinstance(result_df.index, pd.DatetimeIndex):
        logger.debug(f"Processing DatetimeIndex in standardize_timestamp_precision, target precision: {TIMESTAMP_PRECISION}")

        # Get a sample timestamp to determine current precision
        sample_ts = result_df.index[0].value
        current_precision = "us" if len(str(abs(sample_ts))) > MILLISECOND_DIGITS else "ms"

        # Only convert if current precision doesn't match target
        if current_precision != TIMESTAMP_PRECISION:
            logger.debug(f"Converting index from {current_precision} to {TIMESTAMP_PRECISION} precision")

            if current_precision == "us" and TIMESTAMP_PRECISION == "ms":
                # Convert from microseconds to milliseconds (truncate)
                # Create new DatetimeIndex with millisecond precision
                new_index = pd.DatetimeIndex(
                    [pd.Timestamp(ts.timestamp() * 1000, unit="ms", tz=timezone.utc) for ts in result_df.index],
                    name=result_df.index.name,
                )
                result_df.index = new_index

            elif current_precision == "ms" and TIMESTAMP_PRECISION == "us":
                # Convert from milliseconds to microseconds (add zeros)
                # Create new DatetimeIndex with microsecond precision
                new_index = pd.DatetimeIndex(
                    [pd.Timestamp(int(ts.timestamp() * 1000000), unit="us", tz=timezone.utc) for ts in result_df.index],
                    name=result_df.index.name,
                )
                result_df.index = new_index

    # Process timestamp columns
    time_columns = [CANONICAL_INDEX_NAME, CANONICAL_CLOSE_TIME]
    for col in time_columns:
        if col in result_df.columns and pd.api.types.is_datetime64_dtype(result_df[col]):
            logger.debug(f"Processing timestamp column {col}")

            # Get a sample to determine current precision
            if len(result_df) > 0:
                sample_ts = result_df[col].iloc[0].value
                current_precision = "us" if len(str(abs(sample_ts))) > MILLISECOND_DIGITS else "ms"

                # Only convert if current precision doesn't match target
                if current_precision != TIMESTAMP_PRECISION:
                    logger.debug(f"Converting column {col} from {current_precision} to {TIMESTAMP_PRECISION} precision")

                    if current_precision == "us" and TIMESTAMP_PRECISION == "ms":
                        # Convert from microseconds to milliseconds (truncate)
                        result_df[col] = pd.to_datetime(
                            (result_df[col].astype(np.int64) // 1000000) * 1000,
                            unit="ms",
                            utc=True,
                        )

                    elif current_precision == "ms" and TIMESTAMP_PRECISION == "us":
                        # Convert from milliseconds to microseconds (add zeros)
                        result_df[col] = pd.to_datetime(result_df[col].astype(np.int64) * 1000, unit="us", utc=True)

    return result_df


def validate_timestamp_unit(unit: TimestampUnit) -> None:
    """Validate that the timestamp unit is supported.

    Args:
        unit: Timestamp unit to validate

    Raises:
        ValueError: If unit is not supported
    """
    if unit not in ("ms", "us"):
        raise ValueError(f"Unsupported timestamp unit: {unit}. Must be 'ms' or 'us'.")


def datetime_to_milliseconds(dt: datetime) -> int:
    """Convert datetime to milliseconds timestamp for Binance API.

    Args:
        dt: Datetime object (naive or timezone-aware)

    Returns:
        Milliseconds timestamp (int)
    """
    # Ensure datetime is timezone-aware and in UTC
    dt = enforce_utc_timezone(dt)

    # Convert to milliseconds
    return int(dt.timestamp() * 1000)


def milliseconds_to_datetime(ms: int) -> datetime:
    """Convert milliseconds timestamp to datetime object.

    Args:
        ms: Milliseconds timestamp

    Returns:
        Timezone-aware datetime (UTC)
    """
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def enforce_utc_timezone(dt: datetime) -> datetime:
    """Ensures datetime object is timezone-aware and in UTC.

    This is a foundational utility method used by other validation methods
    to normalize datetime objects. It handles both naive and timezone-aware
    datetime objects, ensuring consistent timezone handling throughout the system.

    Args:
        dt: Input datetime, can be naive or timezone-aware

    Returns:
        UTC timezone-aware datetime
    """
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        # If naive datetime, assume it's UTC
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
    if dt.tzinfo == timezone.utc:
        # If already in UTC, return a new copy to ensure it's a different object
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
        >>> from utils.market_constraints import Interval
        >>> from utils.time_utils import get_interval_micros
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
        >>> from utils.market_constraints import Interval
        >>> from utils.time_utils import get_interval_seconds
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
        >>> from utils.market_constraints import Interval
        >>> from utils.time_utils import get_interval_timedelta
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


def get_bar_close_time(open_time: datetime, interval: MarketInterval) -> datetime:
    """Get the close time for a bar given its open time.

    Args:
        open_time: The bar's open time
        interval: The interval specification

    Returns:
        datetime: Close time (interval - 1 microsecond after open time)
    """
    interval_delta = get_interval_timedelta(interval)
    return open_time + interval_delta - timedelta(microseconds=1)


def is_bar_complete(
    timestamp: datetime,
    interval: MarketInterval,
    current_time: datetime | None = None,
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
    df: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
    time_column: str = "open_time",
) -> pd.DataFrame:
    """Filter a DataFrame by time range with robust handling of different timestamp formats.

    This function provides a consistent way to filter market data by a time range,
    handling various timestamp formats and edge cases:

    1. Both DatetimeIndex and regular column-based filtering
    2. Different timestamp precisions (milliseconds/microseconds)
    3. Different timestamp formats (datetime objects, epoch timestamps)
    4. Proper timezone handling (converting all to UTC)

    Args:
        df: DataFrame to filter, can have DatetimeIndex or time column
        start_time: Start of time range (inclusive)
        end_time: End of time range (inclusive)
        time_column: Name of timestamp column to filter by (defaults to "open_time")

    Returns:
        pd.DataFrame: Filtered DataFrame containing only rows within the specified time range

    Example:
        >>> import pandas as pd
        >>> from datetime import datetime, timezone, timedelta
        >>> from utils.time_utils import filter_dataframe_by_time
        >>>
        >>> # Create sample data
        >>> now = datetime.now(timezone.utc)
        >>> dates = [now - timedelta(minutes=i) for i in range(10)]
        >>> df = pd.DataFrame({
        ...     'open_time': dates,
        ...     'value': range(10)
        ... })
        >>>
        >>> # Filter for last 5 minutes
        >>> start = now - timedelta(minutes=5)
        >>> filtered_df = filter_dataframe_by_time(df, start, now)
        >>>
        >>> print(f"Original rows: {len(df)}")
        >>> print(f"Filtered rows: {len(filtered_df)}")

    Note:
        The function handles both the case where the timestamp is the index
        and where it's a regular column. It prioritizes using DatetimeIndex
        filtering for better performance when available.
    """
    if df.empty:
        return df.copy()

    # Ensure times are timezone-aware
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    logger.debug(f"Filtering DataFrame by time: {start_time} to {end_time}")
    logger.debug(f"Before filtering: {len(df)} rows")

    # Debug: Trace timestamps at boundaries
    # Extracted to utils_for_debug/timestamp_debug.py
    from utils_for_debug.timestamp_debug import (
        analyze_filter_conditions,
        compare_filtered_results,
        trace_dataframe_timestamps,
    )

    # Log detailed timestamp information for debugging
    trace_dataframe_timestamps(df, time_column, start_time, end_time)

    # Analyze filter conditions for debugging
    analyze_filter_conditions(df, start_time, end_time, time_column)

    # Check if the time column exists
    if time_column not in df.columns:
        if df.index.name == time_column and isinstance(df.index, pd.DatetimeIndex):
            # Reset index to make the time column available for filtering
            df_with_column = df.reset_index()

            # IMPORTANT: Use >= for start_time and <= for end_time to ensure
            # exact interval boundaries are included correctly
            logger.debug(
                f"Filtering on index reset as column, using criteria: {time_column} >= {start_time} AND {time_column} <= {end_time}"
            )

            filtered_df = df_with_column[(df_with_column[time_column] >= start_time) & (df_with_column[time_column] <= end_time)].copy()

            # Set index back
            if not filtered_df.empty:
                filtered_df = filtered_df.set_index(time_column)
        else:
            logger.warning(f"Time column '{time_column}' not found in DataFrame")
            return df.copy()
    else:
        # Filter dataframe using the time column, preserving exact timestamps
        # IMPORTANT: Use >= for start_time and <= for end_time to include timestamps
        # exactly at the interval boundaries
        logger.debug(f"Filtering on column, using criteria: {time_column} >= {start_time} AND {time_column} <= {end_time}")

        filtered_df = df[(df[time_column] >= start_time) & (df[time_column] <= end_time)].copy()

    # Reset index if it's not already the time column
    if filtered_df.index.name != time_column:
        filtered_df = filtered_df.reset_index(drop=True)

    if len(filtered_df) == 0:
        logger.warning(f"No data within time range {start_time} to {end_time}")
    else:
        logger.debug(f"After filtering: {len(filtered_df)} rows")
        if len(filtered_df) > 0:
            # Log the min and max timestamps in the filtered data
            if time_column in filtered_df.columns:
                min_ts = filtered_df[time_column].min()
                max_ts = filtered_df[time_column].max()
                logger.debug(f"First timestamp: {min_ts} (represents BEGINNING of candle)")
                logger.debug(f"Last timestamp: {max_ts} (represents BEGINNING of candle)")

                # Check if the first expected timestamp is present
                if min_ts > start_time:
                    time_diff = (min_ts - start_time).total_seconds()
                    logger.debug(f"First timestamp ({min_ts}) is later than requested start time ({start_time}), diff: {time_diff} seconds")
                    logger.debug("First candle is missing from result! This may indicate a timestamp interpretation issue.")

                # Check if the last expected timestamp is present
                if max_ts < end_time:
                    logger.debug(f"Last timestamp ({max_ts}) is earlier than requested end time ({end_time})")
            elif isinstance(filtered_df.index, pd.DatetimeIndex):
                min_ts = filtered_df.index.min()
                max_ts = filtered_df.index.max()
                logger.debug(f"First timestamp: {min_ts} (represents BEGINNING of candle)")
                logger.debug(f"Last timestamp: {max_ts} (represents BEGINNING of candle)")

    # Debug: Compare input and output dataframes to verify filtering worked correctly
    compare_filtered_results(df, filtered_df, start_time, end_time, time_column)

    return filtered_df


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
        >>> from utils.market_constraints import Interval
        >>> from utils.time_utils import align_time_boundaries
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
    aligned_start_microseconds = start_floor if start_microseconds == start_floor else start_floor + interval_microseconds
    aligned_end_microseconds = end_floor

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
        >>> from utils.market_constraints import Interval
        >>> from utils.time_utils import estimate_record_count
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
    return (time_diff_secs // interval_secs) + 1


class TimeseriesDataProcessor:
    """Unified processor for handling time series data with consistent timestamp handling.

    This class centralizes the timestamp handling logic to ensure consistent
    behavior across different APIs (REST, Vision) and different timestamp formats
    (milliseconds vs microseconds).
    """

    @staticmethod
    def detect_timestamp_unit(sample_ts: int | str) -> str:
        """Detect timestamp unit based on number of digits.

        Args:
            sample_ts: Sample timestamp value

        Returns:
            "us" for microseconds (16 digits)
            "ms" for milliseconds (13 digits)

        Raises:
            ValueError: If timestamp format is not recognized
        """
        # Use the global function for consistency
        return detect_timestamp_unit(sample_ts)

    @classmethod
    def process_kline_data(cls, raw_data: list[list], columns: list[str]) -> pd.DataFrame:
        """Process raw kline data into a standardized DataFrame.

        This method handles different timestamp formats consistently:
        - Automatically detects millisecond (13 digits) vs microsecond (16 digits) precision
        - Properly handles timezone awareness
        - Normalizes numeric columns
        - Removes duplicates and ensures chronological ordering
        - Preserves exact timestamps from raw data without any shifting

        Args:
            raw_data: List of kline data from an API
            columns: Column names for the data

        Returns:
            Processed DataFrame with standardized index and columns
        """
        if not raw_data:
            return pd.DataFrame()

        # Use provided column definitions
        df = pd.DataFrame(raw_data, columns=pd.Index(columns))

        # Detect timestamp unit from the first row
        timestamp_unit = TIMESTAMP_UNIT  # Default from config
        timestamp_multiplier = 1000  # Default milliseconds to microseconds conversion

        # Detect format based on open_time (first element in kline data)
        if len(raw_data) > 0:
            try:
                sample_ts = raw_data[0][0]  # First element is open_time
                logger.debug(f"Sample open_time: {sample_ts}")
                logger.debug(f"Number of digits: {len(str(int(sample_ts)))}")

                # Use the timestamp format detector
                detected_unit = cls.detect_timestamp_unit(sample_ts)

                if detected_unit == "us":
                    # Already in microseconds, no conversion needed
                    timestamp_unit = "us"
                    timestamp_multiplier = 1
                    logger.debug("Detected microsecond precision (16 digits)")
                else:
                    # Convert from milliseconds to microseconds
                    timestamp_unit = "us"
                    timestamp_multiplier = 1000
                    logger.debug("Detected millisecond precision (13 digits)")

            except (ValueError, TypeError) as e:
                logger.warning(f"Error detecting timestamp format: {e}")

        # Convert timestamps with appropriate precision
        for col in ["open_time", "close_time"]:
            if col in df.columns:
                # Convert to int64 first to ensure full precision
                df[col] = df[col].astype(np.int64)

                # Apply the detected multiplier
                if timestamp_multiplier > 1:
                    df[col] = df[col] * timestamp_multiplier

                # Convert to datetime with the appropriate unit
                df[col] = pd.to_datetime(df[col], unit=timestamp_unit, utc=True)

                # Log the exact timestamp values
                if len(raw_data) > 0:
                    logger.debug(f"Converted {col}: {df[col].iloc[0]}")
                    logger.debug(f"{col} microseconds: {df[col].iloc[0].microsecond}")

        # Convert numeric columns efficiently
        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_asset_volume",
            "taker_buy_volume",
            "taker_buy_quote_volume",
        ]

        # Only convert columns that exist
        numeric_cols = [col for col in numeric_cols if col in df.columns]
        if numeric_cols:
            df[numeric_cols] = df[numeric_cols].astype(np.float64)

        # Convert count if it exists
        if "count" in df.columns:
            df["count"] = df["count"].astype(np.int32)

        # Check for duplicate timestamps and sort by open_time
        if "open_time" in df.columns:
            logger.debug(f"Shape before sorting and deduplication: {df.shape}")

            # First, sort by open_time to ensure chronological order
            df = df.sort_values("open_time")

            # Then check for duplicates and drop them if necessary
            if df.duplicated(subset=["open_time"]).any():
                duplicates_count = df.duplicated(subset=["open_time"]).sum()
                logger.debug(f"Found {duplicates_count} duplicate timestamps, keeping first occurrence")
                df = df.drop_duplicates(subset=["open_time"], keep="first")

            # Set the index to open_time for consistent behavior
            df = df.set_index("open_time")

            # If close_time doesn't exist in the raw data, we need to calculate it
            # but we should clearly log that we are doing this modification
            if "close_time" not in df.columns and "close_time" not in columns:
                logger.debug("close_time not in raw data, calculating it from interval")
                # Generate close_time based on the interval detected from consecutive timestamps
                if len(df) > 1:
                    # Estimate interval from first two timestamps
                    first_two_indices = df.index[:2]
                    estimated_interval = (first_two_indices[1] - first_two_indices[0]).total_seconds()
                    logger.debug(f"Estimated interval from timestamps: {estimated_interval} seconds")
                    # Calculate close_time based on this interval
                    df["close_time"] = df.index + pd.Timedelta(seconds=estimated_interval) - pd.Timedelta(microseconds=1)
                    logger.debug(f"Generated close_time using estimated interval: first value = {df['close_time'].iloc[0]}")
                else:
                    # Default fallback to 1 second if we only have one timestamp
                    logger.debug("Only one timestamp, using default 1-second interval for close_time")
                    df["close_time"] = df.index + pd.Timedelta(seconds=1) - pd.Timedelta(microseconds=1)

        logger.debug(f"Final DataFrame shape: {df.shape}")
        return df

    @classmethod
    def standardize_dataframe(cls, df: pd.DataFrame, canonical_index_name: str = "open_time") -> pd.DataFrame:
        """Standardize a DataFrame to ensure consistent structure.

        Args:
            df: DataFrame to standardize
            canonical_index_name: Name to use for the index

        Returns:
            Standardized DataFrame
        """
        if df.empty:
            return df

        # Ensure index has the canonical name
        if df.index.name != canonical_index_name:
            df.index.name = canonical_index_name

        # Ensure index is sorted
        if not df.index.is_monotonic_increasing:
            df = df.sort_index()

        # Remove duplicates from index if any exist
        if df.index.has_duplicates:
            df = df[~df.index.duplicated(keep="first")]

        # Ensure all datetimes are timezone-aware UTC
        if isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.DatetimeIndex(
                [enforce_utc_timezone(dt) for dt in df.index.to_pydatetime()],
                name=df.index.name,
            )

        # Also fix close_time if it exists
        if "close_time" in df.columns and isinstance(df["close_time"].iloc[0], datetime):
            df["close_time"] = df["close_time"].apply(enforce_utc_timezone)

        return df
