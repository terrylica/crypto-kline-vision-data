#!/usr/bin/env python
"""Time utilities for handling time alignment and boundaries in Binance API requests.

This module centralizes all time-related functionality, providing a single source of truth for:
1. Time zone conversion and normalization
2. Interval calculations and manipulations
3. Time boundary alignment for API requests
4. Time window validation
5. Timestamp unit detection and formatting (for Binance Vision API compatibility)

The module combines functionality previously scattered across time_alignment.py and
api_boundary_validator.py to ensure consistent behavior throughout the application.
"""

import re
from datetime import datetime, timedelta, timezone
from typing import List, Literal, Optional, Tuple, Union

import numpy as np
import pandas as pd

from utils.config import CANONICAL_CLOSE_TIME, CANONICAL_INDEX_NAME, TIMESTAMP_PRECISION
from utils.deprecation_rules import TimeUnit
from utils.logger_setup import logger
from utils.market_constraints import Interval as MarketInterval

# Re-export the get_interval_micros function at the module level for direct import
__all__ = [
    "enforce_utc_timezone",
    "get_interval_micros",
    "get_interval_seconds",
    "get_interval_timedelta",
    "get_smaller_units",
    "get_interval_floor",
    "get_interval_ceiling",
    "get_bar_close_time",
    "is_bar_complete",
    "filter_dataframe_by_time",
    "align_time_boundaries",
    "estimate_record_count",
    "TimeseriesDataProcessor",
    "datetime_to_milliseconds",
    "milliseconds_to_datetime",
    "detect_timestamp_unit",
    "validate_timestamp_unit",
    "standardize_timestamp_precision",
]

# Constants for timestamp format detection
MILLISECOND_DIGITS = 13
MICROSECOND_DIGITS = 16
TIMESTAMP_UNIT = "us"  # Default unit for timestamp parsing

# Timestamp unit type
TimestampUnit = Literal["ms", "us"]  # Supported timestamp units

# Configure module logger


def detect_timestamp_unit(sample_ts: Union[int, str]) -> TimestampUnit:
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
    elif digits == MILLISECOND_DIGITS:
        return "ms"
    else:
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
        logger.debug(
            f"Processing DatetimeIndex in standardize_timestamp_precision, target precision: {TIMESTAMP_PRECISION}"
        )

        # Get a sample timestamp to determine current precision
        sample_ts = result_df.index[0].value
        current_precision = "us" if len(str(abs(sample_ts))) > 13 else "ms"

        # Only convert if current precision doesn't match target
        if current_precision != TIMESTAMP_PRECISION:
            logger.debug(
                f"Converting index from {current_precision} to {TIMESTAMP_PRECISION} precision"
            )

            if current_precision == "us" and TIMESTAMP_PRECISION == "ms":
                # Convert from microseconds to milliseconds (truncate)
                # Create new DatetimeIndex with millisecond precision
                new_index = pd.DatetimeIndex(
                    [
                        pd.Timestamp(ts.timestamp() * 1000, unit="ms", tz=timezone.utc)
                        for ts in result_df.index
                    ],
                    name=result_df.index.name,
                )
                result_df.index = new_index

            elif current_precision == "ms" and TIMESTAMP_PRECISION == "us":
                # Convert from milliseconds to microseconds (add zeros)
                # Create new DatetimeIndex with microsecond precision
                new_index = pd.DatetimeIndex(
                    [
                        pd.Timestamp(
                            int(ts.timestamp() * 1000000), unit="us", tz=timezone.utc
                        )
                        for ts in result_df.index
                    ],
                    name=result_df.index.name,
                )
                result_df.index = new_index

    # Process timestamp columns
    time_columns = [CANONICAL_INDEX_NAME, CANONICAL_CLOSE_TIME]
    for col in time_columns:
        if col in result_df.columns and pd.api.types.is_datetime64_dtype(
            result_df[col]
        ):
            logger.debug(f"Processing timestamp column {col}")

            # Get a sample to determine current precision
            if len(result_df) > 0:
                sample_ts = result_df[col].iloc[0].value
                current_precision = "us" if len(str(abs(sample_ts))) > 13 else "ms"

                # Only convert if current precision doesn't match target
                if current_precision != TIMESTAMP_PRECISION:
                    logger.debug(
                        f"Converting column {col} from {current_precision} to {TIMESTAMP_PRECISION} precision"
                    )

                    if current_precision == "us" and TIMESTAMP_PRECISION == "ms":
                        # Convert from microseconds to milliseconds (truncate)
                        result_df[col] = pd.to_datetime(
                            (result_df[col].astype(np.int64) // 1000000) * 1000,
                            unit="ms",
                            utc=True,
                        )

                    elif current_precision == "ms" and TIMESTAMP_PRECISION == "us":
                        # Convert from milliseconds to microseconds (add zeros)
                        result_df[col] = pd.to_datetime(
                            result_df[col].astype(np.int64) * 1000, unit="us", utc=True
                        )

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
    elif dt.tzinfo == timezone.utc:
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
    df: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
    time_column: str = "open_time",
) -> pd.DataFrame:
    """Filter DataFrame by time range.

    This function filters a DataFrame to include only rows where the time column
    is within the specified time range. It preserves the exact timestamps from
    the raw data without any shifting, maintaining the semantic meaning:
    - open_time represents the BEGINNING of each candle period
    - close_time represents the END of each candle period

    Args:
        df: DataFrame to filter
        start_time: Start time (inclusive)
        end_time: End time (inclusive)
        time_column: Name of the column containing timestamps (default: "open_time")

    Returns:
        Filtered DataFrame with preserved timestamp semantics
    """
    if df.empty:
        return df.copy()

    # Ensure times are timezone-aware
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    logger.debug(f"Filtering DataFrame by time: {start_time} to {end_time}")
    logger.debug(f"Before filtering: {len(df)} rows")

    # Debug: Trace timestamps at boundaries
    logger.debug(
        f"[TIMESTAMP TRACE] filter_dataframe_by_time called with range: {start_time} to {end_time}"
    )
    if len(df) > 0:
        if time_column in df.columns:
            min_ts = df[time_column].min()
            max_ts = df[time_column].max()
            logger.debug(
                f"[TIMESTAMP TRACE] Input DataFrame time range: {min_ts} to {max_ts}"
            )

            # Check for timestamps exactly at the boundaries
            exact_start_match = (df[time_column] == start_time).any()
            exact_end_match = (df[time_column] == end_time).any()
            logger.debug(
                f"[TIMESTAMP TRACE] Exact match at start_time: {exact_start_match}"
            )
            logger.debug(
                f"[TIMESTAMP TRACE] Exact match at end_time: {exact_end_match}"
            )

            # Log first few rows
            for i in range(min(3, len(df))):
                logger.debug(
                    f"[TIMESTAMP TRACE] Before filtering row {i}: {time_column}={df[time_column].iloc[i]}"
                )
        elif df.index.name == time_column and isinstance(df.index, pd.DatetimeIndex):
            min_ts = df.index.min()
            max_ts = df.index.max()
            logger.debug(
                f"[TIMESTAMP TRACE] Input DataFrame index range: {min_ts} to {max_ts}"
            )

    # Check if the time column exists
    if time_column not in df.columns:
        if df.index.name == time_column and isinstance(df.index, pd.DatetimeIndex):
            # Reset index to make the time column available for filtering
            df_with_column = df.reset_index()

            # IMPORTANT: Use >= for start_time and <= for end_time to ensure
            # exact interval boundaries are included correctly
            logger.debug(
                f"[TIMESTAMP TRACE] Filtering on index reset as column, using criteria: {time_column} >= {start_time} AND {time_column} <= {end_time}"
            )

            # Debug: Check how many rows would match each condition separately
            if not df_with_column.empty:
                start_condition = df_with_column[time_column] >= start_time
                end_condition = df_with_column[time_column] <= end_time
                logger.debug(
                    f"[TIMESTAMP TRACE] Rows meeting start condition ({time_column} >= {start_time}): {start_condition.sum()}"
                )
                logger.debug(
                    f"[TIMESTAMP TRACE] Rows meeting end condition ({time_column} <= {end_time}): {end_condition.sum()}"
                )
                logger.debug(
                    f"[TIMESTAMP TRACE] Rows meeting both conditions: {(start_condition & end_condition).sum()}"
                )

                # Check specifically for exact boundary matches
                exact_start = df_with_column[df_with_column[time_column] == start_time]
                exact_end = df_with_column[df_with_column[time_column] == end_time]
                logger.debug(
                    f"[TIMESTAMP TRACE] Rows exactly matching start_time: {len(exact_start)}"
                )
                logger.debug(
                    f"[TIMESTAMP TRACE] Rows exactly matching end_time: {len(exact_end)}"
                )

            filtered_df = df_with_column[
                (df_with_column[time_column] >= start_time)
                & (df_with_column[time_column] <= end_time)
            ].copy()

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
        logger.debug(
            f"[TIMESTAMP TRACE] Filtering on column, using criteria: {time_column} >= {start_time} AND {time_column} <= {end_time}"
        )

        # Debug: Check how many rows would match each condition separately
        if not df.empty:
            start_condition = df[time_column] >= start_time
            end_condition = df[time_column] <= end_time
            logger.debug(
                f"[TIMESTAMP TRACE] Rows meeting start condition ({time_column} >= {start_time}): {start_condition.sum()}"
            )
            logger.debug(
                f"[TIMESTAMP TRACE] Rows meeting end condition ({time_column} <= {end_time}): {end_condition.sum()}"
            )
            logger.debug(
                f"[TIMESTAMP TRACE] Rows meeting both conditions: {(start_condition & end_condition).sum()}"
            )

            # Check specifically for exact boundary matches
            exact_start = df[df[time_column] == start_time]
            exact_end = df[df[time_column] == end_time]
            logger.debug(
                f"[TIMESTAMP TRACE] Rows exactly matching start_time: {len(exact_start)}"
            )
            logger.debug(
                f"[TIMESTAMP TRACE] Rows exactly matching end_time: {len(exact_end)}"
            )

            # If no rows match exact start time, find nearest
            if len(exact_start) == 0:
                earliest_after_start = df[df[time_column] > start_time]
                if not earliest_after_start.empty:
                    earliest_time = earliest_after_start[time_column].min()
                    time_diff = (earliest_time - start_time).total_seconds()
                    logger.debug(
                        f"[TIMESTAMP TRACE] No exact start_time match. Earliest timestamp after start_time is {earliest_time}, which is {time_diff} seconds later"
                    )

        filtered_df = df[
            (df[time_column] >= start_time) & (df[time_column] <= end_time)
        ].copy()

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
                logger.debug(
                    f"First timestamp: {min_ts} (represents BEGINNING of candle)"
                )
                logger.debug(
                    f"Last timestamp: {max_ts} (represents BEGINNING of candle)"
                )

                # Check if the first expected timestamp is present
                if min_ts > start_time:
                    time_diff = (min_ts - start_time).total_seconds()
                    logger.debug(
                        f"First timestamp ({min_ts}) is later than requested start time ({start_time}), diff: {time_diff} seconds"
                    )
                    logger.debug(
                        "[TIMESTAMP TRACE] First candle is missing from result! This may indicate a timestamp interpretation issue."
                    )

                # Check if the last expected timestamp is present
                if max_ts < end_time:
                    logger.debug(
                        f"Last timestamp ({max_ts}) is earlier than requested end time ({end_time})"
                    )
            elif isinstance(filtered_df.index, pd.DatetimeIndex):
                min_ts = filtered_df.index.min()
                max_ts = filtered_df.index.max()
                logger.debug(
                    f"First timestamp: {min_ts} (represents BEGINNING of candle)"
                )
                logger.debug(
                    f"Last timestamp: {max_ts} (represents BEGINNING of candle)"
                )

            # Debug: Log first few rows after filtering
            logger.debug("[TIMESTAMP TRACE] After filtering results:")
            if time_column in filtered_df.columns:
                for i in range(min(3, len(filtered_df))):
                    logger.debug(
                        f"[TIMESTAMP TRACE] Filtered row {i}: {time_column}={filtered_df[time_column].iloc[i]}"
                    )
            elif (
                isinstance(filtered_df.index, pd.DatetimeIndex)
                and filtered_df.index.name == time_column
            ):
                for i in range(min(3, len(filtered_df))):
                    logger.debug(
                        f"[TIMESTAMP TRACE] Filtered row {i}: {time_column}={filtered_df.index[i]}"
                    )

    # Debug: Final check on timestamp interpretation
    logger.debug(
        f"[TIMESTAMP TRACE] filter_dataframe_by_time completed. Input rows: {len(df)}, Output rows: {len(filtered_df)}"
    )
    if len(filtered_df) > 0 and len(df) > 0:
        # Check if rows at exact boundaries were handled correctly
        if time_column in df.columns and time_column in filtered_df.columns:
            start_match_in_input = (df[time_column] == start_time).any()
            start_match_in_output = (filtered_df[time_column] == start_time).any()
            logger.debug(
                f"[TIMESTAMP TRACE] Start time exact match in input: {start_match_in_input}, in output: {start_match_in_output}"
            )
            if start_match_in_input and not start_match_in_output:
                logger.warning(
                    "[TIMESTAMP TRACE] Critical issue: Row with exact start_time existed in input but not in output!"
                )

    return filtered_df


def align_time_boundaries(
    start_time: datetime, end_time: datetime, interval: MarketInterval
) -> Tuple[datetime, datetime]:
    """Align time boundaries according to Binance REST API behavior.

    This is the unified implementation that correctly handles time boundaries for both
    REST and Vision APIs, following the Liskov Substitution Principle.

    Key Binance API boundary handling rules:
    - startTime: Rounds UP to the next interval boundary if not exactly on a boundary
    - endTime: Rounds DOWN to the previous interval boundary if not exactly on a boundary
    - Both boundaries are treated as INCLUSIVE after alignment
    - Microsecond/millisecond precision is ignored and rounded to interval boundaries

    This implementation preserves the semantic meaning of timestamps:
    - The aligned start_time represents the BEGINNING of the first candle in the range
    - The aligned end_time represents the BEGINNING of the last candle in the range
      (the actual end of the data range is end_time + interval_duration - 1 microsecond)

    This implementation is mathematically precise and works for all interval types:
    1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M.

    Args:
        start_time: User-provided start time
        end_time: User-provided end time
        interval: Data interval

    Returns:
        Tuple of (aligned_start_time, aligned_end_time) with proper boundary handling
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
        f"Complete data range after alignment: {aligned_start} to {actual_end_time} "
        f"(from BEGINNING of first candle to END of last candle)"
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
    end_time = min(end_time, now)

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


class TimeseriesDataProcessor:
    """Unified processor for handling time series data with consistent timestamp handling.

    This class centralizes the timestamp handling logic to ensure consistent
    behavior across different APIs (REST, Vision) and different timestamp formats
    (milliseconds vs microseconds).
    """

    @staticmethod
    def detect_timestamp_unit(sample_ts: Union[int, str]) -> str:
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
    def process_kline_data(
        cls, raw_data: List[List], columns: List[str]
    ) -> pd.DataFrame:
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
                logger.debug(
                    f"Found {duplicates_count} duplicate timestamps, keeping first occurrence"
                )
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
                    estimated_interval = (
                        first_two_indices[1] - first_two_indices[0]
                    ).total_seconds()
                    logger.debug(
                        f"Estimated interval from timestamps: {estimated_interval} seconds"
                    )
                    # Calculate close_time based on this interval
                    df["close_time"] = (
                        df.index
                        + pd.Timedelta(seconds=estimated_interval)
                        - pd.Timedelta(microseconds=1)
                    )
                    logger.debug(
                        f"Generated close_time using estimated interval: first value = {df['close_time'].iloc[0]}"
                    )
                else:
                    # Default fallback to 1 second if we only have one timestamp
                    logger.debug(
                        "Only one timestamp, using default 1-second interval for close_time"
                    )
                    df["close_time"] = (
                        df.index
                        + pd.Timedelta(seconds=1)
                        - pd.Timedelta(microseconds=1)
                    )

        logger.debug(f"Final DataFrame shape: {df.shape}")
        return df

    @classmethod
    def standardize_dataframe(
        cls, df: pd.DataFrame, canonical_index_name: str = "open_time"
    ) -> pd.DataFrame:
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
        if "close_time" in df.columns and isinstance(
            df["close_time"].iloc[0], datetime
        ):
            df["close_time"] = df["close_time"].apply(enforce_utc_timezone)

        return df
