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
from typing import Optional, Tuple, List, Union
import re

import pandas as pd
import numpy as np

from utils.market_constraints import Interval as MarketInterval
from utils.deprecation_rules import TimeUnit
from utils.logger_setup import logger

# Re-export the get_interval_micros function at the module level for direct import
__all__ = [
    "enforce_utc_timezone",
    "validate_time_window",
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
]

# Constants for timestamp format detection
MILLISECOND_DIGITS = 13
MICROSECOND_DIGITS = 16
TIMESTAMP_UNIT = "us"  # Default unit for timestamp parsing

# Configure module logger


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
    df: pd.DataFrame, start_time: datetime, end_time: datetime
) -> pd.DataFrame:
    """Filter a dataframe based on time boundaries.

    Args:
        df: Dataframe to filter
        start_time: Start time boundary (inclusive)
        end_time: End time boundary (inclusive)

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
        filtered_df = df[(df[time_col] >= start_time) & (df[time_col] <= end_time)]
    elif "open_time" in df.columns:
        time_col = "open_time"
        filtered_df = df[(df[time_col] >= start_time) & (df[time_col] <= end_time)]
    else:
        # If neither in columns, assume the index is the time
        # This handles cases where 'open_time' is already set as the index
        filtered_df = df[(df.index >= start_time) & (df.index <= end_time)]

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

    logger.debug(
        f"Aligned boundaries: {start_time} -> {aligned_start}, {end_time} -> {aligned_end} (interval: {interval.value})"
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

                # For close_time, add adjustment to match API behavior
                if col == "close_time":
                    # Close time should be 1 microsecond before the next interval
                    df[col] = df[col] + pd.Timedelta(microseconds=-1)

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

            # Ensure close_time exists (calculate it if not provided)
            if "close_time" not in df.columns:
                logger.debug("Calculating close_time from index values")
                # Use 1 second by default for missing close times
                df["close_time"] = (
                    df.index + pd.Timedelta(seconds=1) - pd.Timedelta(microseconds=1)
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
