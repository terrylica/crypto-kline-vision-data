#!/usr/bin/env python
"""Timestamp conversion utilities for market data.

This module provides functions for detecting, converting, and standardizing
timestamps between different formats (milliseconds, microseconds) and
ensuring timezone awareness.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from time_utils.py for modularity
# polars-exception: extracted from existing pandas-based time_utils.py - migration is separate task
"""

from datetime import datetime, timezone
from typing import Literal

import numpy as np
import pandas as pd

from data_source_manager.utils.config import (
    CANONICAL_CLOSE_TIME,
    CANONICAL_INDEX_NAME,
    MICROSECOND_DIGITS,
    MILLISECOND_DIGITS,
    TIMESTAMP_PRECISION,
)
from data_source_manager.utils.loguru_setup import logger

# Constants for timestamp format detection
TIMESTAMP_UNIT = "us"  # Default unit for timestamp parsing

# Timestamp unit type
TimestampUnit = Literal["ms", "us"]  # Supported timestamp units

__all__ = [
    "MICROSECOND_DIGITS",
    "MILLISECOND_DIGITS",
    "TIMESTAMP_UNIT",
    "TimestampUnit",
    "datetime_to_milliseconds",
    "detect_timestamp_unit",
    "enforce_utc_timezone",
    "milliseconds_to_datetime",
    "standardize_timestamp_precision",
    "validate_timestamp_unit",
]


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

    # MEMORY OPTIMIZATION: Operate directly on input DataFrame
    # All known callers reassign the result: df = standardize_timestamp_precision(df)
    # The caller's original reference is overwritten, so copy is unnecessary.
    # Source: docs/adr/2026-01-30-claude-code-infrastructure.md (memory efficiency refactoring)
    result_df = df

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
                # Use numpy array with pd.to_datetime to avoid N Timestamp object allocations
                timestamps_ms = np.array([int(ts.timestamp() * 1000) for ts in result_df.index], dtype="int64")
                new_index = pd.to_datetime(timestamps_ms, unit="ms", utc=True)
                new_index.name = result_df.index.name
                result_df.index = new_index

            elif current_precision == "ms" and TIMESTAMP_PRECISION == "us":
                # Convert from milliseconds to microseconds (add zeros)
                # Use numpy array with pd.to_datetime to avoid N Timestamp object allocations
                timestamps_us = np.array([int(ts.timestamp() * 1000000) for ts in result_df.index], dtype="int64")
                new_index = pd.to_datetime(timestamps_us, unit="us", utc=True)
                new_index.name = result_df.index.name
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
