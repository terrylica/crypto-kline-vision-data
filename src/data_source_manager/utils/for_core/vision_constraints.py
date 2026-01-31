#!/usr/bin/env python
# polars-exception: Vision constraints work with pandas for timestamp validation
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""Vision constraints and utilities for the Binance Vision API.

This module provides constraints and utility functions specific to the Binance
Vision API, leveraging centralized definitions from the utils modules for common
functionality to maintain DRY principles.
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Final,
    NamedTuple,
    NewType,
    TypeVar,
)

import httpx
import pandas as pd
import pyarrow as pa

from data_source_manager.utils.cache_validator import CacheKeyManager
from data_source_manager.utils.config import (
    CANONICAL_INDEX_NAME,
    CONSOLIDATION_DELAY,
    DEFAULT_TIMEZONE,
    ERROR_TYPES,
    FILE_EXTENSIONS,
    VISION_DATA_DELAY_HOURS,
    FileType,
)
from data_source_manager.utils.market_constraints import MarketType, get_market_symbol_format
from data_source_manager.utils.time_utils import (
    MICROSECOND_DIGITS,
    MILLISECOND_DIGITS,
    TimestampUnit,
    detect_timestamp_unit,
    enforce_utc_timezone,
    validate_timestamp_unit,
)

# Import centralized validation utilities
from data_source_manager.utils.validation import DataFrameValidator, DataValidation

# Type definitions for semantic clarity and safety
TimeseriesIndex = NewType("TimeseriesIndex", pd.DatetimeIndex)
CachePath = NewType("CachePath", Path)

# Define cache schema for Arrow files
CACHE_SCHEMA: Final[dict[str, pa.DataType]] = {
    "open_time": pa.timestamp("ns", tz="UTC"),
    "open": pa.float64(),
    "high": pa.float64(),
    "low": pa.float64(),
    "close": pa.float64(),
    "volume": pa.float64(),
    "close_time": pa.timestamp("ns", tz="UTC"),
    "quote_asset_volume": pa.float64(),
    "count": pa.int64(),
    "taker_buy_volume": pa.float64(),
    "taker_buy_quote_volume": pa.float64(),
}

# Constraint constants
FILES_PER_DAY: int = 2

# Type variable for DataFrame with enforced index
T = TypeVar("T")

# File constraints
MIN_VALID_FILE_SIZE: Final[int] = 1024  # 1KB minimum for valid data files
MAX_CACHE_AGE: Final[timedelta] = timedelta(days=30)  # Maximum age before cache revalidation
METADATA_UPDATE_INTERVAL: Final[timedelta] = timedelta(minutes=5)


# File extensions as a class with proper type annotations
class FileExtensions(NamedTuple):
    """Standard file extensions for Vision data."""

    DATA: str = FILE_EXTENSIONS["DATA"]
    CHECKSUM: str = FILE_EXTENSIONS["CHECKSUM"]
    CACHE: str = FILE_EXTENSIONS["CACHE"]
    METADATA: str = FILE_EXTENSIONS["METADATA"]


# Re-export for convenience
__all__ = [
    "CONSOLIDATION_DELAY",
    "MICROSECOND_DIGITS",
    "MILLISECOND_DIGITS",
    "CachePath",
    "FileExtensions",
    "FileType",
    "TimeseriesIndex",
    "TimestampUnit",
    "classify_error",
    "detect_timestamp_unit",
    "enforce_utc_timestamp",
    "get_cache_path",
    "get_vision_url",
    "is_data_likely_available",
    "is_date_too_fresh_for_vision",
    "validate_column_names",
    "validate_data_availability",
    "validate_dataframe_integrity",
    "validate_symbol_format",
    "validate_time_boundaries",
    "validate_timestamp_unit",
]


# Error classification
def classify_error(error: Exception) -> str:
    """Classify an error into a standard error type.

    Args:
        error: Exception to classify

    Returns:
        Standardized error type string
    """
    if isinstance(error, httpx.HTTPError):
        return ERROR_TYPES["NETWORK"]
    if isinstance(error, OSError):
        return ERROR_TYPES["FILE_SYSTEM"]
    if isinstance(error, ValueError | TypeError):
        return ERROR_TYPES["VALIDATION"]
    if isinstance(error, pa.ArrowInvalid):
        return ERROR_TYPES["DATA_INTEGRITY"]
    return ERROR_TYPES["VALIDATION"]


def is_date_too_fresh_for_vision(date: datetime, current_time: datetime | None = None) -> bool:
    """Check if a date is too recent for reliable Vision API data.

    The Vision API typically has a delay of VISION_DATA_DELAY_HOURS before data is available.
    This function helps determine if a date is within that window, indicating that
    failures for that date are expected and should not be treated as critical errors
    or trigger excessive retries.

    Args:
        date: The date to check (will be treated as the end of the period)
        current_time: Optional current time for testing, defaults to now() in UTC

    Returns:
        bool: True if the date is too fresh (within VISION_DATA_DELAY_HOURS of current time),
              False if the date is expected to be available in Vision API
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc)

    # Convert to timezone-aware datetimes if they're not already
    if date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)

    # Calculate the cutoff time for "freshness"
    vision_delay = timedelta(hours=VISION_DATA_DELAY_HOURS)
    cutoff_time = current_time - vision_delay

    # Return True if date is fresher than the cutoff time
    return date > cutoff_time


def get_vision_url(
    symbol: str,
    interval: str,
    date: datetime,
    file_type: FileType = FileType.DATA,
    market_type: str = "spot",
) -> str:
    """Get Binance Vision API URL for the given parameters.

    Args:
        symbol: Trading pair symbol
        interval: Kline interval
        date: Date to fetch
        file_type: File type (DATA or CHECKSUM)
        market_type: Market type (spot, futures_usdt, futures_coin)

    Returns:
        Full URL to the file
    """
    # Format date string
    date_str = date.strftime("%Y-%m-%d")

    from data_source_manager.utils.loguru_setup import logger

    logger.debug(f"Creating Vision API URL for {symbol} {interval} on {date_str} (market: {market_type})")

    # Determine base URL
    base_url = "https://data.binance.vision"

    # Determine path components based on market type
    # Convert market type to lowercase for consistency
    market_type_str = market_type.lower()

    if market_type_str == "spot":
        market_path = "spot"
        market_enum = MarketType.SPOT
    elif market_type_str in ["futures_usdt", "um"]:
        market_path = "futures/um"
        market_enum = MarketType.FUTURES_USDT
    elif market_type_str in ["futures_coin", "cm"]:
        market_path = "futures/cm"
        market_enum = MarketType.FUTURES_COIN
    else:
        raise ValueError(f"Unsupported market type: {market_type}")

    # Use the centralized function to transform the symbol
    symbol = get_market_symbol_format(symbol, market_enum)

    # Construct file name
    file_name = f"{symbol}-{interval}-{date_str}.zip"

    # Add suffix for checksum file - use .zip.CHECKSUM format
    if file_type == FileType.CHECKSUM:
        # The correct format is .zip.CHECKSUM, not just .CHECKSUM
        file_name = f"{symbol}-{interval}-{date_str}.zip.CHECKSUM"

    # Construct full URL
    url = f"{base_url}/data/{market_path}/daily/klines/{symbol}/{interval}/{file_name}"

    logger.debug(f"Generated Vision API URL: {url}")

    # Save URLs to file for debugging - write to /tmp to avoid issues with permissions
    try:
        debug_file = "/tmp/vision_api_urls.txt"
        with open(debug_file, "a") as f:
            f.write(f"{url}\n")
        logger.debug(f"Saved URL to {debug_file}")
    except (OSError, PermissionError) as e:
        logger.debug(f"Failed to save URL to debug file: {e}")

    return url


def is_data_likely_available(target_date: datetime) -> bool:
    """Check if data is likely to be available for a given date.

    Args:
        target_date: Date to check availability for

    Returns:
        True if data is likely available, False otherwise
    """
    # Ensure timezone-aware datetime
    target_date = enforce_utc_timezone(target_date)
    now = datetime.now(DEFAULT_TIMEZONE)

    # Data is likely available if it's older than the consolidation delay
    return (now - target_date) > CONSOLIDATION_DELAY


def validate_data_availability(start_time: datetime, end_time: datetime) -> None:
    """Validate that data is likely to be available for the requested time range.

    Args:
        start_time: Start time of the data
        end_time: End time of the data

    Raises:
        Warning if data within the buffer period is requested
    """
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    now = datetime.now(DEFAULT_TIMEZONE)
    cutoff = now - CONSOLIDATION_DELAY

    if end_time > cutoff:
        logging.warning(
            f"Requested data includes recent time ({end_time}) that may not be fully consolidated. "
            f"Data is typically available with a {CONSOLIDATION_DELAY} delay."
        )


def validate_time_boundaries(df: pd.DataFrame, start_time: datetime, end_time: datetime) -> None:
    """Validate that DataFrame covers the requested time range."""
    # Use centralized utility from DataValidation
    DataValidation.validate_dataframe_time_boundaries(df, start_time, end_time)


def validate_symbol_format(symbol: str) -> None:
    """Validate trading pair symbol format."""
    # Use centralized utility
    DataValidation.validate_symbol_format(symbol)


def validate_dataframe_integrity(df: pd.DataFrame) -> None:
    """Validate DataFrame structure and integrity."""
    # Use centralized DataFrameValidator
    DataFrameValidator.validate_dataframe(df)


def get_cache_path(cache_dir: Path, symbol: str, interval: str, date: datetime) -> Path:
    """Generate standardized cache file path."""
    # Use centralized CacheKeyManager
    return CacheKeyManager.get_cache_path(cache_dir, symbol, interval, date)


def enforce_utc_timestamp(dt: datetime) -> datetime:
    """Ensure timestamp is UTC."""
    # Use the direct function instead of TimeRangeManager
    return enforce_utc_timezone(dt)


def validate_column_names(columns: list[str]) -> list[str]:
    """Validate column names don't conflict with index."""
    if not isinstance(columns, list):
        raise TypeError(f"Expected list of strings, got {type(columns)}")
    if not all(isinstance(col, str) for col in columns):
        raise TypeError("All column names must be strings")
    if CANONICAL_INDEX_NAME in columns:
        raise ValueError(f"{CANONICAL_INDEX_NAME} is reserved for index")
    return columns


# Create logger
logger = logging.getLogger(__name__)
