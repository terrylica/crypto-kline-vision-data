#!/usr/bin/env python
"""Centralized validation utilities for data integrity and constraints.

This module consolidates validation logic from various modules including:
- validation.py
- api_boundary_validator.py
- cache_validator.py

It provides a unified interface for validating:
- Data structures (DataFrames)
- API boundaries
- Cache integrity
- File integrity
- Symbol and interval formats
"""

import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple, NamedTuple
import re
from dataclasses import dataclass

import pandas as pd
import numpy as np

from utils.logger_setup import logger
from utils.market_constraints import MarketType, Interval
from utils.time_utils import enforce_utc_timezone

# Column name constants
OHLCV_COLUMNS = ["open", "high", "low", "close", "volume"]
ALL_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
]

# Regex Patterns
TICKER_PATTERN = re.compile(r"^[A-Z0-9]{1,20}$")  # Match individual tickers
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{1,20}(USDT|BTC|ETH|BNB)$")  # Trading pairs
INTERVAL_PATTERN = re.compile(
    r"^(1s|1m|3m|5m|15m|30m|1h|2h|4h|6h|8h|12h|1d|3d|1w|1M)$"
)  # Valid intervals


# Import configuration constants
from utils.config import (
    CANONICAL_INDEX_NAME,
    MIN_VALID_FILE_SIZE,
    MAX_CACHE_AGE,
    OUTPUT_DTYPES,
    MAX_TIME_RANGE,
)

# Default symbol for tests
TEST_SYMBOL = "BTCUSDT"

# Error types for validation
ERROR_TYPES = {
    "VALIDATION": "data_validation",
    "FILE_INTEGRITY": "file_integrity",
    "API_BOUNDARY": "api_boundary",
    "METADATA": "metadata",
    "CHECKSUM": "checksum",
}


class ValidationError(Exception):
    """Custom exception for validation errors."""


class CacheValidationError(NamedTuple):
    """Standardized cache validation error details."""

    error_type: str
    message: str
    is_recoverable: bool


@dataclass
class ValidationOptions:
    """Options for data validation."""

    allow_empty: bool = False
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    interval: Optional[Interval] = None
    symbol: Optional[str] = None
    min_file_size: int = MIN_VALID_FILE_SIZE
    max_age: timedelta = MAX_CACHE_AGE


# ----- Basic Validation Functions -----


def validate_time_window(
    start_time: datetime, end_time: datetime, max_range: timedelta = MAX_TIME_RANGE
) -> Tuple[datetime, datetime]:
    """Validate time window against maximum allowed time range.

    Args:
        start_time: Start time
        end_time: End time
        max_range: Maximum allowed time range, defaults to MAX_TIME_RANGE from config

    Returns:
        Tuple of validated (start_time, end_time)

    Raises:
        ValueError: If time window exceeds maximum allowed range
    """
    # Ensure timezone-aware datetimes
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    # Validate time range
    validate_time_range(start_time, end_time, max_range)

    return start_time, end_time


def validate_interval(
    interval: Union[str, Interval], market_type: Optional[Union[str, MarketType]] = None
) -> Interval:
    """Validate and convert interval to Interval enum.

    Args:
        interval: String interval (e.g., '1m') or Interval enum
        market_type: Optional market type to validate interval against

    Returns:
        Validated Interval enum

    Raises:
        ValueError: If interval is invalid
    """
    if isinstance(interval, Interval):
        return interval

    if not isinstance(interval, str):
        raise ValueError(
            f"Interval must be a string or Interval enum, got {type(interval)}"
        )

    # Check against pattern
    if not INTERVAL_PATTERN.match(interval):
        raise ValueError(
            f"Invalid interval format: {interval}. "
            f"Must match pattern: {INTERVAL_PATTERN.pattern}"
        )

    # Convert market_type to enum
    if market_type is not None:
        if isinstance(market_type, str):
            # Try to match case-insensitively against MarketType
            market_type_upper = market_type.upper()
            if market_type_upper == "SPOT":
                market_type = MarketType.SPOT
            elif market_type_upper == "FUTURES":
                market_type = MarketType.FUTURES
                # Handle FUTURES market type validation
                if interval == "1s":
                    raise ValueError(
                        f"Invalid interval {interval} for {market_type_upper} market"
                    )
            else:
                raise ValueError(f"Unknown market type: {market_type}")

        # Check market-specific interval constraints
        if market_type == MarketType.SPOT and interval not in [
            "1s",
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
            "1M",
        ]:
            raise ValueError(f"Invalid interval {interval} for {market_type} market")

    try:
        return Interval(interval)
    except ValueError:
        raise ValueError(f"Unknown interval: {interval}")


def validate_symbol(symbol: str) -> str:
    """Validate trading pair symbol.

    Args:
        symbol: Trading pair symbol (e.g., 'BTCUSDT')

    Returns:
        Validated symbol

    Raises:
        ValueError: If symbol is invalid
    """
    if not isinstance(symbol, str):
        raise ValueError(f"Symbol must be a string, got {type(symbol)}")

    # Convert to uppercase
    symbol = symbol.upper()

    # Check against pattern
    if not SYMBOL_PATTERN.match(symbol):
        raise ValueError(
            f"Invalid symbol format: {symbol}. "
            f"Must match pattern: {SYMBOL_PATTERN.pattern}"
        )

    return symbol


def validate_symbol_format(symbol: str) -> str:
    """Validate a symbol string format.

    Args:
        symbol: Trading pair symbol (e.g., 'BTCUSDT')

    Returns:
        Validated symbol

    Raises:
        ValueError: If symbol format is invalid
    """
    if not symbol or not isinstance(symbol, str):
        raise ValueError(f"Symbol must be a non-empty string, got {type(symbol)}")

    if symbol != symbol.upper():
        raise ValueError(f"Symbol {symbol} should be uppercase")

    # Check against pattern
    if not SYMBOL_PATTERN.match(symbol):
        raise ValueError(
            f"Invalid symbol format: {symbol}. "
            f"Must match pattern: {SYMBOL_PATTERN.pattern}"
        )

    return symbol


def validate_time_range(
    start_time: datetime, end_time: datetime, max_range: Optional[timedelta] = None
) -> Tuple[datetime, datetime]:
    """Validate time range parameters.

    Args:
        start_time: Start time
        end_time: End time
        max_range: Maximum allowed time range

    Returns:
        Tuple of (start_time, end_time) with normalized values

    Raises:
        ValueError: If time range is invalid
    """
    # Validate types
    if not isinstance(start_time, datetime):
        raise ValueError(
            f"start_time must be a datetime object, got {type(start_time)}"
        )
    if not isinstance(end_time, datetime):
        raise ValueError(f"end_time must be a datetime object, got {type(end_time)}")

    # Ensure timezone-aware datetimes
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    # Check for future dates
    now = datetime.now(timezone.utc)
    if start_time > now:
        raise ValueError(
            f"Start time ({start_time}) cannot be in the future (current time: {now})"
        )
    if end_time > now:
        raise ValueError(
            f"End time ({end_time}) cannot be in the future (current time: {now})"
        )

    # Check range validity
    if start_time >= end_time:
        raise ValueError(
            f"Start time must be before end time: {start_time} >= {end_time}"
        )

    # Check max range if specified
    if max_range is not None:
        actual_range = end_time - start_time
        if actual_range > max_range:
            raise ValueError(
                f"Time range exceeds maximum allowed: {actual_range} > {max_range}"
            )

    return start_time, end_time


def validate_dates(
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    relative_to: Optional[datetime] = None,
) -> Tuple[datetime, datetime]:
    """Validate and normalize date parameters.

    Args:
        start_time: Optional start time (defaults to 7 days before end_time)
        end_time: Optional end time (defaults to current time)
        relative_to: Optional reference time for relative calculations

    Returns:
        Tuple of (start_time, end_time) with normalized values

    Raises:
        ValueError: If date parameters are invalid
    """
    # Check timezone awareness if dates are provided
    if start_time is not None and start_time.tzinfo is None:
        raise ValueError("start_time must be timezone-aware")
    if end_time is not None and end_time.tzinfo is None:
        raise ValueError("end_time must be timezone-aware")
    if relative_to is not None and relative_to.tzinfo is None:
        raise ValueError("relative_to must be timezone-aware")

    # Set default reference time
    if relative_to is None:
        relative_to = datetime.now(timezone.utc)
    else:
        relative_to = enforce_utc_timezone(relative_to)

    # Set default end_time
    if end_time is None:
        end_time = relative_to
    else:
        end_time = enforce_utc_timezone(end_time)

    # Set default start_time
    if start_time is None:
        start_time = end_time - timedelta(days=7)
    else:
        start_time = enforce_utc_timezone(start_time)

    # Validate time range
    validate_time_range(start_time, end_time)

    return start_time, end_time


def is_data_likely_available(timestamp: datetime, buffer_hours: int = 24) -> bool:
    """Check if data is likely to be available for a given timestamp.

    Args:
        timestamp: The timestamp to check
        buffer_hours: Number of hours before now that data might not be available

    Returns:
        True if data is likely available, False if it might not be
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=buffer_hours)
    return timestamp < cutoff


def validate_data_availability(
    start_time: datetime, end_time: datetime, buffer_hours: int = 24
) -> None:
    """Validate that data is likely to be available for the requested time range.

    Args:
        start_time: Start time of the data
        end_time: End time of the data
        buffer_hours: Number of hours before now that data might not be available

    Raises:
        Warning if data within the buffer period is requested
    """
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=buffer_hours)

    if end_time > cutoff:
        logger.warning(
            f"Requested data includes recent time ({end_time}) that may not be fully consolidated. "
            f"Data is typically available with a {buffer_hours} hour delay."
        )


# ----- Data Availability Validation -----


def has_complete_data(
    df: pd.DataFrame, start_time: datetime, end_time: datetime, interval: Interval
) -> bool:
    """Check if DataFrame has complete data for a time range.

    Args:
        df: DataFrame to check
        start_time: Start time to check
        end_time: End time to check
        interval: Interval to check

    Returns:
        True if data is complete, False otherwise
    """
    if df.empty:
        return False

    # Ensure timezone-aware datetimes
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    # Filter DataFrame to time range
    filtered_df = df[(df.index >= start_time) & (df.index <= end_time)]

    if filtered_df.empty:
        return False

    # Get actual time range covered
    actual_start = filtered_df.index.min()
    actual_end = filtered_df.index.max()

    # Calculate expected number of records based on interval
    interval_micros = interval.to_micros()
    expected_span = (end_time - start_time).total_seconds() * 1_000_000
    expected_records = expected_span // interval_micros

    # Allow for one missing record at the end (common with exchange APIs)
    expected_records_min = expected_records - 1

    # Compare actual records to expected
    actual_records = len(filtered_df)

    # Check time boundaries
    start_aligned = abs((actual_start - start_time).total_seconds()) < 0.001
    end_aligned = actual_end <= end_time

    return start_aligned and end_aligned and actual_records >= expected_records_min


# ----- DataFrame Validation -----


def validate_dataframe(df: pd.DataFrame) -> None:
    """Validate DataFrame structure and integrity.

    Args:
        df: DataFrame to validate

    Raises:
        ValueError: If DataFrame structure is invalid
    """
    if df.empty:
        return

    # Check if index is DatetimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(
            f"DataFrame index must be DatetimeIndex, got {type(df.index).__name__}"
        )

    # Check if index is timezone-aware
    if df.index.tz is None:
        raise ValueError("DataFrame index must be timezone-aware")

    # Check if index is named correctly
    if df.index.name != CANONICAL_INDEX_NAME:
        raise ValueError(
            f"DataFrame index must be named '{CANONICAL_INDEX_NAME}', "
            f"got '{df.index.name}'"
        )

    # Check for duplicate indices
    if df.index.has_duplicates:
        raise ValueError("DataFrame index contains duplicate timestamps")

    # Check if index is sorted
    if not df.index.is_monotonic_increasing:
        raise ValueError("DataFrame index must be monotonically increasing")

    # Check for required columns
    required_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"DataFrame missing required columns: {missing_columns}")


def format_dataframe(
    df: pd.DataFrame, output_dtypes: Dict[str, str] = OUTPUT_DTYPES
) -> pd.DataFrame:
    """Format DataFrame to ensure consistent structure.

    Args:
        df: DataFrame to format
        output_dtypes: Expected data types for columns

    Returns:
        Formatted DataFrame
    """
    if df.empty:
        # Return an empty DataFrame with the correct structure and DatetimeIndex
        standard_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
        ]
        empty_df = pd.DataFrame(columns=standard_columns)
        empty_df.index = pd.DatetimeIndex(
            [], name=CANONICAL_INDEX_NAME, tz=timezone.utc
        )
        return empty_df

    # Create a copy to avoid modifying the original
    formatted_df = df.copy()

    # Ensure index is DatetimeIndex and timezone-aware
    if not isinstance(formatted_df.index, pd.DatetimeIndex):
        # Try to convert to DatetimeIndex
        try:
            formatted_df.index = pd.to_datetime(formatted_df.index)
        except Exception as e:
            raise ValueError(f"Cannot convert index to DatetimeIndex: {e}")

    # Ensure index is timezone-aware (convert to UTC if not)
    if formatted_df.index.tz is None:
        formatted_df.index = formatted_df.index.tz_localize(timezone.utc)
    elif formatted_df.index.tz != timezone.utc:
        formatted_df.index = formatted_df.index.tz_convert(timezone.utc)

    # Set index name
    formatted_df.index.name = CANONICAL_INDEX_NAME

    # Ensure all required columns exist
    standard_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]
    for column in standard_columns:
        if column not in formatted_df.columns:
            formatted_df[column] = np.nan

    # Convert data types if specified
    if output_dtypes:
        for column, dtype in output_dtypes.items():
            if column in formatted_df.columns:
                try:
                    formatted_df[column] = formatted_df[column].astype(dtype)
                except Exception as e:
                    logger.warning(f"Could not convert column {column} to {dtype}: {e}")

    # Sort by index
    if not formatted_df.index.is_monotonic_increasing:
        formatted_df = formatted_df.sort_index()

    # Remove duplicates if any
    if formatted_df.index.has_duplicates:
        formatted_df = formatted_df[~formatted_df.index.duplicated(keep="first")]

    return formatted_df


# ----- File Validation -----


def validate_file_integrity(
    file_path: Path,
    min_size: int = MIN_VALID_FILE_SIZE,
    max_age: timedelta = MAX_CACHE_AGE,
) -> Optional[Dict[str, Any]]:
    """Validate basic file integrity (existence, size, age).

    Args:
        file_path: Path to the file
        min_size: Minimum valid file size in bytes
        max_age: Maximum valid file age

    Returns:
        None if file is valid, or Dict with error details if invalid
    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    # Check file existence
    if not file_path.exists():
        return {
            "error_type": ERROR_TYPES["FILE_INTEGRITY"],
            "message": f"File {file_path} does not exist",
            "is_recoverable": True,
        }

    # Check file size
    file_size = file_path.stat().st_size
    if file_size < min_size:
        return {
            "error_type": ERROR_TYPES["FILE_INTEGRITY"],
            "message": f"File {file_path} is too small ({file_size} bytes < {min_size} bytes)",
            "is_recoverable": True,
        }

    # Check file age
    if max_age is not None:
        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
        file_age = datetime.now(timezone.utc) - file_mtime
        if file_age > max_age:
            return {
                "error_type": ERROR_TYPES["FILE_INTEGRITY"],
                "message": f"File {file_path} is too old (age: {file_age} > {max_age})",
                "is_recoverable": True,
            }

    return None


def validate_file_with_checksum(
    file_path: Path,
    expected_checksum: str = None,
    min_size: int = MIN_VALID_FILE_SIZE,
    max_age: timedelta = MAX_CACHE_AGE,
) -> bool:
    """Validate file integrity with optional checksum verification.

    Args:
        file_path: Path to the file
        expected_checksum: Expected checksum to validate against
        min_size: Minimum valid file size in bytes
        max_age: Maximum valid file age

    Returns:
        True if file passes all integrity checks, False otherwise
    """
    # Check basic integrity first
    integrity_result = validate_file_integrity(file_path, min_size, max_age)
    if integrity_result is not None:
        # Failed basic validation
        return False

    # If checksum validation is requested, perform it
    if expected_checksum:
        try:
            actual_checksum = calculate_checksum(file_path)
            return actual_checksum == expected_checksum
        except (IOError, OSError) as e:
            logger.error(f"Error calculating checksum for {file_path}: {e}")
            return False

    # If no checksum validation requested or it passed
    return True


def validate_cache_integrity(
    file_path: Union[str, Path],
    min_size: int = MIN_VALID_FILE_SIZE,
    max_age: Optional[timedelta] = MAX_CACHE_AGE,
) -> Optional[Dict[str, Any]]:
    """Validate cache file integrity.

    Args:
        file_path: Path to the cache file
        min_size: Minimum valid file size in bytes
        max_age: Maximum valid file age

    Returns:
        Dict with error details if invalid, None if valid
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    # Check file existence
    if not file_path.exists():
        return {
            "error_type": "file_missing",
            "message": f"Cache file {file_path} does not exist",
            "is_recoverable": True,
        }

    # Check file size
    file_size = file_path.stat().st_size
    if file_size < min_size:
        return {
            "error_type": "file_too_small",
            "message": f"Cache file {file_path} is too small ({file_size} bytes < {min_size} bytes)",
            "is_recoverable": True,
        }

    # Check file age if max_age is specified
    if max_age is not None:
        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
        file_age = datetime.now(timezone.utc) - file_mtime
        if file_age > max_age:
            return {
                "error_type": "file_too_old",
                "message": f"Cache file {file_path} is too old (age: {file_age} > {max_age})",
                "is_recoverable": True,
            }

    return None


def calculate_checksum(file_path: Path) -> str:
    """Calculate SHA-256 checksum of a file.

    Args:
        file_path: Path to the file

    Returns:
        Hexadecimal string of the SHA-256 checksum
    """
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


# ----- API Validation -----


class ApiBoundaryValidator:
    """Validates time boundaries and data ranges against actual Binance API behavior.

    This class helps validate whether a given time range is valid according to
    the Binance API, and whether a DataFrame contains the same data that the API
    would return for a given request.
    """

    def __init__(self, binance_client):
        """Initialize with a BinanceClient instance.

        Args:
            binance_client: Initialized BinanceClient for making API calls
        """
        self.client = binance_client

    async def is_valid_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> bool:
        """Check if a time range is valid for the Binance API.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: The data interval
            symbol: The trading pair symbol to check

        Returns:
            True if time range is valid, False otherwise
        """
        # Ensure timezone-aware datetimes
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)

        # First validate basic time range constraints
        try:
            validate_time_range(start_time, end_time)
        except ValueError as e:
            self.logger.warning(f"Invalid time range: {e}")
            return False

        # Test the range by making a minimal API call
        try:
            # Just request a single record to minimize API load
            api_data = await self._call_api(
                start_time, end_time, interval, limit=1, symbol=symbol
            )
            return len(api_data) > 0
        except Exception as e:
            self.logger.warning(f"API call failed: {e}")
            return False

    async def get_api_boundaries(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> Dict[str, Any]:
        """Call Binance API and determine the actual boundaries of returned data.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: The data interval
            symbol: The trading pair symbol to check

        Returns:
            Dictionary with API boundary information:
            - api_start_time: Actual start time from API data
            - api_end_time: Actual end time from API data
            - record_count: Number of records returned
            - matches_request: Whether API boundaries match requested boundaries
        """
        # Ensure timezone-aware datetimes
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)

        try:
            # Call API to get data for the requested range
            api_data = await self._call_api(
                start_time, end_time, interval, limit=1000, symbol=symbol
            )

            if not api_data:
                logger.warning("API returned no data for the requested range")
                return {
                    "api_start_time": None,
                    "api_end_time": None,
                    "record_count": 0,
                    "matches_request": False,
                }

            # Extract timestamps from first and last records
            first_timestamp_ms = api_data[0][0]
            last_timestamp_ms = api_data[-1][0]

            # Convert to datetime objects
            api_start_time = datetime.fromtimestamp(
                first_timestamp_ms / 1000, tz=timezone.utc
            )
            api_end_time = datetime.fromtimestamp(
                last_timestamp_ms / 1000, tz=timezone.utc
            )

            # Check if API boundaries match requested boundaries (within millisecond precision)
            start_matches = abs((api_start_time - start_time).total_seconds()) < 0.001
            end_within_range = api_end_time <= end_time

            result = {
                "api_start_time": api_start_time,
                "api_end_time": api_end_time,
                "record_count": len(api_data),
                "matches_request": start_matches and end_within_range,
            }

            logger.debug(
                f"API boundaries found. Start: {api_start_time}, End: {api_end_time}, "
                f"Count: {len(api_data)}, Matches Request: {start_matches and end_within_range}"
            )

            return result

        except Exception as e:
            logger.error(f"Error determining API boundaries: {e}")
            return {
                "api_start_time": None,
                "api_end_time": None,
                "record_count": 0,
                "matches_request": False,
                "error": str(e),
            }

    async def _call_api(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        limit: int = 1000,
        symbol: str = "BTCUSDT",
    ) -> List:
        """Make a call to the Binance API to get kline data.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: The data interval
            limit: Maximum number of records to retrieve
            symbol: The trading pair symbol

        Returns:
            List of klines from the API
        """
        # Convert timestamps to milliseconds
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)

        # Call API
        try:
            result = await self.client.get_klines(
                symbol=symbol,
                interval=interval.value,
                startTime=start_ms,
                endTime=end_ms,
                limit=limit,
            )
            return result
        except Exception as e:
            logger.error(f"API call failed: {e}")
            raise

    def estimate_record_count(
        self, start_time: datetime, end_time: datetime, interval: Interval
    ) -> int:
        """Estimate number of records between two timestamps for a given interval.

        Args:
            start_time: Start time
            end_time: End time
            interval: Data interval

        Returns:
            Estimated number of records
        """
        # Ensure timezone-aware datetimes
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)

        # Calculate expected number of records based on interval
        interval_micros = interval.to_micros()
        time_span_micros = (end_time - start_time).total_seconds() * 1_000_000

        # Round up to account for partial intervals
        estimated_records = int(time_span_micros // interval_micros) + 1

        return estimated_records

    async def does_data_range_match_api_response(
        self,
        df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> bool:
        """Check if DataFrame data range matches what API would return.

        Args:
            df: DataFrame to validate
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: The data interval
            symbol: The trading pair symbol to check

        Returns:
            True if DataFrame matches API response, False otherwise
        """
        # First check if DataFrame is empty
        if df.empty:
            # If empty, check if API would return any data
            api_response = await self._call_api(
                start_time, end_time, interval, symbol=symbol
            )
            return len(api_response) == 0

        # Get API boundaries to check time alignment
        api_boundaries = await self.get_api_boundaries(
            start_time, end_time, interval, symbol
        )

        # If API couldn't return valid boundaries, we can't validate
        if not api_boundaries.get("api_start_time"):
            logger.warning("Couldn't determine API boundaries, validation skipped")
            return False

        # Compare first and last timestamps
        df_start_time = df.index.min()
        df_end_time = df.index.max()

        api_start_time = api_boundaries["api_start_time"]
        api_end_time = api_boundaries["api_end_time"]

        # Allow a small tolerance (1 millisecond) for timestamp comparisons
        start_time_match = abs((df_start_time - api_start_time).total_seconds()) < 0.001
        end_time_match = abs((df_end_time - api_end_time).total_seconds()) < 0.001

        # Also check record count
        df_record_count = len(df)
        api_record_count = api_boundaries["record_count"]
        record_count_match = df_record_count == api_record_count

        result = start_time_match and end_time_match and record_count_match

        logger.debug(
            f"DataFrame validation result: {'Valid' if result else 'Invalid'} "
            f"(Start: {start_time_match}, End: {end_time_match}, "
            f"Count: {df_record_count} vs {api_record_count} - {record_count_match})"
        )

        return result


class ApiValidator:
    """Validates data against Binance API behavior.

    This class provides validation functionality against actual Binance API behavior,
    consolidating validation logic from ApiBoundaryValidator and other modules.
    """

    def __init__(self, api_boundary_validator: Optional[ApiBoundaryValidator] = None):
        """Initialize the ApiValidator.

        Args:
            api_boundary_validator: Optional ApiBoundaryValidator for API boundary validations
        """
        self.api_boundary_validator = api_boundary_validator

    async def validate_api_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Union[str, Interval],
        symbol: str = "BTCUSDT",
    ) -> bool:
        """Validates time range against Binance API boundaries.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: The data interval as string or Interval enum
            symbol: The trading pair symbol to check

        Returns:
            True if the time range is valid for the API, False otherwise

        Raises:
            ValueError: If ApiBoundaryValidator is not provided
        """
        if not self.api_boundary_validator:
            raise ValueError(
                "ApiBoundaryValidator is required for API time range validation"
            )

        # Convert interval to Interval enum if needed
        if isinstance(interval, str):
            interval = Interval(interval)

        return await self.api_boundary_validator.is_valid_time_range(
            start_time, end_time, interval, symbol=symbol
        )

    async def get_api_aligned_boundaries(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Union[str, Interval],
        symbol: str = "BTCUSDT",
    ) -> Dict[str, Any]:
        """Get API-aligned boundaries for the given time range and interval.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: The data interval as string or Interval enum
            symbol: The trading pair symbol to check

        Returns:
            Dictionary with API-aligned boundaries

        Raises:
            ValueError: If ApiBoundaryValidator is not provided
        """
        if not self.api_boundary_validator:
            raise ValueError(
                "ApiBoundaryValidator is required for API boundary alignment"
            )

        # Convert interval to Interval enum if needed
        if isinstance(interval, str):
            interval = Interval(interval)

        return await self.api_boundary_validator.get_api_boundaries(
            start_time, end_time, interval, symbol=symbol
        )

    async def does_data_range_match_api_response(
        self,
        df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> bool:
        """Check if DataFrame data range matches what API would return.

        Args:
            df: DataFrame to validate
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: The data interval
            symbol: The trading pair symbol to check

        Returns:
            True if DataFrame matches API response, False otherwise

        Raises:
            ValueError: If ApiBoundaryValidator is not provided
        """
        if not self.api_boundary_validator:
            raise ValueError("ApiBoundaryValidator is required for API validation")

        return await self.api_boundary_validator.does_data_range_match_api_response(
            df, start_time, end_time, interval, symbol
        )


# ----- Comprehensive Data Validation -----


class DataValidator:
    """Consolidated data validation utilities.

    This class combines validation functionality from multiple sources:
    - DataFrameValidator
    - API validation
    - Cache validation
    """

    def __init__(self, api_validator: Optional[ApiValidator] = None):
        """Initialize the DataValidator.

        Args:
            api_validator: Optional ApiValidator for API validations
        """
        self.api_validator = api_validator

    async def validate_data(
        self,
        df: pd.DataFrame,
        options: ValidationOptions = None,
    ) -> Optional[CacheValidationError]:
        """Validate data DataFrame structure and content.

        Args:
            df: DataFrame to validate
            options: Validation options

        Returns:
            ValidationError if invalid, None if valid
        """
        # Use default options if none provided
        if options is None:
            options = ValidationOptions()

        # Check if DataFrame is empty
        if df.empty and not options.allow_empty:
            return CacheValidationError(
                ERROR_TYPES["VALIDATION"],
                "DataFrame is empty",
                True,
            )

        # Validate DataFrame structure
        try:
            validate_dataframe(df)
        except ValueError as e:
            return CacheValidationError(
                ERROR_TYPES["VALIDATION"],
                f"DataFrame validation failed: {e}",
                False,
            )

        # Validate API boundaries if validator is available and we have all required parameters
        if (
            self.api_validator
            and options.start_time
            and options.end_time
            and options.interval
            and not df.empty
        ):
            try:
                # Use ApiValidator to validate data matches REST API behavior
                is_api_aligned = (
                    await self.api_validator.does_data_range_match_api_response(
                        df,
                        options.start_time,
                        options.end_time,
                        options.interval,
                        options.symbol or TEST_SYMBOL,
                    )
                )

                if not is_api_aligned:
                    return CacheValidationError(
                        ERROR_TYPES["API_BOUNDARY"],
                        "Data boundaries do not match REST API behavior",
                        True,  # Recoverable by refetching
                    )

                logger.debug("Data boundaries match REST API behavior")
            except (ValueError, RuntimeError, IOError, ConnectionError) as e:
                logger.warning("API boundary validation failed: %s", e)
                # Don't fail validation just because API validation failed
                # This keeps the system robust even if we can't reach the API

        return None

    async def align_data_to_api_boundaries(
        self,
        df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> pd.DataFrame:
        """Align data DataFrame to match API boundaries.

        Args:
            df: DataFrame to align
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: The data interval
            symbol: The trading pair symbol to check

        Returns:
            DataFrame aligned to API boundaries

        Raises:
            ValueError: If api_validator is not available
        """
        if not self.api_validator:
            raise ValueError("ApiValidator is required for API boundary alignment")

        # Get API-aligned boundaries
        boundaries = await self.api_validator.get_api_aligned_boundaries(
            start_time, end_time, interval, symbol
        )

        # Extract API start and end times
        api_start_time = boundaries.get("api_start_time")
        api_end_time = boundaries.get("api_end_time")

        if not api_start_time or not api_end_time:
            logger.warning(
                "Could not determine API boundaries, returning original DataFrame"
            )
            return df

        # Filter DataFrame to match API boundaries
        aligned_df = df[
            (df.index >= api_start_time) & (df.index <= api_end_time)
        ].copy()

        logger.debug(
            f"Successfully aligned DataFrame to API boundaries: {len(df)} rows remain"
        )

        return aligned_df


# ----- Time Boundary Validation -----


def validate_dataframe_time_boundaries(
    df: pd.DataFrame, start_time: datetime, end_time: datetime
) -> None:
    """Validate that DataFrame covers the requested time range.

    Args:
        df: DataFrame to validate
        start_time: Start time boundary
        end_time: End time boundary

    Raises:
        ValueError: If DataFrame doesn't cover the time range
    """
    if df.empty:
        return  # Empty DataFrame cannot be validated against time boundaries

    # Ensure timezone-aware datetimes
    start_time = enforce_utc_timezone(start_time)
    end_time = enforce_utc_timezone(end_time)

    # Get actual time range covered
    actual_start = df.index.min()
    actual_end = df.index.max()

    # Check time boundaries (with small tolerance for floating point precision)
    if actual_start > start_time + timedelta(microseconds=1000):
        raise ValueError(
            f"DataFrame starts at {actual_start}, which is after the requested start time {start_time}"
        )

    if actual_end < end_time - timedelta(microseconds=1000):
        raise ValueError(
            f"DataFrame ends at {actual_end}, which is before the requested end time {end_time}"
        )


def validate_date_range_for_api(
    start_time: datetime, end_time: datetime, max_future_seconds: int = 0
) -> Tuple[bool, str]:
    """Validate a date range for API requests to prevent requesting future data.

    Args:
        start_time: The start time for the request
        end_time: The end time for the request
        max_future_seconds: Maximum number of seconds allowed in the future (default: 0)

    Returns:
        Tuple of (is_valid, error_message)
    """
    now = datetime.now(timezone.utc)
    allowed_future = now + timedelta(seconds=max_future_seconds)

    if start_time > allowed_future:
        return False, f"Start time {start_time.isoformat()} is in the future"

    if end_time > allowed_future:
        return False, f"End time {end_time.isoformat()} is in the future"

    if start_time > end_time:
        return (
            False,
            f"Start time {start_time.isoformat()} is after end time {end_time.isoformat()}",
        )

    return True, ""
