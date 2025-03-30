#!/usr/bin/env python

from typing import TypeVar, NewType, Final, Optional, NamedTuple, Literal, Dict
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
from enum import Enum, auto
import logging

import pyarrow as pa
from curl_cffi.requests.errors import RequestsError

# Import centralized validation utilities
from utils.validation import DataValidation, DataFrameValidator
from utils.cache_validator import (
    CacheKeyManager,
)
from utils.time_utils import enforce_utc_timezone
from utils.validation_utils import (
    validate_time_range,
    validate_dataframe_time_boundaries,
)
from utils.config import (
    CANONICAL_INDEX_NAME,
    DEFAULT_TIMEZONE,
    ERROR_TYPES,
    CONSOLIDATION_DELAY,
)  # Import ERROR_TYPES from central config

# Type definitions for semantic clarity and safety
TimeseriesIndex = NewType("TimeseriesIndex", pd.DatetimeIndex)
CachePath = NewType("CachePath", Path)
TimestampUnit = Literal["ms", "us"]  # Supported timestamp units

# Define cache schema for Arrow files
CACHE_SCHEMA: Final[Dict[str, pa.DataType]] = {
    "open_time": pa.timestamp("ns", tz="UTC"),
    "open": pa.float64(),
    "high": pa.float64(),
    "low": pa.float64(),
    "close": pa.float64(),
    "volume": pa.float64(),
    "close_time": pa.timestamp("ns", tz="UTC"),
    "quote_asset_volume": pa.float64(),
    "number_of_trades": pa.int64(),
    "taker_buy_base_asset_volume": pa.float64(),
    "taker_buy_quote_asset_volume": pa.float64(),
}

# Constraint constants
MAX_CONCURRENT_DOWNLOADS: Final[int] = 50
FILES_PER_DAY: Final[int] = 2
# Import canonical values from config instead of redefining
# CANONICAL_INDEX_NAME: Final[str] = "open_time"  # Removed as it's now imported
# CANONICAL_TIMEZONE: Final[timezone] = timezone.utc  # We use DEFAULT_TIMEZONE from config

# Timestamp format detection thresholds
MILLISECOND_DIGITS: Final[int] = 13
MICROSECOND_DIGITS: Final[int] = 16

# Data availability constraints
# CONSOLIDATION_DELAY = timedelta(
#     hours=48
# )  # Time Binance needs to consolidate daily data - increased from 12h to 48h for safety margin

# Type variable for DataFrame with enforced index
T = TypeVar("T")


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
    elif digits == MILLISECOND_DIGITS:
        return "ms"
    else:
        raise ValueError(
            f"Unrecognized timestamp format with {digits} digits. "
            f"Expected {MILLISECOND_DIGITS} for milliseconds or "
            f"{MICROSECOND_DIGITS} for microseconds."
        )


def validate_timestamp_unit(unit: TimestampUnit) -> None:
    """Validate that the timestamp unit is supported.

    Args:
        unit: Timestamp unit to validate

    Raises:
        ValueError: If unit is not supported
    """
    if unit not in ("ms", "us"):
        raise ValueError(f"Unsupported timestamp unit: {unit}. Must be 'ms' or 'us'.")


# File management constraints
class FileType(Enum):
    """Types of files managed by Vision client."""

    DATA = auto()
    CHECKSUM = auto()
    CACHE = auto()
    METADATA = auto()


class FileExtensions(NamedTuple):
    """Standard file extensions for Vision data."""

    DATA: str = ".zip"
    CHECKSUM: str = ".CHECKSUM"
    CACHE: str = ".arrow"
    METADATA: str = ".json"


# File constraints
MIN_VALID_FILE_SIZE: Final[int] = 1024  # 1KB minimum for valid data files
MAX_CACHE_AGE: Final[timedelta] = timedelta(
    days=30
)  # Maximum age before cache revalidation
METADATA_UPDATE_INTERVAL: Final[timedelta] = timedelta(minutes=5)


# Error classification
def classify_error(error: Exception) -> str:
    """Classify an error into a standard error type.

    Args:
        error: Exception to classify

    Returns:
        Standardized error type string
    """
    if isinstance(error, (RequestsError,)):
        return ERROR_TYPES["NETWORK"]
    elif isinstance(error, OSError):
        return ERROR_TYPES["FILE_SYSTEM"]
    elif isinstance(error, (ValueError, TypeError)):
        return ERROR_TYPES["VALIDATION"]
    elif isinstance(error, pa.ArrowInvalid):
        return ERROR_TYPES["DATA_INTEGRITY"]
    else:
        return ERROR_TYPES["VALIDATION"]


def get_vision_url(
    symbol: str,
    interval: str,
    date: datetime,
    file_type: FileType,
    market_type: str = "spot",
) -> str:
    """Generate standard Vision URLs.

    Args:
        symbol: Trading pair symbol
        interval: Time interval
        date: Target date
        file_type: Type of file to generate URL for
        market_type: Market type (spot, futures_usdt, futures_coin)

    Returns:
        Formatted Vision URL
    """
    # Normalize market_type
    market_type = market_type.lower()

    # Map market types to URL paths
    market_path_mapping = {
        "spot": "spot",
        "futures_usdt": "futures/um",
        "futures_coin": "futures/cm",
    }

    if market_type not in market_path_mapping:
        raise ValueError(f"Invalid market type: {market_type}")

    market_path = market_path_mapping[market_type]

    base_url = f"https://data.binance.vision/data/{market_path}/daily/klines"
    date_str = date.strftime("%Y-%m-%d")
    file_name = f"{symbol}-{interval}-{date_str}"

    if file_type == FileType.DATA:
        return f"{base_url}/{symbol}/{interval}/{file_name}.zip"
    elif file_type == FileType.CHECKSUM:
        return f"{base_url}/{symbol}/{interval}/{file_name}.zip.CHECKSUM"
    else:
        raise ValueError(f"Invalid file type for Vision URL: {file_type}")


def is_data_likely_available(target_date: datetime) -> bool:
    """Check if data is likely to be available from Binance Vision.

    Args:
        target_date: Date to check for data availability

    Returns:
        True if data is likely available based on Binance Vision's constraints
    """
    # Use centralized validation utility
    return DataValidation.is_data_likely_available(target_date, CONSOLIDATION_DELAY)


def validate_data_availability(start_time: datetime, end_time: datetime) -> None:
    """Validate if data should be available for the given time range.

    Args:
        start_time: Start of time range
        end_time: End of time range

    Raises:
        ValueError: If data is definitely not available for the time range
    """
    # Use centralized validation utility
    DataValidation.validate_data_availability(start_time, end_time, CONSOLIDATION_DELAY)


class TimestampedDataFrame(pd.DataFrame):
    """DataFrame with enforced UTC timestamp index.

    This class enforces:
    1. Index must be DatetimeIndex
    2. Index must be timezone-aware and in UTC
    3. Index must be named 'open_time'
    4. Index must be monotonically increasing
    5. No duplicate indices allowed
    """

    def __init__(self, *args, **kwargs):
        """Initialize with DataFrame validation."""
        super().__init__(*args, **kwargs)
        self._validate_and_normalize_index()

    def _validate_and_normalize_index(self):
        """Validate and normalize the index to meet requirements."""
        # Convert index to DatetimeIndex if it's not already
        if not isinstance(self.index, pd.DatetimeIndex):
            try:
                # Try to convert the index to datetime
                self.index = pd.to_datetime(self.index, utc=True)
            except Exception as e:
                raise ValueError(f"Failed to convert index to DatetimeIndex: {e}")

        # Ensure index is timezone-aware and in UTC
        if self.index.tz is None:
            self.index = self.index.tz_localize(DEFAULT_TIMEZONE)
        elif self.index.tz != DEFAULT_TIMEZONE:
            self.index = self.index.tz_convert(DEFAULT_TIMEZONE)

        # Ensure index is named correctly
        if self.index.name != CANONICAL_INDEX_NAME:
            self.index.name = CANONICAL_INDEX_NAME

        # Use the DataFrameValidator for index validation
        # This avoids duplicating validation logic
        try:
            DataFrameValidator.validate_dataframe(self)
        except ValueError as e:
            # If there are duplicates or non-monotonic indices, handle them
            if self.index.has_duplicates:
                # Instead of just raising an error, we'll fix it
                logger.warning("Found duplicate indices, keeping first occurrence")
                # Create a new index without duplicates
                self.reset_index(inplace=True)
                self.drop_duplicates(
                    subset=[CANONICAL_INDEX_NAME], keep="first", inplace=True
                )
                self.set_index(CANONICAL_INDEX_NAME, inplace=True)

            if not self.index.is_monotonic_increasing:
                logger.warning("Index not monotonically increasing, sorting")
                self.sort_index(inplace=True)

            # After fixing, validate again to ensure it's correct
            DataFrameValidator.validate_dataframe(self)

    def __setitem__(self, key, value):
        """Override to prevent modification of index."""
        if key == CANONICAL_INDEX_NAME:
            raise ValueError(
                f"Cannot modify {CANONICAL_INDEX_NAME} directly - it is reserved for index"
            )
        super().__setitem__(key, value)


def validate_cache_path(path: Path) -> CachePath:
    """Validate and convert a Path to a CachePath."""
    if not isinstance(path, Path):
        raise TypeError(f"Expected Path object, got {type(path)}")
    if not path.suffix == ".arrow":
        raise ValueError("Cache path must have .arrow extension")
    return CachePath(path)


def validate_time_range(
    start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
) -> tuple[Optional[datetime], Optional[datetime]]:
    """Validate and normalize time range parameters."""
    # Use centralized utility from validation_utils
    return validate_time_range(start_time, end_time)


def validate_time_boundaries(
    df: pd.DataFrame, start_time: datetime, end_time: datetime
) -> None:
    """Validate that DataFrame covers the requested time range."""
    # Use centralized utility from validation_utils
    validate_dataframe_time_boundaries(df, start_time, end_time)


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


logger = logging.getLogger(__name__)
