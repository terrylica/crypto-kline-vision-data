#!/usr/bin/env python

from typing import TypeVar, NewType, Final, Optional, NamedTuple, Literal
from datetime import datetime, timezone, timedelta
import pandas as pd
from pathlib import Path
from enum import Enum, auto
import httpx
import pyarrow as pa
import hashlib
import logging
import numpy as np

# Import centralized validation utilities
from utils.validation import DataValidation, DataFrameValidator
from utils.cache_validator import (
    CacheValidator,
    CacheKeyManager,
    SafeMemoryMap,
    CacheValidationError,
)

# Type definitions for semantic clarity and safety
TimeseriesIndex = NewType("TimeseriesIndex", pd.DatetimeIndex)
CachePath = NewType("CachePath", Path)
TimestampUnit = Literal["ms", "us"]  # Supported timestamp units

# Constraint constants
MAX_CONCURRENT_DOWNLOADS: Final[int] = 13
FILES_PER_DAY: Final[int] = 2
CANONICAL_INDEX_NAME: Final[str] = "open_time"
CANONICAL_TIMEZONE: Final[timezone] = timezone.utc

# Timestamp format detection thresholds
MILLISECOND_DIGITS: Final[int] = 13
MICROSECOND_DIGITS: Final[int] = 16

# Data availability constraints
CONSOLIDATION_DELAY = timedelta(
    hours=48
)  # Time Binance needs to consolidate daily data - increased from 12h to 48h for safety margin

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
class VisionErrorType(Enum):
    """Classification of Vision client errors."""

    NETWORK = "network_error"
    FILE_SYSTEM = "file_system_error"
    DATA_INTEGRITY = "data_integrity_error"
    CACHE_INVALID = "cache_invalid"
    VALIDATION = "validation_error"
    AVAILABILITY = "availability_error"


def classify_error(error: Exception) -> VisionErrorType:
    """Classify an error into a standard Vision error type.

    Args:
        error: Exception to classify

    Returns:
        Standardized error type
    """
    if isinstance(error, (httpx.RequestError, httpx.HTTPStatusError)):
        return VisionErrorType.NETWORK
    elif isinstance(error, OSError):
        return VisionErrorType.FILE_SYSTEM
    elif isinstance(error, (ValueError, TypeError)):
        return VisionErrorType.VALIDATION
    elif isinstance(error, pa.ArrowInvalid):
        return VisionErrorType.DATA_INTEGRITY
    else:
        return VisionErrorType.VALIDATION


def validate_cache_integrity(
    cache_path: Path,
    max_age: timedelta = MAX_CACHE_AGE,
    min_size: int = MIN_VALID_FILE_SIZE,
) -> Optional[CacheValidationError]:
    """Validate cache file integrity.

    Args:
        cache_path: Path to cache file
        max_age: Maximum allowed age of cache
        min_size: Minimum valid file size

    Returns:
        Error details if validation fails, None if valid
    """
    # Use centralized CacheValidator
    return CacheValidator.validate_cache_integrity(cache_path, max_age, min_size)


def get_vision_url(
    symbol: str, interval: str, date: datetime, file_type: FileType
) -> str:
    """Generate standard Vision URLs.

    Args:
        symbol: Trading pair symbol
        interval: Time interval
        date: Target date
        file_type: Type of file to generate URL for

    Returns:
        Formatted Vision URL
    """
    base_url = "https://data.binance.vision/data/spot/daily/klines"
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
            self.index = self.index.tz_localize(CANONICAL_TIMEZONE)
        elif self.index.tz != CANONICAL_TIMEZONE:
            self.index = self.index.tz_convert(CANONICAL_TIMEZONE)

        # Ensure index is named correctly
        if self.index.name != CANONICAL_INDEX_NAME:
            self.index.name = CANONICAL_INDEX_NAME

        # Validate index properties
        if not self.index.is_monotonic_increasing:
            raise ValueError("Index must be monotonically increasing")
        if self.index.has_duplicates:
            raise ValueError("Index must not contain duplicates")

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


def enforce_utc_timestamp(dt: datetime) -> datetime:
    """Ensure timestamp is UTC."""
    # Use centralized utility
    return DataValidation.enforce_utc_timestamp(dt)


def validate_column_names(columns: list[str]) -> list[str]:
    """Validate column names don't conflict with index."""
    if not isinstance(columns, list):
        raise TypeError(f"Expected list of strings, got {type(columns)}")
    if not all(isinstance(col, str) for col in columns):
        raise TypeError("All column names must be strings")
    if CANONICAL_INDEX_NAME in columns:
        raise ValueError(f"{CANONICAL_INDEX_NAME} is reserved for index")
    return columns


def validate_time_range(
    start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
) -> tuple[Optional[datetime], Optional[datetime]]:
    """Validate and normalize time range parameters."""
    # Use centralized utility
    return DataValidation.validate_time_range(start_time, end_time)


def validate_time_boundaries(
    df: pd.DataFrame, start_time: datetime, end_time: datetime
) -> None:
    """Validate that DataFrame covers the requested time range."""
    # Use centralized utility
    DataValidation.validate_time_boundaries(df, start_time, end_time)


def validate_symbol_format(symbol: str) -> None:
    """Validate trading pair symbol format."""
    # Use centralized utility
    DataValidation.validate_symbol_format(symbol)


def validate_dataframe_integrity(df: pd.DataFrame) -> None:
    """Validate DataFrame structure and integrity."""
    # Use centralized DataFrameValidator
    DataFrameValidator.validate_dataframe(df)


def validate_cache_checksum(cache_path: Path, stored_checksum: str) -> bool:
    """Validate cache file against stored checksum."""
    # Use centralized CacheValidator
    return CacheValidator.validate_cache_checksum(cache_path, stored_checksum)


def validate_cache_metadata(
    cache_info: Optional[dict],
    required_fields: list[str] = ["checksum", "record_count"],
) -> bool:
    """Validate cache metadata contains required information."""
    # Use centralized CacheValidator
    return CacheValidator.validate_cache_metadata(cache_info, required_fields)


def validate_cache_records(record_count: int) -> bool:
    """Validate cache contains records."""
    # Use centralized CacheValidator
    return CacheValidator.validate_cache_records(record_count)


def get_cache_path(cache_dir: Path, symbol: str, interval: str, date: datetime) -> Path:
    """Generate standardized cache file path."""
    # Use centralized CacheKeyManager
    return CacheKeyManager.get_cache_path(cache_dir, symbol, interval, date)


logger = logging.getLogger(__name__)
