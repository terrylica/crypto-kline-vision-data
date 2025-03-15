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
CONSOLIDATION_DELAY = timedelta(hours=12)  # Time Binance needs to consolidate daily data

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


class CacheValidationError(NamedTuple):
    """Cache validation error details."""

    error_type: str
    message: str
    is_recoverable: bool


# File constraints
MIN_VALID_FILE_SIZE: Final[int] = 1024  # 1KB minimum for valid data files
MAX_CACHE_AGE: Final[timedelta] = timedelta(days=30)  # Maximum age before cache revalidation
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
    elif isinstance(error, pa.ArrowInvalid):  # type: ignore
        return VisionErrorType.DATA_INTEGRITY
    else:
        return VisionErrorType.VALIDATION


def validate_cache_integrity(
    cache_path: Path, max_age: timedelta = MAX_CACHE_AGE, min_size: int = MIN_VALID_FILE_SIZE
) -> Optional[CacheValidationError]:
    """Validate cache file integrity.

    Args:
        cache_path: Path to cache file
        max_age: Maximum allowed age of cache
        min_size: Minimum valid file size

    Returns:
        Error details if validation fails, None if valid
    """
    try:
        if not cache_path.exists():
            return CacheValidationError(VisionErrorType.CACHE_INVALID.value, "Cache file does not exist", True)

        stats = cache_path.stat()

        # Check file size
        if stats.st_size < min_size:
            return CacheValidationError(VisionErrorType.CACHE_INVALID.value, f"Cache file too small: {stats.st_size} bytes", True)

        # Check age
        age = datetime.now(timezone.utc) - datetime.fromtimestamp(stats.st_mtime, timezone.utc)
        if age > max_age:
            return CacheValidationError(VisionErrorType.CACHE_INVALID.value, f"Cache too old: {age.days} days", True)

        return None

    except Exception as e:
        return CacheValidationError(VisionErrorType.FILE_SYSTEM.value, f"Error validating cache: {str(e)}", False)


def get_vision_url(symbol: str, interval: str, date: datetime, file_type: FileType) -> str:
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

    Note:
        Binance Vision has two key constraints:
        1. Data for a day is only available after that day is complete
        2. There's a consolidation delay (~12 hours) after day completion
    """
    now = datetime.now(timezone.utc)

    # Convert target_date to start of day in UTC for consistent comparison
    target_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)

    if target_day.date() < now.date():
        # Past dates are always available
        return True
    elif target_day.date() == now.date():
        # Today's data is only available after consolidation delay
        return now - target_day > CONSOLIDATION_DELAY
    else:
        # Future dates are never available
        return False


def validate_data_availability(start_time: datetime, end_time: datetime) -> None:
    """Validate if data should be available for the given time range.

    Args:
        start_time: Start of time range
        end_time: End of time range

    Raises:
        ValueError: If data is definitely not available for the time range
    """
    if not is_data_likely_available(start_time):
        raise ValueError(
            f"Data for {start_time.date()} is not yet available. " f"Binance Vision requires {CONSOLIDATION_DELAY} after day completion."
        )
    if end_time.date() > datetime.now(timezone.utc).date():
        raise ValueError(f"Cannot request future data: {end_time.date()}")


class TimestampedDataFrame(pd.DataFrame):
    """DataFrame with enforced UTC timestamp index.

    This class enforces:
    1. Index must be DatetimeIndex
    2. Index must be timezone-aware and in UTC
    3. Index must be named 'open_time'
    4. Index must be monotonically increasing
    5. No duplicate indices allowed
    """

    def __init__(self, *args, **kwargs):  # type: ignore
        """Initialize with DataFrame validation."""
        super().__init__(*args, **kwargs)  # type: ignore
        self._validate_and_normalize_index()

    def _validate_and_normalize_index(self):
        """Validate and normalize the index to meet requirements."""
        # Convert index to DatetimeIndex if it's not already
        if not isinstance(self.index, pd.DatetimeIndex):  # type: ignore
            try:
                # Try to convert the index to datetime
                self.index = pd.to_datetime(self.index, utc=True)  # type: ignore
            except Exception as e:
                raise ValueError(f"Failed to convert index to DatetimeIndex: {e}")

        # Ensure index is timezone-aware and in UTC
        if self.index.tz is None:  # type: ignore
            self.index = self.index.tz_localize(CANONICAL_TIMEZONE)  # type: ignore
        elif self.index.tz != CANONICAL_TIMEZONE:  # type: ignore
            self.index = self.index.tz_convert(CANONICAL_TIMEZONE)  # type: ignore

        # Ensure index is named correctly
        if self.index.name != CANONICAL_INDEX_NAME:  # type: ignore
            self.index.name = CANONICAL_INDEX_NAME  # type: ignore

        # Validate index properties
        if not self.index.is_monotonic_increasing:  # type: ignore
            raise ValueError("Index must be monotonically increasing")
        if self.index.has_duplicates:  # type: ignore
            raise ValueError("Index must not contain duplicates")

    def __setitem__(self, key, value):  # type: ignore
        """Override to prevent modification of index."""
        if key == CANONICAL_INDEX_NAME:  # type: ignore
            raise ValueError(f"Cannot modify {CANONICAL_INDEX_NAME} directly - it is reserved for index")
        super().__setitem__(key, value)  # type: ignore


def validate_cache_path(path: Path) -> CachePath:
    """Validate and convert a Path to a CachePath."""
    if not isinstance(path, Path):  # type: ignore
        raise TypeError(f"Expected Path object, got {type(path)}")
    if not path.suffix == ".arrow":  # type: ignore
        raise ValueError("Cache path must have .arrow extension")
    return CachePath(path)


def enforce_utc_timestamp(dt: datetime) -> datetime:
    """Ensure timestamp is UTC."""
    if not isinstance(dt, datetime):  # type: ignore
        raise TypeError(f"Expected datetime object, got {type(dt)}")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=CANONICAL_TIMEZONE)
    return dt.astimezone(CANONICAL_TIMEZONE)


def validate_column_names(columns: list[str]) -> list[str]:
    """Validate column names don't conflict with index."""
    if not isinstance(columns, list):  # type: ignore
        raise TypeError(f"Expected list of strings, got {type(columns)}")
    if not all(isinstance(col, str) for col in columns):  # type: ignore
        raise TypeError("All column names must be strings")
    if CANONICAL_INDEX_NAME in columns:
        raise ValueError(f"{CANONICAL_INDEX_NAME} is reserved for index")
    return columns


def validate_time_range(
    start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
) -> tuple[Optional[datetime], Optional[datetime]]:
    """Validate and normalize time range parameters."""
    if start_time is not None:
        start_time = enforce_utc_timestamp(start_time)
    if end_time is not None:
        end_time = enforce_utc_timestamp(end_time)
    if start_time and end_time and start_time >= end_time:
        raise ValueError("End time must be after start time")
    return start_time, end_time


def validate_time_boundaries(df: pd.DataFrame, start_time: datetime, end_time: datetime) -> None:
    """Validate that DataFrame covers the requested time range.

    Args:
        df: DataFrame to validate
        start_time: Start time
        end_time: End time

    Raises:
        ValueError: If data doesn't cover requested time range
    """
    # For empty DataFrame, just validate the time range itself
    if df.empty:
        if start_time > end_time:
            raise ValueError(f"Start time {start_time} is after end time {end_time}")
        if end_time > datetime.now(timezone.utc):
            raise ValueError(f"End time {end_time} is in the future")
        return

    # Ensure index is timezone-aware
    if df.index.tz is None:  # type: ignore
        raise ValueError("DataFrame index must be timezone-aware")

    # Convert times to UTC for comparison
    start_time = enforce_utc_timestamp(start_time)
    end_time = enforce_utc_timestamp(end_time)

    # Get actual data boundaries
    data_start = df.index.min()  # type: ignore
    data_end = df.index.max()  # type: ignore

    # Log time range details
    logger.info(f"Requested time range: {start_time} to {end_time}")
    logger.info(f"Available data range: {data_start} to {data_end}")

    # Check if data covers requested range, ignoring microsecond precision
    data_start_floor = data_start.replace(microsecond=0)  # type: ignore
    data_end_floor = data_end.replace(microsecond=0)  # type: ignore
    start_time_floor = start_time.replace(microsecond=0)  # type: ignore
    end_time_floor = end_time.replace(microsecond=0)  # type: ignore

    if data_start_floor > start_time_floor:
        logger.error(f"Data starts later than requested: {data_start} > {start_time}")
        raise ValueError(f"Data starts later than requested: {data_start} > {start_time}")
    if data_end_floor < end_time_floor:
        logger.error(f"Data ends earlier than requested: {data_end} < {end_time}")
        raise ValueError(f"Data ends earlier than requested: {data_end} < {end_time}")

    # Check for gaps in data
    timestamps = df.index.to_series()  # type: ignore
    time_diffs = timestamps.diff()  # type: ignore
    gaps = time_diffs[time_diffs > timedelta(seconds=1)]  # type: ignore

    if not gaps.empty:  # type: ignore
        logger.warning(f"Found {len(gaps)} gaps in data:")  # type: ignore
        for idx, gap in gaps.head().items():  # type: ignore
            logger.warning(f"Gap at {idx}: {gap}")

    # Check for duplicates
    duplicates = df.index.duplicated()  # type: ignore
    if duplicates.any():
        logger.warning(f"Found {duplicates.sum()} duplicate timestamps")

    # Verify data is sorted
    if not df.index.is_monotonic_increasing:
        raise ValueError("Data is not sorted by time")


def validate_symbol_format(symbol: str) -> None:
    """Validate trading pair symbol format.

    Args:
        symbol: Trading pair symbol to validate

    Raises:
        ValueError: If symbol format is invalid
    """
    if not symbol or len(symbol) < 5:  # Minimum valid symbol length (e.g., "BTCUSDT")
        raise ValueError(f"Invalid symbol format: {symbol}")


def validate_dataframe_integrity(df: pd.DataFrame) -> None:
    """Validate DataFrame structure and integrity.

    Args:
        df: DataFrame to validate

    Raises:
        ValueError: If DataFrame fails validation
    """
    if df is None:  # type: ignore
        raise ValueError("DataFrame is None")

    # Empty DataFrame is allowed but should be properly structured
    if df.empty:
        required_columns = {
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_volume",
            "taker_buy_quote_volume",
        }
        if not all(col in df.columns for col in required_columns):
            raise ValueError("Empty DataFrame missing required columns")
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("Empty DataFrame must have DatetimeIndex")
        if df.index.tz != timezone.utc:
            raise ValueError("Empty DataFrame index must be UTC")
        return

    # For non-empty DataFrame, validate structure and data
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Index must be DatetimeIndex")

    if df.index.tz != timezone.utc:
        raise ValueError("Index timezone must be UTC")

    required_columns = {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_buy_volume",
        "taker_buy_quote_volume",
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Validate data types
    expected_types = {
        "open": ["float64"],
        "high": ["float64"],
        "low": ["float64"],
        "close": ["float64"],
        "volume": ["float64"],
        "close_time": ["int64"],
        "quote_volume": ["float64"],
        "trades": ["int64"],
        "taker_buy_volume": ["float64"],
        "taker_buy_quote_volume": ["float64"],
    }

    for col, expected in expected_types.items():
        if str(df[col].dtype) not in expected:  # type: ignore
            raise ValueError(f"Column {col} has wrong type: {df[col].dtype}, expected one of {expected}")  # type: ignore

    # Validate value ranges
    if (df["high"] < df["low"]).any():  # type: ignore
        raise ValueError("Found high price less than low price")

    if (df["volume"] < 0).any():  # type: ignore
        raise ValueError("Found negative volume")

    if (df["trades"] < 0).any():  # type: ignore
        raise ValueError("Found negative trade count")

    # Validate timestamp ordering
    if not df.index.is_monotonic_increasing:
        raise ValueError("Index is not monotonically increasing")

    # Validate close_time format (should be microseconds or nanoseconds)
    close_time_digits = len(str(df["close_time"].iloc[0]))  # type: ignore
    if close_time_digits not in [16, 19]:  # Accept both microsecond (16) and nanosecond (19) precision
        raise ValueError(f"close_time has wrong precision: {close_time_digits} digits, expected 16 (microseconds) or 19 (nanoseconds)")

    # If nanoseconds, convert to microseconds and ensure exact REST API format
    if close_time_digits == 19:
        df["close_time"] = df["close_time"] // 1000  # Convert nanoseconds to microseconds
        df["close_time"] = df["close_time"].astype(np.int64) * 1000 + 999999  # Match REST API format  # type: ignore


def validate_cache_checksum(cache_path: Path, stored_checksum: str) -> bool:
    """Validate cache file against stored checksum.

    Args:
        cache_path: Path to cache file
        stored_checksum: Previously stored checksum

    Returns:
        True if checksum matches, False otherwise
    """
    try:
        current_checksum = hashlib.sha256(cache_path.read_bytes()).hexdigest()
        return current_checksum == stored_checksum
    except Exception as e:
        logger.error(f"Error validating cache checksum: {e}")
        return False


def validate_cache_metadata(cache_info: Optional[dict], required_fields: list[str] = ["checksum", "record_count"]) -> bool:  # type: ignore
    """Validate cache metadata contains required information.

    Args:
        cache_info: Cache metadata dictionary
        required_fields: List of required fields in metadata

    Returns:
        True if metadata is valid, False otherwise
    """
    if not cache_info:
        return False
    return all(field in cache_info for field in required_fields)


def validate_cache_records(record_count: int) -> bool:
    """Validate cache contains records.

    Args:
        record_count: Number of records in cache

    Returns:
        True if record count is valid, False otherwise
    """
    return record_count > 0


def get_cache_path(cache_dir: Path, symbol: str, interval: str, date: datetime) -> Path:
    """Generate standardized cache file path.

    Args:
        cache_dir: Base cache directory
        symbol: Trading pair symbol
        interval: Time interval
        date: Target date

    Returns:
        Path to cache file
    """
    year_month: str = date.strftime("%Y%m")
    return cache_dir / symbol / interval / f"{year_month}.arrow"


logger = logging.getLogger(__name__)
