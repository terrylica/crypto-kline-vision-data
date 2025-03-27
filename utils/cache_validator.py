#!/usr/bin/env python
"""Centralized cache validation utilities.

This module provides standardized tools for validating cache integrity, checksums,
and metadata across different components to reduce duplication and ensure consistency.
"""

import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, NamedTuple, Optional, Sequence, Union
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa

from utils.logger_setup import get_logger
from utils.validation import DataFrameValidator
from utils.market_constraints import Interval
from utils.api_boundary_validator import ApiBoundaryValidator

logger = get_logger(__name__, "INFO", show_path=False)

# Default symbol for tests
TEST_SYMBOL = "BTCUSDT"


class CacheValidationError(NamedTuple):
    """Standardized cache validation error details."""

    error_type: str
    message: str
    is_recoverable: bool


# Error type constants for consistent error reporting
ERROR_TYPES = {
    "FILE_SYSTEM": "file_system_error",
    "DATA_INTEGRITY": "data_integrity_error",
    "CACHE_INVALID": "cache_invalid",
    "VALIDATION": "validation_error",
    "API_BOUNDARY": "api_boundary_error",
}


class SafeMemoryMap:
    """Context manager for safe memory map handling."""

    def __init__(self, path: Path):
        """Initialize memory map.

        Args:
            path: Path to Arrow file
        """
        self.path = path
        self._mmap = None

    def __enter__(self) -> pa.MemoryMappedFile:
        """Enter context manager.

        Returns:
            Memory mapped file
        """
        self._mmap = pa.memory_map(str(self.path), "r")
        return self._mmap

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[object],
    ) -> None:
        """Exit context manager and clean up resources."""
        if self._mmap is not None:
            self._mmap.close()

    @classmethod
    async def safely_read_arrow_file(
        cls, path: Path, columns: Optional[Sequence[str]] = None
    ) -> Optional[pd.DataFrame]:
        """Safely read an Arrow file without blocking the event loop.

        Args:
            path: Path to Arrow file
            columns: Optional list of columns to read

        Returns:
            DataFrame or None if read fails
        """
        try:
            # Use run_in_executor to make the file reading non-blocking
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, lambda: cls._read_arrow_file_impl(path, columns)
            )
        except (IOError, pa.ArrowInvalid, pa.ArrowIOError, ValueError) as e:
            logger.error(f"Error reading Arrow file {path}: {e}")
            return None

    @staticmethod
    def _read_arrow_file_impl(
        path: Path, columns: Optional[Sequence[str]] = None
    ) -> pd.DataFrame:
        """Internal implementation for reading Arrow files.

        This is the single implementation that all other methods should use.

        Args:
            path: Path to Arrow file
            columns: Optional list of columns to read

        Returns:
            DataFrame with data from Arrow file

        Raises:
            Various exceptions if reading fails
        """
        with SafeMemoryMap(path) as source:
            with pa.ipc.open_file(source) as reader:
                if columns:
                    # Ensure index column is included
                    all_cols = reader.schema.names
                    if "open_time" in all_cols and "open_time" not in columns:
                        cols_to_read = ["open_time"] + list(columns)
                    else:
                        cols_to_read = list(columns)
                    table = reader.read_all().select(cols_to_read)
                else:
                    table = reader.read_all()

                df = table.to_pandas(
                    zero_copy_only=False,  # More robust but might copy data
                    date_as_object=False,
                    use_threads=True,
                )

                # Set index if needed
                if "open_time" in df.columns and df.index.name != "open_time":
                    df.set_index("open_time", inplace=True)

                # Ensure index is datetime with timezone
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index, utc=True)
                elif df.index.tz is None:
                    df.index = df.index.tz_localize("UTC")

                return df


@dataclass
class ValidationOptions:
    """Options for cache data validation."""

    allow_empty: bool = False
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    interval: Optional[Interval] = None
    symbol: str = "BTCUSDT"


@dataclass
class AlignmentOptions:
    """Options for aligning cache data to API boundaries."""

    start_time: datetime
    end_time: datetime
    interval: Interval
    symbol: str = "BTCUSDT"


class CacheValidator:
    """Centralized cache validation utilities.

    This class consolidates cache validation logic that was previously
    scattered across multiple modules, providing consistent validation
    behavior with clear error reporting.
    """

    # Cache validation constraints
    MIN_VALID_FILE_SIZE = 1024  # 1KB minimum for valid data files
    MAX_CACHE_AGE = timedelta(days=30)  # Maximum age before revalidation
    METADATA_UPDATE_INTERVAL = timedelta(minutes=5)

    def __init__(self, api_boundary_validator: Optional[ApiBoundaryValidator] = None):
        """Initialize the CacheValidator with optional ApiBoundaryValidator.

        Args:
            api_boundary_validator: Optional ApiBoundaryValidator for API boundary validations
        """
        self.api_boundary_validator = api_boundary_validator

    @classmethod
    def validate_cache_integrity(
        cls,
        cache_path: Path,
        max_age: timedelta = None,
        min_size: int = None,
    ) -> Optional[CacheValidationError]:
        """Validate cache file existence, size, and age.

        Args:
            cache_path: Path to cache file
            max_age: Maximum allowed age of cache (defaults to MAX_CACHE_AGE)
            min_size: Minimum valid file size (defaults to MIN_VALID_FILE_SIZE)

        Returns:
            Error details if validation fails, None if valid
        """
        max_age = max_age or cls.MAX_CACHE_AGE
        min_size = min_size or cls.MIN_VALID_FILE_SIZE

        try:
            if not cache_path.exists():
                return CacheValidationError(
                    ERROR_TYPES["FILE_SYSTEM"], "Cache file does not exist", True
                )

            stats = cache_path.stat()

            # Check file size
            if stats.st_size < min_size:
                return CacheValidationError(
                    ERROR_TYPES["DATA_INTEGRITY"],
                    f"Cache file too small: {stats.st_size} bytes",
                    True,
                )

            # Check age
            age = datetime.now(timezone.utc) - datetime.fromtimestamp(
                stats.st_mtime, timezone.utc
            )
            if age > max_age:
                return CacheValidationError(
                    ERROR_TYPES["CACHE_INVALID"],
                    f"Cache too old: {age.days} days",
                    True,
                )

            return None

        except (OSError, IOError, PermissionError, ValueError) as e:
            return CacheValidationError(
                ERROR_TYPES["FILE_SYSTEM"],
                f"Error validating cache: {str(e)}",
                False,
            )

    @classmethod
    def validate_cache_checksum(cls, cache_path: Path, stored_checksum: str) -> bool:
        """Validate cache file against stored checksum.

        Args:
            cache_path: Path to cache file
            stored_checksum: Previously stored checksum

        Returns:
            True if checksum matches, False otherwise
        """
        try:
            current_checksum = CacheValidator.calculate_checksum(cache_path)
            return current_checksum == stored_checksum
        except (IOError, OSError, ValueError) as e:
            logger.error(f"Error validating cache checksum: {e}")
            return False

    @classmethod
    def validate_cache_metadata(
        cls,
        cache_info: Optional[Dict[str, Any]],
        required_fields: list = None,
    ) -> bool:
        """Validate cache metadata contains required information.

        Args:
            cache_info: Cache metadata dictionary
            required_fields: List of required fields in metadata

        Returns:
            True if metadata is valid, False otherwise
        """
        if required_fields is None:
            required_fields = ["checksum", "record_count", "last_updated"]

        if not cache_info:
            return False

        return all(field in cache_info for field in required_fields)

    @classmethod
    def validate_cache_records(cls, record_count: Union[int, str]) -> bool:
        """Validate cache contains records.

        Args:
            record_count: Number of records in cache (as int or str)

        Returns:
            True if record count is valid, False otherwise
        """
        try:
            # Convert record_count to int if it's a string
            if isinstance(record_count, str):
                record_count = int(record_count)
            return record_count > 0
        except (ValueError, TypeError):
            return False

    async def validate_cache_data(
        self,
        df: pd.DataFrame,
        options: ValidationOptions = None,
        allow_empty: bool = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        interval: Optional[Interval] = None,
        symbol: str = None,
    ) -> Optional[CacheValidationError]:
        """Validate cached data DataFrame.

        Args:
            df: DataFrame to validate
            options: Validation options including time boundaries and symbol
            allow_empty: Optional flag to allow empty DataFrames
            start_time: Optional start time for validation
            end_time: Optional end time for validation
            interval: Optional interval for validation
            symbol: Optional symbol for validation

        Returns:
            ValidationError if invalid, None if valid
        """
        # Use default options if none provided
        if options is None:
            options = ValidationOptions()

        # Override options with individual parameters if provided
        if allow_empty is not None:
            options.allow_empty = allow_empty
        if start_time is not None:
            options.start_time = start_time
        if end_time is not None:
            options.end_time = end_time
        if interval is not None:
            options.interval = interval
        if symbol is not None:
            options.symbol = symbol

        # Check if DataFrame is empty
        if df.empty and not options.allow_empty:
            return CacheValidationError(
                ERROR_TYPES["VALIDATION"],
                "DataFrame is empty",
                True,
            )

        # Validate DataFrame structure
        try:
            DataFrameValidator.validate_dataframe(df)
        except ValueError as e:
            return CacheValidationError(
                ERROR_TYPES["VALIDATION"],
                f"DataFrame validation failed: {e}",
                False,
            )

        # Validate API boundaries if validator is available and we have all required parameters
        if (
            self.api_boundary_validator
            and options.start_time
            and options.end_time
            and options.interval
            and not df.empty
        ):
            try:
                # Use ApiBoundaryValidator to validate cache data matches REST API behavior
                is_api_aligned = await self.api_boundary_validator.does_data_range_match_api_response(
                    df,
                    options.start_time,
                    options.end_time,
                    options.interval,
                    options.symbol,
                )

                if not is_api_aligned:
                    return CacheValidationError(
                        ERROR_TYPES["API_BOUNDARY"],
                        "Cache data boundaries do not match REST API behavior",
                        True,  # Recoverable by refetching
                    )

                logger.debug("Cache data boundaries match REST API behavior")
            except (ValueError, RuntimeError, IOError, ConnectionError) as e:
                logger.warning("API boundary validation failed: %s", e)
                # Don't fail cache validation just because API validation failed
                # This keeps the system robust even if we can't reach the API

        return None

    @staticmethod
    def calculate_checksum(file_path: Path) -> str:
        """Calculate SHA-256 checksum of a file.

        Args:
            file_path: Path to file

        Returns:
            Hexadecimal checksum string
        """
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            # Read in 64k chunks
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def safely_read_arrow_file(
        file_path: Path, columns: Optional[list] = None
    ) -> Optional[pd.DataFrame]:
        """Safely read an Arrow file with proper error handling.

        This is a blocking version of safely_read_arrow_file_async.

        Args:
            file_path: Path to Arrow file
            columns: Optional list of columns to read

        Returns:
            DataFrame or None if read fails
        """
        try:
            # Use a local copy of the implementation to avoid protected member access
            with SafeMemoryMap(file_path) as source:
                with pa.ipc.open_file(source) as reader:
                    if columns:
                        # Ensure index column is included
                        all_cols = reader.schema.names
                        if "open_time" in all_cols and "open_time" not in columns:
                            cols_to_read = ["open_time"] + list(columns)
                        else:
                            cols_to_read = list(columns)
                        table = reader.read_all().select(cols_to_read)
                    else:
                        table = reader.read_all()

                    df = table.to_pandas(
                        zero_copy_only=False,  # More robust but might copy data
                        date_as_object=False,
                        use_threads=True,
                    )

                    # Set index if needed
                    if "open_time" in df.columns and df.index.name != "open_time":
                        df.set_index("open_time", inplace=True)

                    # Ensure index is datetime with timezone
                    if not isinstance(df.index, pd.DatetimeIndex):
                        df.index = pd.to_datetime(df.index, utc=True)
                    elif df.index.tz is None:
                        df.index = df.index.tz_localize("UTC")

                    return df
        except (IOError, OSError, pa.ArrowInvalid, pa.ArrowIOError, ValueError) as e:
            logger.error("Error reading Arrow file %s: %s", file_path, e)
            return None

    @staticmethod
    async def safely_read_arrow_file_async(
        file_path: Path, columns: Optional[list] = None
    ) -> Optional[pd.DataFrame]:
        """Asynchronously and safely read an Arrow file with proper error handling.

        Args:
            file_path: Path to Arrow file
            columns: Optional list of columns to read

        Returns:
            DataFrame or None if read fails
        """
        return await SafeMemoryMap.safely_read_arrow_file(file_path, columns)

    async def align_cached_data_to_api_boundaries(
        self,
        df: pd.DataFrame,
        options_or_start_time: Union[AlignmentOptions, datetime] = None,
        end_time_or_interval: Optional[Union[datetime, Interval]] = None,
        interval_or_symbol: Optional[Union[Interval, str]] = None,
        symbol: str = None,
    ) -> pd.DataFrame:
        """Align cache data to match what would be returned by the Binance REST API.

        Args:
            df: DataFrame containing cached data
            options_or_start_time: Either AlignmentOptions object or start_time
            end_time_or_interval: Either end_time (if options_or_start_time is start_time) or interval
            interval_or_symbol: Either interval (if options_or_start_time is start_time) or symbol
            symbol: Symbol for alignment

        Returns:
            DataFrame aligned to REST API boundaries

        Raises:
            ValueError: If ApiBoundaryValidator is not provided or if required parameters are missing
        """
        if df.empty:
            return df

        if not self.api_boundary_validator:
            raise ValueError(
                "ApiBoundaryValidator is required for cache data alignment"
            )

        # Check if we're using the old or new parameter pattern
        if isinstance(options_or_start_time, datetime):
            # Old pattern: start_time, end_time, interval, symbol
            start_time = options_or_start_time
            end_time = end_time_or_interval
            interval = interval_or_symbol
            symbol_param = symbol or "BTCUSDT"

            if start_time is None or end_time is None or interval is None:
                raise ValueError(
                    "start_time, end_time, and interval parameters must be provided"
                )

            options = AlignmentOptions(
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                symbol=symbol_param,
            )
        else:
            # New pattern: options, start_time, end_time, interval, symbol
            options = options_or_start_time or AlignmentOptions(
                start_time=datetime.now(timezone.utc),
                end_time=datetime.now(timezone.utc) + timedelta(hours=1),
                interval=Interval.MINUTE_1,
            )

            # Handle potential overrides
            if isinstance(end_time_or_interval, datetime):
                options.start_time = options_or_start_time
                options.end_time = end_time_or_interval
                options.interval = interval_or_symbol
                if symbol:
                    options.symbol = symbol
            else:
                # These might be individual parameter overrides
                if end_time_or_interval is not None:
                    options.end_time = end_time_or_interval
                if interval_or_symbol is not None:
                    options.interval = interval_or_symbol
                if symbol is not None:
                    options.symbol = symbol

        # Get expected API boundaries for these parameters
        api_boundaries = await self.api_boundary_validator.get_api_boundaries(
            options.start_time,
            options.end_time,
            options.interval,
            symbol=options.symbol,
        )

        if api_boundaries["record_count"] == 0:
            # API would return no data, so return empty DataFrame
            return pd.DataFrame(index=pd.DatetimeIndex([], name="open_time"))

        # Filter DataFrame to match API boundaries
        api_start_time = api_boundaries["api_start_time"]
        api_end_time = api_boundaries["api_end_time"]
        aligned_df = df[(df.index >= api_start_time) & (df.index <= api_end_time)]

        return aligned_df


@dataclass
class CachePathOptions:
    """Options for cache path generation."""

    exchange: str = "binance"
    market_type: str = "spot"
    data_nature: str = "klines"
    packaging_frequency: str = "daily"


class CacheKeyManager:
    """Utilities for generating consistent cache keys and paths."""

    @staticmethod
    def get_cache_key(symbol: str, interval: str, date: datetime) -> str:
        """Generate a unique cache key.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            Unique cache key string
        """
        # Ensure consistent date format
        date_str = date.strftime("%Y-%m-%d")
        return f"{symbol.upper()}_{interval}_{date_str}"

    @staticmethod
    def get_cache_path(
        cache_dir: Path,
        symbol: str,
        interval: str,
        date: datetime,
        options: CachePathOptions = None,
        exchange: str = None,
        market_type: str = None,
        data_nature: str = None,
        packaging_frequency: str = None,
    ) -> Path:
        """Generate standardized cache path.

        Generates a path that follows the structure defined in docs/roadmap/upgrade_cache_structure.md:
        cache_dir/{exchange}/{market_type}/{data_nature}/{packaging_frequency}/{SYMBOL}/{INTERVAL}/YYYYMMDD.arrow

        Args:
            cache_dir: Root cache directory
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date
            options: Cache path options with exchange, market type, data nature, etc.
            exchange: Optional custom exchange (overrides options if provided)
            market_type: Optional custom market type (overrides options if provided)
            data_nature: Optional custom data nature (overrides options if provided)
            packaging_frequency: Optional custom packaging frequency (overrides options if provided)

        Returns:
            Path object for cache file
        """
        if options is None:
            options = CachePathOptions()

        # Override options with individual parameters if provided
        if exchange is not None:
            options.exchange = exchange
        if market_type is not None:
            options.market_type = market_type
        if data_nature is not None:
            options.data_nature = data_nature
        if packaging_frequency is not None:
            options.packaging_frequency = packaging_frequency

        # Format date for filename
        year_month_day = date.strftime("%Y%m%d")

        # Standardize symbol and interval format
        symbol = symbol.upper()
        interval = interval.lower()

        # Generate path with standardized structure
        path = (
            cache_dir
            / options.exchange
            / options.market_type
            / options.data_nature
            / options.packaging_frequency
            / symbol
            / interval
        )
        path.mkdir(parents=True, exist_ok=True)

        # Generate filename with standardized format - YYYYMMDD.arrow
        filename = f"{year_month_day}.arrow"

        return path / filename


class VisionCacheManager:
    """Vision-specific cache management utilities."""

    FILE_EXTENSION = ".arrow"

    @staticmethod
    async def save_to_cache(
        df: pd.DataFrame,
        cache_path: Path,
        date_or_unused: Any = None,
    ) -> tuple[str, int]:
        """Save data to cache in Arrow format.

        Args:
            df: DataFrame to cache
            cache_path: Path to cache file
            date_or_unused: Optional date parameter (kept for backward compatibility)

        Returns:
            Tuple of (checksum, record count)
        """
        if df.empty:
            logger.warning("Empty dataframe, not saving to cache")
            return "", 0

        # Ensure directory exists
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to pyarrow table
        table = pa.Table.from_pandas(df)

        # Write to file
        try:
            with pa.OSFile(str(cache_path), "wb") as sink:
                with pa.ipc.new_file(sink, table.schema) as writer:
                    writer.write_table(table)

            # Calculate checksum
            checksum = CacheValidator.calculate_checksum(cache_path)
            record_count = len(df)

            # Log cache save info
            logger.info(
                "Saved %d records to cache at %s. Size: %d bytes",
                record_count,
                cache_path,
                cache_path.stat().st_size,
            )

            return checksum, record_count
        except (
            IOError,
            OSError,
            pa.ArrowException,
            pa.ArrowInvalid,
            pa.ArrowIOError,
        ) as e:
            logger.error("Error saving to cache: %s", e)
            # If there was an error, attempt to clean up partial file
            if cache_path.exists():
                try:
                    cache_path.unlink()
                    logger.info(
                        "Removed partial cache file after error: %s", cache_path
                    )
                except (IOError, OSError, PermissionError) as cleanup_error:
                    logger.error(
                        "Failed to clean up partial cache file: %s", cleanup_error
                    )
            return "", 0

    @staticmethod
    async def load_from_cache(
        cache_path: Path, columns: Optional[Sequence[str]] = None
    ) -> Optional[pd.DataFrame]:
        """Load data from cache with proper error handling.

        Args:
            cache_path: Path to cache file
            columns: Optional list of columns to read

        Returns:
            DataFrame or None if cache is invalid or missing
        """
        try:
            # Validate cache file exists and is not too old
            error = CacheValidator.validate_cache_integrity(cache_path)
            if error:
                logger.warning("Cache validation failed: %s", error.message)
                return None

            # Read the file
            df = await CacheValidator.safely_read_arrow_file_async(cache_path, columns)
            if df is None:
                return None

            if df.empty:
                logger.warning("Cache file is empty: %s", cache_path)
                return None

            logger.info(
                "Successfully loaded %d records from cache: %s", len(df), cache_path
            )
            return df
        except (IOError, OSError, ValueError, pa.ArrowInvalid, pa.ArrowIOError) as e:
            logger.error("Error loading from cache: %s", e)
            return None


# Add standalone versions of the class methods to support direct imports


def validate_cache_integrity(
    cache_path: Path,
    max_age: timedelta = None,
    min_size: int = None,
) -> Optional[CacheValidationError]:
    """Standalone version of CacheValidator.validate_cache_integrity.

    Args:
        cache_path: Path to cache file
        max_age: Maximum allowed age of cache
        min_size: Minimum valid file size

    Returns:
        Error details if validation fails, None if valid
    """
    return CacheValidator.validate_cache_integrity(cache_path, max_age, min_size)


def validate_cache_checksum(cache_path: Path, stored_checksum: str) -> bool:
    """Standalone version of CacheValidator.validate_cache_checksum.

    Args:
        cache_path: Path to cache file
        stored_checksum: Previously stored checksum

    Returns:
        True if checksum matches, False otherwise
    """
    return CacheValidator.validate_cache_checksum(cache_path, stored_checksum)


def validate_cache_metadata(
    cache_info: Optional[Dict[str, Any]],
    required_fields: list = None,
) -> bool:
    """Standalone version of CacheValidator.validate_cache_metadata.

    Args:
        cache_info: Cache metadata dictionary
        required_fields: List of required fields in metadata

    Returns:
        True if metadata is valid, False otherwise
    """
    return CacheValidator.validate_cache_metadata(cache_info, required_fields)


def validate_cache_records(record_count: Union[int, str]) -> bool:
    """Standalone version of CacheValidator.validate_cache_records.

    Args:
        record_count: Number of records in cache (as int or str)

    Returns:
        True if record count is valid, False otherwise
    """
    try:
        # Convert record_count to int if it's a string
        if isinstance(record_count, str):
            record_count = int(record_count)
        return record_count > 0
    except (ValueError, TypeError):
        return False


async def safely_read_arrow_file_async(
    file_path: Path, columns: Optional[list] = None
) -> Optional[pd.DataFrame]:
    """Standalone version of CacheValidator.safely_read_arrow_file_async.

    Args:
        file_path: Path to Arrow file
        columns: Optional list of columns to read

    Returns:
        DataFrame or None if read fails
    """
    return await CacheValidator.safely_read_arrow_file_async(file_path, columns)
