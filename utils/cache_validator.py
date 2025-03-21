#!/usr/bin/env python
"""Centralized cache validation utilities.

This module provides standardized tools for validating cache integrity, checksums,
and metadata across different components to reduce duplication and ensure consistency.
"""

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, Any, NamedTuple, Sequence
import pandas as pd
import pyarrow as pa
import time

from utils.logger_setup import get_logger
from utils.validation import DataFrameValidator

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


class CacheValidationError(NamedTuple):
    """Standardized cache validation error details."""

    error_type: str
    message: str
    is_recoverable: bool


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

    @staticmethod
    def safely_read_arrow_file(
        path: Path, columns: Optional[Sequence[str]] = None
    ) -> Optional[pd.DataFrame]:
        """Safely read Arrow file with error handling.

        Args:
            path: Path to Arrow file
            columns: Optional list of columns to read

        Returns:
            DataFrame or None if read fails
        """
        try:
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
        except Exception as e:
            logger.error(f"Error reading Arrow file {path}: {e}")
            return None


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
                    "cache_invalid", "Cache file does not exist", True
                )

            stats = cache_path.stat()

            # Check file size
            if stats.st_size < min_size:
                return CacheValidationError(
                    "cache_invalid",
                    f"Cache file too small: {stats.st_size} bytes",
                    True,
                )

            # Check age
            age = datetime.now(timezone.utc) - datetime.fromtimestamp(
                stats.st_mtime, timezone.utc
            )
            if age > max_age:
                return CacheValidationError(
                    "cache_invalid",
                    f"Cache too old: {age.days} days",
                    True,
                )

            return None

        except Exception as e:
            return CacheValidationError(
                "file_system_error",
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
        except Exception as e:
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
    def validate_cache_records(cls, record_count: int) -> bool:
        """Validate cache contains records.

        Args:
            record_count: Number of records in cache

        Returns:
            True if record count is valid, False otherwise
        """
        return record_count > 0

    @classmethod
    def validate_cache_data(
        cls, df: pd.DataFrame, allow_empty: bool = False
    ) -> Optional[CacheValidationError]:
        """Validate cached DataFrame structure and content.

        Args:
            df: DataFrame from cache
            allow_empty: Whether empty DataFrames are considered valid

        Returns:
            Error details if validation fails, None if valid
        """
        try:
            DataFrameValidator.validate_dataframe(df, allow_empty=allow_empty)
            return None
        except ValueError as e:
            return CacheValidationError(
                "data_integrity_error",
                f"Invalid cache data: {str(e)}",
                True,
            )

    @staticmethod
    def calculate_checksum(file_path: Path) -> str:
        """Calculate SHA-256 checksum of a file.

        Args:
            file_path: Path to file

        Returns:
            Hexadecimal checksum string
        """
        try:
            return hashlib.sha256(file_path.read_bytes()).hexdigest()
        except Exception as e:
            logger.error(f"Error calculating checksum for {file_path}: {e}")
            raise

    @staticmethod
    def safely_read_arrow_file(
        file_path: Path, columns: Optional[list] = None
    ) -> Optional[pd.DataFrame]:
        """Safely read an Arrow file with error handling.

        This is a wrapper around SafeMemoryMap.safely_read_arrow_file for compatibility.

        Args:
            file_path: Path to Arrow file
            columns: Optional columns to read

        Returns:
            DataFrame if successful, None if failed
        """
        # Delegate to the unified implementation in SafeMemoryMap
        return SafeMemoryMap.safely_read_arrow_file(file_path, columns)


class CacheKeyManager:
    """Centralized manager for cache keys and paths."""

    @staticmethod
    def get_cache_key(symbol: str, interval: str, date: datetime) -> str:
        """Generate a standardized cache key string.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            Standardized cache key string
        """
        return f"{symbol}_{interval}_{date.strftime('%Y%m')}"

    @staticmethod
    def get_cache_path(
        cache_dir: Path, symbol: str, interval: str, date: datetime
    ) -> Path:
        """Generate standardized cache file path.

        Args:
            cache_dir: Base cache directory
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            Path to cache file
        """
        year_month = date.strftime("%Y%m")
        return cache_dir / symbol / interval / f"{year_month}.arrow"


class VisionCacheManager:
    """Manages cache operations for Vision data.

    This class centralizes caching operations that were previously part of VisionDataClient.
    """

    @staticmethod
    async def save_to_cache(
        df: pd.DataFrame,
        cache_path: Path,
        start_time: datetime,
    ) -> tuple[str, int]:
        """Save DataFrame to cache in Arrow format.

        Args:
            df: DataFrame to save
            cache_path: Target cache path
            start_time: Start time for the data

        Returns:
            Tuple of (checksum, record_count)

        Raises:
            pa.ArrowInvalid: If DataFrame cannot be converted to Arrow format
            pa.ArrowIOError: If there are I/O errors during file operations
            ValueError: If input validation fails
            OSError: If file system operations fail
        """
        try:
            # Validate inputs
            if not isinstance(cache_path, Path):
                cache_path = Path(cache_path)

            # Ensure parent directory exists
            cache_path.parent.mkdir(parents=True, exist_ok=True)

            # Ensure we don't include the end date
            if not df.empty:
                last_date = df.index[-1].replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                df = df[df.index < last_date + timedelta(days=1)]

            # Reset index to include it in the Arrow table
            df_with_index = df.reset_index()

            try:
                # Convert to Arrow table
                table = pa.Table.from_pandas(df_with_index)
            except pa.ArrowInvalid as e:
                logger.error(f"Failed to convert DataFrame to Arrow format: {e}")
                raise

            try:
                # Save to Arrow file with proper resource cleanup
                with pa.OSFile(str(cache_path), "wb") as sink:
                    with pa.ipc.new_file(sink, table.schema) as writer:
                        writer.write_table(table)
            except pa.ArrowIOError as e:
                logger.error(f"Failed to write Arrow file: {e}")
                raise

            # Calculate checksum using the centralized utility
            checksum = CacheValidator.calculate_checksum(cache_path)
            record_count = len(df)

            logger.info(f"Saved {record_count} records to {cache_path}")
            return checksum, record_count

        except Exception as e:
            logger.error(f"Unexpected error saving to cache: {e}")
            raise

    @staticmethod
    async def load_from_cache(
        cache_path: Path, columns: Optional[Sequence[str]] = None
    ) -> Optional[pd.DataFrame]:
        """Load data from cache with optimized memory usage.

        Args:
            cache_path: Path to cache file
            columns: Optional list of columns to include

        Returns:
            DataFrame or None if load fails
        """
        start_time_perf = time.perf_counter()

        try:
            # Use the centralized SafeMemoryMap utility to read Arrow file
            logger.debug(f"Loading cached data from {cache_path}")
            df = SafeMemoryMap.safely_read_arrow_file(cache_path, columns)

            if df is None:
                raise ValueError(f"Failed to read data from {cache_path}")

            # Remove duplicates efficiently if needed
            if df.index.has_duplicates:
                t0 = time.perf_counter()
                # Use drop_duplicates method instead of boolean indexing
                df = (
                    df.reset_index()
                    .drop_duplicates(subset=["open_time"], keep="first")
                    .set_index("open_time")
                )
                logger.info(f"Duplicate removal took: {time.perf_counter() - t0:.6f}s")

            total_time = time.perf_counter() - start_time_perf
            logger.info(f"Total cache loading time: {total_time:.6f}s")
            return df

        except Exception as e:
            logger.error(f"Failed to load from cache: {e}")
            raise
