#!/usr/bin/env python
"""Centralized cache validation utilities.

This module provides standardized tools for validating cache integrity, checksums,
and metadata across different components to reduce duplication and ensure consistency.
"""

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple, Any, NamedTuple
import pandas as pd
import pyarrow as pa

from utils.logger_setup import get_logger
from utils.validation import DataFrameValidator

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


class CacheValidationError(NamedTuple):
    """Standardized cache validation error details."""

    error_type: str
    message: str
    is_recoverable: bool


class SafeMemoryMap:
    """Safe memory mapping context manager for Arrow files."""

    def __init__(self, path: Path):
        """Initialize with file path."""
        self.path = path
        self.mmap = None

    def __enter__(self) -> pa.MemoryMappedFile:
        """Open memory map."""
        try:
            self.mmap = pa.memory_map(str(self.path))
            return self.mmap
        except Exception as e:
            logger.error(f"Failed to memory map {self.path}: {e}")
            raise

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[object],
    ) -> None:
        """Close memory map."""
        if self.mmap:
            self.mmap.close()

    @staticmethod
    def safely_read_arrow_file(
        file_path: Path, columns: Optional[list] = None
    ) -> Optional[pd.DataFrame]:
        """Safely read an Arrow file with error handling.

        Args:
            file_path: Path to Arrow file
            columns: Optional columns to read

        Returns:
            DataFrame if successful, None if failed
        """
        from utils.config import CANONICAL_INDEX_NAME

        try:
            with SafeMemoryMap(file_path) as mmap:
                reader = pa.ipc.RecordBatchFileReader(mmap)
                if columns:
                    # Read specific columns if provided
                    table = reader.read_all().select(columns)
                else:
                    # Read all columns
                    table = reader.read_all()

                df = table.to_pandas()

                # Set open_time as index if present
                if CANONICAL_INDEX_NAME in df.columns:
                    df.set_index(CANONICAL_INDEX_NAME, inplace=True)

                return df

        except Exception as e:
            logger.error(f"Error reading Arrow file {file_path}: {e}")
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
