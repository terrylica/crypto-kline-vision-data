#!/usr/bin/env python
"""Core cache validation utilities.

# polars-exception: CacheValidator has deep integration with pandas-based
# ApiBoundaryValidator and DataFrameValidator that need coordinated migration

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from cache_validator.py for modularity
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import pyarrow as pa

from data_source_manager.utils.cache.errors import ERROR_TYPES, CacheValidationError
from data_source_manager.utils.cache.memory_map import SafeMemoryMap
from data_source_manager.utils.cache.options import AlignmentOptions, ValidationOptions
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import Interval
from data_source_manager.utils.validation import DataFrameValidator

if TYPE_CHECKING:
    from data_source_manager.utils.api_boundary_validator import ApiBoundaryValidator

__all__ = [
    "CacheValidator",
]


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

    def __init__(self, api_boundary_validator: ApiBoundaryValidator | None = None) -> None:
        """Initialize the CacheValidator with optional ApiBoundaryValidator.

        Args:
            api_boundary_validator: Optional ApiBoundaryValidator for API boundary validations
        """
        self.api_boundary_validator = api_boundary_validator

    @classmethod
    def validate_cache_integrity(
        cls,
        cache_path: Path,
        max_age: timedelta | None = None,
        min_size: int | None = None,
    ) -> CacheValidationError | None:
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
                return CacheValidationError(ERROR_TYPES["FILE_SYSTEM"], "Cache file does not exist", True)

            stats = cache_path.stat()

            if stats.st_size < min_size:
                return CacheValidationError(
                    ERROR_TYPES["DATA_INTEGRITY"],
                    f"Cache file too small: {stats.st_size} bytes",
                    True,
                )

            age = datetime.now(timezone.utc) - datetime.fromtimestamp(stats.st_mtime, timezone.utc)
            if age > max_age:
                return CacheValidationError(
                    ERROR_TYPES["CACHE_INVALID"],
                    f"Cache too old: {age.days} days",
                    True,
                )

            return None

        except (OSError, PermissionError, ValueError) as e:
            return CacheValidationError(
                ERROR_TYPES["FILE_SYSTEM"],
                f"Error validating cache: {e!s}",
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
            current_checksum = cls.calculate_checksum(cache_path)
            return current_checksum == stored_checksum
        except (OSError, ValueError) as e:
            logger.error(f"Error validating cache checksum: {e}")
            return False

    @classmethod
    def validate_cache_metadata(
        cls,
        cache_info: dict[str, Any] | None,
        required_fields: list | None = None,
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
    def validate_cache_records(cls, record_count: int | str) -> bool:
        """Validate cache contains records.

        Args:
            record_count: Number of records in cache (as int or str)

        Returns:
            True if record count is valid, False otherwise
        """
        try:
            if isinstance(record_count, str):
                record_count = int(record_count)
            return record_count > 0
        except (ValueError, TypeError):
            return False

    async def validate_cache_data(
        self,
        df: pd.DataFrame,
        options: ValidationOptions | None = None,
        allow_empty: bool | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        interval: Interval | None = None,
        symbol: str | None = None,
    ) -> CacheValidationError | None:
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
        if options is None:
            options = ValidationOptions()

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

        if df.empty and not options.allow_empty:
            return CacheValidationError(
                ERROR_TYPES["VALIDATION"],
                "DataFrame is empty",
                True,
            )

        try:
            DataFrameValidator.validate_dataframe(df)
        except ValueError as e:
            return CacheValidationError(
                ERROR_TYPES["VALIDATION"],
                f"DataFrame validation failed: {e}",
                False,
            )

        if (
            self.api_boundary_validator
            and options.start_time
            and options.end_time
            and options.interval
            and not df.empty
        ):
            try:
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
                        True,
                    )

                logger.debug("Cache data boundaries match REST API behavior")
            except (ValueError, RuntimeError, OSError, ConnectionError) as e:
                logger.warning("API boundary validation failed: %s", e)

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
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def safely_read_arrow_file(file_path: Path, columns: list | None = None) -> pd.DataFrame | None:
        """Safely read an Arrow file with proper error handling.

        Args:
            file_path: Path to Arrow file
            columns: Optional list of columns to read

        Returns:
            DataFrame or None if read fails
        """
        try:
            with SafeMemoryMap(file_path) as source, pa.ipc.open_file(source) as reader:
                if columns:
                    all_cols = reader.schema.names
                    cols_to_read = (
                        ["open_time", *list(columns)]
                        if "open_time" in all_cols and "open_time" not in columns
                        else list(columns)
                    )
                    table = reader.read_all().select(cols_to_read)
                else:
                    table = reader.read_all()

                df = table.to_pandas(
                    zero_copy_only=False,
                    date_as_object=False,
                    use_threads=True,
                )

                if "open_time" in df.columns and df.index.name != "open_time":
                    df = df.set_index("open_time")

                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index, utc=True)
                elif df.index.tz is None:
                    df.index = df.index.tz_localize("UTC")

                return df
        except (OSError, pa.ArrowInvalid, pa.ArrowIOError, ValueError) as e:
            logger.error("Error reading Arrow file %s: %s", file_path, e)
            return None

    @staticmethod
    async def safely_read_arrow_file_async(file_path: Path, columns: list | None = None) -> pd.DataFrame | None:
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
        options_or_start_time: AlignmentOptions | datetime | None = None,
        end_time_or_interval: datetime | Interval | None = None,
        interval_or_symbol: Interval | str | None = None,
        symbol: str | None = None,
    ) -> pd.DataFrame:
        """Align cache data to match what would be returned by the Binance REST API.

        Args:
            df: DataFrame containing cached data
            options_or_start_time: Either AlignmentOptions object or start_time
            end_time_or_interval: Either end_time or interval
            interval_or_symbol: Either interval or symbol
            symbol: Symbol for alignment

        Returns:
            DataFrame aligned to REST API boundaries

        Raises:
            ValueError: If ApiBoundaryValidator is not provided or required parameters missing
        """
        if df.empty:
            return df

        if not self.api_boundary_validator:
            raise ValueError("ApiBoundaryValidator is required for cache data alignment")

        if isinstance(options_or_start_time, datetime):
            start_time = options_or_start_time
            end_time = end_time_or_interval
            interval = interval_or_symbol
            symbol_param = symbol or "BTCUSDT"

            if start_time is None or end_time is None or interval is None:
                raise ValueError("start_time, end_time, and interval parameters must be provided")

            options = AlignmentOptions(
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                symbol=symbol_param,
            )
        else:
            options = options_or_start_time or AlignmentOptions(
                start_time=datetime.now(timezone.utc),
                end_time=datetime.now(timezone.utc) + timedelta(hours=1),
                interval=Interval.MINUTE_1,
            )

            if isinstance(end_time_or_interval, datetime):
                options.start_time = options_or_start_time
                options.end_time = end_time_or_interval
                options.interval = interval_or_symbol
                if symbol:
                    options.symbol = symbol
            else:
                if end_time_or_interval is not None:
                    options.end_time = end_time_or_interval
                if interval_or_symbol is not None:
                    options.interval = interval_or_symbol
                if symbol is not None:
                    options.symbol = symbol

        api_boundaries = await self.api_boundary_validator.get_api_boundaries(
            options.start_time,
            options.end_time,
            options.interval,
            symbol=options.symbol,
        )

        if api_boundaries["record_count"] == 0:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="open_time"))

        api_start_time = api_boundaries["api_start_time"]
        api_end_time = api_boundaries["api_end_time"]

        return df[(df.index >= api_start_time) & (df.index <= api_end_time)]
