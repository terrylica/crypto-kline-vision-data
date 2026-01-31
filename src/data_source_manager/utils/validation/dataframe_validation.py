#!/usr/bin/env python
# polars-exception: Legacy validation code - DataFrames from upstream APIs are Pandas
"""DataFrame validation utilities for market data operations.

This module provides validation for:
- DataFrame structure and integrity
- OHLCV data validation
- Cache file integrity
- Timestamp precision standardization

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Split from utils/validation.py for modularity (<400 lines)
"""

from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from data_source_manager.utils.config import (
    CANONICAL_INDEX_NAME,
    DEFAULT_TIMEZONE,
    MAX_CACHE_AGE,
    MILLISECOND_DIGITS,
    MIN_VALID_FILE_SIZE,
    OUTPUT_DTYPES,
    TIMESTAMP_PRECISION,
)
from data_source_manager.utils.loguru_setup import logger


class DataFrameValidator:
    """Validation and standardization for DataFrames."""

    def __init__(self, df: pd.DataFrame | None = None):
        """Initialize with a DataFrame to validate.

        Args:
            df: DataFrame to validate
        """
        self.df = df

    @staticmethod
    def validate_dataframe(df: pd.DataFrame) -> None:
        """Validate DataFrame structure and integrity.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If DataFrame structure is invalid
        """
        if df.empty:
            logger.debug("Validating empty DataFrame - passing validation")
            return

        logger.debug(f"Starting DataFrame validation for DataFrame with {len(df)} rows")

        logger.debug(f"Checking index type: {type(df.index).__name__}")
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError(f"DataFrame index must be DatetimeIndex, got {type(df.index).__name__}")

        logger.debug("Checking if index is timezone-aware")
        if df.index.tz is None:
            raise ValueError("DataFrame index must be timezone-aware")

        logger.debug(f"DataFrame index timezone: {df.index.tz}")
        logger.debug(f"timezone.utc: {timezone.utc}")
        logger.debug(f"DEFAULT_TIMEZONE: {DEFAULT_TIMEZONE}")

        logger.debug(f"Checking index name: {df.index.name} vs expected: {CANONICAL_INDEX_NAME}")
        if df.index.name != CANONICAL_INDEX_NAME:
            raise ValueError(f"DataFrame index must be named '{CANONICAL_INDEX_NAME}', got '{df.index.name}'")

        logger.debug(f"Checking for duplicate indices in DataFrame with {len(df)} rows")
        if df.index.has_duplicates:
            raise ValueError("DataFrame index contains duplicate timestamps")

        logger.debug("Checking if index is monotonically increasing")
        if not df.index.is_monotonic_increasing:
            raise ValueError("DataFrame index must be monotonically increasing")

        required_columns = ["open", "high", "low", "close", "volume"]
        logger.debug(f"Checking for required columns: {required_columns}")
        logger.debug(f"Available columns: {df.columns.tolist()}")
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"DataFrame missing required columns: {missing_columns}")

        logger.debug("DataFrame validation completed successfully")

    def validate_klines_data(self) -> tuple[bool, str | None]:
        """Validate that a DataFrame contains valid klines market data.

        Returns:
            Tuple of (is_valid, error_message)
        """
        if self.df is None:
            return False, "No DataFrame provided for validation"

        if self.df.empty:
            logger.debug("Empty DataFrame passed validation")
            return True, None

        try:
            self.validate_dataframe(self.df)

            if TIMESTAMP_PRECISION == "ms" and hasattr(self.df.index, "astype"):
                sample_ts = self.df.index[0].value
                if len(str(abs(sample_ts))) > MILLISECOND_DIGITS:
                    logger.debug("Converting timestamps from microsecond to millisecond precision")

                    if isinstance(self.df.index, pd.DatetimeIndex):
                        rounded_index = pd.DatetimeIndex(
                            [pd.Timestamp(ts.timestamp() * 1000, unit="ms", tz=timezone.utc) for ts in self.df.index],
                            name=self.df.index.name,
                        )
                        self.df.index = rounded_index

                    if "open_time" in self.df.columns and pd.api.types.is_datetime64_dtype(self.df["open_time"]):
                        self.df["open_time"] = pd.to_datetime(
                            (self.df["open_time"].astype(int) // 1000000) * 1000,
                            unit="ms",
                            utc=True,
                        )

                    if "close_time" in self.df.columns and pd.api.types.is_datetime64_dtype(self.df["close_time"]):
                        self.df["close_time"] = pd.to_datetime(
                            (self.df["close_time"].astype(int) // 1000000) * 1000,
                            unit="ms",
                            utc=True,
                        )

            for col, dtype in OUTPUT_DTYPES.items():
                if col in self.df.columns and not pd.api.types.is_numeric_dtype(self.df[col]) and "time" not in col:
                    logger.warning(f"Column {col} has non-numeric dtype: {self.df[col].dtype}")
                    try:
                        self.df[col] = self.df[col].astype(dtype)
                    except (ValueError, TypeError) as e:
                        return (
                            False,
                            f"Failed to convert column {col} to {dtype}: {e!s}",
                        )

            critical_columns = ["open", "high", "low", "close"]
            for col in critical_columns:
                if col in self.df.columns and self.df[col].isna().any():
                    return False, f"Found NaN values in critical column: {col}"

            return True, None

        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error validating klines data: {e!s}")
            return False, str(e)

    @staticmethod
    def format_dataframe(df: pd.DataFrame, output_dtypes: dict[str, str] | None = None) -> pd.DataFrame:
        """Format DataFrame to ensure consistent structure.

        Args:
            df: Input DataFrame
            output_dtypes: Dictionary mapping column names to dtypes

        Returns:
            Formatted DataFrame
        """
        if output_dtypes is None:
            output_dtypes = OUTPUT_DTYPES

        logger.debug("Formatting DataFrame - Starting timezone analysis")

        if df.empty:
            logger.debug("Creating empty DataFrame with timezone.utc timezone")
            empty_df = pd.DataFrame(columns=list(output_dtypes.keys()))
            for col, dtype in output_dtypes.items():
                empty_df[col] = empty_df[col].astype(dtype)
            empty_df.index = pd.DatetimeIndex([], name=CANONICAL_INDEX_NAME, tz=timezone.utc)
            return empty_df

        logger.debug(f"Copying DataFrame with shape {df.shape}")
        formatted_df = df.copy()

        logger.debug(f"Index type check: {type(formatted_df.index).__name__}")
        if not isinstance(formatted_df.index, pd.DatetimeIndex):
            logger.debug("Converting non-DatetimeIndex to DatetimeIndex")
            if "open_time" in formatted_df.columns:
                logger.debug("Using open_time column for index")
                formatted_df = formatted_df.set_index("open_time")
            else:
                logger.error("Cannot find open_time column for index conversion")
                raise ValueError("DataFrame must have 'open_time' column or DatetimeIndex")

        logger.debug(f"Setting index name to {CANONICAL_INDEX_NAME}")
        formatted_df.index.name = CANONICAL_INDEX_NAME

        if formatted_df.index.tz is None:
            logger.debug("Localizing naive DatetimeIndex to timezone.utc")
            formatted_df.index = formatted_df.index.tz_localize(timezone.utc)
        elif formatted_df.index.tz != timezone.utc:
            logger.debug(f"Converting from {formatted_df.index.tz} to timezone.utc")
            new_index = pd.DatetimeIndex(
                [dt.replace(tzinfo=timezone.utc) for dt in formatted_df.index.to_pydatetime()],
                name=formatted_df.index.name,
            )
            formatted_df.index = new_index

        logger.debug(f"Final DataFrame timezone: {formatted_df.index.tz}")
        logger.debug(f"Final DataFrame shape: {formatted_df.shape}")
        return formatted_df

    @staticmethod
    def validate_cache_integrity(
        file_path: str,
        min_size: int = MIN_VALID_FILE_SIZE,
        max_age: timedelta = MAX_CACHE_AGE,
    ) -> dict[str, Any] | None:
        """Validate cache file integrity.

        Args:
            file_path: Path to cache file
            min_size: Minimum valid file size
            max_age: Maximum allowed age for cache file

        Returns:
            Error information if validation fails, None if valid
        """
        from pathlib import Path

        file_path_obj = Path(file_path)

        if not file_path_obj.exists():
            return {
                "error_type": "file_missing",
                "message": f"File does not exist: {file_path_obj}",
                "is_recoverable": True,
            }

        file_size = file_path_obj.stat().st_size
        if file_size < min_size:
            return {
                "error_type": "file_too_small",
                "message": f"File too small: {file_size} bytes",
                "is_recoverable": True,
            }

        file_mtime = datetime.fromtimestamp(file_path_obj.stat().st_mtime, timezone.utc)
        age = datetime.now(timezone.utc) - file_mtime

        if age > max_age:
            return {
                "error_type": "file_too_old",
                "message": f"File too old: {age.days} days",
                "is_recoverable": True,
            }

        return None

    @staticmethod
    def validate_dataframe_time_boundaries(df: pd.DataFrame, start_time: datetime, end_time: datetime) -> None:
        """Validate that DataFrame covers the requested time range.

        Args:
            df: DataFrame to validate
            start_time: Start time boundary
            end_time: End time boundary

        Raises:
            ValueError: If DataFrame doesn't cover the time range
        """
        if df.empty:
            return

        from data_source_manager.utils.validation.time_validation import DataValidation

        start_time = DataValidation.enforce_utc_timestamp(start_time)
        end_time = DataValidation.enforce_utc_timestamp(end_time)

        actual_start = df.index.min()
        actual_end = df.index.max()

        if actual_start > start_time + timedelta(microseconds=1000):
            raise ValueError(f"DataFrame starts at {actual_start}, which is after the requested start time {start_time}")

        if actual_end < end_time - timedelta(microseconds=1000):
            raise ValueError(f"DataFrame ends at {actual_end}, which is before the requested end time {end_time}")
