#!/usr/bin/env python
"""Centralized validation utilities for data integrity and constraints."""

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Union, Sequence, Any, Tuple
import re
import pandas as pd
import numpy as np

from utils.logger_setup import get_logger

# Column name constants
OHLCV_COLUMNS = ["open", "high", "low", "close", "volume"]
ALL_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trades",
    "taker_buy_volume",
    "taker_buy_quote_volume",
]

# Regex Patterns
TICKER_PATTERN = re.compile(r"^[A-Z0-9]{1,20}$")  # Match individual tickers
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{1,20}(USDT|BTC|ETH|BNB)$")  # Trading pairs
INTERVAL_PATTERN = re.compile(
    r"^(1s|1m|3m|5m|15m|30m|1h|2h|4h|6h|8h|12h|1d|3d|1w|1M)$"
)  # Valid intervals

logger = get_logger(__name__, "INFO", show_path=False)

from utils.config import (
    CANONICAL_INDEX_NAME,
    DEFAULT_TIMEZONE,
    MIN_VALID_FILE_SIZE,
    MAX_CACHE_AGE,
    OUTPUT_DTYPES,
)


class ValidationError(Exception):
    """Custom exception for validation errors."""

    pass


class DataValidation:
    """Centralized data validation utilities."""

    @staticmethod
    def validate_dates(start_time: datetime, end_time: datetime) -> None:
        """Validate dates for market data requests.

        Args:
            start_time: Start time
            end_time: End time

        Raises:
            ValueError: If the time range is invalid
        """
        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise ValueError(
                f"Invalid date types: start_time={type(start_time)}, end_time={type(end_time)}. "
                "Both must be datetime objects."
            )

        # Convert to UTC for consistent comparison
        start_utc = DataValidation.enforce_utc_timestamp(start_time)
        end_utc = DataValidation.enforce_utc_timestamp(end_time)

        # Check if the end time is in the future
        now_utc = datetime.now(timezone.utc)
        if end_utc > now_utc:
            raise ValueError(f"End time {end_utc.isoformat()} is in the future")

        if start_utc >= end_utc:
            raise ValueError(
                f"Invalid time range: start_time ({start_utc.isoformat()}) must be before "
                f"end_time ({end_utc.isoformat()})."
            )

    @staticmethod
    def validate_time_window(start_time: datetime, end_time: datetime) -> None:
        """Validate time window for market data requests.

        Args:
            start_time: Start time
            end_time: End time

        Raises:
            ValueError: If time window is invalid
        """
        # Standard date validation
        DataValidation.validate_dates(start_time, end_time)

        # Add any additional time window validations here
        time_diff = end_time - start_time
        if time_diff > timedelta(days=365):
            logger.warning(
                f"Large time window requested: {time_diff.days} days. "
                "This may cause performance issues."
            )

    @staticmethod
    def enforce_utc_timestamp(dt: datetime) -> datetime:
        """Ensure timestamp is UTC.

        Args:
            dt: Datetime object

        Returns:
            UTC timezone-aware datetime
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        elif dt.tzinfo != timezone.utc:
            # Create a new datetime with timezone.utc instead of just converting
            return datetime(
                dt.year,
                dt.month,
                dt.day,
                dt.hour,
                dt.minute,
                dt.second,
                dt.microsecond,
                tzinfo=timezone.utc,
            )
        return dt

    @staticmethod
    def validate_time_range(
        start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> tuple[Optional[datetime], Optional[datetime]]:
        """Validate and normalize time range parameters.

        Args:
            start_time: Start time of data request
            end_time: End time of data request

        Returns:
            Tuple of (normalized start_time, normalized end_time)

        Raises:
            ValueError: If end time is not after start time
        """
        if start_time is not None:
            start_time = DataValidation.enforce_utc_timestamp(start_time)
        if end_time is not None:
            end_time = DataValidation.enforce_utc_timestamp(end_time)
        if start_time and end_time and start_time >= end_time:
            raise ValueError("End time must be after start time")
        return start_time, end_time

    @staticmethod
    def validate_interval(interval: str, market_type: str = "SPOT") -> None:
        """Validate interval string format.

        Args:
            interval: Time interval string (e.g., '1s', '1m')
            market_type: Market type for context-specific validation

        Raises:
            ValueError: If interval format is invalid
        """
        # Add market-specific interval validation
        supported_intervals = {
            "SPOT": [
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
            ],
            "FUTURES": [
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
            ],
        }

        market = market_type.upper()
        if market not in supported_intervals:
            market = "SPOT"  # Default to SPOT intervals

        if interval not in supported_intervals[market]:
            raise ValueError(
                f"Invalid interval: {interval}. "
                f"Supported intervals for {market}: {supported_intervals[market]}"
            )

    @staticmethod
    def validate_symbol_format(symbol: str, market_type: str = "SPOT") -> None:
        """Validate trading pair symbol format.

        Args:
            symbol: Trading pair symbol
            market_type: Market type for context-specific validation

        Raises:
            ValueError: If symbol format is invalid
        """
        if not isinstance(symbol, str) or not symbol:
            raise ValueError(
                f"Invalid {market_type} symbol format: Symbol must be a non-empty string."
            )
        if not symbol.isupper():
            raise ValueError(
                f"Invalid {market_type} symbol format: {symbol}. "
                "Symbols should be uppercase (e.g., BTCUSDT)."
            )

    @staticmethod
    def validate_data_availability(
        start_time: datetime,
        end_time: datetime,
        consolidation_delay: timedelta = timedelta(hours=48),
    ) -> None:
        """Validate if data should be available for the given time range.

        Args:
            start_time: Start of time range
            end_time: End of time range
            consolidation_delay: Delay after which data is considered available

        Raises:
            ValueError: If data is definitely not available for the time range
        """
        cutoff_date = datetime.now(timezone.utc) - consolidation_delay
        if end_time > cutoff_date:
            raise ValueError(
                "Requested end time is too recent. "
                f"Data is only reliably available up to {cutoff_date.isoformat()} "
                f"due to consolidation delays (delay={consolidation_delay})."
            )
        if start_time > end_time:
            raise ValueError("Start time cannot be after end time.")

    @staticmethod
    def is_data_likely_available(
        target_date: datetime, consolidation_delay: timedelta = timedelta(hours=48)
    ) -> bool:
        """Check if data is likely to be available based on consolidation delay.

        Args:
            target_date: Date to check for data availability
            consolidation_delay: Delay after which data is considered available

        Returns:
            True if data is likely available
        """
        cutoff_date = datetime.now(timezone.utc) - consolidation_delay
        return target_date <= cutoff_date

    @staticmethod
    def validate_time_boundaries(
        df: pd.DataFrame, start_time: datetime, end_time: datetime
    ) -> None:
        """Validate that DataFrame covers the requested time range.

        Args:
            df: DataFrame to validate
            start_time: Start time (inclusive)
            end_time: End time (exclusive)

        Raises:
            ValueError: If data doesn't cover requested time range
        """
        # For empty DataFrame, just validate the time range itself
        if df.empty:
            if start_time > end_time:
                raise ValueError(
                    f"Start time {start_time} is after end time {end_time}"
                )
            if end_time > datetime.now(timezone.utc):
                raise ValueError(f"End time {end_time} is in the future")
            return

        # Ensure index is timezone-aware
        if df.index.tz is None:
            raise ValueError("DataFrame index must be timezone-aware")

        # Convert times to UTC for comparison
        start_time = DataValidation.enforce_utc_timestamp(start_time)
        end_time = DataValidation.enforce_utc_timestamp(end_time)

        # Get actual data boundaries
        data_start = df.index.min()
        data_end = df.index.max()

        # Check if data covers requested range, ignoring microsecond precision
        data_start_floor = data_start.replace(microsecond=0)
        data_end_floor = data_end.replace(microsecond=0)
        start_time_floor = start_time.replace(microsecond=0)
        end_time_floor = end_time.replace(microsecond=0)

        # Adjust end_time_floor for exclusive comparison
        # We need data up to but not including the end time
        adjusted_end_time_floor = end_time_floor - timedelta(seconds=1)

        if data_start_floor > start_time_floor:
            raise ValueError(
                f"Data starts later than requested: {data_start} > {start_time}"
            )

        # Data end checks - don't fail if we have at least some data
        if data_end_floor < adjusted_end_time_floor:
            # Instead of raising an error, just log a warning if we at least have data at the start
            if data_start_floor == start_time_floor:
                logger.warning(
                    f"Data doesn't cover entire requested range: ends at {data_end} < {adjusted_end_time_floor}. "
                    f"This may be due to market-specific limitations or data availability."
                )
            else:
                # Still raise error if data doesn't even start at the requested time
                raise ValueError(
                    f"Data ends earlier than requested: {data_end} < {adjusted_end_time_floor}"
                )

        # Verify data is sorted
        if not df.index.is_monotonic_increasing:
            raise ValueError("Data is not sorted by time")


class DataFrameValidator:
    """Validation and standardization for DataFrames."""

    @staticmethod
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

        # Log timezone information for debugging
        logger.debug(f"DataFrame index timezone: {df.index.tz}")
        logger.debug(f"timezone.utc: {timezone.utc}")
        logger.debug(f"DEFAULT_TIMEZONE: {DEFAULT_TIMEZONE}")
        logger.debug(f"Are they equal? {df.index.tz == timezone.utc}")
        logger.debug(f"Are they the same object? {df.index.tz is timezone.utc}")

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
        required_columns = ["open", "high", "low", "close", "volume"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"DataFrame missing required columns: {missing_columns}")

    @staticmethod
    def format_dataframe(
        df: pd.DataFrame, output_dtypes: Dict[str, str] = OUTPUT_DTYPES
    ) -> pd.DataFrame:
        """Format DataFrame to ensure consistent structure.

        Args:
            df: Input DataFrame
            output_dtypes: Dictionary mapping column names to dtypes

        Returns:
            Formatted DataFrame
        """
        logger.debug("Formatting DataFrame - Starting timezone analysis")
        logger.debug(f"timezone.utc id: {id(timezone.utc)}")
        logger.debug(f"DEFAULT_TIMEZONE id: {id(DEFAULT_TIMEZONE)}")

        if df.empty:
            logger.debug("Creating empty DataFrame with timezone.utc timezone")
            # Create empty DataFrame with correct structure
            empty_df = pd.DataFrame(columns=list(output_dtypes.keys()))
            for col, dtype in output_dtypes.items():
                empty_df[col] = empty_df[col].astype(dtype)
            empty_df.index = pd.DatetimeIndex(
                [], name=CANONICAL_INDEX_NAME, tz=timezone.utc
            )
            logger.debug(f"Empty DataFrame index timezone: {empty_df.index.tz}")
            logger.debug(f"Is timezone.utc? {empty_df.index.tz is timezone.utc}")
            return empty_df

        # Copy to avoid modifying original
        formatted_df = df.copy()

        if (
            isinstance(formatted_df.index, pd.DatetimeIndex)
            and formatted_df.index.tz is not None
        ):
            logger.debug(
                f"Input DataFrame timezone before processing: {formatted_df.index.tz}"
            )
            logger.debug(f"Is timezone.utc? {formatted_df.index.tz is timezone.utc}")

        # Ensure index is DatetimeIndex in UTC
        if not isinstance(formatted_df.index, pd.DatetimeIndex):
            logger.debug("Converting non-DatetimeIndex to DatetimeIndex")
            if "open_time" in formatted_df.columns:
                formatted_df = formatted_df.set_index("open_time")
            else:
                raise ValueError(
                    "DataFrame must have 'open_time' column or DatetimeIndex"
                )

        # Ensure index is named correctly
        formatted_df.index.name = CANONICAL_INDEX_NAME

        # Ensure index is timezone-aware and in UTC
        # Use timezone.utc directly instead of DEFAULT_TIMEZONE
        if formatted_df.index.tz is None:
            logger.debug("Localizing naive DatetimeIndex to timezone.utc")
            formatted_df.index = formatted_df.index.tz_localize(timezone.utc)
        elif formatted_df.index.tz != timezone.utc:
            logger.debug(f"Converting from {formatted_df.index.tz} to timezone.utc")
            # Create a new DatetimeIndex with timezone.utc explicitly
            new_index = pd.DatetimeIndex(
                [
                    dt.replace(tzinfo=timezone.utc)
                    for dt in formatted_df.index.to_pydatetime()
                ],
                name=formatted_df.index.name,
            )
            formatted_df.index = new_index

        logger.debug(f"Final DataFrame timezone: {formatted_df.index.tz}")
        logger.debug(f"Is timezone.utc? {formatted_df.index.tz is timezone.utc}")

        # Convert columns to specified dtypes
        for col, dtype in output_dtypes.items():
            if col in formatted_df.columns:
                try:
                    formatted_df[col] = formatted_df[col].astype(dtype)
                except (ValueError, TypeError) as e:
                    # Handle conversion errors gracefully
                    formatted_df[col] = pd.Series(
                        np.nan, index=formatted_df.index, dtype=dtype
                    )

        # Sort by index
        if not formatted_df.index.is_monotonic_increasing:
            formatted_df = formatted_df.sort_index()

        # Remove duplicates if any
        if formatted_df.index.has_duplicates:
            formatted_df = formatted_df[~formatted_df.index.duplicated(keep="first")]

        return formatted_df

    @staticmethod
    def validate_cache_integrity(
        file_path: pd.DataFrame,
        min_size: int = MIN_VALID_FILE_SIZE,
        max_age: timedelta = MAX_CACHE_AGE,
    ) -> Optional[Dict[str, Any]]:
        """Validate cache file integrity.

        Args:
            file_path: Path to cache file
            min_size: Minimum valid file size
            max_age: Maximum allowed age for cache file

        Returns:
            Error information if validation fails, None if valid
        """
        import os
        from pathlib import Path

        file_path = Path(file_path)

        # Check if file exists
        if not file_path.exists():
            return {
                "error_type": "file_missing",
                "message": f"File does not exist: {file_path}",
                "is_recoverable": True,
            }

        # Check file size
        file_size = os.path.getsize(file_path)
        if file_size < min_size:
            return {
                "error_type": "file_too_small",
                "message": f"File too small: {file_size} bytes",
                "is_recoverable": True,
            }

        # Check file age
        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc)
        age = datetime.now(timezone.utc) - file_mtime

        if age > max_age:
            return {
                "error_type": "file_too_old",
                "message": f"File too old: {age.days} days",
                "is_recoverable": True,
            }

        return None
