#!/usr/bin/env python
"""Centralized validation utilities for data integrity and constraints."""

from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Union, Any
import re
import pandas as pd
import numpy as np

from utils.logger_setup import logger
from utils.market_constraints import Interval
from utils.api_boundary_validator import ApiBoundaryValidator

# Column name constants
OHLCV_COLUMNS = ["open", "high", "low", "close", "volume"]
ALL_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
]

# Regex Patterns
TICKER_PATTERN = re.compile(r"^[A-Z0-9]{1,20}$")  # Match individual tickers
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{1,20}(USDT|BTC|ETH|BNB)$")  # Trading pairs
INTERVAL_PATTERN = re.compile(
    r"^(1s|1m|3m|5m|15m|30m|1h|2h|4h|6h|8h|12h|1d|3d|1w|1M)$"
)  # Valid intervals


from utils.config import (
    CANONICAL_INDEX_NAME,
    DEFAULT_TIMEZONE,
    MIN_VALID_FILE_SIZE,
    MAX_CACHE_AGE,
    OUTPUT_DTYPES,
    MAX_TIME_RANGE,
)


class ValidationError(Exception):
    """Custom exception for validation errors."""


class DataValidation:
    """Centralized data validation utilities."""

    def __init__(self, api_boundary_validator: Optional[ApiBoundaryValidator] = None):
        """Initialize the DataValidation class.

        Args:
            api_boundary_validator: Optional ApiBoundaryValidator instance for API boundary validations
        """
        self.api_boundary_validator = api_boundary_validator

    @staticmethod
    def validate_dates(start_time: datetime, end_time: datetime) -> None:
        """Validate date inputs.

        Args:
            start_time: Start time
            end_time: End time

        Raises:
            ValueError: If start_time is after end_time
        """
        if start_time >= end_time:
            raise ValueError(
                f"Start time must be before end time: {start_time} >= {end_time}"
            )

        # Enforce timezone awareness
        if start_time.tzinfo is None or end_time.tzinfo is None:
            raise ValueError("Start and end times must be timezone-aware")

    @staticmethod
    def validate_time_window(start_time: datetime, end_time: datetime) -> None:
        """Validate time window for market data.

        Args:
            start_time: Start time
            end_time: End time

        Raises:
            ValueError: If time window exceeds maximum allowed
        """
        # Ensure dates are valid first
        DataValidation.validate_dates(start_time, end_time)

        # Ensure time range is not too large
        time_diff = end_time - start_time
        if time_diff > MAX_TIME_RANGE:
            raise ValueError(f"Time range exceeds maximum allowed: {MAX_TIME_RANGE}")

        # REMOVED: validate_time_boundaries - No longer enforcing manual alignment for REST API calls

    @staticmethod
    def enforce_utc_timestamp(dt: datetime) -> datetime:
        """Ensures datetime object is timezone aware and in UTC.

        Args:
            dt: Input datetime

        Returns:
            UTC timezone-aware datetime
        """
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

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

    async def validate_api_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Union[str, Interval],
        symbol: str = "BTCUSDT",
    ) -> bool:
        """Validates time range against Binance API boundaries using ApiBoundaryValidator.

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
            ValidationError: If data may not be available for the entire time range
        """
        # Check if any portion of the time range is too recent
        now = datetime.now(timezone.utc)
        consolidation_threshold = now - consolidation_delay

        if end_time > consolidation_threshold:
            # Time range includes data that may not be fully consolidated
            logger.warning(
                f"Time range includes recent data that may not be fully consolidated. "
                f"Data after {consolidation_threshold.isoformat()} may be incomplete."
            )

    @staticmethod
    def is_data_likely_available(
        target_date: datetime, consolidation_delay: timedelta = timedelta(hours=48)
    ) -> bool:
        """Check if data is likely available for the specified date.

        Args:
            target_date: Date to check
            consolidation_delay: Delay after which data is considered available

        Returns:
            True if data is likely available, False otherwise
        """
        now = datetime.now(timezone.utc)
        consolidation_threshold = now - consolidation_delay
        return target_date <= consolidation_threshold


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

        # Special handling for close_time if it exists - must be datetime64[ns]
        if "close_time" in formatted_df.columns:
            if not pd.api.types.is_datetime64_dtype(formatted_df["close_time"]):
                # Convert close_time to datetime if it's not already
                if pd.api.types.is_numeric_dtype(formatted_df["close_time"]):
                    # If it's a numeric type, convert from timestamp
                    formatted_df["close_time"] = pd.to_datetime(
                        formatted_df["close_time"], unit="us", utc=True
                    )
                else:
                    # Otherwise try standard conversion
                    formatted_df["close_time"] = pd.to_datetime(
                        formatted_df["close_time"], utc=True
                    )

            # Instead of using astype, which fails when converting from tz-aware to tz-naive,
            # we'll convert the timestamps properly preserving timezone information
            if (
                hasattr(formatted_df["close_time"].dtype, "tz")
                and formatted_df["close_time"].dtype.tz is not None
            ):
                # Convert to UTC first if it has timezone info
                formatted_df["close_time"] = formatted_df["close_time"].dt.tz_convert(
                    "UTC"
                )
                # Then either keep as is or convert to timezone-naive if needed for compatibility
                # formatted_df["close_time"] = formatted_df["close_time"].dt.tz_localize(None)
            else:
                # If it's already timezone-naive, ensure it's the right dtype
                formatted_df["close_time"] = pd.to_datetime(formatted_df["close_time"])

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
