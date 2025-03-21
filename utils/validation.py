#!/usr/bin/env python
from utils.logger_setup import get_logger

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)

from datetime import datetime, timezone, timedelta
import pandas as pd
from typing import Optional, Dict, Any
from utils.config import (
    CANONICAL_INDEX_NAME,
    DEFAULT_TIMEZONE,
    MIN_VALID_FILE_SIZE,
    MAX_CACHE_AGE,
    OUTPUT_DTYPES,
)
import logging


class ValidationError(Exception):
    """Custom exception for validation errors."""

    pass


class DataValidation:
    """Data validation utilities."""

    @staticmethod
    def validate_dates(start_time: datetime, end_time: datetime) -> None:
        """Validate date time window.

        Args:
            start_time: Start time
            end_time: End time

        Raises:
            ValueError: If date inputs are invalid
        """
        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise ValueError("Start and end times must be datetime objects")

        # Ensure timezone awareness
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        # Validate time range
        if start_time >= end_time:
            raise ValueError(f"Start time {start_time} is after end time {end_time}")

        # Check for future dates
        now = datetime.now(timezone.utc)
        if end_time > now:
            raise ValueError(f"End time {end_time} is in the future")

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
            dt: Input datetime

        Returns:
            UTC-aware datetime

        Raises:
            TypeError: If input is not a datetime object
        """
        if not isinstance(dt, datetime):
            raise TypeError(f"Expected datetime object, got {type(dt)}")
        if dt.tzinfo is None:
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

    @staticmethod
    def validate_interval(interval: str, market_type: str = "SPOT") -> None:
        """Validate interval parameter.

        Args:
            interval: Time interval
            market_type: Market type

        Raises:
            ValueError: If interval is invalid
        """
        valid_intervals = [
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
        ]

        if interval not in valid_intervals:
            raise ValueError(
                f"Invalid interval: {interval}. "
                f"Must be one of {', '.join(valid_intervals)}"
            )

        # 1-second data is only available for spot market
        if interval == "1s" and market_type != "SPOT":
            raise ValueError(
                f"1-second data is only available for SPOT market, not {market_type}"
            )

    @staticmethod
    def validate_symbol_format(symbol: str, market_type: str = "SPOT") -> None:
        """Validate trading pair symbol format.

        Args:
            symbol: Trading pair symbol to validate
            market_type: Type of market (default: SPOT)

        Raises:
            ValueError: If symbol format is invalid
        """
        if not symbol or not isinstance(symbol, str):
            raise ValueError(f"Invalid symbol: {symbol}")

        # Basic validation - could be expanded for more specific rules
        if len(symbol) < 5:  # Minimum valid symbol length (e.g., "BTCUSDT")
            raise ValueError(f"Symbol too short: {symbol}")

        # Add market-specific validation if needed
        if market_type == "FUTURES" and not (
            symbol.endswith("USDT") or symbol.endswith("BUSD")
        ):
            logger.warning(f"Unusual futures symbol format: {symbol}")

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
            consolidation_delay: Required delay after day completion for data availability

        Raises:
            ValueError: If data is definitely not available for the time range
        """
        if not DataValidation.is_data_likely_available(start_time, consolidation_delay):
            raise ValueError(
                f"Data for {start_time.date()} is not yet available. "
                f"Binance Vision requires {consolidation_delay} after day completion."
            )
        if end_time.date() > datetime.now(timezone.utc).date():
            raise ValueError(f"Cannot request future data: {end_time.date()}")

    @staticmethod
    def is_data_likely_available(
        target_date: datetime, consolidation_delay: timedelta = timedelta(hours=48)
    ) -> bool:
        """Check if data is likely to be available from Binance Vision.

        Args:
            target_date: Date to check for data availability
            consolidation_delay: Required delay after day completion for data availability

        Returns:
            True if data is likely available based on Binance Vision's constraints
        """
        now = datetime.now(timezone.utc)

        # Convert target_date to start of day in UTC for consistent comparison
        target_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)

        if target_day.date() < now.date():
            # Past dates are always available
            return True
        elif target_day.date() == now.date():
            # Today's data is only available after consolidation delay
            return now - target_day > consolidation_delay
        else:
            # Future dates are never available
            return False

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
        if data_end_floor < adjusted_end_time_floor:
            raise ValueError(
                f"Data ends earlier than requested: {data_end} < {adjusted_end_time_floor}"
            )

        # Verify data is sorted
        if not df.index.is_monotonic_increasing:
            raise ValueError("Data is not sorted by time")


class DataFrameValidator:
    """Static methods for DataFrame validation."""

    @staticmethod
    def validate_dataframe(df: pd.DataFrame) -> None:
        """Validate DataFrame structure and content.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If DataFrame is invalid
        """
        # Empty DataFrame is allowed but must have correct structure
        if df.empty:
            if not isinstance(df.index, pd.DatetimeIndex):
                raise ValueError("Empty DataFrame must have DatetimeIndex")
            if df.index.name != CANONICAL_INDEX_NAME:
                raise ValueError(
                    f"DataFrame index must be named {CANONICAL_INDEX_NAME}"
                )
            return

        # Check index
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame index must be DatetimeIndex")

        # Check index name
        if df.index.name != CANONICAL_INDEX_NAME:
            raise ValueError(f"DataFrame index must be named {CANONICAL_INDEX_NAME}")

        # Check timezone
        if df.index.tz is None:
            raise ValueError("DataFrame index must be timezone-aware")

        # Convert timezone if needed
        if df.index.tz != DEFAULT_TIMEZONE:
            df.index = df.index.tz_convert(DEFAULT_TIMEZONE)

        # Check for duplicate indices
        if df.index.has_duplicates:
            duplicates = df.index[df.index.duplicated()].tolist()
            if len(duplicates) > 5:
                duplicate_sample = duplicates[:5]
                raise ValueError(
                    f"DataFrame has {len(duplicates)} duplicate indices. "
                    f"First 5: {duplicate_sample}"
                )
            else:
                raise ValueError(f"DataFrame has duplicate indices: {duplicates}")

        # Check for sorted index
        if not df.index.is_monotonic_increasing:
            raise ValueError("DataFrame index is not monotonically increasing")

    @staticmethod
    def format_dataframe(
        df: pd.DataFrame, output_dtypes: Dict[str, str]
    ) -> pd.DataFrame:
        """Format DataFrame to ensure consistent structure.

        Args:
            df: Input DataFrame
            output_dtypes: Dictionary mapping column names to expected data types

        Returns:
            Formatted DataFrame with consistent structure
        """
        logger = logging.getLogger(__name__)
        logger.debug(
            f"Formatting DataFrame with shape: {df.shape if not df.empty else 'empty'}"
        )

        if df.empty:
            # Create empty DataFrame with correct structure
            empty_df = pd.DataFrame(
                columns=list(output_dtypes.keys()),
                index=pd.DatetimeIndex(
                    [], name=CANONICAL_INDEX_NAME, tz=DEFAULT_TIMEZONE
                ),
            )

            # Apply proper data types
            for col, dtype in output_dtypes.items():
                empty_df[col] = empty_df[col].astype(dtype)

            return empty_df

        # Check if we have an open_time column that will remain after indexing
        has_separate_open_time = False
        if "open_time" in df.columns and df.index.name != "open_time":
            has_separate_open_time = True
            # If open_time is not already the index, save a copy before indexing
            original_open_time = df["open_time"].copy()

        # Ensure open_time is the index and in UTC
        if "open_time" in df.columns:
            df.set_index("open_time", inplace=True)

        if df.index.tz is None:
            df.index = df.index.tz_localize(DEFAULT_TIMEZONE)
        elif df.index.tz != DEFAULT_TIMEZONE:
            df.index = df.index.tz_convert(DEFAULT_TIMEZONE)

        # Set correct index name
        df.index.name = CANONICAL_INDEX_NAME

        # Normalize column names for consistency
        column_mapping = {
            "taker_buy_base": "taker_buy_volume",
            "taker_buy_quote": "taker_buy_quote_volume",
        }
        df = df.rename(columns=column_mapping)

        # Ensure correct columns and types
        required_columns = list(output_dtypes.keys())

        # Filter to required columns if they exist
        existing_columns = [col for col in required_columns if col in df.columns]
        df = df[existing_columns]

        # Add any missing columns with default values
        for col in set(required_columns) - set(existing_columns):
            if output_dtypes[col].startswith("float"):
                df[col] = 0.0
            elif output_dtypes[col].startswith("int"):
                df[col] = 0
            else:
                df[col] = None

        # Ensure correct column order
        df = df[required_columns]

        # Set correct dtypes
        for col, dtype in output_dtypes.items():
            df[col] = df[col].astype(dtype)

        # Handle duplicate timestamps by keeping the first occurrence
        if df.index.has_duplicates:
            logger.debug(f"Removing {df.index.duplicated().sum()} duplicate timestamps")
            df = df[~df.index.duplicated(keep="first")]

        # Sort by index if it's not monotonically increasing
        if not df.index.is_monotonic_increasing:
            logger.debug("Sorting DataFrame by index to ensure monotonic order")
            df = df.sort_index()

        # If there was a separate open_time column, restore it and ensure it's sorted
        if has_separate_open_time:
            # Restore original open_time as a column (now sorted)
            df["open_time"] = df.index.copy()
            logger.debug("Restored open_time as a column (now sorted)")

        return df

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
