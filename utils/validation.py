#!/usr/bin/env python
from utils.logger_setup import get_logger

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)

from datetime import datetime, timezone, timedelta
import pandas as pd
from typing import Optional, Dict, Any
from .market_constraints import (
    MarketType,
    Interval,
    get_market_capabilities,
    is_interval_supported,
    get_endpoint_url,
    get_minimum_interval,
)
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
    """Static methods for data validation."""

    @staticmethod
    def validate_dates(start_time: datetime, end_time: datetime) -> None:
        """Validate date parameters.

        Args:
            start_time: Start time
            end_time: End time

        Raises:
            ValueError: If dates are invalid
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
        """Unified method for validating time windows.

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
            symbol: Trading pair symbol
            market_type: Market type

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
            logger.warning(f"DataFrame has {len(duplicates)} duplicate timestamps")

        # Check index order
        if not df.index.is_monotonic_increasing:
            raise ValueError("DataFrame index must be monotonically increasing")

        # Check required columns based on OUTPUT_DTYPES
        required_columns = set(OUTPUT_DTYPES.keys())
        missing_columns = required_columns - set(df.columns)

        if missing_columns:
            raise ValueError(f"DataFrame missing required columns: {missing_columns}")

        # Additional validations for data quality
        # These could be made optional or configurable

        # Check for valid price data
        if (df["high"] < df["low"]).any():
            raise ValueError("Invalid price data: high < low")

        if (df["open"] < 0).any() or (df["close"] < 0).any():
            raise ValueError("Invalid price data: negative prices")

        if (df["volume"] < 0).any():
            raise ValueError("Invalid volume data: negative volume")

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
