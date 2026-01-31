#!/usr/bin/env python
"""Time and date validation utilities for market data operations.

This module provides validation for:
- Date range validation and normalization
- Future date handling
- Time boundary alignment
- Data availability checks
- Symbol and interval validation
"""

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from data_source_manager.utils.api_boundary_validator import ApiBoundaryValidator
from data_source_manager.utils.config import (
    MAX_CACHE_AGE,
    MIN_VALID_FILE_SIZE,
)
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import Interval

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
INTERVAL_PATTERN = re.compile(r"^(1s|1m|3m|5m|15m|30m|1h|2h|4h|6h|8h|12h|1d|3d|1w|1M)$")


class ValidationError(Exception):
    """Custom exception for validation errors."""


class DataValidation:
    """Centralized data validation utilities for time, dates, and symbols."""

    def __init__(self, api_boundary_validator: ApiBoundaryValidator | None = None):
        """Initialize the DataValidation class.

        Args:
            api_boundary_validator: Optional ApiBoundaryValidator instance for API boundary validations
        """
        self.api_boundary_validator = api_boundary_validator

    @staticmethod
    def validate_dates(
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        relative_to: datetime | None = None,
    ) -> tuple[datetime, datetime]:
        """Validate date inputs and normalize timezone information.

        Args:
            start_time: Start time (default: now)
            end_time: End time (default: start_time + 1 day)
            relative_to: Reference time for relative dates (default: now)

        Returns:
            Tuple of (normalized_start_time, normalized_end_time) with timezone-aware values

        Raises:
            ValueError: If start_time is after end_time
        """
        relative_to = datetime.now(timezone.utc) if relative_to is None else DataValidation.enforce_utc_timestamp(relative_to)

        if start_time is None:
            start_time = relative_to

        if end_time is None:
            end_time = start_time + timedelta(days=1)

        if start_time.tzinfo is None or start_time.tzinfo.utcoffset(start_time) is None:
            raise ValueError(f"Start time ({start_time.isoformat()}) must be timezone-aware")

        if end_time.tzinfo is None or end_time.tzinfo.utcoffset(end_time) is None:
            raise ValueError(f"End time ({end_time.isoformat()}) must be timezone-aware")

        if start_time >= end_time:
            raise ValueError(f"Start time ({start_time.isoformat()}) must be before end time ({end_time.isoformat()})")

        return start_time, end_time

    @staticmethod
    def validate_time_window(start_time: datetime, end_time: datetime) -> tuple[datetime, datetime]:
        """Validate time window for market data and normalize timezones.

        Args:
            start_time: Start time
            end_time: End time

        Returns:
            Tuple of (normalized_start_time, normalized_end_time) with timezone-aware values

        Raises:
            ValueError: If time window exceeds maximum allowed
        """
        start_time, end_time = DataValidation.validate_dates(start_time, end_time)
        return start_time, end_time

    @staticmethod
    def enforce_utc_timestamp(dt: datetime) -> datetime:
        """Ensures datetime object is timezone aware and in UTC.

        Args:
            dt: Input datetime, can be naive or timezone-aware

        Returns:
            UTC timezone-aware datetime
        """
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def validate_time_range(
        start_time: datetime | None = None, end_time: datetime | None = None
    ) -> tuple[datetime | None, datetime | None]:
        """Validate and normalize time range parameters.

        Args:
            start_time: Start time of data request
            end_time: End time of data request

        Returns:
            Tuple of (normalized start_time, normalized end_time)

        Raises:
            ValueError: If end time is not after start time or dates are invalid
        """
        if start_time is not None:
            start_time = DataValidation.enforce_utc_timestamp(start_time)
        if end_time is not None:
            end_time = DataValidation.enforce_utc_timestamp(end_time)

        if start_time is None or end_time is None:
            return start_time, end_time

        start_time, end_time = DataValidation.validate_dates(start_time, end_time)
        start_time, end_time = DataValidation.validate_future_dates(start_time, end_time)

        return start_time, end_time

    def validate_api_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: str | Interval,
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
            raise ValueError("ApiBoundaryValidator is required for API time range validation")

        if isinstance(interval, str):
            interval = Interval(interval)

        return self.api_boundary_validator.is_valid_time_range_sync(start_time, end_time, interval, symbol=symbol)

    def get_api_aligned_boundaries(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: str | Interval,
        symbol: str = "BTCUSDT",
    ) -> dict[str, Any]:
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
            raise ValueError("ApiBoundaryValidator is required for API boundary alignment")

        if isinstance(interval, str):
            interval = Interval(interval)

        return self.api_boundary_validator.get_api_boundaries_sync(start_time, end_time, interval, symbol=symbol)

    @staticmethod
    def validate_interval(interval: str, market_type: str = "SPOT") -> None:
        """Validate interval string format.

        Args:
            interval: Time interval string (e.g., '1s', '1m')
            market_type: Market type for context-specific validation

        Raises:
            ValueError: If interval format is invalid
        """
        supported_intervals = {
            "SPOT": ["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"],
            "FUTURES": ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"],
        }

        market = market_type.upper()
        if market not in supported_intervals:
            market = "SPOT"

        if interval not in supported_intervals[market]:
            raise ValueError(f"Invalid interval: {interval}. Supported intervals for {market}: {supported_intervals[market]}")

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
            raise ValueError(f"Invalid {market_type} symbol format: Symbol must be a non-empty string.")
        if not symbol.isupper():
            raise ValueError(f"Invalid {market_type} symbol format: {symbol}. Symbols should be uppercase (e.g., BTCUSDT).")

    @staticmethod
    def validate_data_availability(start_time: datetime, end_time: datetime, buffer_hours: int = 24) -> tuple[datetime, datetime]:
        """Validate that data is likely to be available for the requested time range.

        Args:
            start_time: Start time of the data
            end_time: End time of the data
            buffer_hours: Number of hours before now that data might not be available

        Returns:
            Tuple of (normalized_start_time, normalized_end_time)
        """
        start_time = DataValidation.enforce_utc_timestamp(start_time)
        end_time = DataValidation.enforce_utc_timestamp(end_time)

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=buffer_hours)

        if end_time > cutoff:
            logger.warning(
                f"Requested data includes recent time ({end_time}) that may not be fully consolidated. "
                f"Data is typically available with a {buffer_hours} hour delay."
            )

        return start_time, end_time

    @staticmethod
    def is_data_likely_available(
        target_date: datetime,
        interval: str | Interval | None = None,
        consolidation_delay: timedelta | None = None,
    ) -> bool:
        """Check if data is likely available for the specified date and interval.

        Args:
            target_date: Date to check data availability for
            interval: Optional interval to use for more precise availability determination
            consolidation_delay: Optional explicit delay override

        Returns:
            True if data is likely available, False otherwise
        """
        target_date = DataValidation.enforce_utc_timestamp(target_date)
        now = datetime.now(timezone.utc)

        logger.debug(f"Checking data availability for target_date={target_date.isoformat()}, interval={interval}, now={now.isoformat()}")

        if target_date > now:
            logger.debug(f"Target date {target_date.isoformat()} is in the future - data not available")
            return False

        if consolidation_delay is not None:
            consolidation_threshold = now - consolidation_delay
            is_available = target_date <= consolidation_threshold
            logger.debug(
                f"Using explicit consolidation_delay={consolidation_delay}, "
                f"threshold={consolidation_threshold.isoformat()}, is_available={is_available}"
            )
            return is_available

        if interval is not None:
            if isinstance(interval, str):
                try:
                    logger.debug(f"Converting string interval '{interval}' to Interval enum")
                    interval = Interval(interval)
                except (ValueError, ImportError) as e:
                    logger.debug(f"Could not parse interval '{interval}' due to {type(e).__name__}: {e!s}, using default delay")
                    consolidation_delay = timedelta(minutes=5)
            else:
                try:
                    from data_source_manager.utils.time_utils import (
                        align_time_boundaries,
                        get_interval_seconds,
                    )

                    interval_seconds = get_interval_seconds(interval)
                    logger.debug(f"Interval {interval} is {interval_seconds} seconds")

                    aligned_target, _ = align_time_boundaries(target_date, target_date, interval)
                    logger.debug(f"Aligned target date to {aligned_target.isoformat()}")

                    if aligned_target > target_date:
                        logger.debug(f"Target date is {target_date.isoformat()}, which is between intervals")
                        aligned_target = aligned_target - timedelta(seconds=interval_seconds)
                        logger.debug(f"Adjusted to previous interval: {aligned_target.isoformat()}")

                    time_since_target = now - target_date
                    seconds_since_target = time_since_target.total_seconds()
                    logger.debug(f"Time since target: {seconds_since_target:.2f} seconds")

                    buffer_seconds = max(30, interval_seconds * 0.2)
                    consolidation_buffer = timedelta(seconds=buffer_seconds)
                    logger.debug(f"Using consolidation buffer of {buffer_seconds} seconds")

                    is_available = (aligned_target + consolidation_buffer) <= now
                    logger.debug(f"Threshold time is {(aligned_target + consolidation_buffer).isoformat()}, is_available={is_available}")

                    if is_available and seconds_since_target < buffer_seconds:
                        logger.debug(f"Very recent target date ({seconds_since_target:.2f}s ago), treating as potentially not consolidated")
                        is_available = False

                    return is_available
                except ImportError as e:
                    logger.debug(f"Import error in interval calculation: {e!s}")
                    logger.warning("Could not import time utils, using default delay")
                    consolidation_delay = timedelta(minutes=5)

        if consolidation_delay is None:
            consolidation_delay = timedelta(minutes=5)
            logger.debug(f"Using default consolidation_delay={consolidation_delay}")

        consolidation_threshold = now - consolidation_delay
        is_available = target_date <= consolidation_threshold
        logger.debug(f"Default check: threshold={consolidation_threshold.isoformat()}, is_available={is_available}")
        return is_available

    @staticmethod
    def validate_future_dates(start_time: datetime, end_time: datetime) -> tuple[datetime, datetime]:
        """Validate that dates are not in the future and normalize to UTC.

        Args:
            start_time: Start time to validate
            end_time: End time to validate

        Returns:
            Tuple of (normalized_start_time, normalized_end_time)

        Raises:
            ValueError: If either start or end time is in the future
        """
        start_time = DataValidation.enforce_utc_timestamp(start_time)
        end_time = DataValidation.enforce_utc_timestamp(end_time)

        now = datetime.now(timezone.utc)

        if start_time > now:
            raise ValueError(f"Start time ({start_time.isoformat()}) cannot be in the future (current time: {now.isoformat()})")
        if end_time > now:
            raise ValueError(f"End time ({end_time.isoformat()}) cannot be in the future (current time: {now.isoformat()})")

        return start_time, end_time

    @staticmethod
    def validate_query_time_boundaries(
        start_time: datetime,
        end_time: datetime,
        max_future_seconds: int = 0,
        reference_time: datetime | None = None,
        handle_future_dates: str = "error",
        interval: str | Interval | None = None,
    ) -> tuple[datetime, datetime, dict[str, Any]]:
        """Comprehensive validation of query time boundaries.

        Args:
            start_time: Start time for the query
            end_time: End time for the query
            max_future_seconds: Maximum number of seconds allowed in the future (default: 0)
            reference_time: Reference time to use for future date checks (default: now)
            handle_future_dates: How to handle future dates ("error", "truncate", "allow")
            interval: Optional interval for data availability checks

        Returns:
            Tuple of (start_time, end_time, metadata)

        Raises:
            ValueError: If validation fails based on the specified handling mode
        """
        metadata: dict[str, Any] = {
            "warnings": [],
            "is_truncated": False,
            "original_start": start_time,
            "original_end": end_time,
        }

        start_time = DataValidation.enforce_utc_timestamp(start_time)
        end_time = DataValidation.enforce_utc_timestamp(end_time)

        reference_time = datetime.now(timezone.utc) if reference_time is None else DataValidation.enforce_utc_timestamp(reference_time)
        metadata["reference_time"] = reference_time

        if start_time >= end_time:
            raise ValueError(f"Start time ({start_time.isoformat()}) must be before end time ({end_time.isoformat()})")

        allowed_future = reference_time + timedelta(seconds=max_future_seconds)

        if start_time > allowed_future:
            message = f"Start time ({start_time.isoformat()}) is in the future (current time: {reference_time.isoformat()})"
            if handle_future_dates == "error":
                raise ValueError(message)
            if handle_future_dates == "truncate":
                metadata["warnings"].append(message + " - truncated to current time")
                metadata["is_truncated"] = True
                start_time = reference_time
            elif handle_future_dates == "allow":
                metadata["warnings"].append(message + " - allowed but may return empty results")
            else:
                raise ValueError(f"Invalid handle_future_dates value: {handle_future_dates}")

        if end_time > allowed_future:
            message = f"End time ({end_time.isoformat()}) is in the future (current time: {reference_time.isoformat()})"
            if handle_future_dates == "error":
                raise ValueError(message)
            if handle_future_dates == "truncate":
                metadata["warnings"].append(message + " - truncated to current time")
                metadata["is_truncated"] = True
                end_time = reference_time
            elif handle_future_dates == "allow":
                metadata["warnings"].append(message + " - allowed but may return empty results")
            else:
                raise ValueError(f"Invalid handle_future_dates value: {handle_future_dates}")

        if start_time >= end_time:
            raise ValueError(
                f"After truncation, start time ({start_time.isoformat()}) must still be before end time ({end_time.isoformat()})"
            )

        logger.debug(f"Checking data availability for end_time={end_time.isoformat()} with interval={interval}")
        is_available = DataValidation.is_data_likely_available(end_time, interval)
        logger.debug(f"Data availability result for end_time={end_time.isoformat()}: {is_available}")

        metadata["data_likely_available"] = is_available

        if is_available is False:
            seconds_since_target = (reference_time - end_time).total_seconds()
            buffer = metadata.get("consolidation_buffer_seconds", 30)

            metadata["data_availability_message"] = (
                f"Data for end time ({end_time.isoformat()}) may not be fully consolidated yet. "
                f"Time since target: {seconds_since_target:.1f}s, buffer needed: {buffer:.1f}s"
            )

            metadata["time_range"] = {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "time_span_seconds": (end_time - start_time).total_seconds(),
            }
        else:
            metadata["data_availability_message"] = ""

        return start_time, end_time, metadata

    @staticmethod
    def validate_date_range_for_api(start_time: datetime, end_time: datetime, max_future_seconds: int = 0) -> tuple[bool, str]:
        """Validate a date range for API requests to prevent requesting future data.

        Args:
            start_time: The start time for the request
            end_time: The end time for the request
            max_future_seconds: Maximum number of seconds allowed in the future (default: 0)

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            DataValidation.validate_query_time_boundaries(start_time, end_time, max_future_seconds, handle_future_dates="error")
            return True, ""
        except ValueError as e:
            return False, str(e)

    @staticmethod
    def calculate_checksum(file_path: Path) -> str:
        """Calculate SHA-256 checksum of a file.

        Args:
            file_path: Path to the file

        Returns:
            Hexadecimal string of the SHA-256 checksum
        """
        import hashlib

        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()

    @staticmethod
    def validate_file_with_checksum(
        file_path: Path,
        expected_checksum: str | None = None,
        min_size: int = MIN_VALID_FILE_SIZE,
        max_age: timedelta = MAX_CACHE_AGE,
    ) -> bool:
        """Validate file integrity with optional checksum verification.

        Args:
            file_path: Path to the file
            expected_checksum: Expected checksum to validate against
            min_size: Minimum valid file size in bytes
            max_age: Maximum valid file age

        Returns:
            True if file passes all integrity checks, False otherwise
        """
        from data_source_manager.utils.validation.dataframe_validation import DataFrameValidator

        integrity_result = DataFrameValidator.validate_cache_integrity(file_path, min_size, max_age)
        if integrity_result is not None:
            return False

        if expected_checksum:
            try:
                actual_checksum = DataValidation.calculate_checksum(file_path)
                return actual_checksum == expected_checksum
            except OSError as e:
                logger.error(f"Error calculating checksum for {file_path}: {e}")
                return False

        return True
