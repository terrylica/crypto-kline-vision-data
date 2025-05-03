#!/usr/bin/env python
"""Centralized validation utilities for data integrity and constraints."""

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd

from utils.api_boundary_validator import ApiBoundaryValidator
from utils.config import (
    CANONICAL_INDEX_NAME,
    DEFAULT_TIMEZONE,
    MAX_CACHE_AGE,
    MILLISECOND_DIGITS,
    MIN_VALID_FILE_SIZE,
    OUTPUT_DTYPES,
    TIMESTAMP_PRECISION,
)
from utils.logger_setup import logger
from utils.market_constraints import Interval

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
    def validate_dates(
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        relative_to: Optional[datetime] = None,
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
        # Set default reference time
        if relative_to is None:
            relative_to = datetime.now(timezone.utc)
        else:
            relative_to = DataValidation.enforce_utc_timestamp(relative_to)

        # Set default start time
        if start_time is None:
            start_time = relative_to

        # Set default end time
        if end_time is None:
            end_time = start_time + timedelta(days=1)

        # First ensure timezone awareness by normalizing to UTC if needed
        if start_time.tzinfo is None or start_time.tzinfo.utcoffset(start_time) is None:
            raise ValueError(
                f"Start time ({start_time.isoformat()}) must be timezone-aware"
            )

        if end_time.tzinfo is None or end_time.tzinfo.utcoffset(end_time) is None:
            raise ValueError(
                f"End time ({end_time.isoformat()}) must be timezone-aware"
            )

        # Then check time ordering
        if start_time >= end_time:
            raise ValueError(
                f"Start time ({start_time.isoformat()}) must be before end time ({end_time.isoformat()})"
            )

        return start_time, end_time

    @staticmethod
    def validate_time_window(
        start_time: datetime, end_time: datetime
    ) -> tuple[datetime, datetime]:
        """Validate time window for market data and normalize timezones.

        Args:
            start_time: Start time
            end_time: End time

        Returns:
            Tuple of (normalized_start_time, normalized_end_time) with timezone-aware values

        Raises:
            ValueError: If time window exceeds maximum allowed
        """
        # Ensure dates are valid first and normalize timezones
        start_time, end_time = DataValidation.validate_dates(start_time, end_time)

        # REMOVED: validate_time_boundaries - No longer enforcing manual alignment for REST API calls

        return start_time, end_time

    @staticmethod
    def enforce_utc_timestamp(dt: datetime) -> datetime:
        """Ensures datetime object is timezone aware and in UTC.

        This is a foundational utility method used by other validation methods
        to normalize datetime objects. It handles both naive and timezone-aware
        datetime objects, ensuring consistent timezone handling throughout the system.

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
        start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> tuple[Optional[datetime], Optional[datetime]]:
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

        # Skip further validation if either date is None
        if start_time is None or end_time is None:
            return start_time, end_time

        # Validate and normalize dates
        start_time, end_time = DataValidation.validate_dates(start_time, end_time)

        # Check for future dates and get normalized values
        start_time, end_time = DataValidation.validate_future_dates(
            start_time, end_time
        )

        return start_time, end_time

    def validate_api_time_range(
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

        return self.api_boundary_validator.is_valid_time_range_sync(
            start_time, end_time, interval, symbol=symbol
        )

    def get_api_aligned_boundaries(
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

        return self.api_boundary_validator.get_api_boundaries_sync(
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
        start_time: datetime, end_time: datetime, buffer_hours: int = 24
    ) -> tuple[datetime, datetime]:
        """Validate that data is likely to be available for the requested time range.

        Args:
            start_time: Start time of the data
            end_time: End time of the data
            buffer_hours: Number of hours before now that data might not be available

        Returns:
            Tuple of (normalized_start_time, normalized_end_time)

        Raises:
            Warning if data within the buffer period is requested
        """
        # Ensure timezone-aware datetimes
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
        interval: Optional[Union[str, Interval]] = None,
        consolidation_delay: Optional[timedelta] = None,
    ) -> bool:
        """Check if data is likely available for the specified date and interval.

        This function determines if data for a given timestamp is likely to be consolidated
        and available in the API based on:
        1. The current time
        2. The interval size (if provided)
        3. The natural consolidation time needed for data at the interval level

        Args:
            target_date: Date to check data availability for
            interval: Optional interval to use for more precise availability determination
            consolidation_delay: Optional explicit delay override

        Returns:
            True if data is likely available, False otherwise
        """
        # Ensure timezone awareness
        target_date = DataValidation.enforce_utc_timestamp(target_date)
        now = datetime.now(timezone.utc)

        logger.debug(
            f"Checking data availability for target_date={target_date.isoformat()}, interval={interval}, now={now.isoformat()}"
        )

        # If we're in the future already, data is certainly not available
        if target_date > now:
            logger.debug(
                f"Target date {target_date.isoformat()} is in the future - data not available"
            )
            return False

        # If an explicit delay was provided, use it
        if consolidation_delay is not None:
            consolidation_threshold = now - consolidation_delay
            is_available = target_date <= consolidation_threshold
            logger.debug(
                f"Using explicit consolidation_delay={consolidation_delay}, threshold={consolidation_threshold.isoformat()}, is_available={is_available}"
            )
            return is_available

        # If interval is provided, we can be more precise about consolidation times
        if interval is not None:
            # Parse string interval if needed
            if isinstance(interval, str):
                try:
                    from utils.market_constraints import Interval

                    logger.debug(
                        f"Converting string interval '{interval}' to Interval enum"
                    )
                    interval = Interval(interval)
                except (ValueError, ImportError) as e:
                    # If we can't parse it, fall back to default delay
                    logger.debug(
                        f"Could not parse interval '{interval}' due to {type(e).__name__}: {e!s}, using default delay"
                    )
                    consolidation_delay = timedelta(minutes=5)
            else:
                # For real intervals, use interval-specific delays
                # Import here to avoid circular imports
                try:
                    from utils.time_utils import (
                        align_time_boundaries,
                        get_interval_seconds,
                    )

                    # Get interval in seconds
                    interval_seconds = get_interval_seconds(interval)
                    logger.debug(f"Interval {interval} is {interval_seconds} seconds")

                    # Align the target date to PREVIOUS interval boundary
                    # This is how align_time_boundaries works: it aligns to the start of the interval
                    aligned_target, _ = align_time_boundaries(
                        target_date, target_date, interval
                    )
                    logger.debug(f"Aligned target date to {aligned_target.isoformat()}")

                    # If the aligned time is after the target date, it means we aligned to the next interval
                    # Adjust to the previous interval in that case
                    if aligned_target > target_date:
                        logger.debug(
                            f"Target date is {target_date.isoformat()}, which is between intervals"
                        )
                        aligned_target = aligned_target - timedelta(
                            seconds=interval_seconds
                        )
                        logger.debug(
                            f"Adjusted to previous interval: {aligned_target.isoformat()}"
                        )
                    else:
                        logger.debug(
                            f"Target date is {target_date.isoformat()}, which is exactly at interval boundary"
                        )

                    # Special case: if target_date is very close to the current time AND at interval boundary,
                    # the data for that interval is likely not consolidated yet
                    time_since_target = now - target_date
                    seconds_since_target = time_since_target.total_seconds()
                    logger.debug(
                        f"Time since target: {seconds_since_target:.2f} seconds"
                    )

                    # We only need a small buffer after the aligned target time
                    buffer_seconds = max(30, interval_seconds * 0.2)
                    consolidation_buffer = timedelta(seconds=buffer_seconds)
                    logger.debug(
                        f"Using consolidation buffer of {buffer_seconds} seconds"
                    )

                    # If aligned time plus buffer is in the past, data should be available
                    is_available = (aligned_target + consolidation_buffer) <= now
                    logger.debug(
                        f"Threshold time is {(aligned_target + consolidation_buffer).isoformat()}, is_available={is_available}"
                    )

                    # Special case: if we're extremely close to a new interval starting (within a few seconds),
                    # the data for the previous interval might not be fully consolidated yet
                    if is_available and seconds_since_target < buffer_seconds:
                        logger.debug(
                            f"Very recent target date ({seconds_since_target:.2f}s ago), treating as potentially not consolidated"
                        )
                        is_available = False

                    # Add metadata to target_date if it's a datetime object
                    if hasattr(target_date, "__dict__"):
                        # Store buffer information for more detailed warning messages
                        target_date.metadata = {
                            "consolidation_buffer_seconds": buffer_seconds,
                            "seconds_since_target": seconds_since_target,
                        }

                    return is_available
                except ImportError as e:
                    # Fall back to default if imports fail
                    logger.debug(f"Import error in interval calculation: {e!s}")
                    logger.warning("Could not import time utils, using default delay")
                    consolidation_delay = timedelta(minutes=5)

        # Default fallback - use a reasonable 5-minute delay
        # This is much less conservative than the previous 48 hours
        if consolidation_delay is None:
            consolidation_delay = timedelta(minutes=5)
            logger.debug(f"Using default consolidation_delay={consolidation_delay}")

        consolidation_threshold = now - consolidation_delay
        is_available = target_date <= consolidation_threshold
        logger.debug(
            f"Default check: threshold={consolidation_threshold.isoformat()}, is_available={is_available}"
        )
        return is_available

    @staticmethod
    def validate_future_dates(
        start_time: datetime, end_time: datetime
    ) -> tuple[datetime, datetime]:
        """Validate that dates are not in the future and normalize to UTC.

        Args:
            start_time: Start time to validate
            end_time: End time to validate

        Returns:
            Tuple of (normalized_start_time, normalized_end_time)

        Raises:
            ValueError: If either start or end time is in the future
        """
        # Ensure dates are normalized to UTC
        start_time = DataValidation.enforce_utc_timestamp(start_time)
        end_time = DataValidation.enforce_utc_timestamp(end_time)

        # Get current time in UTC
        now = datetime.now(timezone.utc)

        # Check for future dates
        if start_time > now:
            raise ValueError(
                f"Start time ({start_time.isoformat()}) cannot be in the future (current time: {now.isoformat()})"
            )
        if end_time > now:
            raise ValueError(
                f"End time ({end_time.isoformat()}) cannot be in the future (current time: {now.isoformat()})"
            )

        return start_time, end_time

    @staticmethod
    def validate_query_time_boundaries(
        start_time: datetime,
        end_time: datetime,
        max_future_seconds: int = 0,
        reference_time: Optional[datetime] = None,
        handle_future_dates: str = "error",
        interval: Optional[Union[str, Interval]] = None,
    ) -> Tuple[datetime, datetime, Dict[str, Any]]:
        """Comprehensive validation of query time boundaries.

        Args:
            start_time: Start time for the query
            end_time: End time for the query
            max_future_seconds: Maximum number of seconds allowed in the future (default: 0)
            reference_time: Reference time to use for future date checks (default: now)
            handle_future_dates: How to handle future dates ("error", "truncate", "allow")
            interval: Optional interval for data availability checks

        Returns:
            Tuple of (start_time, end_time, metadata) where metadata includes:
            - warnings: List of warning messages
            - is_truncated: Whether dates were truncated due to future dates
            - original_start: Original start_time before normalization
            - original_end: Original end_time before normalization

        Raises:
            ValueError: If validation fails based on the specified handling mode
        """
        metadata = {
            "warnings": [],
            "is_truncated": False,
            "original_start": start_time,
            "original_end": end_time,
        }

        # Ensure timezone awareness and normalize to UTC
        start_time = DataValidation.enforce_utc_timestamp(start_time)
        end_time = DataValidation.enforce_utc_timestamp(end_time)

        # Set reference time if not provided
        if reference_time is None:
            reference_time = datetime.now(timezone.utc)
        else:
            reference_time = DataValidation.enforce_utc_timestamp(reference_time)

        metadata["reference_time"] = reference_time

        # Validate sequential ordering (start_time < end_time)
        if start_time >= end_time:
            raise ValueError(
                f"Start time ({start_time.isoformat()}) must be before end time ({end_time.isoformat()})"
            )

        # Validate against future dates
        allowed_future = reference_time + timedelta(seconds=max_future_seconds)

        # Handle start time in future
        if start_time > allowed_future:
            message = f"Start time ({start_time.isoformat()}) is in the future (current time: {reference_time.isoformat()})"
            if handle_future_dates == "error":
                raise ValueError(message)
            if handle_future_dates == "truncate":
                metadata["warnings"].append(message + " - truncated to current time")
                metadata["is_truncated"] = True
                start_time = reference_time
            elif handle_future_dates == "allow":
                metadata["warnings"].append(
                    message + " - allowed but may return empty results"
                )
            else:
                raise ValueError(
                    f"Invalid handle_future_dates value: {handle_future_dates}"
                )

        # Handle end time in future
        if end_time > allowed_future:
            message = f"End time ({end_time.isoformat()}) is in the future (current time: {reference_time.isoformat()})"
            if handle_future_dates == "error":
                raise ValueError(message)
            if handle_future_dates == "truncate":
                metadata["warnings"].append(message + " - truncated to current time")
                metadata["is_truncated"] = True
                end_time = reference_time
            elif handle_future_dates == "allow":
                metadata["warnings"].append(
                    message + " - allowed but may return empty results"
                )
            else:
                raise ValueError(
                    f"Invalid handle_future_dates value: {handle_future_dates}"
                )

        # Re-validate sequential ordering after potential truncation
        if start_time >= end_time:
            raise ValueError(
                f"After truncation, start time ({start_time.isoformat()}) must still be before end time ({end_time.isoformat()})"
            )

        # Add data availability info but don't warn yet - let caller decide
        logger.debug(
            f"Checking data availability for end_time={end_time.isoformat()} with interval={interval}"
        )
        is_available = DataValidation.is_data_likely_available(end_time, interval)
        logger.debug(
            f"Data availability result for end_time={end_time.isoformat()}: {is_available}"
        )

        # Copy any metadata from end_time to our metadata
        if hasattr(end_time, "metadata") and isinstance(end_time.metadata, dict):
            # Only copy consolidation related metadata
            for key in ["consolidation_buffer_seconds", "seconds_since_target"]:
                if key in end_time.metadata:
                    metadata[key] = end_time.metadata[key]

        # Instead of immediately adding to warnings, add flag to metadata
        metadata["data_likely_available"] = is_available

        if is_available is False:
            # Add more details to the warning message
            seconds_since_target = (reference_time - end_time).total_seconds()
            buffer = metadata.get(
                "consolidation_buffer_seconds", 30
            )  # Default to 30 seconds if not set

            metadata["data_availability_message"] = (
                f"Data for end time ({end_time.isoformat()}) may not be fully consolidated yet. "
                f"Time since target: {seconds_since_target:.1f}s, buffer needed: {buffer:.1f}s"
            )

            # Store the time range for reference in the logs
            metadata["time_range"] = {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "time_span_seconds": (end_time - start_time).total_seconds(),
            }
        else:
            metadata["data_availability_message"] = ""

        return start_time, end_time, metadata

    @staticmethod
    def validate_date_range_for_api(
        start_time: datetime, end_time: datetime, max_future_seconds: int = 0
    ) -> Tuple[bool, str]:
        """Validate a date range for API requests to prevent requesting future data.

        Args:
            start_time: The start time for the request
            end_time: The end time for the request
            max_future_seconds: Maximum number of seconds allowed in the future (default: 0)

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            _, _, metadata = DataValidation.validate_query_time_boundaries(
                start_time, end_time, max_future_seconds, handle_future_dates="error"
            )
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
        expected_checksum: str = None,
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
        # Check basic integrity first
        integrity_result = DataFrameValidator.validate_cache_integrity(
            file_path, min_size, max_age
        )
        if integrity_result is not None:
            # Failed basic validation
            return False

        # If checksum validation is requested, perform it
        if expected_checksum:
            try:
                actual_checksum = DataValidation.calculate_checksum(file_path)
                return actual_checksum == expected_checksum
            except (IOError, OSError) as e:
                logger.error(f"Error calculating checksum for {file_path}: {e}")
                return False

        # If no checksum validation requested or it passed
        return True

    @staticmethod
    def validate_dataframe_time_boundaries(
        df: pd.DataFrame, start_time: datetime, end_time: datetime
    ) -> None:
        """Validate that DataFrame covers the requested time range.

        Args:
            df: DataFrame to validate
            start_time: Start time boundary
            end_time: End time boundary

        Raises:
            ValueError: If DataFrame doesn't cover the time range
        """
        if df.empty:
            return  # Empty DataFrame cannot be validated against time boundaries

        # Ensure timezone-aware datetimes
        start_time = DataValidation.enforce_utc_timestamp(start_time)
        end_time = DataValidation.enforce_utc_timestamp(end_time)

        # Get actual time range covered
        actual_start = df.index.min()
        actual_end = df.index.max()

        # Check time boundaries (with small tolerance for floating point precision)
        if actual_start > start_time + timedelta(microseconds=1000):
            raise ValueError(
                f"DataFrame starts at {actual_start}, which is after the requested start time {start_time}"
            )

        if actual_end < end_time - timedelta(microseconds=1000):
            raise ValueError(
                f"DataFrame ends at {actual_end}, which is before the requested end time {end_time}"
            )


class DataFrameValidator:
    """Validation and standardization for DataFrames."""

    def __init__(self, df: pd.DataFrame = None):
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

        # Check if index is DatetimeIndex
        logger.debug(f"Checking index type: {type(df.index).__name__}")
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError(
                f"DataFrame index must be DatetimeIndex, got {type(df.index).__name__}"
            )

        # Check if index is timezone-aware
        logger.debug("Checking if index is timezone-aware")
        if df.index.tz is None:
            raise ValueError("DataFrame index must be timezone-aware")

        # Log timezone information for debugging
        logger.debug(f"DataFrame index timezone: {df.index.tz}")
        logger.debug(f"timezone.utc: {timezone.utc}")
        logger.debug(f"DEFAULT_TIMEZONE: {DEFAULT_TIMEZONE}")
        logger.debug(f"Are they equal? {df.index.tz == timezone.utc}")
        logger.debug(f"Are they the same object? {df.index.tz is timezone.utc}")

        # Additional logging to track what happens after timezone validation
        logger.debug("Continuing validation after timezone checks...")

        # Check if index is named correctly
        logger.debug(
            f"Checking index name: {df.index.name} vs expected: {CANONICAL_INDEX_NAME}"
        )
        if df.index.name != CANONICAL_INDEX_NAME:
            raise ValueError(
                f"DataFrame index must be named '{CANONICAL_INDEX_NAME}', "
                f"got '{df.index.name}'"
            )

        # Check for duplicate indices
        logger.debug(f"Checking for duplicate indices in DataFrame with {len(df)} rows")
        if df.index.has_duplicates:
            raise ValueError("DataFrame index contains duplicate timestamps")

        # Check if index is sorted
        logger.debug("Checking if index is monotonically increasing")
        if not df.index.is_monotonic_increasing:
            raise ValueError("DataFrame index must be monotonically increasing")

        # Check for required columns
        required_columns = ["open", "high", "low", "close", "volume"]
        logger.debug(f"Checking for required columns: {required_columns}")
        logger.debug(f"Available columns: {df.columns.tolist()}")
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"DataFrame missing required columns: {missing_columns}")

        logger.debug("DataFrame validation completed successfully")
        logger.debug("==== END OF DATAFRAME VALIDATION FUNCTION ====")

    def validate_klines_data(self) -> Tuple[bool, Optional[str]]:
        """Validate that a DataFrame contains valid klines market data.

        This method ensures:
        1. The DataFrame has the expected structure for klines data
        2. Timestamps are standardized to millisecond precision (matching REST API)
        3. Required columns are present with correct data types

        Returns:
            Tuple of (is_valid, error_message)
        """
        if self.df is None:
            return False, "No DataFrame provided for validation"

        if self.df.empty:
            logger.debug("Empty DataFrame passed validation")
            return True, None

        try:
            # Basic structure validation
            self.validate_dataframe(self.df)

            # Check timestamp precision - convert to millisecond precision if needed
            # This ensures alignment with REST API format (which is our standard)
            if TIMESTAMP_PRECISION == "ms" and hasattr(self.df.index, "astype"):
                # If timestamps have microsecond precision (from Vision API 2025+ data)
                # we need to truncate to millisecond precision
                sample_ts = self.df.index[0].value
                if (
                    len(str(abs(sample_ts))) > MILLISECOND_DIGITS
                ):  # More than millisecond precision
                    logger.debug(
                        "Converting timestamps from microsecond to millisecond precision"
                    )

                    # For datetime index - round to milliseconds
                    if isinstance(self.df.index, pd.DatetimeIndex):
                        # Round to millisecond precision
                        rounded_index = pd.DatetimeIndex(
                            [
                                pd.Timestamp(
                                    ts.timestamp() * 1000, unit="ms", tz=timezone.utc
                                )
                                for ts in self.df.index
                            ],
                            name=self.df.index.name,
                        )
                        self.df.index = rounded_index

                    # Also handle open_time and close_time columns if present
                    if (
                        "open_time" in self.df.columns
                        and pd.api.types.is_datetime64_dtype(self.df["open_time"])
                    ):
                        self.df["open_time"] = pd.to_datetime(
                            (self.df["open_time"].astype(int) // 1000000) * 1000,
                            unit="ms",
                            utc=True,
                        )

                    if (
                        "close_time" in self.df.columns
                        and pd.api.types.is_datetime64_dtype(self.df["close_time"])
                    ):
                        self.df["close_time"] = pd.to_datetime(
                            (self.df["close_time"].astype(int) // 1000000) * 1000,
                            unit="ms",
                            utc=True,
                        )

            # Verify required numeric columns have proper data types
            for col, dtype in OUTPUT_DTYPES.items():
                if (
                    col in self.df.columns
                    and not pd.api.types.is_numeric_dtype(self.df[col])
                    and "time" not in col
                ):
                    logger.warning(
                        f"Column {col} has non-numeric dtype: {self.df[col].dtype}"
                    )
                    try:
                        self.df[col] = self.df[col].astype(dtype)
                    except Exception as e:
                        return (
                            False,
                            f"Failed to convert column {col} to {dtype}: {e!s}",
                        )

            # Check for NaN values in critical columns
            critical_columns = ["open", "high", "low", "close"]
            for col in critical_columns:
                if col in self.df.columns and self.df[col].isna().any():
                    return False, f"Found NaN values in critical column: {col}"

            # All validation passed
            return True, None

        except Exception as e:
            logger.error(f"Error validating klines data: {e!s}")
            return False, str(e)

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
            logger.debug("==== END OF FORMAT_DATAFRAME (EMPTY DF) ====")
            return empty_df

        # Copy to avoid modifying original
        logger.debug(f"Copying DataFrame with shape {df.shape}")
        formatted_df = df.copy()
        logger.debug(f"Copy created with shape {formatted_df.shape}")

        if (
            isinstance(formatted_df.index, pd.DatetimeIndex)
            and formatted_df.index.tz is not None
        ):
            logger.debug(
                f"Input DataFrame timezone before processing: {formatted_df.index.tz}"
            )
            logger.debug(f"Is timezone.utc? {formatted_df.index.tz is timezone.utc}")

        # Ensure index is DatetimeIndex in UTC
        logger.debug(f"Index type check: {type(formatted_df.index).__name__}")
        if not isinstance(formatted_df.index, pd.DatetimeIndex):
            logger.debug("Converting non-DatetimeIndex to DatetimeIndex")
            if "open_time" in formatted_df.columns:
                logger.debug("Using open_time column for index")
                formatted_df = formatted_df.set_index("open_time")
            else:
                logger.error("Cannot find open_time column for index conversion")
                raise ValueError(
                    "DataFrame must have 'open_time' column or DatetimeIndex"
                )

        # Ensure index is named correctly
        logger.debug(f"Setting index name to {CANONICAL_INDEX_NAME}")
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
        logger.debug(f"Final DataFrame shape: {formatted_df.shape}")
        logger.debug("==== END OF FORMAT_DATAFRAME FUNCTION ====")
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
        file_size = file_path.stat().st_size
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
