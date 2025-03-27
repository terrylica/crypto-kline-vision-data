#!/usr/bin/env python
"""Time alignment utilities for Binance API requests.

DEPRECATED: This module is deprecated in favor of utils.time_utils.
It will be removed in a future version.

This module provides utilities for handling time alignment in API requests
and data processing. It includes functions for:

1. Time zone conversion and normalization
2. Validating time windows for API requests
3. Filtering data based on time boundaries
4. Enforcing consistent time formats throughout the application

These utilities ensure consistent data handling across different data sources.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict, Any
import logging
import re
import warnings

import pandas as pd

from utils.market_constraints import Interval
from utils.deprecation_rules import TimeUnit
from utils.time_utils import (
    enforce_utc_timezone as _enforce_utc_timezone,
    get_interval_micros as _get_interval_micros,
    get_interval_timedelta as _get_interval_timedelta,
    get_smaller_units as _get_smaller_units,
    get_interval_floor as _get_interval_floor,
    get_interval_ceiling as _get_interval_ceiling,
    get_bar_close_time as _get_bar_close_time,
    vision_api_time_window_alignment as _vision_api_time_window_alignment,
    is_bar_complete as _is_bar_complete,
    filter_dataframe_by_time as _filter_dataframe_by_time,
    align_vision_api_to_rest as _align_vision_api_to_rest,
    validate_time_window as _validate_time_window,
)

# Configure module logger
logger = logging.getLogger(__name__)

# Deprecation warning message template
DEPRECATION_WARNING = (
    "{} is deprecated and will be removed in a future version. "
    "Use utils.time_utils.{} instead."
)


def enforce_utc_timezone(dt: datetime) -> datetime:
    """Ensure datetime is in UTC timezone.

    DEPRECATED: Use utils.time_utils.enforce_utc_timezone instead.

    Args:
        dt: Input datetime, potentially with or without timezone

    Returns:
        Datetime object guaranteed to have UTC timezone
    """
    warnings.warn(
        DEPRECATION_WARNING.format("enforce_utc_timezone", "enforce_utc_timezone"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _enforce_utc_timezone(dt)


def get_interval_micros(interval: Interval) -> int:
    """Convert interval to microseconds.

    DEPRECATED: Use utils.time_utils.get_interval_micros instead.

    Args:
        interval: The interval specification

    Returns:
        int: Interval duration in microseconds
    """
    warnings.warn(
        DEPRECATION_WARNING.format("get_interval_micros", "get_interval_micros"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _get_interval_micros(interval)


def get_interval_timedelta(interval: Interval) -> timedelta:
    """Convert interval to timedelta.

    DEPRECATED: Use utils.time_utils.get_interval_timedelta instead.

    Args:
        interval: The interval specification

    Returns:
        timedelta: Interval duration
    """
    warnings.warn(
        DEPRECATION_WARNING.format("get_interval_timedelta", "get_interval_timedelta"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _get_interval_timedelta(interval)


def get_smaller_units(interval: Interval) -> List[TimeUnit]:
    """Get all units smaller than this interval.

    DEPRECATED: Use utils.time_utils.get_smaller_units instead.

    Args:
        interval: The interval specification

    Returns:
        List[TimeUnit]: Units smaller than the interval
    """
    warnings.warn(
        DEPRECATION_WARNING.format("get_smaller_units", "get_smaller_units"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _get_smaller_units(interval)


def get_interval_floor(timestamp: datetime, interval: Interval) -> datetime:
    """Floor timestamp to interval boundary, removing all smaller units.

    DEPRECATED: Use utils.time_utils.get_interval_floor instead.

    Args:
        timestamp: The timestamp to floor
        interval: The interval specification

    Returns:
        datetime: Floor time with sub-interval units removed
    """
    warnings.warn(
        DEPRECATION_WARNING.format("get_interval_floor", "get_interval_floor"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _get_interval_floor(timestamp, interval)


def get_interval_ceiling(timestamp: datetime, interval: Interval) -> datetime:
    """Ceil timestamp to next interval boundary.

    DEPRECATED: Use utils.time_utils.get_interval_ceiling instead.

    Args:
        timestamp: The timestamp to ceiling
        interval: The interval specification

    Returns:
        datetime: Ceiling time (next interval with sub-interval units removed)
    """
    warnings.warn(
        DEPRECATION_WARNING.format("get_interval_ceiling", "get_interval_ceiling"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _get_interval_ceiling(timestamp, interval)


def get_bar_close_time(open_time: datetime, interval: Interval) -> datetime:
    """Get the close time for a bar given its open time.

    DEPRECATED: Use utils.time_utils.get_bar_close_time instead.

    Args:
        open_time: The bar's open time
        interval: The interval specification

    Returns:
        datetime: Close time (interval - 1 microsecond after open time)
    """
    warnings.warn(
        DEPRECATION_WARNING.format("get_bar_close_time", "get_bar_close_time"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _get_bar_close_time(open_time, interval)


# DEPRECATED: This function should not be used for REST API alignment
# Only kept for Vision API and cache to match REST API behavior
def _vision_api_time_window_alignment(
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
) -> Tuple[datetime, datetime]:
    """Align time window for Vision API to match REST API behavior.

    DEPRECATED: Use utils.time_utils.vision_api_time_window_alignment instead.

    This is specifically for Vision API and cache alignment to match REST API behavior.
    DO NOT use for REST API calls - pass timestamps directly to REST API.

    Args:
        start_time: Start time
        end_time: End time
        interval: The interval specification

    Returns:
        Tuple of aligned start and end times that match REST API behavior
    """
    warnings.warn(
        DEPRECATION_WARNING.format(
            "_vision_api_time_window_alignment", "vision_api_time_window_alignment"
        ),
        DeprecationWarning,
        stacklevel=2,
    )
    return _vision_api_time_window_alignment(start_time, end_time, interval)


# DEPRECATED: Do not use for REST API - kept for backward compatibility
# and will emit a warning if used
def adjust_time_window(
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
    current_time: Optional[datetime] = None,
) -> Tuple[datetime, datetime]:
    """DEPRECATED: Do not use for REST API calls.

    Use utils.time_utils.vision_api_time_window_alignment for Vision API and cache instead.
    This function will be removed in a future version.

    Args:
        start_time: Start time
        end_time: End time
        interval: The interval specification
        current_time: Optional current time for testing

    Returns:
        Tuple of adjusted start and end times
    """
    logger.warning(
        "adjust_time_window() is deprecated. "
        "For REST API: Do not manually align timestamps. "
        "For Vision API/cache: Use utils.time_utils.vision_api_time_window_alignment()."
    )
    warnings.warn(
        "adjust_time_window() is deprecated and will be removed in a future version. "
        "Use utils.time_utils.vision_api_time_window_alignment() instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _vision_api_time_window_alignment(start_time, end_time, interval)


def is_bar_complete(
    timestamp: datetime, interval: Interval, current_time: Optional[datetime] = None
) -> bool:
    """Check if a bar is complete based on the current time.

    DEPRECATED: Use utils.time_utils.is_bar_complete instead.

    Args:
        timestamp: The bar's timestamp
        interval: The interval specification
        current_time: Optional current time for testing or comparison.
                     If None, uses the current UTC time.

    Returns:
        bool: True if the bar is complete
    """
    warnings.warn(
        DEPRECATION_WARNING.format("is_bar_complete", "is_bar_complete"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _is_bar_complete(timestamp, interval, current_time)


class TimeRangeManager:
    """Centralized manager for handling time ranges and alignment.

    DEPRECATED: Methods in this class are deprecated in favor of utils.time_utils.
    This class will be removed in a future version.

    IMPORTANT: Do not use get_time_boundaries() or get_adjusted_boundaries()
    for REST API calls - pass timestamps directly to REST API.
    These methods are kept for Vision API and cache alignment only.
    """

    @staticmethod
    def validate_time_window(start_time: datetime, end_time: datetime) -> None:
        """Validate the time window for an API request.

        DEPRECATED: Use utils.time_utils.validate_time_window instead.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval

        Raises:
            ValueError: If start_time is after end_time or time window is invalid
        """
        warnings.warn(
            DEPRECATION_WARNING.format("validate_time_window", "validate_time_window"),
            DeprecationWarning,
            stacklevel=2,
        )
        _validate_time_window(start_time, end_time)

    @staticmethod
    def enforce_utc_timezone(dt: datetime) -> datetime:
        """Ensures datetime object is timezone aware and in UTC.

        DEPRECATED: Use utils.time_utils.enforce_utc_timezone instead.
        """
        warnings.warn(
            DEPRECATION_WARNING.format("enforce_utc_timezone", "enforce_utc_timezone"),
            DeprecationWarning,
            stacklevel=2,
        )
        return _enforce_utc_timezone(dt)

    @staticmethod
    def filter_dataframe(
        df: pd.DataFrame, start_time: datetime, end_time: datetime
    ) -> pd.DataFrame:
        """Filter a dataframe based on time boundaries.

        DEPRECATED: Use utils.time_utils.filter_dataframe_by_time instead.

        Args:
            df: Dataframe to filter
            start_time: Start time boundary (inclusive)
            end_time: End time boundary (exclusive)

        Returns:
            Filtered dataframe
        """
        warnings.warn(
            DEPRECATION_WARNING.format("filter_dataframe", "filter_dataframe_by_time"),
            DeprecationWarning,
            stacklevel=2,
        )
        return _filter_dataframe_by_time(df, start_time, end_time)

    @staticmethod
    def align_vision_api_to_rest(
        start_time: datetime, end_time: datetime, interval: Interval
    ) -> Dict[str, Any]:
        """Apply alignment to Vision API requests that matches REST API's natural boundary behavior.

        DEPRECATED: Use utils.time_utils.align_vision_api_to_rest instead.

        This function should be used ONLY for Vision API requests and cache operations
        to ensure compatibility with REST API behavior.

        Args:
            start_time: Start time for the request
            end_time: End time for the request
            interval: The interval object representing data granularity

        Returns:
            Dictionary containing adjusted start/end times and metadata
        """
        warnings.warn(
            DEPRECATION_WARNING.format(
                "align_vision_api_to_rest", "align_vision_api_to_rest"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return _align_vision_api_to_rest(start_time, end_time, interval)
