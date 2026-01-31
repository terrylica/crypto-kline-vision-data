#!/usr/bin/env python
"""Time utilities for handling time alignment and boundaries in market data operations.

This module re-exports all functions from the modular time package for backwards
compatibility. New code should import directly from data_source_manager.utils.time.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Split into focused modules under utils/time/
# polars-exception: extracted from existing pandas-based time_utils.py - migration is separate task

Example:
    >>> from data_source_manager.utils.time_utils import align_time_boundaries, datetime_to_milliseconds
    >>> from data_source_manager.utils.market_constraints import Interval
    >>> from datetime import datetime, timezone
    >>>
    >>> # Align time boundaries for a 1-minute interval request
    >>> start = datetime(2023, 1, 1, 12, 34, 56, tzinfo=timezone.utc)
    >>> end = datetime(2023, 1, 1, 15, 45, 23, tzinfo=timezone.utc)
    >>> aligned_start, aligned_end = align_time_boundaries(start, end, Interval.MINUTE_1)
    >>>
    >>> print(f"Original: {start} to {end}")
    >>> print(f"Aligned: {aligned_start} to {aligned_end}")
    >>>
    >>> # Convert datetime to milliseconds for API requests
    >>> ms_timestamp = datetime_to_milliseconds(aligned_start)
    >>> print(f"Millisecond timestamp: {ms_timestamp}")
"""

# Re-export everything from the modular time package
from data_source_manager.utils.time import (
    MICROSECOND_DIGITS,
    MILLISECOND_DIGITS,
    TIMESTAMP_UNIT,
    TimeseriesDataProcessor,
    TimestampUnit,
    align_time_boundaries,
    datetime_to_milliseconds,
    detect_timestamp_unit,
    enforce_utc_timezone,
    estimate_record_count,
    filter_dataframe_by_time,
    get_bar_close_time,
    get_interval_ceiling,
    get_interval_floor,
    get_interval_micros,
    get_interval_seconds,
    get_interval_timedelta,
    get_smaller_units,
    is_bar_complete,
    milliseconds_to_datetime,
    standardize_timestamp_precision,
    validate_timestamp_unit,
)

__all__ = [
    "MICROSECOND_DIGITS",
    "MILLISECOND_DIGITS",
    "TIMESTAMP_UNIT",
    "TimeseriesDataProcessor",
    "TimestampUnit",
    "align_time_boundaries",
    "datetime_to_milliseconds",
    "detect_timestamp_unit",
    "enforce_utc_timezone",
    "estimate_record_count",
    "filter_dataframe_by_time",
    "get_bar_close_time",
    "get_interval_ceiling",
    "get_interval_floor",
    "get_interval_micros",
    "get_interval_seconds",
    "get_interval_timedelta",
    "get_smaller_units",
    "is_bar_complete",
    "milliseconds_to_datetime",
    "standardize_timestamp_precision",
    "validate_timestamp_unit",
]
