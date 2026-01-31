#!/usr/bin/env python
"""Time utilities package for handling time operations in market data.

This package provides modular time handling functionality split into focused modules:
- conversion: Timestamp format detection and conversion
- intervals: Interval calculations and boundary operations
- bars: Bar/candle completion detection
- filtering: DataFrame time-based filtering
- processor: Unified timeseries data processing

All functions are re-exported here for backwards compatibility.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Split time_utils.py (991 lines) into focused modules under 400 lines
"""

from data_source_manager.utils.time.bars import (
    get_bar_close_time,
    is_bar_complete,
)
from data_source_manager.utils.time.conversion import (
    MICROSECOND_DIGITS,
    MILLISECOND_DIGITS,
    TIMESTAMP_UNIT,
    TimestampUnit,
    datetime_to_milliseconds,
    detect_timestamp_unit,
    enforce_utc_timezone,
    milliseconds_to_datetime,
    standardize_timestamp_precision,
    validate_timestamp_unit,
)
from data_source_manager.utils.time.filtering import (
    filter_dataframe_by_time,
)
from data_source_manager.utils.time.intervals import (
    align_time_boundaries,
    estimate_record_count,
    get_interval_ceiling,
    get_interval_floor,
    get_interval_micros,
    get_interval_seconds,
    get_interval_timedelta,
    get_smaller_units,
)
from data_source_manager.utils.time.processor import (
    TimeseriesDataProcessor,
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
