"""Utility modules for data source management.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Round 14: Lazy imports via __getattr__ to reduce cold start time.
# Only imports modules when their attributes are actually accessed.
"""

import importlib
from typing import Any

# Map each exported name to its source module (relative to this package)
_LAZY_IMPORTS: dict[str, str] = {
    # From .market_constraints
    "ChartType": ".market_constraints",
    "DataProvider": ".market_constraints",
    "Interval": ".market_constraints",
    "MarketType": ".market_constraints",
    # From .config
    "API_MAX_RETRIES": ".config",
    "API_RETRY_DELAY": ".config",
    "API_TIMEOUT": ".config",
    "CANONICAL_CLOSE_TIME": ".config",
    "CANONICAL_INDEX_NAME": ".config",
    "COLUMN_NAME_MAPPING": ".config",
    "DEFAULT_CACHE_DIR": ".config",
    "DEFAULT_COLUMN_ORDER": ".config",
    "DEFAULT_LOG_DIR": ".config",
    "DEFAULT_TIMEZONE": ".config",
    "FEATURE_FLAGS": ".config",
    "FUNDING_RATE_COLUMN_ORDER": ".config",
    "FUNDING_RATE_COLUMNS": ".config",
    "FUNDING_RATE_DTYPES": ".config",
    "KLINE_COLUMNS": ".config",
    "MAX_CACHE_AGE": ".config",
    "OUTPUT_DTYPES": ".config",
    "REST_CHUNK_SIZE": ".config",
    "TIMESTAMP_PRECISION": ".config",
    "TIMESTAMP_UNIT": ".config",
    "VISION_DATA_DELAY_HOURS": ".config",
    "FeatureFlags": ".config",
    "FileType": ".config",
    "create_empty_dataframe": ".config",
    "create_empty_funding_rate_dataframe": ".config",
    "standardize_column_names": ".config",
    # From .time_utils
    "MICROSECOND_DIGITS": ".time_utils",
    "MILLISECOND_DIGITS": ".time_utils",
    "TimeseriesDataProcessor": ".time_utils",
    "TimestampUnit": ".time_utils",
    "align_time_boundaries": ".time_utils",
    "datetime_to_milliseconds": ".time_utils",
    "detect_timestamp_unit": ".time_utils",
    "enforce_utc_timezone": ".time_utils",
    "estimate_record_count": ".time_utils",
    "filter_dataframe_by_time": ".time_utils",
    "get_bar_close_time": ".time_utils",
    "get_interval_ceiling": ".time_utils",
    "get_interval_floor": ".time_utils",
    "get_interval_micros": ".time_utils",
    "get_interval_seconds": ".time_utils",
    "get_interval_timedelta": ".time_utils",
    "get_smaller_units": ".time_utils",
    "is_bar_complete": ".time_utils",
    "milliseconds_to_datetime": ".time_utils",
    "standardize_timestamp_precision": ".time_utils",
    "validate_timestamp_unit": ".time_utils",
    # From .validation
    "ALL_COLUMNS": ".validation",
    "INTERVAL_PATTERN": ".validation",
    "OHLCV_COLUMNS": ".validation",
    "SYMBOL_PATTERN": ".validation",
    "TICKER_PATTERN": ".validation",
    "DataFrameValidator": ".validation",
    "DataValidation": ".validation",
    "ValidationError": ".validation",
    "calculate_checksum": ".validation",
    "is_data_likely_available": ".validation",
    "validate_data_availability": ".validation",
    "validate_file_with_checksum": ".validation",
}


def __getattr__(name: str) -> Any:
    if name in _LAZY_IMPORTS:
        module = importlib.import_module(_LAZY_IMPORTS[name], __name__)
        val = getattr(module, name)
        # Cache in module globals for subsequent access (no repeated __getattr__)
        globals()[name] = val
        return val
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Validation
    "ALL_COLUMNS",
    # Config constants
    "API_MAX_RETRIES",
    "API_RETRY_DELAY",
    "API_TIMEOUT",
    "CANONICAL_CLOSE_TIME",
    "CANONICAL_INDEX_NAME",
    "COLUMN_NAME_MAPPING",
    "DEFAULT_CACHE_DIR",
    "DEFAULT_COLUMN_ORDER",
    "DEFAULT_LOG_DIR",
    "DEFAULT_TIMEZONE",
    "FEATURE_FLAGS",
    "FUNDING_RATE_COLUMNS",
    "FUNDING_RATE_COLUMN_ORDER",
    "FUNDING_RATE_DTYPES",
    "INTERVAL_PATTERN",
    "KLINE_COLUMNS",
    "MAX_CACHE_AGE",
    # Time utils
    "MICROSECOND_DIGITS",
    "MILLISECOND_DIGITS",
    "OHLCV_COLUMNS",
    "OUTPUT_DTYPES",
    "REST_CHUNK_SIZE",
    "SYMBOL_PATTERN",
    "TICKER_PATTERN",
    "TIMESTAMP_PRECISION",
    "TIMESTAMP_UNIT",
    "VISION_DATA_DELAY_HOURS",
    # Market constraints
    "ChartType",
    "DataFrameValidator",
    "DataProvider",
    "DataValidation",
    "FeatureFlags",
    "FileType",
    "Interval",
    "MarketType",
    "TimeseriesDataProcessor",
    "TimestampUnit",
    "ValidationError",
    "align_time_boundaries",
    "calculate_checksum",
    "create_empty_dataframe",
    "create_empty_funding_rate_dataframe",
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
    "is_data_likely_available",
    "milliseconds_to_datetime",
    "standardize_column_names",
    "standardize_timestamp_precision",
    "validate_data_availability",
    "validate_file_with_checksum",
    "validate_timestamp_unit",
]
