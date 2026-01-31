"""Centralized validation utilities for data integrity and constraints.

This package provides comprehensive validation for:
- Time boundaries and date ranges
- Symbol formats and intervals
- DataFrame structure and integrity
- Cache file validation

The main classes are exported for backward compatibility:
- DataValidation: Time, date, and symbol validation
- DataFrameValidator: DataFrame structure validation
- ValidationError: Custom exception for validation errors
"""

from data_source_manager.utils.validation.dataframe_validation import (
    DataFrameValidator,
)
from data_source_manager.utils.validation.time_validation import (
    DataValidation,
    ValidationError,
)

# Constants re-exported for backward compatibility
from data_source_manager.utils.validation.time_validation import (
    ALL_COLUMNS,
    INTERVAL_PATTERN,
    OHLCV_COLUMNS,
    SYMBOL_PATTERN,
    TICKER_PATTERN,
)

__all__ = [
    # Constants
    "ALL_COLUMNS",
    "INTERVAL_PATTERN",
    "OHLCV_COLUMNS",
    "SYMBOL_PATTERN",
    "TICKER_PATTERN",
    "DataFrameValidator",
    # Classes
    "DataValidation",
    "ValidationError",
]
