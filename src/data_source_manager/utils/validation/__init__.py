"""Centralized validation utilities for data integrity and constraints.

This package provides comprehensive validation for:
- Time boundaries and date ranges
- Symbol formats and intervals
- DataFrame structure and integrity
- Cache file validation
- Data availability checking

The main classes are exported for backward compatibility:
- DataValidation: Time, date, and symbol validation
- DataFrameValidator: DataFrame structure validation
- ValidationError: Custom exception for validation errors

Package structure:
- time_validation.py: Core time/date validation
- availability_validation.py: Data availability checking
- file_validation.py: Checksum and file validation
- dataframe_validation.py: DataFrame structure validation
"""

from data_source_manager.utils.validation.availability_validation import (
    is_data_likely_available,
    validate_data_availability,
)
from data_source_manager.utils.validation.dataframe_validation import (
    DataFrameValidator,
)
from data_source_manager.utils.validation.file_validation import (
    calculate_checksum,
    validate_file_with_checksum,
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
    # Classes
    "DataFrameValidator",
    "DataValidation",
    "ValidationError",
    # Functions
    "calculate_checksum",
    "is_data_likely_available",
    "validate_data_availability",
    "validate_file_with_checksum",
]
