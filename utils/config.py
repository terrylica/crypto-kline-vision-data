#!/usr/bin/env python
"""Centralized configuration for the data services system.

This module centralizes constants and configuration parameters that were previously
scattered across multiple files, creating a single source of truth for system-wide settings.
"""

import os
from datetime import timedelta, timezone
from typing import Dict, Final, Any, List
import pandas as pd
from enum import Enum, auto

# Time-related constants
DEFAULT_TIMEZONE: Final = timezone.utc
CANONICAL_INDEX_NAME: Final = "open_time"
TIMESTAMP_PRECISION: Final = (
    "ms"  # Millisecond precision to align with REST API standard
)

# REST API standardization
# We standardize to millisecond precision as this is what the REST API consistently uses
# All data from Vision API and cache will be converted to this precision for consistency
REST_IS_STANDARD: Final = True  # REST API format is the standard for all data sources

# API-specific constraints
VISION_DATA_DELAY_HOURS: Final = 48

# Time constraints
CONSOLIDATION_DELAY: Final = timedelta(hours=48)
INCOMPLETE_BAR_THRESHOLD: Final = timedelta(
    minutes=5
)  # Time after which bars are considered complete

# Cache settings
MAX_CACHE_AGE: Final = timedelta(days=30)
CACHE_UPDATE_INTERVAL: Final = timedelta(minutes=5)
MIN_VALID_FILE_SIZE: Final = 1024  # 1KB minimum

# API constraints
MAX_TIMEOUT: Final = 9.0  # Maximum timeout for any individual operation in seconds
API_TIMEOUT: Final = 3.0  # Seconds - standardized based on benchmarks
API_MAX_RETRIES: Final = 3
API_RETRY_DELAY: Final = 1  # Seconds

# Resource cleanup timeouts
RESOURCE_CLEANUP_TIMEOUT: Final = 0.1  # Seconds - for generic async resource cleanup
HTTP_CLIENT_CLEANUP_TIMEOUT: Final = 0.2  # Seconds - for HTTP client cleanup
FILE_CLEANUP_TIMEOUT: Final = 0.3  # Seconds - for file handle cleanup
ENABLE_FORCED_GC: Final = True  # Whether to force garbage collection after cleanup

# Task cancellation timeouts
TASK_CANCEL_WAIT_TIMEOUT: Final = (
    1.0  # Seconds - default timeout for cancel_and_wait operations
)
LINGERING_TASK_CLEANUP_TIMEOUT: Final = (
    0.5  # Seconds - timeout for lingering task cleanup
)
AGGRESSIVE_TASK_CLEANUP_TIMEOUT: Final = (
    0.2  # Seconds - timeout for aggressive cleanup after initial failure
)
DEMO_SIMULATED_DELAY: Final = (
    3  # Seconds - delay for the task cancellation demonstration
)

# Canonical column names
CANONICAL_CLOSE_TIME: Final[str] = "close_time"

# Exhaustive list of all column names used in kline data
# These follow the official Binance API documentation
KLINE_COLUMNS: Final[List[str]] = [
    "open_time",  # Kline open time
    "open",  # Open price
    "high",  # High price
    "low",  # Low price
    "close",  # Close price
    "volume",  # Volume
    "close_time",  # Kline Close time
    "quote_asset_volume",  # Quote asset volume
    "count",  # Number of trades
    "taker_buy_volume",  # Taker buy base asset volume
    "taker_buy_quote_volume",  # Taker buy quote asset volume
    "ignore",  # Unused field, ignore
]

# Funding rate column names
FUNDING_RATE_COLUMNS: Final[List[str]] = [
    "time",  # Time of funding rate
    "contracts",  # Contract symbol
    "funding_interval",  # Funding interval
    "funding_rate",  # Funding rate value
]

# Standard column dtypes for all market data DataFrames
OUTPUT_DTYPES: Final[Dict[str, str]] = {
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "close_time": "datetime64[ns]",
    "quote_asset_volume": "float64",
    "count": "int64",
    "taker_buy_volume": "float64",
    "taker_buy_quote_volume": "float64",
}

# Standard column dtypes for funding rate DataFrames
FUNDING_RATE_DTYPES: Final[Dict[str, str]] = {
    "contracts": "string",
    "funding_interval": "string",
    "funding_rate": "float64",
}

# Mapping between various column name variants used in different APIs
# This comprehensive mapping ensures backward compatibility
COLUMN_NAME_MAPPING: Final[Dict[str, str]] = {
    # Quote volume variants
    "quote_volume": "quote_asset_volume",
    # Trade count variants
    "trades": "count",
    # Taker buy base volume variants
    "taker_buy_base": "taker_buy_volume",
    "taker_buy_volume": "taker_buy_volume",
    "taker_buy_base_volume": "taker_buy_volume",
    # Taker buy quote volume variants
    "taker_buy_quote": "taker_buy_quote_volume",
    "taker_buy_quote_volume": "taker_buy_quote_volume",
    # Funding rate mapping
    "time": "open_time",
    "funding_rate_pct": "funding_rate",
}

# Default column order for standardized output
DEFAULT_COLUMN_ORDER: Final[List[str]] = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
]

# Default column order for funding rate output
FUNDING_RATE_COLUMN_ORDER: Final[List[str]] = [
    "contracts",
    "funding_interval",
    "funding_rate",
]

# Timestamp configuration
TIMESTAMP_UNIT: Final[str] = (
    "ms"  # Milliseconds for timestamps - aligns with REST API standard
)
CLOSE_TIME_ADJUSTMENT: Final[int] = 999  # Milliseconds to add to close_time

# HTTP Client configuration
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
DEFAULT_ACCEPT_HEADER: Final[str] = "application/json"
DEFAULT_HTTP_TIMEOUT_SECONDS: Final[float] = (
    3.0  # Standardized timeout for all HTTP requests
)

# Chunk size constraints
REST_CHUNK_SIZE: Final = 1000
REST_MAX_CHUNKS: Final = 1000  # Increased from 5 to 1000 to effectively remove limit
MAXIMUM_CONCURRENT_DOWNLOADS: Final = 50  # Increased from 13 to 50 based on benchmarks


# File management enums and constants
class FileType(Enum):
    """Types of files managed by Vision client."""

    DATA = auto()
    CHECKSUM = auto()
    CACHE = auto()
    METADATA = auto()


# File formats
FILE_EXTENSIONS: Final[Dict[str, str]] = {
    "DATA": ".zip",
    "CHECKSUM": ".CHECKSUM",
    "CACHE": ".arrow",
    "METADATA": ".json",
}

# File constraint values
MIN_VALID_FILE_SIZE: Final[int] = 1024  # 1KB minimum for valid data files
METADATA_UPDATE_INTERVAL: Final[timedelta] = timedelta(minutes=5)

# Error classification
ERROR_TYPES: Final[Dict[str, str]] = {
    "NETWORK": "network_error",
    "FILE_SYSTEM": "file_system_error",
    "DATA_INTEGRITY": "data_integrity_error",
    "CACHE_INVALID": "cache_invalid",
    "VALIDATION": "validation_error",
    "AVAILABILITY": "availability_error",
}

# Environment configuration
ENV: Final = os.getenv("APP_ENV", "development")
DEBUG: Final = os.getenv("DEBUG", "false").lower() == "true"

# Base directories
DEFAULT_CACHE_DIR = os.path.expanduser("~/.binance_data_cache")
DEFAULT_LOG_DIR = os.path.expanduser("~/.binance_data_logs")


# Feature flags
class FeatureFlags:
    """System-wide feature flags for enabling/disabling functionality."""

    ENABLE_CACHE: bool = True
    VALIDATE_CACHE_ON_READ: bool = True
    USE_VISION_FOR_LARGE_REQUESTS: bool = True
    VALIDATE_DATA_ON_WRITE: bool = True

    @classmethod
    def update(cls, **kwargs: Any) -> None:
        """Update feature flags.

        Args:
            **kwargs: Feature flags to update

        Example:
            FeatureFlags.update(ENABLE_CACHE=False)
        """
        for key, value in kwargs.items():
            if hasattr(cls, key):
                setattr(cls, key, value)


# Create a standard empty DataFrame with proper structure
def create_empty_dataframe(chart_type=None) -> pd.DataFrame:
    """Create an empty DataFrame with the standard market data structure.

    Args:
        chart_type: Optional ChartType enum or string to specify the type of DataFrame to create.
                   If None, defaults to KLINES.

    Returns:
        An empty DataFrame with correct column types and index
    """
    from utils.market_constraints import ChartType
    from utils.logger_setup import logger

    # Determine chart type
    if isinstance(chart_type, str):
        try:
            # Use upper() for case insensitivity when converting from string
            chart_type_str = chart_type.upper()
            # Try direct enum lookup first (e.g., "KLINES" to ChartType.KLINES)
            try:
                chart_type = ChartType[chart_type_str]
            except KeyError:
                # Try from_string method as fallback
                chart_type = ChartType.from_string(chart_type)
        except (ValueError, AttributeError):
            # Default to KLINES if conversion fails
            chart_type = ChartType.KLINES

    # Create appropriate empty DataFrame based on chart type
    if chart_type == ChartType.FUNDING_RATE:
        df = pd.DataFrame([], columns=FUNDING_RATE_COLUMN_ORDER)
        dtypes_to_use = FUNDING_RATE_DTYPES
    else:  # Default to KLINES
        df = pd.DataFrame([], columns=DEFAULT_COLUMN_ORDER)
        dtypes_to_use = OUTPUT_DTYPES

    # Set correct data types
    for col, dtype in dtypes_to_use.items():
        df[col] = df[col].astype(dtype)

    # IMPORTANT: To avoid the ambiguity of having 'open_time' as both column and index,
    # we create a consistent structure where open_time is ONLY a column, and we use
    # a different internal name for the index (open_time_us)
    logger.debug("Creating empty DataFrame with consistent open_time structure")

    # Create a proper timestamp index but don't call it 'open_time'
    # to avoid conflict with the 'open_time' column
    df.index = pd.DatetimeIndex([], name="open_time_us")
    df.index = df.index.tz_localize(DEFAULT_TIMEZONE)

    # Ensure open_time exists as a column and not just as index name
    if "open_time" not in df.columns:
        df["open_time"] = pd.Series(dtype="datetime64[ns, UTC]")

    return df


# Create a standard empty funding rate DataFrame with proper structure
def create_empty_funding_rate_dataframe() -> pd.DataFrame:
    """Create an empty DataFrame with the standard funding rate data structure.

    Returns:
        An empty DataFrame with correct column types and index

    Note:
        This function is maintained for backward compatibility.
        New code should use create_empty_dataframe(ChartType.FUNDING_RATE) instead.
    """
    from utils.market_constraints import ChartType

    return create_empty_dataframe(ChartType.FUNDING_RATE)


# Function to standardize column names across different API responses
def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names in DataFrame to use canonical names.

    This function ensures column names follow the canonical naming convention.
    While Vision API data now uses proper column names from the start,
    this function is still needed for:

    1. REST API responses that might contain variant column names
    2. Third-party data sources with different column naming conventions
    3. Backward compatibility with existing code expecting canonical names

    Args:
        df: DataFrame with potentially non-standard column names

    Returns:
        DataFrame with standardized column names
    """
    for old_name, new_name in COLUMN_NAME_MAPPING.items():
        if old_name in df.columns and new_name not in df.columns:
            df[new_name] = df[old_name]
            df = df.drop(columns=[old_name])

    return df
