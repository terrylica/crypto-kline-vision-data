#!/usr/bin/env python
"""Centralized configuration for the data services system.

This module centralizes constants and configuration parameters that were previously
scattered across multiple files, creating a single source of truth for system-wide settings.
"""

from datetime import timedelta, timezone
from typing import Dict, Final, Any

# Time-related constants
DEFAULT_TIMEZONE: Final = timezone.utc
CANONICAL_INDEX_NAME: Final = "open_time"
TIMESTAMP_PRECISION: Final = "us"  # Microsecond precision

# API-specific constraints
VISION_DATA_DELAY_HOURS: Final = (
    36  # Data newer than this isn't available in Vision API
)

# Time constraints
CONSOLIDATION_DELAY: Final = timedelta(
    hours=48
)  # Time Binance needs to consolidate daily data
MAX_TIME_RANGE: Final = timedelta(days=30)  # Maximum time range for single request
MAX_HISTORICAL_DAYS: Final = 1000  # Maximum days back for historical data
INCOMPLETE_BAR_THRESHOLD: Final = timedelta(
    minutes=5
)  # Time after which bars are considered complete

# Cache settings
CACHE_MAX_AGE: Final = timedelta(days=30)
CACHE_UPDATE_INTERVAL: Final = timedelta(minutes=5)
MIN_CACHE_FILE_SIZE: Final = 1024  # 1KB minimum

# API constraints
API_TIMEOUT: Final = 30  # Seconds
API_MAX_RETRIES: Final = 3
API_RETRY_DELAY: Final = 1  # Seconds

# Standard output format for DataFrames
OUTPUT_DTYPES: Final[Dict[str, str]] = {
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "close_time": "int64",
    "quote_volume": "float64",
    "trades": "int64",
    "taker_buy_volume": "float64",
    "taker_buy_quote_volume": "float64",
}

# Chunk size constraints
REST_CHUNK_SIZE: Final = 1000  # Maximum records per REST API request
REST_MAX_CHUNKS: Final = 10  # Maximum number of chunks to request via REST
MAXIMUM_CONCURRENT_DOWNLOADS: Final = 13

# File formats
FILE_EXTENSIONS: Final[Dict[str, str]] = {
    "DATA": ".zip",
    "CHECKSUM": ".CHECKSUM",
    "CACHE": ".arrow",
    "METADATA": ".json",
}

# Error classification
ERROR_TYPES: Final[Dict[str, str]] = {
    "NETWORK": "network_error",
    "FILE_SYSTEM": "file_system_error",
    "DATA_INTEGRITY": "data_integrity_error",
    "CACHE_INVALID": "cache_invalid",
    "VALIDATION": "validation_error",
    "AVAILABILITY": "availability_error",
}


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
