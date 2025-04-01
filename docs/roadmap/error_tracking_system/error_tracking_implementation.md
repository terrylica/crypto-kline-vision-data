# Error Tracking System Implementation

## Overview

This document outlines a comprehensive plan for implementing a centralized error tracking and classification system within the Binance Data Services architecture. The system aims to standardize error handling, improve monitoring, and provide insights into data invalidness patterns.

## Problem Statement

The current implementation lacks systematic categorization of different types of invalidness in data frames. Various scenarios such as network errors, empty responses, and validation failures can lead to invalid data, but these are not consistently categorized or monitored. This makes it difficult to:

1. Distinguish between normal invalidness and extraordinary invalidness
2. Monitor patterns of invalidness
3. Track error frequencies and trends
4. Respond appropriately to different error types

## Implementation Plan

### 1. Error Type Definitions (`utils/config.py`)

Extend the existing configuration with standardized error types and severity levels. After examining the current `utils/config.py`, we'll integrate with the existing config pattern:

```python
# Error type definitions to be added to utils/config.py
ERROR_TYPES = {
    # Network errors
    "NETWORK_TIMEOUT": {"severity": "WARNING", "description": "Network request timed out"},
    "NETWORK_CONNECTION": {"severity": "ERROR", "description": "Failed to establish connection"},
    "NETWORK_DNS": {"severity": "ERROR", "description": "DNS resolution failed"},

    # API errors
    "API_RATE_LIMIT": {"severity": "WARNING", "description": "API rate limit exceeded"},
    "API_AUTH": {"severity": "ERROR", "description": "Authentication failed"},
    "API_PERMISSION": {"severity": "ERROR", "description": "Insufficient permissions"},
    "API_FORMAT": {"severity": "ERROR", "description": "API response format error"},

    # Data errors
    "DATA_EMPTY": {"severity": "INFO", "description": "Empty data returned (expected)"},
    "DATA_VALIDATION": {"severity": "WARNING", "description": "Data validation failed"},
    "DATA_INTEGRITY": {"severity": "ERROR", "description": "Data integrity check failed"},
    "DATA_TRANSFORMATION": {"severity": "ERROR", "description": "Data transformation failed"},

    # Storage errors
    "STORAGE_READ": {"severity": "ERROR", "description": "Failed to read from storage"},
    "STORAGE_WRITE": {"severity": "ERROR", "description": "Failed to write to storage"},
    "STORAGE_SPACE": {"severity": "ERROR", "description": "Insufficient storage space"},
    "STORAGE_PERMISSION": {"severity": "ERROR", "description": "Storage permission denied"},

    # Other
    "UNEXPECTED": {"severity": "ERROR", "description": "Unexpected error occurred"},
    "INTERNAL": {"severity": "ERROR", "description": "Internal system error"},
}

# Normal vs. Extraordinary invalidness thresholds
ERROR_THRESHOLDS = {
    "DATA_EMPTY": {"per_minute": 10, "per_hour": 100, "per_day": 1000},
    "API_RATE_LIMIT": {"per_minute": 5, "per_hour": 20, "per_day": 50},
    "NETWORK_TIMEOUT": {"per_minute": 3, "per_hour": 10, "per_day": 30},
}
```

### 2. Error Handling Module (`utils/error_tracker.py`)

Create a new module with classes for error context, classification, and tracking. After analyzing the existing pattern in `utils` directory and specifically `validation_utils.py` and `network_utils.py`, we'll design this to be compatible with the existing codebase:

```python
import time
import logging
import traceback
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from utils.config import ERROR_TYPES, ERROR_THRESHOLDS

class DataResult:
    """Container for data operation results with error context"""
    def __init__(
        self,
        data: Optional[pd.DataFrame] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        exception: Optional[Exception] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.data = data if data is not None else pd.DataFrame()
        self.error_type = error_type
        self.error_message = error_message
        self.exception = exception
        self.metadata = metadata or {}
        self.timestamp = time.time()

    @property
    def is_empty(self) -> bool:
        """Check if the result contains an empty DataFrame"""
        return self.data.empty

    @property
    def has_error(self) -> bool:
        """Check if the result contains an error"""
        return self.error_type is not None

    @property
    def stack_trace(self) -> Optional[str]:
        """Get the exception stack trace if available"""
        if self.exception:
            return ''.join(traceback.format_exception(
                type(self.exception),
                self.exception,
                self.exception.__traceback__
            ))
        return None

    def get_error_severity(self) -> str:
        """Get the severity level of the error"""
        if not self.error_type:
            return "INFO"
        return ERROR_TYPES.get(self.error_type, {}).get("severity", "ERROR")

    def to_log_dict(self) -> Dict[str, Any]:
        """Convert to a dictionary suitable for logging"""
        return {
            "error_type": self.error_type,
            "error_message": self.error_message,
            "is_empty": self.is_empty,
            "has_error": self.has_error,
            "rows": len(self.data) if not self.is_empty else 0,
            "severity": self.get_error_severity(),
            "timestamp": self.timestamp,
            **self.metadata
        }


def classify_exception(exception: Exception, context: Dict[str, Any] = None) -> Tuple[str, str]:
    """
    Classify an exception into a standardized error type and message

    Args:
        exception: The exception to classify
        context: Additional context for classification

    Returns:
        Tuple of (error_type, error_message)
    """
    context = context or {}
    ex_type = type(exception).__name__
    ex_msg = str(exception)

    # Network errors
    if ex_type in ("ConnectionError", "ConnectTimeout", "ReadTimeout"):
        return "NETWORK_CONNECTION", f"Connection failed: {ex_msg}"
    elif ex_type == "Timeout" or "timeout" in ex_msg.lower():
        return "NETWORK_TIMEOUT", f"Request timed out: {ex_msg}"

    # API errors
    elif ex_type == "HTTPError":
        if "429" in ex_msg:
            return "API_RATE_LIMIT", f"Rate limit exceeded: {ex_msg}"
        elif "401" in ex_msg or "403" in ex_msg:
            return "API_AUTH", f"Authentication failed: {ex_msg}"
        elif "404" in ex_msg:
            return "API_FORMAT", f"Resource not found: {ex_msg}"
        else:
            return "API_FORMAT", f"HTTP error: {ex_msg}"

    # Data errors
    elif ex_type in ("ValueError", "TypeError", "KeyError") and context.get("during_validation"):
        return "DATA_VALIDATION", f"Validation error: {ex_msg}"
    elif ex_type in ("JSONDecodeError", "ParserError"):
        return "DATA_TRANSFORMATION", f"Failed to parse data: {ex_msg}"

    # Storage errors
    elif ex_type in ("IOError", "FileNotFoundError", "PermissionError") and context.get("storage_operation"):
        if "permission" in ex_msg.lower():
            return "STORAGE_PERMISSION", f"Storage permission denied: {ex_msg}"
        elif context.get("storage_operation") == "read":
            return "STORAGE_READ", f"Failed to read from storage: {ex_msg}"
        elif context.get("storage_operation") == "write":
            return "STORAGE_WRITE", f"Failed to write to storage: {ex_msg}"

    # Default case
    return "UNEXPECTED", f"Unexpected error: {ex_type} - {ex_msg}"

def classify_empty_data(context: Dict[str, Any] = None) -> Tuple[str, str]:
    """
    Classify empty data scenarios

    Args:
        context: Additional context about why data is empty

    Returns:
        Tuple of (error_type, error_message)
    """
    context = context or {}

    reason = context.get("empty_reason")
    if reason == "date_range":
        return "DATA_EMPTY", "No data available for the specified date range"
    elif reason == "filter_result":
        return "DATA_EMPTY", "No data matched the filter criteria"
    elif reason == "api_response":
        return "DATA_EMPTY", "API returned empty response"

    return "DATA_EMPTY", "No data available"


class ErrorTracker:
    """
    Tracks error occurrences and detects patterns
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, logger=None):
        if self._initialized:
            return

        self.logger = logger or logging.getLogger(__name__)
        self.error_counts = {}
        self.reset_time = time.time()
        self.time_windows = {
            "minute": 60,
            "hour": 3600,
            "day": 86400
        }
        self._initialized = True

    def track_error(self, error_type: str, metadata: Dict[str, Any] = None) -> None:
        """
        Track an error occurrence

        Args:
            error_type: The type of error that occurred
            metadata: Additional metadata about the error context
        """
        current_time = time.time()

        if error_type not in self.error_counts:
            self.error_counts[error_type] = []

        # Add the current occurrence
        self.error_counts[error_type].append({
            "timestamp": current_time,
            "metadata": metadata or {}
        })

        # Check if this exceeds thresholds
        self._check_thresholds(error_type, current_time)

        # Clean up old entries
        self._cleanup_old_entries(current_time)

    def _check_thresholds(self, error_type: str, current_time: float) -> None:
        """Check if error occurrences exceed defined thresholds"""
        if error_type not in ERROR_THRESHOLDS:
            return

        thresholds = ERROR_THRESHOLDS[error_type]
        occurrences = self.error_counts[error_type]

        for window_name, window_seconds in self.time_windows.items():
            threshold_key = f"per_{window_name}"
            if threshold_key in thresholds:
                # Count occurrences in this time window
                count_in_window = sum(
                    1 for entry in occurrences
                    if current_time - entry["timestamp"] <= window_seconds
                )

                # Check if threshold exceeded
                if count_in_window > thresholds[threshold_key]:
                    severity = ERROR_TYPES.get(error_type, {}).get("severity", "ERROR")
                    self.logger.warning(
                        f"THRESHOLD EXCEEDED: {error_type} occurred {count_in_window} times "
                        f"in the last {window_name} (threshold: {thresholds[threshold_key]})"
                    )

    def _cleanup_old_entries(self, current_time: float) -> None:
        """Remove entries older than the longest time window"""
        max_age = max(self.time_windows.values())

        for error_type in self.error_counts:
            self.error_counts[error_type] = [
                entry for entry in self.error_counts[error_type]
                if current_time - entry["timestamp"] <= max_age
            ]

    def get_stats(self, window_seconds: int = None) -> Dict[str, Dict[str, int]]:
        """
        Get error statistics

        Args:
            window_seconds: Optional time window to filter stats (default: all time)

        Returns:
            Dictionary with error counts by type
        """
        current_time = time.time()
        stats = {}

        for error_type, occurrences in self.error_counts.items():
            if window_seconds:
                filtered_occurrences = [
                    entry for entry in occurrences
                    if current_time - entry["timestamp"] <= window_seconds
                ]
            else:
                filtered_occurrences = occurrences

            stats[error_type] = {
                "count": len(filtered_occurrences),
                "severity": ERROR_TYPES.get(error_type, {}).get("severity", "ERROR")
            }

        return stats


# Global instance for application-wide tracking
error_tracker = ErrorTracker()
```

### 3. Logger Enhancement (`utils/logger_setup.py`)

Based on examining the existing `logger_setup.py` which is relatively simple at 102 lines, we'll enhance it with error context support:

```python
import logging
import json
from logging import LogRecord
from typing import Dict, Any

class ErrorContextFormatter(logging.Formatter):
    """Custom formatter that handles error context in log records"""

    def format(self, record: LogRecord) -> str:
        """Format log records with error context if available"""
        # Check for error context
        error_context = getattr(record, 'error_context', None)

        if error_context and isinstance(error_context, dict):
            # Format the base message
            formatted_message = super().format(record)

            # Add error context as JSON
            try:
                context_str = json.dumps(error_context)
                return f"{formatted_message} | Error Context: {context_str}"
            except (TypeError, ValueError):
                # Fall back to string representation if JSON serialization fails
                return f"{formatted_message} | Error Context: {str(error_context)}"

        # Regular formatting for messages without error context
        return super().format(record)

# Update existing get_logger function
def get_logger(name, level="INFO"):
    """
    Get a configured logger with error context handling

    Args:
        name: Logger name
        level: Logging level

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level))

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create console handler with error context formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ErrorContextFormatter(
        '%(asctime)s [%(name)s] [%(levelname)s] %(message)s'
    ))
    logger.addHandler(console_handler)

    return logger

def log_with_error_context(logger, level, message, error_context=None, exc_info=None):
    """
    Log a message with error context

    Args:
        logger: Logger instance
        level: Log level (e.g., 'INFO', 'ERROR')
        message: Log message
        error_context: Dictionary with error context
        exc_info: Exception info for traceback
    """
    log_method = getattr(logger, level.lower())

    if error_context:
        extra = {'error_context': error_context}
        log_method(message, extra=extra, exc_info=exc_info)
    else:
        log_method(message, exc_info=exc_info)
```

### 4. Data Source Manager Integration (`core/data_source_manager.py`)

After examining the existing `data_source_manager.py` (568 lines), we'll integrate our error tracking system with it:

```python
from utils.error_tracker import DataResult, classify_exception, classify_empty_data, error_tracker
from utils.logger_setup import log_with_error_context

# Modified _fetch_from_source method in DataSourceManager
def _fetch_from_source(self, symbol, interval, start_time, end_time, **kwargs):
    """Fetch data from source with error tracking"""
    try:
        # [... existing code to fetch data ...]

        # Check if data is empty
        if df.empty:
            error_type, error_message = classify_empty_data({
                "empty_reason": "api_response",
                "symbol": symbol,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time
            })

            # Track empty results
            error_tracker.track_error(error_type, {
                "symbol": symbol,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time
            })

            # Log the empty result
            log_with_error_context(
                self.logger,
                "INFO",
                f"Empty data returned for {symbol} {interval} from {start_time} to {end_time}",
                {"error_type": error_type, "error_message": error_message}
            )

            return DataResult(
                data=df,
                error_type=error_type,
                error_message=error_message,
                metadata={
                    "symbol": symbol,
                    "interval": interval,
                    "start_time": start_time,
                    "end_time": end_time
                }
            )

        # Return successful result
        return DataResult(
            data=df,
            metadata={
                "symbol": symbol,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time
            }
        )

    except Exception as e:
        # Classify the error
        error_type, error_message = classify_exception(e, {
            "symbol": symbol,
            "interval": interval,
            "start_time": start_time,
            "end_time": end_time
        })

        # Track the error
        error_tracker.track_error(error_type, {
            "symbol": symbol,
            "interval": interval,
            "start_time": start_time,
            "end_time": end_time,
            "exception_type": type(e).__name__
        })

        # Log the error
        log_with_error_context(
            self.logger,
            "ERROR",
            f"Error fetching data for {symbol} {interval}: {error_message}",
            {"error_type": error_type, "error_message": error_message},
            exc_info=True
        )

        # Return error result with empty dataframe
        empty_df = self.rest_client.create_empty_dataframe()
        return DataResult(
            data=empty_df,
            error_type=error_type,
            error_message=error_message,
            exception=e,
            metadata={
                "symbol": symbol,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time
            }
        )
```

### 5. REST Client Integration (`core/rest_data_client.py`)

Based on examining `rest_data_client.py` (838 lines), which focuses on handling REST API requests for data fetching:

```python
from utils.error_tracker import DataResult, classify_exception, classify_empty_data, error_tracker
from utils.logger_setup import log_with_error_context

# Modify the fetch method in RestDataClient
def fetch(self, symbol, interval, start_time, end_time, **kwargs):
    """Fetch data with error tracking"""
    market_type = kwargs.get('market_type', 'SPOT')

    try:
        # [... existing code to fetch data ...]

        # Check if response is valid and not empty
        # [... existing validation code ...]

        # Process data and return result
        return DataResult(
            data=df,
            metadata={
                "symbol": symbol,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
                "market_type": market_type,
                "source": "rest_api"
            }
        )

    except Exception as e:
        # Classify the error
        error_type, error_message = classify_exception(e, {
            "symbol": symbol,
            "interval": interval,
            "start_time": start_time,
            "end_time": end_time,
            "market_type": market_type,
            "source": "rest_api"
        })

        # Track the error
        error_tracker.track_error(error_type, {
            "symbol": symbol,
            "interval": interval,
            "start_time": start_time,
            "end_time": end_time,
            "market_type": market_type,
            "source": "rest_api",
            "exception_type": type(e).__name__
        })

        # Log the error
        log_with_error_context(
            self.logger,
            "ERROR",
            f"REST API error for {symbol} {interval} in {market_type}: {error_message}",
            {"error_type": error_type, "error_message": error_message},
            exc_info=True
        )

        # Return error result with empty dataframe
        empty_df = self.create_empty_dataframe()
        return DataResult(
            data=empty_df,
            error_type=error_type,
            error_message=error_message,
            exception=e,
            metadata={
                "symbol": symbol,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
                "market_type": market_type,
                "source": "rest_api"
            }
        )
```

### 6. Network Utilities Enhancement (`utils/network_utils.py`)

Based on examining the comprehensive `network_utils.py` (1310 lines) which handles various network operations:

```python
from utils.error_tracker import classify_exception, error_tracker
from utils.logger_setup import log_with_error_context

# Enhanced check_connectivity function
def check_connectivity(url, timeout=5, logger=None):
    """
    Check connectivity to a URL with enhanced error reporting

    Args:
        url: URL to check
        timeout: Connection timeout in seconds
        logger: Logger instance

    Returns:
        Tuple of (bool, error_type, error_message)
    """
    logger = logger or logging.getLogger(__name__)

    try:
        # Using curl_cffi as per project conventions
        response = curl_cffi.requests.head(url, timeout=timeout)
        response.raise_for_status()
        return True, None, None
    except Exception as e:
        # Classify the error
        error_type, error_message = classify_exception(e, {
            "url": url,
            "timeout": timeout
        })

        # Track the error
        error_tracker.track_error(error_type, {
            "url": url,
            "timeout": timeout,
            "exception_type": type(e).__name__
        })

        # Log the error
        log_with_error_context(
            logger,
            "WARNING",
            f"Connectivity check failed for {url}: {error_message}",
            {"error_type": error_type, "error_message": error_message}
        )

        return False, error_type, error_message
```

## Integration Test Plan

1. **Unit Tests for Error Classification**

   - Create tests to verify error classification logic for different exception types
   - Ensure consistent error type assignment based on exception characteristics
   - Test the empty data classification with different context scenarios

2. **Unit Tests for Error Tracking**

   - Verify the singleton pattern of ErrorTracker
   - Test threshold detection for different error frequencies
   - Validate the cleanup of old entries works as expected
   - Confirm statistics gathering functions with time-based filtering

3. **Integration Test for DataResult Usage**

   - Test DataResult creation and property access
   - Verify error context is properly captured and propagated
   - Test serialization to log dictionaries

4. **End-to-End Tests**
   - Create tests that simulate various error conditions in REST and Vision clients
   - Verify error tracking during concurrent operations
   - Test threshold alerting with artificially increased error rates
   - Validate integration with existing error handling mechanisms

## Metrics and Monitoring

The error tracking system will provide the following metrics:

1. **Error counts by type and severity**

   - Track how often each error type occurs
   - Group by severity levels for better prioritization

2. **Error rates over time**

   - Per-minute rates for immediate operational issues
   - Per-hour and per-day rates for trend analysis

3. **Threshold violation alerts**

   - Configurable thresholds for different error types
   - Alerting when error rates exceed normal bounds

4. **Empty data statistics**
   - Track empty data by reason and source
   - Differentiate between expected and unexpected empty results

These metrics will enable:

1. Establishing baseline error rates for normal operation
2. Detecting anomalies in error patterns
3. Identifying problematic symbols, intervals or market types
4. Optimizing retry strategies and timeouts based on actual performance data

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1)

- Add error type definitions to `utils/config.py`
- Create `utils/error_tracker.py` with DataResult class and classification functions
- Update logger setup in `utils/logger_setup.py`

### Phase 2: Integration (Weeks 2-3)

- Integrate with `core/data_source_manager.py`
- Integrate with `core/rest_data_client.py`
- Update network utilities in `utils/network_utils.py`
- Create unit tests for core components

### Phase 3: Monitoring and Reporting (Week 4)

- Finalize ErrorTracker with threshold detection
- Add metrics collection for different time windows
- Create integration tests to verify cross-component functionality

### Phase 4: Optimization (Week 5+)

- Fine-tune thresholds based on operational data
- Implement adaptive retry strategies based on error patterns
- Add predictive error detection for common failure modes

## Conclusion

This implementation provides a comprehensive framework for categorizing, tracking, and responding to different types of data invalidness. By standardizing error handling and providing detailed context, the system enables better monitoring and more effective troubleshooting of data issues.

The modular design allows for incremental implementation and extension as new error patterns are identified. The integration with existing components maintains compatibility with established code patterns and conventions while adding valuable metrics for optimizing the data pipeline and improving overall data quality.
