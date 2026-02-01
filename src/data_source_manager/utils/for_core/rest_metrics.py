#!/usr/bin/env python
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Use UTC-aware datetimes consistently
"""Utilities for tracking REST API metrics and performance.

This module provides functionality for monitoring and tracking metrics related to
REST API requests, such as response times, success rates, and rate limiting.
"""

import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

from data_source_manager.utils.config import SECONDS_IN_HOUR
from data_source_manager.utils.loguru_setup import logger


class RestMetricsTracker:
    """Tracker for REST API metrics with thread-safe implementation."""

    # Singleton instance
    _instance = None
    _lock = threading.Lock()

    def __new__(cls) -> "RestMetricsTracker":
        """Create a singleton instance."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        """Initialize metrics tracking."""
        if self._initialized:
            return

        self._initialized = True
        self._lock = threading.Lock()

        # Track API call statistics
        self._total_calls = 0
        self._successful_calls = 0
        self._failed_calls = 0
        self._rate_limited_calls = 0

        # Track response times (last 100 calls)
        self._response_times = deque(maxlen=100)

        # Track calls by endpoint
        self._calls_by_endpoint = defaultdict(int)

        # Track errors by type
        self._errors_by_type = defaultdict(int)

        # Track rate limiting
        self._rate_limit_windows = []

        # Store the last set of parameters for debugging
        self._last_params = {}

        logger.debug("Initialized REST metrics tracker")

    def record_api_call(
        self,
        endpoint: str,
        params: dict,
        start_time: float,
        end_time: float,
        success: bool,
        error_type: str | None = None,
        status_code: int | None = None,
    ) -> None:
        """Record metrics for an API call.

        Args:
            endpoint: API endpoint called
            params: Parameters used in the call
            start_time: Call start time (from time.time())
            end_time: Call end time (from time.time())
            success: Whether the call was successful
            error_type: Type of error if the call failed
            status_code: HTTP status code if available
        """
        with self._lock:
            self._total_calls += 1

            # Record response time
            response_time = end_time - start_time
            self._response_times.append(response_time)

            # Track by endpoint
            self._calls_by_endpoint[endpoint] += 1

            # Store the last set of params for debugging/auditing
            self._last_params = params

            if success:
                self._successful_calls += 1
            else:
                self._failed_calls += 1
                if error_type:
                    self._errors_by_type[error_type] += 1

                # Track rate limiting
                if status_code in (418, 429):
                    self._rate_limited_calls += 1
                    now = datetime.now(timezone.utc)
                    self._rate_limit_windows.append(now)
                    # Clean up old rate limit windows (older than 1 hour)
                    self._rate_limit_windows = [
                        t for t in self._rate_limit_windows if (now - t).total_seconds() < SECONDS_IN_HOUR
                    ]

    def get_metrics(self) -> dict[str, Any]:
        """Get current metrics.

        Returns:
            Dictionary of metrics
        """
        with self._lock:
            # Calculate average response time
            avg_response_time = sum(self._response_times) / len(self._response_times) if self._response_times else 0

            # Calculate success rate
            success_rate = (self._successful_calls / self._total_calls) * 100 if self._total_calls > 0 else 0

            # Get rate limiting frequency (per hour)
            rate_limit_frequency = len(self._rate_limit_windows)

            return {
                "total_calls": self._total_calls,
                "successful_calls": self._successful_calls,
                "failed_calls": self._failed_calls,
                "rate_limited_calls": self._rate_limited_calls,
                "success_rate": success_rate,
                "avg_response_time_ms": avg_response_time * 1000,  # Convert to ms
                "calls_by_endpoint": dict(self._calls_by_endpoint),
                "errors_by_type": dict(self._errors_by_type),
                "rate_limit_frequency_per_hour": rate_limit_frequency,
            }

    def log_metrics(self) -> None:
        """Log current metrics to the logger."""
        metrics = self.get_metrics()

        logger.info("REST API Metrics Summary:")
        logger.info(f"  Total calls: {metrics['total_calls']}")
        logger.info(f"  Success rate: {metrics['success_rate']:.2f}%")
        logger.info(f"  Avg response time: {metrics['avg_response_time_ms']:.2f}ms")
        logger.info(f"  Rate limited calls: {metrics['rate_limited_calls']}")

        if metrics["errors_by_type"]:
            logger.info("  Errors by type:")
            for error_type, count in metrics["errors_by_type"].items():
                logger.info(f"    {error_type}: {count}")

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._total_calls = 0
            self._successful_calls = 0
            self._failed_calls = 0
            self._rate_limited_calls = 0
            self._response_times.clear()
            self._calls_by_endpoint.clear()
            self._errors_by_type.clear()
            self._rate_limit_windows.clear()

            logger.debug("Reset REST metrics tracker")


# Create the singleton instance
metrics_tracker = RestMetricsTracker()


def track_api_call(endpoint: str, params: dict[str, Any]) -> callable:
    """Decorator to track API call metrics.

    Args:
        endpoint: API endpoint being called
        params: Parameters for the API call

    Returns:
        Decorated function
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            error_type = None
            status_code = None
            success = False

            try:
                result = func(*args, **kwargs)
                success = True
                return result
            except Exception as e:
                error_type = type(e).__name__
                if hasattr(e, "status_code"):
                    status_code = e.status_code
                raise
            finally:
                end_time = time.time()
                metrics_tracker.record_api_call(
                    endpoint=endpoint,
                    params=params,
                    start_time=start_time,
                    end_time=end_time,
                    success=success,
                    error_type=error_type,
                    status_code=status_code,
                )

        return wrapper

    return decorator
