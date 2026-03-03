#!/usr/bin/env python
"""Utilities for REST API client operations.

This module provides common utilities for REST API client operations including:
1. HTTP client creation and configuration
2. Retry logic for API requests
3. Standardized error handling

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""

from datetime import datetime
from typing import Any

import httpx
import orjson

from ckvd.utils.config import DEFAULT_HTTP_TIMEOUT_SECONDS, HTTP_OK
from ckvd.utils.for_core.rest_exceptions import (
    APIError,
    HTTPError,
    JSONDecodeError,
    NetworkError,
    RateLimitError,
    RestAPIError,
    RestTimeoutError,
)
from ckvd.utils.for_core.rest_metrics import metrics_tracker, track_api_call
from ckvd.utils.for_core.rest_retry import create_retry_decorator
from ckvd.utils.loguru_setup import logger
from ckvd.utils.market_constraints import Interval


def create_optimized_client() -> httpx.Client:
    """Create an optimized HTTP client for REST API requests.

    Returns:
        httpx.Client instance with connection pooling and timeouts.
    """
    # Round 13: httpx.Client is thread-safe, supports connection pooling,
    # and has built-in timeout configuration.
    return httpx.Client(
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        timeout=DEFAULT_HTTP_TIMEOUT_SECONDS,
        follow_redirects=True,
    )


# Apply retry decorator at module level (default retry_count=3).
# This preserves the original behavior where @patch("...fetch_chunk") replaces
# the entire decorated function in tests (no retry logic applied to mocks).
@create_retry_decorator()
def fetch_chunk(
    client: httpx.Client,
    endpoint: str,
    params: dict[str, Any],
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
) -> list[list[Any]]:
    """Fetch a chunk of data with retry logic.

    Args:
        client: HTTP client session
        endpoint: API endpoint URL
        params: Request parameters
        timeout: Request timeout in seconds

    Returns:
        List of data points from the API

    Raises:
        RestAPIError: Base exception for all REST API errors
        HTTPError: If an HTTP error occurs
        APIError: If the API returns an error code
        RateLimitError: If rate limited by the API
        NetworkError: If a network error occurs
        RestTimeoutError: If the request times out
        JSONDecodeError: If unable to decode the JSON response
    """

    # Use wrapper to track metrics
    @track_api_call(endpoint=endpoint, params=params)
    def _fetch(client, endpoint, params, timeout):
        try:
            # Send the request with proper headers and explicit timeout
            response = client.get(
                endpoint,
                params=params,
                timeout=timeout,
            )

            # Handle rate limiting
            if response.status_code in (418, 429):
                retry_after = int(response.headers.get("retry-after", 60))
                logger.warning(f"Rate limited by API (HTTP {response.status_code}). Waiting {retry_after}s before continuing")
                raise RateLimitError(retry_after=retry_after)

            # Check for HTTP error codes
            if response.status_code != HTTP_OK:
                error_msg = f"HTTP error {response.status_code}: {response.text}"
                logger.warning(f"Error response from {endpoint}: {error_msg}")
                raise HTTPError(response.status_code, error_msg)

            # Parse JSON response with orjson (3-10x faster than stdlib json)
            try:
                data = orjson.loads(response.content)
            except orjson.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON response: {e}")
                raise JSONDecodeError(f"Failed to decode JSON response: {e!s}") from e

            # Check for API error
            if isinstance(data, dict) and "code" in data and data.get("code", 0) != 0:
                error_code = data.get("code")
                error_msg = data.get("msg", "Unknown error")
                logger.warning(f"API error from {endpoint}: {error_code} - {error_msg}")
                raise APIError(error_code, f"API error {error_code}: {error_msg}")

            return data

        except httpx.ConnectError as e:
            logger.error(f"Network connection error: {e}")
            raise NetworkError(f"Connection error: {e!s}") from e
        except httpx.TimeoutException as e:
            logger.error(f"Request timeout: {e}")
            raise RestTimeoutError(f"Request timed out: {e!s}") from e
        except httpx.HTTPError as e:
            # Catch remaining httpx exceptions (after ConnectError, TimeoutException)
            logger.error(f"Request error: {e}")
            raise RestAPIError(f"Request error: {e!s}") from e

    # Call the wrapped function
    return _fetch(client, endpoint, params, timeout)


def log_rest_metrics():
    """Log REST API metrics to the logger."""
    metrics_tracker.log_metrics()


def calculate_chunks(start_ms: int, end_ms: int, interval_ms: int, chunk_size: int, max_chunks: int) -> list[tuple[int, int]]:
    """Calculate chunk boundaries for a time range.

    This is needed because Binance API limits the number of records per request,
    so we need to break large time ranges into smaller chunks.

    Args:
        start_ms: Start time in milliseconds
        end_ms: End time in milliseconds
        interval_ms: Interval duration in milliseconds
        chunk_size: Maximum number of data points per chunk
        max_chunks: Maximum number of chunks to create

    Returns:
        List of (chunk_start_ms, chunk_end_ms) tuples
    """
    # Calculate max time range per request (in milliseconds)
    # This is based on the chunk size limit and interval duration
    max_range_ms = interval_ms * chunk_size

    # Calculate the number of chunks needed
    chunks = []
    current_start = start_ms

    # Initialize a safety counter to prevent infinite loops
    loop_count = 0

    while current_start < end_ms and loop_count < max_chunks:
        # Calculate the end of this chunk
        chunk_end = min(current_start + max_range_ms, end_ms)

        # Add the chunk to our list
        chunks.append((current_start, chunk_end))

        # Move to the next chunk
        current_start = chunk_end

        # Safety counter
        loop_count += 1

    if loop_count >= max_chunks:
        logger.warning(f"Reached maximum chunk limit ({max_chunks}) for time range {start_ms} to {end_ms}")

    return chunks


def validate_request_params(symbol: str, interval: Interval, start_time: datetime, end_time: datetime) -> None:
    """Validate request parameters for debugging.

    Args:
        symbol: Trading pair symbol
        interval: Time interval
        start_time: Start time
        end_time: End time

    Raises:
        ValueError: If parameters are invalid
    """
    # Validate that we have string parameters where needed
    if not isinstance(symbol, str) or not symbol:
        raise ValueError(f"Symbol must be a non-empty string, got {symbol}")

    # Validate time ranges
    if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
        raise ValueError(f"Start and end times must be datetime objects, got start={type(start_time)}, end={type(end_time)}")

    if start_time >= end_time:
        raise ValueError(f"Start time ({start_time}) must be before end time ({end_time})")

    # Validate interval
    if not isinstance(interval, Interval):
        raise ValueError(f"Interval must be an Interval enum, got {type(interval)}")


_INTERVAL_MS: dict[Interval, int] = {
    Interval.SECOND_1: 1_000,
    Interval.MINUTE_1: 60_000,
    Interval.MINUTE_3: 180_000,
    Interval.MINUTE_5: 300_000,
    Interval.MINUTE_15: 900_000,
    Interval.MINUTE_30: 1_800_000,
    Interval.HOUR_1: 3_600_000,
    Interval.HOUR_2: 7_200_000,
    Interval.HOUR_4: 14_400_000,
    Interval.HOUR_6: 21_600_000,
    Interval.HOUR_8: 28_800_000,
    Interval.HOUR_12: 43_200_000,
    Interval.DAY_1: 86_400_000,
    Interval.DAY_3: 259_200_000,
    Interval.WEEK_1: 604_800_000,
    Interval.MONTH_1: 2_592_000_000,
}

_INTERVAL_BY_VALUE: dict[str, Interval] = {i.value: i for i in Interval}


def get_interval_ms(interval: Interval) -> int:
    """Get the interval duration in milliseconds.

    Args:
        interval: Time interval

    Returns:
        Interval duration in milliseconds
    """
    return _INTERVAL_MS.get(interval, 60_000)


def parse_interval_string(interval_str: str, default_interval: Interval = Interval.MINUTE_1) -> Interval:
    """Parse interval string to Interval enum.

    Args:
        interval_str: Interval string (e.g., '1m', '1h')
        default_interval: Default interval to use if parsing fails

    Returns:
        Interval enum
    """
    try:
        # O(1) dict lookup instead of O(n) enum scan
        interval_enum = _INTERVAL_BY_VALUE.get(interval_str)
        if interval_enum is None:
            # Try by enum name if value lookup failed
            try:
                interval_enum = Interval[interval_str.upper()]
            except KeyError as e:
                raise ValueError(f"Invalid interval: {interval_str}") from e
        return interval_enum
    except (ValueError, StopIteration, AttributeError) as e:
        logger.warning(f"Error converting interval string '{interval_str}': {e}")
        return default_interval  # Fall back to default
