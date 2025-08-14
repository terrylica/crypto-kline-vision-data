#!/usr/bin/env python
"""Utilities for REST API client operations.

This module provides common utilities for REST API client operations including:
1. HTTP client creation and configuration
2. Retry logic for API requests
3. Standardized error handling
"""

import json
from datetime import datetime
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_incrementing,
)

from data_source_manager.utils.config import DEFAULT_HTTP_TIMEOUT_SECONDS, HTTP_OK
from data_source_manager.utils.for_core.rest_exceptions import (
    APIError,
    HTTPError,
    JSONDecodeError,
    NetworkError,
    RateLimitError,
    RestAPIError,
    TimeoutError,
)
from data_source_manager.utils.for_core.rest_metrics import metrics_tracker, track_api_call
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import Interval


def create_optimized_client() -> requests.Session:
    """Create an optimized HTTP client for REST API requests.

    Returns:
        HTTP client instance optimized for performance
    """
    session = requests.Session()

    # Configure the session with reasonable defaults
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    )

    return session


@retry(
    stop=stop_after_attempt(3),
    wait=wait_incrementing(start=1, increment=1, max=3),
    retry=retry_if_exception_type((RestAPIError, requests.RequestException, json.JSONDecodeError)),
    before_sleep=lambda retry_state: logger.warning(
        f"Error fetching data (attempt {retry_state.attempt_number}/3): {retry_state.outcome.exception()}"
    ),
)
def fetch_chunk(
    client: requests.Session,
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
        TimeoutError: If the request times out
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
                retry_after = int(response.headers.get("retry-after", 1))
                logger.warning(f"Rate limited by API (HTTP {response.status_code}). Waiting {retry_after}s before continuing")
                raise RateLimitError(retry_after=retry_after)

            # Check for HTTP error codes
            if response.status_code != HTTP_OK:
                error_msg = f"HTTP error {response.status_code}: {response.text}"
                logger.warning(f"Error response from {endpoint}: {error_msg}")
                raise HTTPError(response.status_code, error_msg)

            # Parse JSON response
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON response: {e}")
                raise JSONDecodeError(f"Failed to decode JSON response: {e!s}") from e

            # Check for API error
            if isinstance(data, dict) and "code" in data and data.get("code", 0) != 0:
                error_code = data.get("code")
                error_msg = data.get("msg", "Unknown error")
                logger.warning(f"API error from {endpoint}: {error_code} - {error_msg}")
                raise APIError(error_code, f"API error {error_code}: {error_msg}")

            return data

        except requests.ConnectionError as e:
            logger.error(f"Network connection error: {e}")
            raise NetworkError(f"Connection error: {e!s}") from e
        except requests.Timeout as e:
            logger.error(f"Request timeout: {e}")
            raise TimeoutError(f"Request timed out: {e!s}") from e
        except (requests.RequestException, Exception) as e:
            # Catch any other requests exceptions
            if not isinstance(e, RestAPIError):  # Avoid wrapping our own exceptions
                logger.error(f"Request error: {e}")
                raise RestAPIError(f"Request error: {e!s}") from e
            raise

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


def get_interval_ms(interval: Interval) -> int:
    """Get the interval duration in milliseconds.

    Args:
        interval: Time interval

    Returns:
        Interval duration in milliseconds
    """
    # Map of interval values to milliseconds
    interval_map = {
        Interval.SECOND_1: 1000,  # 1 second
        Interval.MINUTE_1: 60 * 1000,  # 1 minute
        Interval.MINUTE_3: 3 * 60 * 1000,  # 3 minutes
        Interval.MINUTE_5: 5 * 60 * 1000,  # 5 minutes
        Interval.MINUTE_15: 15 * 60 * 1000,  # 15 minutes
        Interval.MINUTE_30: 30 * 60 * 1000,  # 30 minutes
        Interval.HOUR_1: 60 * 60 * 1000,  # 1 hour
        Interval.HOUR_2: 2 * 60 * 60 * 1000,  # 2 hours
        Interval.HOUR_4: 4 * 60 * 60 * 1000,  # 4 hours
        Interval.HOUR_6: 6 * 60 * 60 * 1000,  # 6 hours
        Interval.HOUR_8: 8 * 60 * 60 * 1000,  # 8 hours
        Interval.HOUR_12: 12 * 60 * 60 * 1000,  # 12 hours
        Interval.DAY_1: 24 * 60 * 60 * 1000,  # 1 day
        Interval.DAY_3: 3 * 24 * 60 * 60 * 1000,  # 3 days
        Interval.WEEK_1: 7 * 24 * 60 * 60 * 1000,  # 1 week
        Interval.MONTH_1: 30 * 24 * 60 * 60 * 1000,  # 1 month (approximation)
    }

    # Return the interval duration
    return interval_map.get(interval, 60 * 1000)  # Default to 1 minute if unknown


def parse_interval_string(interval_str: str, default_interval: Interval = Interval.MINUTE_1) -> Interval:
    """Parse interval string to Interval enum.

    Args:
        interval_str: Interval string (e.g., '1m', '1h')
        default_interval: Default interval to use if parsing fails

    Returns:
        Interval enum
    """
    try:
        # Try direct value lookup first
        interval_enum = next((i for i in Interval if i.value == interval_str), None)
        if interval_enum is None:
            # Try by enum name if value lookup failed
            try:
                interval_enum = Interval[interval_str.upper()]
            except KeyError as e:
                raise ValueError(f"Invalid interval: {interval_str}") from e
        return interval_enum
    except Exception as e:
        logger.warning(f"Error converting interval string '{interval_str}': {e}")
        return default_interval  # Fall back to default
