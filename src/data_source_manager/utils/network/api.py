#!/usr/bin/env python
"""API request handling and connectivity testing.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from network_utils.py for modularity
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_incrementing,
)

from data_source_manager.utils.config import (
    API_MAX_RETRIES,
    API_RETRY_DELAY,
    API_TIMEOUT,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    HTTP_ERROR_CODE_THRESHOLD,
    HTTP_OK,
)
from data_source_manager.utils.for_core.rest_exceptions import RateLimitError
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.network.client_factory import create_client, safely_close_client

__all__ = [
    "make_api_request",
    "test_connectivity",
]


@retry(
    stop=stop_after_attempt(API_MAX_RETRIES),
    wait=wait_incrementing(start=API_RETRY_DELAY, increment=API_RETRY_DELAY, max=API_RETRY_DELAY * 3),
    retry=retry_if_exception_type((json.JSONDecodeError, TimeoutError)),
    before_sleep=lambda retry_state: logger.warning(
        f"API request failed (attempt {retry_state.attempt_number}/{API_MAX_RETRIES}): {retry_state.outcome.exception()} - "
        f"waiting {retry_state.attempt_number * API_RETRY_DELAY} seconds"
    ),
)
def make_api_request(
    client: Any,
    url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
    method: str = "GET",
    json_data: dict[str, Any] | None = None,
    timeout: float | None = None,
    raise_for_status: bool = True,
) -> tuple[int, dict[str, Any]]:
    """Make an API request with retry logic and error handling.

    Args:
        client: HTTP client
        url: URL to make the request to
        headers: Optional headers to include in the request
        params: Optional query parameters
        method: HTTP method (GET, POST, etc.)
        json_data: Optional JSON data for POST/PUT requests
        timeout: Request timeout in seconds (overrides client timeout)
        raise_for_status: Whether to raise an exception for HTTP errors

    Returns:
        Tuple of (status_code, response_data)

    Raises:
        Exception: If HTTP error occurs and raise_for_status is True
    """
    headers = headers or {}
    params = params or {}
    timeout_value = timeout or DEFAULT_HTTP_TIMEOUT_SECONDS

    # Use httpx client
    if method == "GET":
        response = client.get(url, headers=headers, params=params, timeout=timeout_value)
    elif method == "POST":
        response = client.post(
            url,
            headers=headers,
            params=params,
            json=json_data,
            timeout=timeout_value,
        )
    else:
        response = client.request(
            method,
            url,
            headers=headers,
            params=params,
            json=json_data,
            timeout=timeout_value,
        )

    status_code = response.status_code

    # Special handling for rate limiting — raise RateLimitError without sleeping.
    # Previously this did time.sleep(retry_after) + raise TimeoutError, causing a
    # double-wait when tenacity also waited before its retry. (GitHub Issue #18, P2.3)
    if status_code in (418, 429):
        retry_after = int(response.headers.get("retry-after", 60))
        logger.warning(f"Rate limited by API (HTTP {status_code}). Retry after {retry_after}s")
        raise RateLimitError(retry_after=retry_after)

    if raise_for_status and status_code >= HTTP_ERROR_CODE_THRESHOLD:
        raise Exception(f"HTTP error: {status_code} - {response.text}")

    try:
        if response.headers.get("content-type", "").startswith("application/json"):
            response_data = json.loads(response.text)
        else:
            response_data = {"text": response.text}
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse JSON response: {e}")
        # This will be caught by tenacity and retried
        raise

    # Successfully processed the response
    return status_code, response_data


def test_connectivity(
    client: Any = None,
    url: str = "https://data.binance.vision/",
    timeout: float = API_TIMEOUT,
    retry_count: int = API_MAX_RETRIES - 1,
) -> bool:
    """Test connectivity to a URL.

    Args:
        client: HTTP client to use (creates a new one if None)
        url: URL to test
        timeout: Request timeout in seconds
        retry_count: Number of retry attempts

    Returns:
        True if connection is successful, False otherwise
    """
    client_created = False
    if client is None:
        client = create_client(timeout=timeout)
        client_created = True

    try:
        for attempt in range(retry_count + 1):
            try:
                # Try to connect
                response = client.get(url, timeout=timeout)
                if response.status_code == HTTP_OK:
                    logger.info(f"Successfully connected to {url}")
                    return True
                logger.warning(f"Connection test failed with status code {response.status_code}")
                if attempt < retry_count:
                    wait_time = 1 + attempt  # 1s, 2s, etc.
                    logger.info(f"Retrying in {wait_time}s... (attempt {attempt + 1}/{retry_count})")
                    time.sleep(wait_time)
            except (httpx.HTTPError, TimeoutError, ConnectionError) as e:
                logger.warning(f"Connection test attempt {attempt + 1} failed: {e}")
                if attempt < retry_count:
                    wait_time = 1 + attempt
                    logger.info(f"Retrying in {wait_time}s... (attempt {attempt + 1}/{retry_count})")
                    time.sleep(wait_time)

        logger.error(f"Failed to connect to {url} after {retry_count + 1} attempts")
        return False
    finally:
        if client_created and client:
            safely_close_client(client)
