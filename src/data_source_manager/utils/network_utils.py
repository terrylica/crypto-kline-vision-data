#!/usr/bin/env python
"""Network utilities for HTTP requests, downloads, and connectivity testing.

This module provides:
1. HTTP client creation with standardized configuration using httpx
2. Download functions (both single and concurrent) with optimized handling
3. Connection testing and validation
4. API request helpers with retry logic and response handling

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""

import csv
import json
import platform
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
)

import attrs  # Add this import

# Import httpx for HTTP client implementation
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
    HTTP_NOT_FOUND,
    HTTP_OK,
    MAXIMUM_CONCURRENT_DOWNLOADS,
    MEDIUM_BATCH_SIZE,
    SMALL_BATCH_SIZE,
)
from data_source_manager.utils.loguru_setup import logger

# Define a generic Client type for HTTP clients
Client = httpx.Client


# ----- HTTP Client Factory Functions -----


def create_httpx_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 50,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    """Create an httpx Client for high-performance HTTP requests.

    Args:
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional headers to include in all requests
        **kwargs: Additional keyword arguments to pass to Client

    Returns:
        httpx.Client: An initialized HTTP client
    """
    try:
        from httpx import Client, Limits, Timeout

        # Log the kwargs being passed to identify issues
        logger.debug(f"Creating httpx Client with kwargs: {kwargs}")

        # Remove known incompatible parameters
        if "impersonate" in kwargs:
            logger.warning("Removing unsupported 'impersonate' parameter from httpx client creation")
            kwargs.pop("impersonate")

        # Set up timeout with all required parameters defined
        # The error was "httpx.Timeout must either include a default, or set all four parameters explicitly"
        timeout_obj = Timeout(connect=min(timeout, 10.0), read=timeout, write=timeout, pool=timeout)

        # Set up connection limits
        limits = Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_connections // 2,
        )

        # Create default headers if none provided
        if headers is None:
            headers = {
                "User-Agent": f"BinanceDataServices/0.1 Python/{platform.python_version()}",
                "Accept": "application/json",
            }

        # Create the client
        client = Client(
            timeout=timeout_obj,
            limits=limits,
            headers=headers,
            follow_redirects=True,
            **kwargs,
        )

        logger.debug(f"Created httpx Client with timeout={timeout}s, max_connections={max_connections}")
        return client

    except ImportError:
        logger.error("httpx is not installed. To use this function, install httpx: pip install httpx")
        raise


def create_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int | None = None,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    """Create a client for making HTTP requests.

    This function provides a unified interface for creating HTTP clients
    using httpx, which provides better stability and compatibility.

    Args:
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional headers to include in all requests
        **kwargs: Additional keyword arguments to pass to the client

    Returns:
        An initialized async HTTP client
    """
    if max_connections is None:
        max_connections = 50  # Default to 50 connections

    # Create httpx client
    try:
        logger.debug(f"Creating httpx client with {len(kwargs)} additional parameters")
        return create_httpx_client(timeout, max_connections, headers, **kwargs)
    except ImportError as e:
        logger.error("httpx is not available. Please install httpx: pip install httpx>=0.24.0")
        raise ImportError("httpx is required but not available. Install with: pip install httpx>=0.24.0") from e


def create_legacy_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 50,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> Any:
    """Deprecated function that now creates an httpx client instead.

    This function is maintained for backwards compatibility only. All code should
    use create_httpx_client or create_client directly.

    Args:
        timeout: Total timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers
        **kwargs: Additional client configuration options

    Returns:
        httpx.AsyncClient configured with the provided settings
    """
    logger.warning("create_legacy_client is deprecated. Please update your code to use create_httpx_client or create_client directly.")

    # Remove legacy-specific parameters
    if "impersonate" in kwargs:
        logger.warning("Parameter 'impersonate' is not supported by httpx and will be ignored")
        kwargs.pop("impersonate")

    return create_httpx_client(timeout=timeout, max_connections=max_connections, headers=headers, **kwargs)


# ----- Download Handling -----


class DownloadException(Exception):
    """Base exception for download-related errors."""


class DownloadStalledException(DownloadException):
    """Raised when a download appears to have stalled."""


class RateLimitException(DownloadException):
    """Raised when rate limits are hit during downloads."""


@attrs.define
class DownloadProgressTracker:
    """Tracks download progress and detects stalled downloads."""

    # Define attributes with default values
    total_size: int | None = attrs.field(default=None)
    check_interval: int = attrs.field(default=5)

    # Internal state attributes
    bytes_received: int = attrs.field(init=False, default=0)
    last_bytes: int = attrs.field(init=False, default=0)
    start_time: float = attrs.field(init=False, factory=time.monotonic)
    last_progress_time: float = attrs.field(init=False, factory=time.monotonic)

    def __attrs_post_init__(self):
        """Log initial state after initialization."""
        logger.debug(f"Download progress tracker initialized. Total size: {self.total_size or 'unknown'} bytes")

    def update(self, bytes_chunk: int) -> bool:
        """Update progress with newly received bytes.

        Args:
            bytes_chunk: Number of bytes received in this update

        Returns:
            False if download appears stalled, True otherwise
        """
        self.bytes_received += bytes_chunk
        current_time = time.monotonic()
        elapsed = current_time - self.start_time

        # Calculate speed
        speed = self.bytes_received / elapsed if elapsed > 0 else 0

        # Check if progress is stalled
        if current_time - self.last_progress_time >= self.check_interval:
            # If no new bytes since last check, we might be stalled
            if self.bytes_received == self.last_bytes:
                logger.warning(f"Download appears stalled: no progress for {self.check_interval}s")
                return False

            # Log progress
            percent = f"{(self.bytes_received / self.total_size) * 100:.1f}%" if self.total_size else "unknown"
            logger.debug(f"Download progress: {self.bytes_received} bytes ({percent}) at {speed:.1f} bytes/s")

            # Update progress tracking state
            self.last_progress_time = current_time
            self.last_bytes = self.bytes_received

        return True


@attrs.define
class DownloadHandler:
    """Handles HTTP downloads with retry logic and progress tracking."""

    client: Any = attrs.field(default=None)
    timeout: float = attrs.field(default=DEFAULT_HTTP_TIMEOUT_SECONDS)
    _client_is_external: bool = attrs.field(init=False)

    def __attrs_post_init__(self):
        """Initialize state after creation."""
        self._client_is_external = self.client is not None

    def __enter__(self):
        """Enter context manager."""
        if not self.client:
            self.client = create_client(timeout=self.timeout)
            self._client_is_external = False
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit method."""
        self._close_client()

    @retry(
        stop=stop_after_attempt(API_MAX_RETRIES),
        wait=wait_incrementing(start=API_RETRY_DELAY, increment=API_RETRY_DELAY, max=API_RETRY_DELAY * 3),
        retry=retry_if_exception_type(
            (
                DownloadStalledException,
                RateLimitException,
                TimeoutError,
                ConnectionError,
            )
        ),
        before_sleep=lambda retry_state: logger.warning(
            f"Retry attempt {retry_state.attempt_number}/{API_MAX_RETRIES} for download after error: {retry_state.outcome.exception()} - "
            f"waiting {retry_state.attempt_number * API_RETRY_DELAY} seconds"
        ),
    )
    def download_file(
        self,
        url: str,
        local_path: Path,
        timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
        expected_size: int | None = None,
    ) -> bool:
        """Download a file with retry logic, progress tracking and validation.

        Args:
            url: URL to download
            local_path: Local path to save the file
            timeout: Download timeout in seconds
            expected_size: Expected file size for validation

        Returns:
            True on success, False on failure
        """
        # Ensure local directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Create a temporary client if not provided
        client_created = False
        if not self.client:
            self.client = create_client(timeout=self.timeout)
            client_created = True

        try:
            logger.debug(f"Starting download from {url} to {local_path}")

            # Perform download with the client
            response = self.client.get(url, timeout=timeout)

            # Check status code
            if response.status_code != HTTP_OK:
                # Use warning instead of error for 404 (Not Found) status
                if response.status_code == HTTP_NOT_FOUND:
                    # File doesn't exist - this is often expected when checking for file existence
                    # Extract filename from URL for more informative message
                    from urllib.parse import urlparse

                    path = urlparse(url).path
                    filename = path.split("/")[-1] if "/" in path else path

                    logger.warning(f"File not found (404): {filename}")
                    if "NoSuchKey" in response.text:
                        # This is a standard AWS S3 response for missing files
                        logger.debug(f"AWS S3 NoSuchKey: {url}")
                else:
                    # For other non-200 status codes, still log as error
                    logger.error(f"Download failed with status code {response.status_code}: {response.text}")

                return False

            # Get content and write to file
            content = response.content
            local_path.write_bytes(content)

            # Verify file size if expected_size is provided
            if expected_size is not None and local_path.stat().st_size != expected_size:
                logger.error(f"File size mismatch: expected {expected_size}, got {local_path.stat().st_size}")
                return False

            logger.debug(f"Download successful: {url} -> {local_path} ({len(content)} bytes)")
            return True

        except (httpx.HTTPError, OSError, TimeoutError) as e:
            logger.error(f"Error downloading {url}: {e!s}")
            return False

        finally:
            # Clean up client if we created it
            if client_created and self.client:
                safely_close_client(self.client)
                self.client = None

    def _close_client(self):
        """Safely close the HTTP client if we own it."""
        if self.client and not self._client_is_external:
            safely_close_client(self.client)
            self.client = None


# ----- Batch Download Handling -----


def download_files_concurrently(
    client,
    urls: list[str],
    local_paths: list[Path],
    max_concurrent: int = MAXIMUM_CONCURRENT_DOWNLOADS,
    **download_kwargs: Any,
) -> list[bool]:
    """Download multiple files concurrently using ThreadPoolExecutor.

    Args:
        client: HTTP client
        urls: List of URLs to download
        local_paths: List of local paths to save files to
        max_concurrent: Maximum number of concurrent downloads
        **download_kwargs: Additional keyword arguments to pass to download_file

    Returns:
        List of booleans indicating success or failure for each download
    """
    if len(urls) != len(local_paths):
        logger.error(f"URL and path lists must have same length. Got {len(urls)} URLs and {len(local_paths)} paths.")
        return [False] * max(len(urls), len(local_paths))

    # Create download handler
    handler = DownloadHandler(client=client)

    # Dynamically adjust concurrency based on batch size
    batch_size = len(urls)
    adjusted_concurrency = max_concurrent

    if batch_size <= SMALL_BATCH_SIZE:
        # Small batch optimization
        adjusted_concurrency = min(SMALL_BATCH_SIZE, max_concurrent)
    elif batch_size <= MEDIUM_BATCH_SIZE:
        # Medium batch optimization (optimal concurrency)
        adjusted_concurrency = min(MEDIUM_BATCH_SIZE, max_concurrent)
    else:
        # Large batch optimization
        adjusted_concurrency = min(100, max_concurrent)

    if adjusted_concurrency != max_concurrent:
        logger.debug(f"Adjusting concurrency to {adjusted_concurrency} for {batch_size} files")

    def download_worker(url: str, path: Path) -> bool:
        try:
            return handler.download_file(url, path, **download_kwargs)
        except (DownloadException, httpx.HTTPError, OSError, TimeoutError) as e:
            logger.error(f"Error downloading {url}: {e!s}")
            return False

    results = []

    # Use ThreadPoolExecutor for concurrency
    with ThreadPoolExecutor(max_workers=adjusted_concurrency) as executor:
        # Submit all download tasks
        future_to_index = {}
        for i, (url, path) in enumerate(zip(urls, local_paths, strict=False)):
            future = executor.submit(download_worker, url, path)
            future_to_index[future] = i

        # Create a results list with the same length as urls
        results = [False] * len(urls)

        # Process results as they complete
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                results[idx] = future.result()
            except (DownloadException, httpx.HTTPError, OSError, TimeoutError) as e:
                logger.error(f"Download task raised exception: {e}")
                results[idx] = False

    return results


# ----- API Request Handling -----


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
    client,
    url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
    method: str = "GET",
    json_data: dict | None = None,
    timeout: float | None = None,
    raise_for_status: bool = True,
) -> tuple[int, dict]:
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

    # Special handling for rate limiting
    if status_code in (418, 429):
        retry_after = int(response.headers.get("retry-after", 1))
        logger.warning(f"Rate limited by API (HTTP {status_code}). Waiting {retry_after}s before continuing")
        time.sleep(retry_after)
        # Re-raise to trigger tenacity retry
        raise TimeoutError(f"Rate limited (HTTP {status_code})")

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


@attrs.define
class VisionDownloadManager:
    """Handles downloading Vision data files with validation and processing."""

    client: Any = attrs.field()
    symbol: str = attrs.field()
    interval: str = attrs.field()
    market_type: str = attrs.field(default="spot")

    # Non-init fields with default values
    download_handler: DownloadHandler = attrs.field(init=False)
    _external_client: bool = attrs.field(init=False)
    _current_tasks: list = attrs.field(factory=list, init=False)
    _temp_files: list[Path] = attrs.field(factory=list, init=False)

    def __attrs_post_init__(self):
        """Initialize state after creation."""
        self.download_handler = DownloadHandler(self.client, timeout=API_TIMEOUT)
        self._external_client = self.client is not None

    def _cleanup_resources(self):
        """Clean up resources used by the download manager.

        This ensures proper release of HTTP client and any other resources
        to prevent memory leaks or hanging connections.
        """
        logger.debug("[ProgressIndicator] VisionDownloadManager: Starting resource cleanup")
        cleanup_errors = []

        # Step 1: Cancel any ongoing download tasks
        try:
            if hasattr(self, "_current_tasks") and self._current_tasks:
                logger.debug("[ProgressIndicator] VisionDownloadManager: Cancelling remaining download tasks")
                # For synchronous tasks, we can't really cancel them, but we can clear the list
                self._current_tasks = []
        except AttributeError as e:
            error_msg = f"Error cancelling download tasks: {e}"
            logger.warning(error_msg)
            cleanup_errors.append(error_msg)

        # Step 2: Safely close the HTTP client
        try:
            # Only attempt to close if we own the client
            if not self._external_client and hasattr(self, "client") and self.client is not None:
                logger.debug("[ProgressIndicator] VisionDownloadManager: Safely closing HTTP client")
                safely_close_client(self.client)
                self.client = None
                logger.debug("[ProgressIndicator] VisionDownloadManager: HTTP client closed")
        except (AttributeError, OSError) as e:
            error_msg = f"Error closing HTTP client: {e}"
            logger.warning(error_msg)
            cleanup_errors.append(error_msg)

        # Step 3: Clean up any temporary files
        try:
            if hasattr(self, "_temp_files") and self._temp_files:
                logger.debug(f"[ProgressIndicator] VisionDownloadManager: Cleaning up {len(self._temp_files)} temporary files")
                for temp_file in self._temp_files:
                    try:
                        if hasattr(temp_file, "exists") and temp_file.exists():
                            temp_file.unlink()
                    except OSError as e:
                        logger.debug(f"Error removing temp file {temp_file}: {e}")
                self._temp_files = []
                logger.debug("[ProgressIndicator] VisionDownloadManager: Temporary files cleaned up")
        except (AttributeError, OSError) as e:
            error_msg = f"Error cleaning up temporary files: {e}"
            logger.warning(error_msg)
            cleanup_errors.append(error_msg)

        if cleanup_errors:
            error_summary = "; ".join(cleanup_errors)
            logger.warning(f"[ProgressIndicator] VisionDownloadManager: Resource cleanup completed with warnings: {error_summary}")
        else:
            logger.debug("[ProgressIndicator] VisionDownloadManager: Resource cleanup completed successfully")

    def download_date(self, date: datetime) -> list[list] | None:
        """Download data for a specific date from Binance Vision API.

        Args:
            date: Date to download data for

        Returns:
            List of kline data points if successful, None otherwise
        """
        try:
            # Construct URL for the date
            url_template = "https://data.binance.vision/data/{market_type}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
            url = url_template.format(
                market_type=self.market_type,
                symbol=self.symbol,
                interval=self.interval,
                date=date.strftime("%Y-%m-%d"),
            )

            # Create a temp file for the download
            import tempfile
            from pathlib import Path

            temp_dir = tempfile.gettempdir()
            temp_file = Path(temp_dir) / f"{self.symbol}_{self.interval}_{date.strftime('%Y-%m-%d')}.zip"

            # Track the temp file for cleanup
            self._temp_files.append(temp_file)

            # Download the file
            success = self.download_handler.download_file(url, temp_file)
            if not success:
                logger.warning(f"Failed to download data for {date}")
                return None

            # Process the zip file
            from io import StringIO

            data = []
            with zipfile.ZipFile(temp_file, "r") as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
                if not csv_files:
                    logger.warning(f"No CSV file found in downloaded zip for {date}")
                    return None

                # Extract and read the CSV data
                with zip_ref.open(csv_files[0]) as csv_file:
                    content = csv_file.read().decode("utf-8")
                    reader = csv.reader(StringIO(content))
                    data = list(reader)

            # Clean up the temp file
            try:
                temp_file.unlink()
                self._temp_files.remove(temp_file)
            except OSError as e:
                logger.debug(f"Error removing temp file {temp_file}: {e}")

            return data
        except (httpx.HTTPError, OSError, zipfile.BadZipFile, csv.Error) as e:
            logger.error(f"Error downloading data for {date}: {e}")
            return None

    def _download_file(self, url: str, local_path: Path) -> bool:
        """Download a file using the download handler.

        Args:
            url: URL to download
            local_path: Path to save the file to

        Returns:
            True if successful, False otherwise
        """
        return self.download_handler.download_file(url, local_path)

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Exit context manager. Clean up resources."""
        self._cleanup_resources()


def safely_close_client(client):
    """Safely close an HTTP client, handling any exceptions.

    Args:
        client: HTTP client to close
    """
    if client is None:
        return

    try:
        if hasattr(client, "close") and callable(client.close):
            client.close()
            logger.debug("HTTP client closed successfully")
    except OSError as e:
        logger.warning(f"Error while closing HTTP client: {e}")


def test_connectivity(
    client=None,
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
