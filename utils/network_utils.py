#!/usr/bin/env python
"""Network utilities for HTTP requests, downloads, and connectivity testing.

This module provides:
1. HTTP client creation with standardized configuration using httpx
2. Download functions (both single and concurrent) with optimized handling
3. Connection testing and validation
4. API request helpers with retry logic and response handling
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Dict,
    Any,
    Optional,
    List,
    Tuple,
)
import os
import json
import platform
import gc

# Import curl_cffi for HTTP client implementation
# from curl_cffi.requests import AsyncSession

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from utils.config import (
    DEFAULT_HTTP_TIMEOUT_SECONDS,
)
from utils.logger_setup import logger

# Import httpx for HTTP client implementation
import httpx

# For backwards compatibility, define AsyncSession as an alias to httpx.AsyncClient
AsyncSession = httpx.AsyncClient


# ----- HTTP Client Factory Functions -----


def create_httpx_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 50,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Any:
    """Create an httpx AsyncClient for high-performance HTTP requests.

    Args:
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional headers to include in all requests
        **kwargs: Additional keyword arguments to pass to AsyncClient

    Returns:
        httpx.AsyncClient: An initialized async HTTP client
    """
    try:
        from httpx import AsyncClient, Limits, Timeout

        # Log the kwargs being passed to identify issues
        logger.debug(f"Creating httpx AsyncClient with kwargs: {kwargs}")

        # Remove known incompatible parameters
        if "impersonate" in kwargs:
            logger.warning(
                "Removing unsupported 'impersonate' parameter from httpx client creation"
            )
            kwargs.pop("impersonate")

        # Set up timeout with all required parameters defined
        # The error was "httpx.Timeout must either include a default, or set all four parameters explicitly"
        timeout_obj = Timeout(
            connect=min(timeout, 10.0), read=timeout, write=timeout, pool=timeout
        )

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
        client = AsyncClient(
            timeout=timeout_obj,
            limits=limits,
            headers=headers,
            http2=True,  # Enable HTTP/2 for improved performance
            follow_redirects=True,
            **kwargs,
        )

        logger.debug(
            f"Created httpx AsyncClient with timeout={timeout}s, max_connections={max_connections}"
        )
        return client

    except ImportError:
        logger.error(
            "httpx is not installed. To use this function, install httpx: pip install httpx"
        )
        raise


def create_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: Optional[int] = None,
    headers: Optional[Dict[str, str]] = None,
    use_httpx: bool = True,  # Default to using httpx now
    **kwargs: Any,
) -> Any:
    """Create a client for making HTTP requests.

    This function provides a unified interface for creating HTTP clients
    using httpx, which provides better stability and compatibility.

    Args:
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional headers to include in all requests
        use_httpx: Ignored parameter, maintained for backward compatibility. Always uses httpx.
        **kwargs: Additional keyword arguments to pass to the client

    Returns:
        An initialized async HTTP client
    """
    if max_connections is None:
        max_connections = 50  # Default to 50 connections

    if not use_httpx:
        logger.warning(
            "The use_httpx parameter is deprecated - always using httpx for stability"
        )

    # Create httpx client
    try:
        logger.debug(f"Creating httpx client with {len(kwargs)} additional parameters")
        return create_httpx_client(timeout, max_connections, headers, **kwargs)
    except ImportError:
        logger.error(
            "httpx is not available. Please install httpx: pip install httpx>=0.24.0"
        )
        raise ImportError(
            "httpx is required but not available. Install with: pip install httpx>=0.24.0"
        )


def create_curl_cffi_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 50,
    headers: Optional[Dict[str, str]] = None,
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
    logger.warning(
        "create_curl_cffi_client is deprecated and now redirects to create_httpx_client. "
        "Please update your code to use create_httpx_client or create_client directly."
    )

    # Remove curl_cffi-specific parameters
    if "impersonate" in kwargs:
        logger.warning(
            "Parameter 'impersonate' is not supported by httpx and will be ignored"
        )
        kwargs.pop("impersonate")

    return create_httpx_client(
        timeout=timeout, max_connections=max_connections, headers=headers, **kwargs
    )


# ----- Download Handling -----


class DownloadException(Exception):
    """Base class for download-related exceptions."""


class DownloadStalledException(DownloadException):
    """Raised when download progress stalls."""


class RateLimitException(DownloadException):
    """Raised when rate limited by the server."""


class DownloadProgressTracker:
    """Tracks download progress and detects stalled downloads."""

    def __init__(self, total_size: Optional[int] = None, check_interval: int = 5):
        """Initialize progress tracker.

        Args:
            total_size: Expected total size in bytes, if known
            check_interval: How often to check progress in seconds
        """
        self.start_time = time.monotonic()
        self.last_progress_time = self.start_time
        self.bytes_received = 0
        self.total_size = total_size
        self.last_bytes = 0
        self.check_interval = check_interval

        # Log initial state
        logger.debug(
            f"Download progress tracker initialized. Total size: {total_size or 'unknown'} bytes"
        )

    def update(self, url: str, bytes_chunk: int) -> bool:
        """Update progress with newly received bytes.

        Args:
            url: URL of the download
            bytes_chunk: Number of bytes received in this update

        Returns:
            False if download appears stalled, True otherwise
        """
        self.bytes_received += bytes_chunk
        current_time = time.monotonic()
        elapsed = current_time - self.start_time

        # Calculate speed
        if elapsed > 0:
            speed = self.bytes_received / elapsed
        else:
            speed = 0

        # Check if progress is stalled
        if current_time - self.last_progress_time >= self.check_interval:
            # If no new bytes since last check, we might be stalled
            if self.bytes_received == self.last_bytes:
                logger.warning(
                    f"Download appears stalled: no progress for {self.check_interval}s"
                )
                return False

            # Log progress
            percent = (
                f"{(self.bytes_received / self.total_size) * 100:.1f}%"
                if self.total_size
                else "unknown"
            )
            logger.debug(
                f"Download progress: {self.bytes_received} bytes "
                f"({percent}) at {speed:.1f} bytes/s"
            )

            # Update progress tracking state
            self.last_progress_time = current_time
            self.last_bytes = self.bytes_received

        return True


class DownloadHandler:
    """Handles HTTP downloads with retry logic and progress tracking."""

    def __init__(
        self,
        client: Optional[AsyncSession] = None,
        max_retries: int = 3,
        min_wait: int = 1,
        max_wait: int = 60,
        timeout: float = 60.0,
    ):
        """Initialize download handler.

        Args:
            client: HTTP client (httpx.AsyncClient recommended)
            max_retries: Maximum number of retry attempts
            min_wait: Minimum wait time between retries in seconds
            max_wait: Maximum wait time between retries in seconds
            timeout: Download timeout in seconds
        """
        self.client = client
        self.max_retries = max_retries
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.timeout = timeout
        self._client_is_external = client is not None

    async def __aenter__(self):
        """Enter async context manager."""
        if not self.client:
            self.client = create_client(timeout=self.timeout)
            self._client_is_external = False
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        """Exit async context manager."""
        if self.client and not self._client_is_external:
            await safely_close_client(self.client)
            self.client = None

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(
            (
                DownloadStalledException,
                RateLimitException,
                asyncio.TimeoutError,
                ConnectionError,
            )
        ),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def download_file(
        self,
        url: str,
        local_path: Path,
        timeout: float = 60.0,
        _verify_ssl: bool = True,
        expected_size: Optional[int] = None,
        _stall_timeout: int = 30,
    ) -> bool:
        """Download a file with retry logic, progress tracking and validation.

        Args:
            url: URL to download
            local_path: Local path to save the file
            timeout: Download timeout in seconds
            _verify_ssl: Whether to verify SSL certificates (unused)
            expected_size: Expected file size for validation
            _stall_timeout: Time in seconds before considering download stalled (unused)

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
            # Create progress tracker
            tracker = DownloadProgressTracker(total_size=expected_size)

            logger.debug(f"Starting download from {url} to {local_path}")

            # Perform download with the client
            response = await self.client.get(url, timeout=timeout)

            # Check status code
            if response.status_code != 200:
                # Use warning instead of error for 404 (Not Found) status
                if response.status_code == 404:
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
                    logger.error(
                        f"Download failed with status code {response.status_code}: {response.text}"
                    )

                return False

            # Get content and write to file
            content = response.content
            local_path.write_bytes(content)

            # Verify file size if expected_size is provided
            if expected_size is not None and local_path.stat().st_size != expected_size:
                logger.error(
                    f"File size mismatch: expected {expected_size}, got {local_path.stat().st_size}"
                )
                return False

            logger.debug(
                f"Download successful: {url} -> {local_path} ({len(content)} bytes)"
            )
            return True

        except Exception as e:
            logger.error(f"Error downloading {url}: {str(e)}")
            return False

        finally:
            # Clean up client if we created it
            if client_created and self.client:
                await safely_close_client(self.client)
                self.client = None


# ----- Batch Download Handling -----


async def download_files_concurrently(
    client: AsyncSession,
    urls: List[str],
    local_paths: List[Path],
    max_concurrent: int = 50,
    **download_kwargs: Any,
) -> List[bool]:
    """Download multiple files concurrently using httpx.AsyncClient.

    Args:
        client: HTTP client (httpx.AsyncClient recommended)
        urls: List of URLs to download
        local_paths: List of local paths to save files to
        max_concurrent: Maximum number of concurrent downloads
        **download_kwargs: Additional keyword arguments to pass to download_file

    Returns:
        List of booleans indicating success or failure for each download
    """
    if len(urls) != len(local_paths):
        logger.error(
            f"URL and path lists must have same length. "
            f"Got {len(urls)} URLs and {len(local_paths)} paths."
        )
        return [False] * max(len(urls), len(local_paths))

    # Create download handler
    handler = DownloadHandler(client=client)

    # Dynamically adjust concurrency based on batch size
    batch_size = len(urls)
    adjusted_concurrency = max_concurrent

    if batch_size <= 10:
        # Small batch optimization
        adjusted_concurrency = min(10, max_concurrent)
    elif batch_size <= 50:
        # Medium batch optimization (optimal concurrency)
        adjusted_concurrency = min(50, max_concurrent)
    else:
        # Large batch optimization
        adjusted_concurrency = min(100, max_concurrent)

    if adjusted_concurrency != max_concurrent:
        logger.debug(
            f"Adjusting concurrency to {adjusted_concurrency} for {batch_size} files"
        )

    # Set up semaphore for concurrency control
    semaphore = asyncio.Semaphore(adjusted_concurrency)

    async def download_with_semaphore(url: str, path: Path) -> bool:
        async with semaphore:
            try:
                return await handler.download_file(url, path, **download_kwargs)
            except Exception as e:
                logger.error(f"Error downloading {url}: {str(e)}")
                return False

    # Create tasks for all downloads
    tasks = [
        asyncio.create_task(download_with_semaphore(url, path))
        for url, path in zip(urls, local_paths)
    ]

    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks)

    return results


# ----- API Request Handling -----


async def make_api_request(
    client: AsyncSession,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    method: str = "GET",
    json_data: Optional[Dict] = None,
    timeout: Optional[float] = None,
    retries: int = 3,
    retry_delay: float = 1.0,
    raise_for_status: bool = True,
) -> Tuple[int, Dict]:
    """Make an API request with retry logic and error handling.

    Args:
        client: HTTP client (httpx.AsyncClient recommended)
        url: URL to make the request to
        headers: Optional headers to include in the request
        params: Optional query parameters
        method: HTTP method (GET, POST, etc.)
        json_data: Optional JSON data for POST/PUT requests
        timeout: Request timeout in seconds (overrides client timeout)
        retries: Number of retry attempts
        retry_delay: Delay between retries in seconds
        raise_for_status: Whether to raise an exception for HTTP errors

    Returns:
        Tuple of (status_code, response_data)
    """
    headers = headers or {}
    params = params or {}
    timeout_value = timeout or DEFAULT_HTTP_TIMEOUT_SECONDS

    attempt = 0
    last_status = None
    last_response_data = None

    while attempt < retries:
        try:
            # Use curl_cffi client
            if method == "GET":
                response = await client.get(
                    url, headers=headers, params=params, timeout=timeout_value
                )
            elif method == "POST":
                response = await client.post(
                    url,
                    headers=headers,
                    params=params,
                    json=json_data,
                    timeout=timeout_value,
                )
            else:
                response = await client.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_data,
                    timeout=timeout_value,
                )

            status_code = response.status_code

            # Check for rate limiting
            if status_code in (418, 429):
                retry_after = int(response.headers.get("retry-after", retry_delay))
                logger.warning(
                    f"Rate limited by API (HTTP {status_code}). Retry after {retry_after}s ({attempt+1}/{retries})"
                )
                await asyncio.sleep(retry_after)
                attempt += 1
                last_status = status_code
                last_response_data = {"error": f"Rate limited (HTTP {status_code})"}
                continue

            if raise_for_status and status_code >= 400:
                raise Exception(f"HTTP error: {status_code} - {response.text}")

            try:
                if response.headers.get("content-type", "").startswith(
                    "application/json"
                ):
                    response_data = json.loads(response.text)
                else:
                    response_data = {"text": response.text}
            except json.JSONDecodeError:
                response_data = {"text": response.text}

            # Successfully processed the response - return it
            return status_code, response_data

        except (json.JSONDecodeError, asyncio.TimeoutError) as e:
            logger.warning(f"API request failed ({attempt+1}/{retries}): {str(e)}")
            if attempt >= retries - 1:
                # Last attempt failed, raise the exception
                raise

            # Exponential backoff
            wait_time = retry_delay * (2**attempt)
            logger.info(f"Retrying in {wait_time:.1f}s...")
            await asyncio.sleep(wait_time)
            attempt += 1

    # If we got here, we exhausted our retries
    logger.error(f"API request failed after {retries} attempts: {url}")
    return last_status or 500, last_response_data or {"error": "Max retries exceeded"}


class VisionDownloadManager:
    """Handles downloading Vision data files with validation and processing."""

    def __init__(
        self,
        client: AsyncSession,
        symbol: str,
        interval: str,
        market_type: str = "spot",
    ):
        """Initialize the Vision Download Manager.

        Args:
            client: HTTP client (httpx.AsyncClient recommended)
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Time interval (e.g., "1m", "1h")
            market_type: Market type (spot, futures_usdt, futures_coin)
        """
        self.client = client
        self.symbol = symbol
        self.interval = interval
        self.market_type = market_type
        self.download_handler = DownloadHandler(
            client, max_retries=5, min_wait=4, max_wait=60, timeout=3.0
        )
        self._external_client = client is not None
        self._current_tasks = []
        self._temp_files = []

    async def _cleanup_resources(self):
        """Clean up resources used by the download manager.

        This ensures proper release of HTTP client and any other resources
        to prevent memory leaks or hanging connections.
        """
        logger.debug(
            "[ProgressIndicator] VisionDownloadManager: Starting resource cleanup"
        )
        cleanup_errors = []

        # Step 1: Cancel any ongoing download tasks
        try:
            if hasattr(self, "_current_tasks") and self._current_tasks:
                tasks_to_cancel = [t for t in self._current_tasks if not t.done()]
                if tasks_to_cancel:
                    logger.debug(
                        f"[ProgressIndicator] VisionDownloadManager: Cancelling {len(tasks_to_cancel)} ongoing download tasks"
                    )
                    for task in tasks_to_cancel:
                        task.cancel()

                    try:
                        # Wait briefly for cancellation to complete
                        await asyncio.wait(tasks_to_cancel, timeout=0.5)
                    except Exception as e:
                        error_msg = f"Error waiting for download tasks to cancel: {e}"
                        logger.warning(error_msg)
                        cleanup_errors.append(error_msg)
        except Exception as e:
            error_msg = f"Error cancelling download tasks: {e}"
            logger.warning(error_msg)
            cleanup_errors.append(error_msg)

        # Step 2: Safely close the HTTP client
        try:
            # Only attempt to close if we own the client
            if (
                not self._external_client
                and hasattr(self, "client")
                and self.client is not None
            ):
                logger.debug(
                    "[ProgressIndicator] VisionDownloadManager: Safely closing HTTP client"
                )

                try:
                    # Use safely_close_client for proper cleanup
                    from utils.network_utils import safely_close_client

                    await safely_close_client(self.client)
                    self.client = None
                    logger.debug(
                        "[ProgressIndicator] VisionDownloadManager: HTTP client closed successfully"
                    )
                except Exception as e:
                    error_msg = f"Error closing HTTP client: {e}"
                    logger.warning(error_msg)
                    cleanup_errors.append(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error during client cleanup: {e}"
            logger.warning(error_msg)
            cleanup_errors.append(error_msg)

        # Step 3: Clean up any temporary files from previous downloads
        try:
            if hasattr(self, "_temp_files") and self._temp_files:
                logger.debug(
                    f"[ProgressIndicator] VisionDownloadManager: Cleaning up {len(self._temp_files)} temporary files"
                )
                for temp_file in self._temp_files:
                    try:
                        if os.path.exists(temp_file):
                            os.remove(temp_file)
                    except Exception as e:
                        error_msg = f"Error removing temporary file {temp_file}: {e}"
                        logger.debug(error_msg)
                        # Don't treat temporary file cleanup failures as critical errors

                # Clear the list of temporary files
                self._temp_files = []
        except Exception as e:
            error_msg = f"Error cleaning up temporary files: {e}"
            logger.warning(error_msg)
            cleanup_errors.append(error_msg)

        # Step 4: Force garbage collection to help with circular references
        gc.collect()

        # Log summary of cleanup
        if cleanup_errors:
            logger.warning(
                f"VisionDownloadManager cleanup encountered {len(cleanup_errors)} errors"
            )
            for i, error in enumerate(cleanup_errors, 1):
                logger.debug(f"Cleanup error {i}: {error}")
        else:
            logger.debug(
                "[ProgressIndicator] VisionDownloadManager: Cleanup completed successfully"
            )

    async def download_date(self, date: datetime) -> Optional[List[List]]:
        """Download data for a specific date.

        Args:
            date: Target date

        Returns:
            List of raw data rows if download successful, None otherwise.
            The raw data needs to be processed by TimeseriesDataProcessor.
        """
        # Ensure date has proper timezone
        from utils.validation import DataValidation
        from urllib.parse import urlparse
        import tempfile
        import zipfile
        import time

        date = DataValidation.enforce_utc_timestamp(date)

        # Add debugging timestamp
        debug_id = f"{self.symbol}_{self.interval}_{date.strftime('%Y%m%d')}_{int(time.time())}"

        # Check if data is likely available for the date before attempting download
        is_available = DataValidation.is_data_likely_available(date)

        if not is_available:
            now = datetime.now(timezone.utc)
            # This is a future date or very recent data, no need to log as error
            logger.info(
                f"[{debug_id}] Data for {date.strftime('%Y-%m-%d')} may not be available yet (current date: {now.strftime('%Y-%m-%d')})"
            )
            return None

        logger.info(
            f"[{debug_id}] Starting download for {self.symbol} {self.interval} on {date.strftime('%Y-%m-%d')}"
        )

        # Create temporary directory for downloads
        temp_dir = Path(tempfile.mkdtemp(dir="./tmp"))

        # Get URLs for Vision API
        from utils.for_core.vision_constraints import get_vision_url, FileType

        data_url = get_vision_url(
            self.symbol, self.interval, date, FileType.DATA, self.market_type
        )
        checksum_url = get_vision_url(
            self.symbol, self.interval, date, FileType.CHECKSUM, self.market_type
        )

        # Extract original filenames from URLs
        data_filename = Path(urlparse(data_url).path).name
        checksum_filename = Path(urlparse(checksum_url).path).name

        # Use original filenames in temp directory
        data_file = temp_dir / data_filename
        checksum_file = temp_dir / checksum_filename

        try:
            # Download data and checksum files sequentially
            logger.info(
                f"[{debug_id}] Downloading data for {date.strftime('%Y-%m-%d')} from:"
            )
            logger.info(f"[{debug_id}] Data URL: {data_url}")
            logger.info(f"[{debug_id}] Checksum URL: {checksum_url}")

            download_start = time.time()

            # Download data file first
            if not await self._download_file(data_url, data_file):
                # Changed from ERROR to WARNING since this could be an expected condition for future dates
                logger.warning(f"[{debug_id}] Failed to download data file for {date}")
                return None

            # Then download checksum file
            if not await self._download_file(checksum_url, checksum_file):
                # Changed from ERROR to WARNING
                logger.warning(
                    f"[{debug_id}] Failed to download checksum file for {date}"
                )
                return None

            download_time = time.time() - download_start
            logger.info(f"[{debug_id}] Download completed in {download_time:.2f}s")

            # Log file sizes for diagnosis
            try:
                data_size = data_file.stat().st_size if data_file.exists() else 0
                checksum_size = (
                    checksum_file.stat().st_size if checksum_file.exists() else 0
                )
                logger.info(
                    f"[{debug_id}] Data file size: {data_size} bytes, Checksum file size: {checksum_size} bytes"
                )

                if data_size == 0:
                    # Changed from ERROR to WARNING
                    logger.warning(f"[{debug_id}] Downloaded data file is empty")
                    return None
            except Exception as e:
                logger.error(f"[{debug_id}] Error checking file sizes: {e}")

            # Verify checksum
            from utils.validation import DataValidation
            import traceback

            checksum_start = time.time()
            try:
                # Read checksum file and normalize whitespace
                with open(checksum_file, "r") as f:
                    content = f.read().strip()
                    # Split on whitespace and take first part (the checksum)
                    expected = content.split()[0]
                    content_length = len(content)
                    preview_length = min(40, content_length)
                    logger.debug(
                        f"Raw checksum file content: '{content[:preview_length]}' (+ {content_length - preview_length} more chars, {content_length} total)"
                    )
                    logger.debug(f"Expected checksum: '{expected}'")

                # Calculate checksum of the zip file directly
                actual = DataValidation.calculate_checksum(data_file)
                logger.debug(f"Calculated checksum: '{actual}'")

                if actual != expected:
                    logger.error(f"Checksum mismatch:")
                    logger.error(f"Expected: '{expected}'")
                    logger.error(f"Actual  : '{actual}'")
                    # Try normalizing both checksums
                    expected_norm = expected.lower().strip()
                    actual_norm = actual.lower().strip()
                    logger.debug(f"Normalized expected: '{expected_norm}'")
                    logger.debug(f"Normalized actual  : '{actual_norm}'")
                    if expected_norm != actual_norm:
                        return None
            except Exception as e:
                logger.error(f"Error verifying checksum: {e}")
                logger.debug(f"Full traceback: {traceback.format_exc()}")
                return None

            logger.info(
                f"[{debug_id}] Checksum verification completed in {time.time() - checksum_start:.2f}s"
            )

            # Read CSV data from zip file
            try:
                logger.info(f"[{debug_id}] Reading CSV data from {data_file}")
                csv_start = time.time()

                # Read the CSV from the ZIP file
                raw_data = []
                with zipfile.ZipFile(data_file, "r") as zip_file:
                    file_list = zip_file.namelist()
                    logger.info(f"[{debug_id}] Zip file contents: {file_list}")

                    if not file_list:
                        logger.warning(f"[{debug_id}] Empty zip file: {data_file}")
                        return []

                    csv_file = file_list[0]  # Assume first file is the CSV

                    with zip_file.open(csv_file) as file:
                        # Convert file-like object to bytes
                        file_content = file.read()

                        if len(file_content) == 0:
                            logger.warning(f"[{debug_id}] CSV file is empty")
                            return []

                        # Use StringIO for CSV parsing
                        import io
                        import csv

                        csv_buffer = io.StringIO(file_content.decode("utf-8"))
                        csv_reader = csv.reader(csv_buffer)

                        # Read the first line to check if it contains headers
                        try:
                            first_line = next(csv_reader)
                            # Check if the first line contains headers (likely column names)
                            headers_detected = any(
                                isinstance(val, str) and "time" in val.lower()
                                for val in first_line
                            )

                            # Reset buffer and create a new reader
                            csv_buffer.seek(0)
                            csv_reader = csv.reader(csv_buffer)

                            # Skip header row if detected
                            if headers_detected:
                                logger.info(
                                    f"[{debug_id}] CSV headers detected, skipping first row"
                                )
                                next(csv_reader)
                        except StopIteration:
                            logger.warning(f"[{debug_id}] CSV file appears to be empty")
                            return []

                        # Read all rows into a list of lists
                        for row in csv_reader:
                            # Convert string values to appropriate types
                            processed_row = []
                            for val in row:
                                try:
                                    # Try to convert to numeric if possible
                                    if "." in val:
                                        processed_row.append(float(val))
                                    else:
                                        processed_row.append(int(val))
                                except (ValueError, TypeError):
                                    # Keep as string if conversion fails
                                    processed_row.append(val)
                            raw_data.append(processed_row)

                logger.info(
                    f"[{debug_id}] CSV reading completed in {time.time() - csv_start:.2f}s"
                )

                # Check if we got any data
                if not raw_data:
                    logger.warning(f"[{debug_id}] Downloaded CSV is empty")
                    return None

                logger.info(
                    f"[{debug_id}] Successfully extracted {len(raw_data)} rows of raw data"
                )
                return raw_data

            except Exception as e:
                logger.error(f"[{debug_id}] Error reading CSV data: {e}")
                return None

        except Exception as e:
            logger.error(f"[{debug_id}] Error downloading data for {date}: {e}")
            return None
        finally:
            # Cleanup
            try:
                import shutil

                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                logger.error(f"[{debug_id}] Error cleaning up temp directory: {e}")

    async def _download_file(self, url: str, local_path: Path) -> bool:
        """Download a file from URL to local path.

        Args:
            url: URL to download from
            local_path: Path to save to

        Returns:
            True if download successful, False otherwise
        """
        try:
            logger.debug(f"[ProgressIndicator] Downloading {url} to {local_path}")

            # Make the directory if it doesn't exist
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Make the request
            response = await self.client.get(url, timeout=30.0)

            # Check for errors
            if response.status_code >= 400:
                logger.warning(
                    f"[ProgressIndicator] HTTP error: {response.status_code} downloading {url}"
                )
                return False

            # Save the content
            with open(local_path, "wb") as f:
                f.write(response.content)

            logger.debug(
                f"[ProgressIndicator] Successfully downloaded {url} ({len(response.content)} bytes)"
            )
            return True

        except Exception as e:
            logger.error(f"[ProgressIndicator] Error downloading {url}: {e}")
            return False


async def safely_close_client(client):
    """Safely close an HTTP client, with special handling for different client types.

    This function handles safely closing HTTP clients, with proper error handling
    and cleanup of resources.

    Args:
        client: The HTTP client to close (httpx.AsyncClient)
    """
    if client is None:
        return

    # Get client type name for better error messages
    client_type = client.__class__.__name__
    logger.debug(f"[ProgressIndicator] Closing HTTP client of type {client_type}")

    try:
        # Try close() method first (available on most HTTP clients)
        if hasattr(client, "close") and callable(client.close):
            # Check if it's an async method
            if asyncio.iscoroutinefunction(client.close):
                await client.close()
                logger.debug(
                    f"[ProgressIndicator] Closed {client_type} with await client.close()"
                )
            else:
                client.close()
                logger.debug(
                    f"[ProgressIndicator] Closed {client_type} with client.close()"
                )
            return

        # Handle httpx.AsyncClient special case
        if client_type == "AsyncClient" and hasattr(client, "aclose"):
            await client.aclose()
            logger.debug(f"[ProgressIndicator] Closed {client_type} via aclose()")
            return

        # For older versions of httpx
        if hasattr(client, "transport") and hasattr(client.transport, "close"):
            client.transport.close()
            logger.debug(f"[ProgressIndicator] Closed {client_type}.transport")
            return

        # If we get here, we don't know how to close this client
        logger.warning(
            f"[ProgressIndicator] Don't know how to close client of type {client_type}"
        )

    except Exception as e:
        logger.warning(f"[ProgressIndicator] Error closing {client_type}: {e}")
    finally:
        # Suggest garbage collection to clean up resources
        gc.collect()


async def test_connectivity(
    client: Optional[AsyncSession] = None,
    url: str = "https://data.binance.vision/",
    timeout: float = 10.0,
    retry_count: int = 2,
) -> bool:
    """Test connectivity to a URL.

    Args:
        client: Optional httpx.AsyncClient to use (new one is created if None)
        url: URL to test connectivity to
        timeout: Request timeout in seconds
        retry_count: Number of retry attempts

    Returns:
        True if connectivity test passes, False otherwise
    """
    # Create a client if not provided
    should_close_client = False
    if client is None:
        client = create_client(timeout=timeout)
        should_close_client = True

    try:
        for attempt in range(retry_count + 1):
            try:
                logger.debug(
                    f"[ProgressIndicator] Testing connectivity to {url} (attempt {attempt+1}/{retry_count+1})"
                )
                response = await client.get(url, timeout=timeout)

                if response.status_code < 400:
                    logger.debug(
                        f"[ProgressIndicator] Connectivity test successful: {response.status_code}"
                    )
                    return True

                logger.warning(
                    f"[ProgressIndicator] Connectivity test failed with status code: {response.status_code}"
                )

                if attempt < retry_count:
                    await asyncio.sleep(2**attempt)  # Exponential backoff

            except Exception as e:
                logger.warning(
                    f"[ProgressIndicator] Connectivity test failed with error: {str(e)}"
                )
                if attempt < retry_count:
                    await asyncio.sleep(2**attempt)  # Exponential backoff

        # All attempts failed
        return False
    finally:
        # Close the client if we created it
        if should_close_client and client:
            try:
                await safely_close_client(client)
            except Exception as e:
                logger.warning(f"Error closing client after connectivity test: {e}")
