#!/usr/bin/env python
"""Network utilities for handling HTTP client creation and file downloads.

This module centralizes network-related functionality, including:
1. HTTP client creation with standardized configuration using curl_cffi
2. Download handling with progress tracking and retry logic
3. Rate limiting management and stall detection

By consolidating these utilities, we ensure consistent network behavior across the application.
"""

import asyncio
import logging
import time
import tempfile
import zipfile
import traceback
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
import sys
import inspect
import platform

# Import curl_cffi for HTTP client implementation
from curl_cffi.requests import AsyncSession

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from utils.config import (
    DEFAULT_USER_AGENT,
    DEFAULT_ACCEPT_HEADER,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
)
from utils.logger_setup import logger


# ----- HTTP Client Factory Functions -----


def create_httpx_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 50,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Any:
    """Create an httpx AsyncClient as an alternative to curl_cffi.

    Args:
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional headers to include in all requests
        **kwargs: Additional keyword arguments to pass to AsyncClient

    Returns:
        httpx.AsyncClient: An initialized async HTTP client
    """
    try:
        import httpx
        from httpx import AsyncClient, Limits, Timeout

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
    use_httpx: bool = False,
    **kwargs: Any,
) -> Any:
    """Create a client for making HTTP requests.

    This function provides a unified interface for creating HTTP clients,
    supporting both curl_cffi and httpx, with automatic fallback.

    Args:
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional headers to include in all requests
        use_httpx: Whether to prefer httpx over curl_cffi
        **kwargs: Additional keyword arguments to pass to the client

    Returns:
        An initialized async HTTP client
    """
    if max_connections is None:
        max_connections = 50  # Default to 50 connections

    # Try to use httpx if requested
    if use_httpx:
        try:
            return create_httpx_client(timeout, max_connections, headers, **kwargs)
        except ImportError:
            logger.warning("Failed to create httpx client, falling back to curl_cffi")

    # Try curl_cffi
    try:
        return create_curl_cffi_client(timeout, max_connections, headers, **kwargs)
    except ImportError:
        logger.warning("curl_cffi not available, trying httpx instead")

        # Fall back to httpx
        try:
            return create_httpx_client(timeout, max_connections, headers, **kwargs)
        except ImportError:
            logger.error(
                "Neither curl_cffi nor httpx is available. Please install one of these packages."
            )
            raise ImportError(
                "No HTTP client library available. Please install curl_cffi or httpx."
            )


def create_curl_cffi_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 50,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> AsyncSession:
    """Factory function to create a pre-configured curl_cffi AsyncSession.

    Args:
        timeout: Total timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers
        **kwargs: Additional client configuration options

    Returns:
        Configured curl_cffi AsyncSession with standardized settings
    """
    client_headers = {
        "Accept": DEFAULT_ACCEPT_HEADER,
        "User-Agent": DEFAULT_USER_AGENT,
    }

    if headers:
        client_headers.update(headers)

    # Remove any incompatible kwargs that might be passed from httpx configuration
    if "http2" in kwargs:
        del kwargs["http2"]
    if "follow_redirects" in kwargs:
        del kwargs["follow_redirects"]
    if "h2" in kwargs:
        del kwargs["h2"]

    client_kwargs = {
        "timeout": timeout,
        "headers": client_headers,
        "max_clients": max_connections,
    }

    # Add any additional kwargs
    client_kwargs.update(kwargs)

    return AsyncSession(**client_kwargs)


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
            client: HTTP client (curl_cffi AsyncSession recommended)
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

    async def __aexit__(self, exc_type, exc_val, exc_tb):
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
        verify_ssl: bool = True,
        expected_size: Optional[int] = None,
        stall_timeout: int = 30,
    ) -> bool:
        """Download a file with retry logic, progress tracking and validation.

        Args:
            url: URL to download
            local_path: Local path to save the file
            timeout: Download timeout in seconds
            verify_ssl: Whether to verify SSL certificates
            expected_size: Expected file size for validation
            stall_timeout: Time in seconds before considering download stalled

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
    """Download multiple files concurrently with rate limiting.

    Args:
        client: HTTP client (curl_cffi AsyncSession recommended)
        urls: List of URLs to download
        local_paths: List of paths to save files to
        max_concurrent: Maximum number of concurrent downloads
        **download_kwargs: Additional arguments to pass to DownloadHandler.download_file

    Returns:
        List of download results (True for success, False for failure)
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
    """Make an API request with retry logic and timeout handling.

    Args:
        client: HTTP client (curl_cffi AsyncSession recommended)
        url: URL to request
        headers: Optional request headers
        params: Optional query parameters
        method: HTTP method (GET, POST, etc.)
        json_data: Optional JSON payload for POST requests
        timeout: Request timeout in seconds
        retries: Number of retry attempts
        retry_delay: Base delay between retries
        raise_for_status: Whether to raise an exception for error status codes

    Returns:
        Tuple of (status_code, response_data)

    Raises:
        Exception: For network errors or HTTP errors if raise_for_status is True
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
        """Initialize the download manager.

        Args:
            client: HTTP client for downloads (curl_cffi AsyncSession)
            symbol: Trading pair symbol
            interval: Time interval
            market_type: Market type (spot, futures_usdt, futures_coin)
        """
        self.client = client
        self.symbol = symbol
        self.interval = interval
        self.market_type = market_type
        self.download_handler = DownloadHandler(
            client, max_retries=5, min_wait=4, max_wait=60, timeout=3.0
        )

    async def _cleanup_resources(self) -> None:
        """Properly clean up all resources used by the download manager.

        This ensures all HTTP connections and background tasks are properly cleaned up,
        preventing resource leaks and hanging tasks.
        """
        cleanup_errors = []

        # Clean up the download handler if it exists
        if hasattr(self, "download_handler"):
            try:
                # Close the download handler's client if it's different from self.client
                if (
                    hasattr(self.download_handler, "client")
                    and self.download_handler.client is not None
                    and self.download_handler.client is not self.client
                ):
                    logger.debug(
                        f"Cleaning up download handler's client for {self.symbol}"
                    )
                    await safely_close_client(self.download_handler.client)
            except Exception as e:
                error_msg = f"Error cleaning up download handler: {str(e)}"
                logger.warning(error_msg)
                cleanup_errors.append(error_msg)

        # Clean up any pending background tasks
        try:
            # Give background tasks a chance to complete
            await asyncio.sleep(0.1)

            # Run task cleanup to ensure no lingering download tasks
            await _cleanup_all_async_curl_tasks(0.5)
        except Exception as e:
            error_msg = f"Error cleaning up background tasks: {str(e)}"
            logger.warning(error_msg)
            cleanup_errors.append(error_msg)

        # Log any cleanup errors
        if cleanup_errors:
            logger.warning(
                f"Encountered {len(cleanup_errors)} errors during VisionDownloadManager cleanup"
            )
            for i, error in enumerate(cleanup_errors, 1):
                logger.debug(f"Cleanup error {i}: {error}")

    def _get_checksum_url(self, date: datetime) -> str:
        """Get checksum URL for a specific date.

        Args:
            date: Target date

        Returns:
            URL for the checksum file
        """
        # Use DataValidation.enforce_utc_timestamp for consistent timezone handling
        from utils.validation import DataValidation

        date = DataValidation.enforce_utc_timestamp(date)

        # Import vision constraints here to avoid circular imports
        from core.vision_constraints import get_vision_url, FileType

        return get_vision_url(
            self.symbol, self.interval, date, FileType.CHECKSUM, self.market_type
        )

    def _get_data_url(self, date: datetime) -> str:
        """Get data URL for a specific date.

        Args:
            date: Target date

        Returns:
            URL for the data file
        """
        # Use DataValidation.enforce_utc_timestamp for consistent timezone handling
        from utils.validation import DataValidation

        date = DataValidation.enforce_utc_timestamp(date)

        # Import vision constraints here to avoid circular imports
        from core.vision_constraints import get_vision_url, FileType

        return get_vision_url(
            self.symbol, self.interval, date, FileType.DATA, self.market_type
        )

    def _verify_checksum(self, file_path: Path, checksum_path: Path) -> bool:
        """Verify file checksum against expected value.

        Args:
            file_path: Path to data file (zip file)
            checksum_path: Path to checksum file

        Returns:
            Verification status
        """
        try:
            # Read checksum file and normalize whitespace
            with open(checksum_path, "r") as f:
                content = f.read().strip()
                # Split on whitespace and take first part (the checksum)
                expected = content.split()[0]
                logger.debug(f"Raw checksum file content: '{content}'")
                logger.debug(f"Expected checksum: '{expected}'")

            from utils.validation import DataValidation

            # Log file details
            logger.debug(f"Verifying checksum for file: {file_path}")
            logger.debug(f"File exists: {file_path.exists()}")
            logger.debug(f"File size: {file_path.stat().st_size} bytes")

            # Read first few bytes of the file
            with open(file_path, "rb") as f:
                header = f.read(16)
                logger.debug(f"File header (hex): {header.hex()}")

            # Calculate checksum of the zip file directly
            actual = DataValidation.calculate_checksum(file_path)
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
                return expected_norm == actual_norm
            return True
        except Exception as e:
            logger.error(f"Error verifying checksum: {e}")
            logger.debug(f"Full traceback: {traceback.format_exc()}")
            return False

    async def download_file(self, url: str, local_path: Path) -> bool:
        """Download a file from URL to local path.

        Args:
            url: URL to download from
            local_path: Path to save to

        Returns:
            True if download successful, False otherwise
        """
        try:
            return await self.download_handler.download_file(url, local_path)
        except Exception as e:
            logger.error(f"Error downloading {url}: {str(e)}")
            return False

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
        temp_dir = Path(tempfile.mkdtemp())

        # Get URLs first to extract original filenames
        data_url = self._get_data_url(date)
        checksum_url = self._get_checksum_url(date)

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
            data_success = await self.download_file(data_url, data_file)
            if not data_success:
                # Changed from ERROR to WARNING since this could be an expected condition for future dates
                logger.warning(f"[{debug_id}] Failed to download data file for {date}")
                return None

            # Then download checksum file
            checksum_success = await self.download_file(checksum_url, checksum_file)
            if not checksum_success:
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
            checksum_start = time.time()
            if not self._verify_checksum(data_file, checksum_file):
                logger.error(f"[{debug_id}] Checksum verification failed for {date}")
                return None
            logger.info(
                f"[{debug_id}] Checksum verification completed in {time.time() - checksum_start:.2f}s"
            )

            # Read CSV data
            try:
                logger.info(f"[{debug_id}] Reading CSV data from {data_file}")
                csv_start = time.time()

                raw_data = await read_csv_from_zip(
                    data_file, log_prefix=f"[{debug_id}]"
                )

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
                return None


async def read_csv_from_zip(zip_file_path: str, log_prefix: str = "") -> List[List]:
    """
    Read a CSV file from a zip archive and return the raw data as a list of lists.

    This function extracts the raw data without processing it, allowing the caller
    to apply consistent formatting using TimeseriesDataProcessor.

    Args:
        zip_file_path: Path to the zip file
        log_prefix: Prefix for log messages

    Returns:
        List[List]: The raw data from the CSV file as a list of rows
    """
    start_time = time.time()

    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_file:
            file_list = zip_file.namelist()
            logger.info(f"{log_prefix} Zip file contents: {file_list}")

            if not file_list:
                logger.warning(f"{log_prefix} Empty zip file: {zip_file_path}")
                return []

            csv_file = file_list[0]  # Assuming first file is the CSV

            with zip_file.open(csv_file) as file:
                # Convert file-like object to bytes
                file_content = file.read()

                if len(file_content) == 0:
                    logger.warning(f"{log_prefix} CSV file is empty")
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
                            f"{log_prefix} CSV headers detected, skipping first row"
                        )
                        next(csv_reader)
                except StopIteration:
                    logger.warning(f"{log_prefix} CSV file appears to be empty")
                    return []

                # Read all rows into a list of lists
                raw_data = []
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
                    f"{log_prefix} Read {len(raw_data)} rows from CSV in {time.time() - start_time:.2f}s"
                )
                return raw_data

    except Exception as e:
        logger.error(f"{log_prefix} Error reading CSV data: {e}")
        return []


async def download_file(
    client: AsyncSession,
    url: str,
    destination_path: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
    retries: int = 3,
    retry_delay: float = 1.0,
    progress_tracker: Optional[DownloadProgressTracker] = None,
) -> bool:
    """Download a file to a local path with retries and error handling.

    Args:
        client: HTTP client (curl_cffi AsyncSession recommended)
        url: URL to download
        destination_path: Path to save the file to
        headers: Additional HTTP headers
        timeout: Request timeout in seconds
        retries: Number of retries for transient errors
        retry_delay: Delay between retries in seconds
        progress_tracker: Optional progress tracker instance

    Returns:
        True if the download was successful, False otherwise

    Raises:
        Exception: For network or file errors
    """
    headers = headers or {}
    timeout_value = timeout or 30.0
    attempt = 0

    # Create destination directory if it doesn't exist
    if destination_path:
        os.makedirs(os.path.dirname(os.path.abspath(destination_path)), exist_ok=True)

    while attempt < retries:
        try:
            response = await client.get(url, headers=headers, timeout=timeout_value)
            if response.status_code >= 400:
                raise Exception(f"HTTP error: {response.status_code} - {response.text}")
            content = response.content

            # Update progress tracker if provided
            if progress_tracker:
                progress_tracker.update(url, len(content))

            # Save to file if destination_path is provided
            if destination_path:
                with open(destination_path, "wb") as f:
                    f.write(content)

            return True

        except Exception as e:
            attempt += 1
            # Check if this is a timeout, connection error, or rate limiting
            error_str = str(e).lower()
            is_timeout = "timeout" in error_str or isinstance(e, asyncio.TimeoutError)
            is_connection_error = "connection" in error_str
            is_rate_limited = (
                "429" in error_str
                or "too many requests" in error_str
                or hasattr(e, "status")
                and getattr(e, "status", 0) == 429
                or hasattr(e, "status_code")
                and getattr(e, "status_code", 0) == 429
            )

            # Determine retry behavior and delay
            should_retry = attempt < retries and (
                is_timeout or is_connection_error or is_rate_limited
            )

            if should_retry:
                # Use longer delay for rate limiting
                current_delay = retry_delay * (2 if is_rate_limited else 1)
                logger.warning(
                    f"Download failed for {url}: {str(e)}, retrying in {current_delay}s ({attempt}/{retries})"
                )
                await asyncio.sleep(current_delay)
            else:
                logger.error(
                    f"Download failed for {url} after {retries} retries: {str(e)}"
                )
                return False

    # If we've reached this point, all retries failed
    return False


async def safely_close_client(client):
    """Safely close a client with proper error handling and cleanup.

    Args:
        client: HTTP client to close

    This attempts to close the client using multiple methods while preventing
    exceptions from propagating.
    """
    if client is None:
        return

    client_type = type(client).__name__
    logger.debug(f"Safely closing client of type {client_type}")

    # First, pre-emptively break any circular references
    if hasattr(client, "_curlm") and client._curlm is not None:
        logger.debug("Pre-emptively cleaning _curlm reference to prevent hanging")
        client._curlm = None

    # Also clear _timeout_handle if it exists
    if hasattr(client, "_timeout_handle") and client._timeout_handle is not None:
        logger.debug("Pre-emptively cleaning _timeout_handle to prevent hanging")
        client._timeout_handle = None

    # Cancel any force_timeout tasks
    await _cancel_force_timeout_tasks()

    # Then try different closing methods with error handling
    try:
        # Try aclose() method (used by curl_cffi.AsyncSession)
        if hasattr(client, "aclose") and inspect.iscoroutinefunction(client.aclose):
            try:
                logger.debug("Closing client with aclose()")
                await asyncio.wait_for(client.aclose(), timeout=2.0)
                logger.debug("Successfully closed client with aclose()")
                return
            except asyncio.TimeoutError:
                logger.warning("Timeout while closing client with aclose()")
            except Exception as e:
                logger.warning(f"Exception during client.aclose(): {e}")

        # Try close() method (used by some clients)
        if hasattr(client, "close"):
            if inspect.iscoroutinefunction(client.close):
                try:
                    logger.debug("Closing client with async close()")
                    await asyncio.wait_for(client.close(), timeout=2.0)
                    logger.debug("Successfully closed client with async close()")
                    return
                except asyncio.TimeoutError:
                    logger.warning("Timeout while closing client with async close()")
                except Exception as e:
                    logger.warning(f"Exception during async client.close(): {e}")
            else:
                # Synchronous close
                try:
                    logger.debug("Closing client with sync close()")
                    client.close()
                    logger.debug("Successfully closed client with sync close()")
                    return
                except Exception as e:
                    logger.warning(f"Exception during sync client.close(): {e}")

        # For httpx.AsyncClient, try __aexit__
        if hasattr(client, "__aexit__"):
            try:
                logger.debug("Closing client with __aexit__()")
                await client.__aexit__(None, None, None)
                logger.debug("Successfully closed client with __aexit__()")
                return
            except Exception as e:
                logger.warning(f"Exception during client.__aexit__(): {e}")

        logger.warning(
            f"No suitable close method found for client of type {client_type}"
        )
    except Exception as e:
        logger.error(f"Unexpected error during client cleanup: {e}")


async def _cancel_force_timeout_tasks():
    """Find and cancel any _force_timeout tasks that might be active.

    This function helps prevent hanging by explicitly cancelling force_timeout
    tasks created by curl_cffi that might keep the event loop active.

    Returns:
        int: Number of tasks cancelled
    """
    cancelled_count = 0
    try:
        # Find all force_timeout tasks
        force_timeout_tasks = []
        for task in asyncio.all_tasks():
            task_str = str(task)
            if "_force_timeout" in task_str and not task.done():
                force_timeout_tasks.append(task)

        # Cancel all found tasks
        if force_timeout_tasks:
            logger.debug(f"Cancelling {len(force_timeout_tasks)} force_timeout tasks")
            for task in force_timeout_tasks:
                task.cancel()
            cancelled_count = len(force_timeout_tasks)

            # Wait for cancellation to complete with timeout
            try:
                await asyncio.wait_for(
                    asyncio.gather(*force_timeout_tasks, return_exceptions=True),
                    timeout=0.5,  # Short timeout to avoid blocking
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for force_timeout tasks to cancel")

    except Exception as e:
        logger.warning(f"Error cancelling force_timeout tasks: {e}")

    return cancelled_count


async def _cleanup_all_async_curl_tasks(timeout_seconds: float = 0.5) -> None:
    """Find and clean up all AsyncCurl related tasks to prevent hanging.

    This is a more comprehensive cleanup that targets all curl_cffi related tasks.

    Args:
        timeout_seconds: Maximum time to wait for tasks to cancel
    """
    # First pass: Identify and cancel all pending AsyncCurl._force_timeout tasks
    pending_force_timeout_tasks = []

    # Be more specific about which tasks we're targeting - focus on _force_timeout
    for task in asyncio.all_tasks():
        task_str = str(task)
        # Specifically look for the _force_timeout method which causes the issues
        if not task.done() and "AsyncCurl._force_timeout" in task_str:
            pending_force_timeout_tasks.append(task)
            logger.debug(f"Found pending timeout task: {task.get_name()} - {task_str}")

    if pending_force_timeout_tasks:
        logger.debug(
            f"Attempting to clean up {len(pending_force_timeout_tasks)} pending timeout tasks"
        )
        try:
            # First cancel all futures being waited on - this is the key fix
            for task in pending_force_timeout_tasks:
                # Aggressively cancel the _fut_waiter that the task is waiting on
                if hasattr(task, "_fut_waiter") and task._fut_waiter is not None:
                    logger.debug(f"Cancelling future for task {task.get_name()}")
                    try:
                        task._fut_waiter.cancel()
                    except Exception as e:
                        logger.debug(f"Error cancelling future: {e}")

                # Then cancel the task itself if it's not done
                if not task.done() and not task.cancelled():
                    logger.debug(f"Cancelling task {task.get_name()}")
                    task.cancel()

            # Give tasks time to respond to cancellation
            await asyncio.sleep(timeout_seconds)

            # Check if tasks are still pending after cancellation
            still_pending = [t for t in pending_force_timeout_tasks if not t.done()]
            if still_pending:
                logger.debug(
                    f"{len(still_pending)} tasks still pending after cancellation"
                )

                # More aggressive approach for stubborn tasks - wait for them
                try:
                    # Set a timeout to prevent indefinite waiting
                    done, pending = await asyncio.wait(
                        still_pending, timeout=timeout_seconds
                    )

                    # For any tasks that are still pending, try even more aggressive approaches
                    if pending:
                        logger.debug(
                            f"After waiting, {len(pending)} tasks still pending"
                        )

                        # Try setting a custom exception to wake up tasks that are waiting
                        for task in pending:
                            try:
                                # Access internal task state to stop it directly
                                try_unblock_task(task)
                            except Exception as e:
                                logger.debug(f"Error force-stopping task: {e}")
                except asyncio.TimeoutError:
                    logger.debug("Timeout waiting for tasks to complete")
                except Exception as e:
                    logger.debug(f"Error while waiting for tasks: {e}")
            else:
                logger.debug("All pending tasks successfully cancelled")
        except Exception as e:
            logger.debug(f"Error during task cleanup: {e}")
    else:
        logger.debug("No pending curl_cffi tasks found")
        # Still add a short delay to allow any hidden tasks to settle
        await asyncio.sleep(0.1)


def try_unblock_task(task):
    """Attempt to unblock a pending asyncio task using various approaches.

    This function tries different methods to forcibly stop a task that
    isn't responding to normal cancellation.

    Args:
        task: The asyncio.Task object to unblock
    """
    # First check if task has a _fut_waiter
    if hasattr(task, "_fut_waiter") and task._fut_waiter is not None:
        # Try to set an exception on the future to wake it up
        try:
            task._fut_waiter.set_exception(asyncio.CancelledError())
        except Exception:
            pass

    # Try to directly cancel task with low-level SIGINT
    try:
        if hasattr(task, "_coro") and task._coro:
            # Force the coroutine to raise an exception to exit
            if hasattr(task._coro, "throw"):
                task._coro.throw(asyncio.CancelledError())
    except Exception:
        pass


# ----- Network Validation Utilities -----


async def test_connectivity(
    client: Optional[AsyncSession] = None,
    url: str = "https://data.binance.vision/",
    timeout: float = 10.0,
    retry_count: int = 2,
) -> bool:
    """Test connectivity to a specific URL.

    This function can be used to verify network connectivity before
    making API requests, helping to diagnose network issues early.

    Args:
        client: HTTP client to use for the test. If None, a new client will be created.
        url: URL to test connectivity to
        timeout: Request timeout in seconds
        retry_count: Number of retry attempts

    Returns:
        True if connection succeeds, False otherwise
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
                    f"Testing connectivity to {url} (attempt {attempt+1}/{retry_count+1})"
                )
                response = await client.get(url, timeout=timeout)

                if response.status_code < 400:
                    logger.debug(
                        f"Connectivity test successful: {response.status_code}"
                    )
                    return True

                logger.warning(
                    f"Connectivity test failed with status code: {response.status_code}"
                )

                if attempt < retry_count:
                    await asyncio.sleep(2**attempt)  # Exponential backoff
            except Exception as e:
                logger.warning(f"Connectivity test failed with error: {str(e)}")

                if attempt < retry_count:
                    await asyncio.sleep(2**attempt)  # Exponential backoff

        return False
    finally:
        # Close the client if we created it
        if should_close_client:
            await client.close()
