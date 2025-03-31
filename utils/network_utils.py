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
from datetime import datetime
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

# Import curl_cffi for HTTP client implementation
from curl_cffi.requests import AsyncSession

import pandas as pd
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
from utils.logger_setup import get_logger

# Configure module logger
logger = get_logger(__name__, "INFO", show_path=False)

# ----- HTTP Client Factory Functions -----


def create_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: Optional[int] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> AsyncSession:
    """Create a standardized curl_cffi HTTP client.

    Provides a unified interface for creating HTTP clients
    with consistent configuration options.

    Args:
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers to include in all requests
        **kwargs: Additional client-specific configuration options

    Returns:
        Configured curl_cffi AsyncSession with standard settings
    """
    # Use default max connections if not specified
    if max_connections is None:
        max_connections = 50

    # Merge default headers with custom headers
    default_headers = {
        "Accept": DEFAULT_ACCEPT_HEADER,
        "User-Agent": DEFAULT_USER_AGENT,
    }

    if headers:
        default_headers.update(headers)

    return create_curl_cffi_client(
        timeout=timeout,
        max_connections=max_connections,
        headers=default_headers,
        **kwargs,
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

    def _get_checksum_url(self, date: datetime) -> str:
        """Get checksum URL for a specific date.

        Args:
            date: Target date

        Returns:
            URL for the checksum file
        """
        # Import and use enforce_utc_timezone for consistent timezone handling
        from utils.time_utils import enforce_utc_timezone

        date = enforce_utc_timezone(date)

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
        # Import and use enforce_utc_timezone for consistent timezone handling
        from utils.time_utils import enforce_utc_timezone

        date = enforce_utc_timezone(date)

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

            from utils.validation_utils import (
                calculate_checksum,
            )

            # Log file details
            logger.debug(f"Verifying checksum for file: {file_path}")
            logger.debug(f"File exists: {file_path.exists()}")
            logger.debug(f"File size: {file_path.stat().st_size} bytes")

            # Read first few bytes of the file
            with open(file_path, "rb") as f:
                header = f.read(16)
                logger.debug(f"File header (hex): {header.hex()}")

            # Calculate checksum of the zip file directly
            actual = calculate_checksum(file_path)
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

    async def download_date(self, date: datetime) -> Optional[pd.DataFrame]:
        """Download data for a specific date.

        Args:
            date: Target date

        Returns:
            DataFrame with data or None if download failed
        """
        # Ensure date has proper timezone
        from utils.time_utils import enforce_utc_timezone
        from urllib.parse import urlparse

        date = enforce_utc_timezone(date)

        # Add debugging timestamp
        debug_id = f"{self.symbol}_{self.interval}_{date.strftime('%Y%m%d')}_{int(time.time())}"
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
                logger.error(f"[{debug_id}] Failed to download data file for {date}")
                return None

            # Then download checksum file
            checksum_success = await self.download_file(checksum_url, checksum_file)
            if not checksum_success:
                logger.error(
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
                    logger.error(f"[{debug_id}] Downloaded data file is empty")
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

                df = await read_csv_from_zip(data_file, log_prefix=f"[{debug_id}]")

                logger.info(
                    f"[{debug_id}] CSV reading completed in {time.time() - csv_start:.2f}s"
                )

                # Process and return the data
                if df.empty:
                    logger.warning(f"[{debug_id}] Downloaded CSV is empty")
                    return None

                # No need to rename index anymore since read_csv_from_zip already uses KLINE_COLUMNS
                # which includes "open_time" as the timestamp column name

                return df

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


async def read_csv_from_zip(zip_file_path: str, log_prefix: str = "") -> pd.DataFrame:
    """
    Read a CSV file from a zip archive and return the data as a pandas DataFrame.

    Args:
        zip_file_path: Path to the zip file
        log_prefix: Prefix for log messages

    Returns:
        pandas.DataFrame: The data from the CSV file
    """
    start_time = time.time()

    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_file:
            file_list = zip_file.namelist()
            logger.info(f"{log_prefix} Zip file contents: {file_list}")

            if not file_list:
                logger.warning(f"{log_prefix} Empty zip file: {zip_file_path}")
                return pd.DataFrame()

            csv_file = file_list[0]  # Assuming first file is the CSV

            with zip_file.open(csv_file) as file:
                # Convert file-like object to bytes and then to StringIO for pandas
                file_content = file.read()

                if len(file_content) == 0:
                    logger.warning(f"{log_prefix} CSV file is empty")
                    return pd.DataFrame()

                # Use KLINE_COLUMNS from config to ensure consistency with REST API
                from utils.config import KLINE_COLUMNS

                # Use StringIO for CSV parsing
                import io

                try:
                    df = pd.read_csv(
                        io.BytesIO(file_content), header=None, names=KLINE_COLUMNS
                    )

                    # Log sample of raw data for debugging
                    if not df.empty:
                        logger.info(
                            f"{log_prefix} Raw data sample (first row): {df.iloc[0].to_dict()}"
                        )

                    # Drop the 'ignore' column
                    if "ignore" in df.columns:
                        df = df.drop(columns=["ignore"])

                    # Convert numeric columns first
                    numeric_columns = [
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "close_time",
                        "quote_asset_volume",
                        "number_of_trades",
                        "taker_buy_base_asset_volume",
                        "taker_buy_quote_asset_volume",
                    ]

                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")

                    # Convert open_time column (milliseconds since epoch) to datetime
                    if "open_time" in df.columns and not df.empty:
                        try:
                            # Check the timestamp digits to determine format
                            logger.info(
                                f"{log_prefix} Converting open_time column to datetime"
                            )
                            timestamp_val = df["open_time"].astype(float)
                            sample_ts = (
                                timestamp_val.iloc[0] if not timestamp_val.empty else 0
                            )
                            digits = len(str(int(sample_ts)))

                            # Handle different timestamp formats
                            if digits > 13:  # Microseconds (16 digits)
                                logger.info(
                                    f"{log_prefix} Detected microsecond timestamps ({digits} digits)"
                                )
                                # Use direct microsecond conversion to avoid overflow
                                df["open_time"] = pd.to_datetime(
                                    df["open_time"], unit="us", utc=True
                                )
                            else:  # Standard millisecond format (13 digits)
                                logger.info(
                                    f"{log_prefix} Detected millisecond timestamps ({digits} digits)"
                                )
                                df["open_time"] = pd.to_datetime(
                                    df["open_time"], unit="ms", utc=True
                                )

                            df = df.set_index("open_time")

                            # Log sample after conversion
                            if not df.empty:
                                logger.info(
                                    f"{log_prefix} Converted data sample: {df.iloc[0].to_dict()}"
                                )
                                logger.info(
                                    f"{log_prefix} Index type: {type(df.index)}, timezone: {df.index.tz}"
                                )
                        except Exception as e:
                            logger.error(
                                f"{log_prefix} Error converting open_time: {str(e)}"
                            )
                            # Fallback to a more flexible approach
                            try:
                                # Try autodetecting the format based on the number of digits
                                timestamp_val = df["open_time"].astype(float)
                                sample_ts = (
                                    timestamp_val.iloc[0]
                                    if not timestamp_val.empty
                                    else 0
                                )
                                digits = len(str(int(sample_ts)))

                                # Handle timestamp format based on number of digits
                                if digits > 13:  # Microseconds (16 digits)
                                    # Use unit parameter directly to prevent overflow
                                    logger.info(
                                        f"{log_prefix} Using direct microsecond conversion for {digits} digits"
                                    )
                                    df["open_time"] = pd.to_datetime(
                                        df["open_time"], unit="us", utc=True
                                    )
                                elif digits > 10:  # Milliseconds (13 digits)
                                    df["open_time"] = pd.to_datetime(
                                        df["open_time"], unit="ms", utc=True
                                    )
                                else:  # Seconds (10 digits)
                                    df["open_time"] = pd.to_datetime(
                                        df["open_time"], unit="s", utc=True
                                    )

                                df = df.set_index("open_time")
                                logger.info(
                                    f"{log_prefix} Fallback timestamp conversion succeeded with {digits} digits"
                                )
                            except Exception as nested_e:
                                logger.error(
                                    f"{log_prefix} Fallback timestamp conversion failed: {str(nested_e)}"
                                )
                                return pd.DataFrame()

                    # Final check on DataFrame
                    if df.empty:
                        logger.warning(
                            f"{log_prefix} DataFrame is empty after processing"
                        )
                    else:
                        logger.info(
                            f"{log_prefix} Successfully processed CSV with {len(df)} rows"
                        )

                    logger.info(
                        f"{log_prefix} CSV reading completed in {time.time() - start_time:.2f}s"
                    )
                    return df

                except Exception as e:
                    logger.error(f"{log_prefix} Error processing data: {str(e)}")
                    return pd.DataFrame()

    except Exception as e:
        logger.error(f"{log_prefix} Error reading zip file: {str(e)}")
        return pd.DataFrame()


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


async def safely_close_client(client: AsyncSession) -> None:
    """Safely close curl_cffi AsyncSession with proper cleanup of background tasks.

    The curl_cffi AsyncCurl implementation creates background tasks for timeout handling
    that can cause "Task was destroyed but it is pending" warnings if not properly cleaned up.
    This function ensures those tasks can complete before the client is fully closed.

    Args:
        client: curl_cffi AsyncSession to close
    """
    if client is None:
        return

    try:
        # First verify the client has a close method
        if not hasattr(client, "close"):
            logger.warning("Client doesn't have a close method, skipping cleanup")
            return

        # Extract client information for better debugging
        client_id = id(client)
        logger.debug(f"Starting cleanup for curl_cffi client: {client_id}")

        # Extract the curl instance if available
        curl_instance = getattr(client, "curl", None)

        # Count async jobs before closing if possible
        async_jobs_count = 0
        if hasattr(curl_instance, "async_jobs"):
            async_jobs_count = (
                len(curl_instance.async_jobs) if curl_instance.async_jobs else 0
            )
            logger.debug(
                f"Client {client_id} has {async_jobs_count} async jobs before closing"
            )

        # Close the client (this releases connection resources)
        try:
            await client.close()
            logger.debug(f"Client {client_id} close() completed")
        except Exception as e:
            logger.warning(f"Error during client.close(): {e}")

        # Run a series of cleanup passes with increasing aggressiveness
        # The first pass is the most gentle
        await _cleanup_all_async_curl_tasks(0.3)

        # Check if we're in a test environment (detected by pytest being loaded)
        # or if there were many async jobs that might need more cleanup
        in_test = "pytest" in sys.modules
        needs_aggressive_cleanup = in_test or async_jobs_count > 5

        if needs_aggressive_cleanup:
            # Second pass with longer timeout and more aggressive cancellation
            logger.debug(f"Running second cleanup pass for client {client_id}")
            await _cleanup_all_async_curl_tasks(0.5)

            # Check if any tasks are still remaining
            curl_tasks = [
                t
                for t in asyncio.all_tasks()
                if "AsyncCurl._force_timeout" in str(t) and not t.done()
            ]

            # If tasks are still pending after two passes, get even more aggressive
            if curl_tasks and in_test:
                logger.debug(
                    f"Running final aggressive cleanup for {len(curl_tasks)} stubborn tasks"
                )
                # Force garbage collection to break any circular references
                import gc

                gc.collect()

                # Final cancellation attempt with long timeout
                await _cleanup_all_async_curl_tasks(1.0)

        logger.debug(f"Successfully closed AsyncSession client {client_id}")
    except asyncio.CancelledError:
        # Handle explicit task cancellation
        logger.warning("Client close operation was cancelled")
    except Exception as e:
        logger.warning(f"Error closing AsyncSession: {str(e)}")
        logger.debug(f"Close error details: {traceback.format_exc()}")


async def _cleanup_all_async_curl_tasks(timeout_seconds: float = 0.5) -> None:
    """Helper function to find and cancel all AsyncCurl tasks.

    This is extracted to a separate function to avoid code duplication and
    enable multiple cleanup passes if needed.

    Args:
        timeout_seconds: How long to wait for tasks to complete after cancellation
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
    client: AsyncSession,
    url: str = "https://data.binance.vision/",
    timeout: float = 10.0,
    retry_count: int = 2,
) -> bool:
    """Test connectivity to a specific URL.

    This function can be used to verify network connectivity before
    making API requests, helping to diagnose network issues early.

    Args:
        client: HTTP client to use for the test
        url: URL to test connectivity to
        timeout: Request timeout in seconds
        retry_count: Number of retry attempts

    Returns:
        True if connection succeeds, False otherwise
    """
    for attempt in range(retry_count + 1):
        try:
            logger.debug(
                f"Testing connectivity to {url} (attempt {attempt+1}/{retry_count+1})"
            )
            response = await client.get(url, timeout=timeout)

            if response.status_code < 400:
                logger.debug(f"Connectivity test successful: {response.status_code}")
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
