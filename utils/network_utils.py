#!/usr/bin/env python
"""Network utilities for handling HTTP client creation and file downloads.

This module centralizes network-related functionality, including:
1. HTTP client creation with standardized configuration
2. Download handling with progress tracking and retry logic
3. Rate limiting management and stall detection

By consolidating these utilities, we ensure consistent network behavior across the application.
"""

import asyncio
import logging
import time
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import (
    Dict,
    Any,
    Optional,
    Union,
    Literal,
    List,
)

import aiohttp
import httpx
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
    client_type: Literal["aiohttp", "httpx"] = "aiohttp",
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: Optional[int] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Union[aiohttp.ClientSession, httpx.AsyncClient]:
    """Create a standardized HTTP client of the specified type.

    Provides a unified interface for creating both aiohttp and httpx clients
    with consistent configuration options.

    Args:
        client_type: The type of client to create ("aiohttp" or "httpx")
        timeout: Request timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers to include in all requests
        **kwargs: Additional client-specific configuration options

    Returns:
        Configured HTTP client of the requested type with standard settings

    Raises:
        ValueError: If an unsupported client_type is specified
    """
    # Use default max connections based on client type if not specified
    if max_connections is None:
        max_connections = 20 if client_type == "aiohttp" else 13

    # Merge default headers with custom headers
    default_headers = {
        "Accept": DEFAULT_ACCEPT_HEADER,
        "User-Agent": DEFAULT_USER_AGENT,
    }

    if headers:
        default_headers.update(headers)

    # Create the appropriate client type
    if client_type == "aiohttp":
        return create_aiohttp_client(
            timeout=timeout,
            max_connections=max_connections,
            headers=default_headers,
            **kwargs,
        )
    elif client_type == "httpx":
        return create_httpx_client(
            timeout=timeout,
            max_connections=max_connections,
            headers=default_headers,
            **kwargs,
        )
    else:
        raise ValueError(f"Unsupported client type: {client_type}")


def create_aiohttp_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 20,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> aiohttp.ClientSession:
    """Factory function to create a pre-configured aiohttp ClientSession.

    Args:
        timeout: Total timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers
        **kwargs: Additional client configuration options

    Returns:
        Configured aiohttp ClientSession with standardized settings
    """
    client_headers = {
        "Accept": DEFAULT_ACCEPT_HEADER,
        "User-Agent": DEFAULT_USER_AGENT,
    }

    if headers:
        client_headers.update(headers)

    client_timeout = aiohttp.ClientTimeout(
        total=timeout, connect=3, sock_connect=3, sock_read=5
    )
    connector = aiohttp.TCPConnector(limit=max_connections, force_close=False)

    client_kwargs = {
        "timeout": client_timeout,
        "connector": connector,
        "headers": client_headers,
    }

    # Add any additional kwargs
    client_kwargs.update(kwargs)

    return aiohttp.ClientSession(**client_kwargs)


def create_httpx_client(
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    max_connections: int = 13,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """Factory function to create a pre-configured httpx AsyncClient.

    Args:
        timeout: Total timeout in seconds
        max_connections: Maximum number of connections
        headers: Optional custom headers
        **kwargs: Additional client configuration options

    Returns:
        Configured httpx AsyncClient with standardized settings
    """
    limits = httpx.Limits(
        max_connections=max_connections, max_keepalive_connections=max_connections
    )
    timeout_config = httpx.Timeout(timeout)

    client_headers = {
        "Accept": DEFAULT_ACCEPT_HEADER,
        "User-Agent": DEFAULT_USER_AGENT,
    }

    if headers:
        client_headers.update(headers)

    client_kwargs = {
        "limits": limits,
        "timeout": timeout_config,
        "headers": client_headers,
    }

    # Add any additional kwargs
    client_kwargs.update(kwargs)

    return httpx.AsyncClient(**client_kwargs)


# ----- Download Handling -----


class DownloadException(Exception):
    """Base class for download-related exceptions."""

    pass


class DownloadStalledException(DownloadException):
    """Raised when download progress stalls."""

    pass


class RateLimitException(DownloadException):
    """Raised when rate limited by the server."""

    pass


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
    """Handles file downloads with retry logic and progress monitoring."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        max_retries: int = 5,
        min_wait: int = 4,
        max_wait: int = 60,
        chunk_size: int = 8192,
    ):
        """Initialize download handler.

        Args:
            client: Async HTTP client to use
            max_retries: Maximum number of retry attempts
            min_wait: Minimum wait time between retries (seconds)
            max_wait: Maximum wait time between retries (seconds)
            chunk_size: Size of download chunks in bytes
        """
        self.client = client
        self.max_retries = max_retries
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.chunk_size = chunk_size

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type(
            (
                DownloadStalledException,
                RateLimitException,
                httpx.TimeoutException,
                httpx.NetworkError,
            )
        ),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def download_file(
        self,
        url: str,
        local_path: Path,
        headers: Optional[Dict[str, Any]] = None,
        progress_tracker_kwargs: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Download file with progress monitoring and retries.

        Args:
            url: URL to download from
            local_path: Path to save the file to
            headers: Optional HTTP headers
            progress_tracker_kwargs: Optional kwargs for DownloadProgressTracker

        Returns:
            True if download successful, False otherwise

        Raises:
            DownloadStalledException: If download progress stalls
            RateLimitException: If rate limited by server
            httpx.TimeoutException: If request times out
            httpx.NetworkError: If network error occurs
        """
        temp_path = local_path.with_suffix(".tmp")
        progress_tracker_kwargs = progress_tracker_kwargs or {}

        try:
            async with self.client.stream("GET", url, headers=headers) as response:
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limit hit. Retry after {retry_after}s")
                    await asyncio.sleep(retry_after)  # Honor the retry-after header
                    raise RateLimitException()

                if response.status_code != 200:
                    logger.error(f"Download failed with status {response.status_code}")
                    return False

                total_size = int(response.headers.get("content-length", 0))
                progress = DownloadProgressTracker(
                    total_size, **progress_tracker_kwargs
                )

                with open(temp_path, "wb") as f:
                    async for chunk in response.aiter_bytes(self.chunk_size):
                        if not progress.update(len(chunk)):
                            msg = f"Download stalled at {progress.bytes_received}/{total_size} bytes"
                            logger.warning(msg)
                            raise DownloadStalledException(msg)
                        f.write(chunk)

            # Only move to final location if download completed successfully
            temp_path.rename(local_path)
            return True

        except (
            DownloadStalledException,
            RateLimitException,
            httpx.TimeoutException,
            httpx.NetworkError,
        ) as e:
            # Let these exceptions propagate for retry
            raise

        except Exception as e:
            logger.error(f"Unexpected error during download: {str(e)}")
            return False

        finally:
            # Cleanup temp file if it exists
            temp_path.unlink(missing_ok=True)


# ----- Batch Download Handling -----


async def download_files_concurrently(
    client: httpx.AsyncClient,
    urls: List[str],
    local_paths: List[Path],
    max_concurrent: int = 5,
    **download_kwargs: Any,
) -> List[bool]:
    """Download multiple files concurrently with throttling.

    Args:
        client: HTTP client to use for downloads
        urls: List of URLs to download
        local_paths: List of local paths to save files to (must match urls length)
        max_concurrent: Maximum number of concurrent downloads
        **download_kwargs: Additional kwargs to pass to download_file

    Returns:
        List of booleans indicating success/failure of each download (matches urls order)
    """
    if len(urls) != len(local_paths):
        raise ValueError("URLs and local paths must have the same length")

    # Create download handler
    download_handler = DownloadHandler(client)

    # Use semaphore to limit concurrency
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _download_with_semaphore(url: str, path: Path) -> bool:
        async with semaphore:
            return await download_handler.download_file(url, path, **download_kwargs)

    # Create download tasks
    download_tasks = [
        _download_with_semaphore(url, path) for url, path in zip(urls, local_paths)
    ]

    # Run downloads concurrently
    results = await asyncio.gather(*download_tasks, return_exceptions=True)

    # Process results, converting exceptions to False
    return [result if isinstance(result, bool) else False for result in results]


# ----- API Request Handling -----


async def make_api_request(
    client: Union[httpx.AsyncClient, aiohttp.ClientSession],
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    max_retries: int = 3,
    retry_delay: int = 2,
) -> Optional[Dict[str, Any]]:
    """Make an API request with retry logic and error handling.

    Args:
        client: HTTP client (either httpx.AsyncClient or aiohttp.ClientSession)
        url: API URL to request
        params: Query parameters
        headers: Request headers
        max_retries: Maximum number of retry attempts
        retry_delay: Base delay between retries in seconds

    Returns:
        JSON response data or None if the request failed
    """
    params = params or {}
    headers = headers or {}

    for retry in range(max_retries):
        try:
            if isinstance(client, httpx.AsyncClient):
                response = await client.get(url, params=params, headers=headers)

                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", retry_delay))
                    logger.warning(
                        f"Rate limited by API, retrying after {retry_after}s (attempt {retry+1}/{max_retries})"
                    )
                    await asyncio.sleep(retry_after)
                    continue

                # Handle other HTTP errors
                # Some clients (like aiohttp) have awaitable raise_for_status, while others (like httpx in tests) don't
                # This conditional handling prevents "coroutine was never awaited" warnings while supporting both types
                if hasattr(response.raise_for_status, "__await__"):
                    await response.raise_for_status()
                else:
                    response.raise_for_status()

                # Handle both AsyncMock and real response cases
                if hasattr(response.json, "__await__"):
                    return await response.json()
                return response.json()

            elif isinstance(client, aiohttp.ClientSession):
                async with client.get(url, params=params, headers=headers) as response:
                    # Check for rate limiting
                    if response.status == 429:
                        retry_after = int(
                            response.headers.get("Retry-After", retry_delay)
                        )
                        logger.warning(
                            f"Rate limited by API, retrying after {retry_after}s (attempt {retry+1}/{max_retries})"
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    # Handle other HTTP errors
                    # Some clients (like aiohttp) have awaitable raise_for_status, while others don't
                    # This conditional handling prevents "coroutine was never awaited" warnings
                    if hasattr(response.raise_for_status, "__await__"):
                        await response.raise_for_status()
                    else:
                        response.raise_for_status()
                    return await response.json()
            else:
                logger.error(f"Unsupported client type: {type(client)}")
                return None

        except (httpx.HTTPStatusError, aiohttp.ClientResponseError) as e:
            logger.warning(
                f"HTTP error during API request: {str(e)}, retrying ({retry+1}/{max_retries})"
            )
            await asyncio.sleep(retry_delay * (retry + 1))
        except httpx.TimeoutException as e:
            logger.warning(
                f"Timeout during API request: {str(e)}, retrying ({retry+1}/{max_retries})"
            )
            await asyncio.sleep(retry_delay * (retry + 1))
        except aiohttp.ClientConnectorError as e:
            logger.warning(
                f"Connection error during API request: {str(e)}, retrying ({retry+1}/{max_retries})"
            )
            await asyncio.sleep(retry_delay * (retry + 1))
        except Exception as e:
            logger.warning(
                f"Unexpected error during API request: {str(e)}, retrying ({retry+1}/{max_retries})"
            )
            await asyncio.sleep(retry_delay * (retry + 1))

    logger.error(f"Failed to complete API request after {max_retries} retries")
    return None


class VisionDownloadManager:
    """Handles downloading Vision data files with validation and processing."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        interval: str,
        market_type: str = "spot",
    ):
        """Initialize the download manager.

        Args:
            client: HTTP client for downloads
            symbol: Trading pair symbol
            interval: Time interval
            market_type: Market type (spot, futures_usdt, futures_coin)
        """
        self.client = client
        self.symbol = symbol
        self.interval = interval
        self.market_type = market_type
        self.download_handler = DownloadHandler(
            client, max_retries=5, min_wait=4, max_wait=60
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
        """Verify file checksum.

        Args:
            file_path: Path to data file
            checksum_path: Path to checksum file

        Returns:
            Verification status
        """
        try:
            with open(checksum_path, "r") as f:
                expected = f.read().strip().split()[0]

            from utils.validation_utils import (
                validate_cache_integrity,
                calculate_checksum,
            )

            actual = calculate_checksum(file_path)
            return actual == expected
        except Exception as e:
            logger.error(f"Error verifying checksum: {e}")
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

        date = enforce_utc_timezone(date)

        # Add debugging timestamp
        debug_id = f"{self.symbol}_{self.interval}_{date.strftime('%Y%m%d')}_{int(time.time())}"
        logger.info(
            f"[{debug_id}] Starting download for {self.symbol} {self.interval} on {date.strftime('%Y-%m-%d')}"
        )

        # Create temporary directory for downloads
        temp_dir = Path(tempfile.mkdtemp())
        data_file = (
            temp_dir
            / f"{self.symbol}_{self.interval}_{date.strftime('%Y%m%d')}_data.zip"
        )
        checksum_file = (
            temp_dir
            / f"{self.symbol}_{self.interval}_{date.strftime('%Y%m%d')}_checksum"
        )

        try:
            # Download data and checksum files
            data_url = self._get_data_url(date)
            checksum_url = self._get_checksum_url(date)

            logger.info(
                f"[{debug_id}] Downloading data for {date.strftime('%Y-%m-%d')} from:"
            )
            logger.info(f"[{debug_id}] Data URL: {data_url}")
            logger.info(f"[{debug_id}] Checksum URL: {checksum_url}")

            download_start = time.time()
            success = await asyncio.gather(
                self.download_file(data_url, data_file),
                self.download_file(checksum_url, checksum_file),
            )
            download_time = time.time() - download_start

            logger.info(
                f"[{debug_id}] Download completed in {download_time:.2f}s, success: {success}"
            )

            if not all(success):
                logger.error(f"[{debug_id}] Failed to download files for {date}")
                return None

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

                # Set index name to match expected format
                if df.index.name != "open_time":
                    df.index.name = "open_time"

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

                # Define column names explicitly for Binance kline data format
                column_names = [
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_volume",
                    "trades",
                    "taker_buy_volume",
                    "taker_buy_quote_volume",
                    "ignore",
                ]

                # Use StringIO for CSV parsing
                import io

                try:
                    df = pd.read_csv(
                        io.BytesIO(file_content), header=None, names=column_names
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
                        "quote_volume",
                        "trades",
                        "taker_buy_volume",
                        "taker_buy_quote_volume",
                    ]

                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")

                    # Convert timestamp column (milliseconds since epoch) to datetime
                    if "timestamp" in df.columns and not df.empty:
                        try:
                            # Most Binance kline timestamps are in milliseconds
                            logger.info(
                                f"{log_prefix} Converting timestamp column (milliseconds to datetime)"
                            )

                            # Simple approach for Binance data: treat as ms directly
                            df["timestamp"] = pd.to_datetime(
                                df["timestamp"], unit="ms", utc=True
                            )
                            df = df.set_index("timestamp")

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
                                f"{log_prefix} Error converting timestamp: {str(e)}"
                            )
                            # Fallback to a more flexible approach
                            try:
                                # Try autodetecting the format based on the number of digits
                                timestamp_val = df["timestamp"].astype(float)
                                sample_ts = (
                                    timestamp_val.iloc[0]
                                    if not timestamp_val.empty
                                    else 0
                                )
                                digits = len(str(int(sample_ts)))

                                if digits > 13:  # Assume microseconds or finer
                                    df["timestamp"] = pd.to_datetime(
                                        timestamp_val / 1_000_000, unit="s", utc=True
                                    )
                                elif (
                                    digits > 10
                                ):  # Milliseconds (standard Binance format)
                                    df["timestamp"] = pd.to_datetime(
                                        timestamp_val / 1_000, unit="s", utc=True
                                    )
                                else:  # Seconds
                                    df["timestamp"] = pd.to_datetime(
                                        timestamp_val, unit="s", utc=True
                                    )

                                df = df.set_index("timestamp")
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
                    # If there's a specific CSV parsing error, try a more basic approach
                    try:
                        # Attempt with more lenient parsing
                        df = pd.read_csv(
                            io.BytesIO(file_content),
                            header=None,
                            names=column_names,
                            on_bad_lines="skip",
                        )
                        logger.info(
                            f"{log_prefix} Parsed with lenient mode, got {len(df)} rows"
                        )
                        return df
                    except Exception as fallback_e:
                        logger.error(
                            f"{log_prefix} Fallback parsing also failed: {str(fallback_e)}"
                        )
                        return pd.DataFrame()

    except Exception as e:
        logger.error(f"{log_prefix} Error reading zip file: {str(e)}")
        return pd.DataFrame()
