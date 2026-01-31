#!/usr/bin/env python
"""Download handling utilities with progress tracking and concurrency.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from network_utils.py for modularity
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import attrs
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
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    HTTP_NOT_FOUND,
    HTTP_OK,
    MAXIMUM_CONCURRENT_DOWNLOADS,
    MEDIUM_BATCH_SIZE,
    SMALL_BATCH_SIZE,
)
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.network.client_factory import create_client, safely_close_client
from data_source_manager.utils.network.exceptions import (
    DownloadException,
    DownloadStalledException,
    RateLimitException,
)

__all__ = [
    "DownloadHandler",
    "DownloadProgressTracker",
    "download_files_concurrently",
]


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

    def __attrs_post_init__(self) -> None:
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

    def __attrs_post_init__(self) -> None:
        """Initialize state after creation."""
        self._client_is_external = self.client is not None

    def __enter__(self) -> DownloadHandler:
        """Enter context manager."""
        if not self.client:
            self.client = create_client(timeout=self.timeout)
            self._client_is_external = False
        return self

    def __exit__(self, _exc_type: type | None, _exc_val: BaseException | None, _exc_tb: Any) -> None:
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

    def _close_client(self) -> None:
        """Safely close the HTTP client if we own it."""
        if self.client and not self._client_is_external:
            safely_close_client(self.client)
            self.client = None


def download_files_concurrently(
    client: Any,
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
