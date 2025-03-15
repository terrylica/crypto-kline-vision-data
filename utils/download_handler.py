#!/usr/bin/env python
"""Utility module for handling downloads with retry logic and progress monitoring."""

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional, Dict, Any

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from ml_feature_set.utils.logger_setup import get_logger

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


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

    def update(self, chunk_size: int) -> bool:
        """Update progress and check if download is stalled.

        Args:
            chunk_size: Size of the latest chunk in bytes

        Returns:
            False if download appears stalled, True otherwise
        """
        current_time = time.monotonic()
        self.bytes_received += chunk_size

        # Check progress every check_interval seconds
        if current_time - self.last_progress_time >= self.check_interval:
            bytes_per_sec = (self.bytes_received - self.last_bytes) / self.check_interval

            # If progress is less than 1KB/s for check_interval seconds, consider it stalled
            if bytes_per_sec < 1024:
                logger.warning(
                    f"Download stalled: {bytes_per_sec:.2f} B/s " f"({self.bytes_received}/{self.total_size or 'unknown'} bytes)"
                )
                return False

            self.last_progress_time = current_time
            self.last_bytes = self.bytes_received

        return True


class DownloadStalledException(Exception):
    """Raised when download progress stalls."""

    pass


class RateLimitException(Exception):
    """Raised when rate limited by the server."""

    pass


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
        retry=retry_if_exception_type((DownloadStalledException, RateLimitException, httpx.TimeoutException, httpx.NetworkError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def download_file(
        self, url: str, local_path: Path, headers: Optional[Dict[str, Any]] = None, progress_tracker_kwargs: Optional[Dict[str, Any]] = None
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
                progress = DownloadProgressTracker(total_size, **progress_tracker_kwargs)

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

        except (DownloadStalledException, RateLimitException, httpx.TimeoutException, httpx.NetworkError) as e:
            # Let these exceptions propagate for retry
            raise

        except Exception as e:
            logger.error(f"Unexpected error during download: {str(e)}")
            return False

        finally:
            # Cleanup temp file if it exists
            temp_path.unlink(missing_ok=True)
