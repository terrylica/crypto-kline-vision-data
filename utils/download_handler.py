#!/usr/bin/env python
"""Utility module for handling downloads with retry logic and progress monitoring.

DEPRECATED: This module is deprecated in favor of utils.network_utils.
It will be removed in a future version.
"""

import asyncio
import logging
import time
import tempfile
import warnings
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta

import httpx
import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from utils.logger_setup import get_logger
from utils.time_utils import enforce_utc_timezone
from utils.network_utils import (
    DownloadProgressTracker as _DownloadProgressTracker,
    DownloadHandler as _DownloadHandler,
    DownloadStalledException as _DownloadStalledException,
    RateLimitException as _RateLimitException,
    VisionDownloadManager as _VisionDownloadManager,
)

logger = get_logger(__name__, "INFO", show_path=False)

# Deprecation warning message template
DEPRECATION_WARNING = (
    "{} is deprecated and will be removed in a future version. "
    "Use utils.network_utils.{} instead."
)


class DownloadProgressTracker(_DownloadProgressTracker):
    """Tracks download progress and detects stalled downloads.

    DEPRECATED: Use utils.network_utils.DownloadProgressTracker instead.
    """

    def __init__(self, total_size: Optional[int] = None, check_interval: int = 5):
        """Initialize progress tracker.

        DEPRECATED: Use utils.network_utils.DownloadProgressTracker instead.

        Args:
            total_size: Expected total size in bytes, if known
            check_interval: How often to check progress in seconds
        """
        warnings.warn(
            DEPRECATION_WARNING.format(
                "DownloadProgressTracker", "DownloadProgressTracker"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(total_size=total_size, check_interval=check_interval)


class DownloadStalledException(_DownloadStalledException):
    """Raised when download progress stalls.

    DEPRECATED: Use utils.network_utils.DownloadStalledException instead.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            DEPRECATION_WARNING.format(
                "DownloadStalledException", "DownloadStalledException"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class RateLimitException(_RateLimitException):
    """Raised when rate limited by the server.

    DEPRECATED: Use utils.network_utils.RateLimitException instead.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            DEPRECATION_WARNING.format("RateLimitException", "RateLimitException"),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class DownloadHandler:
    """Handles file downloads with retry logic and progress monitoring.

    DEPRECATED: Use utils.network_utils.DownloadHandler instead.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        max_retries: int = 5,
        min_wait: int = 4,
        max_wait: int = 60,
        chunk_size: int = 8192,
    ):
        """Initialize download handler.

        DEPRECATED: Use utils.network_utils.DownloadHandler instead.

        Args:
            client: Async HTTP client to use
            max_retries: Maximum number of retry attempts
            min_wait: Minimum wait time between retries (seconds)
            max_wait: Maximum wait time between retries (seconds)
            chunk_size: Size of download chunks in bytes
        """
        warnings.warn(
            DEPRECATION_WARNING.format("DownloadHandler", "DownloadHandler"),
            DeprecationWarning,
            stacklevel=2,
        )
        self.real_handler = _DownloadHandler(
            client=client,
            max_retries=max_retries,
            min_wait=min_wait,
            max_wait=max_wait,
            chunk_size=chunk_size,
        )

    async def download_file(
        self,
        url: str,
        local_path: Path,
        headers: Optional[Dict[str, Any]] = None,
        progress_tracker_kwargs: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Download file with progress monitoring and retries.

        DEPRECATED: Use utils.network_utils.DownloadHandler.download_file instead.

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
        warnings.warn(
            DEPRECATION_WARNING.format(
                "download_file", "DownloadHandler.download_file"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return await self.real_handler.download_file(
            url=url,
            local_path=local_path,
            headers=headers,
            progress_tracker_kwargs=progress_tracker_kwargs,
        )


class VisionDownloadManager(_VisionDownloadManager):
    """Handles downloading Vision data files with validation and processing.

    DEPRECATED: Use utils.network_utils.VisionDownloadManager instead.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        interval: str,
        market_type: str = "spot",
    ):
        """Initialize the download manager.

        DEPRECATED: Use utils.network_utils.VisionDownloadManager instead.

        Args:
            client: HTTP client for downloads
            symbol: Trading pair symbol
            interval: Time interval
            market_type: Market type (spot, futures_usdt, futures_coin)
        """
        warnings.warn(
            DEPRECATION_WARNING.format(
                "VisionDownloadManager", "VisionDownloadManager"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(
            client=client,
            symbol=symbol,
            interval=interval,
            market_type=market_type,
        )

    def _get_checksum_url(self, date: datetime) -> str:
        """Get checksum URL for a specific date.

        Args:
            date: Target date

        Returns:
            URL for the checksum file
        """
        # Import and use TimeRangeManager for consistent timezone handling
        date = enforce_utc_timezone(date)

        # Note: This assumes the vision_constraints module has this function
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
        # Import and use TimeRangeManager for consistent timezone handling
        date = enforce_utc_timezone(date)

        # Note: This assumes the vision_constraints module has this function
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

            from utils.cache_validator import CacheValidator

            return CacheValidator.validate_cache_checksum(file_path, expected)
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
        # Ensure date has proper timezone using direct function call
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

            # Read CSV data with detailed error handling
            try:
                logger.info(f"[{debug_id}] Reading CSV data from {data_file}")
                csv_start = time.time()

                # Log zip file contents for diagnosis
                try:
                    import zipfile

                    with zipfile.ZipFile(data_file, "r") as zip_ref:
                        file_list = zip_ref.namelist()
                        logger.info(f"[{debug_id}] Zip file contents: {file_list}")

                        # Log first few lines of the CSV file for diagnosis
                        if file_list:
                            with zip_ref.open(file_list[0], "r") as f:
                                sample_lines = [
                                    next(f).decode("utf-8").strip() for _ in range(5)
                                ]
                                logger.info(
                                    f"[{debug_id}] Sample CSV lines: {sample_lines}"
                                )
                except Exception as e:
                    logger.error(f"[{debug_id}] Error examining zip contents: {e}")

                df = pd.read_csv(
                    data_file,
                    compression="zip",
                    names=[
                        "open_time",
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
                        "ignored",
                    ],
                )
                logger.info(
                    f"[{debug_id}] CSV reading completed in {time.time() - csv_start:.2f}s"
                )

                if df.empty:
                    logger.error(
                        f"[{debug_id}] Empty DataFrame after reading CSV for {date}"
                    )
                    return None

                logger.info(
                    f"[{debug_id}] Raw DataFrame shape: {df.shape}, head: {df.head(2).to_dict('records')}"
                )

                # Detect timestamp format and convert
                from core.vision_constraints import detect_timestamp_unit

                sample_ts = df["open_time"].iloc[0]
                ts_unit = detect_timestamp_unit(sample_ts)
                logger.info(
                    f"[{debug_id}] Detected timestamp unit: {ts_unit} for sample value: {sample_ts}"
                )

                # Convert timestamps
                processing_start = time.time()
                df["open_time"] = pd.to_datetime(df["open_time"], unit=ts_unit)
                df["open_time"] = df["open_time"].dt.floor("s") + pd.Timedelta(
                    microseconds=0
                )
                logger.info(
                    f"[{debug_id}] Converted open_time, first value: {df['open_time'].iloc[0]}"
                )

                # Handle close_time
                df["close_time"] = df["close_time"].astype(np.int64)
                if len(str(df["close_time"].iloc[0])) == 19:  # nanoseconds
                    df["close_time"] = (
                        df["close_time"] // 1000
                    )  # Convert to microseconds
                df["close_time"] = (df["close_time"].astype(np.int64) + 999) * 1000
                logger.info(
                    f"[{debug_id}] Converted close_time, first value: {df['close_time'].iloc[0]}"
                )

                # Set index
                df.set_index("open_time", inplace=True)
                df = df.drop(columns=["ignored"])

                # Log shape after processing
                logger.info(
                    f"[{debug_id}] Processed DataFrame shape: {df.shape}, index range: {df.index.min()} to {df.index.max()}"
                )

                # Ensure UTC timezone using direct function call
                if df.index.tz is None:
                    logger.info(
                        f"[{debug_id}] Setting timezone for DatetimeIndex without timezone"
                    )
                    df.index = pd.DatetimeIndex(
                        [enforce_utc_timezone(dt) for dt in df.index],
                        name=df.index.name,
                    )
                else:
                    logger.info(
                        f"[{debug_id}] Converting existing timezone to UTC: {df.index.tz}"
                    )
                    df.index = pd.DatetimeIndex(
                        [enforce_utc_timezone(dt) for dt in df.index.to_pydatetime()],
                        name=df.index.name,
                    )
                logger.info(
                    f"[{debug_id}] Final DataFrame processing completed in {time.time() - processing_start:.2f}s"
                )
                logger.info(
                    f"[{debug_id}] Total processing completed in {time.time() - download_start:.2f}s"
                )
                logger.info(
                    f"[{debug_id}] Final DataFrame shape: {df.shape}, columns: {list(df.columns)}"
                )

                return df

            except pd.errors.EmptyDataError:
                logger.error(f"[{debug_id}] Empty data file for {date}")
                return None
            except Exception as e:
                logger.error(f"[{debug_id}] Error processing data for {date}: {str(e)}")
                logger.error(f"[{debug_id}] Error details:", exc_info=True)
                return None

        except Exception as e:
            logger.error(f"[{debug_id}] Error downloading data for {date}: {str(e)}")
            logger.error(f"[{debug_id}] Error details:", exc_info=True)
            return None

        finally:
            # Cleanup temporary files
            try:
                logger.info(f"[{debug_id}] Cleaning up temporary files")
                data_file.unlink(missing_ok=True)
                checksum_file.unlink(missing_ok=True)
                temp_dir.rmdir()
            except Exception as e:
                logger.error(
                    f"[{debug_id}] Error cleaning up temporary files: {str(e)}"
                )
