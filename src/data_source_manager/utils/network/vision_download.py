#!/usr/bin/env python
"""Vision API download manager.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from network_utils.py for modularity
"""

from __future__ import annotations

import csv
import tempfile
import zipfile
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any

import attrs
import httpx

from data_source_manager.utils.config import API_TIMEOUT
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.network.client_factory import safely_close_client
from data_source_manager.utils.network.download import DownloadHandler

__all__ = [
    "VisionDownloadManager",
]


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
    _current_tasks: list[Any] = attrs.field(factory=list, init=False)
    _temp_files: list[Path] = attrs.field(factory=list, init=False)

    def __attrs_post_init__(self) -> None:
        """Initialize state after creation."""
        self.download_handler = DownloadHandler(self.client, timeout=API_TIMEOUT)
        self._external_client = self.client is not None

    def _cleanup_tasks(self) -> str | None:
        """Cancel any ongoing download tasks. Returns error message if failed."""
        try:
            if hasattr(self, "_current_tasks") and self._current_tasks:
                logger.debug("[ProgressIndicator] VisionDownloadManager: Cancelling remaining download tasks")
                self._current_tasks = []
            return None
        except AttributeError as e:
            return f"Error cancelling download tasks: {e}"

    def _cleanup_client(self) -> str | None:
        """Safely close the HTTP client. Returns error message if failed."""
        try:
            if not self._external_client and hasattr(self, "client") and self.client is not None:
                logger.debug("[ProgressIndicator] VisionDownloadManager: Safely closing HTTP client")
                safely_close_client(self.client)
                self.client = None
                logger.debug("[ProgressIndicator] VisionDownloadManager: HTTP client closed")
            return None
        except (AttributeError, OSError) as e:
            return f"Error closing HTTP client: {e}"

    def _cleanup_temp_files(self) -> str | None:
        """Clean up any temporary files. Returns error message if failed."""
        try:
            if hasattr(self, "_temp_files") and self._temp_files:
                logger.debug(f"[ProgressIndicator] VisionDownloadManager: Cleaning up {len(self._temp_files)} temp files")
                for temp_file in self._temp_files:
                    try:
                        if hasattr(temp_file, "exists") and temp_file.exists():
                            temp_file.unlink()
                    except OSError as e:
                        logger.debug(f"Error removing temp file {temp_file}: {e}")
                self._temp_files = []
            return None
        except (AttributeError, OSError) as e:
            return f"Error cleaning up temporary files: {e}"

    def _cleanup_resources(self) -> None:
        """Clean up resources used by the download manager."""
        logger.debug("[ProgressIndicator] VisionDownloadManager: Starting resource cleanup")
        cleanup_errors = [err for err in [self._cleanup_tasks(), self._cleanup_client(), self._cleanup_temp_files()] if err]
        for error in cleanup_errors:
            logger.warning(error)
        if cleanup_errors:
            logger.warning(f"[ProgressIndicator] VisionDownloadManager: Cleanup completed with {len(cleanup_errors)} warnings")
        else:
            logger.debug("[ProgressIndicator] VisionDownloadManager: Resource cleanup completed successfully")

    def download_date(self, date: datetime) -> list[list[str]] | None:
        """Download data for a specific date from Binance Vision API.

        Args:
            date: Date to download data for

        Returns:
            List of kline data points if successful, None otherwise
        """
        try:
            # Construct URL for the date
            url_template = (
                "https://data.binance.vision/data/{market_type}/daily/klines/"
                "{symbol}/{interval}/{symbol}-{interval}-{date}.zip"
            )
            url = url_template.format(
                market_type=self.market_type,
                symbol=self.symbol,
                interval=self.interval,
                date=date.strftime("%Y-%m-%d"),
            )

            # Create a temp file for the download
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
            data: list[list[str]] = []
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

    def __enter__(self) -> VisionDownloadManager:
        """Enter context manager."""
        return self

    def __exit__(self, _exc_type: type | None, _exc_val: BaseException | None, _exc_tb: Any) -> None:
        """Exit context manager. Clean up resources."""
        self._cleanup_resources()
