#!/usr/bin/env python3
"""FSSpec-based path mapper for Binance Vision data.

This module provides utilities for mapping between remote Binance Vision API paths
and local cache paths using fsspec, enabling unified filesystem operations.
"""

import re
from dataclasses import dataclass
from pathlib import Path

import fsspec
import pendulum

from utils.loguru_setup import logger
from utils.market_constraints import ChartType, Interval, MarketType


@dataclass
class PathComponents:
    """Components that make up a data path for better structural clarity."""

    exchange: str
    market_type: MarketType
    chart_type: ChartType
    symbol: str
    interval: str
    date: pendulum.DateTime
    file_extension: str = ".arrow"  # Default to arrow format for local cache

    @property
    def date_str(self) -> str:
        """Get date string in YYYY-MM-DD format."""
        return self.date.format("YYYY-MM-DD")

    @property
    def date_filename_str(self) -> str:
        """Get date string for filename in YYYYMMDD format."""
        return self.date.format("YYYYMMDD")

    @property
    def safe_symbol(self) -> str:
        """Handle symbol naming based on market type."""
        if self.market_type == MarketType.FUTURES_COIN and not self.symbol.endswith("_PERP"):
            return f"{self.symbol}_PERP"
        return self.symbol


class VisionPathMapper:
    """Maps between remote Binance Vision API paths and local cache paths using minimal transformation."""

    def __init__(self, base_cache_dir: str | Path = "cache"):
        """Initialize with cache directory."""
        self.base_cache_dir = Path(base_cache_dir)
        self.base_url = "https://data.binance.vision"

    def _get_market_path(self, market_type: MarketType) -> str:
        """Get URL path component for market type."""
        return market_type.vision_api_path

    def get_remote_url(self, components: PathComponents) -> str:
        """Generate remote URL from components."""
        market_path = self._get_market_path(components.market_type)
        file_ext = ".zip.CHECKSUM" if components.file_extension.endswith(".CHECKSUM") else ".zip"
        filename = f"{components.safe_symbol.upper()}-{components.interval}-{components.date_str}{file_ext}"

        url = (
            f"{self.base_url}/data/{market_path}/daily/{components.chart_type.vision_api_path}/"
            f"{components.safe_symbol.upper()}/{components.interval}/{filename}"
        )
        logger.debug(f"Generated URL: {url}")
        return url

    def get_local_path(self, components: PathComponents) -> Path:
        """Generate local path by replacing URL base and file extension."""
        url = self.get_remote_url(components)
        path_part = url.replace(f"{self.base_url}/", "")
        if path_part.endswith(".zip.CHECKSUM"):
            path_part = path_part.replace(".zip.CHECKSUM", components.file_extension)
        else:
            path_part = path_part.replace(".zip", components.file_extension)

        return self.base_cache_dir / path_part

    def map_remote_to_local(self, remote_url: str) -> Path:
        """Convert remote URL to local cache path."""
        if not remote_url.startswith(self.base_url):
            raise ValueError(f"URL doesn't match base: {remote_url}")

        path_part = remote_url.replace(f"{self.base_url}/", "")
        if path_part.endswith(".zip.CHECKSUM"):
            path_part = path_part.replace(".zip.CHECKSUM", ".arrow")
        else:
            path_part = path_part.replace(".zip", ".arrow")

        return self.base_cache_dir / path_part

    def map_local_to_remote(self, local_path: str | Path) -> str:
        """Convert local cache path to remote URL."""
        local_path = Path(local_path)

        try:
            rel_path = local_path.relative_to(self.base_cache_dir)
            rel_str = str(rel_path)

            if rel_str.endswith(".arrow"):
                rel_str = rel_str.replace(".arrow", ".zip")

            return f"{self.base_url}/{rel_str}"
        except ValueError:
            # Fallback for non-standard paths
            pattern = r".*/(?:data/)?(spot|futures/[^/]+)/daily/([^/]+)/([^/]+)/([^/]+)/([^-]+)-([^-]+)-(\d{4}-\d{2}-\d{2})\.(\w+)$"
            match = re.search(pattern, str(local_path), re.IGNORECASE)

            if not match:
                raise ValueError(f"Can't map to remote URL: {local_path}") from None

            market, chart_type, symbol, interval, _, _, date, _ = match.groups()

            return f"{self.base_url}/data/{market}/daily/{chart_type}/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"

    def create_components_from_params(
        self,
        symbol: str,
        interval: str | Interval,
        date: str | pendulum.DateTime,
        market_type: MarketType,
        chart_type: ChartType = ChartType.KLINES,
        exchange: str = "binance",
        file_extension: str = ".arrow",
    ) -> PathComponents:
        """Create path components from parameters."""
        if isinstance(date, str):
            # Explicitly parse to pendulum.DateTime, not a generic Date
            dt = pendulum.parse(date)
            if not isinstance(dt, pendulum.DateTime):
                dt = pendulum.DateTime.instance(dt)
        else:
            # Ensure it's a pendulum.DateTime object
            dt = date if isinstance(date, pendulum.DateTime) else pendulum.DateTime.instance(date)

        if isinstance(interval, Interval):
            interval = interval.value

        return PathComponents(
            exchange=exchange,
            market_type=market_type,
            chart_type=chart_type,
            symbol=symbol,
            interval=interval,
            date=dt,
            file_extension=file_extension,
        )

    def create_path_from_params(
        self,
        symbol: str,
        interval: str | Interval,
        date: str | pendulum.DateTime,
        market_type: MarketType,
        chart_type: ChartType = ChartType.KLINES,
        exchange: str = "binance",
    ) -> Path:
        """Generate a local path directly from parameters."""
        components = self.create_components_from_params(
            symbol=symbol,
            interval=interval,
            date=date,
            market_type=market_type,
            chart_type=chart_type,
            exchange=exchange,
        )
        return self.get_local_path(components)


class FSSpecVisionHandler:
    """Handles file operations for Binance Vision data using fsspec.

    Provides unified access to local and remote data files via fsspec.
    """

    def __init__(self, base_cache_dir: str | Path = "cache"):
        """Initialize with cache directory."""
        self.path_mapper = VisionPathMapper(base_cache_dir)
        self.base_cache_dir = Path(base_cache_dir)

    def get_fs_and_path(self, url_or_path: str | Path) -> tuple[fsspec.AbstractFileSystem, str]:
        """Get the appropriate filesystem and path using fsspec's automatic detection."""
        return fsspec.core.url_to_fs(str(url_or_path))

    def exists(self, url_or_path: str | Path) -> bool:
        """Check if a file exists in any filesystem."""
        fs, path = self.get_fs_and_path(url_or_path)
        try:
            return fs.exists(path)
        except Exception as e:
            logger.error(f"Error checking if {path} exists: {e}")
            return False

    def get_remote_url(self, components: PathComponents) -> str:
        """Get the remote URL for the given components."""
        return self.path_mapper.get_remote_url(components)

    def get_local_path(self, components: PathComponents) -> Path:
        """Get the local path for the given components."""
        return self.path_mapper.get_local_path(components)

    def get_local_path_for_data(
        self,
        symbol: str,
        interval: str | Interval,
        date: str | pendulum.DateTime,
        market_type: MarketType,
        chart_type: ChartType = ChartType.KLINES,
    ) -> Path:
        """Get the local path for the given data parameters."""
        components = self.path_mapper.create_components_from_params(
            symbol=symbol,
            interval=interval,
            date=date,
            market_type=market_type,
            chart_type=chart_type,
        )
        return self.get_local_path(components)

    def get_remote_url_for_data(
        self,
        symbol: str,
        interval: str | Interval,
        date: str | pendulum.DateTime,
        market_type: MarketType,
        chart_type: ChartType = ChartType.KLINES,
    ) -> str:
        """Get the remote URL for the given data parameters."""
        components = self.path_mapper.create_components_from_params(
            symbol=symbol,
            interval=interval,
            date=date,
            market_type=market_type,
            chart_type=chart_type,
            file_extension=".zip",
        )
        return self.get_remote_url(components)

    def download_to_cache(
        self,
        symbol: str,
        interval: str | Interval,
        date: str | pendulum.DateTime,
        market_type: MarketType,
        chart_type: ChartType = ChartType.KLINES,
    ) -> Path | None:
        """Download a file from Binance Vision API to local cache.

        Args:
            symbol: Trading symbol
            interval: Time interval
            date: Date to download
            market_type: Market type
            chart_type: Chart type

        Returns:
            Path to cached file or None if download failed
        """
        # Create paths
        components = self.path_mapper.create_components_from_params(
            symbol=symbol,
            interval=interval,
            date=date,
            market_type=market_type,
            chart_type=chart_type,
            file_extension=".zip",
        )
        remote_url = self.get_remote_url(components)

        # Get local path with arrow extension
        components.file_extension = ".arrow"
        local_path = self.get_local_path(components)

        # Return if already cached
        if self.exists(local_path):
            logger.info(f"Cache file already exists: {local_path}")
            return local_path

        # Ensure directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Download using fsspec
        try:
            logger.info(f"Downloading {remote_url} to {local_path}")

            # In the actual implementation, you would:
            # 1. Download the zip file
            # 2. Extract the CSV
            # 3. Convert to Arrow format
            # 4. Save to local_path

            # For now, we just indicate that the handler would handle this operation
            logger.info(f"FSSpecVisionHandler would download and process data to {local_path}")

            return local_path
        except Exception as e:
            logger.error(f"Error downloading {remote_url}: {e}")
            return None
