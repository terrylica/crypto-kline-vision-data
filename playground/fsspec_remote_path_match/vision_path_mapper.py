#!/usr/bin/env python3

import re
from dataclasses import dataclass
from pathlib import Path

import fsspec
import pendulum
import typer
from rich import print

from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import ChartType, MarketType


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
        if self.market_type == MarketType.FUTURES_COIN and not self.symbol.endswith(
            "_PERP"
        ):
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
        if market_type == MarketType.SPOT:
            return "spot"
        if market_type == MarketType.FUTURES_USDT:
            return "futures/um"
        if market_type == MarketType.FUTURES_COIN:
            return "futures/cm"
        raise ValueError(f"Unsupported market type: {market_type}")

    def get_remote_url(self, components: PathComponents) -> str:
        """Generate remote URL from components."""
        market_path = self._get_market_path(components.market_type)
        file_ext = (
            ".zip.CHECKSUM"
            if components.file_extension.endswith(".CHECKSUM")
            else ".zip"
        )
        filename = f"{components.safe_symbol}-{components.interval}-{components.date_str}{file_ext}"

        url = f"{self.base_url}/data/{market_path}/daily/klines/{components.safe_symbol}/{components.interval}/{filename}"
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
            pattern = r".*/(?:data/)?(spot|futures/[^/]+)/daily/klines/([^/]+)/([^/]+)/([^-]+)-([^-]+)-(\d{4}-\d{2}-\d{2})\.(\w+)$"
            match = re.search(pattern, str(local_path), re.IGNORECASE)

            if not match:
                raise ValueError(f"Can't map to remote URL: {local_path}")

            market, symbol, interval, _, _, date, _ = match.groups()

            return f"{self.base_url}/data/{market}/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date}.zip"

    def create_components_from_params(
        self,
        symbol: str,
        interval: str,
        date: str | pendulum.DateTime,
        market_type: MarketType,
        exchange: str = "binance",
        chart_type: ChartType = ChartType.KLINES,
        file_extension: str = ".arrow",
    ) -> PathComponents:
        """Create path components from parameters."""
        if isinstance(date, str):
            date = pendulum.parse(date)

        return PathComponents(
            exchange=exchange,
            market_type=market_type,
            chart_type=chart_type,
            symbol=symbol,
            interval=interval,
            date=date,
            file_extension=file_extension,
        )


class FSSpecVisionHandler:
    """Handles file operations for Binance Vision data using fsspec.

    Provides unified access to local and remote data files via fsspec.
    """

    def __init__(self, base_cache_dir: str | Path = "cache"):
        """Initialize with cache directory."""
        self.path_mapper = VisionPathMapper(base_cache_dir)
        self.base_cache_dir = Path(base_cache_dir)

    def get_fs_and_path(
        self, url_or_path: str | Path
    ) -> tuple[fsspec.AbstractFileSystem, str]:
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

    def download_to_cache(
        self,
        symbol: str,
        interval: str,
        date: str | pendulum.DateTime,
        market_type: MarketType,
    ) -> Path:
        """Download a file from Binance Vision API to local cache."""
        # Create paths
        components = self.path_mapper.create_components_from_params(
            symbol=symbol,
            interval=interval,
            date=date,
            market_type=market_type,
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

            # Use fsspec's direct copy capability
            with fsspec.open(remote_url, "rb") as source:
                # Here you would normally:
                # 1. Read the zip data
                # 2. Extract the CSV
                # 3. Convert to Arrow format
                # For demo, just creating placeholder
                with fsspec.open(local_path, "wb") as target:
                    target.write(
                        source.read() if hasattr(source, "read") else b"placeholder"
                    )

            logger.info(f"Downloaded and processed data to {local_path}")
            return local_path

        except Exception as e:
            logger.error(f"Error downloading {remote_url}: {e}")
            raise

    def find_all_available_dates(
        self,
        symbol: str,
        interval: str,
        market_type: MarketType,
        start_date: str | pendulum.DateTime,
        end_date: str | pendulum.DateTime,
    ) -> dict[pendulum.DateTime, tuple[str, bool]]:
        """Find all available dates in the local cache or that could be downloaded."""
        # Parse dates if needed
        if isinstance(start_date, str):
            start_date = pendulum.parse(start_date)
        if isinstance(end_date, str):
            end_date = pendulum.parse(end_date)

        # Get all dates in the range
        current_date = start_date
        result = {}

        while current_date <= end_date:
            # Get paths for this date
            components = self.path_mapper.create_components_from_params(
                symbol=symbol,
                interval=interval,
                date=current_date,
                market_type=market_type,
            )
            local_path = self.get_local_path(components)

            # Check if cached
            if self.exists(local_path):
                result[current_date] = (str(local_path), True)
            else:
                # Get remote URL
                components.file_extension = ".zip"
                remote_url = self.get_remote_url(components)
                result[current_date] = (remote_url, False)

            # Move to next day
            current_date = current_date.add(days=1)

        return result


def main(
    symbol: str = typer.Option("BTCUSDT", "-s", "--symbol", help="Trading symbol"),
    interval: str = typer.Option("1m", "-i", "--interval", help="Time interval"),
    date_str: str = typer.Option(
        "2025-04-16", "-d", "--date", help="Date in YYYY-MM-DD format"
    ),
    market_type: str = typer.Option(
        "spot", "-m", "--market-type", help="Market type (spot, um, cm)"
    ),
    base_cache_dir: str = typer.Option(
        "cache", "-c", "--cache-dir", help="Base cache directory"
    ),
):
    """Test the Vision Path Mapper."""
    print("[bold green]Testing VisionPathMapper[/bold green]")

    # Parse market type
    market_enum = None
    if market_type.lower() == "spot":
        market_enum = MarketType.SPOT
    elif market_type.lower() in ["um", "futures_usdt"]:
        market_enum = MarketType.FUTURES_USDT
    elif market_type.lower() in ["cm", "futures_coin"]:
        market_enum = MarketType.FUTURES_COIN
    else:
        print(f"[bold red]Invalid market type: {market_type}[/bold red]")
        return

    # Parse date
    date = pendulum.parse(date_str)

    # Create mapper
    mapper = VisionPathMapper(base_cache_dir)

    # Create components
    components = mapper.create_components_from_params(
        symbol=symbol,
        interval=interval,
        date=date,
        market_type=market_enum,
    )

    # Get paths
    remote_url = mapper.get_remote_url(components)
    local_path = mapper.get_local_path(components)

    print("[bold blue]Components[/bold blue]")
    print(f"Exchange: {components.exchange}")
    print(f"Market Type: {components.market_type}")
    print(f"Chart Type: {components.chart_type}")
    print(f"Symbol: {components.symbol}")
    print(f"Interval: {components.interval}")
    print(f"Date: {components.date}")
    print(f"File Extension: {components.file_extension}")
    print()

    print("[bold blue]Remote URL[/bold blue]")
    print(remote_url)
    print()

    print("[bold blue]Local Path[/bold blue]")
    print(local_path)
    print()

    # Test mapping
    mapped_local = mapper.map_remote_to_local(remote_url)
    mapped_remote = mapper.map_local_to_remote(local_path)

    print("[bold blue]Mapped Remote URL -> Local Path[/bold blue]")
    print(mapped_local)
    print()

    print("[bold blue]Mapped Local Path -> Remote URL[/bold blue]")
    print(mapped_remote)
    print()

    # Check if mappings match
    if str(mapped_local) == str(local_path):
        print("[bold green]✓ Remote->Local mapping is consistent[/bold green]")
    else:
        print("[bold red]✗ Remote->Local mapping is inconsistent[/bold red]")
        print(f"Original: {local_path}")
        print(f"Mapped:   {mapped_local}")

    if mapped_remote == remote_url:
        print("[bold green]✓ Local->Remote mapping is consistent[/bold green]")
    else:
        print("[bold red]✗ Local->Remote mapping is inconsistent[/bold red]")
        print(f"Original: {remote_url}")
        print(f"Mapped:   {mapped_remote}")


if __name__ == "__main__":
    typer.run(main)
