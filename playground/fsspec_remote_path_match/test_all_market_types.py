#!/usr/bin/env python3

from pathlib import Path

import pendulum
import typer
from rich.console import Console
from rich.table import Table
from vision_path_mapper import FSSpecVisionHandler, VisionPathMapper

from utils.market_constraints import MarketType

# Create a console for rich output
console = Console()


def print_test_results(
    market_name: str,
    symbol: str,
    remote_url: str,
    local_path: str,
    mapped_remote: str,
    mapped_local: str,
):
    """Print test results in a table."""
    table = Table(title=f"{market_name} Market Test Results")
    table.add_column("Type", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Symbol", symbol)
    table.add_row("Remote URL", remote_url)
    table.add_row("Local Path", local_path)
    table.add_row("Mapped Remote", mapped_remote)
    table.add_row("Mapped Local", str(mapped_local))

    # Add consistency checks
    if remote_url == mapped_remote:
        table.add_row("Remote Consistency", "✓ Consistent", style="green")
    else:
        table.add_row("Remote Consistency", "✗ Inconsistent", style="red")

    if str(local_path) == str(mapped_local):
        table.add_row("Local Consistency", "✓ Consistent", style="green")
    else:
        table.add_row("Local Consistency", "✗ Inconsistent", style="red")

    console.print(table)
    console.print("")


def test_market_mapping(
    market_type: MarketType,
    symbol: str,
    interval: str = "1m",
    date_str: str = "2025-04-16",
    base_cache_dir: str = "cache",
) -> tuple[str, str, str, Path]:
    """Test path mapping for a market type."""
    mapper = VisionPathMapper(base_cache_dir)
    date = pendulum.parse(date_str)

    # Create components and get paths
    components = mapper.create_components_from_params(
        symbol=symbol,
        interval=interval,
        date=date,
        market_type=market_type,
    )

    remote_url = mapper.get_remote_url(components)
    local_path = mapper.get_local_path(components)

    # Test bidirectional mapping
    mapped_local = mapper.map_remote_to_local(remote_url)
    mapped_remote = mapper.map_local_to_remote(local_path)

    return remote_url, str(local_path), mapped_remote, mapped_local


def main(
    base_cache_dir: str = typer.Option(
        "cache", "-c", "--cache-dir", help="Base cache directory"
    ),
    date_str: str = typer.Option(
        "2025-04-16", "-d", "--date", help="Date in YYYY-MM-DD format"
    ),
    interval: str = typer.Option("1m", "-i", "--interval", help="Time interval"),
):
    """Test the VisionPathMapper with different market types."""
    console.print(
        "[bold green]Testing VisionPathMapper for All Market Types[/bold green]"
    )
    console.print("")

    # Test Spot market
    remote_url, local_path, mapped_remote, mapped_local = test_market_mapping(
        market_type=MarketType.SPOT,
        symbol="BTCUSDT",
        interval=interval,
        date_str=date_str,
        base_cache_dir=base_cache_dir,
    )
    print_test_results(
        "Spot", "BTCUSDT", remote_url, local_path, mapped_remote, mapped_local
    )

    # Test UM Futures
    remote_url, local_path, mapped_remote, mapped_local = test_market_mapping(
        market_type=MarketType.FUTURES_USDT,
        symbol="BTCUSDT",
        interval=interval,
        date_str=date_str,
        base_cache_dir=base_cache_dir,
    )
    print_test_results(
        "USDT-margined Futures (UM)",
        "BTCUSDT",
        remote_url,
        local_path,
        mapped_remote,
        mapped_local,
    )

    # Test CM Futures
    remote_url, local_path, mapped_remote, mapped_local = test_market_mapping(
        market_type=MarketType.FUTURES_COIN,
        symbol="BTCUSD_PERP",
        interval=interval,
        date_str=date_str,
        base_cache_dir=base_cache_dir,
    )
    print_test_results(
        "Coin-margined Futures (CM)",
        "BTCUSD_PERP",
        remote_url,
        local_path,
        mapped_remote,
        mapped_local,
    )

    # Show filesystem examples
    console.print("[bold cyan]Path Conversion Example[/bold cyan]")
    handler = FSSpecVisionHandler(base_cache_dir)
    date = pendulum.parse(date_str)

    components = handler.path_mapper.create_components_from_params(
        symbol="BTCUSDT",
        interval=interval,
        date=date,
        market_type=MarketType.SPOT,
    )

    local_path = handler.get_local_path(components)
    remote_url = handler.get_remote_url(components)

    console.print(f"Local path: [green]{local_path}[/green]")
    console.print(f"Remote URL: [green]{remote_url}[/green]")

    fs_local, path_local = handler.get_fs_and_path(local_path)
    fs_remote, path_remote = handler.get_fs_and_path(remote_url)

    console.print(
        f"Local filesystem: [blue]{fs_local.__class__.__name__}[/blue], Path: [green]{path_local}[/green]"
    )
    console.print(
        f"Remote filesystem: [blue]{fs_remote.__class__.__name__}[/blue], Path: [green]{path_remote}[/green]"
    )


if __name__ == "__main__":
    typer.run(main)
