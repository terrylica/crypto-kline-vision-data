#!/usr/bin/env python3
"""
Demo script showing how to use FSSpecVisionHandler for path mapping and cache operations.

This script demonstrates the use of FSSpecVisionHandler for consistent path mapping
between Binance Vision API remote URLs and local cache files.
"""

from rich.console import Console
from rich.table import Table
from rich import print
import typer
import pendulum
from pathlib import Path

from utils.market_constraints import MarketType
from core.sync.vision_path_mapper import FSSpecVisionHandler


console = Console()
app = typer.Typer(help="FSSpecVisionHandler Demo")


def path_demo(
    symbol: str,
    market_type: MarketType,
    interval: str,
    date: pendulum.DateTime,
    cache_dir: Path,
):
    """Demonstrate path mapping for different market types."""
    fs_handler = FSSpecVisionHandler(base_cache_dir=cache_dir)

    # Get paths for this configuration
    local_path = fs_handler.get_local_path_for_data(
        symbol=symbol,
        interval=interval,
        date=date,
        market_type=market_type,
    )

    remote_url = fs_handler.get_remote_url_for_data(
        symbol=symbol,
        interval=interval,
        date=date,
        market_type=market_type,
    )

    # Check if files exist
    local_exists = fs_handler.exists(local_path)

    # Create a table to display the results
    table = Table(title=f"{market_type.name} Market Path Demo")
    table.add_column("Item", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Symbol", symbol)
    table.add_row("Market Type", market_type.name)
    table.add_row("Interval", interval)
    table.add_row("Date", date.format("YYYY-MM-DD"))
    table.add_row("Local Path", str(local_path))
    table.add_row("Remote URL", remote_url)
    table.add_row("Local Exists", "✓" if local_exists else "✗")

    # Get filesystem info
    fs_local, path_local = fs_handler.get_fs_and_path(local_path)
    fs_remote, path_remote = fs_handler.get_fs_and_path(remote_url)

    table.add_row("Local FS", fs_local.__class__.__name__)
    table.add_row("Remote FS", fs_remote.__class__.__name__)

    console.print(table)
    console.print("")


@app.command()
def main(
    symbol: str = typer.Option("BTCUSDT", "--symbol", "-s", help="Trading symbol"),
    market_type: str = typer.Option(
        "spot", "--market-type", "-m", help="Market type (spot, um, cm)"
    ),
    interval: str = typer.Option("1m", "--interval", "-i", help="Time interval"),
    date_str: str = typer.Option(
        "2025-04-01", "--date", "-d", help="Date in YYYY-MM-DD format"
    ),
    cache_dir: str = typer.Option("cache", "--cache-dir", "-c", help="Cache directory"),
    all_markets: bool = typer.Option(
        False, "--all-markets", "-a", help="Show all market types"
    ),
):
    """Demonstrate FSSpecVisionHandler path mapping capabilities."""
    print("[bold green]FSSpecVisionHandler Path Mapping Demo[/bold green]")
    print("")

    # Parse date
    date = pendulum.parse(date_str)

    # Create cache directory
    cache_path = Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    if all_markets:
        # Show all market types
        path_demo(
            symbol="BTCUSDT",
            market_type=MarketType.SPOT,
            interval=interval,
            date=date,
            cache_dir=cache_path,
        )

        path_demo(
            symbol="BTCUSDT",
            market_type=MarketType.FUTURES_USDT,
            interval=interval,
            date=date,
            cache_dir=cache_path,
        )

        path_demo(
            symbol="BTCUSD_PERP",
            market_type=MarketType.FUTURES_COIN,
            interval=interval,
            date=date,
            cache_dir=cache_path,
        )
    else:
        # Parse market type
        market_enum = None
        if market_type.lower() == "spot":
            market_enum = MarketType.SPOT
        elif market_type.lower() in ("um", "futures_usdt"):
            market_enum = MarketType.FUTURES_USDT
        elif market_type.lower() in ("cm", "futures_coin"):
            market_enum = MarketType.FUTURES_COIN
        else:
            print(f"[bold red]Unknown market type: {market_type}[/bold red]")
            raise typer.Exit(1)

        # Adjust symbol for coin-margined futures if needed
        if market_enum == MarketType.FUTURES_COIN and not symbol.endswith("_PERP"):
            original_symbol = symbol
            symbol = f"{symbol}_PERP"
            print(
                f"[yellow]Adjusted symbol for CM futures: {original_symbol} -> {symbol}[/yellow]"
            )

        # Show selected market type
        path_demo(
            symbol=symbol,
            market_type=market_enum,
            interval=interval,
            date=date,
            cache_dir=cache_path,
        )


if __name__ == "__main__":
    app()
