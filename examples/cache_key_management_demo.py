#!/usr/bin/env python3

import typer
import pendulum
from pathlib import Path
import pandas as pd
from rich.console import Console
from rich.table import Table

from utils.market_constraints import Interval, MarketType
from utils.cache_validator import (
    CacheKeyManager,
    CachePathOptions,
    CacheValidator,
    CacheValidationError,
    VisionCacheManager,
)
from core.sync.cache_manager import UnifiedCacheManager

app = typer.Typer(help="Cache Key Management Demo Tool")
console = Console()


def get_market_type_str(market_type: MarketType) -> str:
    """Get string representation of market type for cache paths.

    Args:
        market_type: MarketType enum

    Returns:
        String representation of market type
    """
    if market_type == MarketType.SPOT:
        return "spot"
    elif market_type == MarketType.FUTURES_USDT:
        return "futures_usdt"
    elif market_type == MarketType.FUTURES_COIN:
        return "futures_coin"
    elif market_type == MarketType.FUTURES:
        return "futures_usdt"  # Default to UM for backward compatibility
    elif market_type == MarketType.OPTIONS:
        return "options"
    else:
        return "spot"  # Default fallback


def create_dummy_data(
    symbol: str, interval: str, date: pendulum.DateTime
) -> pd.DataFrame:
    """Create dummy OHLCV data for demonstration purposes"""
    data = []
    # Create 24 hours of data with the given interval
    interval_obj = next((i for i in Interval if i.value == interval), Interval.HOUR_1)
    seconds = interval_obj.to_seconds()

    # Start from midnight
    current_time = date.start_of("day")

    # Generate dummy data points based on interval
    while current_time < date.add(days=1).start_of("day"):
        data.append(
            {
                "open_time": current_time,
                "open": 100.0 + (current_time.timestamp() % 100),
                "high": 110.0 + (current_time.timestamp() % 100),
                "low": 90.0 + (current_time.timestamp() % 100),
                "close": 105.0 + (current_time.timestamp() % 100),
                "volume": 1000.0 + (current_time.timestamp() % 1000),
                "close_time": current_time.add(seconds=seconds - 1),
                "quote_asset_volume": 105000.0 + (current_time.timestamp() % 10000),
                "number_of_trades": 100 + (current_time.timestamp() % 100),
                "taker_buy_base_asset_volume": 500.0 + (current_time.timestamp() % 500),
                "taker_buy_quote_asset_volume": 52500.0
                + (current_time.timestamp() % 5000),
            }
        )
        current_time = current_time.add(seconds=seconds)

    return pd.DataFrame(data)


def generate_and_compare_cache_keys(
    symbol: str, interval: str, date: pendulum.DateTime
) -> None:
    """Generate and compare cache keys from different managers"""
    # Generate cache key using CacheKeyManager - convert pendulum DateTime to standard datetime
    dt = (
        date.naive()
    )  # Get a naive datetime object that's compatible with datetime.datetime
    ckm_key = CacheKeyManager.get_cache_key(symbol, interval, dt)

    # Generate cache key using UnifiedCacheManager
    ucm = UnifiedCacheManager(cache_dir=Path("./cache"))
    ucm_key = ucm.get_cache_key(
        symbol=symbol,
        interval=interval,
        date=dt,
        provider="BINANCE",
        chart_type="KLINES",
        market_type="spot",
    )

    # Display the keys in a table
    table = Table(title="Cache Key Comparison")
    table.add_column("Manager", style="cyan")
    table.add_column("Cache Key", style="green")
    table.add_column("Format", style="yellow")

    table.add_row("CacheKeyManager", ckm_key, "symbol_interval_YYYY-MM-DD")
    table.add_row(
        "UnifiedCacheManager",
        ucm_key,
        "PROVIDER_CHARTTYPE_markettype_SYMBOL_interval_YYYYMMDD",
    )

    console.print(table)


def generate_and_compare_cache_paths(
    symbol: str,
    interval: str,
    date: pendulum.DateTime,
    cache_root: Path,
    market_type: MarketType = MarketType.SPOT,
) -> None:
    """Generate and compare cache paths from different managers"""
    # Convert pendulum DateTime to standard datetime
    dt = date.naive()

    # Generate cache path using CacheKeyManager
    ckm_path = CacheKeyManager.get_cache_path(
        cache_dir=cache_root, symbol=symbol, interval=interval, date=dt
    )

    # Generate cache path with custom options for the specified market type
    market_type_str = "spot"
    if market_type == MarketType.FUTURES_USDT:
        market_type_str = "futures_usdt"
    elif market_type == MarketType.FUTURES_COIN:
        market_type_str = "futures_coin"

    options = CachePathOptions(
        exchange="BINANCE",
        market_type=market_type_str,
        data_nature="KLINES",
        packaging_frequency="daily",
    )

    ckm_custom_path = CacheKeyManager.get_cache_path(
        cache_dir=cache_root, symbol=symbol, interval=interval, date=dt, options=options
    )

    # Generate cache path using UnifiedCacheManager for the specified market type
    ucm = UnifiedCacheManager(cache_dir=cache_root)
    ucm_key = ucm.get_cache_key(
        symbol=symbol,
        interval=interval,
        date=dt,
        provider="BINANCE",
        chart_type="KLINES",
        market_type=market_type_str,
    )
    ucm_path = ucm._get_cache_path(ucm_key)

    # Always show futures path for comparison
    futures_market_type = "futures_usdt"
    if market_type == MarketType.FUTURES_USDT:
        # If we're already in UM, show CM for comparison
        futures_market_type = "futures_coin"

    ucm_alt_key = ucm.get_cache_key(
        symbol=symbol,
        interval=interval,
        date=dt,
        provider="BINANCE",
        chart_type="KLINES",
        market_type=futures_market_type,
    )
    ucm_alt_path = ucm._get_cache_path(ucm_alt_key)

    # Display the paths in a table
    table = Table(title="Cache Path Comparison")
    table.add_column("Manager", style="cyan")
    table.add_column("Market", style="magenta")
    table.add_column("Cache Path", style="green", no_wrap=False)

    table.add_row("CacheKeyManager", "Default", str(ckm_path))
    table.add_row(
        "CacheKeyManager", f"Custom ({market_type.name})", str(ckm_custom_path)
    )
    table.add_row("UnifiedCacheManager", market_type.name, str(ucm_path))
    table.add_row("UnifiedCacheManager", futures_market_type.upper(), str(ucm_alt_path))

    console.print(table)


def demonstrate_vision_cache_manager(
    symbol: str,
    interval: str,
    date: pendulum.DateTime,
    cache_root: Path,
    market_type: MarketType = MarketType.SPOT,
) -> None:
    """Demonstrate VisionCacheManager's save and load operations"""
    console.print(f"\n[bold blue]Demonstrating VisionCacheManager[/bold blue]")

    # Convert pendulum DateTime to standard datetime
    dt = date.naive()

    # Create an instance of VisionCacheManager
    vcm = VisionCacheManager(cache_dir=cache_root)

    # Create dummy data
    df = create_dummy_data(symbol, interval, date)
    console.print(
        f"Created dummy data with {len(df)} rows for {symbol} {interval} on {date.format('YYYY-MM-DD')}"
    )

    # Save data to cache
    success = vcm.save_to_cache(
        df=df, symbol=symbol, interval=interval, date=dt, market_type=market_type
    )

    if success:
        console.print(f"[green]Successfully saved data to cache[/green]")

        # Get the cache path for validation - using custom options to match the market type
        options = CachePathOptions(
            exchange="BINANCE",
            market_type=get_market_type_str(market_type),
            data_nature="KLINES",
            packaging_frequency="daily",
        )

        cache_path = CacheKeyManager.get_cache_path(
            cache_dir=cache_root,
            symbol=symbol,
            interval=interval,
            date=dt,
            options=options,
        )

        console.print(f"Cache file created at: {cache_path}")
        console.print(f"File exists: {cache_path.exists()}")

        if cache_path.exists():
            file_size = cache_path.stat().st_size
            console.print(f"File size: {file_size/1024:.2f} KB")

        # Load data from cache
        loaded_df = vcm.load_from_cache(
            symbol=symbol, interval=interval, date=dt, market_type=market_type
        )

        if loaded_df is not None and not loaded_df.empty:
            console.print(
                f"[green]Successfully loaded {len(loaded_df)} rows from cache[/green]"
            )

            # Verify the data is the same
            if len(loaded_df) == len(df):
                console.print(
                    "[green]Loaded data has the same number of rows as original data[/green]"
                )
            else:
                console.print(
                    "[red]WARNING: Loaded data has different number of rows![/red]"
                )

            # Display first few rows
            console.print("\nSample of loaded data:")
            console.print(loaded_df.head(3))
        else:
            console.print("[red]Failed to load data from cache[/red]")
    else:
        console.print("[red]Failed to save data to cache[/red]")


def demonstrate_cache_validation(
    symbol: str,
    interval: str,
    date: pendulum.DateTime,
    cache_root: Path,
    market_type: MarketType = MarketType.SPOT,
) -> None:
    """Demonstrate cache validation functionality"""
    console.print(f"\n[bold blue]Demonstrating Cache Validation[/bold blue]")

    # Convert pendulum DateTime to standard datetime
    dt = date.naive()

    # Create a cache validator
    validator = CacheValidator()

    # Get the cache path to validate - using custom options to match the market type
    options = CachePathOptions(
        exchange="BINANCE",
        market_type=get_market_type_str(market_type),
        data_nature="KLINES",
        packaging_frequency="daily",
    )

    cache_path = CacheKeyManager.get_cache_path(
        cache_dir=cache_root, symbol=symbol, interval=interval, date=dt, options=options
    )

    if not cache_path.exists():
        console.print(
            f"[yellow]Cache file doesn't exist at {cache_path}. Creating dummy data first...[/yellow]"
        )
        vcm = VisionCacheManager(cache_dir=cache_root)
        df = create_dummy_data(symbol, interval, date)
        vcm.save_to_cache(df, symbol, interval, dt, market_type)

    # Validate the cache file
    console.print(f"Validating cache file at {cache_path}")

    try:
        # Check integrity
        integrity_result = validator.validate_cache_integrity(cache_path)
        console.print(f"[green]Integrity validation passed: {integrity_result}[/green]")

        # Add metadata for demonstration
        metadata = {
            "checksum": "demo_checksum_123456789",
            "record_count": 24,
            "last_updated": pendulum.now().format("YYYY-MM-DD HH:mm:ss.SSS"),
            "market_type": market_type.name,
        }

        # Add the metadata to the file (normally this would be done during saving)
        validator.update_cache_metadata(cache_path, metadata)
        console.print(f"Added metadata to cache file")

        # Retrieve and display metadata
        retrieved_metadata = validator.get_cache_metadata(cache_path)
        if retrieved_metadata:
            console.print("Retrieved metadata:")
            for key, value in retrieved_metadata.items():
                console.print(f"  {key}: {value}")

    except CacheValidationError as e:
        console.print(f"[red]Validation failed: {e}[/red]")


@app.command()
def demo(
    symbol: str = typer.Option("BTCUSDT", "--symbol", "-s", help="Trading pair symbol"),
    interval: str = typer.Option("1h", "--interval", "-i", help="Time interval"),
    date_str: str = typer.Option(
        None, "--date", "-d", help="Date in YYYY-MM-DD format"
    ),
    cache_dir: str = typer.Option(
        "./cache", "--cache-dir", "-c", help="Cache directory path"
    ),
    market_type: str = typer.Option(
        "spot",
        "--market",
        "-m",
        help="Market type: spot, um (USDT-M futures), cm (Coin-M futures)",
    ),
) -> None:
    """Demonstrate Cache Key Management functionality"""
    # Parse the date or use today
    if date_str:
        date = pendulum.parse(date_str)
    else:
        date = pendulum.now().start_of("day")

    # Adjust symbol for CM market if needed
    original_symbol = symbol
    if market_type.lower() in ["cm", "futures_coin"]:
        if symbol == "BTCUSDT":
            symbol = "BTCUSD_PERP"
            console.print(
                f"[yellow]Adjusted symbol for Coin-Margined futures: {symbol}[/yellow]"
            )

    # Convert market type string to enum
    try:
        market_enum = MarketType.from_string(market_type)
    except ValueError:
        console.print(
            f"[red]Invalid market type: {market_type}. Using SPOT instead.[/red]"
        )
        market_enum = MarketType.SPOT

    cache_root = Path(cache_dir)

    console.print(f"[bold]Cache Key Management Demo[/bold]")
    console.print(f"Symbol: {symbol}")
    console.print(f"Market Type: {market_enum.name}")
    console.print(f"Interval: {interval}")
    console.print(f"Date: {date.format('YYYY-MM-DD')}")
    console.print(f"Cache Directory: {cache_root}\n")

    # Generate and compare cache keys
    generate_and_compare_cache_keys(symbol, interval, date)

    # Generate and compare cache paths
    generate_and_compare_cache_paths(symbol, interval, date, cache_root, market_enum)

    # Demonstrate VisionCacheManager
    demonstrate_vision_cache_manager(symbol, interval, date, cache_root, market_enum)

    # Demonstrate cache validation
    demonstrate_cache_validation(symbol, interval, date, cache_root, market_enum)


if __name__ == "__main__":
    app()
