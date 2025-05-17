#!/usr/bin/env python3
# Example usage of ArrowCacheReader with proper market_constraints.py enums

from datetime import datetime

from rich import print

from utils.arrow_cache_reader import ArrowCacheReader
from utils.config import MAX_PREVIEW_ITEMS
from utils.market_constraints import ChartType, DataProvider, Interval, MarketType


def main():
    """
    Demonstrates how to use ArrowCacheReader with market_constraints enums
    """
    # Initialize the ArrowCacheReader
    reader = ArrowCacheReader()

    # Define parameters using proper enums
    provider = DataProvider.BINANCE
    chart_type = ChartType.KLINES
    market_type = MarketType.SPOT
    symbol = "BTCUSDT"
    interval = Interval.HOUR_1
    date = datetime(2025, 1, 1)  # Example date

    # Check if data is available for these parameters
    # Using get_file_path instead of is_available - if path exists, data is available
    file_path = reader.get_file_path(
        symbol=symbol,
        interval=interval,
        date=date,
        market_type=market_type,
    )
    is_available = file_path is not None

    print("[bold cyan]Data availability check:[/bold cyan]")
    print(f"- Provider: {provider.name}")
    print(f"- Chart Type: {chart_type.name}")
    print(f"- Market Type: {market_type.name}")
    print(f"- Symbol: {symbol}")
    print(f"- Interval: {interval.name}")
    print(f"- Date: {date.strftime('%Y-%m-%d')}")
    print(f"- Available: [{'green' if is_available else 'red'}]{is_available}[/]")

    if is_available:
        # Read the data if available
        # Using read_arrow_file instead of read
        df = reader.read_arrow_file(file_path)

        print("\n[bold cyan]Data sample:[/bold cyan]")
        print(df.head(5))

        # Get cache statistics
        stats = reader.get_cache_statistics()
        print("\n[bold cyan]Cache Statistics:[/bold cyan]")
        print(f"- Total entries: {stats['total_entries']}")
        print(f"- Total size: {stats['total_size_mb']:.2f} MB")
    else:
        print("\n[yellow]Data not available in cache.[/yellow]")

    # List all available data for a specific provider and market type
    available_dates = reader.list_available_dates(
        symbol=symbol,
        interval=interval,
        market_type=market_type,
    )

    print(f"\n[bold cyan]Available dates for {symbol} {interval.name}:[/bold cyan]")
    if available_dates:
        for date in available_dates[:MAX_PREVIEW_ITEMS]:  # Show first MAX_PREVIEW_ITEMS dates
            print(f"- {date}")
        if len(available_dates) > MAX_PREVIEW_ITEMS:
            print(f"... and {len(available_dates) - MAX_PREVIEW_ITEMS} more")
    else:
        print("[yellow]No dates available[/yellow]")


if __name__ == "__main__":
    main()
