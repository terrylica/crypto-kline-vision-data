#!/usr/bin/env python3
"""
Test script to demonstrate the integration between the Arrow Cache and market_constraints.py enums.

This script shows how to properly use the market_constraints.py enums with the Arrow Cache system,
ensuring data is stored in the correct locations and can be found by the ArrowCacheReader.
"""

from datetime import datetime
from pathlib import Path

from rich import print

from data_source_manager.utils.arrow_cache_reader import ArrowCacheReader
from data_source_manager.utils.market_constraints import ChartType, DataProvider, Interval, MarketType


def test_market_constraint_access():
    """
    Test different market types, data providers, and chart types to ensure
    they're properly handled by the ArrowCacheReader.
    """
    # Create a reader instance
    reader = ArrowCacheReader()

    # Define test parameters
    symbol = "BTCUSDT"
    date = datetime(2023, 4, 1)  # Historical date for reliable testing

    # Test various combinations of markets, providers, and chart types
    test_cases = [
        # Default case (most common)
        {
            "name": "Default SPOT market",
            "provider": DataProvider.BINANCE,
            "chart_type": ChartType.KLINES,
            "market_type": MarketType.SPOT,
            "interval": Interval.MINUTE_5,
        },
        # Futures USDT market
        {
            "name": "Futures USDT market",
            "provider": DataProvider.BINANCE,
            "chart_type": ChartType.KLINES,
            "market_type": MarketType.FUTURES_USDT,
            "interval": Interval.HOUR_1,
        },
        # Futures COIN market
        {
            "name": "Futures COIN market",
            "provider": DataProvider.BINANCE,
            "chart_type": ChartType.KLINES,
            "market_type": MarketType.FUTURES_COIN,
            "interval": Interval.HOUR_4,
        },
        # Funding rate chart type
        {
            "name": "Funding Rate data",
            "provider": DataProvider.BINANCE,
            "chart_type": ChartType.FUNDING_RATE,
            "market_type": MarketType.FUTURES_USDT,
            "interval": Interval.HOUR_8,
        },
    ]

    # Test each combination
    for case in test_cases:
        print(f"[bold cyan]Testing: {case['name']}[/bold cyan]")
        print(f"  Provider: {case['provider'].name}")
        print(f"  Chart Type: {case['chart_type'].name}")
        print(f"  Market Type: {case['market_type'].name}")
        print(f"  Interval: {case['interval'].name}")

        # Get the file path using ArrowCacheReader
        file_path = reader.get_file_path(
            symbol=symbol,
            interval=case["interval"],
            date=date,
            market_type=case["market_type"],
        )

        # Check if data exists
        print(f"  Path: {file_path}")
        print(f"  Exists: [{'green' if file_path and Path(file_path).exists() else 'red'}]")

        # Get the path components used in the database query
        _, path_pattern = reader._get_cache_path_components(symbol=symbol, interval=case["interval"], market_type=case["market_type"])
        print(f"  Path pattern for DB query: {path_pattern}")

        print()

    return True


def validate_market_constraints_compatibility():
    """
    Validate that the market_constraints enums are being used properly in the cache system.
    """
    print("\n[bold magenta]Validating market_constraints compatibility...[/bold magenta]")

    # Check if ChartType enum values are properly mapped to vision_api_path
    print("\n[bold cyan]ChartType vision_api_path mappings:[/bold cyan]")
    for chart_type in ChartType:
        print(f"  {chart_type.name}: {chart_type.vision_api_path}")

    # Check if MarketType enum values are properly mapped to vision_api_path
    print("\n[bold cyan]MarketType vision_api_path mappings:[/bold cyan]")
    for market_type in MarketType:
        print(f"  {market_type.name}: {market_type.vision_api_path}")

    # Check for consistency in is_futures property
    print("\n[bold cyan]MarketType is_futures check:[/bold cyan]")
    for market_type in MarketType:
        print(f"  {market_type.name}: is_futures = {market_type.is_futures}")

    # Check support matrix
    print("\n[bold cyan]Support matrix for chart types:[/bold cyan]")
    for chart_type in ChartType:
        print(f"  {chart_type.name}:")
        print(f"    - Supported markets: {[m.name for m in chart_type.supported_markets]}")
        print(f"    - Supported providers: {[p.name for p in chart_type.supported_providers]}")

    return True


if __name__ == "__main__":
    print("[bold green]===== Testing market_constraints integration with Arrow Cache =====[/bold green]")

    # Validate the market_constraints enums
    validate_market_constraints_compatibility()

    # Test accessing data using different market parameters
    test_market_constraint_access()

    print("[bold green]===== Tests completed =====[/bold green]")
