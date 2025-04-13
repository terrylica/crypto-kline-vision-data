#!/usr/bin/env python3
"""
Simple demonstration of synchronous data retrieval using DataSourceManager.

This script shows how to retrieve 1-minute candlestick data for Bitcoin
in SPOT, USDT-margined futures (UM), or COIN-margined futures (CM) markets.
It also demonstrates data source merging capabilities across multiple sources:
1. Cache (for data already stored in local Arrow files)
2. VISION API (for historical data older than 48 hours)
3. REST API (for recent data within the last 48 hours)
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
from pathlib import Path
import time
import os
import argparse
import logging

from utils.logger_setup import logger
from rich import print
from utils.market_constraints import MarketType, Interval, DataProvider, ChartType
from core.sync.data_source_manager import DataSourceManager, DataSource
from core.sync.cache_manager import UnifiedCacheManager
from utils.config import VISION_DATA_DELAY_HOURS

# We'll use this cache dir for all demos
CACHE_DIR = Path("./cache")


def get_data_sync(
    market_type: MarketType,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: Interval = Interval.MINUTE_1,
    provider: DataProvider = DataProvider.BINANCE,
    chart_type: ChartType = ChartType.KLINES,
    use_cache: bool = False,
    show_cache_path: bool = False,
    max_retries: int = 3,
    retry_delay: int = 1,
    enforce_source: DataSource = DataSource.AUTO,
):
    """
    Retrieve data synchronously using DataSourceManager.

    This function demonstrates the proper use of the synchronous DataSourceManager
    to retrieve market data with appropriate caching.

    Args:
        market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
        symbol: Symbol to retrieve data for (e.g., "BTCUSDT")
        start_time: Start time for data retrieval
        end_time: End time for data retrieval
        interval: Time interval between data points
        provider: Data provider (currently only BINANCE is supported)
        chart_type: Type of chart data to retrieve (KLINES, FUNDING_RATE)
        use_cache: Whether to use caching
        show_cache_path: Whether to show the cache path
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        enforce_source: Force specific data source (AUTO, REST, VISION)

    Returns:
        Pandas DataFrame containing the retrieved data
    """
    logger.info(
        f"Retrieving {interval.value} {chart_type.name} data for {symbol} in {market_type.name} market using {provider.name}"
    )
    logger.info(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")
    logger.info(f"Cache enabled: {use_cache}")
    if enforce_source != DataSource.AUTO:
        logger.info(f"Enforcing data source: {enforce_source.name}")

    # If cache path display is requested
    if show_cache_path and use_cache:
        cache_dir = Path("./cache")
        # Create an instance of UnifiedCacheManager to get the cache path
        cache_manager = UnifiedCacheManager(cache_dir=cache_dir)
        market_type_str = market_type.name.lower()
        if market_type == MarketType.FUTURES_USDT:
            market_type_str = "futures_usdt"
        elif market_type == MarketType.FUTURES_COIN:
            market_type_str = "futures_coin"

        cache_key = cache_manager.get_cache_key(
            symbol=symbol,
            interval=interval.value,
            date=start_time,
            provider=provider.name,
            chart_type=chart_type.name,
            market_type=market_type_str,
        )
        cache_path = cache_manager._get_cache_path(cache_key)

        print(f"[bold cyan]Cache path:[/bold cyan] {cache_path}")
        if os.path.exists(cache_path):
            print(
                f"[bold green]Cache file exists![/bold green] Size: {os.path.getsize(cache_path)/1024:.2f} KB"
            )
        else:
            print(
                f"[bold yellow]Cache file does not exist yet[/bold yellow] (will be created if cache enabled)"
            )

    start_time_retrieval = time.time()

    # Create a DataSourceManager instance with the specified parameters
    # Use context manager for proper resource management
    with DataSourceManager(
        market_type=market_type,
        provider=provider,
        chart_type=chart_type,
        use_cache=use_cache,
        retry_count=max_retries,
    ) as manager:
        # Retrieve data using the manager
        # The manager will handle the priority: cache → Vision API → REST API
        df = manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            chart_type=chart_type,
        )

    elapsed_time = time.time() - start_time_retrieval

    if df is None or df.empty:
        logger.warning(f"No data retrieved for {symbol}")
        return pd.DataFrame()

    logger.info(
        f"Retrieved {len(df)} records for {symbol} in {elapsed_time:.2f} seconds"
    )
    return df


def get_historical_data_test(
    market_type: MarketType,
    symbol: str = "BTCUSDT",
    interval: Interval = Interval.MINUTE_1,
    provider: DataProvider = DataProvider.BINANCE,
    chart_type: ChartType = ChartType.KLINES,
    use_cache: bool = True,
    show_cache_path: bool = False,
    max_retries: int = 3,
    debug: bool = False,
    enforce_source: DataSource = DataSource.AUTO,
):
    """
    Run a long-term historical data test for DataSourceManager orchestration.

    This test retrieves data from Dec 24, 2024 12:09:03 to Feb 25, 2025 23:56:56
    and demonstrates the Failover Composition Priority (FCP) strategy of the orchestrator.

    Args:
        market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
        symbol: Symbol to retrieve data for (defaults to "BTCUSDT")
        interval: Time interval between data points (defaults to 1 minute)
        provider: Data provider (currently only BINANCE is supported)
        chart_type: Type of chart data to retrieve (KLINES, FUNDING_RATE)
        use_cache: Whether to use caching
        show_cache_path: Whether to show the cache path
        max_retries: Maximum number of retry attempts
        debug: Whether to enable additional debug output
        enforce_source: Force specific data source (AUTO, REST, VISION)

    Returns:
        Pandas DataFrame containing the retrieved data
    """
    # Define the precise time range as specified in the request
    # Today is April 11, 2025, so these dates are in the past
    start_time = datetime(2024, 12, 24, 12, 15, 3, tzinfo=timezone.utc)
    end_time = datetime(2025, 2, 25, 23, 56, 56, tzinfo=timezone.utc)

    logger.info("Running long-term historical data test")
    logger.info(f"Today's date: April 11, 2025")
    logger.info(f"Using date range: {start_time.isoformat()} to {end_time.isoformat()}")
    logger.info(
        f"Market: {market_type.name} | Symbol: {symbol} | Chart Type: {chart_type.name} | Interval: {interval.value}"
    )
    logger.info(
        f"Total duration: {(end_time - start_time).days} days, {(end_time - start_time).seconds // 3600} hours"
    )
    if enforce_source != DataSource.AUTO:
        logger.info(f"Enforcing data source: {enforce_source.name}")

    # Adjust symbol for CM (Coin-Margined Futures) market if needed
    if market_type == MarketType.FUTURES_COIN and symbol == "BTCUSDT":
        symbol = "BTCUSD_PERP"
        print(f"[yellow]Adjusted symbol for CM market: {symbol}[/yellow]")

    # If cache path display is requested
    if show_cache_path and use_cache:
        cache_dir = Path("./cache")
        # Create an instance of UnifiedCacheManager to get the cache path
        cache_manager = UnifiedCacheManager(cache_dir=cache_dir)
        market_type_str = market_type.name.lower()
        if market_type == MarketType.FUTURES_USDT:
            market_type_str = "futures_usdt"
        elif market_type == MarketType.FUTURES_COIN:
            market_type_str = "futures_coin"

        # Display expected cache paths for a few sample dates throughout the range
        sample_dates = [
            start_time,
            datetime(2025, 1, 8, 0, 0, 0, tzinfo=timezone.utc),  # Early January
            datetime(2025, 1, 23, 0, 0, 0, tzinfo=timezone.utc),  # Late January
            datetime(2025, 2, 10, 0, 0, 0, tzinfo=timezone.utc),  # Early February
            end_time,
        ]

        print(f"[bold cyan]Sample cache paths for historical data:[/bold cyan]")
        for date in sample_dates:
            cache_key = cache_manager.get_cache_key(
                symbol=symbol,
                interval=interval.value,
                date=date,
                provider=provider.name,
                chart_type=chart_type.name,
                market_type=market_type_str,
            )
            cache_path = cache_manager._get_cache_path(cache_key)
            print(f"[cyan]Cache path for {date.date()}:[/cyan] {cache_path}")
            if os.path.exists(cache_path):
                print(
                    f"[bold green]Cache file exists![/bold green] Size: {os.path.getsize(cache_path)/1024:.2f} KB"
                )
            else:
                print(
                    f"[bold yellow]Cache file does not exist yet[/bold yellow] (will be created if cache enabled)"
                )

    start_time_retrieval = time.time()

    # Create a DataSourceManager instance with the specified parameters
    # Use context manager for proper resource management
    with DataSourceManager(
        market_type=market_type,
        provider=provider,
        chart_type=chart_type,
        use_cache=use_cache,
        retry_count=max_retries,
    ) as manager:
        # If debug mode is enabled, we'll fetch data in smaller chunks to better observe
        # the failover composition priority (FCP) strategy at work
        if debug:
            print(
                "[bold yellow]Debug mode enabled - fetching data in multiple chunks[/bold yellow]"
            )

            # Split the date range into multiple chunks (approximately weekly)
            date_ranges = []
            current_start = start_time

            while current_start < end_time:
                # Create a chunk of about 7 days or less for the last chunk
                current_end = min(current_start + timedelta(days=7), end_time)
                date_ranges.append((current_start, current_end))
                current_start = current_end

            all_dfs = []

            for i, (chunk_start, chunk_end) in enumerate(date_ranges):
                print(
                    f"[cyan]Fetching chunk {i+1}/{len(date_ranges)}: {chunk_start.date()} to {chunk_end.date()}[/cyan]"
                )

                chunk_start_time = time.time()
                chunk_df = manager.get_data(
                    symbol=symbol,
                    start_time=chunk_start,
                    end_time=chunk_end,
                    interval=interval,
                    chart_type=chart_type,
                    enforce_source=enforce_source,
                )
                chunk_time = time.time() - chunk_start_time

                if chunk_df is not None and not chunk_df.empty:
                    # Count data source breakdown
                    if "_data_source" in chunk_df.columns:
                        source_counts = chunk_df["_data_source"].value_counts()
                        print(f"Data source breakdown: {source_counts.to_dict()}")

                    print(
                        f"Retrieved {len(chunk_df)} records in {chunk_time:.2f} seconds"
                    )
                    all_dfs.append(chunk_df)
                else:
                    print(f"[bold red]No data retrieved for chunk {i+1}[/bold red]")

            # Concatenate all DataFrames and sort by open_time
            if all_dfs:
                df = pd.concat(all_dfs)
                df = df.sort_values("open_time").reset_index(drop=True)
            else:
                df = pd.DataFrame()
        else:
            # Retrieve data in a single call for the entire range
            df = manager.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                chart_type=chart_type,
                enforce_source=enforce_source,
            )

    elapsed_time = time.time() - start_time_retrieval

    if df is None or df.empty:
        logger.warning(f"No data retrieved for {symbol}")
        print("[bold red]No data retrieved for the specified time range[/bold red]")
        return pd.DataFrame()

    logger.info(
        f"Retrieved {len(df)} records for {symbol} in {elapsed_time:.2f} seconds"
    )

    # Print source breakdown
    if "_data_source" in df.columns:
        source_counts = df["_data_source"].value_counts()
        print(f"\n[bold cyan]Data Source Breakdown:[/bold cyan]")
        for source, count in source_counts.items():
            print(f"{source}: {count} records ({count/len(df)*100:.1f}%)")

    # Print summary statistics
    print(f"\n[bold green]Historical Data Test Results:[/bold green]")
    print(f"Retrieved {len(df)} records for {symbol} in {elapsed_time:.2f} seconds")
    print(f"Data spans from {df['open_time'].min()} to {df['open_time'].max()}")
    print(f"Average retrieval rate: {len(df) / elapsed_time:.2f} records/second")

    # Count potential gaps in the data
    expected_intervals = {
        Interval.MINUTE_1: 60,
        Interval.MINUTE_3: 180,
        Interval.MINUTE_5: 300,
        Interval.MINUTE_15: 900,
        Interval.MINUTE_30: 1800,
        Interval.HOUR_1: 3600,
        Interval.HOUR_2: 7200,
        Interval.HOUR_4: 14400,
        Interval.HOUR_6: 21600,
        Interval.HOUR_8: 28800,
        Interval.HOUR_12: 43200,
        Interval.DAY_1: 86400,
        Interval.DAY_3: 259200,
        Interval.WEEK_1: 604800,
        Interval.MONTH_1: 2592000,
    }.get(
        interval, 60
    )  # Default to 60 seconds for unknown intervals

    # Check for gaps
    # Ensure we reset the index if it's open_time to avoid ambiguity
    if df.index.name == "open_time":
        df = df.reset_index()

    df_sorted = df.sort_values("open_time")
    time_diffs = df_sorted["open_time"].diff().dropna().dt.total_seconds()
    gaps = time_diffs[time_diffs > expected_intervals]

    if len(gaps) > 0:
        print(f"[yellow]Detected {len(gaps)} potential gaps in the data[/yellow]")
        if debug and len(gaps) < 10:
            # Show details for up to 10 gaps
            print("[yellow]Gap details (in seconds):[/yellow]")
            for i, gap in enumerate(gaps):
                print(f"  Gap {i+1}: {gap} seconds")
    else:
        print("[green]No gaps detected in the data[/green]")

    return df


def demonstrate_data_source_merging(
    market_type: MarketType,
    symbol: str = "BTCUSDT",
    interval: Interval = Interval.MINUTE_1,
    provider: DataProvider = DataProvider.BINANCE,
    chart_type: ChartType = ChartType.KLINES,
    max_retries: int = 3,
    enforce_source: DataSource = DataSource.AUTO,
):
    """
    Demonstrate data source merging with DataSourceManager.

    This function shows how DataSourceManager retrieves and merges data from multiple sources:
    1. Cache (for data already stored in local Arrow files)
    2. VISION API (for historical data older than 48 hours)
    3. REST API (for recent data within the last 48 hours)

    It creates a scenario that spans multiple data sources and shows how they are
    seamlessly merged together based on the open_time index.

    Args:
        market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
        symbol: Symbol to retrieve data for (defaults to "BTCUSDT")
        interval: Time interval between data points (defaults to 1 minute)
        provider: Data provider (currently only BINANCE is supported)
        chart_type: Type of chart data to retrieve (KLINES, FUNDING_RATE)
        max_retries: Maximum number of retry attempts
        enforce_source: Force specific data source (AUTO, REST, VISION)

    Returns:
        Pandas DataFrame containing the merged data
    """
    print(f"[bold green]Data Source Merging Demonstration[/bold green]")
    print(f"Market: {market_type.name}")
    print(f"Symbol: {symbol}")
    print(f"Interval: {interval.value}")
    print(f"Chart Type: {chart_type.name}")
    print(f"Data Provider: {provider.name}")
    print(f"Vision API delay hours: {VISION_DATA_DELAY_HOURS}")

    if enforce_source != DataSource.AUTO:
        print(
            f"[bold yellow]Enforcing data source: {enforce_source.name}[/bold yellow]"
        )

    # Set up test scenario by clearing and pre-caching data
    ranges = setup_test_scenario(symbol, market_type, interval, chart_type)
    cache_range = ranges[0:2]
    vision_range = ranges[2:4]
    rest_range = ranges[4:6]

    # Fetch and merge data
    df = fetch_and_merge_data(
        symbol,
        market_type,
        interval,
        chart_type,
        provider,
        cache_range,
        vision_range,
        rest_range,
        max_retries,
        enforce_source,
    )

    # Analyze merged data
    analyze_merged_data(df, cache_range, vision_range, rest_range)

    return df


def clear_cache(
    symbol: str, market_type: MarketType, interval: Interval, chart_type: ChartType
):
    """Clear cache for a specific symbol and market type.

    Args:
        symbol: Symbol to clear cache for
        market_type: Market type
        interval: Interval to clear cache for
        chart_type: Type of chart data to clear from cache
    """
    # Create cache manager
    cache_manager = UnifiedCacheManager(cache_dir=CACHE_DIR, create_dirs=True)

    # Build market type string
    market_type_str = market_type.name.lower()
    if market_type == MarketType.FUTURES_USDT:
        market_type_str = "futures_usdt"
    elif market_type == MarketType.FUTURES_COIN:
        market_type_str = "futures_coin"

    # Clear cache for the specific dates we're testing
    now = datetime.now(timezone.utc)

    # We'll prepare 3 different time ranges
    cache_date = now - timedelta(days=10)  # Old data for cache
    vision_date = now - timedelta(days=4)  # Medium-old data for Vision API
    rest_date = now - timedelta(hours=12)  # Recent data for REST API

    dates = [cache_date, vision_date, rest_date]

    # Remove existing cache entries for these dates if they exist
    for date in dates:
        cache_key = cache_manager.get_cache_key(
            symbol=symbol,
            interval=interval.value,
            date=date,
            provider=DataProvider.BINANCE.name,
            chart_type=chart_type.name,
            market_type=market_type_str,
        )
        cache_path = cache_manager._get_cache_path(cache_key)

        if os.path.exists(cache_path):
            print(f"[yellow]Removing existing cache file:[/yellow] {cache_path}")
            os.remove(cache_path)


def setup_test_scenario(
    symbol: str, market_type: MarketType, interval: Interval, chart_type: ChartType
):
    """Set up test scenario for demonstrating data source merging.

    Args:
        symbol: Symbol to set up test for
        market_type: Market type
        interval: Interval to set up test for
        chart_type: Type of chart data to retrieve (KLINES, FUNDING_RATE)

    Returns:
        Tuple of (cache_start, cache_end, vision_start, vision_end, rest_start, rest_end)
    """
    now = datetime.now(timezone.utc)

    # Create time ranges that will force different data sources to be used

    # Range 1: Old data that will be pre-cached (at least 7 days old for reliable caching)
    cache_end = now - timedelta(days=7)  # 7 days ago
    cache_start = cache_end - timedelta(hours=4)  # 4 hour window

    # Range 2: Medium-old data that should come from Vision API
    # This is older than VISION_DATA_DELAY_HOURS but newer than our cache
    vision_start = cache_end + timedelta(minutes=10)  # Gap after cache data
    vision_end = vision_start + timedelta(hours=6)  # 6 hour window

    # Range 3: Recent data that should come from REST API
    # This is newer than VISION_DATA_DELAY_HOURS
    rest_start = now - timedelta(hours=24)  # Last 24 hours
    rest_end = now - timedelta(hours=1)  # Up to 1 hour ago

    print(f"[bold cyan]Setting up test scenario with 3 data sources[/bold cyan]")
    print(f"Cache data: {cache_start.isoformat()} to {cache_end.isoformat()}")
    print(f"Vision data: {vision_start.isoformat()} to {vision_end.isoformat()}")
    print(f"REST data: {rest_start.isoformat()} to {rest_end.isoformat()}")

    # First clear any existing cache data for these ranges
    clear_cache(symbol, market_type, interval, chart_type)

    # Now fetch and cache the first range
    print(f"\n[bold yellow]Pre-caching data for Range 1...[/bold yellow]")
    with DataSourceManager(
        market_type=market_type,
        provider=DataProvider.BINANCE,
        chart_type=chart_type,
        use_cache=True,  # Enable caching
        retry_count=3,
        cache_dir=CACHE_DIR,  # Explicitly set cache directory
    ) as manager:
        # Fetch and cache the first range
        df_cache = manager.get_data(
            symbol=symbol,
            start_time=cache_start,
            end_time=cache_end,
            interval=interval,
            chart_type=chart_type,
        )

        if df_cache is not None and len(df_cache) > 0:
            print(
                f"Pre-cached {len(df_cache)} records from {cache_start} to {cache_end}"
            )

            # Verify data is in the cache by checking for cache file
            cache_manager = UnifiedCacheManager(cache_dir=CACHE_DIR)
            market_type_str = market_type.name.lower()
            if market_type == MarketType.FUTURES_USDT:
                market_type_str = "futures_usdt"
            elif market_type == MarketType.FUTURES_COIN:
                market_type_str = "futures_coin"

            # Check if cache was created
            cache_key = cache_manager.get_cache_key(
                symbol=symbol,
                interval=interval.value,
                date=cache_start.date(),
                provider=DataProvider.BINANCE.name,
                chart_type=chart_type.name,
                market_type=market_type_str,
            )
            cache_path = cache_manager._get_cache_path(cache_key)

            if os.path.exists(cache_path):
                print(f"[bold green]Cache file created: {cache_path}[/bold green]")
            else:
                print(
                    f"[bold red]WARNING: Cache file was not created at {cache_path}[/bold red]"
                )
        else:
            print(f"[bold red]WARNING: Failed to pre-cache data for range 1[/bold red]")

    return (cache_start, cache_end, vision_start, vision_end, rest_start, rest_end)


def fetch_and_merge_data(
    symbol: str,
    market_type: MarketType,
    interval: Interval,
    chart_type: ChartType,
    provider: DataProvider,
    cache_range: tuple,
    vision_range: tuple,
    rest_range: tuple,
    max_retries: int,
    enforce_source: DataSource = DataSource.AUTO,
):
    """Fetch data from multiple ranges and merge them.

    Args:
        symbol: Symbol to fetch data for
        market_type: Market type
        interval: Time interval
        chart_type: Type of chart data to retrieve (KLINES, FUNDING_RATE)
        provider: Data provider
        cache_range: Tuple of (start_time, end_time) for cache data
        vision_range: Tuple of (start_time, end_time) for Vision API data
        rest_range: Tuple of (start_time, end_time) for REST API data
        max_retries: Maximum number of retry attempts
        enforce_source: Force specific data source (AUTO, REST, VISION)

    Returns:
        DataFrame with merged data
    """
    # Create a single time range that spans all three ranges
    overall_start = min(cache_range[0], vision_range[0], rest_range[0])
    overall_end = max(cache_range[1], vision_range[1], rest_range[1])

    print(f"\n[bold green]Fetching data across all ranges...[/bold green]")
    print(
        f"Overall time range: {overall_start.isoformat()} to {overall_end.isoformat()}"
    )

    if enforce_source != DataSource.AUTO:
        print(
            f"[bold yellow]Enforcing data source: {enforce_source.name}[/bold yellow]"
        )

    # Create DataSourceManager with cache enabled
    start_time = time.time()
    with DataSourceManager(
        market_type=market_type,
        provider=provider,
        chart_type=chart_type,
        use_cache=True,  # Enable caching
        retry_count=max_retries,
    ) as manager:
        # Fetch data from the entire range
        df = manager.get_data(
            symbol=symbol,
            start_time=overall_start,
            end_time=overall_end,
            interval=interval,
            chart_type=chart_type,
            enforce_source=enforce_source,
            include_source_info=True,  # Explicitly set to include source information
        )
    elapsed_time = time.time() - start_time

    print(f"Retrieved and merged {len(df)} records in {elapsed_time:.2f} seconds")

    # Column standardization is now handled by DataSourceManager internally

    return df


def analyze_merged_data(
    df: pd.DataFrame,
    cache_range: tuple,
    vision_range: tuple,
    rest_range: tuple,
):
    """Analyze the merged data to show which parts came from which source.

    Args:
        df: DataFrame with merged data
        cache_range: Tuple of (start_time, end_time) for cache data
        vision_range: Tuple of (start_time, end_time) for Vision API data
        rest_range: Tuple of (start_time, end_time) for REST API data
    """
    cache_start, cache_end = cache_range
    vision_start, vision_end = vision_range
    rest_start, rest_end = rest_range

    # Ensure the index is properly set
    if df.index.name != "open_time":
        # If index is not properly set, try to use the open_time column
        if "open_time" in df.columns:
            df = df.set_index("open_time")
        else:
            print(
                "[bold red]ERROR: DataFrame doesn't have an open_time index or column![/bold red]"
            )
            return

    # First, show the true data source breakdown based on the _data_source column
    if "_data_source" in df.columns:
        source_counts = df["_data_source"].value_counts()
        print(
            f"\n[bold cyan]Actual Data Source Breakdown (from _data_source column):[/bold cyan]"
        )
        for source, count in source_counts.items():
            print(f"{source}: {count} records ({count/len(df)*100:.1f}%)")

        # Get sample data from each actual source
        print(f"\n[bold yellow]Sample Data By Actual Source:[/bold yellow]")
        for source in source_counts.index:
            print(f"\n[bold green]Records from {source} source:[/bold green]")
            source_data = df[df["_data_source"] == source].head(3)
            print(source_data)
    else:
        print(
            f"\n[bold red]WARNING: DataFrame doesn't have a _data_source column![/bold red]"
        )
        print(
            f"Cannot determine actual data sources - falling back to time-based analysis only."
        )

    # For comparison, also show time-based range analysis
    print(
        f"\n[bold cyan]Time Range-Based Analysis (may not match actual sources):[/bold cyan]"
    )
    print(f"Note: This is based on time ranges, not actual data sources")

    # Create masks for each range
    cache_mask = (df.index >= cache_start) & (df.index <= cache_end)
    vision_mask = (df.index >= vision_start) & (df.index <= vision_end)
    rest_mask = (df.index >= rest_start) & (df.index <= rest_end)

    # Count records in each range
    cache_count = cache_mask.sum()
    vision_count = vision_mask.sum()
    rest_count = rest_mask.sum()
    other_count = len(df) - cache_count - vision_count - rest_count

    print(f"Cache time range records: {cache_count}")
    print(f"Vision API time range records: {vision_count}")
    print(f"REST API time range records: {rest_count}")
    print(f"Other time range records: {other_count}")
    print(f"Total records: {len(df)}")


def main():
    """Run the demo."""
    # Set info logging level for less verbose logs
    logger.setLevel(logging.INFO)

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Demonstrate DataSourceManager API for Binance data retrieval"
    )

    # Market type selection (explicitly list all options from MarketType enum)
    parser.add_argument(
        "--market",
        type=str,
        default="spot",
        choices=[
            "spot",
            "um",
            "futures_usdt",
            "cm",
            "futures_coin",
            "futures",
            "options",
        ],
        help="Market type: spot (SPOT), um/futures_usdt (USDT-margined futures), "
        + "cm/futures_coin (Coin-margined futures), futures (legacy), options (Options market)",
    )

    # Data provider selection (explicitly list all options from DataProvider enum)
    parser.add_argument(
        "--provider",
        type=str,
        default="binance",
        choices=["binance", "tradestation"],
        help="Data provider (currently only binance is fully implemented)",
    )

    # Chart type selection (explicitly list all options from ChartType enum)
    parser.add_argument(
        "--chart-type",
        type=str,
        default="klines",
        choices=["klines", "fundingRate"],
        help="Chart data type: klines (candlestick data), fundingRate (funding rate data for futures)",
    )

    # Interval selection (explicitly list all options from Interval enum)
    parser.add_argument(
        "--interval",
        type=str,
        default="1m",
        choices=[
            "1s",
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
            "1M",
        ],
        help="Time interval between data points",
    )

    # Symbol selection
    parser.add_argument(
        "--symbol",
        type=str,
        default="BTCUSDT",
        help="Trading symbol (e.g., BTCUSDT for spot/UM, BTCUSD_PERP for CM)",
    )

    # Time range options
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days to fetch (used for regular mode)",
    )

    # Cache options
    parser.add_argument(
        "--use-cache",
        action="store_true",
        help="Enable caching of retrieved data to local Arrow files",
    )

    parser.add_argument(
        "--demo-cache",
        action="store_true",
        help="Demonstrate caching by running the same query twice to show performance difference",
    )

    parser.add_argument(
        "--show-cache", action="store_true", help="Show cache file paths and status"
    )

    # Retry options
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retry attempts for API requests",
    )

    # Demo options
    parser.add_argument(
        "--historical-test",
        action="store_true",
        help="Run long-term historical data test with specific dates (Dec 2024-Feb 2025)",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode with additional output and chunked data retrieval",
    )

    parser.add_argument(
        "--demo-merge",
        action="store_true",
        help="Demonstrate data source merging from cache, VISION API, and REST API",
    )

    # Add enforce_source parameter to the argument parser
    parser.add_argument(
        "--enforce-source",
        type=str,
        choices=["AUTO", "REST", "VISION"],
        default="AUTO",
        help="Force specific data source (default: AUTO - uses the optimal source)",
    )

    args = parser.parse_args()

    # Convert string arguments to enums
    market_type = MarketType.from_string(args.market)
    provider = DataProvider.from_string(args.provider)
    chart_type = ChartType.from_string(args.chart_type)

    # Convert interval string to enum
    try:
        interval_enum = Interval(args.interval)
    except ValueError:
        print(f"[bold red]Invalid interval: {args.interval}[/bold red]")
        print(f"Valid intervals: {', '.join([i.value for i in Interval])}")
        return

    # Convert enforce_source string to enum
    if args.enforce_source == "AUTO":
        enforce_source = DataSource.AUTO
    elif args.enforce_source == "REST":
        enforce_source = DataSource.REST
    elif args.enforce_source == "VISION":
        enforce_source = DataSource.VISION
    else:
        enforce_source = DataSource.AUTO

    # Run data source merging demo if requested
    if args.demo_merge:
        print(f"\n[bold green]Running Data Source Merging Demo[/bold green]")
        df = demonstrate_data_source_merging(
            market_type=market_type,
            symbol=args.symbol,
            interval=interval_enum,
            provider=provider,
            chart_type=chart_type,
            max_retries=args.retries,
            enforce_source=enforce_source,
        )

        # Save data to CSV if retrieved successfully
        if df is not None and not df.empty:
            output_dir = Path("./logs/merge_tests")
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create a filename with relevant info
            filename = f"{market_type.name.lower()}_{args.symbol}_{args.interval}_{chart_type.name.lower()}_merge_test.csv"
            output_path = output_dir / filename

            df.to_csv(output_path, index=False)
            print(f"[bold green]Saved {len(df)} records to {output_path}[/bold green]")

        return

    # Run historical data test if requested
    if args.historical_test:
        df = get_historical_data_test(
            market_type=market_type,
            symbol=args.symbol,
            interval=interval_enum,
            provider=provider,
            chart_type=chart_type,
            use_cache=args.use_cache,
            show_cache_path=args.show_cache,
            max_retries=args.retries,
            debug=args.debug,
            enforce_source=enforce_source,
        )

        # Save data to CSV if retrieved successfully
        if df is not None and not df.empty:
            output_dir = Path("./logs/historical_tests")
            output_dir.mkdir(parents=True, exist_ok=True)

            # Create a filename with relevant info
            filename = f"{market_type.name.lower()}_{args.symbol}_{args.interval}_{chart_type.name.lower()}_historical_test.csv"
            output_path = output_dir / filename

            df.to_csv(output_path, index=False)
            print(f"[bold green]Saved {len(df)} records to {output_path}[/bold green]")

        return

    # Define time range for regular mode
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=args.days)

    # Adjust symbol for CM (Coin-Margined Futures) market
    symbol = args.symbol
    if market_type == MarketType.FUTURES_COIN and symbol == "BTCUSDT":
        symbol = "BTCUSD_PERP"
        print(f"[yellow]Adjusted symbol for CM market: {symbol}[/yellow]")

    # Print configuration
    print(f"[bold cyan]Fetching {args.days} days of data[/bold cyan]")
    print(f"Market Type: {market_type.name}")
    print(f"Symbol: {symbol}")
    print(f"Chart Type: {chart_type.name}")
    print(f"Data Provider: {provider.name}")
    print(f"Interval: {interval_enum.value}")
    print(f"Time Range: {start_time.isoformat()} to {end_time.isoformat()}")
    print(f"Caching: {'Enabled' if args.use_cache else 'Disabled'}")
    print(f"Retries: {args.retries}")

    if args.enforce_source != "AUTO":
        print(
            f"[bold yellow]Enforcing data source: {args.enforce_source}[/bold yellow]"
        )

    # Demonstrate cache effect if requested
    if args.demo_cache:
        # First run - should fetch from the source
        print("\n[bold yellow]First run - fetching from source...[/bold yellow]")
        first_start = time.time()
        df_first = get_data_sync(
            market_type=market_type,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval_enum,
            provider=provider,
            chart_type=chart_type,
            use_cache=True,
            show_cache_path=args.show_cache,
            max_retries=args.retries,
            enforce_source=enforce_source,
        )
        first_elapsed = time.time() - first_start

        # Second run - should use cache
        print("\n[bold yellow]Second run - should use cache...[/bold yellow]")
        second_start = time.time()
        df_second = get_data_sync(
            market_type=market_type,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval_enum,
            provider=provider,
            chart_type=chart_type,
            use_cache=True,
            show_cache_path=args.show_cache,
            max_retries=args.retries,
            enforce_source=enforce_source,
        )
        second_elapsed = time.time() - second_start

        # Compare results
        print("\n[bold green]Results comparison:[/bold green]")
        print(
            f"First run (from source): {len(df_first)} records in {first_elapsed:.2f}s"
        )
        print(
            f"Second run (from cache): {len(df_second)} records in {second_elapsed:.2f}s"
        )
        print(
            f"Speed improvement: {(first_elapsed/second_elapsed):.1f}x faster with cache"
        )

        if not df_first.equals(df_second):
            print("[bold red]Warning: The two dataframes are not identical![/bold red]")
        else:
            print(
                "[bold green]Both dataframes are identical - cache is working perfectly![/bold green]"
            )

    else:
        # Regular single run
        df = get_data_sync(
            market_type=market_type,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval_enum,
            provider=provider,
            chart_type=chart_type,
            use_cache=args.use_cache,
            show_cache_path=args.show_cache,
            max_retries=args.retries,
            enforce_source=enforce_source,
        )

        # Display results
        if df is not None and not df.empty:
            print("\n[bold green]Data Retrieved Successfully![/bold green]")

            # Print source breakdown if available
            if "_data_source" in df.columns:
                source_counts = df["_data_source"].value_counts()
                print(f"\n[bold cyan]Data Source Breakdown:[/bold cyan]")
                for source, count in source_counts.items():
                    print(f"{source}: {count} records ({count/len(df)*100:.1f}%)")

            print(f"\nData sample ({min(5, len(df))} records of {len(df)} total):")
            print(df.head())
        else:
            print("\n[bold red]No data retrieved![/bold red]")


if __name__ == "__main__":
    main()
