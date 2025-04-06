#!/usr/bin/env python
"""Example of the recommended approach for data retrieval using DataSourceManager.

This script demonstrates best practices for retrieving market data using the DataSourceManager
with different market types, intervals, and time ranges. Each example function showcases specific
use cases and recommended approaches for different scenarios.

Note about cache statistics tracking:
- Single DataSourceManager instance examples use direct get_cache_stats() calls
- Multi-instance examples (like example_different_market_types) use aggregated statistics
  tracking to ensure accurate reporting across independent manager lifecycles
- See docs/cache_diagnostics/multi_manager_stats_integrity.md for detailed analysis on
  why this approach is necessary for tracking statistics across multiple instances

Examples included:

1. `example_fetch_recent_data()`:
   - Retrieves recent 1-second BTCUSDT data from SPOT market
   - Demonstrates basic DataSourceManager initialization with explicit market type
   - Shows how to use both automatic source selection and forced REST API source

2. `example_fetch_historical_data()`:
   - Retrieves historical Bitcoin data from 90 days ago
   - Shows how to force the Vision API data source for historical data
   - Demonstrates fallback to 1-minute data when 1-second historical data is unavailable

3. `example_fetch_same_day_minute_data()`:
   - Retrieves intraday 1-minute BTCUSDT data
   - Shows the recommended approach for handling same-day data

4. `example_fetch_unavailable_data()`:
   - Demonstrates robust error handling for unavailable data cases
   - Shows proper exception handling for future dates (catching ValueError exceptions)
   - Creates properly structured empty DataFrames to maintain consistent return interfaces
   - Tests error handling with non-existent symbols

5. `create_dsm_example()`:
   - Utility function with comprehensive error handling
   - Validates if requested intervals are supported by specific market types
   - Converts string intervals to Interval enum values

6. `example_different_market_types()`:
   - Demonstrates data retrieval across different market types (SPOT, FUTURES_USDT, FUTURES_COIN)
   - Shows handling of market-specific symbols and intervals
   - Illustrates proper cache statistics aggregation across multiple DataSourceManager instances
   - Tests combination of different intervals with appropriate market types:
     - 1-second BTCUSDT data from SPOT market
     - 15-minute ETHUSDT data from SPOT market
     - 1-minute BTCUSDT data from FUTURES_USDT market
     - 3-minute BTCUSD data from FUTURES_COIN market (with automatic _PERP suffix handling)

Chart Types:
The system supports various chart data types defined in the `ChartType` enum:
- `KLINES`: Standard candlestick data (default, supported by all markets)
- `FUNDING_RATE`: Funding rate data (futures markets only)

Each chart type is mapped to the corresponding API endpoint, and compatibility with
different market types is handled automatically. The DataSourceManager and underlying
clients use this to construct the proper API URLs for data retrieval.

Best Practices Demonstrated:
- Always specifying market_type explicitly when creating DataSourceManager
- Proper error handling and validation
- Efficient use of caching
- Support for different time intervals across market types
- Handling of market-specific symbol formats
- Graceful degradation and fallback strategies
"""

# IMPORTANT NOTE:
# This example script includes intentional tests of the system's ability to properly
# handle and validate date ranges, including requests for data from future dates.
# In particular, the example_fetch_unavailable_data() function deliberately attempts
# to fetch data for dates in the future to demonstrate that the system correctly
# raises ValueError exceptions for such requests, which are then properly caught and
# handled with appropriate error messages. These are not bugs or mistakes, but
# intentional demonstrations of proper error handling practices in client applications.

import asyncio
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import argparse

from utils.logger_setup import logger

# Set to INFO instead of CRITICAL to see the colorized output
logger.setLevel("INFO")
# Enable rich logging for enhanced visual output
logger.use_rich(True)
# Enable filename display to see which file generates log messages
logger.show_filename(True)
from utils.market_constraints import Interval, MarketType, is_interval_supported
from core.data_source_manager import DataSourceManager, DataSource
from utils.config import create_empty_dataframe


def determine_data_source(manager, prev_stats, current_stats, enforce_source=None):
    """Helper function to determine data source by analyzing cache statistics.

    Args:
        manager: DataSourceManager instance
        prev_stats: Previous cache statistics before the operation
        current_stats: Current cache statistics after the operation
        enforce_source: If specified, the source that was enforced (DataSource.REST or DataSource.VISION)

    Returns:
        String indicating the data source (CACHE, VISION API, REST API)
    """
    # If a source was explicitly enforced, that's our answer
    if enforce_source:
        if enforce_source == DataSource.REST:
            return "REST API (enforced)"
        elif enforce_source == DataSource.VISION:
            return "VISION API (enforced)"

    # If cache hits increased, data came from cache
    if current_stats.get("hits", 0) > prev_stats.get("hits", 0):
        return "CACHE"

    # If we got this far with misses increased, data came from an API
    if current_stats.get("misses", 0) > prev_stats.get("misses", 0):
        # We can't be 100% sure which API without additional tracking,
        # but for most examples we can make an educated guess based on time range
        now = datetime.now(timezone.utc)
        threshold = now - timedelta(hours=DataSourceManager.VISION_DATA_DELAY_HOURS)

        # This is a heuristic - not guaranteed to be accurate in all cases
        # For production, you would want to add direct tracking in DataSourceManager
        return "REST or VISION API (based on cache miss)"

    # Default fallback
    return "UNKNOWN SOURCE"


def format_data_source(data_source):
    """Format the data source string with distinctive visual styling using rich markup."""
    source_styling = {
        "CACHE": "[bold bright_green on black]CACHE[/bold bright_green on black]",
        "REST API": "[bold bright_blue on black]REST API[/bold bright_blue on black]",
        "VISION API": "[bold bright_magenta on black]VISION API[/bold bright_magenta on black]",
        "REST API (enforced)": "[bold bright_blue on black]REST API[/bold bright_blue on black] [bright_yellow](enforced)[/bright_yellow]",
        "VISION API (enforced)": "[bold bright_magenta on black]VISION API[/bold bright_magenta on black] [bright_yellow](enforced)[/bright_yellow]",
        "REST or VISION API": "[bold bright_cyan on black]REST or VISION API[/bold bright_cyan on black]",
        "UNKNOWN SOURCE": "[bold bright_red on black]UNKNOWN SOURCE[/bold bright_red on black]",
    }

    # Find the appropriate styling for this data source
    for key, style in source_styling.items():
        if key in data_source:
            return style

    # Default styling if no match
    return f"[bold bright_cyan on black]{data_source}[/bold bright_cyan on black]"


def format_header(title, time_range=None):
    """Creates a formatted header with rich markup using distinctive colors and styling."""
    # Create a colorful decorator line with alternating colors
    decorator = (
        "[bright_magenta]★[/bright_magenta][bright_cyan]★[/bright_cyan][bright_yellow]★[/bright_yellow]"
        * 5
    )

    # Format the title with gradient-like effect
    formatted_title = f"[bold bright_blue on black]{title}[/bold bright_blue on black]"

    # Assemble the header with decorative elements
    header = f"\n{decorator}\n[bold bright_green]┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓[/bold bright_green]\n"
    header += f"[bold bright_green]┃[/bold bright_green] {formatted_title} [bold bright_green]┃[/bold bright_green]\n"
    header += f"[bold bright_green]┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛[/bold bright_green]\n"
    header += f"{decorator}"

    # Add time range if provided
    if time_range:
        start, end = time_range
        header += f"\n[bright_yellow]Time range: {start} to {end}[/bright_yellow]"

    return header


def format_dataframe_section(df, title, data_source, time_range=None):
    """Creates a formatted section for dataframe display with distinctive rich markup."""
    if df.empty:
        return f"[bold bright_red]⚠️ {title}: No data available ⚠️[/bold bright_red]"

    start_time, end_time = time_range if time_range else (None, None)

    # Create multicolor separator
    separator = "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]" * 20

    # Build output with decorative elements
    output = [
        f"\n{separator}",
        f"[bold bright_green on black]◉ {title} ◉[/bold bright_green on black]",
        f"[bright_blue]⟫ Data source: {data_source} ⟪[/bright_blue]",
    ]

    if start_time and end_time:
        output.append(
            f"[bright_yellow]⏱ Time range: {start_time} to {end_time}[/bright_yellow]"
        )

    output.append(f"[bold bright_cyan]📊 Retrieved {len(df)} rows[/bold bright_cyan]")
    output.append(f"{separator}")

    # Convert DataFrame to string but don't add markup inside it
    # as it would interfere with the tabular structure
    return "\n".join(output)


async def example_fetch_recent_data():
    """Example function to fetch recent data using DataSourceManager."""
    # Log current time for reference
    now = datetime.now(timezone.utc)
    logger.debug("Starting example_fetch_recent_data function")
    logger.info(f"Current time: {now.isoformat()}")
    logger.info(format_header("Fetching Recent Bitcoin Data (Recommended Approach)"))

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Define time range (recent data that should be available, not in the future)
    # Use data from 48 hours ago to ensure data availability
    end_time = now - timedelta(hours=48)
    start_time = end_time - timedelta(hours=1)

    logger.info(
        f"Time range: [yellow]{start_time}[/yellow] to [yellow]{end_time}[/yellow]"
    )

    # Track cache statistics (single manager instance)
    cache_stats = {"hits": 0, "misses": 0, "errors": 0}

    # Using DataSourceManager (recommended approach with async context manager)
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,  # Enable caching through the unified cache manager
        use_httpx=True,  # Use httpx instead of curl_cffi to avoid hanging issues
    ) as manager:
        # Store initial stats
        prev_stats = manager.get_cache_stats().copy()

        # The manager will automatically:
        # 1. Choose the appropriate data source (REST or Vision API)
        # 2. Handle caching through UnifiedCacheManager
        # 3. Validate and format the data consistently
        df = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
        )

        # Update stats after first operation
        current_stats = manager.get_cache_stats()
        for key in current_stats:
            cache_stats[key] += current_stats[key]

        # Determine data source
        data_source = determine_data_source(manager, prev_stats, current_stats)

        # Display results with rich formatting
        logger.info(
            format_dataframe_section(
                df,
                "BTCUSDT 1-second data",
                format_data_source(data_source),
                (start_time, end_time),
            )
        )

        if not df.empty:
            # Rich logging will format the DataFrame output nicely
            logger.info(df.head().to_string())
            # Use multicolor separator after dataframe display
            logger.info(
                "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]" * 20
            )

        # Store stats before second operation
        prev_stats = manager.get_cache_stats().copy()

        # Example of forcing a specific data source
        # You can force REST API for very recent data or testing
        logger.info("\n[bold]Fetching with forced REST API source:[/bold]")
        df_rest = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            enforce_source=DataSource.REST,  # Force REST API
        )

        # Update stats after second operation
        current_stats = manager.get_cache_stats()
        for key in current_stats:
            cache_stats[key] = current_stats[
                key
            ]  # Reset to latest since these are cumulative

        # Determine data source with enforced source information
        data_source = determine_data_source(
            manager, prev_stats, current_stats, DataSource.REST
        )

        # Display REST API results with rich formatting
        logger.info(
            format_dataframe_section(
                df_rest,
                "BTCUSDT 1-second data (REST API)",
                format_data_source(data_source),
                (start_time, end_time),
            )
        )

        if not df_rest.empty:
            logger.info(df_rest.head().to_string())
            # Use multicolor separator after dataframe display
            logger.info(
                "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]" * 20
            )

        # Display final cache statistics with enhanced formatting
        logger.info(
            f"\n[bold bright_green]🔍 Final Cache Statistics:[/bold bright_green] [bright_cyan]{cache_stats}[/bright_cyan]"
        )


async def example_fetch_historical_data():
    """Example function to fetch historical data using DataSourceManager."""
    # Log current time for reference
    now = datetime.now(timezone.utc)
    logger.info(f"Current time: {now.isoformat()}")
    logger.info(
        format_header("Fetching Historical Bitcoin Data (Recommended Approach)")
    )

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Define historical time range relative to current time
    # Use a date 3 months in the past to ensure Vision API has the data
    end_time = now - timedelta(days=90)
    start_time = end_time - timedelta(
        hours=8
    )  # Reduce to 8 hours instead of full day to speed up testing

    logger.info(
        f"Historical time range: [yellow]{start_time}[/yellow] to [yellow]{end_time}[/yellow]"
    )

    # Create a progress indicator
    logger.info(
        "[bold]Starting historical data download - this may take a minute...[/bold]"
    )

    # Track cache statistics (single manager instance)
    cache_stats = {"hits": 0, "misses": 0, "errors": 0}

    # Using DataSourceManager
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,
        use_httpx=True,  # Use httpx instead of curl_cffi to avoid hanging issues
    ) as manager:
        try:
            # Store initial stats
            prev_stats = manager.get_cache_stats().copy()

            # For historical data, Vision API will automatically be selected
            # but we enforce it here to demonstrate the capability
            df = await manager.get_data(
                symbol="BTCUSDT",
                start_time=start_time,
                end_time=end_time,
                interval=Interval.SECOND_1,
                enforce_source=DataSource.VISION,  # Enforce Vision API
            )

            # Update stats after first operation
            current_stats = manager.get_cache_stats()
            for key in current_stats:
                cache_stats[key] += current_stats[key]

            # Determine data source with enforced source information
            data_source = determine_data_source(
                manager, prev_stats, current_stats, DataSource.VISION
            )

            # Display results with rich formatting
            logger.info(
                format_dataframe_section(
                    df,
                    "BTCUSDT 1-second historical data",
                    format_data_source(data_source),
                    (start_time, end_time),
                )
            )

            # Display a sample of the data
            if not df.empty:
                logger.info(df.head().to_string())
                # Use multicolor separator after dataframe display
                logger.info(
                    "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]"
                    * 20
                )
                logger.info(
                    f"\n[bold bright_green]🔍 Cache Statistics:[/bold bright_green] [bright_cyan]{cache_stats}[/bright_cyan]"
                )

                # Explicitly mark completion to show progress
                logger.info(
                    "[bold green]✓ Historical data download complete[/bold green]"
                )
            else:
                logger.info(
                    "[yellow]No data retrieved. Attempting with 1-minute data instead.[/yellow]"
                )

                # Store stats before second operation
                prev_stats = manager.get_cache_stats().copy()

                # Try with 1-minute data which might be more available
                df_minute = await manager.get_data(
                    symbol="BTCUSDT",
                    start_time=start_time,
                    end_time=end_time,
                    interval=Interval.MINUTE_1,
                )

                # Update stats after second operation
                current_stats = manager.get_cache_stats()
                for key in current_stats:
                    cache_stats[key] = current_stats[
                        key
                    ]  # Reset to latest since these are cumulative

                # Determine data source for 1-minute data
                data_source = determine_data_source(manager, prev_stats, current_stats)

                # Display 1-minute results with rich formatting
                logger.info(
                    format_dataframe_section(
                        df_minute,
                        "BTCUSDT 1-minute historical data (fallback)",
                        format_data_source(data_source),
                        (start_time, end_time),
                    )
                )

                if not df_minute.empty:
                    logger.info(df_minute.head().to_string())
                    # Use multicolor separator after dataframe display
                    logger.info(
                        "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]"
                        * 20
                    )
                    logger.info(
                        f"\n[bold bright_green]🔍 Final Cache Statistics:[/bold bright_green] [bright_cyan]{cache_stats}[/bright_cyan]"
                    )

                    # Explicitly mark completion to show progress
                    logger.info(
                        "[bold green]✓ Historical data download complete[/bold green]"
                    )

        except Exception as e:
            logger.error(f"[bold red]Error fetching historical data: {e}[/bold red]")
            return  # Exit the function to prevent further processing


async def example_fetch_same_day_minute_data():
    """Example function to fetch 1-minute data for the current day using DataSourceManager."""
    # Log current time for reference
    now = datetime.now(timezone.utc)
    logger.info(f"Current time: {now.isoformat()}")
    logger.info(
        format_header("Fetching 1-minute Data for Current Day (Recommended Approach)")
    )

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Define time range for today
    # Set end time a few hours in the past to ensure data availability
    end_time = now - timedelta(hours=2)
    # Set start time to the beginning of the same day
    start_time = datetime(
        end_time.year, end_time.month, end_time.day, 0, 0, 0, tzinfo=timezone.utc
    )

    logger.info(
        f"Same-day time range: [yellow]{start_time}[/yellow] to [yellow]{end_time}[/yellow]"
    )

    # Using DataSourceManager with async context manager
    # Note: This example uses a single manager instance, so cache statistics
    # are reported directly after operations complete.
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,  # Enable caching
        use_httpx=True,  # Use httpx instead of curl_cffi to avoid hanging issues
    ) as manager:
        # Store initial stats
        prev_stats = manager.get_cache_stats().copy()

        # Fetch 1-minute data
        df = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.MINUTE_1,  # Using 1-minute interval
        )

        # Get current stats and determine source
        current_stats = manager.get_cache_stats()
        data_source = determine_data_source(manager, prev_stats, current_stats)

        # Display results with rich formatting
        logger.info(
            format_dataframe_section(
                df,
                "BTCUSDT 1-minute same-day data",
                format_data_source(data_source),
                (start_time, end_time),
            )
        )

        if not df.empty:
            logger.info(df.head().to_string())
            # Use multicolor separator after dataframe display
            logger.info(
                "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]" * 20
            )

        # Get cache statistics - single instance so direct reporting is appropriate
        cache_stats = manager.get_cache_stats()
        logger.info(
            f"\n[bold bright_green]🔍 Cache Statistics:[/bold bright_green] [bright_cyan]{cache_stats}[/bright_cyan]"
        )


async def example_fetch_unavailable_data():
    """Example function demonstrating robust handling of unavailable data.

    This example INTENTIONALLY requests data for future dates to demonstrate the
    robust error handling of the DataSourceManager. The example shows that the system
    correctly rejects future date requests by raising a ValueError exception, which
    is then properly caught and handled. This demonstrates proper error handling practices
    for client applications that might accidentally request future dates.

    It also demonstrates handling of non-existent symbols, returning empty DataFrames
    with the correct structure in both error cases to maintain consistent interfaces.
    """
    # Log current time for reference
    now = datetime.now(timezone.utc)
    logger.info(f"Current time: {now.isoformat()}")
    logger.info(format_header("Demonstrating Robust Handling of Unavailable Data"))

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Track cache statistics (single manager instance with multiple operations)
    cache_stats = {"hits": 0, "misses": 0, "errors": 0}

    # INTENTIONALLY create future dates (one day ahead and two days ahead) to test error handling
    # These dates will be properly rejected by DataSourceManager with appropriate error messages
    future_start_time = now + timedelta(days=1)
    future_end_time = now + timedelta(days=2)

    logger.info(
        f"Attempting to fetch future data: [yellow]{future_start_time}[/yellow] to [yellow]{future_end_time}[/yellow]"
    )
    logger.info(
        "[bold red](This request is EXPECTED to fail with an appropriate error message)[/bold red]"
    )

    # Using DataSourceManager
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,
        use_httpx=True,  # Use httpx instead of curl_cffi to avoid hanging issues
    ) as manager:
        try:
            # Try to fetch future data (this should fail with a ValueError)
            future_df = await manager.get_data(
                symbol="BTCUSDT",
                start_time=future_start_time,
                end_time=future_end_time,
                interval=Interval.MINUTE_1,
            )

            # If we get here, the request didn't fail as expected
            logger.warning(
                "[bold yellow]Future data request did not fail as expected![/bold yellow]"
            )

        except ValueError as e:
            # This is the expected path - validation should raise ValueError for future dates
            logger.info(
                f"[bold green]✓ Correctly rejected future dates:[/bold green] {e}"
            )
            # Create empty DataFrame with proper structure for demonstrating the result
            future_df = create_empty_dataframe()
            # Update stats to record the error
            cache_stats["errors"] += 1

        # Display results - should be an empty DataFrame with the correct structure
        logger.info(f"[bold]Future data request result:[/bold] {len(future_df)} rows")
        logger.info(
            f"[bold]Is empty DataFrame properly structured:[/bold] [{'green' if future_df.empty and not future_df.columns.empty else 'red'}]{future_df.empty and not future_df.columns.empty}[/{'green' if future_df.empty and not future_df.columns.empty else 'red'}]"
        )

        # Try with a non-existent symbol
        logger.info(
            "\n[bold yellow]Attempting to fetch data for a non-existent symbol:[/bold yellow]"
        )
        try:
            # Ensure manager is valid before proceeding
            if manager is None:
                logger.error(
                    "[bold red]DataSourceManager is not initialized[/bold red]"
                )
                # Use the imported function directly
                invalid_df = create_empty_dataframe()
            else:
                invalid_df = await manager.get_data(
                    symbol="INVALIDCOIN",
                    start_time=now - timedelta(days=1),
                    end_time=now - timedelta(hours=1),
                    interval=Interval.MINUTE_1,
                )

                # Update stats after second operation
                current_stats = manager.get_cache_stats()
                for key in current_stats:
                    cache_stats[key] = current_stats[
                        key
                    ]  # Reset to latest since these are cumulative

            logger.info(
                f"[bold]Invalid symbol result:[/bold] [red]{len(invalid_df)} rows[/red]"
            )
        except Exception as e:
            logger.info(
                f"[bold]Invalid symbol resulted in error:[/bold] [red]{e}[/red]"
            )
            # Use the imported create_empty_dataframe function directly for safety
            invalid_df = create_empty_dataframe()
            cache_stats["errors"] += 1

        logger.info(
            f"\n[bold bright_green]🔍 Final Cache Statistics:[/bold bright_green] [bright_cyan]{cache_stats}[/bright_cyan]"
        )


async def create_dsm_example(
    market_type: MarketType,
    symbol: str,
    interval: str,  # Use string interval
    start_time: datetime,
    end_time: datetime,
    cache_dir: Path,
    description: str,
):
    """Utility function to create a DSM example with error handling."""
    logger.info(f"\n[bold blue]{description}[/bold blue]")
    try:
        # Convert string interval to Interval enum if needed
        interval_enum = None
        if isinstance(interval, str):
            for i in Interval:
                if i.value == interval:
                    interval_enum = i
                    break

        if interval_enum is None:
            logger.error(f"[bold red]Invalid interval string: {interval}[/bold red]")
            return pd.DataFrame()

        # Check if the interval is supported by this market type
        if not is_interval_supported(market_type, interval_enum):
            logger.error(
                f"[bold red]Interval {interval} is not supported for market type {market_type.name}[/bold red]"
            )
            return pd.DataFrame()

        async with DataSourceManager(
            market_type=market_type,
            cache_dir=cache_dir,
            use_cache=True,
            use_httpx=True,  # Use httpx instead of curl_cffi to avoid hanging issues
            max_concurrent=50,
            retry_count=3,
            max_concurrent_downloads=10,
        ) as manager:
            # Store initial stats
            prev_stats = manager.get_cache_stats().copy()

            df = await manager.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval_enum,
            )

            # Get current stats and determine source
            current_stats = manager.get_cache_stats()
            data_source = determine_data_source(manager, prev_stats, current_stats)

            # Display results with rich formatting
            logger.info(
                format_dataframe_section(
                    df,
                    f"{market_type.name} {symbol} {interval} data",
                    format_data_source(data_source),
                    (start_time, end_time),
                )
            )

            if not df.empty:
                logger.info(df.head(3).to_string())
                # Use multicolor separator after dataframe display
                logger.info(
                    "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]"
                    * 20
                )

            return df
    except Exception as e:
        logger.error(
            f"[bold red]Error retrieving {market_type.name} {symbol} {interval} data: {e}[/bold red]"
        )
        return pd.DataFrame()


async def example_different_market_types():
    """Example function demonstrating data retrieval across different market types and intervals.

    This example implements proper cache statistics aggregation across multiple DataSourceManager
    instances as described in docs/cache_diagnostics/multi_manager_stats_integrity.md.
    """
    # Log current time for reference
    now = datetime.now(timezone.utc)
    logger.info(f"Current time: {now.isoformat()}")
    logger.info(
        format_header("Data Retrieval Across Different Market Types and Intervals")
    )

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Define time range (recent data that should be available)
    # Use data from 5 days ago to ensure data availability in all markets
    end_time = now - timedelta(days=5)
    start_time = end_time - timedelta(hours=4)  # 4-hour window

    logger.info(
        f"Time range: [yellow]{start_time}[/yellow] to [yellow]{end_time}[/yellow]"
    )

    # Log which intervals are supported by each market type
    logger.info("\n[bold]Supported intervals by market type:[/bold]")
    for market_type in [
        MarketType.SPOT,
        MarketType.FUTURES_USDT,
        MarketType.FUTURES_COIN,
    ]:
        supported = [
            interval.value
            for interval in Interval
            if is_interval_supported(market_type, interval)
        ]
        logger.info(
            f"  [cyan]{market_type.name}:[/cyan] [green]{', '.join(supported)}[/green]"
        )

    # Create 1s window for high-frequency data (shorter time range)
    short_end_time = start_time + timedelta(minutes=10)

    # Track cache statistics for each market type
    market_stats = {
        MarketType.SPOT: {"hits": 0, "misses": 0, "errors": 0},
        MarketType.FUTURES_USDT: {"hits": 0, "misses": 0, "errors": 0},
        MarketType.FUTURES_COIN: {"hits": 0, "misses": 0, "errors": 0},
    }

    # Example 1: SPOT market with 1-second BTCUSDT data
    # SPOT market supports 1-second data
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,
        use_httpx=True,  # Use httpx instead of curl_cffi to avoid hanging issues
        max_concurrent=50,
        retry_count=3,
        max_concurrent_downloads=10,
    ) as manager:
        # Store initial stats
        prev_stats = manager.get_cache_stats().copy()

        logger.info(
            "\n[bold blue]1. SPOT market with 1-second BTCUSDT data[/bold blue]"
        )
        df = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=short_end_time,
            interval=Interval.SECOND_1,
        )

        # Get current stats and determine source
        current_stats = manager.get_cache_stats()
        data_source = determine_data_source(manager, prev_stats, current_stats)

        # Display results with rich formatting
        logger.info(
            format_dataframe_section(
                df,
                f"SPOT BTCUSDT {Interval.SECOND_1.value} data",
                format_data_source(data_source),
                (start_time, short_end_time),
            )
        )

        if not df.empty:
            logger.info(df.head(3).to_string())
            # Use multicolor separator after dataframe display
            logger.info(
                "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]" * 20
            )

        # Update market stats
        stats = manager.get_cache_stats()
        for key in stats:
            market_stats[MarketType.SPOT][key] += stats[key]

    # Example 2: SPOT market with 15-minute ETHUSDT data
    # All markets support 15-minute data
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,
        use_httpx=True,  # Use httpx instead of curl_cffi to avoid hanging issues
        max_concurrent=50,
        retry_count=3,
        max_concurrent_downloads=10,
    ) as manager:
        # Store initial stats
        prev_stats = manager.get_cache_stats().copy()

        logger.info(
            "\n[bold blue]2. SPOT market with 15-minute ETHUSDT data[/bold blue]"
        )
        df = await manager.get_data(
            symbol="ETHUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.MINUTE_15,
        )

        # Get current stats and determine source
        current_stats = manager.get_cache_stats()
        data_source = determine_data_source(manager, prev_stats, current_stats)

        # Display results with rich formatting
        logger.info(
            format_dataframe_section(
                df,
                f"SPOT ETHUSDT {Interval.MINUTE_15.value} data",
                format_data_source(data_source),
                (start_time, end_time),
            )
        )

        if not df.empty:
            logger.info(df.head(3).to_string())
            # Use multicolor separator after dataframe display
            logger.info(
                "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]" * 20
            )

        # Update market stats
        stats = manager.get_cache_stats()
        for key in stats:
            market_stats[MarketType.SPOT][key] += stats[key]

    # Example 3: USDT-margined futures (UM) with 1-minute BTCUSDT data
    # Futures markets support 1-minute data
    async with DataSourceManager(
        market_type=MarketType.FUTURES_USDT,
        cache_dir=cache_dir,
        use_cache=True,
        use_httpx=True,  # Use httpx instead of curl_cffi to avoid hanging issues
        max_concurrent=50,
        retry_count=3,
        max_concurrent_downloads=10,
    ) as manager:
        # Store initial stats
        prev_stats = manager.get_cache_stats().copy()

        logger.info(
            "\n[bold blue]3. USDT-margined futures (UM) with 1-minute BTCUSDT data[/bold blue]"
        )
        df = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.MINUTE_1,
        )

        # Get current stats and determine source
        current_stats = manager.get_cache_stats()
        data_source = determine_data_source(manager, prev_stats, current_stats)

        # Display results with rich formatting
        logger.info(
            format_dataframe_section(
                df,
                f"FUTURES_USDT BTCUSDT {Interval.MINUTE_1.value} data",
                format_data_source(data_source),
                (start_time, end_time),
            )
        )

        if not df.empty:
            logger.info(df.head(3).to_string())
            # Use multicolor separator after dataframe display
            logger.info(
                "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]" * 20
            )

        # Update market stats
        stats = manager.get_cache_stats()
        for key in stats:
            market_stats[MarketType.FUTURES_USDT][key] += stats[key]

    # Example 4: Coin-margined futures (CM) with 3-minute BTCUSD data
    # Futures markets support 3-minute data
    async with DataSourceManager(
        market_type=MarketType.FUTURES_COIN,
        cache_dir=cache_dir,
        use_cache=True,
        use_httpx=True,  # Use httpx instead of curl_cffi to avoid hanging issues
        max_concurrent=50,
        retry_count=3,
        max_concurrent_downloads=10,
    ) as manager:
        # Store initial stats
        prev_stats = manager.get_cache_stats().copy()

        logger.info(
            "\n[bold blue]4. Coin-margined futures (CM) with 3-minute BTCUSD data[/bold blue]"
        )
        df = await manager.get_data(
            symbol="BTCUSD",  # _PERP suffix should be added automatically
            start_time=start_time,
            end_time=end_time,
            interval=Interval.MINUTE_3,
        )

        # Get current stats and determine source
        current_stats = manager.get_cache_stats()
        data_source = determine_data_source(manager, prev_stats, current_stats)

        # Display results with rich formatting
        logger.info(
            format_dataframe_section(
                df,
                f"FUTURES_COIN BTCUSD {Interval.MINUTE_3.value} data",
                format_data_source(data_source),
                (start_time, end_time),
            )
        )

        if not df.empty:
            logger.info(df.head(3).to_string())
            # Use multicolor separator after dataframe display
            logger.info(
                "[bright_magenta]━━[/bright_magenta][bright_cyan]━━[/bright_cyan]" * 20
            )

        # Update market stats
        stats = manager.get_cache_stats()
        for key in stats:
            market_stats[MarketType.FUTURES_COIN][key] += stats[key]

    # Display cache statistics for all markets with enhanced formatting
    logger.info(
        "\n[bold bright_green]📊 Cache Statistics by Market Type:[/bold bright_green]"
    )
    for market_type, market_name in [
        (MarketType.SPOT, "SPOT"),
        (MarketType.FUTURES_USDT, "FUTURES_USDT (UM)"),
        (MarketType.FUTURES_COIN, "FUTURES_COIN (CM)"),
    ]:
        logger.info(
            f"  [bold bright_cyan]{market_name}:[/bold bright_cyan] [bright_yellow]{market_stats[market_type]}[/bright_yellow]"
        )


def handle_signals():
    """Set up signal handlers for graceful shutdown."""
    loop = asyncio.get_event_loop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(sig, loop))
        )


async def shutdown(sig, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info(f"[yellow]Received exit signal {sig.name}...[/yellow]")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]

    logger.info(f"[yellow]Cancelling {len(tasks)} outstanding tasks[/yellow]")
    await asyncio.gather(*tasks, return_exceptions=True)

    loop.stop()


if __name__ == "__main__":
    import argparse

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Run data retrieval examples.")
    parser.add_argument(
        "--example",
        choices=[
            "recent",
            "historical",
            "same_day",
            "unavailable",
            "different_market_types",
            "all",
        ],
        default="all",
        help="Specify which example to run (default: all)",
    )

    args = parser.parse_args()

    logger.warning(
        f"[bold yellow]Current UTC date time precision up to milliseconds:[/bold yellow] [cyan]{datetime.now(timezone.utc).isoformat(timespec='milliseconds')}[/cyan]"
    )
    # Set up signal handlers
    handle_signals()

    async def run_examples():
        """Run the selected examples."""
        # Log the current time at the start of execution with enhanced formatting
        now = datetime.now(timezone.utc)
        # Create a colorful script title banner
        title_banner = "\n".join(
            [
                "[bright_magenta]━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[/bright_magenta]",
                "[bold bright_cyan on black]                 BINANCE DATA RETRIEVAL EXAMPLES                [/bold bright_cyan on black]",
                "[bright_magenta]━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[/bright_magenta]",
            ]
        )
        logger.info(title_banner)
        logger.info(
            f"[bold bright_green]🚀 Script starting at:[/bold bright_green] [bright_yellow]{now.isoformat()}[/bright_yellow]"
        )

        example_map = {
            "recent": example_fetch_recent_data,
            "historical": example_fetch_historical_data,
            "same_day": example_fetch_same_day_minute_data,
            "unavailable": example_fetch_unavailable_data,
            "different_market_types": example_different_market_types,
        }

        if args.example == "all":
            for name, func in example_map.items():
                try:
                    logger.info(f"[bold]Running example:[/bold] [cyan]{name}[/cyan]")
                    await func()
                except Exception as e:
                    logger.error(f"[bold red]Error in example {name}: {e}[/bold red]")
        else:
            try:
                func = example_map[args.example]
                logger.info(
                    f"[bold]Running example:[/bold] [cyan]{args.example}[/cyan]"
                )
                await func()
            except Exception as e:
                logger.error(
                    f"[bold red]Error in example {args.example}: {e}[/bold red]"
                )

    # Run the examples
    asyncio.run(run_examples())
