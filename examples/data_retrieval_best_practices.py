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
   - Shows proper handling of future dates (which should be rejected)
   - Tests the behavior with non-existent symbols

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
- `UI_KLINES`: Optimized klines for UI applications (spot market only)
- `MARK_PRICE_KLINES`: Mark price klines (futures markets only)
- `PREMIUM_INDEX_KLINES`: Premium index klines (futures markets only)
- `CONTINUOUS_KLINES`: Continuous contract klines (futures markets only)

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

import asyncio
import signal
import sys, os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import traceback
import pandas as pd

from utils.logger_setup import logger

logger.setLevel("ERROR")
from utils.market_constraints import Interval, MarketType, is_interval_supported
from core.data_source_manager import DataSourceManager, DataSource


async def example_fetch_recent_data():
    """Example function to fetch recent data using DataSourceManager."""
    # Log current time for reference
    now = datetime.now(timezone.utc)
    logger.debug("Starting example_fetch_recent_data function")
    logger.info(f"Current time: {now.isoformat()}")
    logger.info("Fetching recent Bitcoin data using the recommended approach")

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Define time range (recent data that should be available, not in the future)
    # Use data from 48 hours ago to ensure data availability
    end_time = now - timedelta(hours=48)
    start_time = end_time - timedelta(hours=1)

    logger.info(f"Time range: {start_time} to {end_time}")

    # Track cache statistics (single manager instance)
    cache_stats = {"hits": 0, "misses": 0, "errors": 0}

    # Using DataSourceManager (recommended approach with async context manager)
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,  # Enable caching through the unified cache manager
    ) as manager:
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

        # Display results
        logger.info(f"Data retrieved: {len(df)} rows")
        logger.info(f"Data shape: {df.shape}")
        logger.info(f"Data columns: {df.columns.tolist()}")

        # Display a sample of the data
        if not df.empty:
            logger.info("\nSample data:")
            print(df.head().to_string())

        # Example of forcing a specific data source
        # You can force REST API for very recent data or testing
        logger.info("\nFetching with forced REST API source:")
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

        logger.info(f"REST API data retrieved: {len(df_rest)} rows")
        logger.info(f"\nFinal cache statistics: {cache_stats}")


async def example_fetch_historical_data():
    """Example function to fetch historical data using DataSourceManager."""
    # Log current time for reference
    now = datetime.now(timezone.utc)
    logger.info(f"Current time: {now.isoformat()}")
    logger.info("\nFetching historical Bitcoin data (recommended approach)")

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Define historical time range relative to current time
    # Use a date 3 months in the past to ensure Vision API has the data
    end_time = now - timedelta(days=90)
    start_time = end_time - timedelta(days=1)

    logger.info(f"Historical time range: {start_time} to {end_time}")

    # Track cache statistics (single manager instance)
    cache_stats = {"hits": 0, "misses": 0, "errors": 0}

    # Using DataSourceManager
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,
    ) as manager:
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

        # Display results
        logger.info(f"Historical data retrieved: {len(df)} rows")

        # Display a sample of the data
        if not df.empty:
            logger.info("\nSample historical data:")
            print(df.head().to_string())

            # No need to call get_cache_stats() again as we're tracking it
            logger.info(f"\nCache statistics: {cache_stats}")
        else:
            logger.info("No data retrieved. Attempting with 1-minute data instead.")

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

            logger.info(f"1-minute data retrieved: {len(df_minute)} rows")
            if not df_minute.empty:
                logger.info("\nSample 1-minute data:")
                print(df_minute.head().to_string())
                logger.info(f"\nFinal cache statistics: {cache_stats}")


async def example_fetch_same_day_minute_data():
    """Example function to fetch 1-minute data for the current day using DataSourceManager."""
    # Log current time for reference
    now = datetime.now(timezone.utc)
    logger.info(f"Current time: {now.isoformat()}")
    logger.info("\nFetching 1-minute data for the current day (recommended approach)")

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

    logger.info(f"Same-day time range: {start_time} to {end_time}")

    # Using DataSourceManager with async context manager
    # Note: This example uses a single manager instance, so cache statistics
    # are reported directly after operations complete.
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,  # Enable caching
    ) as manager:
        # Fetch 1-minute data
        df = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.MINUTE_1,  # Using 1-minute interval
        )

        # Display results
        logger.info(f"1-minute data retrieved: {len(df)} rows")
        logger.info(f"Data shape: {df.shape}")
        logger.info(f"Data columns: {df.columns.tolist()}")

        # Display a sample of the data
        if not df.empty:
            logger.info("\nSample 1-minute data:")
            print(df.head().to_string())

        # Get cache statistics - single instance so direct reporting is appropriate
        cache_stats = manager.get_cache_stats()
        logger.info(f"\nCache statistics: {cache_stats}")


async def example_fetch_unavailable_data():
    """Example function demonstrating robust handling of unavailable data."""
    # Log current time for reference
    now = datetime.now(timezone.utc)
    logger.info(f"Current time: {now.isoformat()}")
    logger.info("\nDemonstrating robust handling of unavailable data")

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Track cache statistics (single manager instance with multiple operations)
    cache_stats = {"hits": 0, "misses": 0, "errors": 0}

    # A future date - this should be properly rejected by the DataSourceManager
    future_start_time = now + timedelta(days=1)
    future_end_time = now + timedelta(days=2)

    logger.info(
        f"Attempting to fetch future data: {future_start_time} to {future_end_time}"
    )

    # Using DataSourceManager
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,
    ) as manager:
        # Try to fetch future data (this should return an empty DataFrame)
        future_df = await manager.get_data(
            symbol="BTCUSDT",
            start_time=future_start_time,
            end_time=future_end_time,
            interval=Interval.MINUTE_1,
        )

        # Update stats after first operation
        current_stats = manager.get_cache_stats()
        for key in current_stats:
            cache_stats[key] += current_stats[key]

        # Display results - should be an empty DataFrame with the correct structure
        logger.info(f"Future data request result: {len(future_df)} rows")
        logger.info(
            f"Is empty DataFrame properly structured: {future_df.empty and not future_df.columns.empty}"
        )

        # Try with a non-existent symbol
        logger.info("\nAttempting to fetch data for a non-existent symbol:")
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

        logger.info(f"Invalid symbol result: {len(invalid_df)} rows")
        logger.info(f"\nFinal cache statistics: {cache_stats}")


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
    logger.info(f"\n=== {description} ===")
    try:
        # Convert string interval to Interval enum if needed
        interval_enum = None
        if isinstance(interval, str):
            for i in Interval:
                if i.value == interval:
                    interval_enum = i
                    break

        if interval_enum is None:
            logger.error(f"Invalid interval string: {interval}")
            return pd.DataFrame()

        # Check if the interval is supported by this market type
        if not is_interval_supported(market_type, interval_enum):
            logger.error(
                f"Interval {interval} is not supported for market type {market_type.name}"
            )
            return pd.DataFrame()

        async with DataSourceManager(
            market_type=market_type,
            cache_dir=cache_dir,
            use_cache=True,
            max_concurrent=50,
            retry_count=3,
            max_concurrent_downloads=10,
        ) as manager:
            df = await manager.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval_enum,
            )

            logger.info(
                f"{market_type.name} {symbol} {interval} data retrieved: {len(df)} rows"
            )
            if not df.empty:
                logger.info(f"\nSample {market_type.name} {symbol} {interval} data:")
                print(df.head(3).to_string())

            return df
    except Exception as e:
        logger.error(
            f"Error retrieving {market_type.name} {symbol} {interval} data: {e}"
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
        "\nDemonstrating data retrieval across different market types and intervals"
    )

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Define time range (recent data that should be available)
    # Use data from 5 days ago to ensure data availability in all markets
    end_time = now - timedelta(days=5)
    start_time = end_time - timedelta(hours=4)  # 4-hour window

    logger.info(f"Time range: {start_time} to {end_time}")

    # Log which intervals are supported by each market type
    logger.info("\nSupported intervals by market type:")
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
        logger.info(f"  {market_type.name}: {', '.join(supported)}")

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
        max_concurrent=50,
        retry_count=3,
        max_concurrent_downloads=10,
    ) as manager:
        logger.info("\n=== 1. SPOT market with 1-second BTCUSDT data ===")
        df = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=short_end_time,
            interval=Interval.SECOND_1,
        )
        logger.info(
            f"SPOT BTCUSDT {Interval.SECOND_1.value} data retrieved: {len(df)} rows"
        )
        if not df.empty:
            logger.info(f"\nSample SPOT BTCUSDT {Interval.SECOND_1.value} data:")
            print(df.head(3).to_string())

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
        max_concurrent=50,
        retry_count=3,
        max_concurrent_downloads=10,
    ) as manager:
        logger.info("\n=== 2. SPOT market with 15-minute ETHUSDT data ===")
        df = await manager.get_data(
            symbol="ETHUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.MINUTE_15,
        )
        logger.info(
            f"SPOT ETHUSDT {Interval.MINUTE_15.value} data retrieved: {len(df)} rows"
        )
        if not df.empty:
            logger.info(f"\nSample SPOT ETHUSDT {Interval.MINUTE_15.value} data:")
            print(df.head(3).to_string())

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
        max_concurrent=50,
        retry_count=3,
        max_concurrent_downloads=10,
    ) as manager:
        logger.info(
            "\n=== 3. USDT-margined futures (UM) with 1-minute BTCUSDT data ==="
        )
        df = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.MINUTE_1,
        )
        logger.info(
            f"FUTURES_USDT BTCUSDT {Interval.MINUTE_1.value} data retrieved: {len(df)} rows"
        )
        if not df.empty:
            logger.info(
                f"\nSample FUTURES_USDT BTCUSDT {Interval.MINUTE_1.value} data:"
            )
            print(df.head(3).to_string())

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
        max_concurrent=50,
        retry_count=3,
        max_concurrent_downloads=10,
    ) as manager:
        logger.info("\n=== 4. Coin-margined futures (CM) with 3-minute BTCUSD data ===")
        df = await manager.get_data(
            symbol="BTCUSD",  # _PERP suffix should be added automatically
            start_time=start_time,
            end_time=end_time,
            interval=Interval.MINUTE_3,
        )
        logger.info(
            f"FUTURES_COIN BTCUSD {Interval.MINUTE_3.value} data retrieved: {len(df)} rows"
        )
        if not df.empty:
            logger.info(f"\nSample FUTURES_COIN BTCUSD {Interval.MINUTE_3.value} data:")
            print(df.head(3).to_string())

        # Update market stats
        stats = manager.get_cache_stats()
        for key in stats:
            market_stats[MarketType.FUTURES_COIN][key] += stats[key]

    # Display cache statistics for all markets
    logger.info("\nCache statistics by market type:")
    for market_type, market_name in [
        (MarketType.SPOT, "SPOT"),
        (MarketType.FUTURES_USDT, "FUTURES_USDT (UM)"),
        (MarketType.FUTURES_COIN, "FUTURES_COIN (CM)"),
    ]:
        logger.info(f"  {market_name}: {market_stats[market_type]}")


async def main():
    """Run the example functions."""
    # Log the current time at the start of execution for reference
    now = datetime.now(timezone.utc)
    logger.info(f"Example script starting at: {now.isoformat()}")

    # Check if a specific example function was requested
    if len(sys.argv) > 1:
        example_name = sys.argv[1]
        example_map = {
            "example_fetch_recent_data": example_fetch_recent_data,
            "example_fetch_historical_data": example_fetch_historical_data,
            "example_fetch_same_day_minute_data": example_fetch_same_day_minute_data,
            "example_fetch_unavailable_data": example_fetch_unavailable_data,
            "example_different_market_types": example_different_market_types,
        }

        if example_name in example_map:
            try:
                logger.info(f"Running example: {example_name}")
                await example_map[example_name]()
                return
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down gracefully...")
            except Exception as e:
                logger.error(f"Error in example {example_name}: {e}")
                logger.debug(f"Error details: {traceback.format_exc()}")
                sys.exit(1)
        else:
            logger.error(f"Unknown example: {example_name}")
            logger.info(f"Available examples: {', '.join(example_map.keys())}")
            sys.exit(1)

    # If no specific example is requested, run all examples
    try:
        # Run all example functions
        await example_fetch_recent_data()
        await example_fetch_historical_data()
        await example_fetch_same_day_minute_data()
        await example_fetch_unavailable_data()
        await example_different_market_types()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.debug(f"Main function error: {traceback.format_exc()}")
        sys.exit(1)


def handle_signals():
    """Set up signal handlers for graceful shutdown."""
    loop = asyncio.get_event_loop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(sig, loop))
        )


async def shutdown(sig, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info(f"Received exit signal {sig.name}...")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]

    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)

    loop.stop()


if __name__ == "__main__":
    # clear screen
    os.system("cls" if os.name == "nt" else "clear")

    # precisly find out what this script path and how this script is executed
    logger.debug(f"This script path: {os.path.abspath(__file__)}")
    logger.debug(f"This script is executed with: {sys.argv}")

    logger.info(
        f"Current UTC date time precision up to milliseconds: {datetime.now(timezone.utc).isoformat(timespec='milliseconds')}"
    )
    # Set up signal handlers
    handle_signals()

    # Run the main function
    asyncio.run(main())
