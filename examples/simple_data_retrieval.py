#!/usr/bin/env python
"""
Simple example script demonstrating basic usage of the DataSourceManager.

This example shows:
1. Basic data retrieval for a single symbol
2. Using the caching mechanism
3. Asynchronous operations for data retrieval

This serves as a simpler introduction to the system compared to the
more comprehensive verify_data_retrieval.py example.
"""

import asyncio
import time
from datetime import datetime, timezone, timedelta

from utils.logger_setup import logger
from rich import print
from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import MarketType, Interval, DataProvider
from utils.error_handling import cleanup_tasks

# Setup logging
logger.setup_root(level="INFO", show_filename=True)


async def fetch_single_symbol(symbol, interval, days_back, use_cache=True):
    """
    Fetch data for a single symbol using the DataSourceManager.

    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")
        interval: Time interval for the data
        days_back: Number of days to go back from current time
        use_cache: Whether to use the caching system

    Returns:
        DataFrame with market data if successful, None otherwise
    """
    print(
        f"Fetching data for {symbol} with interval {interval.value}, cache={'enabled' if use_cache else 'disabled'}"
    )

    # Calculate time range
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days_back)

    print(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")

    # Create data manager
    async with DataSourceManager(
        market_type=MarketType.SPOT, provider=DataProvider.BINANCE, use_cache=use_cache
    ) as manager:
        try:
            # Request data with timing but no timeout
            start_fetch = time.time()
            df = await manager.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                enforce_source=DataSource.REST,
            )
            elapsed = time.time() - start_fetch

            # Show results
            if df is not None and not df.empty:
                print(f"✓ Retrieved {len(df)} records in {elapsed:.2f}s")
                print(f"  First record: {df.index.min()}")
                print(f"  Last record: {df.index.max()}")

                # Display a few example records
                print("\nSample data (first 3 records):")
                print(df.head(3))
                return df
            else:
                print(f"✗ No data retrieved for {symbol}")
                return None

        except Exception as e:
            logger.error(f"Error retrieving data: {str(e)}")
            print(f"✗ Error: {str(e)}")
            return None


async def demonstrate_caching_benefit(symbol, interval, days_back):
    """
    Demonstrate the benefit of caching by fetching the same data twice,
    first without cache and then with cache enabled.
    """
    print("\n" + "=" * 50)
    print("DEMONSTRATING CACHING BENEFIT")
    print("=" * 50)

    # First fetch: Without cache
    print("\n1. First fetch (cache disabled):")
    await fetch_single_symbol(symbol, interval, days_back, use_cache=False)

    # Second fetch: With cache
    print("\n2. Second fetch (cache enabled):")
    await fetch_single_symbol(symbol, interval, days_back, use_cache=True)

    print(
        "\nNote: The second fetch should be significantly faster if data was cached successfully."
    )


async def fetch_multiple_symbols_async(symbols, interval, days_back):
    """
    Fetch data for multiple symbols concurrently using asyncio.

    Args:
        symbols: List of trading symbols
        interval: Time interval for the data
        days_back: Number of days to go back from current time
    """
    print("\n" + "=" * 50)
    print("DEMONSTRATING ASYNCHRONOUS OPERATIONS")
    print("=" * 50)

    # Calculate time range
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days_back)

    # Create data manager
    async with DataSourceManager(
        market_type=MarketType.SPOT, provider=DataProvider.BINANCE, use_cache=True
    ) as manager:
        # Create tasks for each symbol
        tasks = []
        for symbol in symbols:
            tasks.append(
                asyncio.create_task(
                    manager.get_data(
                        symbol=symbol,
                        start_time=start_time,
                        end_time=end_time,
                        interval=interval,
                        enforce_source=DataSource.REST,
                    )
                )
            )

        print(f"Started {len(tasks)} concurrent fetch tasks")

        try:
            # Wait for all tasks to complete without timeout
            start_time = time.time()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            elapsed = time.time() - start_time

            # Process results
            success_count = 0
            for i, result in enumerate(results):
                symbol = symbols[i]
                if isinstance(result, Exception):
                    print(f"✗ {symbol}: Error - {str(result)}")
                elif result is None or result.empty:
                    print(f"✗ {symbol}: No data retrieved")
                else:
                    success_count += 1
                    print(f"✓ {symbol}: Retrieved {len(result)} records")

            print(f"\nCompleted {len(tasks)} fetch operations in {elapsed:.2f}s")
            print(
                f"Success rate: {success_count}/{len(tasks)} ({success_count/len(tasks)*100:.1f}%)"
            )
        except Exception as e:
            print(f"Error in concurrent operations: {str(e)}")
            # Use the existing cleanup_tasks utility from error_handling.py
            await cleanup_tasks(tasks)


async def main():
    """Run all example operations."""
    print("\n" + "=" * 60)
    print("SIMPLE DATA RETRIEVAL EXAMPLE")
    print("=" * 60)

    # Record all running tasks at start for leak detection
    tasks_at_start = len(asyncio.all_tasks())
    logger.info(f"Starting with {tasks_at_start} active tasks")

    # Example parameters
    symbol = "BTCUSDT"
    interval = Interval.HOUR_1
    days_back = 3

    # Example 1: Basic data retrieval
    print("\n" + "=" * 50)
    print("BASIC DATA RETRIEVAL EXAMPLE")
    print("=" * 50)
    await fetch_single_symbol(symbol, interval, days_back)

    # Example 2: Caching benefit demonstration
    await demonstrate_caching_benefit(symbol, interval, days_back)

    # Example 3: Asynchronous operations with fewer symbols and less data
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]  # Reduced symbol count
    await fetch_multiple_symbols_async(symbols, interval, 1)

    # Check for task leakage at the end
    tasks_at_end = len(asyncio.all_tasks())
    if tasks_at_end > tasks_at_start:
        logger.warning(
            f"Task leakage detected: {tasks_at_end - tasks_at_start} more tasks at end than at start"
        )
        print(
            f"Task leakage detected: {tasks_at_end - tasks_at_start} more tasks at end than at start"
        )
    else:
        logger.info(f"No task leakage detected. Tasks at end: {tasks_at_end}")
        print(f"No task leakage detected. Tasks at end: {tasks_at_end}")

    print("\n" + "=" * 60)
    print("EXAMPLE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
