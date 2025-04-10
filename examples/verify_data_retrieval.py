#!/usr/bin/env python
"""
Comprehensive verification script for timeout handling in the DataSourceManager.

Tests multiple scenarios:
1. Concurrent data retrieval - Runs multiple data fetches simultaneously
2. Extended historical data - Tests retrieving larger historical datasets
3. Recent hourly data - Tests retrieving recent data that may not be fully consolidated
4. Partial data retrieval - Tests data spanning from available past data to recent data

Note:
    Currently, Binance is the only supported data provider, but the system
    is designed to support additional providers like TradeStation in the future.
    This is why we explicitly set Binance as the provider when creating a DataSourceManager.
"""

import asyncio
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd

from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import MarketType, Interval, DataProvider
from utils.logger_setup import logger
from utils.error_handling import (
    capture_warnings,
    with_timeout_handling,
    safe_execute_verification,
    execute_with_task_cleanup,
    cleanup_tasks,
    suppress_consolidation_warnings,
    display_verification_results,
)
from rich import print

# Set up logging for the verification script
logger.setup_root(level="WARNING", show_filename=True)


async def with_manager(func, *args, **kwargs):
    """Run a function with a DataSourceManager, handling cleanup and errors."""
    manager = None
    try:
        # Explicitly set Binance as the data provider
        manager = DataSourceManager(
            market_type=MarketType.SPOT, provider=DataProvider.BINANCE, use_cache=False
        )
        return await func(manager, *args, **kwargs)
    except Exception as e:
        logger.error(f"Error in {func.__name__}: {str(e)}")
        return None
    finally:
        if manager:
            try:
                await manager.__aexit__(None, None, None)
            except Exception as e:
                logger.error(f"Error during manager cleanup: {str(e)}")


async def get_data_with_timeout(
    manager, symbol, start_time, end_time, interval, timeout=30, source=DataSource.REST
):
    """Get data with timeout protection, handling errors consistently."""
    result, elapsed = await with_timeout_handling(
        manager.get_data,
        timeout,
        f"data retrieval for {symbol}",
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        enforce_source=source,
        # We don't need to pass provider explicitly in the get_data call
        # as it's already set in the manager during initialization
    )
    return result, elapsed


async def fetch_data_for_verification(manager, symbol, start_time, end_time, interval):
    """Fetch data for verification and return success indicator."""
    try:
        logger.info(f"Starting concurrent fetch for {symbol}")

        # Use the task execution utility with proper cleanup
        df = await execute_with_task_cleanup(
            manager.get_data,
            timeout=30,
            operation_name=f"concurrent fetch for {symbol}",
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            enforce_source=DataSource.REST,
        )

        if df is None:
            return (0, symbol, None)

        if df.empty:
            logger.debug(f"No data retrieved for {symbol} in concurrent operation")
            return (0, symbol, None)

        logger.info(f"Successfully retrieved {len(df)} records for {symbol}")
        return (len(df), symbol, df)

    except Exception as e:
        logger.error(f"Error in concurrent fetch for {symbol}: {str(e)}")
        raise


async def _verify_concurrent_data_retrieval(manager):
    """Verify concurrent data retrieval for multiple symbols."""
    logger.info("===== VERIFYING CONCURRENT DATA RETRIEVAL =====")

    # Define symbols and time range
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    # Create tasks for concurrent data retrieval
    tasks = []
    for symbol in symbols:
        for _ in range(2):  # Create two tasks for each symbol
            tasks.append(
                asyncio.create_task(
                    fetch_data_for_verification(
                        manager, symbol, start_time, end_time, Interval.MINUTE_1
                    )
                )
            )

    # Using our context manager to suppress consolidation warnings during concurrent tests
    with suppress_consolidation_warnings():
        try:
            # Run tasks concurrently with timeout protection
            start_op = time.time()
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=60,  # Overall timeout for all tasks
            )
            elapsed = time.time() - start_op

            # Analyze results
            total_operations = len(results)
            success_count = sum(1 for r in results if isinstance(r, tuple) and r[0] > 0)
            empty_count = sum(1 for r in results if isinstance(r, tuple) and r[0] == 0)
            error_count = sum(1 for r in results if isinstance(r, Exception))

            # Check for timeout errors
            timeout_errors = sum(
                1
                for r in results
                if isinstance(r, Exception) and "timeout" in str(r).lower()
            )
            if timeout_errors > 0:
                logger.warning(f"Found {timeout_errors} timeout errors")

            # Print only the first successful result as an example
            for result in results:
                if isinstance(result, tuple) and result[0] > 0:
                    df_data = result[2]
                    if df_data is not None and isinstance(df_data, pd.DataFrame):
                        symbol = result[1]
                        additional_info = {
                            "Summary": f"{success_count}/{total_operations} successful operations, {error_count} errors"
                        }
                        display_verification_results(
                            df=df_data,
                            symbol=symbol,
                            interval=Interval.MINUTE_1,
                            start_time=start_time,
                            end_time=end_time,
                            manager=manager,
                            elapsed=elapsed,
                            test_name=f"CONCURRENT DATA RETRIEVAL EXAMPLE ({symbol})",
                            additional_info=additional_info,
                        )
                        break  # Only show one example

            return success_count
        except asyncio.TimeoutError:
            # Clean up any remaining tasks safely
            logger.warning(
                "Timeout during concurrent data retrieval, cleaning up tasks..."
            )
            await cleanup_tasks(tasks)
            return 0
        except Exception as e:
            logger.error(f"Error in concurrent verification: {str(e)}")
            # Clean up any remaining tasks safely
            await cleanup_tasks(tasks)
            return 0


async def _verify_extended_historical_data(manager):
    """Verify retrieval of extended historical data."""
    logger.info("===== VERIFYING EXTENDED HISTORICAL DATA RETRIEVAL =====")
    print("===== VERIFYING EXTENDED HISTORICAL DATA RETRIEVAL =====")

    # Specify exact dates with precise, odd-second granularity
    # Format: datetime(year, month, day, hour, minute, second, microsecond, tzinfo=timezone.utc)
    start_time = datetime(
        2025, 1, 1, 12, 34, 57, 123456, tzinfo=timezone.utc
    )  # 12:34:57.123456 on Jan 1, 2025
    end_time = datetime(
        2025, 1, 3, 15, 27, 39, 987654, tzinfo=timezone.utc
    )  # 15:27:39.987654 on Jan 3, 2025

    # Specify the trading symbol and interval
    symbol = "BTCUSDT"
    interval = Interval.SECOND_1  # Using 1-second interval

    print(
        f"Requesting data for {symbol} from {start_time.isoformat()} to {end_time.isoformat()} with {interval.value} interval"
    )

    try:
        print("Starting data retrieval with timeout...")
        df, elapsed = await get_data_with_timeout(
            manager, symbol, start_time, end_time, interval, timeout=60
        )
        print(
            f"Data retrieval complete in {elapsed:.2f}s, got {len(df) if df is not None else 'None'} records"
        )

        if df is None or df.empty:
            logger.warning(f"No historical data retrieved for {symbol}")
            print(f"Warning: No historical data retrieved for {symbol}")
            return False

        # Use common display function
        display_verification_results(
            df=df,
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            manager=manager,
            elapsed=elapsed,
            test_name="EXTENDED HISTORICAL DATA RETRIEVAL",
        )
        return True

    except Exception as e:
        logger.error(f"Error in extended historical data verification: {str(e)}")
        print(f"Error in extended historical data verification: {str(e)}")
        return False


async def _verify_very_recent_hourly_data(manager):
    """Verify retrieval of very recent hourly data that may not be fully consolidated."""
    logger.info("===== VERIFYING VERY RECENT HOURLY DATA RETRIEVAL =====")

    # Define time range - use current time to ensure we hit consolidation warnings
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=24)

    try:
        # Use warning capture context manager
        with capture_warnings() as warnings_detected:
            df, elapsed = await get_data_with_timeout(
                manager, "BTCUSDT", start_time, end_time, Interval.HOUR_1
            )

        if df is None or len(df) == 0:
            logger.warning("No recent hourly data retrieved")
            return False

        # Calculate expected records and completeness
        total_hours = (end_time - start_time).total_seconds() / 3600
        completion_pct = (len(df) / int(total_hours)) * 100 if total_hours > 0 else 0

        # Use common display function with additional info
        additional_info = {
            "Expected Records": f"~{int(total_hours)}",
            "Completion": f"{completion_pct:.1f}%",
            "Warnings": f"{len([w for w in warnings_detected if 'may not be fully consolidated' in w])}",
        }

        display_verification_results(
            df=df,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start_time,
            end_time=end_time,
            manager=manager,
            elapsed=elapsed,
            test_name="RECENT HOURLY DATA RETRIEVAL",
            warnings_detected=warnings_detected,
            additional_info=additional_info,
        )
        return True

    except Exception:
        # Error already logged in get_data_with_timeout
        return False


async def _verify_partial_hour_data(manager):
    """Verify retrieval of data that spans from available past data to current incomplete hour."""
    logger.info("===== VERIFYING PARTIAL HOUR DATA RETRIEVAL =====")

    # Define time range that spans from certainly available data to incomplete current hour
    current_time = datetime.now(timezone.utc)
    # Start time is 48 hours ago (should be fully available)
    start_time = current_time - timedelta(hours=48)
    # End time is current time (current hour will not be fully consolidated)
    end_time = current_time

    try:
        # Use warning capture context manager
        with capture_warnings() as warnings_detected:
            df, elapsed = await get_data_with_timeout(
                manager, "BTCUSDT", start_time, end_time, Interval.HOUR_1, timeout=30
            )

        if df is None or len(df) == 0:
            logger.warning("No partial hour data retrieved at all")
            return False

        # Calculate expected records and completeness
        total_hours = (end_time - start_time).total_seconds() / 3600
        actual_expected = int(total_hours) + 1  # Add one for partial current hour
        completion_pct = (len(df) / actual_expected) * 100 if actual_expected > 0 else 0
        missing_records = actual_expected - len(df)

        # Prepare additional info
        additional_info = {
            "Expected": f"~{actual_expected}",
            "Completion": f"{completion_pct:.1f}%",
        }

        # Check if data is missing (likely the current incomplete hour)
        if missing_records > 0:
            additional_info["Missing"] = (
                f"{missing_records} record(s) (likely the current incomplete hour)"
            )

            # Find the most recent hour timestamp
            current_hour = current_time.replace(minute=0, second=0, microsecond=0)
            most_recent_data = df.index.max()

            # Check if the current hour is missing from the dataset
            if most_recent_data < current_hour:
                time_diff = current_hour - most_recent_data
                additional_info["Current hour data missing"] = (
                    f"Latest data is from {time_diff} ago"
                )

        # Use common display function
        display_verification_results(
            df=df,
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start_time,
            end_time=end_time,
            manager=manager,
            elapsed=elapsed,
            test_name="PARTIAL HOUR DATA RETRIEVAL",
            warnings_detected=warnings_detected,
            additional_info=additional_info,
        )

        # Show the most recent available data point
        if not df.empty:
            print("\nMost recent available data:")
            print(df.iloc[-1:])
            print("=" * 50)

        return True

    except Exception as e:
        logger.error(f"Error retrieving partial data: {str(e)}")
        return False


# Define verification functions with manager handling - simplified using a consistent pattern
verification_funcs = [
    ("concurrent data retrieval", _verify_concurrent_data_retrieval),
    ("extended historical data", _verify_extended_historical_data),
    ("recent hourly data", _verify_very_recent_hourly_data),
    ("partial hour data", _verify_partial_hour_data),
]

# Create verification functions that handle manager creation and cleanup
verify_functions = {
    name: lambda f=func: with_manager(f) for name, func in verification_funcs
}

# Extract individual verification functions for backward compatibility
verify_concurrent_data_retrieval = verify_functions["concurrent data retrieval"]
verify_extended_historical_data = verify_functions["extended historical data"]
verify_very_recent_hourly_data = verify_functions["recent hourly data"]
verify_partial_hour_data = verify_functions["partial hour data"]


async def main():
    """Run all verification tests."""
    logger.info("===== STARTING DATA RETRIEVAL VERIFICATION =====")
    print("STARTING DATA RETRIEVAL VERIFICATION")

    # Add a prominent notice about using Binance as the data provider
    print("\n" + "=" * 60)
    print("NOTICE: Using BINANCE as the explicit data provider for all tests")
    print("This system is designed to support additional providers in the future")
    print("=" * 60 + "\n")

    # Ensure logs directory exists
    Path("logs/timeout_incidents").mkdir(parents=True, exist_ok=True)

    # Record all running tasks at start
    tasks_at_start = len(asyncio.all_tasks())
    logger.info(f"Starting with {tasks_at_start} active tasks")

    # Run all tests sequentially but with consolidated error handling
    # Wrap the entire verification process in our custom context manager to suppress warnings
    with suppress_consolidation_warnings():
        for name, test_func in verification_funcs:
            print(f"Starting test: {name}")
            result = await safe_execute_verification(verify_functions[name], name)
            print(f"Completed test: {name}, result: {result}")

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

    logger.info("===== DATA RETRIEVAL VERIFICATION COMPLETE =====")
    print("DATA RETRIEVAL VERIFICATION COMPLETE")


if __name__ == "__main__":
    # Run the verification
    asyncio.run(main())
