#!/usr/bin/env python
"""
Comprehensive verification script for timeout handling in the DataSourceManager.

Tests multiple scenarios:
1. Concurrent data retrieval - Runs multiple data fetches simultaneously
2. Extended historical data - Tests retrieving larger historical datasets
3. Recent hourly data - Tests retrieving recent data that may not be fully consolidated
4. Partial data retrieval - Tests data spanning from available past data to recent data
"""

import asyncio
import contextlib
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import pandas as pd

from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import MarketType, Interval
from utils.logger_setup import logger
from rich import print

# Set up logging for the verification script
logger.setup_root(level="WARNING", show_filename=True)


@contextlib.contextmanager
def capture_warnings():
    """Context manager to capture warnings while preserving normal logging."""
    warnings = []
    original_warning_fn = logger.warning

    def warning_capture(*args, **kwargs):
        message = args[0] if args else kwargs.get("msg", "")
        warnings.append(message)
        return original_warning_fn(*args, **kwargs)

    try:
        logger.warning = warning_capture
        yield warnings
    finally:
        logger.warning = original_warning_fn


def display_df_summary(df, label="DataFrame"):
    """Display just the first and last record of a DataFrame."""
    if df.empty:
        print(f"{label}: Empty DataFrame")
        return

    print(f"{label} Summary ({len(df)} records):")
    print(f"First record ({df.index[0]}):")
    print(df.iloc[0:1])
    print("...")
    print(f"Last record ({df.index[-1]}):")
    print(df.iloc[-1:])


async def with_manager(func, *args, **kwargs):
    """Run a function with a DataSourceManager, handling cleanup and errors."""
    manager = None
    try:
        manager = DataSourceManager(market_type=MarketType.SPOT, use_cache=False)
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
    try:
        start_op = time.time()
        df = await asyncio.wait_for(
            manager.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                enforce_source=source,
            ),
            timeout=timeout,
        )
        elapsed = time.time() - start_op
        return df, elapsed
    except asyncio.TimeoutError:
        logger.error(f"Timeout retrieving data for {symbol}")
        raise
    except Exception as e:
        logger.error(f"Error retrieving data for {symbol}: {str(e)}")
        raise


async def fetch_data_for_verification(manager, symbol, start_time, end_time, interval):
    """Fetch data for verification and return success indicator."""
    try:
        logger.info(f"Starting concurrent fetch for {symbol}")
        df, elapsed = await get_data_with_timeout(
            manager, symbol, start_time, end_time, interval
        )

        if df.empty:
            logger.debug(f"No data retrieved for {symbol} in concurrent operation")
            return (0, symbol, None)

        logger.info(
            f"Successfully retrieved {len(df)} records for {symbol} in {elapsed:.2f}s"
        )
        return (len(df), symbol, df)
    except Exception:
        # Error already logged in get_data_with_timeout
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

    # Run tasks concurrently
    start_op = time.time()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.time() - start_op

    # Analyze results
    total_operations = len(results)
    success_count = sum(1 for r in results if isinstance(r, tuple) and r[0] > 0)
    empty_count = sum(1 for r in results if isinstance(r, tuple) and r[0] == 0)
    error_count = sum(1 for r in results if isinstance(r, Exception))

    # Check for timeout errors
    timeout_errors = sum(
        1 for r in results if isinstance(r, Exception) and "timeout" in str(r).lower()
    )
    if timeout_errors > 0:
        logger.warning(f"Found {timeout_errors} timeout errors")

    # Print only the first successful result as an example
    for result in results:
        if isinstance(result, tuple) and result[0] > 0:
            df_data = result[2]
            if df_data is not None and isinstance(df_data, pd.DataFrame):
                symbol = result[1]
                print(f"\n===== CONCURRENT DATA RETRIEVAL EXAMPLE ({symbol}) =====")
                print(f"Data Source: {DataSource.REST.name}, Records: {len(df_data)}")
                print(f"Time Range: {start_time.isoformat()} to {end_time.isoformat()}")
                display_df_summary(df_data, f"{symbol} Data")
                print(
                    f"Summary: {success_count}/{total_operations} successful operations, {error_count} errors"
                )
                print("=" * 50)
                break  # Only show one example

    return success_count


async def _verify_extended_historical_data(manager):
    """Verify retrieval of extended historical data."""
    logger.info("===== VERIFYING EXTENDED HISTORICAL DATA RETRIEVAL =====")

    # Define historical time range (3 days ago to now)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=3)

    try:
        df, elapsed = await get_data_with_timeout(
            manager, "BTCUSDT", start_time, end_time, Interval.HOUR_1, timeout=60
        )

        if df.empty:
            logger.warning("No historical data retrieved")
            return False

        # Print the DataFrame summary
        print("\n===== EXTENDED HISTORICAL DATA RETRIEVAL =====")
        print(f"Symbol: BTCUSDT, Interval: {Interval.HOUR_1.value}")
        print(f"Time Range: {start_time.isoformat()} to {end_time.isoformat()}")
        print(f"Retrieval Time: {elapsed:.2f}s, Records: {len(df)}")

        if not df.empty:
            data_range = f"{df.index.min()} to {df.index.max()}"
            print(f"Data Range: {data_range}")
            display_df_summary(df, "Historical Data")

        print("=" * 50)
        return True

    except Exception:
        # Error already logged in get_data_with_timeout
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

        if len(df) == 0:
            logger.warning("No recent hourly data retrieved")
            return False

        # Calculate expected records and completeness
        total_hours = (end_time - start_time).total_seconds() / 3600
        completion_pct = (len(df) / int(total_hours)) * 100 if total_hours > 0 else 0

        # Print minimal summary
        print("\n===== RECENT HOURLY DATA RETRIEVAL =====")
        print(f"Symbol: BTCUSDT, Interval: {Interval.HOUR_1.value}")
        print(f"Time Range: {start_time.isoformat()} to {end_time.isoformat()}")
        print(
            f"Expected Records: ~{int(total_hours)}, Actual: {len(df)} ({completion_pct:.1f}%)"
        )
        print(
            f"Warnings: {len([w for w in warnings_detected if 'may not be fully consolidated' in w])}"
        )

        if not df.empty:
            print(f"Data Range: {df.index.min()} to {df.index.max()}")
            display_df_summary(df, "Recent Hourly Data")

        print("=" * 50)
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
            df, _ = await get_data_with_timeout(
                manager, "BTCUSDT", start_time, end_time, Interval.HOUR_1
            )

        if len(df) == 0:
            logger.warning("No partial hour data retrieved at all")
            return False

        # Calculate expected records and completeness
        total_hours = (end_time - start_time).total_seconds() / 3600
        actual_expected = int(total_hours) + 1  # Add one for partial current hour
        completion_pct = (len(df) / actual_expected) * 100 if actual_expected > 0 else 0
        missing_records = actual_expected - len(df)

        # Print data summary focusing on the natural partial data scenario
        print("\n===== PARTIAL HOUR DATA RETRIEVAL =====")
        print(f"Symbol: BTCUSDT, Interval: {Interval.HOUR_1.value}")
        print(
            f"Time Range: {start_time.isoformat()} to {end_time.isoformat()} (current time)"
        )
        print(
            f"Expected: ~{actual_expected}, Actual: {len(df)} ({completion_pct:.1f}%)"
        )

        # Check if data is missing (likely the current incomplete hour)
        if missing_records > 0:
            print(
                f"Missing: {missing_records} record(s) (likely the current incomplete hour)"
            )

            # Find the most recent hour timestamp
            current_hour = current_time.replace(minute=0, second=0, microsecond=0)
            most_recent_data = df.index.max()

            # Check if the current hour is missing from the dataset
            if most_recent_data < current_hour:
                time_diff = current_hour - most_recent_data
                print(f"Current hour data missing: Latest data is from {time_diff} ago")

            print(f"Warnings: {len(warnings_detected)}")

            if len(warnings_detected) > 0:
                print("Sample warning: " + next(iter(warnings_detected)))

        if not df.empty:
            print(f"Data Range: {df.index.min()} to {df.index.max()}")

            # Show the most recent available data point
            print("\nMost recent available data:")
            print(df.iloc[-1:])

            # Show complete data summary
            display_df_summary(df, "Partial Hour Dataset")

        print("=" * 50)
        return True

    except Exception:
        # Error already logged in get_data_with_timeout
        return False


# Define verification functions with manager handling
verify_concurrent_data_retrieval = lambda: with_manager(
    _verify_concurrent_data_retrieval
)
verify_extended_historical_data = lambda: with_manager(_verify_extended_historical_data)
verify_very_recent_hourly_data = lambda: with_manager(_verify_very_recent_hourly_data)
verify_partial_hour_data = lambda: with_manager(_verify_partial_hour_data)


async def main():
    """Run all verification tests."""
    logger.info("===== STARTING DATA RETRIEVAL VERIFICATION =====")

    # Ensure logs directory exists
    Path("logs/timeout_incidents").mkdir(parents=True, exist_ok=True)

    # Run all tests sequentially but with consolidated error handling
    verification_tests = [
        ("concurrent data retrieval", verify_concurrent_data_retrieval),
        ("extended historical data", verify_extended_historical_data),
        ("recent hourly data", verify_very_recent_hourly_data),
        ("partial hour data", verify_partial_hour_data),
    ]

    for name, test_func in verification_tests:
        try:
            await test_func()
        except Exception as e:
            logger.error(f"Error during {name} verification: {str(e)}")

    logger.info("===== DATA RETRIEVAL VERIFICATION COMPLETE =====")


if __name__ == "__main__":
    # Run the verification
    asyncio.run(main())
