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
from utils.validation import DataValidation
from utils.time_utils import align_time_boundaries, estimate_record_count
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


def validate_and_align_time_boundaries(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
    handle_future_dates: str = "truncate",
):
    """Validate and align time boundaries for market data retrieval.

    This function:
    1. Validates the time range and handles future dates according to specified strategy
    2. Aligns the time boundaries to interval boundaries following Binance API rules
    3. Estimates the expected number of records

    Args:
        symbol: Trading pair symbol
        start_time: Original start time
        end_time: Original end time
        interval: Time interval
        handle_future_dates: How to handle future dates ("error", "truncate", "allow")

    Returns:
        Tuple of (aligned_start_time, aligned_end_time, metadata)
        where metadata includes expected record count and warnings
    """
    # First validate the query time boundaries
    validated_start, validated_end, metadata = (
        DataValidation.validate_query_time_boundaries(
            start_time=start_time,
            end_time=end_time,
            handle_future_dates=handle_future_dates,
            interval=interval,
        )
    )

    # Then align the validated boundaries to interval boundaries
    aligned_start, aligned_end = align_time_boundaries(
        validated_start, validated_end, interval
    )

    # Estimate expected record count
    expected_records = estimate_record_count(aligned_start, aligned_end, interval)

    # Add alignment and record count info to metadata
    metadata["aligned_start"] = aligned_start
    metadata["aligned_end"] = aligned_end
    metadata["expected_records"] = expected_records
    metadata["original_symbol"] = symbol.upper()

    logger.info(
        f"Validated and aligned time boundaries for {symbol}:\n"
        f"Original: {start_time.isoformat()} -> {end_time.isoformat()}\n"
        f"Validated: {validated_start.isoformat()} -> {validated_end.isoformat()}\n"
        f"Aligned: {aligned_start.isoformat()} -> {aligned_end.isoformat()}\n"
        f"Expected records: {expected_records}"
    )

    # Log any warnings
    for warning in metadata.get("warnings", []):
        logger.warning(warning)

    return aligned_start, aligned_end, metadata


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
    # Validate and align time boundaries
    aligned_start, aligned_end, metadata = validate_and_align_time_boundaries(
        symbol=symbol, start_time=start_time, end_time=end_time, interval=interval
    )

    # Use the aligned time boundaries for the data request
    result, elapsed = await with_timeout_handling(
        manager.get_data,
        timeout,
        f"data retrieval for {symbol}",
        symbol=symbol,
        start_time=aligned_start,
        end_time=aligned_end,
        interval=interval,
        enforce_source=source,
    )

    # Add metadata to the result for analysis
    if result is not None and isinstance(result, pd.DataFrame) and not result.empty:
        result.attrs["request_metadata"] = metadata

    return result, elapsed


async def fetch_data_for_verification(manager, symbol, start_time, end_time, interval):
    """Fetch data for verification and return success indicator."""
    try:
        logger.info(f"Starting concurrent fetch for {symbol}")

        # Validate and align time boundaries
        aligned_start, aligned_end, metadata = validate_and_align_time_boundaries(
            symbol=symbol, start_time=start_time, end_time=end_time, interval=interval
        )

        # Use the task execution utility with proper cleanup
        df = await execute_with_task_cleanup(
            manager.get_data,
            timeout=30,
            operation_name=f"concurrent fetch for {symbol}",
            symbol=symbol,
            start_time=aligned_start,
            end_time=aligned_end,
            interval=interval,
            enforce_source=DataSource.REST,
        )

        if df is None:
            return (0, symbol, None)

        if df.empty:
            logger.debug(f"No data retrieved for {symbol} in concurrent operation")
            return (0, symbol, None)

        # Add metadata to returned dataframe
        df.attrs["request_metadata"] = metadata

        # Analyze completeness
        actual_records = len(df)
        expected_records = metadata.get("expected_records", 0)
        completeness = (
            (actual_records / expected_records * 100) if expected_records > 0 else 0
        )

        logger.info(
            f"Successfully retrieved {actual_records} records for {symbol} "
            f"({completeness:.1f}% of expected {expected_records})"
        )
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

                        # Extract metadata for additional info
                        metadata = df_data.attrs.get("request_metadata", {})
                        expected_records = metadata.get("expected_records", 0)
                        completeness = (
                            (len(df_data) / expected_records * 100)
                            if expected_records > 0
                            else 0
                        )

                        additional_info = {
                            "Summary": f"{success_count}/{total_operations} successful operations, {error_count} errors",
                            "Expected Records": f"{expected_records}",
                            "Completeness": f"{completeness:.1f}%",
                        }

                        # Add time boundary info
                        if "aligned_start" in metadata and "aligned_end" in metadata:
                            additional_info["Aligned Boundaries"] = (
                                f"{metadata['aligned_start'].isoformat()} - {metadata['aligned_end'].isoformat()}"
                            )

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

    # Validate and align time boundaries before printing request info
    aligned_start, aligned_end, metadata = validate_and_align_time_boundaries(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        handle_future_dates="allow",  # Allow future dates for this test
    )

    # Print both original and aligned times
    print(
        f"Original request: {symbol} from {start_time.isoformat()} to {end_time.isoformat()} "
        f"with {interval.value} interval"
    )
    print(
        f"Aligned request: {symbol} from {aligned_start.isoformat()} to {aligned_end.isoformat()} "
        f"with {interval.value} interval"
    )
    print(f"Expected records: {metadata.get('expected_records', 0)}")

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

        # Extract metadata for additional info
        request_metadata = df.attrs.get("request_metadata", {})
        expected_records = request_metadata.get("expected_records", 0)
        completeness = (len(df) / expected_records * 100) if expected_records > 0 else 0

        additional_info = {
            "Expected Records": f"{expected_records}",
            "Actual Records": f"{len(df)}",
            "Completeness": f"{completeness:.1f}%",
            "Aligned Start": f"{aligned_start.isoformat()}",
            "Aligned End": f"{aligned_end.isoformat()}",
        }

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
            additional_info=additional_info,
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

    # Validate and align time boundaries, with special handling for very recent data
    symbol = "BTCUSDT"
    interval = Interval.HOUR_1

    aligned_start, aligned_end, metadata = validate_and_align_time_boundaries(
        symbol=symbol, start_time=start_time, end_time=end_time, interval=interval
    )

    try:
        # Use warning capture context manager
        with capture_warnings() as warnings_detected:
            df, elapsed = await get_data_with_timeout(
                manager, symbol, start_time, end_time, interval
            )

        if df is None or len(df) == 0:
            logger.warning("No recent hourly data retrieved")
            return False

        # Get metadata for analysis
        request_metadata = df.attrs.get("request_metadata", {})
        expected_records = request_metadata.get("expected_records", 0)

        # Calculate completeness
        actual_records = len(df)
        completeness = (
            (actual_records / expected_records * 100) if expected_records > 0 else 0
        )
        missing_records = (
            expected_records - actual_records
            if expected_records > actual_records
            else 0
        )

        # Use common display function with additional info
        additional_info = {
            "Expected Records": f"{expected_records}",
            "Actual Records": f"{actual_records}",
            "Completeness": f"{completeness:.1f}%",
            "Missing Records": f"{missing_records}",
            "Warnings": f"{len([w for w in warnings_detected if 'may not be fully consolidated' in w])}",
            "Aligned Start": f"{aligned_start.isoformat()}",
            "Aligned End": f"{aligned_end.isoformat()}",
        }

        display_verification_results(
            df=df,
            symbol=symbol,
            interval=interval,
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

    # Validate and align time boundaries
    symbol = "BTCUSDT"
    interval = Interval.HOUR_1

    aligned_start, aligned_end, metadata = validate_and_align_time_boundaries(
        symbol=symbol, start_time=start_time, end_time=end_time, interval=interval
    )

    try:
        # Use warning capture context manager
        with capture_warnings() as warnings_detected:
            df, elapsed = await get_data_with_timeout(
                manager, symbol, start_time, end_time, interval, timeout=30
            )

        if df is None or len(df) == 0:
            logger.warning("No partial hour data retrieved at all")
            return False

        # Get metadata for analysis
        request_metadata = df.attrs.get("request_metadata", {})
        expected_records = request_metadata.get("expected_records", 0)

        # Calculate completeness
        actual_records = len(df)
        completeness = (
            (actual_records / expected_records * 100) if expected_records > 0 else 0
        )
        missing_records = (
            expected_records - actual_records
            if expected_records > actual_records
            else 0
        )

        # Prepare additional info
        additional_info = {
            "Expected Records": f"{expected_records}",
            "Actual Records": f"{actual_records}",
            "Completeness": f"{completeness:.1f}%",
            "Aligned Start": f"{aligned_start.isoformat()}",
            "Aligned End": f"{aligned_end.isoformat()}",
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
            symbol=symbol,
            interval=interval,
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
