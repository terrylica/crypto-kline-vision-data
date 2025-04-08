#!/usr/bin/env python
"""
Comprehensive verification script for timeout handling in the DataSourceManager.

This script verifies that data retrieval works properly with the timeout fixes
implemented in the DataSourceManager by testing multiple scenarios:

1. Concurrent data retrieval - Runs multiple data fetches simultaneously
2. Extended historical data - Tests retrieving larger historical datasets
3. Recent hourly data - Tests retrieving recent data that may not be fully consolidated
4. Partial data retrieval - Tests data spanning from available past data to recent data

The script captures warnings, analyzes data completeness, and verifies no timeout errors
occur when using the fixed DataSourceManager implementation.

This file consolidates the key testing functionality from several previous test scripts
into a single comprehensive verification suite.
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
# logger.setup_root(level="DEBUG", show_filename=True)
logger.setup_root(level="WARNING", show_filename=True)
logger.info("Logger configured for data retrieval verification")


@contextlib.contextmanager
def capture_warnings():
    """Context manager to capture warnings while preserving normal logging.

    Returns:
        List of warning messages captured during execution
    """
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


async def verify_concurrent_data_retrieval():
    """Verify concurrent data retrieval for multiple symbols."""
    logger.info("===== VERIFYING CONCURRENT DATA RETRIEVAL =====")

    # Create manager
    manager = None
    try:
        manager = DataSourceManager(market_type=MarketType.SPOT, use_cache=False)

        # Define symbols and time range
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        logger.info(f"Time range for verification: {start_time} to {end_time}")

        # Create tasks for concurrent data retrieval
        tasks = []

        # Add multiple tasks for each symbol to increase concurrency
        for symbol in symbols:
            # Create two tasks for each symbol
            for _ in range(2):
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

        logger.info(f"Concurrent operations completed in {elapsed:.2f}s")
        logger.info(f"Total operations: {total_operations}")
        logger.info(f"Successful operations: {success_count}")
        logger.info(f"Empty results: {empty_count}")
        logger.info(f"Error count: {error_count}")

        # Check for timeout errors
        timeout_errors = sum(
            1
            for r in results
            if isinstance(r, Exception) and "timeout" in str(r).lower()
        )
        if timeout_errors > 0:
            logger.warning(f"Found {timeout_errors} timeout errors")
        else:
            logger.info("No timeout errors detected")

        # Print successful results DataFrames
        for i, result in enumerate(results):
            if isinstance(result, tuple) and result[0] > 0:
                df_data = result[2] if len(result) > 2 else None
                if df_data is not None and isinstance(df_data, pd.DataFrame):
                    symbol = result[1]
                    print(f"\n===== CONCURRENT DATA RETRIEVAL TASK {i+1} =====")
                    print(f"Symbol: {symbol}")
                    print(f"Interval: {Interval.MINUTE_1.value}")
                    print(f"Start Time: {start_time.isoformat()}")
                    print(f"End Time: {end_time.isoformat()}")
                    print(f"Data Source: {DataSource.REST.name}")
                    print(f"Records Retrieved: {len(df_data)}")
                    print(
                        f"Purpose: Testing concurrent data retrieval of the last 1 hour for {symbol}"
                    )
                    print(
                        f"Testing Scenario: Multiple simultaneous data requests (performance & stability test)"
                    )
                    print("\nDataFrame:")
                    print(df_data)
                    print("=" * 80)

    except Exception as e:
        logger.error(f"Error in concurrent verification: {str(e)}")
    finally:
        # Ensure proper cleanup
        if manager:
            try:
                await manager.__aexit__(None, None, None)
            except Exception as e:
                logger.error(f"Error during manager cleanup: {str(e)}")


async def verify_extended_historical_data():
    """Verify retrieval of extended historical data."""
    logger.info("===== VERIFYING EXTENDED HISTORICAL DATA RETRIEVAL =====")

    # Create manager
    manager = None
    try:
        manager = DataSourceManager(market_type=MarketType.SPOT, use_cache=False)

        # Define historical time range (3 days ago to now)
        end_time = datetime.now(timezone.utc)  # Use current time to test warnings
        start_time = end_time - timedelta(days=3)

        logger.info(f"Historical time range: {start_time} to {end_time}")

        try:
            start_op = time.time()

            # Retrieve extended historical data with timeout protection
            df = await asyncio.wait_for(
                manager.get_data(
                    symbol="BTCUSDT",
                    start_time=start_time,
                    end_time=end_time,
                    interval=Interval.HOUR_1,
                    enforce_source=DataSource.REST,
                ),
                timeout=60,  # 60-second timeout for historical data
            )

            elapsed = time.time() - start_op

            if df.empty:
                logger.warning("No historical data retrieved")
            else:
                logger.info(
                    f"Successfully retrieved {len(df)} historical records in {elapsed:.2f}s"
                )

                # Log time range of data
                if not df.empty:
                    first_time = df.index.min()
                    last_time = df.index.max()
                    logger.info(f"Data spans from {first_time} to {last_time}")

                    # Log a sample of the data
                    logger.info(
                        f"Historical data sample: {df.iloc[0:2].to_dict('records')}"
                    )

                    # Print the DataFrame
                    print("\n===== EXTENDED HISTORICAL DATA RETRIEVAL =====")
                    print(f"Symbol: BTCUSDT")
                    print(f"Interval: {Interval.HOUR_1.value}")
                    print(f"Start Time: {start_time.isoformat()} (3 days ago)")
                    print(f"End Time: {end_time.isoformat()} (now)")
                    print(f"Data Source: {DataSource.REST.name}")
                    print(f"Records Retrieved: {len(df)}")
                    print(
                        f"Purpose: Testing retrieval of larger historical datasets (3 days of hourly data)"
                    )
                    print(
                        f"Testing Scenario: Validating ability to retrieve extended historical periods"
                    )
                    print(f"Date Range: {first_time} to {last_time}")
                    print("\nDataFrame:")
                    print(df)
                    print("=" * 80)

        except asyncio.TimeoutError:
            logger.error("Timeout retrieving historical data")
        except Exception as e:
            logger.error(f"Error retrieving historical data: {str(e)}")

    except Exception as e:
        logger.error(f"Error creating DataSourceManager: {str(e)}")
    finally:
        # Ensure proper cleanup
        if manager:
            try:
                await manager.__aexit__(None, None, None)
            except Exception as e:
                logger.error(f"Error during manager cleanup: {str(e)}")


async def verify_very_recent_hourly_data():
    """Verify retrieval of very recent hourly data that may not be fully consolidated."""
    logger.info("===== VERIFYING VERY RECENT HOURLY DATA RETRIEVAL =====")

    # Create manager
    manager = None
    try:
        manager = DataSourceManager(market_type=MarketType.SPOT, use_cache=False)

        # Define time range - use current time to ensure we hit consolidation warnings
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(
            hours=24
        )  # Use last 24 hours to ensure some data is returned

        logger.info(f"Recent hourly time range: {start_time} to {end_time}")

        try:
            start_op = time.time()

            # Use warning capture context manager
            with capture_warnings() as warnings_detected:
                # Retrieve hourly data with timeout protection
                df = await asyncio.wait_for(
                    manager.get_data(
                        symbol="BTCUSDT",
                        start_time=start_time,
                        end_time=end_time,
                        interval=Interval.HOUR_1,
                        enforce_source=DataSource.REST,
                    ),
                    timeout=30,  # 30-second timeout
                )

            elapsed = time.time() - start_op

            logger.info(f"Recent hourly data retrieval completed in {elapsed:.2f}s")
            logger.info(f"Retrieved {len(df)} hourly records")

            if len(df) > 0:
                # Check data coverage
                data_start = df.index.min()
                data_end = df.index.max()
                logger.info(f"Data spans from {data_start} to {data_end}")

                # Calculate expected records
                total_hours = (end_time - start_time).total_seconds() / 3600
                logger.info(
                    f"Expected ~{int(total_hours)} hourly records, got {len(df)} ({len(df)/total_hours*100:.1f}% complete)"
                )

                # Print the DataFrame
                print("\n===== RECENT HOURLY DATA RETRIEVAL =====")
                print(f"Symbol: BTCUSDT")
                print(f"Interval: {Interval.HOUR_1.value}")
                print(f"Start Time: {start_time.isoformat()} (24 hours ago)")
                print(f"End Time: {end_time.isoformat()} (now)")
                print(f"Data Source: {DataSource.REST.name}")
                print(f"Records Retrieved: {len(df)}")
                print(
                    f"Purpose: Testing retrieval of recent hourly data that may not be fully consolidated"
                )
                print(
                    f"Testing Scenario: Validating behavior with potentially incomplete recent data"
                )
                print(
                    f"Expected Records: ~{int(total_hours)}, Actual: {len(df)} ({len(df)/total_hours*100:.1f}% complete)"
                )
                print(f"Data Range: {data_start} to {data_end}")
                print(
                    f"Consolidation Warnings: {len([w for w in warnings_detected if 'may not be fully consolidated' in w])}"
                )
                print("\nDataFrame:")
                print(df)
                print("=" * 80)

            # Log any warnings that were captured
            consolidation_warnings = [
                w for w in warnings_detected if "may not be fully consolidated" in w
            ]
            if consolidation_warnings:
                logger.info(
                    f"Detected {len(consolidation_warnings)} data consolidation warnings during hourly retrieval"
                )
                for warning in consolidation_warnings[:2]:  # Show first few warnings
                    logger.info(f"Sample warning: {warning}")

        except asyncio.TimeoutError:
            logger.error("Timeout retrieving recent hourly data")
        except Exception as e:
            logger.error(f"Error retrieving recent hourly data: {str(e)}")

    except Exception as e:
        logger.error(f"Error creating DataSourceManager: {str(e)}")
    finally:
        # Ensure proper cleanup
        if manager:
            try:
                await manager.__aexit__(None, None, None)
            except Exception as e:
                logger.error(f"Error during manager cleanup: {str(e)}")


async def verify_partial_minute_data():
    """Verify retrieval of data that spans from available past data to potentially unavailable recent data."""
    logger.info("===== VERIFYING PARTIAL DATA RETRIEVAL (PAST TO RECENT) =====")

    # Create manager
    manager = None
    try:
        manager = DataSourceManager(market_type=MarketType.SPOT, use_cache=False)

        # Define time range that spans from certainly available data to potentially unavailable recent data
        current_time = datetime.now(timezone.utc)

        # Start time is 60 minutes ago (should be fully available)
        start_time = current_time - timedelta(minutes=60)
        # End time is current time (some very recent data may not be consolidated)
        end_time = current_time

        logger.info(f"Current time: {current_time.isoformat()}")
        logger.info(f"Partial data time range: {start_time} to {end_time}")
        logger.info(
            f"This range intentionally spans from available past data (60 min ago) "
            f"to very recent data that may not be fully consolidated yet"
        )

        try:
            start_op = time.time()

            # Use warning capture context manager
            with capture_warnings() as warnings_detected:
                # First case: Get data for the full range with timeout protection
                df = await asyncio.wait_for(
                    manager.get_data(
                        symbol="BTCUSDT",
                        start_time=start_time,
                        end_time=end_time,
                        interval=Interval.MINUTE_1,
                        enforce_source=DataSource.REST,
                    ),
                    timeout=30,  # 30-second timeout
                )

            elapsed = time.time() - start_op

            logger.info(f"Partial data retrieval completed in {elapsed:.2f}s")
            logger.info(f"Retrieved {len(df)} records")

            if len(df) > 0:
                # Check data coverage
                data_start = df.index.min()
                data_end = df.index.max()
                logger.info(f"Data spans from {data_start} to {data_end}")

                # Calculate expected records
                total_minutes = (end_time - start_time).total_seconds() / 60
                actual_expected = (
                    int(total_minutes) + 1
                )  # Add one for partial current minute

                # Calculate completion percentage
                completion_pct = (
                    (len(df) / actual_expected) * 100 if actual_expected > 0 else 0
                )

                logger.info(
                    f"Expected ~{actual_expected} minute records (up to current time), got {len(df)} ({completion_pct:.1f}% complete)"
                )

                missing_records = actual_expected - len(df)
                logger.info(
                    f"Missing {missing_records} records (likely the most recent ones)"
                )

                # Show the most recent records to see where data cuts off
                if len(df) >= 2:
                    logger.info(f"Last 2 records: {df.iloc[-2:].to_dict('records')}")

                # Print the full DataFrame
                print("\n===== PARTIAL DATA RETRIEVAL (FULL DATASET) =====")
                print(f"Symbol: BTCUSDT")
                print(f"Interval: {Interval.MINUTE_1.value}")
                print(f"Current Time: {current_time.isoformat()}")
                print(f"Start Time: {start_time.isoformat()} (60 minutes ago)")
                print(f"End Time: {end_time.isoformat()} (current time)")
                print(f"Data Source: {DataSource.REST.name}")
                print(f"Records Retrieved: {len(df)}")
                print(
                    f"Purpose: Testing data spanning from available past to potentially unavailable recent data"
                )
                print(
                    f"Testing Scenario: Validating data completeness across time boundaries"
                )
                print(
                    f"Expected Records: ~{actual_expected}, Actual: {len(df)} ({completion_pct:.1f}% complete)"
                )
                print(
                    f"Missing Records: {missing_records} (likely the most recent ones)"
                )
                print(f"Data Range: {data_start} to {data_end}")
                print(f"Warnings Detected: {len(warnings_detected)}")
                print("\nDataFrame:")
                print(df)
                print("=" * 80)

                # Second case: Demonstrate artificial partial retrieval by limiting data to first 45 records
                logger.info(
                    "\n=== DEMONSTRATING PARTIAL RETRIEVAL WITH SUBSET OF DATA ==="
                )
                if len(df) > 45:
                    partial_df = df.iloc[:45]
                    logger.info(
                        f"Created artificial partial dataset with {len(partial_df)}/{len(df)} records ({len(partial_df)/len(df)*100:.1f}%)"
                    )
                    logger.info(
                        f"Partial data spans from {partial_df.index.min()} to {partial_df.index.max()}"
                    )
                    logger.info(
                        f"Missing {len(df) - len(partial_df)} records from the end of the dataset"
                    )

                    # Show a sample of where the data cuts off
                    if len(partial_df) >= 2:
                        logger.info(
                            f"Last 2 records of partial dataset: {partial_df.iloc[-2:].to_dict('records')}"
                        )

                    # Calculate completeness metrics
                    partial_completion_pct = (
                        (len(partial_df) / actual_expected) * 100
                        if actual_expected > 0
                        else 0
                    )
                    logger.info(
                        f"Partial dataset has {len(partial_df)}/{actual_expected} expected records ({partial_completion_pct:.1f}% complete)"
                    )

                    # Print the partial DataFrame
                    print("\n===== PARTIAL DATA RETRIEVAL (ARTIFICIAL SUBSET) =====")
                    print(f"Symbol: BTCUSDT")
                    print(f"Interval: {Interval.MINUTE_1.value}")
                    print(f"Current Time: {current_time.isoformat()}")
                    print(f"Original Dataset Size: {len(df)} records")
                    print(
                        f"Artificial Subset Size: {len(partial_df)} records (first 45 records only)"
                    )
                    print(
                        f"Purpose: Demonstrating behavior with intentionally incomplete data"
                    )
                    print(f"Testing Scenario: Simulating partial data availability")
                    print(
                        f"Data Range: {partial_df.index.min()} to {partial_df.index.max()}"
                    )
                    print(
                        f"Completeness: {len(partial_df)}/{actual_expected} expected records ({partial_completion_pct:.1f}%)"
                    )
                    print(
                        f"Missing Records: {len(df) - len(partial_df)} from the end of the original dataset"
                    )
                    print("\nPartial DataFrame:")
                    print(partial_df)
                    print("=" * 80)

            else:
                logger.warning("No data retrieved at all")

            # Log any warnings that were captured
            if warnings_detected:
                logger.info(
                    f"Detected {len(warnings_detected)} data availability warnings"
                )
                for warning in warnings_detected[:3]:  # Show first few warnings
                    logger.info(f"Sample warning: {warning}")

        except asyncio.TimeoutError:
            logger.error("Timeout retrieving partial data")
        except Exception as e:
            logger.error(f"Error retrieving partial data: {str(e)}")

    except Exception as e:
        logger.error(f"Error creating DataSourceManager: {str(e)}")
    finally:
        # Ensure proper cleanup
        if manager:
            try:
                await manager.__aexit__(None, None, None)
            except Exception as e:
                logger.error(f"Error during manager cleanup: {str(e)}")


async def fetch_data_for_verification(manager, symbol, start_time, end_time, interval):
    """Fetch data for verification and return success indicator."""
    try:
        logger.info(f"Starting concurrent fetch for {symbol}")

        start_op = time.time()
        # Add timeout protection
        df = await asyncio.wait_for(
            manager.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                enforce_source=DataSource.REST,
            ),
            timeout=30,  # 30-second timeout
        )
        elapsed = time.time() - start_op

        if df.empty:
            logger.debug(f"No data retrieved for {symbol} in concurrent operation")
            return (0, symbol, None)  # Return None for DataFrame to avoid errors

        logger.info(
            f"Successfully retrieved {len(df)} records for {symbol} in {elapsed:.2f}s"
        )
        return (len(df), symbol, df)

    except asyncio.TimeoutError:
        logger.error(f"Timeout in concurrent fetch for {symbol}")
        raise
    except Exception as e:
        logger.error(f"Error in concurrent fetch for {symbol}: {str(e)}")
        raise


async def main():
    """Run all verification tests."""
    logger.info("===== STARTING DATA RETRIEVAL VERIFICATION =====")
    logger.info(f"Current time: {datetime.now(timezone.utc)}")

    # Ensure logs directory exists
    Path("logs/timeout_incidents").mkdir(parents=True, exist_ok=True)

    # Run verification tests with error handling
    try:
        await verify_concurrent_data_retrieval()
    except Exception as e:
        logger.error(f"Error during concurrent verification: {str(e)}")

    try:
        await verify_extended_historical_data()
    except Exception as e:
        logger.error(f"Error during historical data verification: {str(e)}")

    try:
        await verify_very_recent_hourly_data()
    except Exception as e:
        logger.error(f"Error during recent hourly verification: {str(e)}")

    try:
        await verify_partial_minute_data()
    except Exception as e:
        logger.error(f"Error during partial data verification: {str(e)}")

    logger.info("===== DATA RETRIEVAL VERIFICATION COMPLETE =====")


if __name__ == "__main__":
    # Run the verification
    asyncio.run(main())
