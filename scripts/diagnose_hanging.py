#!/usr/bin/env python
"""
Diagnostic script to identify where data retrieval hangs when retrieving 3599 records.
This script adds detailed logging and timeouts to track the exact point of hanging.
"""

import asyncio
import signal
import sys
import time
import traceback
from functools import partial
import gc
import tracemalloc
from datetime import datetime, timedelta, timezone
import os
import shutil
from pathlib import Path

from utils.logger_setup import logger
from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import MarketType, Interval
from scripts.fix_hanging_client import (
    emergency_cleanup,
    fix_client_reference,
    find_force_timeout_tasks,
    find_curl_references_in_memory,
)


# Timestamp utility function since it's not directly imported from time_utils
def get_timestamp_ms(dt: datetime) -> int:
    """Convert datetime to millisecond timestamp.

    Args:
        dt: Input datetime

    Returns:
        int: Millisecond timestamp
    """
    return int(dt.timestamp() * 1000)


# Enable trace malloc for memory leak detection
tracemalloc.start()

# Global timeout for the entire operation
GLOBAL_TIMEOUT_SECONDS = 300  # 5 minutes

# Timeout for data retrieval only
DATA_RETRIEVAL_TIMEOUT_SECONDS = 240  # 4 minutes

# Timeout for cleanup operations
CLEANUP_TIMEOUT_SECONDS = 10  # 10 seconds

# Default cache directory location
DEFAULT_CACHE_DIR = "./cache"

# Flag to enable task monitoring
ENABLE_TASK_MONITORING = True


def clear_cache_directory(cache_dir=DEFAULT_CACHE_DIR):
    """Clear the cache directory to ensure a clean test environment.

    Args:
        cache_dir: Path to the cache directory
    """
    cache_path = Path(cache_dir)

    if cache_path.exists():
        logger.info(f"Clearing cache directory: {cache_path}")
        try:
            shutil.rmtree(cache_path)
            logger.info("Cache directory successfully cleared")
        except Exception as e:
            logger.error(f"Error clearing cache directory: {e}")

    # Recreate the empty directory
    cache_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created empty cache directory at {cache_path}")


class DiagnosticTimer:
    """Simple context manager to measure and log execution time of code blocks."""

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.start_time = time.time()
        logger.debug(f"Starting {self.name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = time.time() - self.start_time
        logger.debug(f"Completed {self.name} in {self.elapsed:.3f}s")


async def proactive_cleanup():
    """
    Proactively clean up any force_timeout tasks that might cause hanging.
    """
    logger.info("Running proactive cleanup before data retrieval")

    # Find and cancel any force_timeout tasks
    force_timeout_tasks = await find_force_timeout_tasks()
    if force_timeout_tasks:
        logger.warning(
            f"Proactively cancelling {len(force_timeout_tasks)} force_timeout tasks"
        )
        for task in force_timeout_tasks:
            task.cancel()

        # Wait for cancellation to complete
        try:
            await asyncio.wait_for(
                asyncio.gather(*force_timeout_tasks, return_exceptions=True),
                timeout=1.0,
            )
            logger.info(
                f"Successfully cancelled {len(force_timeout_tasks)} force_timeout tasks"
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Timeout waiting for force_timeout tasks to cancel, proceeding anyway"
            )
    else:
        logger.info("No force_timeout tasks found to clean up")

    # Also proactively find and fix curl references in memory
    fixed_refs = await find_curl_references_in_memory()
    if fixed_refs > 0:
        logger.info(f"Proactively fixed {fixed_refs} curl references in memory")

    # Force garbage collection
    gc.collect()


async def monitor_tasks():
    """Monitor running tasks to identify potential hanging issues."""
    if not ENABLE_TASK_MONITORING:
        return

    logger.info("Starting task monitoring")
    check_interval = 5  # Check every 5 seconds

    # Keep a record of tasks we've already seen
    known_tasks = set()

    while True:
        try:
            # Get all tasks
            all_tasks = asyncio.all_tasks()
            current_tasks = set(all_tasks)

            # Identify new tasks
            new_tasks = current_tasks - known_tasks
            completed_tasks = known_tasks - current_tasks

            if new_tasks:
                logger.debug(f"Detected {len(new_tasks)} new tasks:")
                for i, task in enumerate(new_tasks, 1):
                    task_str = str(task)
                    # Only show a short preview of the task to avoid log spam
                    preview = (
                        task_str[:100] + "..." if len(task_str) > 100 else task_str
                    )
                    logger.debug(f"  New task {i}: {preview}")

            if completed_tasks:
                logger.debug(f"{len(completed_tasks)} tasks completed since last check")

            # Check for hanging curl_cffi tasks
            curl_tasks = [
                t for t in all_tasks if "curl_cffi" in str(t) and not t.done()
            ]
            if curl_tasks:
                logger.warning(
                    f"Found {len(curl_tasks)} active curl_cffi tasks that might hang"
                )
                # Look specifically for force_timeout tasks
                force_timeout_tasks = [
                    t for t in curl_tasks if "_force_timeout" in str(t)
                ]
                if force_timeout_tasks:
                    logger.warning(
                        f"Found {len(force_timeout_tasks)} force_timeout tasks that might cause hanging"
                    )

                # Proactively apply fixes to prevent hanging
                if (
                    len(curl_tasks) > 5 or force_timeout_tasks
                ):  # Only fix if there are force_timeout tasks or too many curl tasks
                    logger.warning("Proactively fixing curl_cffi tasks")
                    for task in force_timeout_tasks:
                        task.cancel()

            # Update known tasks
            known_tasks = current_tasks

            # Sleep for the check interval
            await asyncio.sleep(check_interval)
        except asyncio.CancelledError:
            logger.info("Task monitoring cancelled")
            break
        except Exception as e:
            logger.error(f"Error in task monitoring: {e}")
            await asyncio.sleep(check_interval)


async def run_with_timeout(coro, timeout, description):
    """Run a coroutine with timeout and detailed logging."""
    try:
        with DiagnosticTimer(description):
            return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"TIMEOUT: {description} timed out after {timeout}s")

        # Try emergency cleanup when a timeout occurs
        logger.warning("Running emergency cleanup to fix potential hanging clients")
        await emergency_cleanup()

        raise


async def diagnose_data_retrieval():
    """Run data retrieval with enhanced logging to diagnose hanging."""
    logger.info("Starting diagnostic data retrieval test")

    # Start task monitoring in background
    if ENABLE_TASK_MONITORING:
        monitoring_task = asyncio.create_task(monitor_tasks())

    # Clear the cache directory first to ensure a clean test
    clear_cache_directory()

    # Track objects before test
    gc.collect()
    object_count_before = len(gc.get_objects())
    logger.debug(f"Object count before test: {object_count_before}")

    # Take memory snapshot
    snapshot1 = tracemalloc.take_snapshot()

    # Run proactive cleanup before starting
    await proactive_cleanup()

    data_manager = None
    data = None
    try:
        # Initialize DataSourceManager with debug logging
        with DiagnosticTimer("DataSourceManager initialization"):
            # Initialize with cache directory and ensure caching is enabled
            cache_dir = Path(DEFAULT_CACHE_DIR)
            data_manager = DataSourceManager(
                market_type=MarketType.SPOT,
                cache_dir=cache_dir,
                use_cache=True,  # We want to test with caching enabled
                max_concurrent=50,
                retry_count=3,
            )
            logger.debug(f"Created DataSourceManager: {data_manager}")

        # Define parameters for data retrieval
        symbol = "BTCUSDT"

        # Get the current time and calculate start/end times
        current_time = datetime.now(timezone.utc)
        end_time = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)

        # Convert to timestamps
        start_ts = get_timestamp_ms(start_time)
        end_ts = get_timestamp_ms(end_time)

        logger.info(f"Data retrieval parameters:")
        logger.info(f"  Symbol: {symbol}")
        logger.info(f"  Interval: {Interval.MINUTE_1.value}")
        logger.info(f"  Start Time: {start_time} ({start_ts})")
        logger.info(f"  End Time: {end_time} ({end_ts})")

        # Pre-emptively fix data_manager to avoid circular references
        logger.info("Pre-emptively fixing any references in data_manager")
        fix_client_reference(data_manager, max_depth=3)

        # First, retrieve the data with a separate timeout
        try:
            async with data_manager:
                # Check active tasks before retrieval
                logger.debug(
                    f"Active tasks before retrieval: {len(asyncio.all_tasks())}"
                )

                # Proactive cleanup right before data retrieval
                await proactive_cleanup()

                # Get data with a timeout
                with DiagnosticTimer("Data Retrieval Operation"):
                    data = await run_with_timeout(
                        data_manager.get_data(
                            symbol=symbol,
                            start_time=start_time,
                            end_time=end_time,
                            interval=Interval.MINUTE_1,
                            enforce_source=DataSource.REST,  # Force REST API
                        ),
                        timeout=DATA_RETRIEVAL_TIMEOUT_SECONDS,
                        description="Data retrieval",
                    )

                    logger.info(f"Successfully retrieved {len(data)} records")

                    # Check active tasks after successful retrieval
                    curl_tasks = [
                        t
                        for t in asyncio.all_tasks()
                        if "curl_cffi" in str(t) and not t.done()
                    ]
                    logger.debug(
                        f"Active curl_cffi tasks after retrieval: {len(curl_tasks)}"
                    )
                    if curl_tasks:
                        logger.warning("Proactively cleaning up active curl_cffi tasks")
                        await emergency_cleanup()
        except asyncio.TimeoutError:
            logger.error("Data retrieval operation timed out")
            raise
        except Exception as e:
            logger.error(f"Error during data retrieval: {e}")
            raise

        # Now if we have data, process it separately
        if data is not None:
            logger.info("Processing the retrieved data")

            # Check if we're at the 3599 mark where hanging occurs
            if len(data) == 3599:
                logger.warning(
                    "Retrieved exactly 3599 records - this is where hanging typically occurs"
                )

            try:
                # Data processing with a separate timeout (should be quick)
                with DiagnosticTimer("Data Processing"):
                    # Sample data
                    sample = data[:5] if len(data) > 5 else data
                    logger.info(f"Sample data: {sample}")

                    # Check memory usage
                    memory_usage = data.memory_usage(deep=True).sum() / (1024 * 1024)
                    logger.info(f"DataFrame memory usage: {memory_usage:.2f} MB")

                    # Force GC before processing
                    gc.collect()

                    # Simple processing test
                    if "volume" in data.columns:
                        with DiagnosticTimer("Volume calculation"):
                            hourly_volume = data.resample("1H").agg({"volume": "sum"})
                            logger.info(
                                f"Hourly volume calculation completed: {len(hourly_volume)} periods"
                            )
            except Exception as e:
                logger.error(f"Error during data processing: {e}")
                # Continue with cleanup even if processing fails
    except asyncio.TimeoutError:
        logger.error(
            f"Data retrieval operation timed out after {DATA_RETRIEVAL_TIMEOUT_SECONDS}s"
        )

        # If timeout occurred, try emergency cleanup
        if data_manager is not None:
            logger.warning("Trying emergency fix for data_manager and its clients")
            # Try to fix any _curlm references first
            fixed = fix_client_reference(data_manager, max_depth=3)
            logger.info(f"Fixed {fixed} references in data_manager")
    except Exception as e:
        logger.error(f"Error during diagnostic test: {e}", exc_info=True)
    finally:
        # Cancel task monitoring
        if ENABLE_TASK_MONITORING:
            monitoring_task.cancel()
            try:
                await monitoring_task
            except asyncio.CancelledError:
                pass

        # Manual cleanup if context manager failed
        if data_manager is not None:
            try:
                logger.debug("Performing manual cleanup of data_manager")
                with DiagnosticTimer("Manual cleanup"):
                    if hasattr(data_manager, "__aexit__"):
                        await run_with_timeout(
                            data_manager.__aexit__(None, None, None),
                            timeout=CLEANUP_TIMEOUT_SECONDS,
                            description="Manual DataSourceManager.__aexit__",
                        )
            except Exception as e:
                logger.error(f"Error during manual cleanup: {e}")
                # Try emergency cleanup as a last resort
                await emergency_cleanup()

        # Force garbage collection
        with DiagnosticTimer("Final garbage collection"):
            gc.collect()
            object_count_after = len(gc.get_objects())
            logger.debug(f"Object count after test: {object_count_after}")
            logger.debug(
                f"Object difference: {object_count_after - object_count_before}"
            )

        # Take final memory snapshot and compare
        snapshot2 = tracemalloc.take_snapshot()
        top_stats = snapshot2.compare_to(snapshot1, "lineno")
        logger.debug("Top 10 memory differences:")
        for stat in top_stats[:10]:
            logger.debug(str(stat))

    logger.info("Diagnostic test completed")


def handle_timeout(signum, frame):
    """Handle timeout signal."""
    logger.critical(
        f"GLOBAL TIMEOUT: Script execution exceeded timeout. Current stack trace:"
    )
    for line in traceback.format_stack():
        logger.critical(line.strip())
    sys.exit(1)


async def main():
    """Main entry point with global timeout protection."""
    # Set a global timeout for the entire script
    signal.signal(signal.SIGALRM, handle_timeout)
    signal.alarm(
        GLOBAL_TIMEOUT_SECONDS + 30
    )  # 30s grace period on top of internal timeout

    try:
        await diagnose_data_retrieval()
    except Exception as e:
        logger.error(f"Uncaught exception in main: {e}", exc_info=True)
    finally:
        # Cancel the alarm
        signal.alarm(0)


if __name__ == "__main__":
    # Check if we're using the event loop policy that supports the cleanup properly
    loop_policy = asyncio.get_event_loop_policy()
    logger.info(f"Using event loop policy: {type(loop_policy).__name__}")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Diagnostic script interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        traceback.print_exc()
