#!/usr/bin/env python3
"""
Task Cancellation Demo for EventBasedFetcher focusing on best practices.

This script demonstrates MDC-compliant task cancellation patterns:
1. Event-based cancellation (Tier 1 - critical)
2. Task tracking and proper cleanup (Tier 1 - critical)
3. Concurrent cancellation propagation (Tier 1 - critical)

This is a standalone implementation that doesn't rely on external fetcher classes.
"""

from utils.logger_setup import logger
from rich import print
import asyncio
import time
import gc
import sys
import importlib
import shutil
import os
import argparse
from datetime import datetime, timezone, timedelta
import pandas as pd

from utils.market_constraints import Interval  # Import for Interval
from utils.async_cleanup import cancel_and_wait  # Import for better cancellation
from utils.config import (
    FeatureFlags,
    DEMO_SIMULATED_DELAY,
    TASK_CANCEL_WAIT_TIMEOUT,
    LINGERING_TASK_CLEANUP_TIMEOUT,
    AGGRESSIVE_TASK_CLEANUP_TIMEOUT,
)

# Import task management utilities instead of duplicating code
from utils.task_management import (
    wait_with_cancellation,
    cleanup_lingering_tasks,
    propagate_cancellation,
    TaskTracker,
)

# Configure logger
logger.setup_root(level="INFO", show_filename=True)

# Enable caching globally
FeatureFlags.update(ENABLE_CACHE=True)

# Define a constant for MAX_SINGLE_OPERATION_TIMEOUT
MAX_SINGLE_OPERATION_TIMEOUT = 15  # 15 seconds for single operations


def enable_debug_logging():
    """Enable detailed DEBUG level logging for diagnostics"""
    logger.setup_root(level="DEBUG", show_filename=True)
    logger.debug("DEBUG logging enabled for detailed task diagnostics")


def clear_caches():
    """Clear all caches to ensure a clean start for the demonstration"""
    logger.info("Clearing caches to ensure clean slate for demonstration")

    # Delete cache directory if it exists
    cache_dir = os.path.join(os.getcwd(), "cache")
    if os.path.exists(cache_dir):
        logger.info(f"Removing cache directory: {cache_dir}")
        try:
            shutil.rmtree(cache_dir)
            logger.info("Cache directory successfully removed")
        except Exception as e:
            logger.error(f"Error removing cache directory: {str(e)}")

    # Create fresh cache directory
    try:
        os.makedirs(cache_dir, exist_ok=True)
        logger.info(f"Created fresh cache directory: {cache_dir}")
    except Exception as e:
        logger.error(f"Error creating cache directory: {str(e)}")

    # Force garbage collection
    gc.collect()
    logger.info("Caches cleared")


class BaseEventFetcher:
    """
    Base class for fetchers with event-based completion signaling.
    This is a standalone implementation for demonstration purposes.
    """

    def __init__(
        self,
        symbol,
        interval,
        days_back=1,
        use_cache=True,
        fallback_timeout=MAX_SINGLE_OPERATION_TIMEOUT,
    ):
        """
        Initialize the event-based fetcher.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            interval: Time interval for the data
            days_back: Number of days to go back from current time
            use_cache: Whether to use the caching system
            fallback_timeout: Fallback timeout in seconds (only used if events fail)
        """
        self.symbol = symbol
        self.interval = interval
        self.days_back = days_back
        self.use_cache = use_cache
        self.fallback_timeout = fallback_timeout

        # Calculate time range
        self.end_time = datetime.now(timezone.utc)
        self.start_time = self.end_time - timedelta(days=days_back)

        # Event to signal completion
        self.completion_event = asyncio.Event()

        # Container for results and errors
        self.result = None
        self.error = None

        # Progress tracking
        self.progress = {
            "stage": "initializing",
            "chunks_total": 0,
            "chunks_completed": 0,
            "records": 0,
        }

        # Safety fallback timeout - still have this as a fallback
        self.safety_timeout = fallback_timeout

        # Keep track of tasks for proper cleanup
        self.tasks = set()

    async def _fetch_impl(self):
        """
        Base implementation of the fetch operation.
        In this base class, this just simulates a successful fetch operation.
        """
        self.progress["stage"] = "simulated_fetch"

        try:
            logger.debug(f"Starting simulated fetch for {self.symbol}")
            # Simulate some work
            await asyncio.sleep(0.5)

            # Create a simple DataFrame as a result
            dates = pd.date_range(self.start_time, self.end_time, periods=10)
            df = pd.DataFrame(
                {
                    "open": [100.0] * 10,
                    "high": [105.0] * 10,
                    "low": [95.0] * 10,
                    "close": [102.0] * 10,
                    "volume": [1000.0] * 10,
                },
                index=dates,
            )

            self.progress["stage"] = "completed"
            self.progress["records"] = len(df)

            return df

        except asyncio.CancelledError:
            self.progress["stage"] = "cancelled"
            logger.warning(f"Simulated fetch for {self.symbol} was cancelled")
            raise

        except Exception as e:
            self.progress["stage"] = "error"
            logger.error(f"Error in simulated fetch for {self.symbol}: {str(e)}")
            self.error = e
            return None

    async def fetch(self):
        """
        Execute the fetch operation with completion tracking.

        Returns:
            DataFrame with simulated data
        """
        logger.info(f"Fetching data for {self.symbol}")

        # Start the fetch task
        fetch_task = asyncio.create_task(self._fetch_impl())
        self.tasks.add(fetch_task)
        fetch_task.add_done_callback(lambda t: self.tasks.discard(t))

        try:
            # Use wait_with_cancellation instead of asyncio.wait
            success = await wait_with_cancellation(
                fetch_task,
                completion_event=self.completion_event,
                timeout=self.fallback_timeout,
            )

            if not success:
                if not fetch_task.done():
                    logger.warning(f"Cancelling fetch task for {self.symbol}")
                    await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)
                return None

            # Get result if task completed successfully
            if fetch_task.done() and not fetch_task.cancelled():
                self.result = fetch_task.result()
                return self.result

            return None

        except asyncio.CancelledError:
            logger.warning(f"Fetch operation for {self.symbol} was cancelled")
            # Re-raise to ensure proper cancellation
            raise

        finally:
            # Ensure completion event is set
            if not self.completion_event.is_set():
                self.completion_event.set()


class DelayedEventBasedFetcher(BaseEventFetcher):
    """
    A subclass of BaseEventFetcher that introduces an artificial delay in fetching,
    and checks for cancellation requests during the delay.
    """

    def __init__(self, symbol, interval, days_back=1, fallback_timeout=None):
        # Ensure we have a default timeout value
        if fallback_timeout is None:
            fallback_timeout = MAX_SINGLE_OPERATION_TIMEOUT

        # Init parent class with explicit cache enabled
        super().__init__(
            symbol,
            interval,
            days_back,
            use_cache=True,
            fallback_timeout=fallback_timeout,
        )

        # Add cancel event for event-based cancellation (MDC Tier 1 practice)
        self.cancel_event = asyncio.Event()

        # Initialize task tracker (MDC Tier 1 practice)
        self.task_tracker = TaskTracker()

        # Improve progress tracking (MDC Tier 2 practice)
        self.progress = {
            "stage": "initialized",
            "delay_progress": "0%",
            "completed": False,
            "cancellation_source": None,
        }

    async def _fetch_impl(self):
        """
        Override the fetch implementation to add delays and cancellation checks.
        """
        self.progress["stage"] = "delayed_fetch_started"

        try:
            # Log that we're starting the delayed operation
            logger.info(
                f"Starting delayed fetch for {self.symbol} with {DEMO_SIMULATED_DELAY}s delay"
            )
            print(
                f"🕒 Fetching {self.symbol} with artificial {DEMO_SIMULATED_DELAY}s delay..."
            )

            # Split the delay into small chunks to check for cancellation
            for i in range(DEMO_SIMULATED_DELAY * 2):
                # Check for cancellation using multiple mechanisms (MDC Tier 1 practice)
                if self.cancel_event.is_set() or asyncio.current_task().cancelled():
                    # Record cancellation source for better logging (MDC Tier 2)
                    if self.cancel_event.is_set():
                        self.progress["cancellation_source"] = "cancel_event"
                    else:
                        self.progress["cancellation_source"] = "task_cancelled"

                    logger.warning(
                        f"Cancellation detected during delay for {self.symbol} (source: {self.progress['cancellation_source']})"
                    )
                    print(f"⚠️ Cancellation detected during delay for {self.symbol}")

                    # Raise cancellation error to simulate cancellation
                    raise asyncio.CancelledError("Cancellation during delay")

                # Update progress (MDC Tier 2 practice)
                self.progress["delay_progress"] = (
                    f"{(i+1)/(DEMO_SIMULATED_DELAY*2)*100:.0f}%"
                )

                # Cancellation checkpoints (MDC Tier 2 practice)
                await asyncio.sleep(0.5)  # Sleep for a small chunk of time
                await asyncio.sleep(0)  # Yield control to allow cancellation to occur

            # After delay, proceed with normal fetch
            self.progress["stage"] = "delay_complete_proceeding_with_fetch"
            logger.info(f"Delay complete for {self.symbol}, proceeding with fetch")

            # Call the parent implementation to do the actual fetch
            return await super()._fetch_impl()

        except asyncio.CancelledError:
            # Handle cancellation during the delay with improved logging (MDC Tier 2)
            self.progress["stage"] = "cancelled_during_delay"

            # Enhanced logging with context (MDC Tier 2 practice)
            logger.warning(
                f"Fetch operation for {self.symbol} was cancelled during delay "
                f"at progress {self.progress['delay_progress']} (source: {self.progress.get('cancellation_source', 'unknown')})"
            )
            print(f"✗ Fetch cancelled during delay for {self.symbol}")

            # Re-raise to ensure proper cancellation (MDC Tier 1 practice)
            raise

    async def fetch(self):
        """
        Override fetch to add cancellation monitoring.
        """
        # Record task start time for monitoring
        start_time = time.time()

        try:
            # Start the actual fetch operation
            fetch_task = asyncio.create_task(super().fetch())

            # Track task (MDC Tier 1 practice)
            self.task_tracker.add(fetch_task)

            # Use event-based waiting instead of timeouts (MDC Tier 1)
            success = await wait_with_cancellation(
                fetch_task,
                completion_event=self.completion_event,
                cancel_event=self.cancel_event,
                timeout=self.fallback_timeout,  # Only as fallback
            )

            if not success and not fetch_task.done():
                logger.warning(
                    f"Cancelling fetch task for {self.symbol} after wait_with_cancellation returned False"
                )
                await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)

            # Get result if task completed successfully
            if fetch_task.done() and not fetch_task.cancelled():
                return fetch_task.result()
            return None

        except asyncio.CancelledError:
            # Enhanced logging with context (MDC Tier 2 practice)
            logger.warning(
                f"Fetch operation for {self.symbol} was cancelled during {self.progress['stage']}"
            )
            # Re-raise to ensure proper cancellation (MDC Tier 1 practice)
            raise
        finally:
            # Ensure completion event is set
            if not self.completion_event.is_set():
                self.completion_event.set()


class EventControlledFetcher(DelayedEventBasedFetcher):
    """
    A fetcher that relies on events for control flow rather than timeouts.
    This demonstrates how to implement cancellation using pure event-based mechanisms.
    """

    def __init__(self, symbol, interval, days_back=1):
        # Initialize with a very long timeout to effectively disable timeout-based cancellation
        super().__init__(symbol, interval, days_back)

        # We already have cancel_event from parent class, just add pause/resume
        self.pause_event = asyncio.Event()
        self.resume_event = asyncio.Event()

        # Track additional state in progress dict (MDC Tier 2)
        self.progress.update(
            {"paused": False, "pause_count": 0, "last_state_change": time.time()}
        )

        # Set resume event initially to allow execution
        self.resume_event.set()

    async def fetch(self):
        """
        Override fetch method to incorporate pure event-based control.
        This implementation follows MDC Tier 1 practices.
        """
        # Start the actual fetch operation as a task
        self.progress["stage"] = "fetch_started"

        # Create and track task (MDC Tier 1)
        fetch_task = asyncio.create_task(super().fetch())
        self.task_tracker.add(fetch_task)

        try:
            # Control loop that monitors events and manages the task
            while not fetch_task.done():
                # Check for cancellation request (MDC Tier 1)
                if self.cancel_event.is_set() or asyncio.current_task().cancelled():
                    logger.info(
                        f"Cancel event detected for {self.symbol} during {self.progress['stage']}"
                    )
                    self.progress["cancellation_source"] = "event_controlled_cancel"
                    await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)
                    break

                # Check for pause request (MDC Tier 3 - least important)
                if self.pause_event.is_set() and not self.resume_event.is_set():
                    if not self.progress["paused"]:
                        self.progress["paused"] = True
                        self.progress["pause_count"] += 1
                        self.progress["last_state_change"] = time.time()
                        logger.info(
                            f"Fetch operation for {self.symbol} is paused during {self.progress['stage']}"
                        )
                    # Wait for resume signal
                    await self.resume_event.wait()
                    self.progress["paused"] = False
                    self.progress["last_state_change"] = time.time()
                    logger.info(f"Resuming fetch operation for {self.symbol}")

                # Yield control briefly (MDC Tier 2 - cancellation checkpoints)
                await asyncio.sleep(0.1)

            # Get result if task completed successfully
            if fetch_task.done() and not fetch_task.cancelled():
                result = fetch_task.result()
                self.progress["stage"] = "completed_successfully"
                self.progress["completed"] = True
                return result

            # Task was cancelled
            self.progress["stage"] = "cancelled"
            return None

        except asyncio.CancelledError:
            # Enhanced logging with task context (MDC Tier 2)
            self.progress["stage"] = "cancelled_via_exception"
            logger.warning(
                f"Fetch operation for {self.symbol} was cancelled through event "
                f"(pause count: {self.progress['pause_count']})"
            )
            # Re-raise to ensure proper cancellation (MDC Tier 1)
            raise
        finally:
            # Cleanup tasks (MDC Tier 1)
            if fetch_task and not fetch_task.done():
                await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)

            # Ensure completion event is set (MDC Tier 1)
            if not self.completion_event.is_set():
                self.completion_event.set()


class DelayedConcurrentFetcher:
    """A simple concurrent fetcher implementation that uses our delayed fetcher"""

    def __init__(self):
        self.fetchers = []
        self.task_tracker = TaskTracker()  # Use TaskTracker instead of raw set
        self.all_complete_event = asyncio.Event()
        self.cancel_event = asyncio.Event()  # Add global cancel event (MDC Tier 1)

        # Tracking progress for all fetchers (MDC Tier 2)
        self.progress = {
            "stage": "initialized",
            "total_requests": 0,
            "completed": 0,
            "cancelled": 0,
            "failed": 0,
            "cancellation_source": None,
        }

    async def fetch_multiple(self, requests):
        """Fetch data for multiple symbols concurrently using delayed fetchers"""
        results = {}
        fetch_tasks = []
        self.progress["total_requests"] = len(requests)
        self.progress["stage"] = "starting_fetchers"

        try:
            # Create a fetcher for each request
            for req in requests:
                symbol = req["symbol"]
                interval = req["interval"]

                # Create a delayed fetcher
                fetcher = DelayedEventBasedFetcher(
                    symbol=symbol, interval=interval, days_back=1
                )
                self.fetchers.append(fetcher)

                # Link cancellation events (MDC Tier 1 practice - propagate cancellation)
                # When self.cancel_event is set, it will trigger cancellation in each fetcher
                # Use the propagate_cancellation utility for this
                propagation_task = asyncio.create_task(
                    propagate_cancellation(self.cancel_event, [fetcher.cancel_event])
                )
                self.task_tracker.add(propagation_task)

                # Create task for this fetcher
                task = asyncio.create_task(fetcher.fetch())
                fetch_tasks.append((symbol, task))

                # Add to task tracker (MDC Tier 1 practice)
                self.task_tracker.add(task)

            # Update progress
            self.progress["stage"] = "fetchers_started"

            # Print start message
            print(f"Starting {len(fetch_tasks)} concurrent fetch operations...")

            # Wait using event-based mechanism (MDC Tier 1 practice)
            completion_tasks = [task for _, task in fetch_tasks]
            while completion_tasks and not self.cancel_event.is_set():
                # Use asyncio.wait with a short timeout to check for completion
                done, pending = await asyncio.wait(
                    completion_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=0.5,  # Short timeout for responsive cancellation
                )

                # Update our completion task list
                completion_tasks = list(pending)

                # Check for cancellation
                if asyncio.current_task().cancelled():
                    logger.info("Concurrent fetcher parent task was cancelled")
                    self.progress["cancellation_source"] = "parent_cancelled"
                    self.cancel_event.set()
                    break

                # Process newly completed tasks
                for symbol, task in fetch_tasks:
                    if task in done and task not in pending:
                        try:
                            results[symbol] = task.result()
                            print(f"✓ {symbol}: Fetch completed")
                            self.progress["completed"] += 1
                        except asyncio.CancelledError:
                            print(f"✗ {symbol}: Fetch was cancelled")
                            results[symbol] = None
                            self.progress["cancelled"] += 1
                        except Exception as e:
                            print(f"✗ {symbol}: Error - {str(e)}")
                            results[symbol] = None
                            self.progress["failed"] += 1

                # Yield to allow cancellation
                await asyncio.sleep(0)

            # Process any unprocessed tasks
            for symbol, task in fetch_tasks:
                if symbol not in results:
                    if task.done():
                        try:
                            results[symbol] = task.result()
                            print(f"✓ {symbol}: Fetch completed")
                            self.progress["completed"] += 1
                        except asyncio.CancelledError:
                            print(f"✗ {symbol}: Fetch was cancelled")
                            results[symbol] = None
                            self.progress["cancelled"] += 1
                        except Exception as e:
                            print(f"✗ {symbol}: Error - {str(e)}")
                            results[symbol] = None
                            self.progress["failed"] += 1
                    else:
                        print(f"⏳ {symbol}: Task still pending")

            self.progress["stage"] = "fetch_operations_complete"
            return results

        except asyncio.CancelledError:
            # Handle cancellation with context (MDC Tier 2 practice)
            self.progress["stage"] = "cancelled_during_fetch_multiple"
            self.progress["cancellation_source"] = "cancelled_error_exception"

            # Set the cancel event to propagate cancellation
            self.cancel_event.set()

            # Enhanced logging (MDC Tier 2)
            pending_count = sum(1 for _, task in fetch_tasks if not task.done())
            logger.warning(
                f"Concurrent fetch operation was cancelled with {pending_count} pending tasks"
            )
            print(
                f"Concurrent fetch operation was cancelled ({pending_count} tasks pending)"
            )

            # Cancel all fetchers through their events (MDC Tier 1)
            for fetcher in self.fetchers:
                if not fetcher.cancel_event.is_set():
                    fetcher.cancel_event.set()

                if (
                    hasattr(fetcher, "completion_event")
                    and not fetcher.completion_event.is_set()
                ):
                    fetcher.completion_event.set()

            # Cancel all pending tasks using task_tracker (MDC Tier 1)
            await self.task_tracker.cancel_all(timeout=LINGERING_TASK_CLEANUP_TIMEOUT)

            # Raise to properly handle cancellation (MDC Tier 1)
            raise

        finally:
            # Ensure cleanup (MDC Tier 1)
            self.all_complete_event.set()

            # Set cancellation event to make sure propagation tasks complete
            self.cancel_event.set()

            # Clean up any lingering tasks using task_tracker (MDC Tier 1)
            await self.task_tracker.cancel_all(timeout=LINGERING_TASK_CLEANUP_TIMEOUT)

            # Final stats for logging
            self.progress["stage"] = "cleanup_complete"
            logger.info(
                f"Concurrent fetch stats: {self.progress['completed']} completed, "
                f"{self.progress['cancelled']} cancelled, {self.progress['failed']} failed"
            )


async def demonstrate_event_based_cancellation():
    """
    Demonstrates pure event-based cancellation without relying on timeouts.
    This demonstrates a more robust approach that can replace timeout-based mechanisms.
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATING EVENT-BASED CANCELLATION (MDC TIER 1)")
    print("=" * 70)
    print(
        "This demonstrates how to control task execution using events rather than timeouts."
    )
    print(
        "The fetcher is controlled entirely through events for pause, resume, and cancel."
    )

    # Clear caches before this demonstration (clean slate)
    clear_caches()

    # Track tasks for leak detection (MDC Tier 2)
    tasks_before = len(asyncio.all_tasks())
    logger.info(f"Event-based cancellation demo starting with {tasks_before} tasks")

    symbol = "DOGEUSDT"
    interval = Interval.HOUR_1

    # Create event-controlled fetcher
    fetcher = EventControlledFetcher(symbol, interval, days_back=1)

    # Start the fetch operation
    print(f"Starting event-controlled fetch for {symbol}...")
    fetch_task = asyncio.create_task(fetcher.fetch())
    logger.debug(f"Created event-controlled fetch task {id(fetch_task)} for {symbol}")

    # Track task using the TaskTracker from the fetcher
    fetcher.task_tracker.add(fetch_task)

    try:
        # Let it run for 1 second
        await asyncio.sleep(1)
        logger.info(
            f"Event-based fetch running, current stage: {fetcher.progress['stage']}"
        )

        # Demonstrate pausing the operation
        print(f"🔶 Pausing the fetch operation...")
        fetcher.pause_event.set()
        fetcher.resume_event.clear()

        # Wait while paused
        await asyncio.sleep(1)
        print(f"Operation is paused. Current stage: {fetcher.progress['stage']}")
        logger.info(
            f"Fetch paused for {symbol}, paused: {fetcher.progress['paused']}, count: {fetcher.progress['pause_count']}"
        )

        # Resume the operation
        print(f"▶️ Resuming the fetch operation...")
        fetcher.resume_event.set()

        # Let it run a bit more
        await asyncio.sleep(1)
        logger.info(
            f"Fetch resumed for {symbol}, current stage: {fetcher.progress['stage']}"
        )

        # Cancel through the event mechanism (MDC Tier 1 practice)
        print(f"🛑 Cancelling through event mechanism...")
        logger.debug(f"Setting cancel event for event-controlled fetcher {symbol}")
        fetcher.cancel_event.set()

        # Wait for completion using event-based approach (MDC Tier 1)
        completion_future = asyncio.create_task(
            wait_with_cancellation(
                fetch_task,
                completion_event=fetcher.completion_event,
                timeout=5.0,  # Fallback timeout
            )
        )

        # Wait for completion
        if await completion_future:
            if fetch_task.done() and not fetch_task.cancelled():
                result = fetch_task.result()
                print(
                    f"Fetch completed successfully with {len(result) if result is not None else 0} records"
                )
        else:
            print(f"Fetch was cancelled through event mechanism as expected")
            # Ensure the task is cancelled
            if not fetch_task.done():
                logger.debug(
                    f"Cancelling fetch task {id(fetch_task)} after wait completion"
                )
                await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)

    except asyncio.CancelledError:
        # Enhanced logging with task context (MDC Tier 2)
        logger.warning(
            f"Event-based cancellation demo was cancelled during {fetcher.progress['stage']}"
        )
        print(f"✓ Fetch was cancelled through parent task cancellation")

    except Exception as e:
        logger.error(f"Error during event-based cancellation: {str(e)}")
        print(f"Error: {str(e)}")
    finally:
        # Ensure proper cleanup (MDC Tier 1)
        if not fetcher.completion_event.is_set():
            logger.debug(f"Setting completion event for fetcher {fetcher.symbol}")
            fetcher.completion_event.set()

        if not fetcher.cancel_event.is_set():
            logger.debug(f"Setting cancel event for fetcher {fetcher.symbol}")
            fetcher.cancel_event.set()

        # Clean up any lingering tasks using the task tracker (MDC Tier 1)
        if hasattr(fetcher, "task_tracker"):
            await fetcher.task_tracker.cancel_all(
                timeout=LINGERING_TASK_CLEANUP_TIMEOUT
            )

        # Run cleanup to prevent task leakage
        await cleanup_lingering_tasks()

        # Check for task leakage (MDC Tier 2)
        tasks_after = len(asyncio.all_tasks())
        if tasks_after > tasks_before:
            logger.warning(
                f"Task leakage detected in event-based cancellation demo: {tasks_after - tasks_before} more tasks"
            )
        else:
            logger.info(f"No task leakage in event-based cancellation demo")

    # Show final state
    print(f"Final operation state: {fetcher.progress['stage']}")
    logger.info(
        f"Event-based cancellation stats: paused {fetcher.progress['pause_count']} times, "
        f"completed: {fetcher.progress['completed']}"
    )

    print(f"Event-based cancellation demonstration complete")


async def demonstrate_concurrent_cancellation():
    """
    Demonstrates cancellation in a concurrent fetcher that is handling
    multiple delayed fetchers at once, showing proper propagation of cancellation.
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATING CONCURRENT CANCELLATION (MDC TIER 1)")
    print("=" * 70)
    print(
        "This demonstrates how cancellation propagates through concurrent operations."
    )
    print("We'll start multiple fetchers and then cancel the main task.")

    # Clear caches before this demonstration (clean slate)
    clear_caches()

    # Track tasks for leak detection (MDC Tier 2)
    tasks_before = len(asyncio.all_tasks())
    logger.info(f"Concurrent cancellation demo starting with {tasks_before} tasks")

    # Create a concurrent fetcher
    concurrent_fetcher = DelayedConcurrentFetcher()

    # Define multiple requests
    requests = [
        {"symbol": "BTCUSDT", "interval": Interval.HOUR_1},
        {"symbol": "ETHUSDT", "interval": Interval.HOUR_1},
        {"symbol": "BNBUSDT", "interval": Interval.HOUR_1},
    ]

    # Start fetch task but don't await it yet
    fetch_task = asyncio.create_task(concurrent_fetcher.fetch_multiple(requests))
    logger.debug(f"Created concurrent fetch task {id(fetch_task)}")

    try:
        # Let it run for 1.5 seconds
        print(f"Letting concurrent fetch run for 1.5 seconds...")
        await asyncio.sleep(1.5)

        # Then cancel it - demonstrating MDC Tier 1 cancellation via events
        print(f"🛑 Cancelling all concurrent fetch operations via cancel event...")
        logger.debug(f"Setting cancel event for concurrent fetcher")
        concurrent_fetcher.cancel_event.set()

        # Wait a bit for cancellation to propagate
        await asyncio.sleep(0.5)

        # Check status
        print(f"Checking cancellation propagation status...")
        for i, fetcher in enumerate(concurrent_fetcher.fetchers):
            print(
                f"Fetcher {i+1} ({fetcher.symbol}): "
                + f"Cancel event {'✓' if fetcher.cancel_event.is_set() else '✗'}, "
                + f"Stage: {fetcher.progress['stage']}"
            )

        # Demonstrate MDC Tier 1 cancellation via task.cancel()
        if not fetch_task.done():
            print(f"🛑 Now cancelling the parent task directly...")
            logger.debug(f"Initiating direct cancellation for task {id(fetch_task)}")
            # Use cancel_and_wait instead of just cancel() - MDC Tier 1
            await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)

            print(
                f"✓ Task cancellation status: {'Cancelled' if fetch_task.cancelled() else 'Not cancelled'}"
            )

    except Exception as e:
        logger.error(f"Error during concurrent cancellation demonstration: {str(e)}")
        print(f"Error: {str(e)}")
    finally:
        # Use the task tracker for cleanup
        await concurrent_fetcher.task_tracker.cancel_all(
            timeout=LINGERING_TASK_CLEANUP_TIMEOUT
        )

        # Clean up fetchers
        for fetcher in concurrent_fetcher.fetchers:
            if not fetcher.cancel_event.is_set():
                logger.debug(f"Setting cancel event for fetcher {fetcher.symbol}")
                fetcher.cancel_event.set()

            if (
                hasattr(fetcher, "completion_event")
                and not fetcher.completion_event.is_set()
            ):
                logger.debug(f"Setting completion event for fetcher {fetcher.symbol}")
                fetcher.completion_event.set()

        # Run cleanup to prevent task leakage
        await cleanup_lingering_tasks()

        # Check for task leakage (MDC Tier 2)
        tasks_after = len(asyncio.all_tasks())
        if tasks_after > tasks_before:
            logger.warning(
                f"Task leakage detected in concurrent cancellation demo: {tasks_after - tasks_before} more tasks"
            )
        else:
            logger.info(f"No task leakage in concurrent cancellation demo")

    print(f"Concurrent cancellation demonstration complete")


async def test_task_tracking_cleanup():
    """
    Demonstrates the importance of task tracking and proper cleanup
    to prevent resource leaks and hanging operations (MDC Tier 1).
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATING TASK TRACKING AND CLEANUP (MDC TIER 1)")
    print("=" * 70)
    print("This test verifies our implementation properly tracks and cleans up tasks")
    print("to prevent resource leaks - a critical MDC Tier 1 practice.")

    # Clear caches before test
    clear_caches()

    # Track tasks for leak detection (MDC Tier 2)
    tasks_before = len(asyncio.all_tasks())
    logger.info(f"Task tracking test starting with {tasks_before} tasks")

    # Create multiple fetchers to test task tracking
    fetchers = []
    symbols = ["BTCUSDT", "ETHUSDT", "DOGEUSDT"]

    for symbol in symbols:
        fetcher = DelayedEventBasedFetcher(symbol, Interval.HOUR_1, days_back=1)
        fetchers.append(fetcher)

    # Start all fetch operations
    print(f"Starting {len(symbols)} fetch operations to test task tracking...")
    fetch_tasks = []

    for fetcher in fetchers:
        task = asyncio.create_task(fetcher.fetch())
        fetch_tasks.append(task)

        # Properly track the task using the TaskTracker (MDC Tier 1)
        fetcher.task_tracker.add(task)

    # Let them run for a moment
    await asyncio.sleep(1)

    # Cancel all operations via their events
    print(f"Cancelling all operations via events...")
    for fetcher in fetchers:
        fetcher.cancel_event.set()

    # Wait for all to complete or timeout
    try:
        # Use wait_for with a reasonable timeout
        await asyncio.wait_for(
            asyncio.gather(*fetch_tasks, return_exceptions=True), timeout=5.0
        )
    except asyncio.TimeoutError:
        print(f"⚠️ Some tasks did not complete within timeout")

    # Check task tracking status
    print("\nChecking task tracking status:")
    for i, fetcher in enumerate(fetchers):
        tracked_count = fetcher.task_tracker.get_count()
        print(f"Fetcher {i+1} ({fetcher.symbol}): {tracked_count} tasks still tracked")

    # Verify all tracked tasks are properly removed from tracking sets
    print("\nCleaning up any lingering tracked tasks...")
    lingering_task_count = 0

    for fetcher in fetchers:
        # Cancel all tasks with the task tracker
        success_count, error_count = await fetcher.task_tracker.cancel_all(
            timeout=LINGERING_TASK_CLEANUP_TIMEOUT
        )
        lingering_task_count += success_count + error_count

    print(f"Found {lingering_task_count} lingering tracked tasks to clean up")

    # Final cleanup
    await cleanup_lingering_tasks()

    # Check for task leakage
    tasks_after = len(asyncio.all_tasks())
    task_diff = tasks_after - tasks_before

    if task_diff > 0:
        print(f"⚠️ Task leakage detected: {task_diff} more tasks after test")
        logger.warning(f"Task leakage detected: {task_diff} tasks")
    else:
        print(f"✓ No task leakage detected after proper cleanup")
        logger.info("No task leakage detected")

    print("Task tracking and cleanup demonstration complete")


async def main():
    """
    Main function that orchestrates the demonstration.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Task Cancellation Demonstration")
    parser.add_argument(
        "--debug", action="store_true", help="Enable DEBUG level logging"
    )
    parser.add_argument(
        "--only",
        choices=["event", "concurrent", "tracking"],
        help="Run only the specified demonstration",
    )
    args = parser.parse_args()

    # Set logging level if debug is enabled
    if args.debug:
        enable_debug_logging()

    # Clear caches at startup
    clear_caches()

    print("\n" + "=" * 70)
    print("MDC-COMPLIANT TASK CANCELLATION DEMONSTRATION")
    print("=" * 70)
    print(
        "This script demonstrates best practices for task cancellation from MDC guidelines."
    )
    print(
        "Each demo focuses on a critical (Tier 1) aspect of proper cancellation handling."
    )

    # Record all running tasks at start for leak detection
    tasks_at_start = len(asyncio.all_tasks())
    logger.info(f"Starting with {tasks_at_start} active tasks")

    try:
        # Import time module with proper error handling
        import time
    except ImportError:
        logger.error("Could not import time module")
        return

    # Run demonstrations based on user selection
    if args.only is None or args.only == "event":
        await demonstrate_event_based_cancellation()

    if args.only is None or args.only == "concurrent":
        await demonstrate_concurrent_cancellation()

    if args.only is None or args.only == "tracking":
        await test_task_tracking_cleanup()

    # Force cleanup of any lingering tasks before checking for leakage
    await cleanup_lingering_tasks()

    # Force garbage collection
    gc.collect()

    # Check for task leakage at the end
    tasks_at_end = len(asyncio.all_tasks())
    if tasks_at_end > tasks_at_start:
        logger.warning(
            f"Task leakage detected: {tasks_at_end - tasks_at_start} more tasks at end than at start"
        )
        print(
            f"⚠️ Task leakage detected: {tasks_at_end - tasks_at_start} more tasks at end than at start"
        )
    else:
        logger.info(f"No task leakage detected. Tasks at end: {tasks_at_end}")
        print(f"✓ No task leakage detected. Tasks at end: {tasks_at_end}")

    print("\n" + "=" * 70)
    print("DEMONSTRATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    # Run the demonstration
    asyncio.run(main())
