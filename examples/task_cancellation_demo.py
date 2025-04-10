#!/usr/bin/env python3
"""
Task Cancellation Demo for EventBasedFetcher and ConcurrentFetcher.

This script demonstrates how task cancellation works in the EventBasedFetcher and
ConcurrentFetcher classes. It shows various cancellation scenarios:
1. Manual cancellation (explicitly calling cancel on tasks)
2. Timeout cancellation (tasks cancelled due to timeout)
3. Concurrent cancellation (cancellation propagation in parallel tasks)
4. Signal cancellation (handling Ctrl+C and other signals)
5. Event-based cancellation (using completion events without timeouts)

The demo includes a subclass of EventBasedFetcher that introduces an artificial
delay in the fetch process to make cancellation more observable.
"""

from utils.logger_setup import logger
from rich import print
import asyncio
import signal
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

# Import task management utilities
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

# Reset the cancellation flag at the start
cancellation_requested = False

# Define a constant for MAX_SINGLE_OPERATION_TIMEOUT locally
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


# Event-based wait alternative to asyncio.wait_for (recommended in MDC)
async def wait_with_cancellation(
    task, completion_event=None, cancel_event=None, timeout=None, check_interval=0.1
):
    """
    Wait for task completion, cancellation, or timeout using events instead of asyncio.wait_for.

    Args:
        task: Task to wait for
        completion_event: Event that signals completion (optional)
        cancel_event: Event that signals cancellation request (optional)
        timeout: Optional timeout in seconds (only used as fallback)
        check_interval: How often to check events

    Returns:
        True if completed normally, False if cancelled or timed out
    """
    start_time = time.time()

    while not task.done():
        # Check for cancellation event
        if cancel_event and cancel_event.is_set():
            logger.info(f"Cancellation event detected during wait")
            return False

        # Check for completion event
        if completion_event and completion_event.is_set():
            logger.info(f"Completion event detected during wait")
            return True

        # Check for cancellation of current task
        if asyncio.current_task().cancelled():
            logger.info(f"Current task cancelled during wait")
            return False

        # Check for timeout (fallback only)
        if timeout and (time.time() - start_time > timeout):
            logger.info(f"Timeout ({timeout}s) reached during wait")
            return False

        # Yield to allow task to progress
        try:
            await asyncio.sleep(check_interval)
        except asyncio.CancelledError:
            # If our wait task is cancelled, report that
            logger.info(f"Wait operation was cancelled")
            return False

    # Task is done - check if it was cancelled
    if task.cancelled():
        logger.debug(f"Task {id(task)} was cancelled before completing")
        return False

    # Check if task completed with an exception
    if task.done():
        try:
            # This will re-raise any exception from the task
            exception = task.exception()
            if exception:
                logger.warning(
                    f"Task {id(task)} completed with exception: {str(exception)}"
                )
                # Don't count exceptions as successful completion
                return False
        except asyncio.CancelledError:
            # This happens when we check .exception() on a cancelled task
            logger.debug(f"Task {id(task)} was confirmed cancelled")
            return False

    # Task completed normally
    return True


# Improved utility function for more reliable task cancellation
async def cancel_and_wait(task, timeout=TASK_CANCEL_WAIT_TIMEOUT):
    """
    Cancel a task and wait for it to complete with detailed status reporting.
    This function is more reliable than just task.cancel() as it waits for
    the cancellation to complete.

    Args:
        task: The asyncio.Task to cancel
        timeout: Maximum time to wait for task completion after cancellation

    Returns:
        True if task was successfully cancelled and completed
        False if timed out waiting for task to complete
    """
    if task is None:
        logger.warning("Attempted to cancel None task")
        return False

    if task.done():
        logger.debug(f"Task {id(task)} already completed - no cancellation needed")
        return True

    task_id = id(task)
    logger.debug(f"Cancelling task {task_id}")

    # Request cancellation
    task.cancel()

    try:
        # Wait for the task to complete (with a timeout)
        start_time = time.time()
        try:
            # Use shield to prevent wait_for from cancelling the task
            await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
        except asyncio.CancelledError:
            # Expected - when the shielded task is cancelled, we get this
            logger.debug(
                f"Task {task_id} raised CancelledError during wait as expected"
            )
            # Don't re-raise CancelledError
        except asyncio.TimeoutError:
            logger.warning(
                f"Timed out waiting for task {task_id} to complete after cancellation"
            )
            return False

        elapsed = time.time() - start_time
        logger.debug(
            f"Task {task_id} took {elapsed:.3f}s to complete after cancellation"
        )

        # Log task status
        if task.done() and not task.cancelled():
            try:
                exception = task.exception()
                if exception:
                    if isinstance(exception, asyncio.CancelledError):
                        logger.debug(
                            f"Task {task_id} confirmed cancelled with CancelledError"
                        )
                    else:
                        logger.warning(
                            f"Task {task_id} completed with exception: {exception}"
                        )
                else:
                    logger.debug(
                        f"Task {task_id} completed normally after cancellation"
                    )
            except asyncio.CancelledError:
                # This happens when we check exception() on a cancelled task
                logger.debug(f"Task {task_id} was confirmed cancelled")
        elif task.cancelled():
            logger.debug(f"Task {task_id} confirmed cancelled")
        elif task.done():
            logger.debug(f"Task {task_id} completed normally after cancellation")
        else:
            logger.warning(
                f"Task {task_id} has unexpected state: done={task.done()}, cancelled={task.cancelled()}"
            )

        return True

    except Exception as e:
        logger.error(f"Error during task {task_id} cancellation: {str(e)}")
        return False
    finally:
        # Report final task status
        logger.debug(
            f"Final task {task_id} status: done={task.done()}, cancelled={task.cancelled()}"
        )


# Utility function for cleaning up lingering tasks
async def cleanup_lingering_tasks():
    """Clean up any lingering tasks to prevent leakage."""
    tasks = [t for t in asyncio.all_tasks() if t != asyncio.current_task()]

    if tasks:
        logger.info(f"Cleaning up {len(tasks)} lingering tasks")

        # Log task details for debugging
        for i, task in enumerate(tasks):
            task_name = (
                task.get_name() if hasattr(task, "get_name") else f"Task-{id(task)}"
            )
            logger.debug(
                f"Lingering task {i+1}/{len(tasks)}: {task_name}, done={task.done()}, cancelled={task.cancelled()}"
            )

        # Cancel all tasks using cancel_and_wait
        success_count = 0
        error_count = 0
        for task in tasks:
            if not task.done():
                try:
                    task_id = id(task)
                    logger.debug(f"Cancelling lingering task {task_id}")
                    success = await cancel_and_wait(
                        task, timeout=LINGERING_TASK_CLEANUP_TIMEOUT
                    )
                    if success:
                        success_count += 1
                    else:
                        error_count += 1
                        logger.warning(f"Failed to cancel task {task_id} cleanly")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error cancelling task: {str(e)}")

        # Force garbage collection to clean up resources
        gc.collect()

        # Log cleanup results
        if success_count > 0:
            logger.info(f"Successfully cancelled {success_count} tasks")
        if error_count > 0:
            logger.warning(f"Failed to cancel {error_count} tasks cleanly")

        # Check if any tasks are still running
        remaining = [t for t in tasks if not t.done()]
        if remaining:
            logger.warning(f"{len(remaining)} tasks still not completed after cleanup")

            # More aggressive task cancellation for lingering tasks
            for task in remaining:
                if not task.done():
                    task_id = id(task)
                    logger.debug(
                        f"Aggressive cancellation for persistent task {task_id}"
                    )
                    try:
                        # Direct cancellation without waiting
                        task.cancel()

                        # Brief wait to let the task respond to cancellation
                        try:
                            await asyncio.wait_for(
                                asyncio.shield(task),
                                timeout=AGGRESSIVE_TASK_CLEANUP_TIMEOUT,
                            )
                        except (
                            asyncio.CancelledError,
                            asyncio.TimeoutError,
                            Exception,
                        ):
                            # We expect exceptions here, so just log and move on
                            pass
                    except Exception as e:
                        logger.error(f"Error during aggressive cancellation: {str(e)}")

            # Final count of remaining tasks after aggressive cleanup
            still_remaining = [t for t in remaining if not t.done()]
            if still_remaining:
                logger.error(
                    f"{len(still_remaining)} tasks still running despite aggressive cancellation"
                )
            else:
                logger.info("All tasks successfully cancelled after aggressive cleanup")
        else:
            logger.info("All tasks successfully cancelled on first attempt")


class BaseEventFetcher:
    """
    Base class for fetchers with event-based completion signaling.
    This provides the basic functionality for the demonstration.
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
        self.progress.update(
            {
                "stage": "initialized",
                "delay_progress": "0%",
                "completed": False,
                "cancellation_source": None,
            }
        )

    async def _fetch_impl(self):
        """
        Override the fetch implementation to add delays and cancellation checks.
        """
        global cancellation_requested
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
                if (
                    cancellation_requested
                    or self.cancel_event.is_set()
                    or asyncio.current_task().cancelled()
                ):

                    # Record cancellation source for better logging (MDC Tier 2)
                    if cancellation_requested:
                        self.progress["cancellation_source"] = "global_flag"
                    elif self.cancel_event.is_set():
                        self.progress["cancellation_source"] = "cancel_event"
                    else:
                        self.progress["cancellation_source"] = "task_cancelled"

                    logger.warning(
                        f"Cancellation detected during delay for {self.symbol} (source: {self.progress['cancellation_source']})"
                    )
                    print(f"⚠️ Cancellation detected during delay for {self.symbol}")

                    # Raise cancellation error to simulate cancellation
                    raise asyncio.CancelledError("Manual cancellation during delay")

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
            # Log completion time for performance monitoring (MDC Tier 2)
            elapsed = time.time() - start_time
            logger.info(
                f"Fetch for {self.symbol} took {elapsed:.2f}s to complete (stage: {self.progress['stage']})"
            )

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


async def demonstrate_manual_cancellation():
    """
    Demonstrates how to manually cancel a fetch task after letting it
    run for a short period of time.
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATING MANUAL CANCELLATION")
    print("=" * 70)
    print("This demonstrates explicitly cancelling a task by calling .cancel() on it.")
    print(
        "The fetcher will show how it handles the cancellation and cleans up resources."
    )

    # Clear caches before this demonstration (clean slate)
    clear_caches()

    # Track tasks for leak detection (MDC Tier 2)
    tasks_before = len(asyncio.all_tasks())
    logger.info(f"Manual cancellation demo starting with {tasks_before} tasks")

    symbol = "BTCUSDT"
    interval = Interval.HOUR_1
    fetcher = DelayedEventBasedFetcher(symbol, interval, days_back=1)

    # Start the fetch but don't await it yet
    fetch_task = asyncio.create_task(fetcher.fetch())
    logger.debug(f"Created fetch task {id(fetch_task)} for {symbol}")

    # Track the task using TaskTracker
    fetcher.task_tracker.add(fetch_task)

    try:
        # Let it run for 2 seconds
        print(f"Letting fetch task run for 2 seconds before cancellation...")
        await asyncio.sleep(2)

        # Check if it's still running
        if not fetch_task.done():
            print(f"🛑 Manually cancelling fetch task for {symbol}...")
            logger.debug(
                f"Initiating cancellation for task {id(fetch_task)} for {symbol}"
            )
            # Use cancel_and_wait instead of just cancel()
            await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)

            print(
                f"✓ Task cancellation status: {'Cancelled' if fetch_task.cancelled() else 'Not cancelled'}"
            )
        else:
            print(f"Task already completed before we could cancel it")

    except Exception as e:
        logger.error(f"Error during manual cancellation demonstration: {str(e)}")
        print(f"Error: {str(e)}")
    finally:
        # Ensure completion event is set
        if not fetcher.completion_event.is_set():
            fetcher.completion_event.set()

        # Clean up any lingering tasks using task tracker
        await fetcher.task_tracker.cancel_all(timeout=LINGERING_TASK_CLEANUP_TIMEOUT)

        # Run cleanup to prevent task leakage
        await cleanup_lingering_tasks()

        # Check for task leakage (MDC Tier 2)
        tasks_after = len(asyncio.all_tasks())
        if tasks_after > tasks_before:
            logger.warning(
                f"Task leakage detected in manual cancellation demo: {tasks_after - tasks_before} more tasks"
            )
        else:
            logger.info(f"No task leakage in manual cancellation demo")

    print(f"Manual cancellation demonstration complete")


async def demonstrate_timeout_cancellation():
    """
    Demonstrates cancellation due to timeout when a fetch operation
    takes too long to complete.
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATING TIMEOUT CANCELLATION")
    print("=" * 70)
    print("This demonstrates how tasks can be cancelled due to a timeout.")
    print("We'll set a very short timeout that's shorter than our artificial delay.")

    # Clear caches before this demonstration (clean slate)
    clear_caches()

    # Track tasks for leak detection (MDC Tier 2)
    tasks_before = len(asyncio.all_tasks())
    logger.info(f"Timeout cancellation demo starting with {tasks_before} tasks")

    symbol = "ETHUSDT"
    interval = Interval.HOUR_1

    # Create a fetcher with a very short timeout to ensure it times out
    fetcher = DelayedEventBasedFetcher(symbol, interval, days_back=1)

    # The artificial delay is longer than this timeout, so it should timeout
    very_short_timeout = 1.5  # seconds

    print(f"Starting fetch with a {very_short_timeout}s timeout (should fail)")

    # Create a task to track
    fetch_task = asyncio.create_task(fetcher.fetch())
    logger.debug(f"Created fetch task {id(fetch_task)} for {symbol}")

    # Add task to tracker
    fetcher.task_tracker.add(fetch_task)

    # Flag to track if timeout occurred
    timeout_occurred = False

    try:
        # Create a timeout event and timer task
        timeout_event = asyncio.Event()

        # Create a task for the timeout
        async def timeout_timer():
            nonlocal timeout_occurred
            await asyncio.sleep(very_short_timeout)
            if not fetch_task.done():
                logger.info(
                    f"Timeout reached after {very_short_timeout}s, triggering cancellation"
                )
                timeout_occurred = True
                timeout_event.set()
                fetcher.cancel_event.set()  # Signal fetcher to cancel

        timer_task = asyncio.create_task(timeout_timer())
        logger.debug(f"Created timer task {id(timer_task)} for timeout")
        fetcher.task_tracker.add(timer_task)

        # Wait for completion using event-based method (MDC Tier 1 practice)
        result = await wait_with_cancellation(
            fetch_task,
            completion_event=fetcher.completion_event,
            cancel_event=timeout_event,
            timeout=very_short_timeout,
        )

        # Check if timeout occurred - this is determined by our timer rather than the result of wait_with_cancellation
        if timeout_occurred:
            print(f"✓ Task timed out as expected after {very_short_timeout}s")
            # Make sure to cancel the task after timeout
            if not fetch_task.done():
                logger.debug(f"Cancelling fetch task {id(fetch_task)} after timeout")
                await cancel_and_wait(
                    fetch_task, timeout=LINGERING_TASK_CLEANUP_TIMEOUT
                )
        else:
            # Safely get result, handling potential CancelledError
            task_result = None
            if fetch_task.done() and not fetch_task.cancelled():
                try:
                    task_result = fetch_task.result()
                    print(
                        f"Unexpectedly completed without timeout: {task_result is not None}"
                    )
                except asyncio.CancelledError:
                    # This should not happen, but handle it just in case
                    print(f"Task completed but was cancelled")
            else:
                print(f"Task did not complete normally")

    except Exception as e:
        logger.error(f"Unexpected error during timeout demonstration: {str(e)}")
        print(f"Unexpected error: {str(e)}")
    finally:
        # Ensure completion event is set
        if not fetcher.completion_event.is_set():
            fetcher.completion_event.set()

        # Clean up any lingering tasks using task tracker
        await fetcher.task_tracker.cancel_all(timeout=LINGERING_TASK_CLEANUP_TIMEOUT)

        # Run cleanup to prevent task leakage
        await cleanup_lingering_tasks()

        # Check for task leakage (MDC Tier 2)
        tasks_after = len(asyncio.all_tasks())
        if tasks_after > tasks_before:
            logger.warning(
                f"Task leakage detected in timeout cancellation demo: {tasks_after - tasks_before} more tasks"
            )
        else:
            logger.info(f"No task leakage in timeout cancellation demo")

    print(f"Timeout cancellation demonstration complete")


def handle_signal(sig, frame):
    """
    Signal handler to gracefully handle cancellation requests.
    NOTE: Signal-based cancellation is explicitly discouraged by the MDC guidelines
    for production code. This is implemented here for demonstration purposes only.
    In production, use event-based cancellation patterns instead.
    """
    global cancellation_requested

    if cancellation_requested:
        print("\n⚠️ Second interrupt received, forcing exit...")
        sys.exit(1)

    print("\n⚠️ Interrupt received, requesting graceful cancellation...")
    cancellation_requested = True


async def demonstrate_signal_cancellation():
    """
    Demonstrates handling external cancellation requests via signals.

    NOTE: This demonstration shows why signal-based cancellation is problematic
    and how to convert such signals to event-based cancellation, which is the
    recommended approach in the MDC guidelines. This pattern should not be used
    in production code except as a bridge to convert signals to cancellation events.
    """
    global cancellation_requested

    # Reset the cancellation flag at the start
    cancellation_requested = False

    print("\n" + "=" * 70)
    print("DEMONSTRATING SIGNAL CANCELLATION")
    print("=" * 70)
    print("This demonstrates how to handle external cancellation (e.g., Ctrl+C).")
    print("The long-running fetch operation will monitor the cancellation flag.")
    print("Press Ctrl+C during execution to trigger cancellation.")
    print("(You have 10 seconds to press Ctrl+C before it completes normally)")

    # Clear caches before this demonstration (clean slate)
    clear_caches()

    # Track tasks for leak detection (MDC Tier 2)
    tasks_before = len(asyncio.all_tasks())
    logger.info(f"Signal cancellation demo starting with {tasks_before} tasks")

    # Register signal handler
    original_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        symbol = "BNBUSDT"
        interval = Interval.HOUR_1
        fetcher = DelayedEventBasedFetcher(symbol, interval, days_back=2)

        # Create cancel event that will be triggered by the signal handler
        cancel_event = asyncio.Event()

        # Connect global cancellation flag to the cancel event
        async def monitor_cancellation_flag():
            while not cancel_event.is_set() and not fetcher.completion_event.is_set():
                if cancellation_requested:
                    logger.info(
                        f"Global cancellation flag detected, triggering cancel event"
                    )
                    cancel_event.set()
                    fetcher.cancel_event.set()
                    break
                await asyncio.sleep(0.1)

        # Start the monitor task
        monitor_task = asyncio.create_task(monitor_cancellation_flag())
        logger.debug(f"Created monitor task {id(monitor_task)} for signal handling")
        fetcher.task_tracker.add(monitor_task)

        # Start the fetch
        print(f"Starting fetch for {symbol}...")
        fetch_task = asyncio.create_task(fetcher.fetch())
        logger.debug(f"Created fetch task {id(fetch_task)} for {symbol}")
        fetcher.task_tracker.add(fetch_task)

        # Wait for completion using event-based approach (MDC Tier 1)
        try:
            success = await wait_with_cancellation(
                fetch_task,
                completion_event=fetcher.completion_event,
                cancel_event=cancel_event,
                timeout=10.0,  # Fallback timeout
            )

            if success and fetch_task.done() and not fetch_task.cancelled():
                result = fetch_task.result()
                if not cancellation_requested:
                    print(f"✓ Fetch completed normally without cancellation")
                else:
                    print(f"✓ Fetch completed with cancellation request")
            else:
                print(f"Fetch was cancelled or timed out")
                # Make sure to cancel the task
                if not fetch_task.done():
                    logger.debug(f"Cancelling fetch task {id(fetch_task)} after wait")
                    await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)

        except asyncio.CancelledError:
            print(f"✓ Fetch was cancelled as expected")
            logger.warning(
                f"Signal cancellation demo was cancelled during {fetcher.progress['stage']}"
            )

        except Exception as e:
            logger.error(f"Error during signal cancellation demonstration: {str(e)}")
            print(f"Error: {str(e)}")

    finally:
        # Restore original signal handler
        signal.signal(signal.SIGINT, original_handler)

        # Clean up fetcher
        if hasattr(fetcher, "cancel_event") and not fetcher.cancel_event.is_set():
            logger.debug(f"Setting cancel event for fetcher {fetcher.symbol}")
            fetcher.cancel_event.set()

        if not fetcher.completion_event.is_set():
            logger.debug(f"Setting completion event for fetcher {fetcher.symbol}")
            fetcher.completion_event.set()

        # Clean up any lingering tasks using the task tracker
        await fetcher.task_tracker.cancel_all(timeout=LINGERING_TASK_CLEANUP_TIMEOUT)

        # Run cleanup to prevent task leakage
        await cleanup_lingering_tasks()

        # Reset the cancellation flag
        cancellation_requested = False

        # Check for task leakage (MDC Tier 2)
        tasks_after = len(asyncio.all_tasks())
        if tasks_after > tasks_before:
            logger.warning(
                f"Task leakage detected in signal cancellation demo: {tasks_after - tasks_before} more tasks"
            )
        else:
            logger.info(f"No task leakage in signal cancellation demo")

    print(f"Signal cancellation demonstration complete")


async def demonstrate_event_based_cancellation():
    """
    Demonstrates pure event-based cancellation without relying on timeouts.
    This demonstrates a more robust approach that can replace timeout-based mechanisms.
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATING EVENT-BASED CANCELLATION")
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
        fetcher.task_tracker.add(completion_future)

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

        # Clean up any lingering tasks using task tracker
        await fetcher.task_tracker.cancel_all(timeout=LINGERING_TASK_CLEANUP_TIMEOUT)

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


async def test_for_warnings():
    """
    Focused test specifically designed to check if warnings have been fixed.
    This function runs quick cancellation scenarios and monitors for the warnings.
    """
    print("\n" + "=" * 70)
    print("RUNNING FOCUSED WARNING TEST")
    print("=" * 70)

    # Clear caches before this warning test (clean slate)
    clear_caches()

    # Track tasks for leak detection (MDC Tier 2)
    tasks_before = len(asyncio.all_tasks())
    logger.info(f"Warning test starting with {tasks_before} tasks")

    # Setup test logger to capture warnings
    class WarningCounter:
        def __init__(self):
            self.reset()

        def reset(self):
            self.task_leakage_warnings = 0
            self.still_running_warnings = 0
            self.cancelled_during_delay_warnings = 0

    warning_counter = WarningCounter()

    # Test handler to monitor log messages
    original_warning = logger.warning

    def warning_monitor(msg, *args, **kwargs):
        if "Task leakage detected" in msg:
            warning_counter.task_leakage_warnings += 1
        elif "still running after completion" in msg:
            warning_counter.still_running_warnings += 1
        elif "cancelled during delay" in msg:
            warning_counter.cancelled_during_delay_warnings += 1
        return original_warning(msg, *args, **kwargs)

    # Replace warning function
    logger.warning = warning_monitor

    try:
        # Test 1: Quick cancel test
        print("Test 1: Quick cancellation test")
        # Clear caches before this specific test
        clear_caches()

        symbol = "XRPUSDT"
        interval = Interval.HOUR_1
        fetcher = DelayedEventBasedFetcher(symbol, interval, days_back=1)

        fetch_task = asyncio.create_task(fetcher.fetch())
        logger.debug(f"Created fetch task {id(fetch_task)} for {symbol}")

        # Use a custom callback that logs task status changes
        def task_status_callback(t):
            logger.debug(
                f"Task {id(t)} for {symbol} completed with status: done={t.done()}, cancelled={t.cancelled()}"
            )
            fetcher.tasks.discard(t)

        fetch_task.add_done_callback(task_status_callback)
        fetcher.tasks.add(fetch_task)

        # Cancel immediately
        await asyncio.sleep(0.5)
        logger.debug(f"Cancelling fetch task {id(fetch_task)} after 0.5s")
        await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)

        # Clean up
        if not fetcher.cancel_event.is_set():
            logger.debug(f"Setting cancel event for fetcher {fetcher.symbol}")
            fetcher.cancel_event.set()

        if not fetcher.completion_event.is_set():
            logger.debug(f"Setting completion event for fetcher {fetcher.symbol}")
            fetcher.completion_event.set()

        # Clean up any lingering tasks - Make a copy to avoid "Set changed size during iteration"
        tasks_to_cleanup = list(fetcher.tasks)
        logger.debug(
            f"Cleaning up {len(tasks_to_cleanup)} lingering tasks in fetcher for {symbol}"
        )
        for task in tasks_to_cleanup:
            if not task.done():
                logger.debug(f"Cancelling lingering task {id(task)} in fetcher")
                await cancel_and_wait(task, timeout=LINGERING_TASK_CLEANUP_TIMEOUT)

        # Run cleanup
        await cleanup_lingering_tasks()

        # Test 2: Concurrent cancel test
        print("\nTest 2: Concurrent cancellation test")
        # Clear caches before this specific test
        clear_caches()

        test2_tasks_before = len(asyncio.all_tasks())

        concurrent_fetcher = DelayedConcurrentFetcher()
        requests = [
            {"symbol": "SOLUSDT", "interval": Interval.HOUR_1},
            {"symbol": "ADAUSDT", "interval": Interval.HOUR_1},
        ]

        fetch_task = asyncio.create_task(concurrent_fetcher.fetch_multiple(requests))
        logger.debug(f"Created concurrent fetch task {id(fetch_task)}")

        # Cancel after brief delay
        await asyncio.sleep(0.5)
        logger.debug(f"Cancelling concurrent fetch task {id(fetch_task)} after 0.5s")
        await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)

        # Clean up
        if not concurrent_fetcher.cancel_event.is_set():
            logger.debug(f"Setting cancel event for concurrent fetcher")
            concurrent_fetcher.cancel_event.set()

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

        # Clean up any lingering tasks - Make a copy to avoid "Set changed size during iteration"
        tasks_to_cleanup = list(concurrent_fetcher.tasks)
        logger.debug(
            f"Cleaning up {len(tasks_to_cleanup)} lingering tasks in concurrent fetcher"
        )
        for task in tasks_to_cleanup:
            if not task.done():
                logger.debug(
                    f"Cancelling lingering task {id(task)} in concurrent fetcher"
                )
                await cancel_and_wait(task, timeout=LINGERING_TASK_CLEANUP_TIMEOUT)

        # Run cleanup
        await cleanup_lingering_tasks()

        # Force garbage collection
        gc.collect()

        # Check for task leakage in test 2
        test2_tasks_after = len(asyncio.all_tasks())
        if test2_tasks_after > test2_tasks_before:
            print(
                f"⚠️ Task leakage detected in test 2: {test2_tasks_after - test2_tasks_before} more tasks"
            )
            logger.warning(
                f"Task leakage detected in test 2: {test2_tasks_after - test2_tasks_before} more tasks"
            )
        else:
            print(f"✓ No task leakage detected in test 2")

        # Report results
        print("\nWarning Test Results:")
        print(f"Task leakage warnings: {warning_counter.task_leakage_warnings}")
        print(f"Still running warnings: {warning_counter.still_running_warnings}")
        print(
            f"Cancelled during delay warnings: {warning_counter.cancelled_during_delay_warnings}"
        )

        success = (
            warning_counter.task_leakage_warnings == 0
            and warning_counter.still_running_warnings <= 1
        )  # Allow at most 1 still running warning

        if success:
            print("✅ WARNING TEST PASSED - Critical warnings have been fixed!")
        else:
            print("❌ WARNING TEST FAILED - Some warnings still present")

        # Overall test leakage check (MDC Tier 2)
        tasks_after = len(asyncio.all_tasks())
        if tasks_after > tasks_before:
            logger.warning(
                f"Task leakage detected in warning tests overall: {tasks_after - tasks_before} more tasks"
            )
        else:
            logger.info(f"No task leakage in warning tests overall")

    finally:
        # Restore original warning function
        logger.warning = original_warning

        # Final cleanup to prevent leakage between tests
        await cleanup_lingering_tasks()

    print("=" * 70)


async def demonstrate_concurrent_cancellation():
    """
    Demonstrates cancellation in a concurrent fetcher that is handling
    multiple delayed fetchers at once.
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATING CONCURRENT CANCELLATION")
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

        # Then cancel it
        print(f"🛑 Cancelling all concurrent fetch operations...")
        logger.debug(
            f"Initiating cancellation for concurrent fetch task {id(fetch_task)}"
        )
        # Use cancel_and_wait instead of just cancel()
        await cancel_and_wait(fetch_task, timeout=TASK_CANCEL_WAIT_TIMEOUT)

        print(
            f"✓ Concurrent task cancellation status: {'Cancelled' if fetch_task.cancelled() else 'Not cancelled'}"
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


async def main():
    """
    Main function that orchestrates demonstrations of task cancellation.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Demonstrate different task cancellation approaches"
    )
    parser.add_argument(
        "--only",
        type=str,
        help="Run only a specific demo (manual, timeout, signal, event, concurrent)",
        default=None,
    )
    args = parser.parse_args()

    # Initialize logging
    enable_debug_logging()

    try:
        # Run demos based on CLI args
        if args.only:
            if args.only == "manual":
                await demonstrate_manual_cancellation()
            elif args.only == "timeout":
                await demonstrate_timeout_cancellation()
            elif args.only == "signal":
                await demonstrate_signal_cancellation()
            elif args.only == "event":
                await demonstrate_event_based_cancellation()
            elif args.only == "concurrent":
                await demonstrate_concurrent_cancellation()
            else:
                print(f"Unknown demo: {args.only}")
        else:
            # Run all demos in sequence
            await demonstrate_manual_cancellation()
            await demonstrate_timeout_cancellation()
            await demonstrate_signal_cancellation()
            await demonstrate_event_based_cancellation()
            await demonstrate_concurrent_cancellation()

    except asyncio.CancelledError:
        logger.warning("Main task was cancelled")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        # Clean up any lingering tasks
        await cleanup_lingering_tasks()


if __name__ == "__main__":
    # Run the main function
    asyncio.run(main())
