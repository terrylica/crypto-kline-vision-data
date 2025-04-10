#!/usr/bin/env python

"""
Task Management Utilities for Asynchronous Operations.

This module provides utilities for managing asyncio tasks with a focus on:
1. Event-based cancellation (MDC Tier 1 - critical)
2. Task tracking and proper cleanup (MDC Tier 1 - critical)
3. Concurrent cancellation propagation (MDC Tier 1 - critical)

Key features:
- Event-based alternatives to asyncio.wait_for
- Cleanup utilities for preventing task leakage
- Task cancellation with proper error handling

MDC Tiers:
- Tier 1: Critical practices that prevent resource leaks and ensure proper cancellation
- Tier 2: Important practices for better observability and control
- Tier 3: Nice-to-have practices that improve overall robustness
"""

from utils.logger_setup import logger
from rich import print
import asyncio
import time
import gc
import sys

from utils.config import (
    TASK_CANCEL_WAIT_TIMEOUT,
    LINGERING_TASK_CLEANUP_TIMEOUT,
    AGGRESSIVE_TASK_CLEANUP_TIMEOUT,
)
from utils.async_cleanup import cancel_and_wait


# Event-based wait alternative to asyncio.wait_for (MDC Tier 1 practice)
async def wait_with_cancellation(
    task, completion_event=None, cancel_event=None, timeout=None, check_interval=0.1
):
    """
    Wait for task completion, cancellation, or timeout using events instead of asyncio.wait_for.

    This is a MDC Tier 1 practice that provides more reliable task cancellation handling
    by using events rather than timeouts for controlling task execution flow.

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


# MDC Tier 1 practice - preventing task leakage
async def cleanup_lingering_tasks():
    """
    Clean up any lingering tasks to prevent leakage.

    This is a MDC Tier 1 practice that ensures all tasks are properly cancelled
    and cleaned up, preventing resource leaks that could lead to system instability.
    """
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


# MDC Tier 1 practice for propagating cancellation to child tasks
async def propagate_cancellation(parent_event, child_events):
    """
    Propagate cancellation from a parent event to multiple child events.

    This is a MDC Tier 1 practice that ensures cancellation requests properly
    cascade through the task hierarchy, preventing orphaned tasks.

    Args:
        parent_event: The parent cancellation event to monitor
        child_events: List of child cancellation events to trigger when parent is set

    Returns:
        None
    """
    try:
        # Wait for the parent event to be set
        await parent_event.wait()

        # Once parent event is set, set all child events
        for child_event in child_events:
            if not child_event.is_set():
                logger.debug("Propagating cancellation to child event")
                child_event.set()
    except asyncio.CancelledError:
        # If this propagation task itself is cancelled, still try to propagate
        for child_event in child_events:
            if not child_event.is_set():
                logger.debug(
                    "Propagating cancellation to child event (during task cancellation)"
                )
                child_event.set()
        raise


# Track tasks with automatic cleanup on completion
class TaskTracker:
    """
    Track a set of tasks and automatically remove them when they complete.

    This implements MDC Tier 1 practices for task tracking and management,
    ensuring that all tasks are properly accounted for and cleaned up.
    """

    def __init__(self):
        """Initialize an empty task tracking set."""
        self.tasks = set()

    def add(self, task):
        """
        Add a task to the tracker with automatic cleanup.

        Args:
            task: The asyncio.Task to track
        """
        self.tasks.add(task)
        task.add_done_callback(lambda t: self.tasks.discard(t))
        return task

    async def cancel_all(self, timeout=TASK_CANCEL_WAIT_TIMEOUT):
        """
        Cancel all tracked tasks and wait for them to complete.

        Args:
            timeout: Maximum time to wait for each task to cancel

        Returns:
            Tuple of (success_count, error_count)
        """
        if not self.tasks:
            return 0, 0

        # Make a copy to avoid "Set changed size during iteration"
        tasks_to_cancel = list(self.tasks)
        success_count = 0
        error_count = 0

        for task in tasks_to_cancel:
            if not task.done():
                try:
                    success = await cancel_and_wait(task, timeout=timeout)
                    if success:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    logger.error(f"Error during task cancellation: {e}")
                    error_count += 1

        return success_count, error_count

    def get_count(self):
        """Get the number of currently tracked tasks."""
        return len(self.tasks)
