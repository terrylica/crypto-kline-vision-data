#!/usr/bin/env python3
"""
Task Management Demo - Simplified standalone demo for task_management.py utilities.

This script demonstrates the usage of the task_management utilities
showing key MDC-compliant patterns:

1. Event-based cancellation (MDC Tier 1)
2. Task tracking and cleanup (MDC Tier 1)
3. Cancellation propagation (MDC Tier 1)
"""

from utils.logger_setup import logger
from rich import print
import asyncio
import time
import gc
import sys
import argparse

from utils.config import (
    TASK_CANCEL_WAIT_TIMEOUT,
    LINGERING_TASK_CLEANUP_TIMEOUT,
    AGGRESSIVE_TASK_CLEANUP_TIMEOUT,
    DEMO_SIMULATED_DELAY,
)

from utils.task_management import (
    wait_with_cancellation,
    cleanup_lingering_tasks,
    propagate_cancellation,
    TaskTracker,
)
from utils.async_cleanup import cancel_and_wait

# Configure logger
logger.setup_root(level="INFO", show_filename=True)


class MockWorker:
    """
    A simple mock worker that simulates a task with artificial delays
    and proper cancellation support.
    """

    def __init__(self, name, delay_seconds=DEMO_SIMULATED_DELAY):
        self.name = name
        self.delay_seconds = delay_seconds
        self.cancel_event = asyncio.Event()
        self.completion_event = asyncio.Event()
        self.task_tracker = TaskTracker()

        # Progress tracking
        self.progress = {
            "stage": "initialized",
            "delay_progress": "0%",
            "completed": False,
            "cancellation_source": None,
        }

    async def run(self):
        """
        Run a simulated task that properly handles cancellation.

        Returns:
            Dict with result data if successful, None if cancelled
        """
        self.progress["stage"] = "starting"

        try:
            # Log that we're starting the delayed operation
            logger.info(f"Starting task {self.name} with {self.delay_seconds}s delay")
            print(f"🕒 Running task {self.name} with {self.delay_seconds}s delay...")

            # Split the delay into small chunks to check for cancellation
            for i in range(self.delay_seconds * 2):
                # Check for cancellation through events or task cancellation
                if self.cancel_event.is_set() or asyncio.current_task().cancelled():
                    # Record cancellation source for better logging
                    if self.cancel_event.is_set():
                        self.progress["cancellation_source"] = "cancel_event"
                    else:
                        self.progress["cancellation_source"] = "task_cancelled"

                    logger.warning(
                        f"Cancellation detected during delay for {self.name} "
                        f"(source: {self.progress['cancellation_source']})"
                    )
                    print(f"⚠️ Cancellation detected during delay for {self.name}")

                    # Raise cancellation error to properly handle cancellation
                    raise asyncio.CancelledError("Cancellation during delay")

                # Update progress
                self.progress["delay_progress"] = (
                    f"{(i+1)/(self.delay_seconds*2)*100:.0f}%"
                )

                # Cancellation checkpoints - yield control to allow cancellation
                await asyncio.sleep(0.5)  # Sleep for a small chunk of time
                await asyncio.sleep(0)  # Explicitly yield control

            # After delay, complete the "work"
            self.progress["stage"] = "work_complete"
            logger.info(f"Task {self.name} completed successfully")

            # Return some mock result data
            result = {
                "task_name": self.name,
                "duration": self.delay_seconds,
                "timestamp": time.time(),
            }

            self.progress["completed"] = True
            return result

        except asyncio.CancelledError:
            # Handle cancellation properly
            self.progress["stage"] = "cancelled"
            logger.warning(
                f"Task {self.name} was cancelled during {self.progress['stage']} "
                f"at progress {self.progress['delay_progress']}"
            )
            print(f"✗ Task {self.name} was cancelled")

            # Re-raise to ensure proper cancellation handling
            raise

        finally:
            # Always signal completion in finally block
            if not self.completion_event.is_set():
                self.completion_event.set()
                logger.debug(f"Set completion event for {self.name}")


async def demonstrate_event_based_cancellation():
    """
    Demonstrates pure event-based cancellation without relying on timeouts.
    This shows a more robust approach that can replace timeout-based mechanisms.
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATING EVENT-BASED CANCELLATION (MDC TIER 1)")
    print("=" * 70)
    print(
        "This demonstrates how to control task execution using events rather than timeouts."
    )

    # Track tasks for leak detection
    tasks_before = len(asyncio.all_tasks())
    logger.info(f"Event-based cancellation demo starting with {tasks_before} tasks")

    # Create a worker
    worker = MockWorker("event-demo-task")

    # Start the task
    print(f"Starting event-controlled task...")
    task = asyncio.create_task(worker.run())

    # Add to task tracker
    worker.task_tracker.add(task)

    try:
        # Let it run for 1 second
        await asyncio.sleep(1)
        logger.info(f"Task running, current stage: {worker.progress['stage']}")

        # Cancel through the event mechanism (MDC Tier 1 practice)
        print(f"🛑 Cancelling through event mechanism...")
        logger.debug(f"Setting cancel event for task {worker.name}")
        worker.cancel_event.set()

        # Wait for completion using event-based approach (MDC Tier 1)
        completion_future = asyncio.create_task(
            wait_with_cancellation(
                task,
                completion_event=worker.completion_event,
                timeout=5.0,  # Fallback timeout
            )
        )

        # Wait for completion
        if await completion_future:
            if task.done() and not task.cancelled():
                result = task.result()
                print(f"Task completed successfully with result: {result}")
        else:
            print(f"Task was cancelled through event mechanism as expected")
            # Ensure the task is cancelled
            if not task.done():
                logger.debug(f"Cancelling task after wait completion")
                await cancel_and_wait(task, timeout=TASK_CANCEL_WAIT_TIMEOUT)

    except asyncio.CancelledError:
        logger.warning(f"Event-based cancellation demo was cancelled")
        print(f"✓ Task was cancelled through parent task cancellation")

    except Exception as e:
        logger.error(f"Error during event-based cancellation: {str(e)}")
        print(f"Error: {str(e)}")

    finally:
        # Ensure proper cleanup
        if not worker.completion_event.is_set():
            worker.completion_event.set()

        if not worker.cancel_event.is_set():
            worker.cancel_event.set()

        # Clean up any lingering tasks
        success_count, error_count = await worker.task_tracker.cancel_all()
        if success_count > 0 or error_count > 0:
            logger.info(f"Cancelled {success_count} tasks, {error_count} failed")

        # Run system-wide cleanup
        await cleanup_lingering_tasks()

        # Check for task leakage
        tasks_after = len(asyncio.all_tasks())
        if tasks_after > tasks_before:
            logger.warning(
                f"Task leakage detected: {tasks_after - tasks_before} more tasks"
            )
        else:
            logger.info(f"No task leakage in event-based cancellation demo")

    # Show final state
    print(f"Final operation state: {worker.progress['stage']}")
    print(f"Event-based cancellation demonstration complete")


async def demonstrate_cancellation_propagation():
    """
    Demonstrates cancellation propagation from parent to child tasks.
    This shows how to properly structure task hierarchies.
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATING CANCELLATION PROPAGATION (MDC TIER 1)")
    print("=" * 70)
    print("This demonstrates how cancellation propagates through parent/child tasks.")

    # Track tasks for leak detection
    tasks_before = len(asyncio.all_tasks())
    logger.info(f"Cancellation propagation demo starting with {tasks_before} tasks")

    # Create a parent cancel event and task tracker
    parent_cancel_event = asyncio.Event()
    task_tracker = TaskTracker()

    # Create child workers with different delays
    workers = [
        MockWorker("child-task-1", delay_seconds=3),
        MockWorker("child-task-2", delay_seconds=4),
        MockWorker("child-task-3", delay_seconds=5),
    ]

    child_tasks = []

    try:
        # Start all workers
        print("Starting multiple child tasks...")
        for worker in workers:
            task = asyncio.create_task(worker.run())
            child_tasks.append(task)
            worker.task_tracker.add(task)

            # Add to our main task tracker too
            task_tracker.add(task)

            # Set up propagation from parent cancel event to each worker
            propagation_task = asyncio.create_task(
                propagate_cancellation(
                    parent_event=parent_cancel_event, child_events=[worker.cancel_event]
                )
            )
            task_tracker.add(propagation_task)

        # Let tasks run for a bit
        await asyncio.sleep(1.5)

        # Trigger cancellation at the parent level
        print("🛑 Cancelling all child tasks through parent event...")
        parent_cancel_event.set()

        # Wait a bit for propagation
        await asyncio.sleep(0.5)

        # Check if all cancel events were properly set
        print("\nChecking cancellation propagation status:")
        for i, worker in enumerate(workers):
            is_cancelled = worker.cancel_event.is_set()
            print(
                f"Worker {i+1} ({worker.name}): Cancel event {'✓' if is_cancelled else '✗'}"
            )

    except Exception as e:
        logger.error(f"Error during cancellation propagation demo: {str(e)}")
        print(f"Error: {str(e)}")

    finally:
        # Clean up all tasks
        success_count, error_count = await task_tracker.cancel_all()
        logger.info(
            f"Cancelled {success_count} tasks, {error_count} failed during cleanup"
        )

        # Ensure all worker completion events are set
        for worker in workers:
            if not worker.completion_event.is_set():
                worker.completion_event.set()

        # Global cleanup
        await cleanup_lingering_tasks()

        # Check for task leakage
        tasks_after = len(asyncio.all_tasks())
        if tasks_after > tasks_before:
            logger.warning(
                f"Task leakage detected: {tasks_after - tasks_before} more tasks"
            )
        else:
            logger.info(f"No task leakage in cancellation propagation demo")

    print("Cancellation propagation demonstration complete")


async def demonstrate_task_tracking():
    """
    Demonstrates task tracking and cleanup to prevent leaks.
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATING TASK TRACKING AND CLEANUP (MDC TIER 1)")
    print("=" * 70)
    print("This demonstrates how to track tasks and clean them up properly.")

    # Track tasks for leak detection
    tasks_before = len(asyncio.all_tasks())
    logger.info(f"Task tracking demo starting with {tasks_before} tasks")

    # Create a task tracker
    tracker = TaskTracker()

    # Create several tasks
    print("Creating multiple tasks...")
    tasks = []

    # Create some dummy tasks that sleep for different times
    for i in range(5):

        async def dummy_task(task_id, sleep_time):
            try:
                print(f"Task {task_id} started, will run for {sleep_time}s")
                await asyncio.sleep(sleep_time)
                print(f"Task {task_id} completed")
                return task_id
            except asyncio.CancelledError:
                print(f"Task {task_id} was cancelled")
                raise

        # Create and track the task
        task = asyncio.create_task(dummy_task(i, 2 + i))
        tracker.add(task)
        tasks.append(task)

    # Let some tasks run to completion
    print("Letting tasks run for 3 seconds...")
    await asyncio.sleep(3)

    # Check how many tasks are still tracked
    print(f"Tasks still being tracked: {tracker.get_count()}")

    # Cancel remaining tasks
    print("Cancelling remaining tasks...")
    success_count, error_count = await tracker.cancel_all()
    print(f"Successfully cancelled {success_count} tasks, {error_count} failed")

    # Check tracking status after cancellation
    print(f"Tasks still being tracked after cancellation: {tracker.get_count()}")

    # Final cleanup
    await cleanup_lingering_tasks()

    # Check for task leakage
    tasks_after = len(asyncio.all_tasks())
    if tasks_after > tasks_before:
        print(f"⚠️ Task leakage detected: {tasks_after - tasks_before} more tasks")
    else:
        print(f"✓ No task leakage detected")

    print("Task tracking demonstration complete")


async def main():
    """
    Main function that orchestrates the demonstration.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Task Management Demonstration")
    parser.add_argument(
        "--debug", action="store_true", help="Enable DEBUG level logging"
    )
    parser.add_argument(
        "--only",
        choices=["event", "propagation", "tracking"],
        help="Run only the specified demonstration",
    )
    args = parser.parse_args()

    # Set logging level if debug is enabled
    if args.debug:
        logger.setup_root(level="DEBUG", show_filename=True)

    print("\n" + "=" * 70)
    print("TASK MANAGEMENT UTILITIES DEMONSTRATION")
    print("=" * 70)
    print(
        "This script demonstrates best practices for task management using the new utils/task_management.py."
    )
    print("Each demo focuses on a critical (Tier 1) aspect of proper task handling.")

    # Record all running tasks at start for leak detection
    tasks_at_start = len(asyncio.all_tasks())
    logger.info(f"Starting with {tasks_at_start} active tasks")

    # Run demonstrations based on user selection
    if args.only is None or args.only == "event":
        await demonstrate_event_based_cancellation()

    if args.only is None or args.only == "propagation":
        await demonstrate_cancellation_propagation()

    if args.only is None or args.only == "tracking":
        await demonstrate_task_tracking()

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
