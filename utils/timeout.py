"""Timeout utilities for preventing operations from hanging indefinitely.

This module provides timeout functionality with diagnostics to help identify
operations that are taking too long or hanging indefinitely.
"""

import asyncio
import contextlib
import functools
import sys
import threading
import time
import traceback
from typing import Any, Callable, Optional, TypeVar

from utils.logger_setup import logger
from utils.config import MAX_TIMEOUT

T = TypeVar("T")


class TimeoutError(Exception):
    """Exception raised when an operation times out."""

    def __init__(
        self, operation: str, timeout: float, stack_trace: Optional[str] = None
    ):
        """Initialize TimeoutError.

        Args:
            operation: Description of the operation that timed out
            timeout: Timeout in seconds
            stack_trace: Optional stack trace at the time of the timeout
        """
        self.operation = operation
        self.timeout = timeout
        self.stack_trace = stack_trace
        super().__init__(f"Operation '{operation}' timed out after {timeout} seconds")


@contextlib.contextmanager
def timeout(seconds: float, operation_name: str = "unnamed operation"):
    """Context manager that raises TimeoutError if the code block takes too long.

    Args:
        seconds: Timeout in seconds
        operation_name: Name of the operation for logging

    Raises:
        TimeoutError: If the operation takes longer than the specified timeout

    Example:
        with timeout(5, "file read"):
            with open("large_file.csv") as f:
                data = f.read()
    """
    timer = threading.Timer(
        seconds,
        lambda: sys.stderr.write(
            f"TIMEOUT WARNING: {operation_name} is taking longer than {seconds}s\n"
        ),
    )
    timer.daemon = True
    timer.start()

    start_time = time.time()

    try:
        yield
    finally:
        timer.cancel()
        elapsed = time.time() - start_time
        if elapsed > seconds * 0.8:  # Log if we used more than 80% of our time budget
            logger.warning(
                f"Operation '{operation_name}' took {elapsed:.2f}s (timeout: {seconds}s)"
            )


async def with_timeout(
    func: Callable[..., T], timeout_seconds: float, *args: Any, **kwargs: Any
) -> T:
    """Execute an async function with a timeout.

    Args:
        func: Async function to execute
        timeout_seconds: Timeout in seconds
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function

    Returns:
        Result of the function

    Raises:
        TimeoutError: If the operation takes longer than the specified timeout

    Example:
        result = await with_timeout(fetch_data, 5, url)
    """
    func_name = getattr(func, "__name__", str(func))

    try:
        return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        # Capture stack trace for diagnostic purposes
        current_stack = "".join(traceback.format_stack())

        # Log the current event loop state for debugging
        all_tasks = asyncio.all_tasks()
        pending_tasks = [t for t in all_tasks if not t.done()]

        logger.error(f"Timeout in {func_name} after {timeout_seconds}s")
        logger.error(
            f"Event loop has {len(pending_tasks)}/{len(all_tasks)} pending tasks"
        )

        # Log details of the first few pending tasks
        for i, task in enumerate(pending_tasks[:5]):
            task_str = str(task)
            if len(task_str) > 500:
                task_str = task_str[:500] + "..."
            logger.error(f"Pending task {i+1}: {task_str}")

        # Raise a more informative exception
        raise TimeoutError(func_name, timeout_seconds, current_stack)


def timeout_decorator(timeout_seconds: Optional[float] = None):
    """Decorator to apply timeout to an async function.

    Args:
        timeout_seconds: Timeout in seconds (defaults to MAX_TIMEOUT)

    Returns:
        Decorated function

    Example:
        @timeout_decorator(5)
        async def fetch_data(url):
            # ...
    """

    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any):
            seconds = timeout_seconds or MAX_TIMEOUT
            return await with_timeout(func, seconds, *args, **kwargs)

        return wrapper

    return decorator


async def diagnose_hanging_operation(operation_name: str, detailed: bool = False):
    """Run diagnostics on the current event loop to help identify hanging operations.

    Args:
        operation_name: Name of the operation for logging
        detailed: Whether to include detailed task information

    Example:
        await diagnose_hanging_operation("metadata save")
    """
    import gc
    import psutil

    logger.warning(
        f"Running diagnostics for potentially hanging operation: {operation_name}"
    )

    # Get current tasks
    all_tasks = asyncio.all_tasks()
    pending_tasks = [t for t in all_tasks if not t.done()]
    logger.info(f"Event loop has {len(pending_tasks)}/{len(all_tasks)} pending tasks")

    # Get memory usage
    process = psutil.Process()
    memory_info = process.memory_info()
    logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")

    # Get open files
    try:
        open_files = process.open_files()
        logger.info(f"Open files: {len(open_files)}")
        if detailed:
            for i, file in enumerate(open_files[:10]):
                logger.info(f"  {i+1}. {file.path} (mode: {file.mode})")
    except Exception as e:
        logger.error(f"Could not get open files: {e}")

    # Look at pending tasks
    if detailed:
        for i, task in enumerate(pending_tasks[:5]):
            task_str = str(task)
            if len(task_str) > 1000:
                task_str = task_str[:1000] + "..."
            logger.info(f"Task {i+1}: {task_str}")

    # Run garbage collection and report results
    collected = gc.collect()
    logger.info(f"Garbage collection found {collected} objects to collect")

    return {
        "pending_tasks": len(pending_tasks),
        "memory_mb": memory_info.rss / 1024 / 1024,
        "open_files": len(open_files) if "open_files" in locals() else "unknown",
        "collected_objects": collected,
    }
