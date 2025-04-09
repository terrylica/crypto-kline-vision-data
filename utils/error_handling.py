#!/usr/bin/env python

"""
Error handling utilities for data verification scripts.

This module provides standardized error handling patterns to simplify
verification scripts and make them more readable.
"""

import asyncio
import contextlib
import logging
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union
import pandas as pd

from utils.logger_setup import logger
from utils.async_cleanup import cancel_and_wait
from rich import print

T = TypeVar("T")
U = TypeVar("U")


@contextlib.contextmanager
def capture_warnings():
    """Context manager to capture warnings while preserving normal logging.

    Yields:
        List[str]: List of captured warning messages
    """
    warnings = []
    original_warning_fn = logger.warning
    original_debug_fn = logger.debug

    def warning_capture(*args, **kwargs):
        message = args[0] if args else kwargs.get("msg", "")
        warnings.append(message)

        # For curl_cffi task warnings, use debug level instead to reduce noise
        if "_force_timeout" in message or "curl_cffi" in message:
            return original_debug_fn(*args, **kwargs)
        else:
            return original_warning_fn(*args, **kwargs)

    try:
        logger.warning = warning_capture
        yield warnings
    finally:
        logger.warning = original_warning_fn


@contextlib.contextmanager
def suppress_consolidation_warnings():
    """Context manager to suppress specific consolidation warnings during verification tests.

    This temporarily filters out warnings about unconsolidated data that are expected
    during verification testing without affecting other important warnings.
    """
    # Store original log levels to restore later
    original_levels = {}
    for logger_name in ["core.rest_data_client", "core.data_source_manager"]:
        logger_instance = logging.getLogger(logger_name)
        original_levels[logger_name] = logger_instance.level

    # Temporarily increase log level for specific loggers to hide consolidation warnings
    for logger_name in ["core.rest_data_client", "core.data_source_manager"]:
        logging.getLogger(logger_name).setLevel(logging.ERROR)

    try:
        yield
    finally:
        # Restore original log levels
        for logger_name, level in original_levels.items():
            logging.getLogger(logger_name).setLevel(level)


async def with_timeout_handling(
    coro: Callable[..., T],
    timeout: float,
    operation_name: str,
    *args: Any,
    **kwargs: Any,
) -> Tuple[Optional[T], float]:
    """Execute an operation with timeout handling and return elapsed time.

    Args:
        coro: Coroutine function to execute
        timeout: Timeout in seconds
        operation_name: Name of operation for logging
        *args: Arguments to pass to the coroutine
        **kwargs: Keyword arguments to pass to the coroutine

    Returns:
        Tuple of (result, elapsed_time) or (None, elapsed_time) if failed
    """
    try:
        # Start timing the operation
        start_op = asyncio.get_event_loop().time()

        # Create a task for the operation
        task = asyncio.create_task(coro(*args, **kwargs))

        # Wait for the task with timeout protection
        result = await asyncio.wait_for(task, timeout=timeout)

        # Calculate elapsed time
        elapsed = asyncio.get_event_loop().time() - start_op

        return result, elapsed
    except asyncio.TimeoutError:
        elapsed = asyncio.get_event_loop().time() - start_op
        logger.error(
            f"Timeout in {operation_name} after {elapsed:.2f}s (limit: {timeout}s)"
        )
        return None, elapsed
    except Exception as e:
        elapsed = asyncio.get_event_loop().time() - start_op
        logger.error(f"Error in {operation_name}: {str(e)}")
        return None, elapsed


async def safe_execute_verification(
    verification_func: Callable[..., T], name: str, *args: Any, **kwargs: Any
) -> Optional[T]:
    """Execute a verification function with comprehensive error handling.

    Args:
        verification_func: Async verification function to execute
        name: Name of the verification for logging
        *args: Arguments to pass to the verification function
        **kwargs: Keyword arguments to pass to the verification function

    Returns:
        The result of the verification function, or None if it failed
    """
    tasks_at_start = len(asyncio.all_tasks())
    logger.info(f"Starting {name} verification with {tasks_at_start} active tasks")

    try:
        result = await verification_func(*args, **kwargs)
        return result
    except Exception as e:
        logger.error(f"Error during {name} verification: {str(e)}")
        return None
    finally:
        # Check for leaked tasks
        tasks_after = len(asyncio.all_tasks())
        if tasks_after > tasks_at_start:
            logger.warning(
                f"Task leakage in {name}: {tasks_after - tasks_at_start} more tasks at end than at start"
            )
        else:
            logger.info(
                f"{name} verification completed with {tasks_after} active tasks"
            )


async def execute_with_task_cleanup(
    coro: Callable[..., T],
    timeout: float,
    operation_name: str,
    *args: Any,
    **kwargs: Any,
) -> Optional[T]:
    """Execute a coroutine with proper task cleanup on failure.

    Args:
        coro: Coroutine function to execute
        timeout: Timeout in seconds
        operation_name: Name of operation for logging
        *args: Arguments to pass to the coroutine
        **kwargs: Keyword arguments to pass to the coroutine

    Returns:
        Result of the coroutine or None if failed
    """
    task = None
    try:
        # Create a task for the operation
        task = asyncio.create_task(coro(*args, **kwargs))

        # Wait for the task with timeout protection
        return await asyncio.wait_for(task, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"Timeout in {operation_name}")
        # Clean up the task safely
        if task and not task.done():
            await cancel_and_wait(task)
        return None
    except Exception as e:
        logger.error(f"Error in {operation_name}: {str(e)}")
        # Clean up the task safely
        if task and not task.done():
            await cancel_and_wait(task)
        return None


def display_df_summary(df: pd.DataFrame, label: str = "DataFrame") -> None:
    """Display just the first and last record of a DataFrame.

    Args:
        df: DataFrame to display
        label: Description label for the DataFrame
    """
    if df.empty:
        print(f"{label}: Empty DataFrame")
        return

    print(f"{label} Summary ({len(df)} records):")
    print(f"First record ({df.index[0]}):")
    print(df.iloc[0:1])
    print("...")
    print(f"Last record ({df.index[-1]}):")
    print(df.iloc[-1:])


async def cleanup_tasks(tasks: List[asyncio.Task], timeout: float = 5.0) -> None:
    """Safely clean up a list of tasks.

    Args:
        tasks: List of tasks to clean up
        timeout: Maximum time to wait for tasks to complete
    """
    for task in tasks:
        if not task.done():
            await cancel_and_wait(task, timeout=timeout)
