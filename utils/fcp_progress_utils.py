#!/usr/bin/env python3
"""
Progress display utilities for the Failover Composition Priority (FCP) mechanism.
"""

import time
from utils.logger_setup import logger
from typing import Callable, TypeVar, Any
from rich.progress import Progress, SpinnerColumn, TextColumn

T = TypeVar("T")


def with_progress(
    operation: Callable[..., T],
    message: str = "Processing...",
    force_progress: bool = False,
    **kwargs,
) -> T:
    """
    Execute an operation with a progress bar that respects the logger's configuration.

    This function uses logger.progress() from logger_setup.py which automatically
    determines whether to show progress based on the current log level, unless overridden.

    Args:
        operation: Function to execute with progress tracking
        message: Message to display in the progress bar
        force_progress: If True, show progress regardless of log level
        **kwargs: Arguments to pass to the operation

    Returns:
        The result of the operation
    """
    import sys

    start_time = time.time()
    result = None

    # Add debug output about progress decision - use sys.stdout directly
    sys.stdout.write(
        f"DEBUG: with_progress called with force_progress={force_progress}, logger.level={logger.level}\n"
    )
    sys.stdout.flush()

    if force_progress:
        # Show progress regardless of log level using direct progress
        sys.stdout.write("DEBUG: Using direct progress display (bypassing logger)\n")
        sys.stdout.flush()

        # Create progress bar components
        from rich.progress import SpinnerColumn, TextColumn

        # Use our direct_progress function that bypasses logger
        with direct_progress(
            SpinnerColumn(),
            TextColumn(f"[bold green]{message}"),
            transient=True,
        ) as progress:
            task = progress.add_task("Processing...", total=None)
            result = operation(**kwargs)
            progress.update(task, completed=100)
    else:
        # Use logger.progress which automatically handles visibility based on log level
        sys.stdout.write("DEBUG: Using logger.progress for conditional display\n")
        sys.stdout.flush()
        with logger.progress(transient=True) as progress:
            task = progress.add_task(f"[bold green]{message}", total=None)
            result = operation(**kwargs)
            progress.update(task, completed=100)

    elapsed_time = time.time() - start_time
    logger.debug(f"Operation completed in {elapsed_time:.2f} seconds")

    return result


def configure_log_level(verbose: int, quiet: bool = False) -> str:
    """
    Configure the logger level based on verbose and quiet flags.

    This function is designed to be used with CLI tools that have
    verbose and quiet flags to control logging output.

    Args:
        verbose: Verbosity level (0=ERROR, 1=WARNING, 2=INFO, 3=DEBUG)
        quiet: If True, set to CRITICAL level regardless of verbose

    Returns:
        str: The log level that was set
    """
    # Map verbose levels to logging levels
    level_map = {
        0: "ERROR",
        1: "WARNING",
        2: "INFO",
        3: "DEBUG",
    }

    # Clamp verbose between 0 and 3
    verbose = max(0, min(verbose, 3))

    # If quiet is set, use CRITICAL regardless of verbose
    if quiet:
        log_level = "CRITICAL"
    else:
        log_level = level_map.get(verbose, "INFO")

    # Set the logger level
    logger.setLevel(log_level)
    logger.info(f"Log level set to {log_level}")

    return log_level


def direct_progress(*args, **kwargs):
    """
    Create a Progress object directly without going through logger's mechanisms.
    This function always shows a progress bar regardless of log level.

    Args:
        *args: Arguments to pass to Progress constructor
        **kwargs: Keyword arguments to pass to Progress constructor

    Returns:
        Progress: A rich Progress object
    """
    from rich.progress import Progress, SpinnerColumn, TextColumn

    # Default components if none provided
    if not args:
        args = (SpinnerColumn(), TextColumn("[bold green]Processing..."))

    return Progress(*args, **kwargs)
