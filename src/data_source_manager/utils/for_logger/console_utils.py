#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Console utilities for the logger system.

This module provides console and rich-related utilities for the logger system.
"""

import builtins
import logging
import sys


class NoOpProgress:
    """No-operation progress bar for when rich output is suppressed."""

    def __enter__(self) -> "NoOpProgress":
        """Enter the context manager, returning self."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        """Exit the context manager, doing nothing."""
        pass

    def add_task(self, *_args, **_kwargs):
        """No-op implementation of add_task that simply returns a task ID of 0."""
        return 0

    def update(self, *_args, **_kwargs):
        """No-op implementation of update that does nothing."""


def should_show_rich_output() -> bool:
    """Determine if rich Progress and print should be displayed based on log level.

    Returns:
        bool: True if the current log level allows rich output (DEBUG, INFO, WARNING),
              False if the current log level is ERROR or CRITICAL.
    """
    # Get the root logger level
    root_level = logging.getLogger().level

    # Allow rich output for DEBUG, INFO, WARNING levels
    # Suppress rich output for ERROR and CRITICAL levels
    return root_level < logging.ERROR


def enable_smart_print(enabled: bool = True, console: object | None = None) -> bool:
    """Enable or disable smart print that respects log level settings.

    When enabled, the built-in print function is monkey-patched to use console.print,
    making it respect the current log level (prints for DEBUG, INFO, WARNING;
    suppresses for ERROR, CRITICAL) while providing rich object rendering.

    Args:
        enabled (bool): Whether to enable (True) or disable (False) smart print
        console: The rich console instance to use for printing

    Returns:
        bool: True if successful
    """
    if enabled:
        # Store original print function if not already stored
        if not hasattr(builtins, "_original_print"):
            builtins._original_print = builtins.print

        # Replace built-in print with a function that uses logger.console.print
        def smart_print(*args, **kwargs):
            if should_show_rich_output():
                # Use our shared console for consistent rendering
                # Extract file and end parameters as they're not supported by console.print
                file = kwargs.pop("file", None)

                # If output is being redirected to a file (like in exception handling),
                # use the original print function
                if file is not None and file is not sys.stdout and file is not sys.stderr:
                    if hasattr(builtins, "_original_print"):
                        builtins._original_print(*args, **kwargs)
                    return

                # Otherwise use the console to print
                if console:
                    console.print(*args, **kwargs)
                else:
                    # Fallback if no console is provided
                    from rich.console import Console

                    temp_console = Console()
                    temp_console.print(*args, **kwargs)

        # Replace the built-in print
        builtins.print = smart_print

        # Use debug level message to not appear in higher log levels
        if logging.getLogger().level <= logging.DEBUG:
            logging.debug("Smart print enabled - print statements now respect log level")

        # Always show this message regardless of level
        if logging.getLogger().level >= logging.ERROR and hasattr(builtins, "_original_print"):
            # For ERROR and CRITICAL, use the original print function to show a message
            builtins._original_print("Smart print enabled - print output will be suppressed at current log level")
    # Restore original print if we have it stored
    elif hasattr(builtins, "_original_print"):
        builtins.print = builtins._original_print
        if logging.getLogger().level <= logging.DEBUG:
            logging.debug("Smart print disabled - print statements restored to normal")

    return True


def create_rich_progress(*args, **kwargs):
    """Create a rich.progress.Progress instance only if the current log level allows rich output.

    This function returns a Progress object when the log level is DEBUG, INFO, or WARNING,
    but returns a no-op context manager when the log level is ERROR or CRITICAL.

    Args:
        *args: Positional arguments to pass to rich.progress.Progress
        **kwargs: Keyword arguments to pass to rich.progress.Progress

    Returns:
        Context manager: Either a Progress object or a no-op context manager
    """
    if should_show_rich_output():
        try:
            # Import locally to avoid circular imports
            from rich.progress import Progress

            return Progress(*args, **kwargs)
        except ImportError:
            # Fallback if rich is not available
            return NoOpProgress()

    # Return a no-op context manager when output should be suppressed
    return NoOpProgress()


def get_console(highlight: bool = False) -> object:
    """Get a rich.console.Console instance for direct rendering of rich objects.

    Args:
        highlight (bool): Whether to enable syntax highlighting

    Returns:
        rich.console.Console: A Console instance for direct rendering
    """
    try:
        from rich.console import Console

        return Console(highlight=highlight)
    except ImportError:
        # Create a minimal console-like object if rich is not available
        class MinimalConsole:
            def print(self, *args, **kwargs):
                if hasattr(builtins, "_original_print"):
                    builtins._original_print(*args)
                else:
                    print(*args)

        return MinimalConsole()
