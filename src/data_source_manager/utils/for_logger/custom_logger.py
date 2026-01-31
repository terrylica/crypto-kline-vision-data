#!/usr/bin/env python3
"""Custom logger classes.

This module provides custom logger classes for the logger system.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""

import inspect
import logging
import traceback


class CustomLogger(logging.Logger):
    """Custom logger class that correctly identifies the actual source of log messages.

    This class overrides the standard findCaller method to properly identify where
    log messages are actually coming from in the application code, rather than showing
    the logging infrastructure's files (like logger_setup.py) as the source.

    This is especially important when:
    1. Using the show_filename feature to display source file information
    2. Working with proxy loggers or wrapper functions
    3. Building logging frameworks that should be "invisible" to the end user

    The findCaller method skips through frames in the logging module and this module
    to find the actual application code that initiated the logging call.
    """

    def findCaller(
        self, stack_info: bool = False, stacklevel: int = 1
    ) -> tuple[str, int, str, str | None]:
        """Find the stack frame of the caller.

        This customizes the stack level search to skip past the LoggerProxy class
        and standard logging infrastructure to find the actual application caller.
        This ensures that when filename display is enabled, the correct source file
        is shown rather than the logging infrastructure files.

        Args:
            stack_info: If True, collect stack trace information
            stacklevel: How many frames to skip

        Returns:
            tuple: (filename, line number, function name, stack info)
        """
        # Get current stack frames
        try:
            # Get the stack frames
            frame_records = inspect.stack()

            # Skip frames based on stacklevel (default is 1 in logging)
            # We need to skip more frames to account for the logger proxy and other infrastructure
            adjusted_level = stacklevel + 4  # Skip extra frames for our infrastructure

            if len(frame_records) <= adjusted_level:
                return "(unknown file)", 0, "(unknown function)", None

            # Get the relevant frame based on adjusted stacklevel
            caller_frame = frame_records[adjusted_level]

            # Extract file, line, and function information
            fn = caller_frame.filename
            lno = caller_frame.lineno
            func = caller_frame.function

            # Get stack info if requested
            sinfo = None
            if stack_info:
                sinfo = "".join(traceback.format_stack(frame_records[adjusted_level][0]))

            return fn, lno, func, sinfo

        except (IndexError, AttributeError, ValueError):
            return "(unknown file)", 0, "(unknown function)", None
