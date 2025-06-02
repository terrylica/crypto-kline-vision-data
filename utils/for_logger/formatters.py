#!/usr/bin/env python3
"""
Formatters for the logger system.

This module provides formatters for the logger system to reduce code
duplication and improve maintainability.
"""

import logging
import re

from colorlog import ColoredFormatter

# Default color scheme
DEFAULT_LOG_COLORS = {
    "DEBUG": "cyan",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red,bg_white",
}

# Format strings
# Base format without filename
FORMAT_BASE = "%(log_color)s%(levelname)-8s%(reset)s %(name)s: %(message)s"

# Format with custom file/line information from our proxy
CUSTOM_FORMAT_WITH_FILENAME = (
    "%(log_color)s%(levelname)-8s%(reset)s %(name)s: %(message)s%(blue)s "
    "[%(cyan)s%(source_file)s%(blue)s:%(yellow)s%(source_line)s%(blue)s]%(reset)s"
)

# Rich format (Rich handles the styling)
RICH_FORMAT = "%(message)s"


def create_colored_formatter(format_string=CUSTOM_FORMAT_WITH_FILENAME):
    """
    Create a ColoredFormatter with default colors and format.

    Args:
        format_string: Format string to use

    Returns:
        ColoredFormatter: A configured formatter instance
    """
    return ColoredFormatter(
        format_string,
        log_colors=DEFAULT_LOG_COLORS,
        secondary_log_colors={
            "filename": {
                "DEBUG": "cyan",
                "INFO": "cyan",
                "WARNING": "cyan",
                "ERROR": "cyan",
                "CRITICAL": "cyan",
            },
            "lineno": {
                "DEBUG": "yellow",
                "INFO": "yellow",
                "WARNING": "yellow",
                "ERROR": "yellow",
                "CRITICAL": "yellow",
            },
        },
        style="%",
        reset=True,
    )


class RichMarkupStripper(logging.Filter):
    """Filter that strips Rich markup from log messages."""

    def __init__(self):
        """Initialize the filter with the Rich markup pattern."""
        super().__init__()
        self.rich_markup_pattern = re.compile(r"\[(.*?)\]")

    def filter(self, record):
        """
        Strip Rich markup from the log record.

        Args:
            record: Log record to filter

        Returns:
            bool: Always returns True to allow the record
        """
        if isinstance(record.msg, str):
            # Remove Rich markup tags
            record.msg = self.rich_markup_pattern.sub("", record.msg)

            # Also strip markup from any values in the record.args tuple if it exists
            if record.args and isinstance(record.args, tuple):
                args_list = list(record.args)
                for i, arg in enumerate(args_list):
                    if isinstance(arg, str):
                        args_list[i] = self.rich_markup_pattern.sub("", arg)
                record.args = tuple(args_list)

            # Also strip markup from any extra values
            if hasattr(record, "source_file") and isinstance(record.source_file, str):
                record.source_file = self.rich_markup_pattern.sub("", record.source_file)
            if hasattr(record, "source_line") and isinstance(record.source_line, str):
                record.source_line = self.rich_markup_pattern.sub("", record.source_line)
        return True


class ErrorFilter(logging.Filter):
    """Filter that only allows records with level WARNING or higher."""

    def filter(self, record):
        """
        Filter log records based on level.

        Args:
            record: Log record to filter

        Returns:
            bool: True if record level is WARNING or higher
        """
        return record.levelno >= logging.WARNING
