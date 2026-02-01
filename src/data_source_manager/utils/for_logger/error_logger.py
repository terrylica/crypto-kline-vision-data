#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Error logger functionality.

This module provides functions for logging errors to a dedicated log file.
"""

import logging
import os

from data_source_manager.utils.for_logger.formatters import ErrorFilter, RichMarkupStripper

# Default error log file
DEFAULT_ERROR_LOG_FILE = "./logs/error_logs/error_log.txt"


class ErrorLoggerState:
    """Singleton class that manages error logger state, avoiding global variables."""

    _instance = None

    def __new__(cls) -> "ErrorLoggerState":
        """Create or return the singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._error_log_file = os.environ.get("ERROR_LOG_FILE", DEFAULT_ERROR_LOG_FILE)
            cls._instance._error_logger_configured = False
            cls._instance._error_logger = None
            cls._instance._error_logging_enabled = False
        return cls._instance

    @property
    def error_log_file(self):
        """Return the error log file path."""
        return self._error_log_file

    @error_log_file.setter
    def error_log_file(self, value):
        self._error_log_file = value

    @property
    def error_logger_configured(self):
        """Return whether error logger is configured."""
        return self._error_logger_configured

    @error_logger_configured.setter
    def error_logger_configured(self, value):
        self._error_logger_configured = value

    @property
    def error_logger(self):
        """Return the error logger instance."""
        return self._error_logger

    @error_logger.setter
    def error_logger(self, value):
        self._error_logger = value

    @property
    def error_logging_enabled(self):
        """Return whether error logging is enabled."""
        return self._error_logging_enabled

    @error_logging_enabled.setter
    def error_logging_enabled(self, value):
        self._error_logging_enabled = value


# Create singleton instance
_state = ErrorLoggerState()


def configure_error_logger() -> logging.Logger:
    """Configure the dedicated error logger for monitoring and troubleshooting.

    Handles error, warning, and critical level events separately from main logging.

    Returns:
        logging.Logger: The configured error logger
    """
    if _state.error_logger_configured:
        return _state.error_logger

    # Create directory if it doesn't exist
    log_dir = os.path.dirname(_state.error_log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # Set up file handler
    error_logger = logging.getLogger("error_logger")
    error_logger.setLevel(logging.WARNING)  # Capture WARNING and above
    error_logger.propagate = False  # Don't propagate to the root logger

    # Create a FileHandler that appends to the error log file
    handler = logging.FileHandler(_state.error_log_file, mode="a")
    handler.setLevel(logging.WARNING)  # Only WARNING, ERROR, and CRITICAL

    # Create a formatter that includes timestamp, module name, and message
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s (%(filename)s:%(lineno)d)",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    # Add filter to strip Rich markup
    handler.addFilter(RichMarkupStripper())

    # Add the handler to the logger
    error_logger.handlers.clear()
    error_logger.addHandler(handler)

    # Set flag and store logger
    _state.error_logger = error_logger
    _state.error_logger_configured = True

    return error_logger


def enable_error_logging(error_log_file: str | None = None, root_configured: bool = False) -> bool:
    """Enable logging of all errors, warnings, and critical messages to a dedicated file.

    This configures a separate logger that captures all WARNING, ERROR, and CRITICAL
    level messages from all modules and writes them to a centralized log file.

    Args:
        error_log_file: Path to the error log file. If None, uses the default path.
        root_configured: Whether the root logger is configured

    Returns:
        bool: True if successful
    """
    # Update log file path if provided
    if error_log_file:
        _state.error_log_file = error_log_file
        # Reset the logger so it will be reconfigured with the new path
        if _state.error_logger_configured:
            _state.error_logger_configured = False
            _state.error_logger = None

    # Configure error logger
    configure_error_logger()

    # Set up root logger handler to forward error messages
    if root_configured and "logging" in globals():
        root_logger = logging.getLogger()

        # Create a handler that sends to error logger
        handler = logging.Handler()
        handler.setLevel(logging.WARNING)
        handler.addFilter(ErrorFilter())

        # Create the formatter
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s (%(filename)s:%(lineno)d)",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        # Custom emit method to send to error log file
        def custom_emit(record):
            if _state.error_logger:
                _state.error_logger.handle(record)

        handler.emit = custom_emit

        # Add handler to root logger
        root_logger.addHandler(handler)

    _state.error_logging_enabled = True
    return True


def get_error_log_file() -> str:
    """Get the current error log file path.

    Returns:
        str: Path to the error log file
    """
    return _state.error_log_file


def set_error_log_file(path: str) -> bool:
    """Set the file path for error logging.

    Args:
        path: Path to the log file

    Returns:
        bool: True if successful
    """
    _state.error_log_file = path

    # Reset the logger so it will be reconfigured with the new path
    if _state.error_logger_configured:
        _state.error_logger_configured = False
        _state.error_logger = None

    return True
