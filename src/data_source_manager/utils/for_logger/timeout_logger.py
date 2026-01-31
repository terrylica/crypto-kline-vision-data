#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Timeout logger functionality.

This module provides functions for logging timeout events to a dedicated log file
and the standard logger.
"""

import inspect
import logging
import os

# Default timeout log file
DEFAULT_TIMEOUT_LOG_FILE = "./logs/timeout_incidents/timeout_log.txt"


class TimeoutLoggerState:
    """Singleton class that manages timeout logger state, avoiding global variables."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._timeout_log_file = os.environ.get("TIMEOUT_LOG_FILE", DEFAULT_TIMEOUT_LOG_FILE)
            cls._instance._timeout_logger_configured = False
            cls._instance._timeout_logger = None
        return cls._instance

    @property
    def timeout_log_file(self):
        return self._timeout_log_file

    @timeout_log_file.setter
    def timeout_log_file(self, value):
        self._timeout_log_file = value

    @property
    def timeout_logger_configured(self):
        return self._timeout_logger_configured

    @timeout_logger_configured.setter
    def timeout_logger_configured(self, value):
        self._timeout_logger_configured = value

    @property
    def timeout_logger(self):
        return self._timeout_logger

    @timeout_logger.setter
    def timeout_logger(self, value):
        self._timeout_logger = value


# Create singleton instance
_state = TimeoutLoggerState()


def configure_timeout_logger() -> logging.Logger:
    """Configure the timeout logger for dedicated timeout logging.

    Used for analyzing performance issues.

    Returns:
        logging.Logger: The configured timeout logger
    """
    if _state.timeout_logger_configured:
        return _state.timeout_logger

    # Create directory if it doesn't exist
    log_dir = os.path.dirname(_state.timeout_log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # Set up file handler
    timeout_logger = logging.getLogger("timeout_logger")
    timeout_logger.setLevel(logging.INFO)

    # Create a FileHandler that appends to the timeout log file
    handler = logging.FileHandler(_state.timeout_log_file, mode="a")

    # Create a formatter that includes timestamp, module name, and message
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)

    # Add the handler to the logger
    timeout_logger.handlers.clear()
    timeout_logger.addHandler(handler)

    # Set flag and store logger
    _state.timeout_logger = timeout_logger
    _state.timeout_logger_configured = True

    return timeout_logger


def log_timeout(
    operation: str,
    timeout_value: float,
    module_name: str | None = None,
    details: dict | None = None,
    get_module_logger: object | None = None,
) -> bool:
    """Log a timeout event to the dedicated timeout log file.

    Args:
        operation: Description of the operation that timed out
        timeout_value: The timeout value in seconds that was breached
        module_name: Optional name of the module where the timeout occurred
        details: Optional dictionary with additional details about the operation
        get_module_logger: Function to get a module logger (required if module_name is not None)

    Returns:
        bool: True if successful
    """
    # Configure logger if needed
    configure_timeout_logger()

    # Auto-detect module name if not provided
    if module_name is None:
        frame = inspect.currentframe().f_back
        module = inspect.getmodule(frame)
        module_name = module.__name__ if module else "__main__"

    # Format the details
    details_str = ""
    if details:
        details_str = " | " + " | ".join([f"{k}={v}" for k, v in details.items()])

    # Create the log message
    message = f"TIMEOUT EXCEEDED: {operation} (limit: {timeout_value}s){details_str}"

    # Log to the timeout log
    _state.timeout_logger.error(message)

    # Log to the module-specific logger if a getter function is provided
    if get_module_logger:
        module_logger = get_module_logger(module_name)
        module_logger.error(message)

    # Return True to indicate the message was logged
    return True


def set_timeout_log_file(path: str) -> bool:
    """Set the file path for timeout logging.

    Args:
        path: Path to the log file

    Returns:
        bool: True if successful
    """
    _state.timeout_log_file = path

    # Reset the logger so it will be reconfigured with the new path
    if _state.timeout_logger_configured:
        _state.timeout_logger_configured = False
        _state.timeout_logger = None

    return True


def get_timeout_log_file() -> str:
    """Get the current timeout log file path.

    Returns:
        str: Path to the timeout log file
    """
    return _state.timeout_log_file
