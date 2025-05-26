#!/usr/bin/env python3
"""
Advanced Logging System for Data Source Manager

This module provides a sophisticated logging system that extends Python's standard
logging facilities with additional features:

Key Features:
- Automatic module name detection in log messages
- Source file and line tracking for accurate debugging
- Method chaining support for cleaner code
- Rich-powered logging for improved readability in terminals
- Dedicated timeout and error logging subsystems
- Smart print functionality that respects log levels

The central component is the `logger` object which serves as a drop-in replacement
for the standard logging module with enhanced capabilities.

Basic Usage Examples:
    # Import the logger
    from utils.logger_setup import logger

    # Basic logging with auto-detected module name
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")

    # Chainable logging API
    logger.info("First message").debug("Second message")

    # Rich formatting (if available)
    logger.info("Status: [bold green]SUCCESS[/bold green]")

Advanced Usage:
    # Configure log level
    logger.setLevel(logger.DEBUG)  # Or using string: "DEBUG"

    # Switch between rich and standard logging
    logger.use_rich(True)

    # Log timeouts to a dedicated file
    log_timeout("database_query", 5.0, details={"query_id": "abc123"})

    # Enable error logging to a separate file
    enable_error_logging("/path/to/errors.log")

    # Make print statements respect log levels
    enable_smart_print()
"""

import logging
import os

from utils.for_logger.console_utils import (
    enable_smart_print as _enable_smart_print,
)

# Import utility modules
from utils.for_logger.custom_logger import CustomLogger
from utils.for_logger.error_logger import (
    enable_error_logging as _enable_error_logging,
)
from utils.for_logger.error_logger import (
    get_error_log_file as _get_error_log_file,
)
from utils.for_logger.error_logger import (
    set_error_log_file as _set_error_log_file,
)
from utils.for_logger.logger_proxy import LoggerProxy
from utils.for_logger.logger_setup_utils import (
    DEFAULT_LEVEL,
    RICH_AVAILABLE,
)
from utils.for_logger.logger_setup_utils import (
    get_module_logger as _get_module_logger_internal,
)
from utils.for_logger.logger_setup_utils import (
    use_rich_logging as _use_rich_logging,
)
from utils.for_logger.session_utils import (
    configure_session_logging as _configure_session_logging,
)
from utils.for_logger.timeout_logger import (
    log_timeout as _log_timeout,
)
from utils.for_logger.timeout_logger import (
    set_timeout_log_file as _set_timeout_log_file,
)

# Register our custom logger class
logging.setLoggerClass(CustomLogger)

# Global state tracking
_root_configured = False
_module_loggers = {}
_use_rich = os.environ.get("USE_RICH_LOGGING", "true").lower() in ("true", "1", "yes")

# Store state in a dictionary instead of using global variables
_logger_state = {
    "root_configured": False,
    "module_loggers": {},
    "use_rich": os.environ.get("USE_RICH_LOGGING", "true").lower() in ("true", "1", "yes"),
}

# Rich console instance
if RICH_AVAILABLE:
    from rich.console import Console
    from rich.traceback import install as install_rich_traceback

    console = Console(highlight=False)  # Disable syntax highlighting to preserve markup
    install_rich_traceback(show_locals=True)


def _setup_root_logger(level=None, use_rich=None):
    """
    Configure the root logger with specified level and colorized output.
    """
    # Determine whether to use rich
    if use_rich is None:
        use_rich = _logger_state["use_rich"]
    else:
        _logger_state["use_rich"] = use_rich

    root_logger = logging.getLogger()
    root_logger.handlers.clear()  # Clear existing handlers

    # Set level
    log_level = (level or DEFAULT_LEVEL).upper()
    level_int = getattr(logging, log_level)
    root_logger.setLevel(level_int)

    # Set up handler based on preference and availability
    if use_rich and RICH_AVAILABLE:
        # Use rich handler
        from utils.for_logger.logger_setup_utils import setup_rich_handler

        handler = setup_rich_handler(console)
    else:
        # Use colorlog handler (fallback or default)
        handler = logging.StreamHandler()
        from utils.for_logger.formatters import create_colored_formatter

        formatter = create_colored_formatter()
        handler.setFormatter(formatter)

    root_logger.addHandler(handler)

    # Update existing loggers
    for name in logging.root.manager.loggerDict:
        logging.getLogger(name).setLevel(level_int)

    # Add a marker attribute to the root logger to indicate it's configured
    root_logger._root_configured = True
    _logger_state["root_configured"] = True
    return root_logger


def get_module_logger(name=None, level=None, setup_root=False, use_rich=None):
    """
    Get a module logger with the specified name and level.

    Args:
        name (str, optional): Logger name. If None, auto-detected from caller
        level (str, optional): Logging level
        setup_root (bool): Whether to set up the root logger
        use_rich (bool): Whether to use rich logging

    Returns:
        logging.Logger: The requested logger instance
    """
    return _get_module_logger_internal(
        name,
        level,
        _logger_state["module_loggers"],
        setup_root,
        use_rich,
        _setup_root_logger,
    )


def use_rich_logging(enable=True, level=None):
    """
    Enable or disable Rich logging.

    Parameters:
        enable (bool): True to enable Rich logging, False to use standard colorlog.
        level (str, optional): Logging level to set when reconfiguring.

    Returns:
        bool: True if Rich logging was enabled, False otherwise.
    """
    # Use the extracted function with our root logger setup function
    _logger_state["use_rich"] = _use_rich_logging(enable, level, _setup_root_logger)
    return _logger_state["use_rich"]


# Create the auto-detecting logger proxy
logger = LoggerProxy(
    get_module_logger_fn=get_module_logger,
    setup_root_logger_fn=_setup_root_logger,
    use_rich=_logger_state["use_rich"],
)

# Enable rich logging by default for best experience
if RICH_AVAILABLE:
    use_rich_logging(True)

# Enable smart print by default to support rich object rendering
logger.enable_smart_print(True)

# Public API functions


def enable_error_logging(error_log_file=None):
    """Enable logging of all errors, warnings, and critical messages to a dedicated file.

    Args:
        error_log_file (str, optional): Path to the error log file.
                                       If None, uses the default path.

    Returns:
        bool: True if successful
    """
    return _enable_error_logging(error_log_file, _logger_state["root_configured"])


def enable_smart_print(enabled=True):
    """
    Enable or disable the smart print feature that makes all print statements
    respect log level settings.

    When enabled, the built-in print function is monkey-patched to use rich print,
    making it respect the current log level (prints for DEBUG, INFO, WARNING; suppresses for ERROR, CRITICAL).

    Args:
        enabled (bool): Whether to enable (True) or disable (False) smart print

    Returns:
        bool: True if successful
    """
    return _enable_smart_print(enabled, logger.console if hasattr(logger, "console") else None)


def get_error_log_file():
    """Get the current error log file path.

    Returns:
        str: Path to the error log file
    """
    return _get_error_log_file()


def log_timeout(
    operation: str,
    timeout_value: float,
    module_name: str | None = None,
    details: dict | None = None,
):
    """Log a timeout event to the dedicated timeout log file.

    Args:
        operation: Description of the operation that timed out
        timeout_value: The timeout value in seconds that was breached
        module_name: Optional name of the module where the timeout occurred
        details: Optional dictionary with additional details about the operation
    """
    return _log_timeout(operation, timeout_value, module_name, details, get_module_logger)


def set_error_log_file(path):
    """Set the file path for error logging.

    Args:
        path (str): Path to the log file

    Returns:
        bool: True if successful
    """
    return _set_error_log_file(path)


def set_timeout_log_file(path: str):
    """Set the file path for timeout logging.

    Args:
        path: Path to the log file

    Returns:
        bool: True if successful
    """
    return _set_timeout_log_file(path)


def configure_session_logging(session_name, log_level="DEBUG"):
    """
    Configure comprehensive session logging with timestamp-based files.

    This function:
    1. Creates necessary log directories
    2. Generates timestamped log files
    3. Sets up file handlers for regular logs and errors
    4. Returns paths to log files for reference

    Args:
        session_name (str): Name of the session (used in log filenames)
        log_level (str): Logging level to use

    Returns:
        tuple: (main_log_path, error_log_path, timestamp) for reference
    """
    return _configure_session_logging(session_name, log_level, logger)


# Add these to __all__ to make them available when importing
__all__ = [
    "configure_session_logging",
    "enable_error_logging",
    "enable_smart_print",
    "get_error_log_file",
    "log_timeout",
    "logger",
    "set_error_log_file",
    "set_timeout_log_file",
]

# Test logger if run directly
if __name__ == "__main__":
    # Test the proxy logger with conventional syntax
    print("\nTesting logger with file/line detection:")
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")

    # Test with different logging levels
    print("\nTesting logger with different levels:")
    logger.setLevel("WARNING")
    logger.debug("Debug message (should not appear)")
    logger.info("Info message (should not appear)")
    logger.warning("Warning message (should appear)")

    # Reset to DEBUG
    logger.setLevel("DEBUG")

    # Test rich logging if available
    if RICH_AVAILABLE:
        print("\nTesting Rich logging:")
        logger.use_rich(True)
        logger.info("Info with [bold green]Rich formatting[/bold green]")

        try:
            # Fix B018: Use a variable assignment to prevent "useless expression" warning
            result = 1 / 0  # This will raise ZeroDivisionError
        except Exception:
            logger.exception("Exception with Rich traceback")

        # Switch back to colorlog
        print("\nSwitching back to colorlog:")
        logger.use_rich(False)
    else:
        print("\nRich library not installed, skipping Rich logging tests")

    # Test method chaining
    print("\nTesting method chaining:")
    logger.debug("First message").info("Second message").warning("Third message")

    # Test creating and writing to a temporary file handler
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as temp:
        temp_path = temp.name

    logger.add_file_handler(temp_path)
    logger.info(f"Test log message to file: {temp_path}")
    print(f"\nWrote test log to temporary file: {temp_path}")

    # Clean up
    import os

    os.remove(temp_path)
