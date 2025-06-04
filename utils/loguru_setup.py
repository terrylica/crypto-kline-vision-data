#!/usr/bin/env python3
"""
Loguru-based Logging System for Data Source Manager

This module provides a simple, powerful loguru-based logging system that addresses
user complaints about log level control in the DSM package.

Key Features:
- Simple log level control via environment variables or direct API calls
- Better performance and more intuitive configuration than standard logging
- Automatic log rotation and compression
- Rich formatting support with colors
- Easy migration from utils.logger_setup

Basic Usage Examples:
    # Import the logger (drop-in replacement)
    from utils.loguru_setup import logger

    # Set log level (much simpler than standard logging)
    logger.configure_level("INFO")  # or DEBUG, WARNING, ERROR, CRITICAL

    # Or use environment variable
    # export DSM_LOG_LEVEL=DEBUG

    # All standard logging calls work
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")

    # Rich formatting works automatically
    logger.info("Status: <green>SUCCESS</green>")
    logger.error("Error: <red>FAILED</red>")

Environment Variables:
    DSM_LOG_LEVEL: Set the global log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    DSM_LOG_FILE: Optional log file path for file output
    DSM_DISABLE_COLORS: Set to "true" to disable colored output

Migration from utils.logger_setup:
    Simply change the import:
    # Old: from utils.loguru_setup import logger
    # New: from utils.loguru_setup import logger

    All existing code continues to work without changes.
"""

import os
import sys
from pathlib import Path

from loguru import logger as _loguru_logger

# Remove default loguru handler to have full control
_loguru_logger.remove()

# Configuration from environment
DEFAULT_LOG_LEVEL = os.getenv("DSM_LOG_LEVEL", "ERROR").upper()
LOG_FILE = os.getenv("DSM_LOG_FILE")
DISABLE_COLORS = os.getenv("DSM_DISABLE_COLORS", "false").lower() == "true"

# Format template with colors and module info
LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)

# Simple format for when colors are disabled
SIMPLE_FORMAT = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}"


class DSMLogger:
    """Simple wrapper around loguru that provides easy configuration and compatibility."""

    def __init__(self):
        """Initialize the DSM logger with environment-based configuration."""
        self._current_level = DEFAULT_LOG_LEVEL
        self._log_file = LOG_FILE
        self._disable_colors = DISABLE_COLORS
        self._setup_logger()

    def _setup_logger(self) -> None:
        """Set up loguru logger with current configuration."""
        # Remove any existing handlers
        _loguru_logger.remove()

        # Choose format based on color settings
        format_template = SIMPLE_FORMAT if self._disable_colors else LOG_FORMAT

        # Add console handler
        _loguru_logger.add(
            sys.stderr,
            level=self._current_level,
            format=format_template,
            colorize=not self._disable_colors,
            backtrace=True,
            diagnose=True,
        )

        # Add file handler if specified
        if self._log_file:
            log_path = Path(self._log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            _loguru_logger.add(
                str(log_path),
                level=self._current_level,
                format=format_template,
                rotation="10 MB",  # Rotate when file reaches 10MB
                retention="1 week",  # Keep logs for 1 week
                compression="zip",  # Compress rotated logs
                backtrace=True,
                diagnose=True,
            )

    def configure_level(self, level: str) -> "DSMLogger":
        """Configure the log level.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

        Returns:
            Self for method chaining
        """
        self._current_level = level.upper()
        self._setup_logger()
        return self

    def configure_file(self, log_file: str | Path | None) -> "DSMLogger":
        """Configure file logging.

        Args:
            log_file: Path to log file, or None to disable file logging

        Returns:
            Self for method chaining
        """
        self._log_file = str(log_file) if log_file else None
        self._setup_logger()
        return self

    def disable_colors(self, disable: bool = True) -> "DSMLogger":
        """Enable or disable colored output.

        Args:
            disable: Whether to disable colors

        Returns:
            Self for method chaining
        """
        self._disable_colors = disable
        self._setup_logger()
        return self

    # Delegate all logging methods to loguru
    def debug(self, message: str, *args, **kwargs):
        """Log a debug message."""
        _loguru_logger.opt(depth=1).debug(message, *args, **kwargs)
        return self

    def info(self, message: str, *args, **kwargs):
        """Log an info message."""
        _loguru_logger.opt(depth=1).info(message, *args, **kwargs)
        return self

    def warning(self, message: str, *args, **kwargs):
        """Log a warning message."""
        _loguru_logger.opt(depth=1).warning(message, *args, **kwargs)
        return self

    def error(self, message: str, *args, **kwargs):
        """Log an error message."""
        _loguru_logger.opt(depth=1).error(message, *args, **kwargs)
        return self

    def critical(self, message: str, *args, **kwargs):
        """Log a critical message."""
        _loguru_logger.opt(depth=1).critical(message, *args, **kwargs)
        return self

    def exception(self, message: str, *args, **kwargs):
        """Log an exception with traceback."""
        _loguru_logger.opt(depth=1).exception(message, *args, **kwargs)
        return self

    # Compatibility methods for existing logger interface
    def setLevel(self, level: str | int):
        """Set log level (compatibility method)."""
        if isinstance(level, int):
            # Convert numeric levels to string
            level_map = {10: "DEBUG", 20: "INFO", 30: "WARNING", 40: "ERROR", 50: "CRITICAL"}
            level = level_map.get(level, "INFO")
        return self.configure_level(level)

    def getEffectiveLevel(self) -> str:
        """Get the effective log level."""
        return self._current_level

    def isEnabledFor(self, level: str | int) -> bool:
        """Check if logging is enabled for the given level."""
        if isinstance(level, int):
            level_map = {10: "DEBUG", 20: "INFO", 30: "WARNING", 40: "ERROR", 50: "CRITICAL"}
            level = level_map.get(level, "INFO")

        level_hierarchy = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        current_index = level_hierarchy.index(self._current_level)
        check_index = level_hierarchy.index(level.upper())
        return check_index >= current_index

    # Expose loguru's advanced features
    def bind(self, **kwargs):
        """Bind additional context to logger."""
        return _loguru_logger.bind(**kwargs)

    def patch(self, patcher):
        """Patch logger with additional functionality."""
        return _loguru_logger.patch(patcher)

    def opt(self, **kwargs):
        """Configure logger options."""
        return _loguru_logger.opt(**kwargs)


# Create the global logger instance
logger = DSMLogger()


# Convenience functions for quick configuration
def configure_level(level: str):
    """Configure the global logger level.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    logger.configure_level(level)


def configure_file(log_file: str | Path | None):
    """Configure global file logging.

    Args:
        log_file: Path to log file, or None to disable file logging
    """
    logger.configure_file(log_file)


def disable_colors(disable: bool = True):
    """Enable or disable colored output globally.

    Args:
        disable: Whether to disable colors
    """
    logger.disable_colors(disable)


def configure_session_logging(session_name: str, log_level: str = "DEBUG"):
    """Configure session-specific logging with timestamped files.

    This function provides compatibility with the old logger setup by creating
    timestamped log files for a specific session.

    Args:
        session_name: Name of the session (used in log filenames)
        log_level: Logging level to use (ignored if DSM_LOG_LEVEL environment variable is set)

    Returns:
        tuple: (main_log_path, error_log_path, timestamp) for reference
    """
    import os
    from pathlib import Path

    import pendulum

    # Generate timestamp for consistent filenames
    timestamp = pendulum.now("UTC").format("YYYYMMDD_HHmmss")

    # Create log directories in workspace root
    main_log_dir = Path("logs") / f"{session_name}_logs"
    error_log_dir = Path("logs/error_logs")

    main_log_dir.mkdir(parents=True, exist_ok=True)
    error_log_dir.mkdir(parents=True, exist_ok=True)

    # Define log paths
    main_log_path = main_log_dir / f"{session_name}_{timestamp}.log"
    error_log_path = error_log_dir / f"{session_name}_errors_{timestamp}.log"

    # Respect environment variable if set, otherwise use the passed log_level
    env_log_level = os.getenv("DSM_LOG_LEVEL")
    effective_log_level = env_log_level.upper() if env_log_level else log_level.upper()

    # Configure the logger with both main and error files
    logger.configure_file(str(main_log_path))
    logger.configure_level(effective_log_level)

    # Add a separate handler for ERROR and CRITICAL messages to the error log file
    # This creates a dedicated error log file that only contains ERROR and CRITICAL messages
    format_template = SIMPLE_FORMAT if logger._disable_colors else LOG_FORMAT
    _loguru_logger.add(
        str(error_log_path),
        level="ERROR",  # Only ERROR and CRITICAL messages go to error log
        format=format_template,
        rotation="10 MB",
        retention="1 week",
        compression="zip",
        backtrace=True,
        diagnose=True,
    )

    # Log initialization
    logger.info(f"Session logging initialized for {session_name}")
    logger.debug(f"Log level: {effective_log_level}")
    logger.debug(f"Main log: {main_log_path}")
    logger.debug(f"Error log: {error_log_path}")

    return str(main_log_path), str(error_log_path), timestamp


# Example usage and testing
if __name__ == "__main__":
    # Demo script showing loguru logger capabilities
    print("=== DSM Loguru Logger Demo ===")

    # Test different log levels
    logger.configure_level("DEBUG")
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")

    print("\n=== Testing Rich Formatting ===")
    logger.info("Status: <green>SUCCESS</green>")
    logger.warning("Warning: <yellow>CAUTION</yellow>")
    logger.error("Error: <red>FAILED</red>")

    print("\n=== Testing Method Chaining ===")
    logger.configure_level("INFO").info("Configured to INFO level").warning("This warning should appear")

    print("\n=== Testing Environment Configuration ===")
    print(f"Current level: {logger.getEffectiveLevel()}")
    print(f"Log file: {LOG_FILE}")
    print(f"Colors disabled: {DISABLE_COLORS}")
