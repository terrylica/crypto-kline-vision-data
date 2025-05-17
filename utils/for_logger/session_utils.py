#!/usr/bin/env python3
"""
Session logging utilities.

This module provides utilities for configuring session-specific logging.
"""

import builtins
import time
from pathlib import Path

import pendulum


def configure_session_logging(session_name, log_level="DEBUG", logger=None):
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
        logger: The logger object to use

    Returns:
        tuple: (main_log_path, error_log_path, timestamp) for reference
    """
    if logger is None:
        from utils.logger_setup import logger as default_logger

        logger = default_logger

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

    # Configure logging
    logger.setLevel(log_level)
    logger.add_file_handler(str(main_log_path), level=log_level, mode="w", strip_rich_markup=True)
    logger.enable_error_logging(str(error_log_path))

    # Verify log files exist
    # Wait a short time for file handlers to flush
    time.sleep(0.1)
    # Check and log file status
    main_exists = Path(main_log_path).exists()
    error_exists = Path(error_log_path).exists()
    main_size = Path(main_log_path).stat().st_size if main_exists else 0
    error_size = Path(error_log_path).stat().st_size if error_exists else 0

    # Use original print to ensure this message gets through regardless of log level
    if hasattr(builtins, "_original_print"):
        builtins._original_print(f"Main log created: {main_exists}, size: {main_size} bytes")
        builtins._original_print(f"Error log created: {error_exists}, size: {error_size} bytes")

    # Log initialization
    logger.info(f"Session logging initialized for {session_name}")
    logger.debug(f"Log level: {log_level}")
    logger.debug(f"Main log: {main_log_path}")
    logger.debug(f"Error log: {error_log_path}")

    return str(main_log_path), str(error_log_path), timestamp
