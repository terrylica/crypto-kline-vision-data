#!/usr/bin/env python
"""
Demonstration of enhanced logger features without importing standard logging.

This module illustrates the enhanced logger_setup.py implementation that eliminates
the need to import the standard logging module directly, while providing all
of its functionality via a clean, chainable API.
"""

from utils.logger_setup import logger
from rich import print
from pathlib import Path
import time
from examples.logger_demo.test_modules import do_task_a, do_task_b

# Configure the root logger with method chaining
logger.setup_root(level="DEBUG").show_filename(True).use_rich(True)

# Ensure logs directory exists
log_dir = Path("logs/logger_example")
log_dir.mkdir(parents=True, exist_ok=True)

# Add file handlers without importing logging module
main_log = log_dir / f"main_{time.strftime('%Y%m%d_%H%M%S')}.log"
error_log = log_dir / f"error_{time.strftime('%Y%m%d_%H%M%S')}.log"

# Add file handlers with a single call
logger.add_file_handler(str(main_log), level="DEBUG", mode="w")
logger.enable_error_logging(str(error_log))


def main():
    """Execute enhanced logger demonstration sequence."""
    # Log messages at different levels
    logger.debug("This is a debug message - detailed information")
    logger.info("This is an info message - general information")
    logger.warning("This is a warning message - potential issue")
    logger.error("This is an error message - actual problem")
    logger.critical("This is a critical message - major problem")

    # Method chaining for concise logging
    (
        logger.info("Starting process")
        .debug("Initializing components")
        .info("Components initialized")
    )

    # Demonstrate level constants without importing logging
    if logger.INFO < logger.WARNING:
        logger.info("INFO level is lower than WARNING level")

    # Demonstrate custom logger
    custom_logger = logger.get_logger("custom_component")
    custom_logger.info("This message comes from a custom logger")

    # Call task functions from the test_modules
    result_a = do_task_a()
    result_b = do_task_b()
    logger.info(f"Process completed with results: {result_a}, {result_b}")

    # Log file paths and completion
    logger.info(f"Main log file: {main_log}")
    logger.info(f"Error log file: {error_log}")
    logger.info("Example completed successfully")

    # Return results for testing
    return "Enhanced logging demo completed"


if __name__ == "__main__":
    main()

    # Display implementation usage pattern
    print("\n[bold green]Enhanced Logger Implementation Usage:[/bold green]")
    print("1. Module-level import pattern:")
    print("   [yellow]from utils.logger_setup import logger[/yellow]")
    print("   [yellow]from rich import print  # Optional[/yellow]")
    print("\n2. Application entrypoint configuration:")
    print('   [yellow]logger.setup_root(level="DEBUG").show_filename(True)[/yellow]')
    print("\n3. File handler without importing logging:")
    print('   [yellow]logger.add_file_handler("log_file.log", level="DEBUG")[/yellow]')
    print("\n4. Level constants without importing logging:")
    print("   [yellow]if level >= logger.ERROR:  # Instead of logging.ERROR[/yellow]")
    print("\n5. Documentation:")
    print("   [bold]See examples/logger_demo/README_ENHANCED.md for details[/bold]")
