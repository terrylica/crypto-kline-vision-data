"""
Demonstration of hierarchical logger configuration with module isolation.

This module illustrates the application entrypoint configuration pattern,
establishing a global logging threshold that propagates to imported modules.
"""

# Standard module-level logger import
from utils.logger_setup import logger
from examples.logger_demo.test_modules import do_task_a, do_task_b

# Configure the logging hierarchy with ERROR threshold
# Environment variable LOG_LEVEL overrides this configuration when present
logger.setLevel("ERROR")


def main():
    """Execute primary demonstration sequence with cross-module logging."""
    # Log messages at progressive severity levels
    logger.debug("Debug message - only visible with DEBUG level")
    logger.info("Info message - application starting")
    logger.warning("Warning message - resource usage high")
    logger.error("Error message - operation failed")
    logger.critical("Critical message - system compromise detected")

    # Invoke functions from imported modules to demonstrate hierarchy propagation
    result_a = do_task_a()
    result_b = do_task_b()

    # Demonstrate fluent interface pattern via method chaining
    logger.info(f"Process completed with results: {result_a}, {result_b}").debug(
        "Additional debug details - only visible in DEBUG mode"
    ).critical("Final critical notice - process terminating")


if __name__ == "__main__":
    main()

    # Display implementation usage pattern
    print("\nLogger Implementation Usage:")
    print("1. Module-level import pattern:")
    print("   from utils.logger_setup import logger")
    print("\n2. Application entrypoint configuration:")
    print('   logger.setLevel("DEBUG")  # Hierarchy threshold configuration')
    print("\n3. Environment variable precedence:")
    print("   LOG_LEVEL=INFO python examples/logger_demo/main_example.py")
