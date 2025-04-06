#!/usr/bin/env python
"""
Tracing script that intercepts function calls to help identify where the hanging occurs.
This adds detailed logging to track the sequence of function calls during cleanup.
"""

import asyncio
import functools
import inspect
import os
import sys
import time
import traceback
from datetime import datetime

from utils.logger_setup import logger

# Configure logger for detailed tracing
logger.setLevel("DEBUG")

# List of modules to trace
TRACE_MODULES = [
    "core.data_source_manager",
    "core.vision_data_client",
    "core.rest_data_client",
    "utils.async_cleanup",
    "utils.network_utils",
]

# Set to True to follow the full sequence of calls
TRACE_FULL_CALL_SEQUENCE = True

# Dictionary to store original functions
original_functions = {}


def async_trace_decorator(func):
    """Decorator for tracing async function calls and returns."""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        module_name = func.__module__
        func_name = func.__qualname__

        # Get the caller frame info
        frame = inspect.currentframe().f_back
        caller_info = (
            f"{frame.f_code.co_filename}:{frame.f_lineno}" if frame else "unknown"
        )

        # Generate a unique ID for this call
        call_id = id(args[0]) if args else 0

        logger.debug(
            f"TRACE {datetime.now().strftime('%H:%M:%S.%f')[:-3]} >> CALL {module_name}.{func_name} [id:{call_id}] from {caller_info}"
        )

        try:
            start_time = time.time()
            result = await func(*args, **kwargs)
            elapsed = time.time() - start_time

            # Log successful completion
            logger.debug(
                f"TRACE {datetime.now().strftime('%H:%M:%S.%f')[:-3]} << RETURN {module_name}.{func_name} [id:{call_id}] after {elapsed:.3f}s"
            )
            return result
        except Exception as e:
            # Log exceptions
            logger.debug(
                f"TRACE {datetime.now().strftime('%H:%M:%S.%f')[:-3]} !! EXCEPTION in {module_name}.{func_name} [id:{call_id}]: {type(e).__name__}: {e}"
            )
            raise

    return wrapper


def trace_decorator(func):
    """Decorator for tracing synchronous function calls and returns."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        module_name = func.__module__
        func_name = func.__qualname__

        # Get the caller frame info
        frame = inspect.currentframe().f_back
        caller_info = (
            f"{frame.f_code.co_filename}:{frame.f_lineno}" if frame else "unknown"
        )

        # Generate a unique ID for this call
        call_id = id(args[0]) if args else 0

        logger.debug(
            f"TRACE {datetime.now().strftime('%H:%M:%S.%f')[:-3]} >> CALL {module_name}.{func_name} [id:{call_id}] from {caller_info}"
        )

        try:
            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time

            # Log successful completion
            logger.debug(
                f"TRACE {datetime.now().strftime('%H:%M:%S.%f')[:-3]} << RETURN {module_name}.{func_name} [id:{call_id}] after {elapsed:.3f}s"
            )
            return result
        except Exception as e:
            # Log exceptions
            logger.debug(
                f"TRACE {datetime.now().strftime('%H:%M:%S.%f')[:-3]} !! EXCEPTION in {module_name}.{func_name} [id:{call_id}]: {type(e).__name__}: {e}"
            )
            raise

    return wrapper


def apply_tracing():
    """Apply tracing decorators to targeted modules."""
    logger.info("Applying function call tracing...")

    # Import all modules that need tracing
    for module_name in TRACE_MODULES:
        try:
            logger.debug(f"Importing module for tracing: {module_name}")
            module = __import__(module_name, fromlist=["*"])

            # Get all classes in the module
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and obj.__module__ == module_name:
                    logger.debug(f"Tracing class: {obj.__name__}")

                    # Iterate through all methods of the class
                    for method_name, method in inspect.getmembers(obj):
                        # Skip private methods, dunder methods, and properties
                        if method_name.startswith("_") and not (
                            method_name.startswith("__") and method_name.endswith("__")
                        ):
                            continue

                        if inspect.isfunction(method) or inspect.ismethod(method):
                            # Store original method
                            original_functions[
                                (module_name, obj.__name__, method_name)
                            ] = method

                            # Apply appropriate decorator based on whether it's an async method
                            if inspect.iscoroutinefunction(method):
                                logger.debug(
                                    f"Tracing async method: {obj.__name__}.{method_name}"
                                )
                                decorated = async_trace_decorator(method)
                            else:
                                logger.debug(
                                    f"Tracing sync method: {obj.__name__}.{method_name}"
                                )
                                decorated = trace_decorator(method)

                            # Replace the original method
                            setattr(obj, method_name, decorated)

            # Special focus on specific functions that might be related to hanging
            if module_name == "utils.async_cleanup":
                # Trace the direct_resource_cleanup function specifically
                if hasattr(module, "direct_resource_cleanup"):
                    logger.debug("Tracing direct_resource_cleanup function")
                    original = module.direct_resource_cleanup
                    original_functions[
                        ("utils.async_cleanup", None, "direct_resource_cleanup")
                    ] = original
                    module.direct_resource_cleanup = async_trace_decorator(original)

                # Also trace cleanup_client and close_resource_with_timeout
                if hasattr(module, "cleanup_client"):
                    logger.debug("Tracing cleanup_client function")
                    original = module.cleanup_client
                    original_functions[
                        ("utils.async_cleanup", None, "cleanup_client")
                    ] = original
                    module.cleanup_client = async_trace_decorator(original)

                if hasattr(module, "close_resource_with_timeout"):
                    logger.debug("Tracing close_resource_with_timeout function")
                    original = module.close_resource_with_timeout
                    original_functions[
                        ("utils.async_cleanup", None, "close_resource_with_timeout")
                    ] = original
                    module.close_resource_with_timeout = async_trace_decorator(original)

        except ImportError as e:
            logger.error(f"Error importing module {module_name}: {e}")

    logger.info("Tracing applied successfully")


def run_diagnostic_script():
    """Run the diagnostic script with tracing enabled."""
    # Apply tracing first
    apply_tracing()

    # Now import and run the diagnostic script
    logger.info("Running diagnostic script with tracing enabled...")
    try:
        from scripts import diagnose_hanging

        asyncio.run(diagnose_hanging.main())
    except Exception as e:
        logger.error(f"Error running diagnostic script: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    # Add parent directory to path to import diagnose_hanging
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    # Run the diagnostic script with tracing enabled
    run_diagnostic_script()
