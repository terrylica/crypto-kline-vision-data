#!/usr/bin/env python
"""
Utility script for fixing hanging curl_cffi client issues.

This script provides functions that can be imported to fix hanging curl_cffi clients
by proactively nullifying _curlm references that cause circular dependencies.
"""

import asyncio
import gc
import inspect
import time
import traceback
from typing import Any, Optional, List, Dict, Set

from utils.logger_setup import logger

# Timeout for cleanup operations
CLEANUP_TIMEOUT = 1.0


def fix_client_reference(obj: Any, max_depth: int = 2, current_depth: int = 0) -> bool:
    """
    Recursively fix _curlm references in an object's attributes.

    Args:
        obj: The object to fix
        max_depth: Maximum recursion depth
        current_depth: Current recursion depth

    Returns:
        True if any reference was fixed, False otherwise
    """
    if obj is None or current_depth > max_depth:
        return False

    # Check if this is a curl_cffi client with _curlm reference
    fixed = False
    if hasattr(obj, "_curlm") and obj._curlm is not None:
        logger.debug(f"Found _curlm reference in {type(obj).__name__}, nullifying it")
        try:
            obj._curlm = None
            fixed = True
        except Exception as e:
            logger.warning(f"Error nullifying _curlm: {e}")

    # Special case for AsyncCurl objects with _timeout_handle
    if hasattr(obj, "_timeout_handle") and obj._timeout_handle is not None:
        logger.debug(f"Found _timeout_handle in {type(obj).__name__}, nullifying it")
        try:
            obj._timeout_handle = None
            fixed = True
        except Exception as e:
            logger.warning(f"Error nullifying _timeout_handle: {e}")

    # Check first-level attributes
    if current_depth < max_depth:
        # Only check common attributes that might contain clients
        client_attr_names = [
            "_client",
            "client",
            "session",
            "http_client",
            "rest_client",
            "vision_client",
        ]

        for attr_name in client_attr_names:
            if hasattr(obj, attr_name):
                attr = getattr(obj, attr_name)
                if attr is not None and attr is not obj:  # Avoid infinite recursion
                    if fix_client_reference(attr, max_depth, current_depth + 1):
                        fixed = True

    return fixed


async def fix_and_close_client(client: Any, timeout: float = CLEANUP_TIMEOUT) -> bool:
    """
    Fix and close a client that might be hanging.

    Args:
        client: The client to fix and close
        timeout: Maximum time to wait for closing

    Returns:
        True if the client was successfully fixed and closed, False otherwise
    """
    if client is None:
        return False

    start_time = time.time()
    logger.debug(f"Fixing and closing client of type {type(client).__name__}")

    # First fix any _curlm references
    fixed = fix_client_reference(client)

    # Then force garbage collection to break circular references
    gc.collect()

    # Finally, try to close the client
    try:
        if hasattr(client, "aclose") and inspect.iscoroutinefunction(client.aclose):
            try:
                await asyncio.wait_for(client.aclose(), timeout=timeout)
                logger.debug(
                    f"Successfully closed client with aclose() in {time.time() - start_time:.3f}s"
                )
                return True
            except asyncio.TimeoutError:
                logger.warning(f"aclose() timed out after {timeout}s")
            except Exception as e:
                logger.warning(f"Error in aclose(): {e}")

        elif hasattr(client, "close") and inspect.iscoroutinefunction(client.close):
            try:
                await asyncio.wait_for(client.close(), timeout=timeout)
                logger.debug(
                    f"Successfully closed client with close() in {time.time() - start_time:.3f}s"
                )
                return True
            except asyncio.TimeoutError:
                logger.warning(f"close() timed out after {timeout}s")
            except Exception as e:
                logger.warning(f"Error in close(): {e}")

        else:
            logger.warning("Client has no compatible close method")
            return fixed  # Return True if we at least fixed references

    except Exception as e:
        logger.error(f"Unexpected error fixing client: {e}")
        traceback.print_exc()

    return False


async def find_force_timeout_tasks() -> List[asyncio.Task]:
    """
    Find all force_timeout tasks that might be hanging.

    Returns:
        List of tasks that are related to force_timeout
    """
    force_timeout_tasks = []
    for task in asyncio.all_tasks():
        task_str = str(task)
        # Look specifically for _force_timeout tasks
        if "_force_timeout" in task_str and not task.done():
            force_timeout_tasks.append(task)

    return force_timeout_tasks


async def fix_all_pending_tasks(timeout: float = 2.0) -> int:
    """
    Find and fix all pending tasks related to curl_cffi.

    Args:
        timeout: Maximum time to wait for all tasks to complete

    Returns:
        Number of fixed tasks
    """
    # First, specifically target _force_timeout tasks
    force_timeout_tasks = await find_force_timeout_tasks()
    if force_timeout_tasks:
        logger.warning(
            f"Found {len(force_timeout_tasks)} hanging _force_timeout tasks, cancelling them first"
        )
        for task in force_timeout_tasks:
            task.cancel()

        # Wait for cancellation to complete with timeout
        try:
            await asyncio.wait_for(
                asyncio.gather(*force_timeout_tasks, return_exceptions=True),
                timeout=timeout / 2,  # Use half the timeout for these critical tasks
            )
            logger.debug(
                f"Successfully cancelled {len(force_timeout_tasks)} _force_timeout tasks"
            )
        except asyncio.TimeoutError:
            logger.warning(
                f"Timeout waiting for _force_timeout tasks to cancel, proceeding anyway"
            )

    # Then find other curl_cffi tasks
    other_curl_tasks = [
        t
        for t in asyncio.all_tasks()
        if ("AsyncCurl" in str(t) or "curl_cffi" in str(t))
        and not t.done()
        and t not in force_timeout_tasks  # Exclude already processed tasks
    ]

    total_fixed = len(force_timeout_tasks)

    if other_curl_tasks:
        logger.info(
            f"Found {len(other_curl_tasks)} other pending curl_cffi tasks, attempting to fix"
        )

        # Cancel all curl tasks
        for task in other_curl_tasks:
            task.cancel()

        # Wait for cancellation to complete with timeout
        try:
            await asyncio.wait_for(
                asyncio.gather(*other_curl_tasks, return_exceptions=True),
                timeout=timeout / 2,
            )
            logger.debug(
                f"Successfully cancelled {len(other_curl_tasks)} other curl tasks"
            )
            total_fixed += len(other_curl_tasks)
        except asyncio.TimeoutError:
            logger.warning(
                f"Timeout waiting for curl tasks to cancel after {timeout/2}s"
            )

    # Force garbage collection
    gc.collect()

    return total_fixed


async def find_curl_references_in_memory() -> int:
    """
    Attempt to find and clean curl_cffi references in memory.

    Returns:
        Number of references fixed
    """
    fixed_count = 0
    # Get all objects in memory
    all_objects = gc.get_objects()

    # Look for curl_cffi related objects
    for obj in all_objects:
        obj_type = type(obj).__name__
        if "_curl" in str(obj_type).lower() or "asynccurl" in str(obj_type).lower():
            # Try to fix this object
            if fix_client_reference(obj, max_depth=1):
                fixed_count += 1

    if fixed_count > 0:
        logger.info(f"Fixed {fixed_count} curl_cffi references in memory")

    return fixed_count


async def emergency_cleanup():
    """
    Perform emergency cleanup when the application is hanging.

    This function attempts to break circular references, cancel pending tasks,
    and force garbage collection to unblock the application.
    """
    logger.warning("Starting emergency cleanup procedure")

    # First, specifically find and fix any force_timeout tasks
    force_timeout_tasks = await find_force_timeout_tasks()
    if force_timeout_tasks:
        logger.warning(f"Found {len(force_timeout_tasks)} hanging _force_timeout tasks")
        for task in force_timeout_tasks:
            task.cancel()

    # Find and fix all pending tasks
    fixed_tasks = await fix_all_pending_tasks()

    # Try to find curl references directly in memory
    fixed_refs = await find_curl_references_in_memory()

    # Force garbage collection multiple times
    for i in range(3):
        collected = gc.collect()
        logger.debug(f"Garbage collection pass {i+1}: collected {collected} objects")

    logger.info(
        f"Emergency cleanup completed: fixed {fixed_tasks} tasks and {fixed_refs} references"
    )

    return fixed_tasks


if __name__ == "__main__":
    # If run directly, perform emergency cleanup
    async def main():
        print("Running emergency cleanup to fix hanging curl_cffi clients...")
        fixed = await emergency_cleanup()
        print(f"Fixed {fixed} hanging tasks.")

    asyncio.run(main())
