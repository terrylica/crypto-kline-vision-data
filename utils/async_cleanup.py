#!/usr/bin/env python

"""Utilities for safe cleanup of async resources in Python 3.13+.

This module provides utilities for handling resource cleanup in a way that's compatible
with Python 3.13's stricter handling of coroutines and avoids "coroutine never awaited"
warnings, hanging during cleanup, and resource leaks.

Key features:
- Timeout protection for all cleanup operations
- Error handling that prevents exceptions from propagating
- Specialized handling for HTTP clients like curl_cffi.AsyncSession
- Support for both async and sync cleanup methods
- Garbage collection forcing to help with circular references

Usage examples:

1. Basic usage in a class:

```python
from utils.async_cleanup import direct_resource_cleanup

class MyAsyncResource:
    async def __aenter__(self):
        self._client = create_client()
        self._other_resource = await create_other_resource()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await direct_resource_cleanup(
            self,
            ("_client", "HTTP client", False),
            ("_other_resource", "other resource", False),
        )
```

2. For more complex cleanup needs:

```python
from utils.async_cleanup import close_resource_with_timeout, cleanup_client

async def __aexit__(self, exc_type, exc_val, exc_tb):
    # Handle special resources first
    if hasattr(self, "_special_resource") and self._special_resource:
        # Custom cleanup logic
        self._special_resource.special_sync_cleanup()
        self._special_resource = None
        
    # Then use the utilities for standard resources
    await cleanup_client(self._client, is_external=self._client_is_external)
    await close_resource_with_timeout(
        self._other_resource, 
        timeout=0.2,  # Custom timeout
        resource_name="important resource"
    )
    
    # Force garbage collection
    gc.collect()
```

3. With custom close method:

```python
await close_resource_with_timeout(
    resource=self._websocket,
    close_method="disconnect",  # Method name other than __aexit__
    close_args=(), 
    timeout=0.5
)
```
"""

import asyncio
import gc
from typing import Any, Callable, Optional, TypeVar, Union
import inspect

from utils.logger_setup import logger
from utils.config import (
    RESOURCE_CLEANUP_TIMEOUT,
    HTTP_CLIENT_CLEANUP_TIMEOUT,
    FILE_CLEANUP_TIMEOUT,
    ENABLE_FORCED_GC,
)

logger.setLevel("DEBUG")

T = TypeVar("T")


async def close_resource_with_timeout(
    resource: Any,
    timeout: float = RESOURCE_CLEANUP_TIMEOUT,
    resource_name: str = "resource",
    close_method: str = "__aexit__",
    close_args: tuple = (None, None, None),
) -> None:
    """Close an async resource with timeout protection to prevent hanging.

    Args:
        resource: The async resource to close
        timeout: Maximum time in seconds to wait for resource cleanup (default: from config)
        resource_name: Name of the resource for logging (default: "resource")
        close_method: Name of the close method to call (default: "__aexit__")
        close_args: Arguments to pass to the close method (default: (None, None, None))
    """
    if resource is None:
        logger.debug(f"Skipping cleanup for {resource_name}: resource is None")
        return

    logger.debug(
        f"Starting cleanup for {resource_name} (type: {type(resource).__name__}) with timeout {timeout}s"
    )

    try:
        # Check if the method exists and if it's a coroutine function
        method = getattr(resource, close_method, None)
        if method is None:
            logger.debug(f"{resource_name} does not have a {close_method} method")
            return

        if inspect.iscoroutinefunction(method):
            # For async close methods
            logger.debug(
                f"Using async cleanup for {resource_name} with {close_method}()"
            )
            try:
                await asyncio.shield(
                    asyncio.wait_for(method(*close_args), timeout=timeout)
                )
                logger.debug(f"Successfully closed {resource_name}")
            except asyncio.TimeoutError:
                logger.warning(f"{resource_name} cleanup timed out after {timeout}s")
            except asyncio.CancelledError:
                logger.warning(f"{resource_name} cleanup was cancelled")
        else:
            # For synchronous close methods
            logger.debug(
                f"Using sync cleanup for {resource_name} with {close_method}()"
            )
            method(*close_args)
            logger.debug(f"Successfully closed {resource_name} (sync)")

    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        logger.debug(f"{resource_name} cleanup timed out or was cancelled: {str(e)}")
    except Exception as e:
        logger.warning(f"Error closing {resource_name}: {str(e)}")
    finally:
        logger.debug(f"Completed cleanup attempt for {resource_name}")


async def cleanup_client(
    client: Any, is_external: bool = False, timeout: float = HTTP_CLIENT_CLEANUP_TIMEOUT
) -> None:
    """Cleanup an HTTP client with timeout protection.

    Handles both curl_cffi AsyncSession and other HTTP clients.

    Args:
        client: The HTTP client to close
        is_external: If True, client won't be closed (it's managed externally)
        timeout: Maximum time in seconds to wait for client cleanup (default: from config)
    """
    if client is None:
        logger.debug("Skipping client cleanup: client is None")
        return

    if is_external:
        logger.debug("Skipping client cleanup: client is external")
        return

    client_type = type(client).__name__
    logger.debug(
        f"Starting cleanup for HTTP client (type: {client_type}) with timeout {timeout}s"
    )

    try:
        # Try direct aclose if available (curl_cffi AsyncSession)
        if hasattr(client, "aclose"):
            logger.debug(f"Found aclose() method on client (type: {client_type})")
            try:
                await asyncio.shield(asyncio.wait_for(client.aclose(), timeout=timeout))
                logger.debug("Directly closed HTTP client with aclose()")
            except asyncio.TimeoutError:
                logger.warning(f"HTTP client aclose() timed out after {timeout}s")
            except asyncio.CancelledError:
                logger.warning("HTTP client aclose() was cancelled")
        else:
            # Use our utility if available
            logger.debug(
                f"No aclose() method found, trying safely_close_client (type: {client_type})"
            )
            try:
                from utils.network_utils import safely_close_client

                # Check if the client has a _curlm attribute that might be causing issues
                if hasattr(client, "_curlm"):
                    logger.debug("Client has _curlm attribute, setting to None first")
                    client._curlm = None

                await asyncio.shield(
                    asyncio.wait_for(safely_close_client(client), timeout=timeout)
                )
                logger.debug("Safely closed HTTP client with safely_close_client()")
            except ImportError:
                logger.warning(
                    "Could not import safely_close_client from utils.network_utils"
                )
            except asyncio.TimeoutError:
                logger.warning(f"safely_close_client() timed out after {timeout}s")
            except asyncio.CancelledError:
                logger.warning("safely_close_client() was cancelled")
            except Exception as e:
                logger.warning(f"Could not safely close client: {str(e)}")

    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        logger.debug(f"HTTP client cleanup timed out or was cancelled: {str(e)}")
    except Exception as e:
        logger.warning(f"Error closing HTTP client: {str(e)}")
    finally:
        logger.debug(f"Completed cleanup attempt for HTTP client (type: {client_type})")


async def cleanup_file_handle(
    file_handle: Any, timeout: float = FILE_CLEANUP_TIMEOUT
) -> None:
    """Cleanup a file handle with timeout protection.

    Handles both sync and async file handles.

    Args:
        file_handle: The file handle to close
        timeout: Maximum time in seconds to wait for async file cleanup (default: from config)
    """
    if file_handle is None:
        return

    try:
        # Try sync close first
        if hasattr(file_handle, "close"):
            file_handle.close()
            logger.debug("Closed file handle (sync)")
            return

        # Try async close if available
        if hasattr(file_handle, "aclose"):
            await asyncio.shield(
                asyncio.wait_for(file_handle.aclose(), timeout=timeout)
            )
            logger.debug("Closed file handle (async)")
            return

        logger.debug("File handle doesn't have close/aclose method")

    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        logger.debug(f"File handle cleanup timed out or was cancelled: {str(e)}")
    except Exception as e:
        logger.warning(f"Error closing file handle: {str(e)}")


async def direct_resource_cleanup(
    instance: Any,
    *resource_tuples: tuple[str, str, bool],
    force_gc: bool = ENABLE_FORCED_GC,
) -> None:
    """Direct cleanup of resources with timeout protection.

    This is the recommended approach for cleaning up resources in __aexit__ methods,
    providing a simple way to handle cleanup of multiple resources with proper error
    handling and timeout protection.

    Args:
        instance: The object instance containing the resources
        *resource_tuples: Variable number of tuples in the format:
                         (attribute_name, resource_name_for_logs, is_external)
        force_gc: Whether to force garbage collection after cleanup

    Example:
    ```python
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await direct_resource_cleanup(
            self,
            ("_client", "HTTP client", self._client_is_external),
            ("_download_manager", "download manager", False),
        )
    ```
    """
    if instance is None:
        logger.debug("Skipping resource cleanup: instance is None")
        return

    logger.debug(f"Starting direct resource cleanup for {type(instance).__name__}")

    # First, break any circular references in curl_cffi clients
    for attr_name, resource_name, is_external in resource_tuples:
        # Only handle non-external resources
        if is_external:
            continue

        # Check if the resource exists
        if not hasattr(instance, attr_name) or getattr(instance, attr_name) is None:
            continue

        resource = getattr(instance, attr_name)

        # Special handling for curl_cffi clients - break circular references first
        if "curl" in str(type(resource)).lower() or hasattr(resource, "_curlm"):
            logger.debug(
                f"Found curl_cffi client in {attr_name}, breaking circular references"
            )

            # Nullify _curlm reference that causes circular dependencies
            if hasattr(resource, "_curlm") and resource._curlm is not None:
                logger.debug(f"Nullifying _curlm reference in {resource_name}")
                resource._curlm = None

            # Also clear _timeout_handle if it exists
            if (
                hasattr(resource, "_timeout_handle")
                and resource._timeout_handle is not None
            ):
                logger.debug(f"Nullifying _timeout_handle in {resource_name}")
                resource._timeout_handle = None

    # Cancel any _force_timeout tasks that might be lingering
    await _cancel_force_timeout_tasks()

    # Now proceed with normal resource cleanup
    for attr_name, resource_name, is_external in resource_tuples:
        # Skip if the attribute doesn't exist or is None
        if not hasattr(instance, attr_name) or getattr(instance, attr_name) is None:
            logger.debug(f"Skipping cleanup for {resource_name}: not available")
            continue

        # Get the resource
        resource = getattr(instance, attr_name)

        # Special handling for HTTP clients
        if resource_name.lower() in ("http client", "client", "session"):
            await cleanup_client(resource, is_external)
        # Special handling for file handles
        elif resource_name.lower() in ("file", "file handle"):
            await cleanup_file_handle(resource)
        # General resource cleanup
        else:
            await close_resource_with_timeout(resource, resource_name=resource_name)

        # Set the resource to None to break reference cycles
        if not is_external:
            setattr(instance, attr_name, None)
            logger.debug(f"Set {resource_name} to None after cleanup")

    # Force garbage collection if enabled
    if force_gc:
        collected = gc.collect()
        logger.debug(f"Forced garbage collection: collected {collected} objects")

    logger.debug(f"Completed direct resource cleanup for {type(instance).__name__}")


async def _cancel_force_timeout_tasks() -> int:
    """Find and cancel any _force_timeout tasks that might be active.

    Returns:
        int: Number of tasks cancelled
    """
    cancelled_count = 0
    try:
        # Find all force_timeout tasks
        force_timeout_tasks = []
        for task in asyncio.all_tasks():
            task_str = str(task)
            if "_force_timeout" in task_str and not task.done():
                force_timeout_tasks.append(task)

        # Cancel all found tasks
        if force_timeout_tasks:
            logger.debug(f"Cancelling {len(force_timeout_tasks)} force_timeout tasks")
            for task in force_timeout_tasks:
                task.cancel()
            cancelled_count = len(force_timeout_tasks)

            # Wait for cancellation to complete with timeout
            try:
                await asyncio.wait_for(
                    asyncio.gather(*force_timeout_tasks, return_exceptions=True),
                    timeout=0.5,  # Short timeout to avoid blocking
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for force_timeout tasks to cancel")

    except Exception as e:
        logger.warning(f"Error cancelling force_timeout tasks: {e}")

    return cancelled_count
