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
        return

    try:
        # Check if the method exists and if it's a coroutine function
        method = getattr(resource, close_method, None)
        if method is None:
            logger.debug(f"{resource_name} does not have a {close_method} method")
            return

        if inspect.iscoroutinefunction(method):
            # For async close methods
            await asyncio.shield(asyncio.wait_for(method(*close_args), timeout=timeout))
            logger.debug(f"Successfully closed {resource_name}")
        else:
            # For synchronous close methods
            method(*close_args)
            logger.debug(f"Successfully closed {resource_name} (sync)")

    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        logger.debug(f"{resource_name} cleanup timed out or was cancelled: {str(e)}")
    except Exception as e:
        logger.warning(f"Error closing {resource_name}: {str(e)}")


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
    if client is None or is_external:
        return

    try:
        # Try direct aclose if available (curl_cffi AsyncSession)
        if hasattr(client, "aclose"):
            await asyncio.shield(asyncio.wait_for(client.aclose(), timeout=timeout))
            logger.debug("Directly closed HTTP client")
        else:
            # Use our utility if available
            try:
                from utils.network_utils import safely_close_client

                await asyncio.shield(
                    asyncio.wait_for(safely_close_client(client), timeout=timeout)
                )
                logger.debug("Safely closed HTTP client")
            except (ImportError, Exception) as e:
                logger.warning(f"Could not safely close client: {str(e)}")

    except (asyncio.TimeoutError, asyncio.CancelledError) as e:
        logger.debug(f"HTTP client cleanup timed out or was cancelled: {str(e)}")
    except Exception as e:
        logger.warning(f"Error closing HTTP client: {str(e)}")


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
    """Directly clean up multiple resources on an instance using the Python 3.13 compatible pattern.

    This implements the pattern of capturing references, nullifying instance attributes,
    and safely cleaning up each resource with timeout protection.

    Args:
        instance: The instance containing resources to clean up
        *resource_tuples: Variable number of tuples containing:
            - Attribute name of the resource
            - Display name for logging
            - Boolean flag indicating if resource is external (True = don't clean up)
        force_gc: Whether to force garbage collection after cleanup (default: from config)

    Example:
        ```python
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            await direct_resource_cleanup(
                self,
                ("_client", "HTTP client", self._client_is_external),
                ("_download_manager", "download manager", False),
                ("_data_client", "data client", False),
            )
        ```
    """
    # Capture and process each resource
    for attr_name, display_name, is_external in resource_tuples:
        # Skip if we shouldn't clean this resource
        if is_external:
            continue

        # Capture reference
        resource = getattr(instance, attr_name, None)

        # Immediately nullify
        setattr(instance, attr_name, None)

        # Skip None resources
        if resource is None:
            continue

        # Choose appropriate cleanup method based on resource type
        if attr_name.endswith("client") or attr_name.endswith("_client"):
            # HTTP client cleanup
            await cleanup_client(
                resource, is_external, timeout=HTTP_CLIENT_CLEANUP_TIMEOUT
            )
        elif attr_name.endswith("file") or attr_name.endswith("mmap"):
            # File handle cleanup
            await cleanup_file_handle(resource, timeout=FILE_CLEANUP_TIMEOUT)
        else:
            # Generic resource cleanup
            await close_resource_with_timeout(
                resource, timeout=RESOURCE_CLEANUP_TIMEOUT, resource_name=display_name
            )

    # Force garbage collection if requested
    if force_gc:
        gc.collect()
        logger.debug("Cleanup complete, garbage collection forced")
