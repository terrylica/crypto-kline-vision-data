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
from typing import Any, TypeVar, Union, Tuple
import inspect
import sys

from utils.logger_setup import logger
from utils.config import (
    RESOURCE_CLEANUP_TIMEOUT,
    HTTP_CLIENT_CLEANUP_TIMEOUT,
    FILE_CLEANUP_TIMEOUT,
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
        # Check for curl_cffi AsyncSession and handle special cases
        if client_type == "AsyncSession" and hasattr(client, "_asynccurl"):
            logger.debug("Detected curl_cffi AsyncSession, using targeted cleanup")

            # First try to clean up the asynccurl object directly
            if hasattr(client._asynccurl, "close") and callable(
                client._asynccurl.close
            ):
                try:
                    client._asynccurl.close()
                    logger.debug("Closed curl_cffi client via _asynccurl.close()")
                except Exception as e:
                    logger.warning(f"Error closing _asynccurl: {e}")

            # Clear the Session's attributes to break circular references
            # For C-pointer attributes, we use delattr instead of setting to None
            for attr in ["_asynccurl", "_curlm", "_timeout_handle"]:
                if hasattr(client, attr):
                    try:
                        # Use delattr for C-pointer attributes to avoid type errors
                        delattr(client, attr)
                        logger.debug(f"Deleted client.{attr} attribute")
                    except Exception as e:
                        logger.debug(f"Error deleting client.{attr} attribute: {e}")

            # Session doesn't have aclose but has close
            if hasattr(client, "close") and callable(client.close):
                if inspect.iscoroutinefunction(client.close):
                    try:
                        await asyncio.shield(
                            asyncio.wait_for(client.close(), timeout=timeout)
                        )
                        logger.debug("Closed curl_cffi client with async close()")
                    except Exception as e:
                        logger.warning(f"Error during client.close(): {e}")
                else:
                    try:
                        client.close()
                        logger.debug("Closed curl_cffi client with sync close()")
                    except Exception as e:
                        logger.warning(f"Error during client.close(): {e}")

            return

        # Try direct aclose if available
        if hasattr(client, "aclose") and callable(client.aclose):
            logger.debug(f"Found aclose() method on client (type: {client_type})")
            try:
                await asyncio.shield(asyncio.wait_for(client.aclose(), timeout=timeout))
                logger.debug("Directly closed HTTP client with aclose()")
                return
            except asyncio.TimeoutError:
                logger.warning(f"HTTP client aclose() timed out after {timeout}s")
            except asyncio.CancelledError:
                logger.warning("HTTP client aclose() was cancelled")
            except Exception as e:
                logger.warning(f"Error during client.aclose(): {e}")

        # Try regular close method
        if hasattr(client, "close") and callable(client.close):
            logger.debug(f"Found close() method on client (type: {client_type})")
            if inspect.iscoroutinefunction(client.close):
                try:
                    await asyncio.shield(
                        asyncio.wait_for(client.close(), timeout=timeout)
                    )
                    logger.debug("Closed HTTP client with async close()")
                    return
                except Exception as e:
                    logger.warning(f"Error during async client.close(): {e}")
            else:
                try:
                    client.close()
                    logger.debug("Closed HTTP client with sync close()")
                    return
                except Exception as e:
                    logger.warning(f"Error during sync client.close(): {e}")

        # Use our utility if available as a last resort
        logger.debug(f"Trying safely_close_client as fallback (type: {client_type})")
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
        # Last resort measure: clean up attributes to help garbage collection
        if client_type == "AsyncSession":
            # Modified approach: Use delattr for C-pointer attributes, set to None for others
            c_pointer_attributes = ["_curlm", "_timeout_handle", "_asynccurl"]
            
            for attr_name in dir(client):
                if not attr_name.startswith("__") and hasattr(client, attr_name):
                    try:
                        # Use delattr for C-pointer attributes to avoid type errors
                        if attr_name in c_pointer_attributes:
                            # Use delattr for C-pointer attributes to avoid type errors with None
                            if hasattr(client, attr_name):
                                delattr(client, attr_name)
                                logger.debug(f"Deleted {attr_name} attribute during final cleanup")
                        else:
                            # For non-pointer attributes, we can set to None
                            setattr(client, attr_name, None)
                    except Exception as e:
                        # Just log at debug level as this is last-resort cleanup
                        logger.debug(f"Error during attribute cleanup for {attr_name}: {e}")
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
    obj: Any,
    *resources: Union[Tuple[str, str, bool], Tuple[str, str]],
):
    """Directly clean up resources using explicit approach.

    Args:
        obj: Object containing resources to clean up
        *resources: Sequence of (attr_name, description, is_external) tuples
    """
    logger.debug(
        f"[ProgressIndicator] Starting direct resource cleanup for {type(obj).__name__}"
    )

    # Track cleanup errors
    cleanup_errors = []

    # Track which resources were successfully cleaned
    cleaned_resources = []

    for res_spec in resources:
        if len(res_spec) >= 2:
            attr_name, desc = res_spec[0], res_spec[1]
            is_external = res_spec[2] if len(res_spec) >= 3 else False

            # Skip resources we don't own
            if is_external:
                logger.debug(
                    f"[ProgressIndicator] Skipping external {desc} ({attr_name})"
                )
                continue

            # Get the resource
            resource = getattr(obj, attr_name, None)
            if resource is None:
                logger.debug(
                    f"[ProgressIndicator] {desc} ({attr_name}) is None, skipping"
                )
                continue

            logger.debug(
                f"[ProgressIndicator] Cleaning up {desc} ({attr_name}) of type {type(resource).__name__}"
            )

            try:
                # Handle specific resource types
                if hasattr(resource, "__aexit__"):
                    logger.debug(
                        f"[ProgressIndicator] Using __aexit__ to clean up {desc}"
                    )
                    try:
                        await resource.__aexit__(None, None, None)
                        logger.debug(
                            f"[ProgressIndicator] Successfully called __aexit__ on {desc}"
                        )
                    except Exception as e:
                        error_msg = f"Error during __aexit__ for {desc}: {e}"
                        logger.warning(error_msg)
                        cleanup_errors.append(error_msg)

                # For client resources with _client attribute (HTTP clients)
                if hasattr(resource, "_client") and resource._client:
                    logger.debug(
                        f"[ProgressIndicator] Found nested HTTP client in {desc}"
                    )
                    try:
                        from utils.network_utils import safely_close_client

                        await safely_close_client(resource._client)
                        logger.debug(
                            f"[ProgressIndicator] Successfully closed nested client in {desc}"
                        )
                    except Exception as e:
                        error_msg = f"Error closing nested client in {desc}: {e}"
                        logger.warning(error_msg)
                        cleanup_errors.append(error_msg)

                # Try close method (synchronous or asynchronous)
                if hasattr(resource, "close"):
                    logger.debug(f"[ProgressIndicator] Calling close() on {desc}")
                    try:
                        # Check if this is a curl_cffi client with a NULL pointer issue
                        if hasattr(resource, "_curlm") and resource._curlm is None:
                            logger.debug(
                                f"Skipping close for {desc} with NULL _curlm handle"
                            )
                        # Check if this is a curl_cffi client by class name
                        elif type(resource).__name__ == "AsyncSession":
                            logger.debug(
                                f"[ProgressIndicator] Detected curl_cffi AsyncSession, using specialized cleanup"
                            )
                            # First try to directly access the _asynccurl if it exists
                            if hasattr(resource, "_asynccurl") and resource._asynccurl is not None:
                                try:
                                    if hasattr(resource._asynccurl, "close"):
                                        resource._asynccurl.close()
                                        logger.debug(f"[ProgressIndicator] Closed {desc}._asynccurl directly")
                                except Exception as e:
                                    logger.debug(f"[ProgressIndicator] Error closing {desc}._asynccurl: {e}")
                            
                            # Delete C pointer attributes instead of trying to use them
                            for attr in ["_curlm", "_timeout_handle", "_asynccurl"]:
                                if hasattr(resource, attr):
                                    try:
                                        delattr(resource, attr)
                                        logger.debug(f"[ProgressIndicator] Deleted {desc}.{attr} attribute")
                                    except Exception as e:
                                        logger.debug(f"[ProgressIndicator] Error deleting {attr} from {desc}: {e}")
                            
                            # Skip the regular close() call which would use these attributes
                            logger.debug(f"[ProgressIndicator] Skipping regular close() for {desc} curl_cffi client")
                        else:
                            # For non-curl_cffi clients, call close normally
                            if inspect.iscoroutinefunction(resource.close):
                                await resource.close()
                            else:
                                resource.close()
                            logger.debug(
                                f"[ProgressIndicator] Successfully called close() on {desc}"
                            )
                    except Exception as e:
                        error_msg = f"Error during close() for {desc}: {e}"
                        logger.warning(error_msg)
                        cleanup_errors.append(error_msg)

                # Set resource to None to break circular references
                # Check if this is a curl_cffi client with C pointer attributes first
                if hasattr(resource, "_curlm") or hasattr(resource, "_asynccurl") or hasattr(resource, "_timeout_handle"):
                    logger.debug(f"[ProgressIndicator] Detected curl_cffi client for {desc}, handling C pointer attributes")
                    # Use safe cleanup for curl_cffi clients
                    try:
                        from utils.network_utils import safely_close_client
                        await safely_close_client(resource)
                        logger.debug(f"[ProgressIndicator] Used safely_close_client for {desc} with C pointer attributes")
                    except Exception as e:
                        error_msg = f"Error safely closing {desc} with C pointer attributes: {e}"
                        logger.warning(error_msg)
                        cleanup_errors.append(error_msg)
                    
                    # After safely closing, we can safely remove the reference
                    setattr(obj, attr_name, None)
                else:
                    # For regular resources, just set to None
                    setattr(obj, attr_name, None)
                
                cleaned_resources.append(attr_name)
                logger.debug(f"[ProgressIndicator] Successfully cleaned up {desc}")

            except Exception as e:
                error_msg = f"Unexpected error cleaning up {desc}: {e}"
                logger.error(error_msg)
                cleanup_errors.append(error_msg)

    # Final cleanup status
    if cleanup_errors:
        logger.warning(f"Resource cleanup encountered {len(cleanup_errors)} errors")
        for i, error in enumerate(cleanup_errors, 1):
            logger.debug(f"Cleanup error {i}: {error}")

    if cleaned_resources:
        logger.debug(
            f"[ProgressIndicator] Successfully cleaned up {len(cleaned_resources)} resources: {', '.join(cleaned_resources)}"
        )
    else:
        logger.debug("[ProgressIndicator] No resources were cleaned up")

    # Force garbage collection to help with resources
    gc.collect()

    logger.debug(
        f"[ProgressIndicator] Resource cleanup for {type(obj).__name__} completed"
    )


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


async def cleanup_all_force_timeout_tasks():
    """Find and cancel all curl_cffi _force_timeout tasks that might cause hanging.

    This is a global utility function to help resolve hanging issues with curl_cffi
    by directly targeting and cancelling _force_timeout tasks.
    """
    # Find all tasks that might be related to _force_timeout
    force_timeout_tasks = []
    for task in asyncio.all_tasks():
        task_str = str(task)
        # Look specifically for _force_timeout tasks
        if "_force_timeout" in task_str and not task.done():
            force_timeout_tasks.append(task)

    if not force_timeout_tasks:
        logger.debug("No _force_timeout tasks found to clean up")
        return 0

    # Log what we found
    logger.warning(
        f"Found {len(force_timeout_tasks)} hanging _force_timeout tasks to cancel"
    )

    # Cancel futures first, then tasks
    for task in force_timeout_tasks:
        if hasattr(task, "_fut_waiter") and task._fut_waiter is not None:
            try:
                task._fut_waiter.cancel()
            except Exception as e:
                logger.debug(f"Error cancelling future in task: {e}")
        task.cancel()

    # Multi-pass cleanup with escalating measures
    remaining_tasks = force_timeout_tasks
    for attempt in range(3):
        if not remaining_tasks:
            break

        try:
            # Wait for cancellation to complete
            done, pending = await asyncio.wait(
                remaining_tasks,
                timeout=0.5,  # Short timeout
                return_when=asyncio.ALL_COMPLETED,
            )

            remaining_tasks = list(pending)
            logger.debug(
                f"Pass {attempt+1}: {len(done)} tasks done, {len(pending)} tasks pending"
            )

            # If we still have pending tasks after final attempt, try more aggressive approach
            if attempt == 2 and pending:
                logger.warning(
                    f"Attempting aggressive cleanup of {len(pending)} stuck tasks"
                )
                for task in pending:
                    # Try to unblock the task by directly accessing internal APIs
                    if hasattr(task, "_coro"):
                        try:
                            task._coro.throw(asyncio.CancelledError)
                        except Exception:
                            pass
        except Exception as e:
            logger.warning(f"Error during _force_timeout task cleanup: {e}")

    # Force garbage collection to help with circular references
    gc.collect()

    # Special handling for tests
    if "pytest" in sys.modules:
        # Extra sleep and GC in test environments
        try:
            await asyncio.sleep(0.5)
            gc.collect()
        except Exception:
            pass

    return len(force_timeout_tasks) - len(remaining_tasks)
