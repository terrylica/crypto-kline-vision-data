# Python 3.13 Resource Cleanup: Evolution and Final Solution

## Problem Background

When upgrading to Python 3.13, we encountered persistent issues with asynchronous resource cleanup, particularly when exiting async context managers. This manifested as:

1. Runtime warnings: `RuntimeWarning: coroutine was never awaited`
2. Hanging processes that never completed
3. Resource leaks when handling large volumes of data

## Root Causes Identified

### 1. Python 3.13 Event Loop Changes

Python 3.13 introduced stricter coroutine handling and warnings, particularly:

- More aggressive warnings for unawaited coroutines
- Changes in event loop policy and management
- Stricter handling of event loop interactions during cleanup phases

### 2. Resource Management Complexities

Our original implementation had several weak points:

- Direct interaction with the event loop during cleanup
- Attempts to wait for tasks that might never complete
- Circular references between resources that prevented proper garbage collection
- Reliance on cooperative cancellation that wasn't always honored

### 3. Architectural Challenges

The original architecture had inherent weaknesses:

- Nested async context managers with complex dependencies
- Resource trees where parent cleanup depended on child cleanup
- Mixed synchronous and asynchronous cleanup operations
- No timeout enforcement during cleanup operations

## Solution Evolution

### Initial Attempts - Task-based Cleanup

Our first approach used `asyncio.TaskGroup` for structured concurrency:

```python
async def __aexit__(self, exc_type, exc_val, exc_tb):
    """Cleanup using structured concurrency pattern."""
    try:
        async with asyncio.TaskGroup() as tg:
            # Schedule cleanup tasks
            if hasattr(self, "_download_manager") and self._download_manager:
                tg.create_task(self._download_manager.close())
            if hasattr(self, "_client") and self._client:
                tg.create_task(safely_close_client(self._client))
    except ExceptionGroup as eg:
        logger.warning(f"Errors during cleanup: {eg}")
    finally:
        # Nullify references
        self._download_manager = None
        self._client = None
```

**Problems encountered:**

- TaskGroup still requires event loop interaction
- Resource release wasn't immediate enough
- Cleanup tasks could still hang indefinitely

### Intermediate Solution - Timeout-based Cleanup

We then implemented timeout-based cleanup:

```python
async def __aexit__(self, exc_type, exc_val, exc_tb):
    """Resource cleanup with timeout."""
    try:
        async def cleanup_with_timeout():
            if self._download_manager:
                await self._download_manager.close()
            if self._client:
                await safely_close_client(self._client)

        # Run cleanup with timeout
        await asyncio.wait_for(cleanup_with_timeout(), timeout=0.5)
    except asyncio.TimeoutError:
        logger.warning("Cleanup timed out, forcing resource release")
    finally:
        # Nullify references
        self._download_manager = None
        self._client = None
```

**Problems encountered:**

- Still relied on event loop for timeout management
- Coroutines could still be left unawaited
- Generated runtime warnings in Python 3.13

### Approach with AsyncExitStack

We also explored using `contextlib.AsyncExitStack` for structured resource management:

```python
def __aexit__(self, exc_type, exc_val, exc_tb):
    """Python 3.13 compatible resource cleanup using contextlib.AsyncExitStack."""
    import gc
    import contextlib

    # Immediately capture references before nullifying them
    download_manager = getattr(self, "_download_manager", None)
    current_mmap = getattr(self, "_current_mmap", None)
    http_client = getattr(self, "_client", None)

    # Immediately nullify all instance references
    self._download_manager = None
    self._current_mmap = None
    self._client = None

    # Schedule background async cleanup with AsyncExitStack
    asyncio.create_task(background_cleanup())

    # Force immediate garbage collection
    gc.collect()
```

**Problems encountered:**

- Still complex to manage with multiple resource types
- Required additional background tasks
- Not as direct and deterministic as we needed

## Final Solution: Direct Cleanup Pattern

Our final approach is a direct cleanup pattern that avoids relying on background tasks or complex event loop interactions, with these key features:

1. **Immediate Reference Capture**: Capture all references before nullifying them
2. **Immediate Nullification**: Immediately nullify all instance references to assist garbage collection
3. **Timeout-Protected Cleanup**: Use `asyncio.shield` and `wait_for` with short timeouts to prevent hanging
4. **Error Handling**: Gracefully handle all exceptions during cleanup without propagating them
5. **Synchronous Cleanup First**: Perform any synchronous cleanup first (like closing file handles)
6. **Centralized Configuration**: All timeout values and cleanup settings are defined in `utils/config.py`

### Centralized Configuration

We've centralized all timeout values and cleanup settings in `utils/config.py` to ensure consistency across the codebase:

```python
# Resource cleanup timeouts
RESOURCE_CLEANUP_TIMEOUT: Final = 0.1  # Seconds - for generic async resource cleanup
HTTP_CLIENT_CLEANUP_TIMEOUT: Final = 0.2  # Seconds - for HTTP client cleanup (curl_cffi)
FILE_CLEANUP_TIMEOUT: Final = 0.3  # Seconds - for file handle cleanup
ENABLE_FORCED_GC: Final = True  # Whether to force garbage collection after cleanup
```

### Centralized Implementation

We've centralized this pattern in `utils/async_cleanup.py` to follow the DRY principle and provide a standardized way to handle resource cleanup. The key functions are:

```python
from utils.async_cleanup import direct_resource_cleanup, close_resource_with_timeout, cleanup_client, cleanup_file_handle
```

- `direct_resource_cleanup`: Main utility for comprehensive cleanup of multiple resources
- `close_resource_with_timeout`: For individual resource cleanup with timeout protection
- `cleanup_client`: Specialized for HTTP client cleanup (curl_cffi and others)
- `cleanup_file_handle`: Specialized for file handle and memory-mapped file cleanup

### Implementation Example

Before our optimization:

```python
async def __aexit__(self, exc_type, exc_val, exc_tb):
    # Immediately capture references
    client = self._client
    client_is_external = self._client_is_external

    # Immediately nullify references
    self._client = None

    # Only clean up client if we created it internally
    if client and not client_is_external:
        try:
            await asyncio.shield(asyncio.wait_for(client.aclose(), timeout=0.1))
            logger.debug("Closed HTTP client")
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception) as e:
            logger.debug(f"HTTP client cleanup issue: {str(e)}")

    # Force garbage collection
    gc.collect()
```

After our optimization:

```python
async def __aexit__(self, exc_type, exc_val, exc_tb):
    await direct_resource_cleanup(
        self,
        ("_client", "HTTP client", self._client_is_external),
    )
```

For file handles and memory-mapped files:

```python
# Handle memory-mapped file with specialized file handle cleanup
current_mmap = getattr(self, "_current_mmap", None)
self._current_mmap = None  # Immediately nullify reference
if current_mmap is not None:
    await cleanup_file_handle(current_mmap)
```

## Benefits of the Direct Cleanup Pattern

1. **Prevents Hanging**: Timeout-protected cleanup ensures the process never hangs
2. **Python 3.13 Compatible**: Avoids "coroutine never awaited" warnings
3. **Resource Release**: Guarantees resources are released even if cleanup encounters errors
4. **Memory Management**: Helps garbage collection by breaking reference cycles
5. **Visibility**: Improved logging provides better insight into cleanup process
6. **DRY Principle**: Centralizes common cleanup logic, eliminating code duplication
7. **Standardization**: Ensures consistent cleanup behavior across components
8. **Maintainability**: Makes code more readable and easier to maintain
9. **Configuration Management**: All timeout values are defined in one place for easy adjustment

## Where Implemented

This pattern was successfully implemented in:

1. `RestDataClient.__aexit__`
2. `VisionDataClient.__aexit__`
3. `DataSourceManager.__aexit__`

All implementations now use the centralized utilities from `utils/async_cleanup.py` with timeout values from `utils/config.py`.

## Testing Strategy

We created comprehensive test cases to verify the cleanup approach:

- `tests/rest_data_client/test_rest_cleanup.py`

These tests perform multiple validation checks:

1. Simple client creation and cleanup
2. Client cleanup after data fetching operations
3. Multiple consecutive client creation/cleanup cycles
4. Error handling during cleanup

The tests confirm the clients cleanly exit without hanging, even after performing API operations.

## Key Learning Points

1. **Direct Cleanup is Preferable**: Direct cleanup with timeouts is more reliable than background task scheduling
2. **Capture Before Nullify**: Always capture references before nullifying to maintain access for cleanup
3. **Error Handling is Critical**: Proper error handling during cleanup is essential to prevent exceptions from propagating
4. **Timeout Everything**: Always apply timeouts to async cleanup operations to prevent hanging
5. **Python 3.13 Compatibility**: Python 3.13's stricter handling of coroutines requires more explicit cleanup approaches
6. **Logging Matters**: Extensive logging helps track resource lifecycle and identify cleanup issues
7. **Centralization Helps**: Centralizing common patterns and configuration improves code quality and maintainability

## Structured Cleanup Sequence

The optimal sequence for resource cleanup is:

1. Capture all resource references before nullification
2. Immediately nullify all instance references
3. Perform synchronous cleanup operations first
4. Handle asynchronous cleanup with timeout protection
5. Handle errors gracefully without propagation
6. Force garbage collection to help with circular references

By following this sequence and using our centralized utilities, we've created a robust, efficient, and maintainable solution for asynchronous resource cleanup in Python 3.13.
