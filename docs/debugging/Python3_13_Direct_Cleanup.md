# Python 3.13 Direct Cleanup Pattern

## Problem

When using asynchronous resources in Python 3.13, we encountered hanging issues during resource cleanup, particularly when:

1. Multiple nested async resources needed to be cleaned up during context exit
2. Background task handling could lead to "coroutine never awaited" warnings
3. Blocking behavior was observed during cleanup of HTTP clients (curl_cffi)

## Solution: Direct Cleanup Pattern

We implemented a direct cleanup approach that avoids relying on background tasks or complex event loop interactions, with these key features:

1. **Immediate Reference Capture**: Capture all references before nullifying them
2. **Immediate Nullification**: Immediately nullify all instance references to assist garbage collection
3. **Timeout-Protected Cleanup**: Use `asyncio.shield` and `wait_for` with short timeouts to prevent hanging
4. **Error Handling**: Gracefully handle all exceptions during cleanup without propagating them
5. **Synchronous Cleanup First**: Perform any synchronous cleanup first (like closing file handles)
6. **Centralized Configuration**: All timeout values and cleanup settings are defined in `utils/config.py`

## Centralized Configuration

We've centralized all timeout values and cleanup settings in `utils/config.py` to ensure consistency across the codebase:

```python
# Resource cleanup timeouts
RESOURCE_CLEANUP_TIMEOUT: Final = 0.1  # Seconds - for generic async resource cleanup
HTTP_CLIENT_CLEANUP_TIMEOUT: Final = 0.2  # Seconds - for HTTP client cleanup (curl_cffi)
FILE_CLEANUP_TIMEOUT: Final = 0.3  # Seconds - for file handle cleanup
ENABLE_FORCED_GC: Final = True  # Whether to force garbage collection after cleanup
```

## Centralized Implementation

We've centralized this pattern in `utils/async_cleanup.py` to follow the DRY principle and provide a standardized way to handle resource cleanup. The key functions are:

### Centralized Utilities

```python
from utils.async_cleanup import direct_resource_cleanup, close_resource_with_timeout, cleanup_client, cleanup_file_handle
```

- `direct_resource_cleanup`: Main utility for comprehensive cleanup of multiple resources
- `close_resource_with_timeout`: For individual resource cleanup with timeout protection
- `cleanup_client`: Specialized for HTTP client cleanup (curl_cffi and others)
- `cleanup_file_handle`: Specialized for file handle and memory-mapped file cleanup

### Simplified Usage Example

Before:

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

After:

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

## Benefits

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

## Testing

We created comprehensive test cases to verify the cleanup approach:

- `tests/rest_data_client/test_rest_cleanup.py`

These tests confirm the clients cleanly exit without hanging, even after performing API operations.

## Lessons Learned

1. Direct cleanup with timeouts is more reliable than background task scheduling for resource cleanup
2. Python 3.13's stricter handling of coroutines requires more explicit cleanup approaches
3. Always capture references before nullifying to maintain access for cleanup
4. Proper error handling during cleanup is essential to prevent exceptions from propagating
5. Extensive logging helps track resource lifecycle and identify cleanup issues
6. Centralizing common patterns improves code quality and maintainability
7. Centralizing configuration values makes the system more maintainable and consistent
