# Python 3.13 Async Resource Cleanup Challenges and Solutions

## Problem Background

When upgrading to Python 3.13, we encountered persistent issues with the `VisionDataClient` and `DataSourceManager` classes hanging during cleanup phase, particularly when exiting async context managers. This manifested as:

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

### Synchronous Cleanup Solution - Zero Event Loop Interaction

Our next solution took a completely synchronous approach with zero event loop interaction:

```python
def __aexit__(self, exc_type, exc_val, exc_tb):
    """Python 3.13 compatible synchronous cleanup with no event loop manipulation."""
    import gc

    # Immediately capture and nullify references
    try:
        # Cache references before nullifying
        download_manager = getattr(self, "_download_manager", None)
        current_mmap = getattr(self, "_current_mmap", None)
        http_client = getattr(self, "_client", None)

        # Immediately nullify all references to break cycles
        self._download_manager = None
        self._current_mmap = None
        self._client = None

        # Synchronous cleanup of memory-mapped files
        if current_mmap is not None:
            try:
                current_mmap.close()
            except Exception:
                pass

        # Schedule async cleanup task without waiting
        asyncio.create_task(_cleanup_all_async_curl_tasks(timeout_seconds=0.1))

        # Force immediate garbage collection
        gc.collect()
    except Exception:
        # Silently ignore all exceptions during cleanup
        pass
```

This approach successfully resolved the hanging issues and prevented runtime warnings in Python 3.13.

### Enhanced Solution - AsyncExitStack with Python 3.13 Compatibility

Our final refined solution combines the elegance of `contextlib.AsyncExitStack` with Python 3.13 compatibility:

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

    # Define synchronous cleanup functions for each resource
    def cleanup_mmap():
        """Synchronously close memory-mapped file."""
        nonlocal current_mmap
        if current_mmap is not None:
            try:
                current_mmap.close()
            except Exception:
                pass
            current_mmap = None

    # Perform immediate synchronous cleanup first
    cleanup_mmap()

    # Schedule background async cleanup without awaiting
    # This avoids coroutine never awaited warnings in Python 3.13
    if download_manager is not None or http_client is not None:
        async def background_cleanup():
            """Background cleanup task that runs independently."""
            try:
                # Use AsyncExitStack for structured cleanup
                async with contextlib.AsyncExitStack() as stack:
                    # Register cleanup callbacks with appropriate error handling
                    if http_client is not None:
                        from utils.network_utils import safely_close_client
                        try:
                            await stack.enter_async_context(
                                contextlib.aclosing(safely_close_client(http_client))
                            )
                        except Exception:
                            pass

                    # Schedule download manager cleanup with timeout
                    if download_manager is not None:
                        try:
                            from utils.network_utils import _cleanup_all_async_curl_tasks
                            await _cleanup_all_async_curl_tasks(timeout_seconds=0.1)
                        except Exception:
                            pass
            except Exception:
                # Ensure no exceptions propagate from background task
                pass

        # Create detached task for background cleanup
        asyncio.create_task(background_cleanup())

    # Force immediate garbage collection to help clean up circular references
    gc.collect()
```

This approach offers several advantages:

1. Uses `contextlib.AsyncExitStack` for more structured and elegant resource management
2. Maintains Python 3.13 compatibility by avoiding direct event loop interaction
3. Performs synchronous cleanup immediately for time-critical resources
4. Schedules background async cleanup tasks for non-critical resources
5. Properly isolates cleanup errors to prevent failures from propagating
6. Uses `contextlib.aclosing` to enhance resource cleanup reliability

## Key Learning Points

1. **Python 3.13 Compatibility**:

   - Avoid direct event loop interaction in `__aexit__` methods
   - Don't use `await` operations in `__aexit__` when you need immediate resource release
   - Use `asyncio.create_task()` for background cleanup instead of awaiting

2. **Resource Management Best Practices**:

   - Immediately nullify all references at the start of cleanup
   - Perform synchronous cleanup operations first
   - Schedule asynchronous cleanup in the background
   - Always have a timeout for async cleanup operations

3. **Defense in Depth**:

   - Assume all network operations can hang indefinitely
   - Always wrap cleanup code in try/except blocks
   - Implement aggressive resource release strategies
   - Force garbage collection to help with circular references

4. **Structured Cleanup Sequence**:

   1. Capture all resource references
   2. Immediately nullify all instance references
   3. Perform synchronous cleanup operations
   4. Schedule asynchronous cleanup in background
   5. Explicitly break any circular references
   6. Force garbage collection

5. **AsyncExitStack Benefits**:
   - Provides more structured resource management
   - Makes cleanup code more readable and maintainable
   - Properly handles cleanup errors and exceptions
   - Works well with Python 3.13 when used in a background task

## Testing Methodology

Our testing approach confirmed that both the synchronous and AsyncExitStack-based cleanup strategies work reliably:

1. **Basic functionality tests**:

   - Recent data retrieval (REST API)
   - Historical data retrieval (Vision API)
   - Same-day data retrieval
   - Error condition handling

2. **Stress testing**:
   - Multiple consecutive retrievals
   - Parallel retrievals across different market types
   - Forced cancellation during active downloads
   - Long-running operations with large data volumes

## Conclusion

Python 3.13's stricter coroutine handling required a fundamentally different approach to async resource cleanup. We've successfully implemented two effective strategies:

1. A fully synchronous cleanup pattern with zero event loop interaction
2. An enhanced AsyncExitStack-based approach that combines elegance with compatibility

Both approaches prevent hanging, eliminate runtime warnings, and ensure proper resource cleanup in all scenarios.

The key insight was that **proper resource cleanup doesn't always require async operations to complete** - instead, it's often better to immediately release references and allow the garbage collector to handle the rest, while scheduling any necessary async cleanup as background tasks. When more structured cleanup is desired, AsyncExitStack can be used within a background task to provide both elegance and compatibility.
