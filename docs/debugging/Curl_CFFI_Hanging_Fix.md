# Resolving curl_cffi Hanging Issue in Python 3.13

## Problem Description

We encountered a persistent hanging issue when using `curl_cffi` with Python 3.13, particularly with the `AsyncCurl` class. The application would freeze after retrieving 3599 records (or sometimes a different count) from the REST API.

The hanging occurred specifically during cleanup of resources when:

1. The `__aexit__` method of data clients was called
2. The resources were not properly released
3. The `_force_timeout` task from `AsyncCurl` remained active and prevented the application from exiting

## Root Cause Analysis

After extensive investigation, we identified that the hanging issue was caused by:

1. **Circular References**: The `_curlm` object within `AsyncCurl` was creating circular references that prevented garbage collection.
2. **Hanging Timeout Tasks**: The `_force_timeout` task created by `AsyncCurl` was never properly cancelled and remained active.
3. **Improper Resource Cleanup**: The Python 3.13 implementation of resource cleanup was more strict, requiring explicit cleanup of all resources.

## Solution Components

We developed a comprehensive solution with multiple layers of protection:

### 1. Direct Cleanup Pattern

Implemented a direct resource cleanup pattern in the `__aexit__` methods of our client classes:

```python
async def __aexit__(self, exc_type, exc_val, exc_tb):
    """Python 3.13 compatible cleanup implementation."""
    # Pre-emptively clean up _curlm objects that might cause hanging
    if hasattr(self, "_client") and self._client:
        if hasattr(self._client, "_curlm") and self._client._curlm:
            self._client._curlm = None

    # Use direct resource cleanup
    await direct_resource_cleanup(
        self,
        ("_client", "HTTP client", self._client_is_external),
    )
```

### 2. Proactive Force-Timeout Task Cancellation

Added code to detect and cancel any `_force_timeout` tasks that might be active:

```python
async def _cleanup_force_timeout_tasks(self):
    """Find and clean up any _force_timeout tasks that might cause hanging."""
    force_timeout_tasks = []
    for task in asyncio.all_tasks():
        if "_force_timeout" in str(task) and not task.done():
            force_timeout_tasks.append(task)

    if force_timeout_tasks:
        for task in force_timeout_tasks:
            task.cancel()
```

### 3. Emergency Cleanup Utility

Created a dedicated utility for emergency cleanup when hanging is detected:

```python
async def emergency_cleanup():
    """Perform emergency cleanup when the application is hanging."""
    # Find and fix any force_timeout tasks
    force_timeout_tasks = await find_force_timeout_tasks()
    if force_timeout_tasks:
        for task in force_timeout_tasks:
            task.cancel()

    # Fix curl references in memory
    await find_curl_references_in_memory()

    # Force garbage collection
    gc.collect()
```

### 4. Client Context Manager Enhancements

Enhanced the context manager implementations to:

- Proactively clean up timeout tasks on entry
- Nullify circular references on exit
- Implement proper resource cleanup with timeouts

## Testing and Verification

We created dedicated test scripts to verify the solution:

1. **Diagnostic Script**: `scripts/diagnose_hanging.py` - Monitors tasks and detects hanging issues
2. **Client Test Script**: `scripts/test_client_cleanup.py` - Specifically tests our client implementations

The tests confirm that the implemented solution successfully prevents the hanging issue, allowing proper cleanup of resources and application termination.

## Usage Guidelines

To prevent hanging issues in your own code:

1. Always use clients within an async context manager
2. Ensure all circular references are broken
3. Proactively cancel any timeout tasks
4. Use the emergency cleanup utility when necessary

## Conclusion

The hanging issue has been successfully resolved by implementing a multi-layered approach to resource cleanup. The solution is robust and prevents the application from hanging during normal operation and error conditions.

This fix ensures compatibility with Python 3.13's stricter resource cleanup requirements and provides better overall resource management.
