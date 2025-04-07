# Cache Verification and curl_cffi Hanging Issue Resolution

## Problem Description

We encountered two main issues while testing the caching functionality:

1. **Cache verification failures**: Cache hits were not being recorded properly, and we couldn't reliably test if the cache mechanism was working.

2. **curl_cffi hanging**: Scripts would hang after making API requests using the `RestDataClient`, typically after retrieving data from the REST API.

## Root Causes

### Cache Verification Issues

- Each instance of `DataSourceManager` maintained its own separate cache stats counter
- When creating multiple instances in sequence, cache hit statistics were reset between instances
- Missing `_endpoint_lock` in `RestDataClient` caused errors when trying to fetch data

### curl_cffi Hanging

- `curl_cffi`'s `AsyncCurl` class creates `_force_timeout` tasks that run continuous loops
- These tasks maintain references to uncompleted futures
- Improper cleanup of these tasks and circular references in the client objects
- `RestDataClient` lacked comprehensive resource cleanup in its `__aexit__` method

## Solution Components

### 1. Improved `RestDataClient` Cleanup

We updated the `__aexit__` method with a comprehensive, structured cleanup approach:

```python
async def __aexit__(self, exc_type, exc_val, exc_tb):
    """Clean up resources when exiting the context."""
    # STEP 1: Pre-emptively clean up problematic objects causing hanging
    if hasattr(self, "_client") and self._client:
        # Clean up circular references
        self._client._curlm = None
        self._client._asynccurl = None
        self._client._timeout_handle = None

    # STEP 2: Cancel any force_timeout tasks that might be hanging
    await cleanup_all_force_timeout_tasks()

    # STEP 3: Use direct resource cleanup for consistent management
    await direct_resource_cleanup(
        self,
        ("_client", "HTTP client", self._client_is_external),
    )

    # STEP 4: Force garbage collection to help with circular references
    gc.collect()
```

### 2. Dedicated Force-Timeout Task Cleanup Utility

We created a utility function that specifically targets and cleans up the problematic tasks:

```python
async def cleanup_all_force_timeout_tasks():
    """Find and cancel all curl_cffi _force_timeout tasks."""
    # Find all tasks related to _force_timeout
    force_timeout_tasks = []
    for task in asyncio.all_tasks():
        if "_force_timeout" in str(task) and not task.done():
            force_timeout_tasks.append(task)

    # Cancel futures first, then tasks
    for task in force_timeout_tasks:
        if hasattr(task, "_fut_waiter"):
            task._fut_waiter.cancel()
        task.cancel()

    # Multi-pass cleanup with escalating measures
    # ... (implementation details)
```

### 3. Simplified Cache Verification Test

We created a simplified test that:

- Uses the same cache directory across multiple `DataSourceManager` instances
- Performs multiple data retrieval operations with the same parameters
- Monitors cache hit statistics between operations
- Runs emergency cleanup after each client operation to prevent hanging

### 4. Missing Attribute Fixes

Added required attributes to `RestDataClient`:

- Added `_endpoint_lock` for endpoint rotation
- Added `_endpoints` and `_endpoint_index` for endpoint management
- Ensured correct initialization of `hw_monitor`
- Added proper `CHUNK_SIZE` constant

## Findings and Lessons Learned

1. **Proper Resource Cleanup is Critical**: Async clients require special cleanup patterns to avoid hanging issues, especially when dealing with low-level networking libraries.

2. **Cache Statistics Need Context**: Cache hit statistics are maintained per-instance, not globally, which means they need to be evaluated within the context of a single client instance.

3. **Hanging Tasks Detection**: We learned to identify and target specific task types (`_force_timeout`) that cause hanging issues.

4. **Comprehensive Cleanup Pattern**: We implemented a multi-step cleanup pattern with progressive escalation that ensures all resources are properly released.

5. **Cache Functioning Properly**: Once the hanging issues were resolved, we confirmed that the cache mechanism is working as expected, with proper hit statistics tracking.

## Verification of Fix

We verified the fix with a dedicated test that:

1. Performed a first data fetch that hit the cache successfully
2. Performed a second data fetch with the same parameters in a new client instance
3. Confirmed cache hit statistics increased correctly
4. Returned consistent data from both fetches
5. Completed cleanly without hanging issues

Both cache functionality and hanging issues have been successfully resolved.
