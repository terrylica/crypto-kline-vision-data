# Cache Partial Days Optimization

## Overview

This document describes a critical optimization in the DataSourceManager's cache handling system.

## TEST_CASE_ID: CACHE-OPT-001

### Problem Statement

The original cache handling system would consider a day "incomplete" if it had less than the expected number of records for a full day (typically 1440 records for 1-minute data). However, this approach was inefficient when:

1. A user requested data for a specific time range within a day
2. The cache contained all data for the requested time range
3. But the day was still marked as "incomplete" because it didn't have all records for the entire day

In such cases, the system would unnecessarily make API calls to refetch data that was already available in the cache.

### Solution

The optimization implements a smarter check that:

1. Identifies potentially incomplete days (less than 90% of the expected full-day records)
2. For each such day, checks if the cache actually contains all records needed for the _requested time range_
3. Only flags the day for refetching if there are actual gaps in the data for the requested time range

### Implementation

The optimization is implemented in `utils/for_core/dsm_cache_utils.py` in the `get_from_cache` function. The critical code block is marked with `@critical_optimization: TEST_CASE_ID:CACHE-OPT-001`.

### Configuration

This optimization can be enabled or disabled via the `OPTIMIZE_CACHE_PARTIAL_DAYS` feature flag in `utils/config.py`.

```python
# Feature flags for critical optimizations
FEATURE_FLAGS = {
    # Prevents refetching days that have all required data for the requested time range
    # even if they are incomplete compared to a full day
    "OPTIMIZE_CACHE_PARTIAL_DAYS": True,
}
```

### Testing

This optimization is protected by:

1. Unit tests in `tests/test_dsm_cache_utils.py`
2. Integration tests in `tests/integration/test_dsm_cache_optimization.py`

All tests must pass before making any modifications to this code.

## Performance Impact

This optimization significantly reduces unnecessary API calls, especially when:

1. Users request data for short time ranges
2. The cache contains data from a previous request that overlaps with the current request
3. The cache doesn't have complete data for all full days, but does have all the data for the specific time range requested

## Maintenance Guidelines

When working with this code:

1. Never remove or modify the optimization without running the associated tests
2. Any modifications should maintain the core business logic of only refetching data when there are actual gaps
3. Update the documentation and tests if you enhance the optimization
4. The feature flag should remain in place to allow quick disabling if issues arise

## Related Components

- `utils/for_core/dsm_cache_utils.py`: Contains the optimization implementation
- `utils/config.py`: Contains the feature flag
- `tests/test_dsm_cache_utils.py`: Contains unit tests
- `tests/integration/test_dsm_cache_optimization.py`: Contains integration tests
