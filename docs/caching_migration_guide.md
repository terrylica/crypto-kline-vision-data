# Caching Migration Guide

## Overview

This guide explains the migration path from the deprecated direct caching mechanism in `VisionDataClient` to the recommended approach using `DataSourceManager` with `UnifiedCacheManager`.

## Deprecation Notice

The direct caching mechanism in `VisionDataClient` is now deprecated and will be removed in a future version. When using the deprecated approach, you will see the following warning:

```
DeprecationWarning: Direct caching through VisionDataClient is deprecated. Please use DataSourceManager with UnifiedCacheManager for caching. This will be removed in a future version.
```

## Why This Change?

1. **Unified Caching**: The new approach provides a unified caching mechanism that works with multiple data sources, not just the Vision API.
2. **Better Cache Management**: `UnifiedCacheManager` offers improved cache validation, statistics, and performance.
3. **Simplified API**: `DataSourceManager` provides a cleaner, more consistent API for data retrieval regardless of the source.
4. **Improved Source Selection**: Automatic selection of the most appropriate data source based on the request parameters.

## Migration Options

### Option 1: Complete Migration (Recommended)

Replace all direct usage of `VisionDataClient` with `DataSourceManager`. This is the recommended approach for all new and existing code.

#### Before:

```python
from core.vision_data_client import VisionDataClient

# Create VisionDataClient with direct caching
client = VisionDataClient(
    symbol="BTCUSDT",
    interval="1s",
    cache_dir="./cache",  # DEPRECATED
    use_cache=True,       # DEPRECATED
)

# Using the client to fetch data
async with client:
    data = await client.fetch(start_time, end_time)
```

#### After:

```python
from core.data_source_manager import DataSourceManager
from utils.market_constraints import Interval, MarketType

# Using DataSourceManager with unified caching
async with DataSourceManager(
    market_type=MarketType.SPOT,
    cache_dir="./cache",
    use_cache=True,
) as manager:
    # Fetch data using the manager
    data = await manager.get_data(
        symbol="BTCUSDT",
        start_time=start_time,
        end_time=end_time,
        interval=Interval.SECOND_1,
    )
```

### Option 2: Hybrid Approach (For Transitional Periods)

If you need to keep using `VisionDataClient` for specific reasons but want to benefit from the unified caching mechanism, you can use the hybrid approach:

```python
from core.vision_data_client import VisionDataClient
from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import Interval, MarketType

# Create VisionDataClient without direct caching
client = VisionDataClient(
    symbol="BTCUSDT",
    interval="1s",
    use_cache=False,  # Disable direct caching
)

# Pass the VisionDataClient to DataSourceManager
async with DataSourceManager(
    market_type=MarketType.SPOT,
    cache_dir="./cache",
    use_cache=True,
    vision_client=client,  # Use the custom VisionDataClient
) as manager:
    # Force using Vision API source
    data = await manager.get_data(
        symbol="BTCUSDT",
        start_time=start_time,
        end_time=end_time,
        interval=Interval.SECOND_1,
        enforce_source=DataSource.VISION,  # Force Vision API
    )
```

## Advanced Features of the New Approach

### 1. Cache Statistics

The new approach provides cache statistics that can help with debugging and performance analysis:

```python
async with DataSourceManager(
    market_type=MarketType.SPOT,
    cache_dir="./cache",
    use_cache=True,
) as manager:
    # ... data fetching ...

    # Access cache statistics
    cache_stats = manager.get_cache_stats()
    print(f"Cache statistics: {cache_stats}")
```

### 2. Source Selection Control

You can control the data source selection process:

```python
# Force using REST API
data = await manager.get_data(
    symbol="BTCUSDT",
    start_time=start_time,
    end_time=end_time,
    interval=Interval.SECOND_1,
    enforce_source=DataSource.REST,
)
```

### 3. Cache Configuration

You can configure various cache parameters:

```python
async with DataSourceManager(
    market_type=MarketType.SPOT,
    cache_dir="./cache",
    use_cache=True,
    cache_validation=True,  # Enable cache validation
    cache_update_interval=86400,  # Update cache every 24 hours
) as manager:
    # ... data fetching ...
```

## Example Code

For complete working examples, refer to the following files in the `examples` directory:

1. `recommended_data_retrieval.py` - Shows the recommended approach for data retrieval
2. `migration_guide.py` - Demonstrates all migration approaches side by side

## Running the Examples

To run the examples, use the following commands from the project root:

```bash
# Run the recommended data retrieval example
python -m examples.recommended_data_retrieval

# Run the migration guide
python -m examples.migration_guide
```

## Timeline

- **Current Phase**: Deprecation warnings are shown when using the deprecated approach
- **Future Release**: Direct caching through `VisionDataClient` will be removed completely

We recommend migrating your code to the new approach as soon as possible to avoid breaking changes in future releases.
