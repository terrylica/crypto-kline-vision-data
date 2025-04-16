# FCP Technical Implementation Guide

This guide provides technical details on how the Failover Control Protocol (FCP) mechanism is implemented in the codebase.

## Core Components

### 1. DataSourceManager (DSM)

The `DataSourceManager` class is the central component that implements the FCP mechanism:

```python
class DataSourceManager:
    def __init__(self, config=None):
        # Initialize data sources and configuration
        self._init_sources()
        self._configure(config)

    def get_data(self, market, symbol, interval, start_time=None, end_time=None,
                chart_type="klines", enforce_source=None):
        # This is where FCP logic is implemented
        # ...
```

The DSM handles:

- Configuration of data sources
- Prioritized data retrieval
- Source selection and fallback logic
- Data merging and deduplication

### 2. Data Sources

Three main data sources are used in priority order:

1. **CacheDataSource**: Retrieves data from local Arrow files

   ```python
   class CacheDataSource:
       def get_data(self, market, symbol, interval, start_time, end_time, chart_type="klines"):
           # Retrieves data from cache files
           # ...
   ```

2. **VisionDataSource**: Retrieves data from Binance VISION API

   ```python
   class VisionDataSource:
       def get_data(self, market, symbol, interval, start_time, end_time, chart_type="klines"):
           # Retrieves data from VISION API
           # ...
   ```

3. **RestDataSource**: Retrieves data from Binance REST API

   ```python
   class RestDataSource:
       def get_data(self, market, symbol, interval, start_time, end_time, chart_type="klines"):
           # Retrieves data from REST API
           # ...
   ```

## FCP Algorithm

The FCP mechanism follows this algorithm:

1. **Initialize Request**:

   - Parse and validate input parameters
   - Convert time range to standardized format
   - Determine time boundaries for the request

2. **Cache Lookup**:

   - Query the cache for data in the requested time range
   - Identify missing portions (time gaps)

3. **Vision API Retrieval**:

   - For each identified gap, attempt to fetch from VISION API
   - Mark successful retrievals with source = "VISION"

4. **REST API Fallback**:

   - For any remaining gaps, fetch from REST API
   - Mark these data points with source = "REST"

5. **Data Composition**:

   - Merge data from all sources
   - Remove duplicates (prioritizing higher-quality sources)
   - Sort by timestamp
   - Add source attribution to each record

6. **Cache Update**:
   - Store new data in the cache for future requests
   - Optimize cache storage format for quick retrieval

## Implementation Details

### Time Range Handling

```python
def _standardize_time_range(self, start_time, end_time, interval_obj):
    # Convert string timestamps to pendulum objects
    # Ensure time boundaries align with interval
    # Handle timezone conversions
    # ...
```

### Gap Detection

```python
def _find_gaps(self, df, start_time, end_time, interval_obj):
    # Find missing time periods in the dataframe
    # Return list of (gap_start, gap_end) tuples
    # ...
```

### Source Selection Logic

```python
def _select_source(self, time_range, available_sources, enforce_source=None):
    # Logic to select the appropriate source based on:
    # - Time range (recent vs historical)
    # - Data completeness requirements
    # - Enforce_source override parameter
    # - Rate limiting considerations
    # ...
```

### Data Merging

```python
def _merge_dataframes(self, dfs, priority_order=None):
    # Combine multiple dataframes
    # Handle overlapping data with prioritization
    # Ensure consistent schema
    # ...
```

## FCP Extension

FCP extends the basic FCP mechanism with more granular data retrieval:

```python
def _fcp_pm_process(self, market, symbol, interval, start_time, end_time, chart_type="klines"):
    # Divide time range into parcels
    # Process each parcel independently
    # Use parallel processing when possible
    # Merge results with special attention to boundaries
    # ...
```

FCP handles:

- Concurrent requests for different time parcels
- Optimal source selection for each parcel
- Smart boundary handling to avoid duplicates
- Progress tracking for long-running operations

## Configuration Options

The FCP mechanism can be configured through:

```python
{
    "cache": {
        "enabled": True,
        "directory": "path/to/cache",
        "format": "arrow",
        "expiry_days": 7
    },
    "sources": {
        "cache": {
            "enabled": True,
            "priority": 1
        },
        "vision": {
            "enabled": True,
            "priority": 2,
            "base_url": "https://data.binance.vision"
        },
        "rest": {
            "enabled": True,
            "priority": 3,
            "base_url": "https://api.binance.com",
            "rate_limits": {
                "requests_per_minute": 1200
            }
        }
    },
    "FCP": {
        "use_pm": False,  # Parcel Merge feature
        "parcel_size": "1d",  # For PM mode
        "max_workers": 4  # For parallel processing
    }
}
```

## Error Handling

The FCP mechanism includes comprehensive error handling:

```python
def _handle_source_error(self, source_name, error, retry_count=0):
    # Log the error
    # Determine if retry is appropriate
    # Implement backoff strategy
    # Fall back to next source if needed
    # ...
```

Common error scenarios:

- Network connectivity issues
- API rate limiting
- Data availability gaps
- Source-specific formatting differences

## Performance Considerations

- **Caching Strategy**: Balances disk space with retrieval speed
- **Request Batching**: Minimizes API calls by batching requests
- **Parallel Processing**: Used for multi-parcel requests
- **Memory Management**: Handles large datasets efficiently
