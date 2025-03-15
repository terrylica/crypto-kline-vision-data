# DataSourceManager Documentation

## Overview

The `DataSourceManager` serves as a mediator between different Binance data sources, primarily the REST API (`EnhancedRetriever`) and Vision API (`VisionDataClient`). It provides intelligent source selection, caching, and fallback mechanisms to optimize data retrieval.

### Core Responsibilities

1. Data source selection between REST and Vision APIs
2. Unified caching strategy across all data sources
3. Cache integrity validation and management
4. Data format standardization

### Module Location

```python
from ml_feature_set.binance_data_services.core.data_source_manager import DataSourceManager
from ml_feature_set.binance_data_services.utils.market_constraints import Interval, MarketType
```

## System Architecture

### 1. Mediator Layer

#### Key Components

- `DataSource` Enum:
  - `AUTO`: Automatically select best source
  - `REST`: Force REST API usage
  - `VISION`: Force Vision API usage

- Dependencies:
  - `EnhancedRetriever` from `market_data_client.py`
  - `VisionDataClient` from `vision_data_client.py`
  - `UnifiedCacheManager` from `cache_manager.py`
  - `Interval`, `MarketType` from `market_constraints.py`
  - `adjust_time_window` from `time_alignment.py`

#### Configuration Constants

```python
class DataSourceManager:
    VISION_DATA_DELAY_HOURS = 36  # Data newer than this isn't available in Vision API
    REST_CHUNK_SIZE = 1000        # Maximum records per REST API request
    REST_MAX_CHUNKS = 10          # Maximum number of chunks to request via REST
```

### 2. Data Layer

#### Output Format Specification

```python
OUTPUT_DTYPES = {
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "close_time": "int64",
    "quote_volume": "float64",
    "trades": "int64",
    "taker_buy_volume": "float64",
    "taker_buy_quote_volume": "float64",
}
```

#### Data Requirements

1. Index Properties:
   - pd.DatetimeIndex in UTC timezone
   - Monotonically increasing
   - No duplicates
   - Aligned to interval boundaries

2. Column Requirements:
   - All OUTPUT_DTYPES columns present
   - Exact dtype matching
   - Consistent ordering
   - Normalized naming

### 3. Cache Layer

#### Storage Implementation

The cache uses a structured directory layout:

```tree
/cache_dir
    /data
        /SYMBOL
            /INTERVAL
                /YYYYMM.arrow
    /metadata
        cache_index.json
```

Features:

- Apache Arrow MMAP for zero-copy reads
- Monthly file partitioning by symbol and interval
- JSON-based metadata tracking
- Checksum validation for data integrity
- Memory-mapped file access for efficiency
- Selective column loading capability

#### Cache Management

```python
# Cache Statistics
async def get_cache_stats() -> Dict[str, int]:
    """Get cache performance metrics.
    
    Returns:
        {
            "hits": int,    # Successful cache retrievals
            "misses": int,  # Cache misses requiring source fetch
            "errors": int   # Cache validation or loading errors
        }
    """

# Cache Validation
async def validate_cache_integrity(
    symbol: str,
    interval: str,
    date: datetime
) -> Tuple[bool, Optional[str]]:
    """Validate cache integrity for specific data.
    
    Validates:
    1. Cache existence
    2. Data format and structure
    3. Column presence and types
    4. Index properties and timezone
    """

# Cache Repair
async def repair_cache(
    symbol: str,
    interval: str,
    date: datetime
) -> bool:
    """Attempt to repair corrupted cache entry.
    
    Process:
    1. Invalidate corrupted entry
    2. Refetch from source
    3. Save to cache
    4. Verify repair success
    """
```

### 4. Time Handling Layer

#### Core Principle

The system employs strict **inclusive start** and **inclusive end** timing for 1-second intervals:

- Mathematical notation: [start, end]
- Precise alignment with exchange data bars
- No overlapping or missing intervals

#### Time Window Adjustments

```python
def _validate_dates(self, start_time: datetime, end_time: datetime) -> None:
    """Validate date ranges for data retrieval.
    
    Validations:
    1. End time must not be in the future
    2. Start time must be before end time
    """
    now = datetime.now(timezone.utc)
    if end_time > now:
        raise ValueError(f"End time {end_time} is in the future")
    if start_time > end_time:
        raise ValueError(f"Start time {start_time} is after end time {end_time}")

def _estimate_data_points(self, start_time: datetime, end_time: datetime, interval: Interval) -> int:
    """Estimate number of data points for a time range.
    
    Rules:
    - SECOND_1: total_seconds
    - MINUTE_1: total_seconds // 60
    """
    time_diff = end_time - start_time
    if interval == Interval.SECOND_1:
        return int(time_diff.total_seconds())
    elif interval == Interval.MINUTE_1:
        return int(time_diff.total_seconds()) // 60
    else:
        raise ValueError(f"Unsupported interval: {interval}")
```

#### Example Adjustments

```python
# Input with microsecond precision
start = datetime(2024, 3, 15, 12, 34, 56, 500_000, tzinfo=timezone.utc)  # 12:34:56.500
end = datetime(2024, 3, 15, 12, 35, 2, 123_456, tzinfo=timezone.utc)     # 12:35:02.123

# Adjusted to aligned [start, end] interval:
start_adjusted = datetime(2024, 3, 15, 12, 34, 57, tzinfo=timezone.utc)  # Round up to next second
end_adjusted = datetime(2024, 3, 15, 12, 35, 2, tzinfo=timezone.utc)     # Round down to second
```

## Usage Guide

### 1. Basic Interface

#### Constructor

```python
def __init__(
    market_type: MarketType = MarketType.SPOT,  # Type of market (SPOT, FUTURES, etc.)
    rest_client: Optional[EnhancedRetriever] = None,  # Optional pre-configured REST client
    vision_client: Optional[VisionDataClient] = None,  # Optional pre-configured Vision client
    cache_dir: Optional[Path] = None,  # Directory for caching data
    use_cache: bool = True,  # Whether to use caching
)
```

#### Main Method

```python
async def get_data(
    symbol: str,                                # Trading pair (e.g., "BTCUSDT")
    interval: Interval = Interval.SECOND_1,     # Time interval
    start_time: datetime,                       # Start time in UTC
    end_time: datetime,                         # End time in UTC
    use_cache: bool = True,                     # Whether to use caching
    enforce_source: DataSource = DataSource.AUTO # Force specific data source
) -> pd.DataFrame:
    """Get market data from the most appropriate source."""
```

### 2. Data Source Selection

#### Selection Logic

```python
def _should_use_vision_api(self, start_time: datetime, end_time: datetime, interval: Interval) -> bool:
    """Determine if Vision API should be used based on request parameters.
    
    Decision Factors:
    1. Data size: Use Vision for requests > REST_CHUNK_SIZE * REST_MAX_CHUNKS
    2. Default: Try Vision first with REST fallback
    """
    estimated_points = self._estimate_data_points(start_time, end_time, interval)

    # Rule 1: Always use Vision API for large requests
    if estimated_points > self.REST_CHUNK_SIZE * self.REST_MAX_CHUNKS:
        return True

    # Rule 2: Try Vision API first for all other cases
    return True
```

#### Vision API Characteristics

- Advantages:
  - Best for bulk historical data (>10,000 points)
  - No rate limits
  - More stable for large requests
- Limitations:
  - 36-hour data delay (VISION_DATA_DELAY_HOURS)
  - Potential gaps in data
  - Different column naming (handled by _format_dataframe)

#### REST API Characteristics

- Advantages:
  - Real-time data access
  - Immediate availability
  - Direct from source
- Limitations:
  - Rate limits apply
  - Max 10,000 points per request (REST_CHUNK_SIZE * REST_MAX_CHUNKS)
  - Higher failure rate for large requests

#### Fallback Mechanism

```python
async def _fetch_from_source(
    self,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
    use_vision: bool = True
) -> pd.DataFrame:
    """Fetch data with automatic fallback.
    
    Process:
    1. Try Vision API if use_vision=True
    2. Fall back to REST API if:
       - Vision API returns no data
       - Vision API fails
       - use_vision=False
    3. Format and validate results
    """
```

### 3. Best Practices

1. Source Selection:
   - Use AUTO for optimal selection
   - Force REST for real-time needs
   - Force Vision for large historical

2. Performance:
   - Enable caching for repeats
   - Use appropriate time windows
   - Mind chunk size limits

3. Error Handling:
   - Validate date ranges
   - Check empty results
   - Handle rate limits

### 4. Example Usage

```python
# Recent Data
data = await manager.get_data(
    symbol="BTCUSDT",
    start_time=datetime.now(timezone.utc) - timedelta(hours=1),
    end_time=datetime.now(timezone.utc)
)

# Historical Data with Vision
data = await manager.get_data(
    symbol="BTCUSDT",
    start_time=start_time,
    end_time=end_time,
    enforce_source=DataSource.VISION
)
```

## Testing & Quality Assurance

### Test Configuration

```python
# Test Setup
@pytest.fixture
def data_source_manager():
    return DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=Path("./test_cache"),
        use_cache=True
    )

# Test Parameters
SYMBOL = "BTCUSDT"
INTERVAL = Interval.SECOND_1
TEST_WINDOWS = [
    timedelta(minutes=5),
    timedelta(hours=1),
    timedelta(hours=24)
]
```

### Coverage Areas

#### 1. Source Selection Tests

- Auto selection logic
- Vision API enforcement
- REST API enforcement
- Fallback mechanisms
- Data size thresholds

#### 2. Data Validation Tests

- DataFrame structure
- Column presence and types
- Index properties
- Timezone handling
- Empty result handling

#### 3. Cache Management Tests

- Cache hit/miss tracking
- Integrity validation
- Auto-repair functionality
- Concurrent access
- Error recovery

#### 4. Time Handling Tests

- Window adjustments
- Boundary conditions
- Invalid ranges
- Future dates
- Zero-duration requests

#### 5. Integration Tests

- End-to-end workflows
- Error propagation
- Resource cleanup
- Memory management
- Performance metrics

## Future Improvements

1. Caching Enhancements:
   - Implement expiration
   - Add LRU implementation
   - Optimize disk caching

2. Performance Optimization:
   - Dynamic chunk sizing
   - Parallel fetching
   - Progress tracking

3. Monitoring:
   - Performance metrics
   - API usage tracking
   - Cache hit rates
