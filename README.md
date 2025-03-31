# Binance Data Service

## Overview

This package provides efficient market data retrieval from Binance Vision using Apache Arrow MMAP for optimal performance.

## Key Features

- Zero-copy reads with Apache Arrow MMAP
- Automatic caching with efficient partial data loading
- Column-based data access
- Timezone-aware timestamp handling
- Connection limit enforcement (13 concurrent)
- Built-in data validation and integrity checks
- Exponential backoff retry mechanism
- Prefetch capability for data caching
- Cache validation and metadata tracking

## Usage

### Basic Usage

```python
from binance_data_services import DataSourceManager
from datetime import datetime, timezone
from pathlib import Path

# Initialize manager with caching
manager = DataSourceManager(
    cache_dir=Path("/path/to/cache"),
    use_cache=True
)

# Fetch data with automatic source selection
df = await manager.get_data(
    symbol="BTCUSDT",
    start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
    columns=["open", "close", "volume"]  # Optional column selection
)

# DataFrame is indexed by 'open_time' in UTC
print(df.head())
```

### Advanced Usage

```python
from binance_data_services import DataSourceManager, DataSource
from datetime import datetime, timezone
from pathlib import Path

manager = DataSourceManager(cache_dir=Path("/path/to/cache"))

try:
    # Force Vision API for historical data
    df_historical = await manager.get_data(
        symbol="BTCUSDT",
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2024, 1, 6, tzinfo=timezone.utc),
        enforce_source=DataSource.VISION
    )

    # Force REST API for recent data
    df_recent = await manager.get_data(
        symbol="BTCUSDT",
        start_time=datetime(2024, 3, 14, tzinfo=timezone.utc),
        end_time=datetime(2024, 3, 15, tzinfo=timezone.utc),
        enforce_source=DataSource.REST
    )
except Exception as e:
    print(f"Error: {e}")
```

### Efficient Data Loading

```python
from binance_data_services import DataSourceManager
from datetime import datetime, timezone
from pathlib import Path

manager = DataSourceManager(cache_dir=Path("/path/to/cache"))

# Client automatically optimizes based on hardware:
# - Concurrent downloads
# - Memory usage
# - Network bandwidth
# - I/O operations

# Load specific columns for better performance
df = await manager.get_data(
    symbol="BTCUSDT",
    start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
    columns=["open", "high", "low", "close"]  # Only load needed columns
)
```

## Time Boundary Handling

The system implements precise time boundary handling:

- All timestamps are aligned to second boundaries
- Start times with microseconds are rounded UP to the next interval
- End times with microseconds are rounded DOWN to the current interval
- Both start and end boundaries are INCLUSIVE after alignment
- Automatic handling of market open/close times
- Timezone conversion to UTC if needed
- Microsecond precision support in internal calculations
- Consistent handling across data sources

### Example Time Window Behavior

For a time window from `2025-03-17 08:37:25.528448` to `2025-03-17 08:37:30.056345` with 1-second intervals:

- Adjusted start: `2025-03-17 08:37:26.000000` (rounded UP from 25.528448)
- Adjusted end: `2025-03-17 08:37:30.000000` (rounded DOWN from 30.056345)
- Expected records: 5 (seconds 26, 27, 28, 29, 30 inclusive)

## Performance Characteristics

The client automatically optimizes for your hardware:

- CPU: Utilizes available cores for parallel processing
- Memory: Adjusts concurrent operations based on available RAM
- Network: Measures bandwidth and adapts request concurrency
- I/O: Monitors system load and adjusts operations accordingly
- Endpoints: Load balances across multiple Binance endpoints

Typical performance metrics:

- Single-day data retrieval: ~1-2 seconds with cache
- Multi-day cached reads: ~0.2-0.3 seconds per day
- Vision API preferred for data older than 36 hours
- REST API used for recent data with automatic chunking
- Memory usage: ~100MB per day of data

## API Reference

### VisionDataClient

#### Constructor Parameters

- `symbol: str` - Trading pair symbol (e.g., "BTCUSDT")
- `interval: str = "1s"` - Time interval (only "1s" is supported)
- `cache_dir: Optional[Path] = None` - Directory for caching data
- `max_concurrent_downloads: int = 13` - Maximum concurrent downloads (None for auto-detect)
- `use_cache: bool = False` - Whether to enable caching mechanisms (disabled by default)

#### Methods

- `fetch(start_time: datetime, end_time: datetime, columns: Optional[List[str]] = None) -> TimestampedDataFrame`

  - Retrieves market data for the specified time range
  - Supports column selection for efficient data loading
  - Returns timezone-aware DataFrame indexed by open_time
  - Validates time boundaries and data integrity
  - Handles cache hits/misses automatically
  - Automatically retries on network errors
  - Supports partial data loading from cache

- `prefetch(start_time: datetime, end_time: datetime, max_days: int = 5) -> None`
  - Prefetches data in background for future use
  - Improves performance for subsequent fetch calls
  - Limited to max_days to prevent excessive downloads
  - Handles failures gracefully without blocking
  - Supports concurrent prefetch operations

#### Available Intervals

Only 1-second interval ("1s") is supported by this client.

## Architecture

The system implements a layered architecture with a mediator pattern at its core:

1. Mediator Layer (`DataSourceManager`):

   - Smart source selection between REST and Vision APIs
   - 36-hour threshold for Vision API preference
   - Unified data format standardization
   - Centralized caching coordination
   - Automatic time window adjustment
   - Output dtype enforcement
   - Market type handling (SPOT, FUTURES)

2. Data Source Layer:

   - REST API Client (`RestDataClient`):

     - Optimized for recent data (< 36 hours)
     - Chunk-based retrieval (1000 records/chunk)
     - Hardware-aware connection pooling
     - Microsecond-precision timestamps
     - Real-time data validation
     - Automatic retry with backoff

   - Vision API Client:
     - Bulk historical data retrieval
     - Zero-copy Arrow MMAP reads
     - Concurrent download management (13 max)
     - Progress monitoring and recovery
     - Atomic file operations
     - Comprehensive error classification

3. Cache Layer (`UnifiedCacheManager`):

   - Arrow-based columnar storage
   - Monthly file organization
   - JSON metadata tracking
   - SHA-256 integrity checks
   - Partial data loading
   - Cache invalidation logic
   - Memory map lifecycle management

4. Validation Layer:
   - Multi-stage validation pipeline:
     - Symbol format validation
     - Time range boundaries
     - DataFrame structure
     - Cache integrity
     - Data completeness
   - Timezone enforcement (UTC)
   - Index consistency ('open_time')
   - Column type verification
   - Hardware resource monitoring

Each layer maintains clear responsibilities and communicates through well-defined interfaces. The `DataSourceManager` acts as the facade, providing a simplified interface while coordinating complex interactions between layers.

## Error Handling

The client implements comprehensive error handling:

- Network errors: Exponential backoff retry
- Rate limiting: Honors Retry-After headers
- Stalled downloads: Progress monitoring and recovery
- Cache corruption: Automatic invalidation and redownload
- Data integrity: Multi-stage validation
- Memory management: Proper resource cleanup
- Timezone mismatches: Automatic conversion to UTC

## Data Integrity

The client enforces strict data integrity rules:

- SHA-256 checksum verification for downloads
- UTC timezone enforcement throughout
- Strict index name and type validation
- Cache metadata tracking and validation
- Time boundary completeness checks
- Column name and type validation
- Symbol format validation

## Caching and Performance

The library includes a sophisticated caching system that dramatically improves performance for repeated queries.

### Caching Configuration

Caching is enabled by default when using `DataSourceManager`.

```python
# Create a DataSourceManager with caching enabled (default)
manager = DataSourceManager(market_type=MarketType.SPOT)

# Or explicitly configure the cache
from core.cache_manager import UnifiedCacheManager

manager = DataSourceManager(
    cache_manager=UnifiedCacheManager(
        use_cache=True,
        cache_dir=Path("./custom_cache_directory")
    ),
    market_type=MarketType.SPOT
)
```

The caching system automatically:

- Stores data to disk in an efficient format
- Validates data integrity on retrieval
- Handles cache invalidation for stale data
- Optimizes memory usage with memory-mapped files

For more details on caching, see the [Caching Documentation](docs/caching/).

## Dependencies

- Python 3.8+
- pandas
- pyarrow
- httpx
- aiohttp
- psutil (for hardware monitoring)
- tenacity (for retry logic)
- colorlog (for colored console logging)
- typing_extensions

## Cache Directory Structure

```tree
cache_dir/
├── BTCUSDT/
│   ├── 1s/
│   │   ├── 202403.arrow
│   │   └── 202404.arrow
│   └── metadata.json
└── ETHUSDT/
    ├── 1s/
    │   └── 202403.arrow
    └── metadata.json
```

## Recent Improvements

### Download-First Approach for Vision API

We've implemented the download-first approach for Vision API based on benchmark testing. This approach:

- Directly attempts to download files without pre-checking file existence
- Significantly reduces latency by eliminating unnecessary HEAD requests
- Improves performance by dynamically adjusting concurrency based on batch size

### Automatic Fallback from Vision to REST API

The system now automatically falls back to REST API when Vision API fails or returns no data:

1. First attempts to retrieve data from Vision API (for historical data)
2. If Vision API fails or returns empty data, automatically tries REST API
3. Caches data from whichever source successfully returns it
4. Returns data to the client from the successful source

## Architecture Update

The Binance Data Services library provides a unified interface for retrieving market data from both REST and Vision APIs, with intelligent source selection and automatic caching.

Main components:

- DataSourceManager: Central coordinator for data source selection and caching
- VisionDataClient: Client for historical data from Binance Vision API
- RestDataClient: Client for real-time and recent data from Binance REST API
- UnifiedCacheManager: Manages caching of data from all sources
