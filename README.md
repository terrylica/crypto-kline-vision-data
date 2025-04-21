# Binance Data Service

A high-performance, robust package for efficient market data retrieval from multiple data providers, including Binance Vision, using Apache Arrow MMAP for optimal performance.

## Important Documentation

- **[DSM Demo Documentation](docs/dsm_demo/README.md)**: Interactive demonstration of the Failover Control Protocol mechanism, the core data retrieval strategy that ensures robust and efficient data collection from multiple sources.

## Operational

1. Initialization

- Execute `scripts/binance_vision_api_aws_s3/fetch_binance_data_availability.sh` to build `scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv`
- The archaic word _synchronal_ contextually means the Binance Exchanges crypto base pair that we're interested in monitoring, because they must be active in the SPOT, UM and CM market of the Binance Exchange.
- `scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv` contains only the Binance SPOT market symbols, their earliest date available, and their available intervals (i.e. 1s, 1m, 3m, ..., 1d), and which base pairs (e.g. BTC) are also on the UM and CM markets.

1. Shortlisting

- To exclude specific symbols from subsequent operations below, simply remove their corresponding lines from `spot_synchronal.csv`

1. Failover Control Protocol (FCP) Mechanism

   - Run `./examples/sync/dsm_demo.py` to understand the FCP mechanism through a live demonstration
   - This interactive tool shows how data is retrieved intelligently from local cache first, then Vision API for historical data, and finally REST API for the most recent data
   - Type `./examples/sync/dsm_demo.py -h` for detailed usage and options
   - Generate comprehensive documentation with `./examples/sync/dsm_demo.py -gd`

1. Cache Building

- Execute `to-be-constructed.sh` to build `.cache/BINANCE/KLINES/...`
- Locally hosted cached files organized in a hierarchical structure using Apache Arrow format for high-performance columnar memory-mapped access, enabling efficient zero-copy reads and optimal performance for time series analysis.
- Refer to [Arrow Cache Builder documentation](scripts/arrow_cache/README.md) for more information.

## Features

- **Multi-Provider Support**: Supports multiple data providers (currently Binance with TradeStation coming soon)
- **Multiple Chart Types**: Supports various chart types including KLines and Funding Rates
- **Zero-copy Reads**: Uses Apache Arrow memory-mapped files for efficient data access
- **Automatic Caching**: Intelligent caching system with provider and chart type awareness
- **Timezone-aware Timestamps**: All timestamps are UTC with proper timezone handling
- **Connection Limit Enforcement**: Built-in rate limiting to prevent API throttling
- **Data Validation**: Comprehensive validation of all data sources
- **Exponential Backoff**: Retry mechanism for transient failures
- **Factory Pattern**: Extensible architecture for adding new data providers and chart types
- **Pythonic Interface**: Elegant configuration using dataclasses and factory methods

## Installation

```bash
pip install binance-data-service
```

## Usage

### 1. Fetch Available Data Information

As the first step, run the data availability script to identify which symbols and time intervals are available from Binance:

```bash
cd scripts/binance_vision_api_aws_s3
./fetch_binance_data_availability.sh -o reports -t --markets spot,um,cm -p 5
```

This script scans the Binance Vision API repository and generates:

- Market-specific reports with earliest available dates for each symbol
- A crucial filtered list: `scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv`

The `scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv` file is especially valuable as it contains SPOT market symbols that also have corresponding instruments in both UM (USDT-M futures) and CM (COIN-M futures) perpetual markets. This ensures you can work with instruments that have data available across all three market types.

The script identifies available intervals for each symbol, though it typically reports intervals up to 1d (daily) even when higher granularity intervals like 1w (weekly) are available from the Binance Vision API.

#### Understanding the Generated Files

1. **Market-specific reports**: `scripts/binance_vision_api_aws_s3/reports/spot_earliest_dates.csv`, `scripts/binance_vision_api_aws_s3/reports/um_earliest_dates.csv`, `scripts/binance_vision_api_aws_s3/reports/cm_earliest_dates.csv`

   - These files contain all available symbols for each market type with their earliest available data dates.

2. **Filtered lists**:

   - `scripts/binance_vision_api_aws_s3/reports/spot_um_usdt_filtered.csv` - Symbols available in both SPOT and UM (USDT-M) markets
   - `scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv` - Symbols available across all three market types (SPOT, UM, CM)

3. **Consolidated file**:
   - `scripts/binance_vision_api_aws_s3/reports/consolidated_base_symbols.csv` - A comprehensive view of base symbols and their availability across all markets

The `scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv` file is particularly valuable as it ensures you can work with instruments that have consistent data availability across all market types.

### 2. Simple Approach

```python
from datetime import datetime, timezone, timedelta
from pathlib import Path
from utils.market_constraints import Interval, MarketType, ChartType, DataProvider
from core.sync.data_source_manager import DataSourceManager

# Set time range for data retrieval
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=5)

def basic_example():
    # Initialize DataSourceManager using the factory method
    with DataSourceManager.create(
        market_type=MarketType.SPOT,  # Required: Specify market type
        cache_dir=Path("./cache"),     # Optional: Specify cache directory
    ) as dsm:
        # Fetch data
        df = dsm.get_data(
            symbol="BTCUSDT",          # Required: Trading pair symbol
            start_time=start_time,     # Required: Start time (UTC)
            end_time=end_time,         # Required: End time (UTC)
            interval=Interval.HOUR_1,  # Optional: Time interval (default: SECOND_1)
        )

        print(f"Retrieved {len(df)} records")
        print(df.head())
```

### Elegant Configuration Approach

```python
from datetime import datetime, timezone, timedelta
from pathlib import Path
from utils.market_constraints import Interval, MarketType, ChartType, DataProvider
from core.sync.data_source_manager import DataSourceManager

# Set time range for data retrieval
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=5)

def config_example():
    # Create manager configuration
    config = DataSourceConfig.create(
        market_type=MarketType.SPOT,   # Required: Market type
        cache_dir=Path("./cache"),     # Optional: Cache directory
        use_httpx=True,                # Optional: Kept for backward compatibility, httpx is always used
    )

    # Initialize DataSourceManager with configuration
    with DataSourceManager.create(
        MarketType.SPOT,               # Shorthand for market_type
        **config.__dict__,             # Pass all config values
    ) as dsm:
        # Create query configuration
        query = DataQueryConfig.create(
            symbol="BTCUSDT",          # Required: Trading pair
            start_time=start_time,     # Required: Start time (UTC)
            end_time=end_time,         # Required: End time (UTC)
            interval=Interval.HOUR_1,  # Optional: Time interval
        )

        # Query data with configuration object
        df = dsm.query_data(query)

        print(f"Retrieved {len(df)} records")
        print(df.head())
```

### Configuring Default Market Type

You can configure a global default market type to simplify code when working primarily with one market type:

```python
from utils.market_constraints import MarketType
from core.sync.data_source_manager import DataSourceManager

# Configure FUTURES_USDT as the default market type for all new instances
DataSourceManager.configure_defaults(MarketType.FUTURES_USDT)

def futures_example():
    # Create manager with default market type (FUTURES_USDT)
    with DataSourceManager.create() as dsm:
        # Fetch futures data
        df = dsm.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
        )

        print(f"Retrieved {len(df)} FUTURES_USDT records")

    # You can still override the default when needed
    with DataSourceManager.create(MarketType.SPOT) as spot_dsm:
        # Fetch spot data
        spot_df = spot_dsm.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
        )

        print(f"Retrieved {len(spot_df)} SPOT records")
```

### Fetching Funding Rate Data

```python
from datetime import datetime, timezone, timedelta
from pathlib import Path
from utils.market_constraints import Interval, MarketType, ChartType, DataProvider
from core.sync.data_source_manager import DataSourceManager

# Set time range for data retrieval
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=5)

def funding_rate_example():
    # Initialize DataSourceManager for funding rate data
    with DataSourceManager.create(
        market_type=MarketType.FUTURES_USDT,
        chart_type=ChartType.FUNDING_RATE,
        cache_dir=Path("./cache"),
        use_httpx=True,  # Kept for backward compatibility, httpx is always used
    ) as dsm:
        # Fetch funding rate data
        df = dsm.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_8,  # Funding rates typically use 8h interval
        )

        print(f"Retrieved {len(df)} funding rate records")
        print(df.head())
```

### Working with Multiple Data Types

```python
def multi_data_example():
    # Create a DataSourceManager that can handle all data types
    with DataSourceManager.create(
        market_type=MarketType.FUTURES_USDT,
        cache_dir=Path("./cache"),
    ) as dsm:
        # Create query for price data
        klines_query = DataQueryConfig.create(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_1,
            chart_type=ChartType.KLINES,  # Explicitly specify chart type
        )

        # Create query for funding rate data
        funding_query = DataQueryConfig.create(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_8,
            chart_type=ChartType.FUNDING_RATE,  # Explicitly specify chart type
        )

        # Fetch both data types from the same manager
        klines_df = dsm.query_data(klines_query)
        funding_df = dsm.query_data(funding_query)

        print(f"Retrieved {len(klines_df)} kline records and {len(funding_df)} funding rate records")
```

### Forcing Specific Data Sources

```python
from core.sync.data_source_manager import DataSource

def force_data_source_example():
    with DataSourceManager.create(MarketType.SPOT) as dsm:
        # Create query that forces REST API usage
        rest_query = DataQueryConfig.create(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            enforce_source=DataSource.REST,  # Force REST API for recent data
        )

        # Create query that forces Vision API usage
        vision_query = DataQueryConfig.create(
            symbol="ETHUSDT",
            start_time=start_time - timedelta(days=90),  # Historical data
            end_time=end_time - timedelta(days=89),
            enforce_source=DataSource.VISION,  # Force Vision API for historical data
        )

        # Execute both queries
        rest_df = dsm.query_data(rest_query)
        vision_df = dsm.query_data(vision_query)
```

## Performance Characteristics

- **Memory Efficiency**: Zero-copy reads minimize memory usage
- **High Throughput**: Arrow-based columnar storage optimizes data access patterns
- **Optimal Caching**: Hierarchical cache structure optimizes storage and retrieval

## API Reference

### DataSourceManager

```python
# Create a DataSourceManager (recommended factory method)
manager = DataSourceManager.create(
    market_type=MarketType.SPOT,  # Required: Market type
    provider=DataProvider.BINANCE,  # Optional: Data provider
    chart_type=ChartType.KLINES,  # Optional: Chart type
    cache_dir=Path("./cache"),  # Optional: Cache directory
    use_cache=True,  # Optional: Whether to use cache
    max_concurrent=50,  # Optional: Max concurrent requests
    retry_count=5,  # Optional: Number of retries
    max_concurrent_downloads=None,  # Optional: Max concurrent downloads
    use_httpx=True,  # Optional: Kept for backward compatibility, httpx is always used
)

# Query data with the manager
df = manager.get_data(
    symbol="BTCUSDT",  # Required: Trading pair symbol
    start_time=datetime(...),  # Required: Start time (UTC)
    end_time=datetime(...),  # Required: End time (UTC)
    interval=Interval.MINUTE_1,  # Optional: Time interval
    use_cache=True,  # Optional: Whether to use cache
    enforce_source=DataSource.AUTO,  # Optional: Force specific data source
    provider=None,  # Optional: Override provider
    chart_type=None,  # Optional: Override chart type
)

# Using Config objects (more Pythonic approach)
query_config = DataQueryConfig.create(
    symbol="BTCUSDT",  # Required: Trading pair symbol
    start_time=datetime(...),  # Required: Start time (UTC)
    end_time=datetime(...),  # Required: End time (UTC)
    interval=Interval.MINUTE_1,  # Optional: Time interval
)
df = manager.query_data(query_config)
```

## Architecture

The system is built with a layered architecture:

1. **Mediator Layer**: `DataSourceManager` orchestrates data access across providers and chart types
2. **Client Factory Layer**: `DataClientFactory` creates appropriate clients for each provider/chart type combination
3. **Data Client Layer**: Implements provider-specific API access logic
4. **Cache Layer**: Efficiently stores retrieved data with provider and chart type awareness
5. **Time Handling Layer**: Manages timezone-aware timestamps
6. **Configuration Layer**: Provides clean, Pythonic interfaces for configuration

## Error Handling

The system provides comprehensive error handling:

- **ApiError**: Represents errors from data provider APIs
- **CacheError**: Represents errors in cache operations
- **ValidationError**: Represents data validation failures
- **RequestError**: Represents errors in HTTP requests

All errors are properly propagated and logged with detailed context.

## Data Integrity

Data integrity is maintained through:

1. **Boundary Validation**: Validates time boundaries for data consistency
2. **Schema Validation**: Ensures data structure matches expected schema
3. **Type Validation**: Validates data types for all columns
4. **Missing Data Detection**: Identifies and reports missing data points

## Extending the Framework

### Adding a New Data Provider

1. Add the provider to the `DataProvider` enum in `market_constraints.py`
2. Create a new client implementation that implements `DataClientInterface`
3. Register the client with the `DataClientFactory`

### Adding a New Chart Type

1. Add the chart type to the `ChartType` enum in `market_constraints.py`
2. Add appropriate column definitions and DTYPEs in `config.py`
3. Create a client implementation for the new chart type
4. Register the client with the `DataClientFactory`

## License

MIT
