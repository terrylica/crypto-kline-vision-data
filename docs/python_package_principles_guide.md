# Applying Python Package Principles to Data Source Manager

This guide provides a step-by-step approach to applying Python package principles to the Data Source Manager codebase. It includes specific examples from the codebase and practical advice for implementation.

## Getting Started

### 1. Prioritize Modules for Documentation

Start by prioritizing modules for documentation enhancement:

1. **Core API Modules**: Focus first on modules directly exposed to users

   - `core/sync/dsm_lib.py` (contains `fetch_market_data`)
   - `core/sync/data_source_manager.py` (core implementation)

2. **Public Interface Modules**: Modules that define public interfaces

   - `utils/market_constraints.py` (defines enums used in the API)
   - `utils/dataframe_types.py` (defines data types)

3. **Example Modules**: Modules that demonstrate usage
   - `examples/sync/dsm_demo_cli.py`
   - `examples/lib_module/dsm_demo_module.py`

### 2. Update Package-Level Documentation

Ensure the top-level `__init__.py` file provides a clear overview of the package:

```python
"""Data Source Manager package for efficient market data retrieval.

This package provides tools for downloading and caching market data from Binance Vision.
The primary interface is the fetch_market_data function, which implements the
Failover Control Protocol for robust data retrieval from multiple sources.

Key Features:
- Efficient data retrieval using Apache Arrow MMAP
- Automatic caching with zero-copy reads
- Progressive data retrieval from multiple sources
- Timezone-aware timestamp handling
- Column-based data access

Example:
    >>> from data_source_manager import fetch_market_data, MarketType, DataProvider, Interval, ChartType
    >>> from datetime import datetime
    >>>
    >>> df, elapsed_time, records_count = fetch_market_data(
    ...     provider=DataProvider.BINANCE,
    ...     market_type=MarketType.SPOT,
    ...     chart_type=ChartType.KLINES,
    ...     symbol="BTCUSDT",
    ...     interval=Interval.MINUTE_1,
    ...     start_time=datetime(2023, 1, 1),
    ...     end_time=datetime(2023, 1, 10),
    ...     use_cache=True,
    ... )
"""

# Import and expose public API
from core.providers.binance.vision_data_client import VisionDataClient
from core.sync.data_source_manager import DataSource, DataSourceConfig
from core.sync.dsm_lib import fetch_market_data
from utils.dataframe_types import TimestampedDataFrame
from utils.market_constraints import ChartType, DataProvider, Interval, MarketType

__all__ = [
    "ChartType",
    "DataProvider",
    "DataSource",
    "DataSourceConfig",
    "Interval",
    "MarketType",
    "TimestampedDataFrame",
    "VisionDataClient",
    "fetch_market_data",
]
```

## Module Documentation

### Example: Enhancing a Core Module

Here's how to enhance documentation for a core module like `dsm_lib.py`:

```python
"""Data Source Manager library interface module.

This module provides the primary high-level interface for the Data Source Manager,
implementing the Failover Control Protocol (FCP) for robust data retrieval.

The FCP mechanism consists of three integrated phases:
1. Local Cache Retrieval: Quickly obtain data from local Apache Arrow files
2. Vision API Retrieval: Supplement missing data segments from Vision API
3. REST API Fallback: Ensure complete data coverage for any remaining segments

The main entry point is the fetch_market_data function, which orchestrates
data retrieval from all available sources based on the provided parameters.

Example:
    >>> from data_source_manager import fetch_market_data, MarketType, DataProvider, Interval, ChartType
    >>> from datetime import datetime
    >>>
    >>> df, elapsed_time, records_count = fetch_market_data(
    ...     provider=DataProvider.BINANCE,
    ...     market_type=MarketType.SPOT,
    ...     chart_type=ChartType.KLINES,
    ...     symbol="BTCUSDT",
    ...     interval=Interval.MINUTE_1,
    ...     start_time=datetime(2023, 1, 1),
    ...     end_time=datetime(2023, 1, 10),
    ...     use_cache=True,
    ... )
"""

# Import statements...

def fetch_market_data(
    provider: DataProvider,
    market_type: MarketType,
    chart_type: ChartType,
    symbol: str,
    interval: Interval,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    days: Optional[int] = None,
    use_cache: bool = True,
) -> Tuple[Optional[TimestampedDataFrame], float, int]:
    """Fetch market data using the Failover Control Protocol.

    This function retrieves market data from multiple sources using a progressive
    approach that prioritizes speed and reliability:
    1. First attempts to retrieve data from local cache (if use_cache=True)
    2. Then retrieves missing data from Vision API
    3. Finally falls back to REST API for any remaining data

    The function handles time range validation, data normalization, and merging
    data from multiple sources into a consistent DataFrame.

    Args:
        provider: The data provider (e.g., BINANCE)
        market_type: Type of market (SPOT, UM, CM)
        chart_type: Type of chart data (KLINES, etc.)
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: Time interval (e.g., MINUTE_1, HOUR_1)
        start_time: Start datetime (UTC)
        end_time: End datetime (UTC)
        days: Number of days to fetch (backward from end_time)
        use_cache: Whether to use the local cache

    Returns:
        Tuple containing:
        - DataFrame with market data (or None if error)
        - Elapsed time in seconds
        - Number of records retrieved

    Raises:
        ValueError: If time parameters are invalid or incompatible
        RuntimeError: If data cannot be retrieved from any source

    Example:
        >>> df, elapsed_time, count = fetch_market_data(
        ...     provider=DataProvider.BINANCE,
        ...     market_type=MarketType.SPOT,
        ...     chart_type=ChartType.KLINES,
        ...     symbol="BTCUSDT",
        ...     interval=Interval.MINUTE_1,
        ...     start_time=datetime(2023, 1, 1),
        ...     end_time=datetime(2023, 1, 10),
        ...     use_cache=True
        ... )
    """
    # Function implementation...
```

### Example: Enhancing Configuration Objects

For configuration objects, use `attrs` with validators and clear documentation:

```python
@attr.s(auto_attribs=True, slots=True, frozen=True)
class DataSourceConfig:
    """Configuration for data source retrieval.

    This class encapsulates the configuration for data source retrieval,
    including source preferences and cache settings.

    Attributes:
        provider: The data provider (e.g., BINANCE)
        market_type: Type of market (SPOT, UM, CM)
        chart_type: Type of chart data (KLINES, etc.)
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: Time interval (e.g., MINUTE_1, HOUR_1)
        use_cache: Whether to use the local cache
        retry_attempts: Number of retry attempts for API calls
        cache_path: Path to the cache directory
        timeout_seconds: Timeout for network operations in seconds

    Example:
        >>> config = DataSourceConfig(
        ...     provider=DataProvider.BINANCE,
        ...     market_type=MarketType.SPOT,
        ...     chart_type=ChartType.KLINES,
        ...     symbol="BTCUSDT",
        ...     interval=Interval.MINUTE_1,
        ... )
    """

    provider: DataProvider = attr.ib(
        validator=attr.validators.instance_of(str),
    )
    market_type: MarketType = attr.ib(
        validator=attr.validators.instance_of(str),
    )
    chart_type: ChartType = attr.ib(
        validator=attr.validators.instance_of(str),
    )
    symbol: str = attr.ib(
        validator=attr.validators.instance_of(str),
    )
    interval: Interval = attr.ib(
        validator=attr.validators.instance_of(str),
    )
    use_cache: bool = attr.ib(
        default=True,
        validator=attr.validators.instance_of(bool),
    )
    retry_attempts: int = attr.ib(
        default=3,
        validator=[
            attr.validators.instance_of(int),
            lambda _, __, value: value > 0,
        ],
    )
    cache_path: str = attr.ib(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of(str)),
    )
    timeout_seconds: int = attr.ib(
        default=30,
        validator=[
            attr.validators.instance_of(int),
            lambda _, __, value: value > 0,
        ],
    )
```

## CLI Documentation

### Example: Enhancing a CLI Tool

For CLI tools, ensure help text is comprehensive:

```python
import typer
from typing import Optional
from datetime import datetime

app = typer.Typer(
    help="Data Source Manager CLI demo tool",
    add_completion=False,
)

@app.command()
def main(
    symbol: str = typer.Option(
        "BTCUSDT",
        "--symbol", "-s",
        help="Trading pair symbol (e.g., 'BTCUSDT')",
    ),
    interval: str = typer.Option(
        "1m",
        "--interval", "-i",
        help="Time interval (e.g., '1m', '1h')",
    ),
    days: int = typer.Option(
        1,
        "--days", "-d",
        help="Number of days to fetch (backward from end time)",
    ),
    end_date: Optional[datetime] = typer.Option(
        None,
        "--end-date", "-e",
        formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"],
        help="End date (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)",
    ),
    use_cache: bool = typer.Option(
        True,
        "--use-cache/--no-cache", "-c/-nc",
        help="Whether to use the local cache",
    ),
):
    """Fetch and display market data using the Failover Control Protocol.

    This demo tool demonstrates the Data Source Manager's ability to retrieve
    market data from multiple sources using a progressive approach that
    prioritizes speed and reliability:

    1. First attempts to retrieve data from local cache (if use_cache=True)
    2. Then retrieves missing data from Vision API
    3. Finally falls back to REST API for any remaining data

    Example usage:

        dsm-demo-cli -s BTCUSDT -i 1m -d 10

        dsm-demo-cli --symbol ETHUSDT --interval 1h --days 5 --no-cache
    """
    # Command implementation...
```

## Step-by-Step Implementation Process

### 1. Audit the Codebase

Use the checklist to audit each module:

```bash
# Create a file to track progress
touch docs/documentation_audit.md

# Use the checklist to review each module
```

### 2. Prioritize Enhancements

Create a prioritized list of modules to enhance:

1. Public API modules
2. Core implementation modules
3. Utility modules
4. Example and demo modules

### 3. Enhance Module-Level Docstrings

For each module, update the module-level docstring to include:

- Purpose and functionality
- Key classes and functions
- Usage examples
- How it fits into the broader package

### 4. Enhance Function and Class Docstrings

For each function and class, update docstrings to include:

- Purpose and functionality
- Parameters, return values, and exceptions
- Usage examples
- Implementation notes where appropriate

### 5. Add Type Hints

Ensure all functions and methods have appropriate type hints.

### 6. Enhance CLI Help

Update CLI tools to provide comprehensive help text and follow consistent patterns.

### 7. Update Examples

Ensure examples are up-to-date and demonstrate best practices.

### 8. Update Package-Level Documentation

Update `__init__.py` and README.md to provide a clear overview of the package.

## Resources

- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [PEP 257: Docstring Conventions](https://www.python.org/dev/peps/pep-0257/)
- [PEP 484: Type Hints](https://www.python.org/dev/peps/pep-0484/)
- [attrs Documentation](https://www.attrs.org/en/stable/)
- [typer Documentation](https://typer.tiangolo.com/)
