# Data Source Manager

A high-performance, robust package for efficient market data retrieval from multiple data providers, including [Binance Vision](https://data.binance.vision/) and Binance REST ([Spot](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints), [USDS-Margined Futures](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info), [Coin-Margined Futures](https://developers.binance.com/docs/derivatives/coin-margined-futures/general-info)) using Apache Arrow MMAP for optimal performance.

## Features

- **Failover Control Protocol (FCP)**: Robust data retrieval from multiple sources
- **Local Cache**: Fast access to previously downloaded data using Apache Arrow
- **Vision API**: Efficient historical data from Binance Vision API on AWS S3
- **REST API**: Real-time and recent data from Binance REST API
- **Automatic Retry**: Built-in retry logic with exponential backoff
- **Data Validation**: Comprehensive data integrity checks
- **Rich Logging**: Beautiful, configurable logging with loguru support
- **Professional Package Structure**: Proper src-layout with clean namespace imports

## Package Structure

Data Source Manager follows modern Python packaging standards with a clean src-layout structure:

```
data-source-manager/
├── src/
│   └── data_source_manager/        # Main package namespace
│       ├── __init__.py             # Public API exports (lazy loading)
│       ├── core/                   # Core functionality
│       │   ├── sync/               # Synchronous data managers
│       │   │   ├── data_source_manager.py  # Main DSM class with FCP
│       │   │   ├── dsm_types.py            # DataSource, DataSourceConfig
│       │   │   └── dsm_lib.py              # High-level fetch functions
│       │   └── providers/          # Data provider implementations
│       │       └── binance/        # Binance-specific clients
│       └── utils/                  # Utility modules
│           ├── market_constraints.py  # Enums: DataProvider, MarketType, Interval
│           └── loguru_setup.py        # Logging configuration
├── examples/                       # Usage examples and demos
├── tests/                          # Test suite
└── docs/                           # Documentation
```

**Note**: The public API returns pandas DataFrames for compatibility with downstream consumers. Polars is used internally for some performance-critical operations.

## Installation

There are two main ways to install Data Source Manager, depending on your needs:

### 1. For Development or Running Demos Directly

If you want to run the provided demos directly from the cloned repository or use the core library while having the source files available in your workspace, follow these steps:

```bash
# Clone the repository
git clone https://github.com/terrylica/data-source-manager.git
cd data-source-manager

# Install with uv (recommended, 10-100x faster than pip)
uv sync --dev

# Or with pip (slower, not recommended)
pip install -e ".[dev]"
```

**Note**: This project uses [uv](https://docs.astral.sh/uv/) for package management. Install it via `curl -LsSf https://astral.sh/uv/install.sh | sh`.

This method keeps all the source files in your workspace and includes necessary tools for development workflows.

### 2. As a Dependency in Your Project (`pyproject.toml`)

If you want to use Data Source Manager as a library in your own Python project (managed with `pyproject.toml`) without including its entire source code in your project's directory, you can add it as a Git dependency.

Add the following to your project's `pyproject.toml` file under the `[project.dependencies]` array (as per PEP 621):

```toml
[project]
# ... other project configurations like name, version ...
dependencies = [
    # ... other dependencies ...
    "data-source-manager @ git+https://github.com/terrylica/data-source-manager.git"
    # You can also specify a particular branch, tag, or commit hash:
    # "data-source-manager @ git+https://github.com/terrylica/data-source-manager.git@main"
    # "data-source-manager @ git+https://github.com/terrylica/data-source-manager.git@<version>"
]
```

This will install Data Source Manager into your Python environment's `site-packages` directory, keeping your project workspace clean.

**Note on CLI Tools:**
The installation process (through either method) automatically registers the CLI commands (`dsm-demo-cli` and `dsm-demo-module`) as executable scripts in your Python environment. These commands will be available in your terminal after successful installation.

## Usage

### Basic Usage

```python
from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval
from datetime import datetime, timedelta, timezone

# Create a manager for USDT-margined futures
manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

# Fetch recent BTCUSDT data with automatic failover
# IMPORTANT: Always use UTC timezone-aware datetimes
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=7)

df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start_time,
    end_time=end_time,
    interval=Interval.HOUR_1
)

print(f"Loaded {len(df)} bars of BTCUSDT data")
print(df.head())
manager.close()  # Always close when done
```

### Failover Control Protocol (FCP)

The DSM automatically handles data retrieval through multiple sources:

```python
# The FCP follows this sequence automatically:
# 1. 🚀 Local cache lookup (fastest)
# 2. 📡 Vision API for historical data (efficient)
# 3. 🔄 REST API fallback (real-time)

# All with automatic retry, data validation, and gap detection
manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

# This single call handles all the complexity:
data = manager.get_data("ETHUSDT", start_time, end_time, Interval.MINUTE_5)
# ✅ Cache checked, Vision API queried, REST API fallback - all automatic!
```

### Advanced Configuration

```python
from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval
from data_source_manager.core.sync.data_source_manager import DataSource

# Force specific data source (bypass FCP)
manager = DataSourceManager.create(
    provider=DataProvider.BINANCE,
    market_type=MarketType.FUTURES_USDT,
    enforce_source=DataSource.VISION,  # Force Vision API only
)

# Multiple market types supported
spot_manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
futures_manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
coin_manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_COIN)
```

## Running the Demos

Once installed, you can run the demos using the command-line tools:

### DSM Demo CLI

The CLI demonstration provides an interactive way to explore the Failover Control Protocol:

```bash
# Run the DSM Demo CLI with default parameters
dsm-demo-cli

# Run with specific parameters (get BTC data for a 10-day period)
dsm-demo-cli -s BTCUSDT -i 1m -d 10

# Get help and see all available options
dsm-demo-cli --help
```

The CLI tool will automatically:

1. Check for data in local cache
2. Try to fetch missing data from Binance Vision API
3. Fall back to REST API for data not available in cache or Vision API
4. Save retrieved data to cache for future use

### DSM Demo Module

The module demo provides a programmatic interface:

```bash
# Run the DSM Demo Module to see examples
dsm-demo-module
```

## Using as a Library

The core data fetching functionality of Data Source Manager is available for direct import and use in your Python projects after installation.

The main function for retrieving market data is `fetch_market_data`.

### Example 1: Fetching with Specific Date Range

```python
from datetime import datetime, timezone
from data_source_manager import fetch_market_data, MarketType, DataProvider, Interval, ChartType

# Define parameters
provider = DataProvider.BINANCE
market_type = MarketType.SPOT
chart_type = ChartType.KLINES
symbol = "BTCUSDT"
interval = Interval.MINUTE_1
# IMPORTANT: Always use UTC timezone-aware datetimes
start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
end_time = datetime(2023, 1, 10, tzinfo=timezone.utc)

# Fetch data (returns pandas DataFrame)
df, elapsed_time, records_count = fetch_market_data(
    provider=provider,
    market_type=market_type,
    chart_type=chart_type,
    symbol=symbol,
    interval=interval,
    start_time=start_time,
    end_time=end_time,
    use_cache=True,
)

# Process results
print(f"Fetched {records_count} records in {elapsed_time:.2f} seconds")
if df is not None:
    print(df.head())
```

### Example 2: Fetching Backward from a Specific End Time

This example demonstrates how to fetch data backward from a precise end time in May 2025:

```python
import pendulum
from data_source_manager import fetch_market_data, MarketType, DataProvider, Interval, ChartType

# Define parameters
provider = DataProvider.BINANCE
market_type = MarketType.SPOT
chart_type = ChartType.KLINES
symbol = "BTCUSDT"
interval = Interval.MINUTE_1

# Define a specific end time with precise minutes and seconds
# Note: Using pendulum for better datetime handling as per project standards
end_time = pendulum.datetime(2025, 5, 15, 13, 45, 30, tz="UTC")  # 2025-05-15 13:45:30 UTC
days = 7  # Fetch 7 days backward from the end time

# Fetch data (no need to specify start_time, it will be calculated)
df, elapsed_time, records_count = fetch_market_data(
    provider=provider,
    market_type=market_type,
    chart_type=chart_type,
    symbol=symbol,
    interval=interval,
    end_time=end_time,
    days=days,
    use_cache=True,
)

# Process results
print(f"Fetched {records_count} records in {elapsed_time:.2f} seconds")
print(f"Date range: {end_time.subtract(days=days).format('YYYY-MM-DD HH:mm:ss.SSS')} to {end_time.format('YYYY-MM-DD HH:mm:ss.SSS')}")
if df is not None:
    print(df.head())
```

You can import `fetch_market_data` directly from the `data_source_manager` package. The necessary enums (`MarketType`, `DataProvider`, `ChartType`, `Interval`, `DataSource`) and `DataSourceConfig` are also exposed at the top level for easy access.

Refer to the source code of `data_source_manager.core.sync.dsm_lib.fetch_market_data` and `data_source_manager.core.sync.data_source_manager.DataSourceConfig` for detailed parameter information and usage.

## Data Source Manager (DSM) Demo

### Quick Start

- **[DSM Demo CLI Documentation](examples/sync/)**: Interactive demonstration of the Failover Control Protocol mechanism, the core data retrieval strategy that ensures robust and efficient data collection from multiple sources.
- **[DSM Demo Module Documentation](examples/lib_module/)**: Programmatic interface to `src/data_source_manager/core/sync/dsm_lib.py` functions, complementing the CLI tool by providing a library approach to implement the same data retrieval functionality in custom applications.

### Understanding Data Sources

The DSM implements a Failover Control Protocol (FCP) that follows this sequence:

1. **Cache**: First checks local Arrow files for requested data
2. **VISION API**: For missing data, attempts to download from Binance Vision API
3. **REST API**: Falls back to Binance REST API for any remaining data gaps

Note that recent data (within ~48 hours) is typically not available in the Vision API and will be retrieved from the REST API.

## Development Guidelines

### Core Principles

- **[Focus DSM FCP Demo Rule](.cursor/rules/focus-dsm-fcp-demo.mdc)**: The authoritative instruction file guiding the Cursor Agent to strictly adhere to the demo plan and maintain focus on the Failover Control Protocol demonstration.
- [scripts/dev](scripts/dev): Contains various scripts for development and maintenance tasks.

## API Documentation

The `docs/api` folder provides in-depth documentation on data source characteristics and retrieval mechanisms. Refer to the [API Documentation Overview](docs/api/) for a summary of the contents in this directory.

## Data Initialization and Shortlisting

1. Initialization
   - Execute `scripts/binance_vision_api_aws_s3/fetch_binance_data_availability.sh` to build `scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv`
   - The archaic word _synchronal_ contextually means the Binance Exchanges crypto base pair that we're interested in monitoring, because they must be active in the SPOT, UM and CM market of the Binance Exchange.
   - `scripts/binance_vision_api_aws_s3/reports/spot_synchronal.csv` contains only the Binance SPOT market symbols, their earliest date available, and their available intervals (i.e. 1s, 1m, 3m, ..., 1d), and which base pairs (e.g. BTC) are also on the UM and CM markets.

2. Shortlisting
   - To exclude specific symbols from subsequent operations below, simply remove their corresponding lines from `spot_synchronal.csv`

## Development Scripts

The `scripts/dev` directory contains a collection of utility scripts designed to assist with various development, testing, and maintenance tasks. These scripts leverage modern Python tooling and practices to streamline workflows.

Some of the key tools and libraries used across these scripts include:

- **Ruff**: For fast linting and code formatting.
- **Vulture**: To identify dead code.
- **pytest-xdist**: For parallel test execution.
- **rope**: For Python code refactoring, used in conjunction with `git mv` for moving files.
- **fsspec**: For seamless interaction with various filesystems.

Explore the scripts and their individual READMEs within the [`scripts/dev`](scripts/dev) directory for more details.

## Logging Control

DSM now supports **loguru** for much easier log level control:

### DSM Logging Suppression for Feature Engineering

**Problem**: DSM produces extensive logging that clutters console output during feature engineering workflows.

**Solution**: Use `DSM_LOG_LEVEL=CRITICAL` to suppress all non-critical DSM logs:

```python
# Clean feature engineering code - no boilerplate needed!
import os
os.environ["DSM_LOG_LEVEL"] = "CRITICAL"

from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval

# Create DSM instance - minimal logging
dsm = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

# Fetch data - clean output, only your logs visible
data = dsm.get_data(
    symbol="SOLUSDT",
    start_time=start_time,
    end_time=end_time,
    interval=Interval.MINUTE_1,
)
# ✅ Clean output - no more cluttered DSM logs!
```

**Benefits**:

- ✅ **No Boilerplate**: Eliminates 15+ lines of logging suppression code
- ✅ **Clean Output**: Professional console output for feature engineering
- ✅ **Easy Control**: Single environment variable controls all DSM logging
- ✅ **Cleaner Default**: Default ERROR level provides quieter operation

### Simple Environment Variable Control

```bash
# Clean output for feature engineering (suppress DSM logs)
export DSM_LOG_LEVEL=CRITICAL

# Normal development with basic info
export DSM_LOG_LEVEL=INFO

# Default behavior (errors and critical only)
# No need to set anything - ERROR is the default

# Detailed debugging
export DSM_LOG_LEVEL=DEBUG

# Optional: Log to file with automatic rotation
export DSM_LOG_FILE=./logs/dsm.log

# Run your application
python your_script.py
```

### Programmatic Control

```python
from data_source_manager.utils.loguru_setup import logger

# Set log level
logger.configure_level("DEBUG")

# Enable file logging
logger.configure_file("./logs/dsm.log")

# Use rich formatting
logger.info("Status: <green>SUCCESS</green>")
```

### Demo

Try the logging demos to see the benefits:

```bash
# DSM logging control demo
python examples/dsm_logging_demo.py

# Test different log levels with actual DSM
python examples/dsm_logging_demo.py --log-level CRITICAL --test-dsm
python examples/dsm_logging_demo.py --log-level DEBUG --test-dsm

# Clean feature engineering example
python examples/clean_feature_engineering_example.py

# General loguru demo
python examples/loguru_demo.py

# Environment variable control
DSM_LOG_LEVEL=CRITICAL python examples/clean_feature_engineering_example.py
DSM_LOG_LEVEL=DEBUG python examples/dsm_logging_demo.py --test-dsm
```

## Benefits of Loguru

- **🎯 Easy Control**: `DSM_LOG_LEVEL=DEBUG` vs complex logging configuration
- **🚀 Better Performance**: Loguru is faster than Python's standard logging
- **🔄 Auto Rotation**: Built-in log file rotation and compression
- **🎨 Rich Formatting**: Beautiful colored output with module/function info
- **🔧 Same API**: All existing logging calls work unchanged

## Documentation

- [API Documentation](docs/api/) - Complete API reference
- [Examples](examples/) - Usage examples and demos
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues and solutions

## License

MIT License - See [LICENSE](LICENSE) file for details.
