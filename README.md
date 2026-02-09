# Crypto Kline Vision Data

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

Crypto Kline Vision Data follows modern Python packaging standards with a clean src-layout structure:

```
crypto-kline-vision-data/
├── src/
│   └── ckvd/        # Main package namespace
│       ├── __init__.py             # Public API exports (lazy loading)
│       ├── core/                   # Core functionality
│       │   ├── sync/               # Synchronous data managers
│       │   │   ├── crypto_kline_vision_data.py  # Main CKVD class with FCP
│       │   │   ├── ckvd_types.py            # DataSource, CKVDConfig
│       │   │   └── ckvd_lib.py              # High-level fetch functions
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

There are two main ways to install Crypto Kline Vision Data, depending on your needs:

### 1. For Development or Running Demos Directly

If you want to run the provided demos directly from the cloned repository or use the core library while having the source files available in your workspace, follow these steps:

```bash
# Clone the repository
git clone https://github.com/terrylica/crypto-kline-vision-data.git
cd crypto-kline-vision-data

# Install with uv (recommended, 10-100x faster than pip)
uv sync --dev

# Or with pip (slower, not recommended)
pip install -e ".[dev]"
```

**Note**: This project uses [uv](https://docs.astral.sh/uv/) for package management. Install it via `curl -LsSf https://astral.sh/uv/install.sh | sh`.

This method keeps all the source files in your workspace and includes necessary tools for development workflows.

### 2. As a Dependency in Your Project (`pyproject.toml`)

If you want to use Crypto Kline Vision Data as a library in your own Python project (managed with `pyproject.toml`) without including its entire source code in your project's directory, you can add it as a Git dependency.

Add the following to your project's `pyproject.toml` file under the `[project.dependencies]` array (as per PEP 621):

```toml
[project]
# ... other project configurations like name, version ...
dependencies = [
    # ... other dependencies ...
    "crypto-kline-vision-data @ git+https://github.com/terrylica/crypto-kline-vision-data.git"
    # You can also specify a particular branch, tag, or commit hash:
    # "crypto-kline-vision-data @ git+https://github.com/terrylica/crypto-kline-vision-data.git@main"
    # "crypto-kline-vision-data @ git+https://github.com/terrylica/crypto-kline-vision-data.git@<version>"
]
```

This will install Crypto Kline Vision Data into your Python environment's `site-packages` directory, keeping your project workspace clean.

**Note on CLI Tools:**
The installation process (through either method) automatically registers the CLI commands (`ckvd-demo-cli` and `ckvd-demo-module`) as executable scripts in your Python environment. These commands will be available in your terminal after successful installation.

## Usage

### Basic Usage

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval
from datetime import datetime, timedelta, timezone

# Create a manager for USDT-margined futures
manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

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

The CKVD automatically handles data retrieval through multiple sources:

```python
# The FCP follows this sequence automatically:
# 1. 🚀 Local cache lookup (fastest)
# 2. 📡 Vision API for historical data (efficient)
# 3. 🔄 REST API fallback (real-time)

# All with automatic retry, data validation, and gap detection
manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)

# This single call handles all the complexity:
data = manager.get_data("ETHUSDT", start_time, end_time, Interval.MINUTE_5)
# ✅ Cache checked, Vision API queried, REST API fallback - all automatic!
```

### Advanced Configuration

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval
from ckvd.core.sync.crypto_kline_vision_data import DataSource

# Force specific data source (bypass FCP)
manager = CryptoKlineVisionData.create(
    provider=DataProvider.BINANCE,
    market_type=MarketType.FUTURES_USDT,
    enforce_source=DataSource.VISION,  # Force Vision API only
)

# Multiple market types supported
spot_manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
futures_manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
coin_manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_COIN)
```

## Running the Demos

Once installed, you can run the demos using the command-line tools:

### CKVD Demo CLI

The CLI demonstration provides an interactive way to explore the Failover Control Protocol:

```bash
# Run the CKVD Demo CLI with default parameters
ckvd-demo-cli

# Run with specific parameters (get BTC data for a 10-day period)
ckvd-demo-cli -s BTCUSDT -i 1m -d 10

# Get help and see all available options
ckvd-demo-cli --help
```

The CLI tool will automatically:

1. Check for data in local cache
2. Try to fetch missing data from Binance Vision API
3. Fall back to REST API for data not available in cache or Vision API
4. Save retrieved data to cache for future use

### CKVD Demo Module

The module demo provides a programmatic interface:

```bash
# Run the CKVD Demo Module to see examples
ckvd-demo-module
```

## Using as a Library

The core data fetching functionality of Crypto Kline Vision Data is available for direct import and use in your Python projects after installation.

The main function for retrieving market data is `fetch_market_data`.

### Example 1: Fetching with Specific Date Range

```python
from datetime import datetime, timezone
from ckvd import fetch_market_data, MarketType, DataProvider, Interval, ChartType

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
from ckvd import fetch_market_data, MarketType, DataProvider, Interval, ChartType

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

You can import `fetch_market_data` directly from the `ckvd` package. The necessary enums (`MarketType`, `DataProvider`, `ChartType`, `Interval`, `DataSource`) and `CKVDConfig` are also exposed at the top level for easy access.

Refer to the source code of `ckvd.core.sync.ckvd_lib.fetch_market_data` and `ckvd.core.sync.crypto_kline_vision_data.CKVDConfig` for detailed parameter information and usage.

## Crypto Kline Vision Data (CKVD) Demo

### Quick Start

- **[CKVD Demo CLI Documentation](examples/sync/)**: Interactive demonstration of the Failover Control Protocol mechanism, the core data retrieval strategy that ensures robust and efficient data collection from multiple sources.
- **[CKVD Demo Module Documentation](examples/lib_module/)**: Programmatic interface to `src/ckvd/core/sync/ckvd_lib.py` functions, complementing the CLI tool by providing a library approach to implement the same data retrieval functionality in custom applications.

### Understanding Data Sources

The CKVD implements a Failover Control Protocol (FCP) that follows this sequence:

1. **Cache**: First checks local Arrow files for requested data
2. **VISION API**: For missing data, attempts to download from Binance Vision API
3. **REST API**: Falls back to Binance REST API for any remaining data gaps

Note that recent data (within ~48 hours) is typically not available in the Vision API and will be retrieved from the REST API.

## Development Guidelines

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

CKVD now supports **loguru** for much easier log level control:

### CKVD Logging Suppression for Feature Engineering

**Problem**: CKVD produces extensive logging that clutters console output during feature engineering workflows.

**Solution**: Use `CKVD_LOG_LEVEL=CRITICAL` to suppress all non-critical CKVD logs:

```python
# Clean feature engineering code - no boilerplate needed!
import os
os.environ["CKVD_LOG_LEVEL"] = "CRITICAL"

from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval

# Create CKVD instance - minimal logging
ckvd = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)

# Fetch data - clean output, only your logs visible
data = ckvd.get_data(
    symbol="SOLUSDT",
    start_time=start_time,
    end_time=end_time,
    interval=Interval.MINUTE_1,
)
# ✅ Clean output - no more cluttered CKVD logs!
```

**Benefits**:

- ✅ **No Boilerplate**: Eliminates 15+ lines of logging suppression code
- ✅ **Clean Output**: Professional console output for feature engineering
- ✅ **Easy Control**: Single environment variable controls all CKVD logging
- ✅ **Cleaner Default**: Default ERROR level provides quieter operation

### Simple Environment Variable Control

```bash
# Clean output for feature engineering (suppress CKVD logs)
export CKVD_LOG_LEVEL=CRITICAL

# Normal development with basic info
export CKVD_LOG_LEVEL=INFO

# Default behavior (errors and critical only)
# No need to set anything - ERROR is the default

# Detailed debugging
export CKVD_LOG_LEVEL=DEBUG

# Optional: Log to file with automatic rotation
export CKVD_LOG_FILE=./logs/ckvd.log

# Run your application
python your_script.py
```

### Programmatic Control

```python
from ckvd.utils.loguru_setup import logger

# Set log level
logger.configure_level("DEBUG")

# Enable file logging
logger.configure_file("./logs/ckvd.log")

# Use rich formatting
logger.info("Status: <green>SUCCESS</green>")
```

### Demo

Try the logging demos to see the benefits:

```bash
# CKVD logging control demo
python examples/ckvd_logging_demo.py

# Test different log levels with actual CKVD
python examples/ckvd_logging_demo.py --log-level CRITICAL --test-ckvd
python examples/ckvd_logging_demo.py --log-level DEBUG --test-ckvd

# Clean feature engineering example
python examples/clean_feature_engineering_example.py

# Environment variable control
CKVD_LOG_LEVEL=CRITICAL python examples/clean_feature_engineering_example.py
CKVD_LOG_LEVEL=DEBUG python examples/ckvd_logging_demo.py --test-ckvd
```

## Benefits of Loguru

- **🎯 Easy Control**: `CKVD_LOG_LEVEL=DEBUG` vs complex logging configuration
- **🚀 Better Performance**: Loguru is faster than Python's standard logging
- **🔄 Auto Rotation**: Built-in log file rotation and compression
- **🎨 Rich Formatting**: Beautiful colored output with module/function info
- **🔧 Same API**: All existing logging calls work unchanged

## Feature Flags

CKVD supports an optional feature flag for output format optimization.

**Note**: The Polars pipeline is always active internally (`USE_POLARS_PIPELINE` was removed — see [CHANGELOG](CHANGELOG.md) for details). The only remaining flag controls the API output format.

### Zero-Copy Polars Output

When combined with `return_polars=True`, skip pandas conversion entirely for 10-15% memory savings:

```bash
export CKVD_USE_POLARS_OUTPUT=true
```

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval

manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

# Returns pl.DataFrame directly (zero-copy, skips pandas conversion)
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start_time,
    end_time=end_time,
    interval=Interval.HOUR_1,
    return_polars=True
)
```

### Feature Flag Summary

| Flag                | Environment Variable     | Effect                  |
| ------------------- | ------------------------ | ----------------------- |
| `USE_POLARS_OUTPUT` | `CKVD_USE_POLARS_OUTPUT` | Zero-copy Polars output |

Defaults to `False` for backward compatibility. Set to `true`, `1`, or `yes` to enable.

## Documentation

- [API Documentation](docs/api/) - Complete API reference
- [Examples](examples/) - Usage examples and demos
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Common issues and solutions

## License

MIT License - See [LICENSE](LICENSE) file for details.
