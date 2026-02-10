# Arrow Cache Builder

**Hub**: [scripts/](../CLAUDE.md) | **Root**: [CLAUDE.md](../../CLAUDE.md)

A robust tool for building and managing local Arrow cache files from Binance Vision API data.

## Current Status & Recent Updates

**Last Updated:** April 2025

### Recent Improvements & Fixes

- **Market Parameters Integration**: Added full support for `--market-type`, `--data-provider`, and `--chart-type` parameters in the shell script wrapper, enabling proper market-specific caching
- **Unified Testing Framework**: Merged the functionality of `test_incremental.sh` into a more comprehensive `run_tests.sh` with an interactive menu
- **Date Handling Fix**: Fixed issues with date object handling in `cache_builder_sync.py` that was causing errors with different date formats
- **Constrained Testing**: Optimized test date ranges to use historical data (e.g., 2023-04-01 to 2023-04-03) for more reliable and faster tests
- **Auto Mode Fix**: Resolved an issue where auto mode was processing all symbols from CSV instead of respecting the specified symbols
- **Menu-Driven Testing**: Added interactive test selection to allow running specific test scenarios without running everything

### Known Issues & Limitations

- **Auto Mode Behavior**: When using `--auto` flag directly, it sets `MODE="production"` which causes it to ignore the `--symbols` parameter and try to use all symbols from the CSV file
- **Future Dates**: Default date range (2025-04-02) might cause issues with data availability; use historical dates for testing
- **Cache Directory Structure**: Ensure proper market parameters are used when accessing files or directories
- **Timezone Handling**: Occasionally timestamp conversion issues may occur when processing data with different timestamp formats

## Overview

The Arrow Cache Builder provides a reliable and efficient way to download historical market data from Binance Vision API and store it in optimized Apache Arrow format for fast access. It includes data integrity verification through checksums and comprehensive failure handling.

> **Note:** This implementation uses a fully synchronous approach for maximum reliability. An asynchronous version was previously attempted but was removed due to hanging issues and complexity.

## Features

- **Direct Binance Vision API Access**: Download historical market data for any symbol and interval
- **Efficient Storage**: Convert and store data in Apache Arrow format for optimized read/write performance
- **Checksum Verification**: Ensure data integrity by verifying downloaded files against official checksums
- **Failure Handling**: Comprehensive handling of download and checksum failures with detailed logging
- **Multithreaded Processing**: Controlled parallelism for efficient downloading of large datasets
- **Flexible Configuration**: Supports various modes and options through command-line arguments
- **Checksum Failure Management**: Tools for tracking, reporting, and resolving checksum failures
- **Incremental Updates**: Only download missing or modified data to efficiently update the cache
- **Gap Detection**: Automatically identify and fill gaps in the cache
- **Force Updates**: Re-download and refresh existing data when needed
- **Auto Mode**: Combines incremental updates and gap detection for optimal maintenance
- **Cache Metadata**: Track and manage cache content with comprehensive metadata
- **Small Footprint Testing**: Built-in support for various test sizes with minimal resource usage
- **Market Type Support**: Full support for different market types via proper directory structure

## Usage

### Basic Usage

```bash
# Test mode (default): Download data for BTCUSDT, ETHUSDT, BNBUSDT with 5m interval
./cache_builder.sh

# Specify symbols and intervals
./cache_builder.sh --symbols BTCUSDT,ETHUSDT --intervals 1m,5m --start-date 2023-04-01 --end-date 2023-04-03

# Production mode: Download all symbols/intervals from the CSV file
./cache_builder.sh --mode production --start-date 2023-01-01
```

### Market Type Parameters

```bash
# Specify market type, data provider and chart type
./cache_builder.sh --symbols BTCUSDT --intervals 1h --market-type futures_usdt --data-provider BINANCE --chart-type KLINES

# Spot market example (default)
./cache_builder.sh --symbols BTCUSDT --intervals 5m --market-type spot

# Futures USDT market example
./cache_builder.sh --symbols BTCUSDT --intervals 1h --market-type futures_usdt
```

### Testing Framework

```bash
# Run the interactive test suite
./run_tests.sh

# You'll be presented with a menu:
# 1. Run Incremental Testing Suite
# 2. Run Basic Cache Building Test
# 3. Run Incremental Update Test
# ...and more options

# To run multiple tests at once:
# Enter test numbers separated by spaces, e.g. "1 3 5"
# Enter "10" to run all tests
```

### Small Footprint Testing

The tool provides built-in support for small footprint testing with different test sizes:

```bash
# Very small test (1 symbol, 1 interval, 1 day)
./cache_builder.sh -m test -t very-small

# Small test (3 symbols, 1 interval, 3 days) - Default test mode
./cache_builder.sh -m test -t small

# Medium test (5 symbols, 2 intervals, 7 days)
./cache_builder.sh -m test -t medium

# Combine test size with feature flags
./cache_builder.sh -m test -t very-small --incremental
./cache_builder.sh -m test -t small --detect-gaps
./cache_builder.sh -m test -t medium --force-update
```

### Incremental Mode Examples

```bash
# Only download missing data (skip existing files)
./cache_builder.sh --symbols BTCUSDT,ETHUSDT --incremental

# Detect and fill gaps in the cache
./cache_builder.sh --symbols BTCUSDT,ETHUSDT --detect-gaps

# Force update (re-download even if files exist)
./cache_builder.sh --symbols BTCUSDT,ETHUSDT --force-update

# Auto mode with explicit test mode to limit symbols
./cache_builder.sh -m test -t very-small --symbols BTCUSDT --incremental --detect-gaps
```

### Historical Date Testing

When working with the cache builder, especially in testing environments, it's recommended to use historical dates rather than relying on the default date ranges:

```bash
# Use specific historical dates for reliable testing
./cache_builder.sh -m test -t very-small --start-date 2023-04-01 --end-date 2023-04-03

# Combine historical dates with feature testing
./cache_builder.sh -m test -t very-small --start-date 2023-04-01 --end-date 2023-04-03 --incremental
./cache_builder.sh -m test -t very-small --start-date 2023-04-01 --end-date 2023-04-03 --detect-gaps
./cache_builder.sh -m test -t very-small --start-date 2023-04-01 --end-date 2023-04-03 --force-update
```

This approach avoids issues with future dates that might not have data available or could cause checksum verification failures.

### Checksum Options

```bash
# Skip checksum verification entirely
./cache_builder.sh --symbols BTCUSDT --skip-checksum

# Proceed even on checksum failures (but still log them)
./cache_builder.sh --symbols BTCUSDT --proceed-on-failure

# Retry previously failed checksums
./cache_builder.sh --retry-failed-checksums
```

### Managing Checksum Failures

```bash
# View all checksum failures
./view_checksum_failures.sh

# View summary statistics
./view_checksum_failures.sh --summary

# View details for a specific symbol
./view_checksum_failures.sh --detail BTCUSDT

# Retry all failed checksums
./view_checksum_failures.sh --retry

# Clear the failures registry (with backup)
./view_checksum_failures.sh --clear
```

## Command Line Options

### cache_builder.sh

| Option                      | Description                                                |
| --------------------------- | ---------------------------------------------------------- |
| `-s, --symbols SYMBOLS`     | Comma-separated list of symbols (e.g., BTCUSDT,ETHUSDT)    |
| `-i, --intervals INTERVALS` | Comma-separated list of intervals (default: 5m)            |
| `-f, --csv-file FILE`       | Path to symbols CSV file                                   |
| `-d, --start-date DATE`     | Start date (YYYY-MM-DD)                                    |
| `-e, --end-date DATE`       | End date (YYYY-MM-DD)                                      |
| `-l, --limit N`             | Limit to N symbols                                         |
| `-m, --mode MODE`           | Mode (test or production)                                  |
| `-t, --test-size SIZE`      | Test size (very-small, small, medium) for test mode        |
| `--skip-checksum`           | Skip checksum verification entirely                        |
| `--proceed-on-failure`      | Proceed with caching even when checksum verification fails |
| `--retry-failed-checksums`  | Retry downloading files with previously failed checksums   |
| `--incremental`             | Only download missing data (skip existing files)           |
| `--detect-gaps`             | Detect and fill gaps in the cache                          |
| `--force-update`            | Re-download data even if it exists in cache                |
| `--auto`                    | Automatic mode (all symbols, incremental, gap detection)   |
| `--market-type TYPE`        | Market type (spot, futures_usdt, futures_coin)             |
| `--data-provider PROVIDER`  | Data provider (default: BINANCE)                           |
| `--chart-type TYPE`         | Chart type (default: KLINES)                               |
| `--error-log FILE`          | Log errors, warnings, and critical messages to a file      |
| `-h, --help`                | Display help message                                       |

### run_tests.sh

The unified testing script provides an interactive menu for running various test scenarios. Simply run:

```bash
./run_tests.sh
```

And select from the available test options. You can also run multiple tests at once by entering space-separated test numbers.

## Small Footprint Test Sizes

The cache builder supports three predefined test sizes for quick testing and development:

1. **Very Small Test**
   - 1 symbol (BTCUSDT)
   - 1 interval (5m)
   - 1 day of data (yesterday to today)
   - Ideal for quick feature testing

2. **Small Test (Default)**
   - 3 symbols (BTCUSDT, ETHUSDT, BNBUSDT)
   - 1 interval (5m)
   - 3 days of data
   - Good balance between coverage and speed

3. **Medium Test**
   - 5 symbols (BTCUSDT, ETHUSDT, BNBUSDT, XRPUSDT, ADAUSDT)
   - 2 intervals (5m, 1h)
   - 7 days of data
   - More comprehensive testing with multiple intervals

## File Structure

The Arrow cache is organized in a hierarchical structure that encodes the market type:

```tree
cache/
  {DATA_PROVIDER}/
    {CHART_TYPE}/
      {MARKET_TYPE}/
        {symbol}/
          {interval}/
            {date}.arrow

Example:
cache/
  BINANCE/
    KLINES/
      spot/
        BTCUSDT/
          5m/
            2023-04-01.arrow
      futures_usdt/
        BTCUSDT/
          1h/
            2023-04-01.arrow
```

## Market Type Support

The Arrow cache system follows the market constraints defined in `src/ckvd/utils/market_constraints.py`. The directory structure encodes key information:

```bash
cache/{provider}/{chart_type}/{market_type}/{symbol}/{interval}/{date}.arrow
```

Standard supported values:

- **Data Provider**: `BINANCE` (from `DataProvider` enum)
- **Chart Type**: `KLINES` (from `ChartType` enum)
- **Market Type**: `spot`, `futures_usdt`, `futures_coin` (from `MarketType` enum)

## Development Notes

### Future Enhancements

- Improve error handling for network timeouts
- Add support for additional data providers
- Enhance the gap detection algorithm for more efficient fills
- Add a web interface for monitoring and management

### Troubleshooting Tips

- Use fixed historical dates rather than the default 2025 date
- When encountering issues with auto mode, use explicit test modes
- Always verify the correct market parameters are being used
- Check for timestamp format conversion issues when parsing data

### Next Steps

- Further enhance market type parameter handling
- Improve test performance for large datasets
- Consider implementing batch download for very large date ranges
- Add a data validation step after downloads to ensure integrity

## Cache Metadata 01

The cache metadata is stored in a SQLite database at `logs/cache_index.db` with the following structure:

- `cache_metadata` table: Stores global metadata about the cache
  - `key`: Metadata key (e.g., "last_update")
  - `value`: Metadata value

- `cache_entries` table: Stores information about each cached file
  - `symbol`: Trading pair symbol (e.g., "BTCUSDT")
  - `interval`: Time interval (e.g., "5m")
  - `date`: Data date (YYYY-MM-DD)
  - `file_size`: Size of the Arrow file in bytes
  - `num_records`: Number of records in the file
  - `last_updated`: Timestamp of last update
  - `path`: Path to the Arrow file

This database provides a comprehensive index of all cached data, including file sizes, record counts, and timestamps.

## Cache Metadata 02

The cache metadata is stored in a SQLite database at `logs/cache_index.db` with the following structure:

- `cache_metadata` table: Stores global metadata about the cache
  - `key`: Metadata key (e.g., "last_update")
  - `value`: Metadata value

- `cache_entries` table: Stores information about each cached file
  - `symbol`: Trading pair symbol (e.g., "BTCUSDT")
  - `interval`: Time interval (e.g., "5m")
  - `date`: Data date (YYYY-MM-DD)
  - `file_size`: Size of the Arrow file in bytes
  - `num_records`: Number of records in the file
  - `last_updated`: Timestamp of last update
  - `path`: Path to the Arrow file

This database provides a comprehensive index of all cached data, including file sizes, record counts, and timestamps.

## Using ArrowCacheReader with Market Constraints

The ArrowCacheReader is designed to work seamlessly with the enums defined in `src/ckvd/utils/market_constraints.py`. This ensures type safety and consistent use of market parameters throughout the system.

Here's an example of how to use the ArrowCacheReader with proper enum values:

```python
from ckvd.ckvd.utils.logger_setup import logger
from rich import print
from datetime import datetime

from ckvd.ckvd.utils.market_constraints import DataProvider, MarketType, ChartType, Interval
from ckvd.ckvd.utils.arrow_cache_reader import ArrowCacheReader

# Initialize the reader
reader = ArrowCacheReader()

# Define parameters using proper enums
provider = DataProvider.BINANCE
chart_type = ChartType.KLINES
market_type = MarketType.SPOT
symbol = "BTCUSDT"
interval = Interval.HOUR_1
date = datetime(2025, 1, 1)

# Check if data is available
file_path = reader.get_file_path(
    symbol=symbol,
    interval=interval,
    date=date,
    market_type=market_type
)
is_available = file_path is not None

# Read data if available
if is_available:
    df = reader.read_arrow_file(file_path)
    print(df.head())

    # Or read data for a date range
    df_range = reader.read_symbol_data(
        symbol=symbol,
        interval=interval,
        start_date="2025-01-01",
        end_date="2025-01-10",
        market_type=market_type
    )
    print(f"Read {len(df_range)} records for date range")
```

For a complete example, see `scripts/arrow_cache/example_usage.py`.
