# Bybit Data Tools

**Hub**: [playground/](../CLAUDE.md) | **Root**: [CLAUDE.md](../../CLAUDE.md)

Tools for downloading and analyzing historical price data from the Bybit cryptocurrency exchange.

## Prerequisites

Before using these tools, you need to install the required Python packages:

```bash
pip install httpx typer platformdirs rich polars loguru
```

### Dependency Explanation

- **httpx**: Modern HTTP client with sync and async APIs
- **typer**: Command-line interface creation (built on Click)
- **platformdirs**: Platform-specific directories for data and logs
- **rich**: Terminal formatting and progress displays
- **polars**: Fast DataFrame manipulation
- **loguru**: Advanced logging with automatic rotation and formatting

## Main Tool: Download Spot Klines

The primary tool in this directory is [`download_spot_klines.py`](./download_spot_klines.py), which allows you to download historical kline (candlestick) data from the Bybit API.

### Features

- Find the true genesis timestamp for any specified market
- Automatically download all available data from genesis to current time
- Detect and handle gaps in the data through adaptive binary search
- Fill missing timestamps with NaN values
- Perform data integrity checks including duplicate detection
- Format data in standard OHLCV format
- Advanced logging with loguru (including log rotation and compression)
- Strict symbol validation for market categories (inverse vs linear contracts)
- API response verification to ensure data integrity

### Usage

You can run the script directly as a standalone CLI tool:

```bash
# Make the script executable first
chmod +x playground/bybit/download_spot_klines.py

# Run with default settings (BTCUSDT 15m data)
./playground/bybit/download_spot_klines.py

# Get help and see all available options
./playground/bybit/download_spot_klines.py --help
```

### Example Commands

```bash
# Download 5-minute ETHUSDT spot data
./playground/bybit/download_spot_klines.py -s ETHUSDT -i 5

# Download 15-minute BTCUSDT spot data, limit to 10 batches
./playground/bybit/download_spot_klines.py -s BTCUSDT -i 15 -n 10 -A

# Download with specific batch size (must be between 1 and 1000)
./playground/bybit/download_spot_klines.py -s BTCUSDT -i 15 -l 500

# Download BTCUSD inverse perpetual data (note the USD suffix for inverse)
./playground/bybit/download_spot_klines.py -s BTCUSD -c inverse -i 5

# Download BTCUSDT linear perpetual data (note the USDT suffix for linear)
./playground/bybit/download_spot_klines.py -s BTCUSDT -c linear -i 5
```

### Market Category and Symbol Naming Conventions

Bybit's API requires specific symbol naming conventions for different market categories:

| Market Type | Category Parameter | Symbol Naming Convention       | Example   |
| ----------- | ------------------ | ------------------------------ | --------- |
| Spot        | `spot`             | Base currency + Quote currency | `BTCUSDT` |
| Linear      | `linear`           | Base currency + `USDT` suffix  | `BTCUSDT` |
| Inverse     | `inverse`          | Base currency + `USD` suffix   | `BTCUSD`  |

The script validates that the provided symbol follows the correct naming convention for the specified market category:

- For inverse markets, symbols must end with `USD` (e.g., `BTCUSD`, `ETHUSD`)
- For linear markets, symbols must end with `USDT` (e.g., `BTCUSDT`, `ETHUSDT`)

If an incorrect symbol format is provided, the script will suggest the correct format and exit with an error message.

You can override this validation with the `--force` flag if necessary:

```bash
# Force download with a non-standard symbol name
./playground/bybit/download_spot_klines.py -s BTCUSDT -c inverse -i 5 --force
```

**⚠️ Important API Behavior Warning:**

Our empirical testing revealed a critical issue with Bybit's API when using incorrect symbol formats:

- When using `category=inverse` with a USDT-suffixed symbol (e.g., BTCUSDT), the API returns data from the linear market
- The API response incorrectly labels this data as "category": "inverse" while actually providing linear market data
- The timestamps and values are identical to those returned by linear market queries
- Without our validation, users could unknowingly analyze linear market data thinking it's inverse market data

This discovery validates the importance of our symbol validation feature and why strict adherence to proper naming conventions is essential.

### Earliest Data Availability (Based on Empirical Testing)

Based on our empirical testing, here's when data becomes available for different markets:

| Market Type       | Symbol  | Earliest Available Timestamp (UTC) |
| ----------------- | ------- | ---------------------------------- |
| Spot              | BTCUSDT | 2021-07-05 12:00:00                |
| Linear Perpetual  | BTCUSDT | 2020-03-25 10:35:00                |
| Inverse Perpetual | BTCUSD  | 2018-11-14 16:00:00                |

### Important API Limits

- The batch size (`--limit` or `-l` option) is set to 1000 by default, which is the maximum allowed by the Bybit REST API v5.
- This is a hard limit imposed by the Bybit exchange, not a limitation of the script.
- You can set a smaller batch size if needed, but it cannot exceed 1000.

### Output Data

The downloaded data is saved to a platform-specific location determined by the `platformdirs` module:

```shell
# Platform-specific data locations
macOS:     ~/Documents/ckvd/data/bybit/{category}/{symbol}/{interval}m/
Linux:     ~/.local/share/ckvd/data/bybit/{category}/{symbol}/{interval}m/
Windows:   C:\Users\<username>\Documents\ckvd\data\bybit\{category}\{symbol}\{interval}m\
```

The filename follows the format: `bybit-{symbol}-{interval}m.csv` for spot markets or `bybit-{category}-{symbol}-{interval}m.csv` for other markets.

#### CSV Format

The data is saved in a standardized CSV format with the following columns:

- `low`: Lowest price during the interval
- `open`: Opening price of the interval
- `volume`: Trading volume during the interval
- `high`: Highest price during the interval
- `close`: Closing price of the interval
- `timeStamp`: Unix timestamp in milliseconds

The data is sorted chronologically (oldest to newest) and includes filled gaps where data might be missing.

#### Data Validation Summary

At the end of script execution, you'll see a summary of the data that includes:

- Total number of klines downloaded and saved
- Results of duplicate checks (whether any duplicates were found)
- Results of continuity checks (whether there are any gaps in the timestamps)
- Last timestamp in the dataset
- Data integrity information

This summary helps validate the quality and completeness of the downloaded data.

## Additional Tools

The [`non-overlap`](./non-overlap/) subdirectory contains tools for testing and validating the data fetching methodology, specifically addressing potential duplicate data issues when fetching historical data in consecutive batches.

Key files in this directory:

- [`download_spot_klines.py`](./download_spot_klines.py): Main tool for downloading kline data
- [`non-overlap/fetch_data_batch_test.py`](./non-overlap/fetch_data_batch_test.py): Tool for testing duplicate prevention

## Logging

The tool uses loguru for enhanced logging with the following features:

- **Automatic rotation**: Log files are automatically rotated when they reach 10MB
- **Retention policy**: Old logs are kept for 1 week then automatically deleted
- **Compression**: Rotated logs are compressed to save disk space
- **Better formatting**: Timestamp, log level, and message are clearly formatted
- **Enhanced exceptions**: Detailed traceback information for exceptions

The log files are stored in platform-specific locations determined by the `platformdirs` module:

```shell
# Platform-specific log locations
macOS:     ~/Library/Logs/ckvd/bybit_download.log
Linux:     ~/.local/state/ckvd/bybit_download.log
Windows:   C:\Users\<username>\AppData\Local\ckvd\ckvd\Logs\bybit_download.log
```

Rotated logs follow the naming convention `bybit_download.{timestamp}.log.zip` and are stored in the same directory.
