# Bybit Data Tools

This directory contains tools for downloading and analyzing historical price data from the Bybit cryptocurrency exchange.

## Prerequisites

Before using these tools, you need to install the required Python packages:

```bash
pip install httpx typer platformdirs rich polars
```

## Main Tool: Download Spot Klines

The primary tool in this directory is [`download_spot_klines.py`](./download_spot_klines.py), which allows you to download historical kline (candlestick) data from the Bybit API.

### Features

- Find the true genesis timestamp for any specified market
- Automatically download all available data from genesis to current time
- Detect and handle gaps in the data through adaptive binary search
- Fill missing timestamps with NaN values
- Perform data integrity checks including duplicate detection
- Format data in standard OHLCV format

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
```

### Important API Limits

- The batch size (`--limit` or `-l` option) is set to 1000 by default, which is the maximum allowed by the Bybit REST API v5.
- This is a hard limit imposed by the Bybit exchange, not a limitation of the script.
- You can set a smaller batch size if needed, but it cannot exceed 1000.

### Output Data

The downloaded data is saved to a platform-specific location determined by the `platformdirs` module:

```shell
# Platform-specific data locations
macOS:     ~/Documents/data_source_manager/data/bybit/{category}/{symbol}/{interval}m/
Linux:     ~/.local/share/data_source_manager/data/bybit/{category}/{symbol}/{interval}m/
Windows:   C:\Users\<username>\Documents\data_source_manager\data\bybit\{category}\{symbol}\{interval}m\
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

The tool logs all operations to platform-specific locations determined by the `platformdirs` module:

```shell
# Platform-specific log locations
macOS:     ~/Library/Logs/data_source_manager/bybit_download.log
Linux:     ~/.local/state/data_source_manager/bybit_download.log
Windows:   C:\Users\<username>\AppData\Local\data_source_manager\data_source_manager\Logs\bybit_download.log
```

This log can be useful for debugging or tracking the historical execution of data downloads.
