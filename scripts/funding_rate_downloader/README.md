# Binance Funding Rate History Downloader

This tool allows you to download funding rate history data from Binance Futures API and save it as CSV files.

## Features

- Download funding rate history for any perpetual futures symbol
- Convert data to CSV format with proper headers
- Save with consistent naming pattern: `Funding Rate History_SYMBOL Perpetual_DATE.csv`
- Run as a one-time download or schedule regular downloads
- Process multiple symbols sequentially

## Usage

### One-time Download

To download funding rate history for a specific symbol once:

```bash
# Download for default symbols (BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT)
python automate_funding_rate_download.py --run-once

# Download for specific symbols
python automate_funding_rate_download.py --symbols BTCUSDT ETHUSDT --run-once

# Specify output directory
python automate_funding_rate_download.py --output-dir data/funding_rates --run-once
```

### Scheduled Downloads

To set up a scheduled job that downloads funding rate history at regular intervals:

```bash
# Download every hour (default)
python automate_funding_rate_download.py

# Download every 15 minutes
python automate_funding_rate_download.py --interval 15

# Custom symbols and interval
python automate_funding_rate_download.py --symbols BTCUSDT ETHUSDT --interval 30
```

## Command-line Options

- `--symbols`: List of trading symbols to download (default: BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT)
- `--interval`: Download interval in minutes (default: 60)
- `--output-dir`: Output directory (default: tmp/funding_rate_history)
- `--run-once`: Run once and exit (default: run continuously)

## Example Output

The CSV files will have the following structure:

```csv
Symbol,Funding Time,Funding Rate,Mark Price
BTCUSDT,2025-04-04 08:00:00,0.00001659,88106.70000000
BTCUSDT,2025-04-04 16:00:00,-0.00003082,86704.27858519
...
```

## How It Works

The script uses the Binance Futures API endpoint `/fapi/v1/fundingRate` to retrieve the funding rate history. It fetches up to 1000 entries per API call (the maximum allowed by Binance), converts the data to CSV format, and saves it with the appropriate filename.

When running in continuous mode, the script will run at the specified interval, downloading the latest funding rate data for all specified symbols.

## Requirements

- Python 3.7+
- pandas
- httpx

## Directory Structure

The default output directory is `tmp/funding_rate_history/`. You can customize this with the `--output-dir` parameter.

## Notes

- The API has rate limits. If you're downloading data for many symbols or at frequent intervals, be aware of Binance's rate limiting.
- The filename includes the current date, so each day will generate new files.
