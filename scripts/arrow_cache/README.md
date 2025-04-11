# Arrow Cache Builder

A robust tool for building and managing local Arrow cache files from Binance Vision API data.

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

## Usage

### Basic Usage

```bash
# Test mode (default): Download data for BTCUSDT, ETHUSDT, BNBUSDT with 5m interval
./cache_builder.sh

# Specify symbols and intervals
./cache_builder.sh --symbols BTCUSDT,ETHUSDT --intervals 1m,5m --start-date 2024-01-01

# Production mode: Download all symbols/intervals from the CSV file
./cache_builder.sh --mode production --start-date 2023-01-01
```

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
| `--skip-checksum`           | Skip checksum verification entirely                        |
| `--proceed-on-failure`      | Proceed with caching even when checksum verification fails |
| `--retry-failed-checksums`  | Retry downloading files with previously failed checksums   |
| `-h, --help`                | Display help message                                       |

### view_checksum_failures.sh

| Option                | Description                                                       |
| --------------------- | ----------------------------------------------------------------- |
| `-l, --list`          | List all checksum failures (default)                              |
| `-s, --summary`       | Show summary statistics of checksum failures                      |
| `-r, --retry`         | Retry all failures by running the cache builder with retry option |
| `-c, --clear`         | Clear the checksum failures registry (with backup)                |
| `-d, --detail SYMBOL` | Show detailed failures for specific symbol                        |
| `-h, --help`          | Display help message                                              |

## File Structure

The Arrow cache is organized in a hierarchical structure:

```tree
cache/
  BINANCE/
    KLINES/
      {symbol}/
        {interval}/
          {date}.arrow
```

For example: `cache/BINANCE/KLINES/BTCUSDT/5m/2024-01-01.arrow`

## Implementation Details

- **Synchronous Approach**: Reliable operation without hanging issues
- **PyArrow Integration**: Efficient file operations with PyArrow
- **Controlled Concurrency**: ThreadPoolExecutor for optimal parallelism
- **SHA-256 Checksums**: Strong cryptographic verification of data integrity
- **JSON-based Failure Registry**: Structured tracking of checksum failures
- **Shell Script Wrappers**: User-friendly command-line interfaces

## Error Handling

- **Download Failures**: Automatically reported and can be retried
- **Checksum Mismatches**: Recorded in a dedicated registry with detailed information
- **Processing Errors**: Comprehensive logging with error details
- **Graceful Shutdown**: Proper handling of interruption signals

## Requirements

- Python 3.6+
- pandas
- pyarrow
- rich (for colorized output)
- jq (optional, for enhanced checksum failure reporting)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
