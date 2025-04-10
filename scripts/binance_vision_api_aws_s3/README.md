# Binance Vision API & AWS S3 Data Downloader

This directory contains scripts for downloading and verifying data from the Binance Vision API and AWS S3 storage.

## Binance Data Availability Fetcher

The `fetch_binance_data_availability.sh` script efficiently retrieves all available trading symbols and their earliest available data date from Binance Vision data repository. It works with spot, um (USDT-M futures), and cm (COIN-M futures) markets and creates filtered lists based on specified criteria.

### Features

- Multi-market support (spot, USDT-M futures, COIN-M futures)
- Parallel processing for faster data retrieval
- Automatic generation of market-specific and combined reports
- Cross-market symbol filtering based on quote currencies
- Customizable output formats and directories

### CSV Output Structure

The script creates CSV files with the following columns:

- `market` - Market type (spot, um, cm)
- `symbol` - Trading symbol (e.g., BTCUSDT)
- `earliest_date` - Earliest date data is available
- `available_intervals` - Comma-separated list of available kline intervals

### Recent Changes

- **Removed redundant `interval` column**: Since the script primarily uses the 1d interval to find the earliest available date, the interval column was removed from CSV outputs for clarity.

### Usage

```bash
# Run with default settings
./fetch_binance_data_availability.sh

# Customize with options
./fetch_binance_data_availability.sh --output custom_dir --markets spot,um --parallel 30
```

## Multi-Interval Verification Tool

The `verify_multi_interval.sh` script downloads and verifies historical kline (candlestick) data for multiple symbols and intervals from Binance Vision and AWS S3. It provides comprehensive validation, download management, and reporting.

### Key Features

1. **Improved Date Terminology**

   - Uses clear terminology (`LATEST_DATE`/`EARLIEST_DATE` instead of START/END) to accurately reflect the date processing direction
   - Processes data chronologically from newest to oldest dates
   - Creates filenames with chronological ordering for improved clarity

2. **Dependency Management**

   - Automatically detects required dependencies (curl, aria2c, unzip, sha256sum)
   - Option to automatically install missing dependencies
   - Graceful fallbacks (e.g., curl if aria2c is not available)

3. **Error Handling and Recovery**

   - Smart distinction between 404 errors (missing data) and network failures
   - Exponential backoff retry mechanism with jitter
   - Detailed error categorization and reporting

4. **Performance Optimization**

   - Parallel download and processing
   - Configurable connection parameters
   - Efficient file operations and cleanup

5. **Comprehensive Reports**
   - Detailed CSV reports with validation results
   - Separate tracking of failed downloads
   - Summary statistics and targeted recommendations

### Date Processing Logic

The script processes data from the **newest date (LATEST_DATE)** backward to the **oldest date (EARLIEST_DATE)**. This approach:

- Allows finding the most recent data first
- Correctly handles symbols that may have been delisted (like LUNA during the May 2022 crash)
- Provides meaningful filenames that indicate the date range of contained data

Output filenames follow the convention: `market_symbol_interval_earliest-date_to_latest-date_label_timestamp.csv`

## Usage Examples and Configuration

```bash
# Run with default settings
./verify_multi_interval.sh

# Run with custom configuration
SYMBOLS="BTCUSDT ETHUSDT" INTERVALS="1m 1h" ./verify_multi_interval.sh

# Enable automatic dependency installation
AUTO_INSTALL_DEPS=true ./verify_multi_interval.sh
```

### Configuration

Edit these variables at the top of the script:

```bash
# Data source configuration
MARKET_TYPE="spot"         # "spot", "um" (USDT-M futures), or "cm" (COIN-M futures)
SYMBOLS="BTCUSDT ETHUSDT"  # Space-separated list of symbols
INTERVALS="1m 1h 1d"       # Space-separated list of intervals

# Date range configuration
LATEST_DATE="2023-01-01"   # Latest date to process from
EARLIEST_DATE="2022-01-01" # Earliest date to process until
LATEST_DATE_AUTO=true      # Auto-detect latest available date
EARLIEST_DATE_AUTO=true    # Auto-detect earliest available date

# Performance configuration
MAX_PARALLEL=50            # Number of parallel processes
DOWNLOAD_TIMEOUT=30        # Download timeout in seconds
```

## Troubleshooting Common Issues

### "File not found" errors

These are expected for dates before a symbol started trading or for dates when trading was suspended. They are not script errors.

### Download failures

If you encounter download failures:

1. Check your network connection
2. Reduce MAX_PARALLEL (try 10-20)
3. Increase DOWNLOAD_TIMEOUT (try 60-120 seconds)
4. Run again with a more focused symbol/interval list

### For LUNA/UST specific issues

During May 2022, the LUNA/UST collapse occurred, which may have resulted in trading suspensions and missing data files for certain dates. The script will correctly identify these as "File not found" errors and properly document them in the failure reports.
