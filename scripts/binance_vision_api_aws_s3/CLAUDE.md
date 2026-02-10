# Binance Vision API & AWS S3 Data Downloader

**Hub**: [scripts/](../CLAUDE.md) | **Root**: [CLAUDE.md](../../CLAUDE.md)

Scripts for downloading and verifying data from the Binance Vision API and AWS S3 storage.

## Binance Data Availability Fetcher

The `fetch_binance_data_availability.sh` script efficiently retrieves all available trading symbols and their earliest available data date from Binance Vision data repository. It works with spot, um (USDT-M futures), and cm (COIN-M futures) markets and creates filtered lists based on specified criteria.

### Features

- Multi-market support (spot, USDT-M futures, COIN-M futures)
- Parallel processing for faster data retrieval
- Automatic generation of market-specific and combined reports
- Cross-market symbol filtering based on quote currencies
- Customizable output formats and directories
- Historical data store for improved performance on subsequent runs

### CSV Output Structure

The script creates CSV files with the following columns:

- `market` - Market type (spot, um, cm)
- `symbol` - Trading symbol (e.g., BTCUSDT)
- `earliest_date` - Earliest date data is available
- `available_intervals` - Comma-separated list of available kline intervals (properly quoted as a single field)

Additionally, filtered files include:

- `base_symbol` - The base symbol extracted from the trading pair (e.g., BTC from BTCUSDT)

The consolidated base symbols file contains columns:

- `base_symbol` - Base symbol (e.g., BTC)
- `spot_symbol`, `spot_earliest_date`, `spot_available_intervals` - Data for spot market
- `um_symbol`, `um_earliest_date`, `um_available_intervals` - Data for USDT-M futures
- `cm_symbol`, `cm_earliest_date`, `cm_available_intervals` - Data for COIN-M futures

### Recent Changes

- **CSV Format Improvement**: The `available_intervals` field is now properly quoted to ensure strict CSV format compliance where each comma-separated value corresponds to exactly one column.
- **Removed redundant `interval` column**: Since the script primarily uses the 1d interval to find the earliest available date, the interval column was removed from CSV outputs for clarity.
- **Added historical data store**: Speeds up consecutive runs by caching previous results.
- **Better cross-market symbol matching**: Improved filtering for spot+um and spot+um+cm combinations.

### Usage

Basic usage:

```bash
# Run with default settings
./fetch_binance_data_availability.sh
```

Advanced options:

```bash
# Customize with options
./fetch_binance_data_availability.sh --output custom_dir --markets spot,um --parallel 30

# Run in test mode (processes only a few symbols per market)
./fetch_binance_data_availability.sh -t

# Skip using the historical data store
./fetch_binance_data_availability.sh -s

# Run with debugging information
./fetch_binance_data_availability.sh -d
```

Full list of options:

```bash
Usage: ./fetch_binance_data_availability.sh [OPTIONS]

Options:
  -o, --output DIR       Output directory (default: scripts/binance_vision_api_aws_s3/reports)
  -c, --data-store DIR   Historical data store directory
  -m, --markets MARKETS  Comma-separated list of markets to scan (default: spot,um,cm)
  -p, --parallel N       Number of parallel processes (default: 100)
  -i, --interval INTVL   Default interval to check for earliest date (default: 1d)
  -d, --debug            Enable debug logging
  -t, --test             Test mode: only process a few symbols per market
  -s, --skip-data-store  Skip using historical data store and fetch all data fresh
  -q, --quiet            Suppress progress information
  --no-perf              Disable performance statistics
  --auto-install-deps    Automatically install missing dependencies
  -h, --help             Display this help message and exit
```

### Output Files

The script generates several output files in the specified output directory:

1. **Market-specific files**:
   - `spot_earliest_dates.csv` - All spot market symbols and their earliest dates
   - `um_earliest_dates.csv` - All USDT-M futures symbols and earliest dates
   - `cm_earliest_dates.csv` - All COIN-M futures symbols and earliest dates

2. **Combined files**:
   - `all_markets_earliest_dates.csv` - Combined results from all scanned markets

3. **Filtered files**:
   - `spot_um_usdt_filtered.csv` - Symbols that exist in both spot and USDT-M markets with USDT quote currency
   - `spot_synchronal.csv` - Symbols that exist in all three markets

4. **Consolidated file**:
   - `consolidated_base_symbols.csv` - Base symbols with their data from all available markets

5. **Performance data**:
   - `performance.log` - Timing information about script execution

### Understanding and Using the Results

The generated CSV files can be used for several purposes:

1. **Data Availability Analysis**: Find when a symbol's data first became available across different markets
2. **Cross-Market Correlation**: Identify symbols trading across multiple markets for cross-market analysis
3. **Data Collection Planning**: Plan data collection jobs based on available intervals and date ranges

Example of working with the data:

```bash
# Sort symbols by earliest date (newest first)
sort -t, -k3,3 -r reports/spot_earliest_dates.csv | head -10

# Find symbols available in both spot and futures markets
cat reports/spot_um_usdt_filtered.csv

# Extract specific base symbols
grep "^BTC," reports/consolidated_base_symbols.csv
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

## Workflow Examples

### Example 1: Find the earliest date for all BTC pairs

```bash
# Run the fetch script to get all data
./fetch_binance_data_availability.sh

# Filter the results for BTC pairs
grep "BTC" reports/all_markets_earliest_dates.csv > btc_pairs.csv
```

### Example 2: Find symbols available in all markets and download their data

```bash
# Generate the filtered list of symbols in all markets
./fetch_binance_data_availability.sh

# Use the filtered list to download data
SYMBOLS=$(awk -F, 'NR>1 {print $2}' reports/spot_synchronal.csv | tr '\n' ' ')
INTERVALS="1d" ./verify_multi_interval.sh
```

### Example 3: Generate a report of recently added symbols

```bash
# Get all symbols and sort by newest first
./fetch_binance_data_availability.sh
sort -t, -k3,3 -r reports/all_markets_earliest_dates.csv | head -20 > newest_symbols.csv

# Use these symbols for analysis or data collection
```
