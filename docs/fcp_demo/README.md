# Failover Control Protocol (FCP) Mechanism

The Failover Control Protocol (FCP) mechanism is a data retrieval strategy implemented in the `DataSourceManager` that automatically fetches data from multiple sources in a prioritized sequence. This document describes how FCP works, its benefits, and how to use it in your applications.

## Overview

FCP automatically retrieves data from different sources following a priority order:

1. **Cache** (Local Arrow files) - fastest, but may not have all requested data
2. **VISION API** - reliable for historical data with higher rate limits
3. **REST API** - fallback for real-time or missing data

The mechanism seamlessly combines data from these sources to provide a complete dataset for the requested time period.

## Key Features

- **Automatic Source Selection**: Intelligently chooses the appropriate data source based on availability and completeness
- **Transparent Source Tracking**: Each data point includes metadata about its source
- **Efficient Caching**: Saves retrieved data for faster future access
- **Gap Filling**: Automatically identifies and fills gaps from alternative sources
- **Error Handling**: Gracefully handles API errors and retries failed requests

## Benefits

- **Reliability**: Multiple data sources ensure maximum data availability
- **Performance**: Optimizes for speed by prioritizing faster sources
- **Completeness**: Fills gaps to ensure complete data coverage
- **Efficiency**: Minimizes API calls by leveraging cached data

## How It Works

1. The system first checks the local cache for the requested data
2. If any data is missing, it attempts to retrieve it from the VISION API
3. If VISION API data is still incomplete, it falls back to the REST API
4. All data sources are merged into a single, coherent DataFrame
5. Each record includes source information in the `_data_source` column

## Demo Script

The `fcp_demo.py` script demonstrates the FCP mechanism in action. It shows:

- Data retrieval from multiple sources
- Source breakdown statistics
- Sample data from each source
- Timeline visualization of data sources

## Usage Examples

Run the demo with default parameters:

```bash
./examples/dsm_sync_simple/fcp_demo.py
```

Specify custom parameters:

```bash
./examples/dsm_sync_simple/fcp_demo.py --symbol ETHUSDT --market um --interval 5m --days 2
```

Force a specific data source:

```bash
./examples/dsm_sync_simple/fcp_demo.py --enforce-source REST
```

Run the special FCP test:

```bash
./examples/dsm_sync_simple/fcp_demo.py --test-fcp --prepare-cache
```

## Command-Line Options

| Option             | Shorthand | Description                                                 |
| ------------------ | --------- | ----------------------------------------------------------- |
| `--symbol`         | `-s`      | Trading symbol (e.g., BTCUSDT)                              |
| `--market`         | `-m`      | Market type: spot, um (USDT-M futures), cm (Coin-M futures) |
| `--interval`       | `-i`      | Time interval (e.g., 1m, 5m, 1h)                            |
| `--start-time`     | `-st`     | Start time in ISO format or YYYY-MM-DD                      |
| `--end-time`       | `-et`     | End time in ISO format or YYYY-MM-DD                        |
| `--days`           | `-d`      | Number of days to fetch (if start/end not provided)         |
| `--no-cache`       | `-nc`     | Disable caching (cache is enabled by default)               |
| `--clear-cache`    | `-cc`     | Clear the cache directory before running                    |
| `--enforce-source` | `-es`     | Force specific data source (AUTO, REST, VISION)             |
| `--test-fcp`       | `-fcp`    | Run FCP mechanism test                                      |
| `--retries`        | `-r`      | Maximum number of retry attempts                            |
| `--chart-type`     | `-ct`     | Type of chart data (klines, fundingRate)                    |
| `--log-level`      | `-l`      | Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)       |
| `--prepare-cache`  | `-pc`     | Pre-populate cache (for FCP test)                           |
