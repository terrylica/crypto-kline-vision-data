# DSM Sync Simple Examples

## IMPORTANT: Code Refactoring

**The FCP Demo has been refactored for improved maintainability.**

- The refactored implementation is in `fcp_demo.py`
- The legacy implementation has been moved to `legacy/fcp_demo_legacy.py`
- See [README_REFACTORING.md](./README_REFACTORING.md) for details

## Overview

This directory contains a simple demonstration of the synchronous data retrieval capabilities using the `DataSourceManager` with Failover Composition Priority (FCP) strategy.

The demonstration shows how to retrieve 1-minute candlestick data for Bitcoin in various markets (SPOT, USDT-margined futures, COIN-margined futures) using multiple data sources:

1. **Cache** (for data already stored in local Arrow files)
2. **VISION API** (for historical data older than 48 hours)
3. **REST API** (for recent data within the last 48 hours)

The `DataSourceManager` orchestrates these data sources with automatic failover and merges the results seamlessly.

## Usage

The demo has been consolidated into a single Python script with a user-friendly interface.

```bash
python examples/dsm_sync_simple/demo.py [OPTIONS]
python examples/dsm_sync_simple/demo.py market symbol interval chart_type
```

### Parameters

- `market`: Market type: spot, um, or cm (default: spot)
- `symbol`: Trading symbol (default: BTCUSDT)
- `interval`: Time interval: 1m, 5m, etc. (default: 1m)
- `chart_type`: Type of chart data: klines or fundingRate (default: klines)

### Options

- `-h, --help`: Show help message and exit
- `--cache-demo`: Demonstrate cache behavior by running the data retrieval twice
- `--historical-test`: Run historical test with specific dates (Dec 2024-Feb 2025)
- `--detailed-stats`: Show detailed statistics after the run and save to JSON file
- `--gap-report`: Generate a detailed gap report analyzing data continuity

### Examples

```bash
# Run default FCP merge demo for BTCUSDT in SPOT market
python examples/dsm_sync_simple/demo.py

# Run merge demo for ETH with 5m interval
python examples/dsm_sync_simple/demo.py spot ETHUSDT 5m klines

# Run merge demo for BTC in UM futures market
python examples/dsm_sync_simple/demo.py um BTCUSDT 1m klines

# Run merge demo for BTC in CM futures market
python examples/dsm_sync_simple/demo.py cm BTCUSD_PERP 1m klines

# Demonstrate cache performance
python examples/dsm_sync_simple/demo.py --cache-demo spot BTCUSDT

# Run historical test in SPOT market
python examples/dsm_sync_simple/demo.py --historical-test spot

# Run with detailed statistics
python examples/dsm_sync_simple/demo.py --detailed-stats spot

# Generate a gap analysis report
python examples/dsm_sync_simple/demo.py --gap-report spot BTCUSDT 1m --gap-threshold 0.3
```

## Real Data Analysis

For more advanced data quality analysis with real market data, use the `real_data_diagnostics.py` script:

```bash
# Basic usage with default parameters
python examples/dsm_sync_simple/real_data_diagnostics.py

# Analyze ETHUSDT in UM futures market over 2 days with 5-minute intervals
python examples/dsm_sync_simple/real_data_diagnostics.py --market um --symbol ETHUSDT --interval 5m --days 2

# Analyze with a specific gap threshold (10% above expected interval)
python examples/dsm_sync_simple/real_data_diagnostics.py --market spot --symbol BTCUSDT --gap-threshold 0.1
```

This script provides comprehensive data quality analysis including:

- Detailed gap detection and analysis
- Data source distribution
- Day boundary transition validation
- Continuity metrics

All analysis results are saved to JSON files in the `logs/real_data_analysis/` directory.

## Notes

- For CM (Coin-Margined) futures, the BTC symbol is `BTCUSD_PERP` (not `BTCUSDT`).
- The script validates the current working directory and will attempt to navigate to the project root if necessary.
- Dependency checking is performed to ensure required packages are installed.
- All output data is saved to CSV files in the `logs/` directory.
- Detailed statistics are saved to JSON files in the `logs/statistics/` directory.

_Note: The previous shell script (demo.sh) has been consolidated into this Python script for easier maintenance and use._
