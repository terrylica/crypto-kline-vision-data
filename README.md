# Raw Data Service

A high-performance, robust package for efficient market data retrieval from multiple data providers, including [Binance Vision](https://data.binance.vision/) and Binance REST ([Spot](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints), [USDS-Margined Futures](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info), [Coin-Margined Futures](https://developers.binance.com/docs/derivatives/coin-margined-futures/general-info)) using Apache Arrow MMAP for optimal performance.

## Installation

You can quickly get started with Raw Data Service using pip:

```bash
# Clone the repository
git clone https://github.com/Eon-Labs/raw-data-services.git
cd raw-data-services

# Create and activate a virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate

# Install the package (in development mode)
pip install -e .

# For development dependencies (optional)
pip install -e ".[dev]"
```

The installation process automatically registers the CLI commands (`dsm-demo-cli` and `dsm-demo-module`) as executable scripts in your Python environment. These commands will be available in your terminal after installation.

> **Note**: If you encounter import errors related to `utils_for_debug` or other modules, ensure your `pyproject.toml` file includes all required packages in the `packages` list under `[tool.setuptools]`.

## Running the Demos

Once installed, you can run the demos using the command-line tools:

### DSM Demo CLI

The CLI demonstration provides an interactive way to explore the Failover Control Protocol:

```bash
# Run the DSM Demo CLI with default parameters
dsm-demo-cli

# Run with specific parameters (get BTC data for a 10-day period)
dsm-demo-cli -s BTCUSDT -i 1m -d 10

# Get help and see all available options
dsm-demo-cli --help
```

The CLI tool will automatically:

1. Check for data in local cache
2. Try to fetch missing data from Binance Vision API
3. Fall back to REST API for data not available in cache or Vision API
4. Save retrieved data to cache for future use

### DSM Demo Module

The module demo provides a programmatic interface:

```bash
# Run the DSM Demo Module to see examples
dsm-demo-module

# Get help and see all available options
dsm-demo-module --help
```

## Data Source Manager (DSM) Demo

### Quick Start

- **[DSM Demo CLI Documentation](examples/sync/)**: Interactive demonstration of the Failover Control Protocol mechanism, the core data retrieval strategy that ensures robust and efficient data collection from multiple sources.
- **[DSM Demo Module Documentation](examples/lib_module/)**: Programmatic interface to `core/sync/dsm_lib.py` functions, complementing the CLI tool by providing a library approach to implement the same data retrieval functionality in custom applications.

### Understanding Data Sources

The DSM implements a Failover Control Protocol (FCP) that follows this sequence:

1. **Cache**: First checks local Arrow files for requested data
2. **VISION API**: For missing data, attempts to download from Binance Vision API
3. **REST API**: Falls back to Binance REST API for any remaining data gaps

Note that recent data (within ~48 hours) is typically not available in the Vision API and will be retrieved from the REST API.

## Development Guidelines

### Core Principles

- **[Always Focus Demo Rule](.cursor/rules/always-focus-demo.mdc)**: The authoritative instruction file guiding the Cursor Agent to strictly adhere to the demo plan and maintain focus on the Failover Control Protocol demonstration.
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
