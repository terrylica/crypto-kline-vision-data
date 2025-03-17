# Binance Data Services Examples

This directory contains example scripts demonstrating how to use the Binance Data Services package.

## Available Examples

### 1. Recommended Data Retrieval (`recommended_data_retrieval.py`)

Demonstrates the current recommended approach for retrieving data using `DataSourceManager` with unified caching.

```bash
python -m examples.recommended_data_retrieval
```

Features demonstrated:

- Fetching recent market data (last hour)
- Fetching historical data for a specific date
- Proper caching configuration
- Using the correct API based on the data timeframe

### 2. Migration Guide (`migration_guide.py`)

Demonstrates the migration path from the deprecated direct caching with `VisionDataClient` to the recommended approach using `DataSourceManager` with `UnifiedCacheManager`.

```bash
python -m examples.migration_guide
```

This example shows three approaches:

1. **Deprecated Approach**: Direct caching with `VisionDataClient` (will show deprecation warnings)
2. **Recommended Approach**: Using `DataSourceManager` with unified caching
3. **Hybrid Approach**: Using `VisionDataClient` through `DataSourceManager` (useful during migration)

## Running the Examples

All examples are designed to be run from the root of the project. Use the following format:

```bash
# From project root
python -m examples.example_name
```

## Cache Directory

The examples create cache directories to demonstrate caching functionality. You can find the cached data in:

- `./cache/` - for the recommended data retrieval example
- `./cache/deprecated/` - for the deprecated approach in the migration guide
- `./cache/recommended/` - for the recommended approach in the migration guide
- `./cache/hybrid/` - for the hybrid approach in the migration guide

You can delete these directories to clear the cache and force a fresh data download.

## Testing

When running these examples with code coverage, you can use the pytest coverage features:

```bash
# Run any example with coverage
PYTHONPATH=. pytest examples/recommended_data_retrieval.py --cov=. --cov-report=term
```

This will display the coverage report directly in the terminal, showing which parts of the code are covered by the tests.
