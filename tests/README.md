# Binance Data Services Test Suite

This directory contains tests for all components of the Binance Data Services library.

## How to Run Tests

All tests should be run using the `scripts/run_tests_parallel.sh` script, which configures pytest with the proper settings:

```bash
# Run all tests
scripts/run_tests_parallel.sh tests/

# Run specific test directory
scripts/run_tests_parallel.sh tests/interval_1s

# Run with specific log level
scripts/run_tests_parallel.sh tests/api_boundary DEBUG

# Run with additional pytest parameters
scripts/run_tests_parallel.sh tests/interval_1s INFO "-k 'test_cache' --tb=short"
```

## Test Organization

The tests are organized by category:

### API Boundary Tests

`tests/api_boundary/test_api_boundary.py` - Consolidated tests for the ApiBoundaryValidator:

- Core validation functionality
- Boundary alignment
- Edge cases (month/year boundaries)
- Error handling

### Market Data Tests

`tests/interval_1s/test_market_data_validation.py` - Consolidated market data validation:

- Data structure validation
- Data integrity
- API limits and chunking
- Retriever integration

### Cache Tests

`tests/interval_1s/test_cache_unified.py` - Unified cache testing:

- Core cache operations
- Directory structure
- Cache lifecycle (validation, repair)
- Concurrent access
- Integration with DataSourceManager

### Additional Test Directories

- `tests/interval_new/` - Tests for upcoming interval features
- `tests/cache_structure/` - Tests specific to cache structure
- `tests/utils/` - Utility functions for testing

## Test Principles

- All tests use real API data, never mocks
- Tests properly initialize and clean up resources
- Event loops are configured with `loop_scope="function"`
- Tests handle errors appropriately without skipping
- Detailed logging with `caplog` from pytest

For detailed testing guidelines, see the [internal testing guide](pytest-construction.mdc).
