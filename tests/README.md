# Binance Data Services Test Suite

This directory contains tests for all components of the Binance Data Services library.

## How to Run Tests

All tests should be run using the `scripts/run_tests_parallel.sh` script, which configures pytest with the proper settings:

```bash
# Run all tests
scripts/run_tests_parallel.sh tests/

# Run specific test directory
scripts/run_tests_parallel.sh tests/time_boundary

# Run with specific log level
scripts/run_tests_parallel.sh tests/api_boundary DEBUG

# Run with additional pytest parameters
scripts/run_tests_parallel.sh tests/time_boundary INFO "-k 'test_cache' --tb=short"
```

## Test Organization

The tests are organized by category:

### Data Source Tests

`tests/data_sources/test_fallback.py` - Tests for data source selection and fallback:

- Automatic fallback from Vision API to REST API
- Download-first approach efficiency
- Caching integration with different data sources
- Error handling for unavailable sources

### API Boundary Tests

`tests/api_boundary/test_api_boundary.py` - Tests for the ApiBoundaryValidator:

- Core validation functionality
- Boundary alignment
- Edge cases (month/year boundaries)
- Error handling

### Market Data Tests

`tests/time_boundary/test_rest_data_validation.py` - Market data validation:

- Data structure validation
- Data integrity
- API limits and chunking
- Retriever integration

### Cache Tests

`tests/time_boundary/test_cache_unified.py` - Cache testing:

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
