# Data Sources Tests

This directory contains integration tests for the data source selection and fallback mechanisms.

## Test Files

- `test_fallback.py` - Tests the automatic fallback from Vision API to REST API and download-first approach

## Purpose

These tests verify:

1. **Data Source Selection** - Tests that the system correctly chooses between Vision API and REST API
2. **Fallback Mechanism** - Validates automatic fallback from Vision API to REST API when necessary
3. **Download-First Approach** - Tests the efficiency of the direct download approach without pre-checking
4. **Caching Integration** - Ensures caching works correctly with different data sources

## Running the Tests

The tests should be run using the project's `run_tests_parallel.sh` script to ensure proper Python path configuration:

```bash
# Run all data source tests
scripts/op/run_tests_parallel.sh tests/data_sources

# Run with debug logging
scripts/op/run_tests_parallel.sh tests/data_sources DEBUG

# Run only a specific test function
scripts/op/run_tests_parallel.sh tests/data_sources INFO "-k test_vision_to_rest_fallback"
```

## Test Environment

These tests require the following to run successfully:

- Network connectivity to test against real APIs
- Cache directory with write permissions
- Sufficient memory for data processing

Some tests may be skipped in CI environments where network access is limited.

## Implementation Notes

- Tests use the `conftest.py` fixture to ensure proper import paths
- Direct imports from `core` and `utils` are used (paths set by the conftest file)
- Each test is runnable individually via pytest
- The tests are designed to work in the Docker development container
