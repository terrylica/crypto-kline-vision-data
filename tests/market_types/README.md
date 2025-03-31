# Market Types Tests

This directory contains integration tests for different market types supported by Binance:

- **SPOT**: Regular spot trading (e.g., BTCUSDT)
- **FUTURES_USDT (UM)**: USDT-margined futures (e.g., BTCUSDT)
- **FUTURES_COIN (CM)**: Coin-margined futures (e.g., BTCUSD_PERP)

## Test Files

- `test_vision_market_types.py` - Tests Vision API data retrieval across different market types

## Purpose

These tests verify:

1. **Market Type Handling** - Test that the system correctly handles different market types
2. **Data Availability** - Find available data by searching backward in time
3. **Symbol Format Handling** - Ensure proper handling of market-specific symbol formats (\_PERP suffix for CM)
4. **URL Path Construction** - Verify correct path segments in URLs (/spot/, /futures/um/, /futures/cm/)
5. **Data Format Consistency** - Validate consistent data format across market types

## Running the Tests

The tests should be run using the project's `run_tests_parallel.sh` script:

```bash
# Run all market type tests
scripts/run_tests_parallel.sh tests/market_types

# Run with debug logging
scripts/run_tests_parallel.sh tests/market_types DEBUG

# Run only a specific test function
scripts/run_tests_parallel.sh tests/market_types INFO "-k test_spot_market_data_retrieval"
```

## Implementation Notes

### Data Availability Search

Following the `pytest-construction.mdc` guidelines, these tests implement a backward search for available data:

1. Start from current date and search backward up to 3 days
2. For each date, try to fetch 1 hour of data
3. Use the most recent date with available data
4. Continue testing even if no data is found (validate empty DataFrame structure)

This strategy ensures:

- Tests use real data (no mocks)
- Tests don't fail due to data availability issues
- Tests properly validate empty result handling

### No Skipping

Tests will run even if no data is available, verifying that:

1. The system handles empty results gracefully
2. Empty DataFrames maintain proper structure
3. Error conditions are handled properly
