# Interval Tests

This directory contains integration tests focused on different interval types for data retrieval across market types.

## Market Types & Intervals Tested

The tests focus on:

- **SPOT**: Regular spot trading (e.g., BTCUSDT) with all intervals including 1s interval (only available for SPOT)
- **FUTURES_USDT (UM)**: USDT-margined futures (e.g., BTCUSDT) with all intervals except 1s
- **FUTURES_COIN (CM)**: Coin-margined futures (e.g., BTCUSD_PERP) with all intervals except 1s

## Intervals Tested

Tests cover all standard intervals:

- 1 second (SPOT only)
- 1 minute
- 3 minutes
- 5 minutes
- 15 minutes
- 30 minutes
- 1 hour
- 2 hours
- 4 hours
- 6 hours
- 8 hours
- 12 hours
- 1 day

## Key Testing Principles

Following the `pytest-construction.mdc` guidelines:

1. **Uses real data only** - No mocks, always testing against real API endpoints
2. **Backward search for availability** - For each test case, searches backward up to 3 days to find data
3. **Proper cleanup** - Ensures proper initialization and cleanup of resources
4. **Error handling without skipping** - Tests properly handle errors without using pytest.skip()
5. **Proper async test configuration** - Uses `asyncio_default_fixture_loop_scope=function`

## Running the Tests

The tests should be run using the project's `run_tests_parallel.sh` script:

```bash
# Run all interval tests
scripts/op/run_tests_parallel.sh tests/intervals

# Run with debug logging
scripts/op/run_tests_parallel.sh tests/intervals DEBUG

# Run specific interval tests
scripts/op/run_tests_parallel.sh tests/intervals INFO "-k test_minute_1_interval"
```
