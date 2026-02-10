# OKX API Tests

**Hub**: [tests/](../CLAUDE.md) | **Root**: [CLAUDE.md](../../CLAUDE.md)

Extensive test suite for OKX API implementation — historical market data retrieval via REST API (`/api/v5/market/candles` and `/api/v5/market/history-candles`). Verifies endpoint behavior, data availability, formatting, validation, and edge cases per `docs/api/okx_data_download_guide.md`.

## Test Files

This test suite includes the following files:

- **`okx_api_test.py`**:
  - **Purpose:** Provides basic functional tests for the main OKX candlestick endpoints (`/market/candles` and `/market/history-candles`).
  - **Key Tests:**
    - Verifies successful data retrieval for various standard intervals (`1m`, `1H`, `1D`, etc.) for both SPOT (`BTC-USDT`) and SWAP (`BTC-USD-SWAP`) instruments.
    - Tests the behavior of the `limit` parameter, including checking against the documented and configured maximum limits.
    - Analyzes the structure and data types of the returned candlestick data.

- **`okx_symbol_test.py`**:
  - **Purpose:** Focuses on testing the internal utilities related to OKX symbol handling and endpoint resolution, particularly functions within `src/ckvd/utils/market_constraints.py`.
  - **Key Tests:**
    - Validates the correct formatting of symbols for different market types (SPOT, FUTURES_USDT), ensuring adherence to the `BASE-QUOTE` or `BASE-USD-SWAP` format (e.g., converts `BTCUSDT` to `BTC-USDT`).
    - Tests the validation logic for OKX symbols to ensure only correctly formatted symbols are accepted for specific market types.
    - Verifies that the correct API endpoint URLs are returned based on the market type and chart type (candles or history-candles).

- **`okx_endpoint_comparison_test.py`**:
  - **Purpose:** Performs a detailed comparison between the `/market/candles` and `/market/history-candles` endpoints to understand their differences, overlaps, and specific behaviors.
  - **Key Tests:**
    - **Boundary Overlap:** Confirms if the two endpoints provide overlapping data for recent timestamps.
    - **Latency/Freshness:** Measures the typical time difference between the most recent data available from each endpoint, noting that `history-candles` is usually slightly delayed compared to `candles`.
    - **Data Consistency:** Checks if the OHLCV data returned for the exact same timestamp is identical across both endpoints when available in both.
    - **Timestamp Handling:** Validates the behavior of `before` and `after` pagination parameters, including exclusivity and handling of out-of-range timestamps.
    - **Backfill Depth:** Determines the maximum historical depth of data available from each endpoint for various intervals.

- **`okx_api_edge_cases_test.py`**:
  - **Purpose:** Tests how the OKX API responds to various invalid inputs and less common scenarios.
  - **Key Tests:**
    - **Limit Parameter Constraints:** Explores the actual maximum number of records returned by the API when requesting more than the typical limit.
    - **Invalid Instrument IDs:** Verifies API responses for malformed, non-existent, or empty instrument IDs.
    - **Invalid Intervals:** Tests API responses for unsupported or improperly cased/formatted interval parameters (e.g., `1h` instead of `1H`, `1s` which is not supported).
    - **Timestamp Edge Cases:** Tests API behavior with timestamps that are in the far future, far past (before available data), or invalid combinations of `before` and `after`.
    - **Missing Required Parameters:** Checks if the API correctly returns errors when essential parameters like `instId` are omitted.

- **`test_okx_1s_availability.py`**: **REMOVED**
  - This file has been removed because OKX REST API v5 does NOT support 1-second (`1s`) intervals.
  - Error code `51000` ("Parameter bar error") is returned for the `1s` interval.
  - The minimum supported interval is `1m` (1 minute).

- **`test_okx_candles_depth.py`**:
  - **Purpose:** Systematically determines the historical depth of candlestick data available for a range of intervals from both the `candles` and `history-candles` endpoints.
  - **Key Tests:**
    - Uses a binary search approach to efficiently find the approximate earliest date for which data is available for a given instrument and interval.
    - Verifies the earliest available date by checking for data existence in a range around the estimated date.
    - Provides a clear picture of how far back each endpoint provides data for intervals from `1m` up to `1D`.

- **`okx_interval_validation_test.py`**:
  - **Purpose:** Focuses solely on validating the OKX API's handling of different time interval parameters, especially regarding case sensitivity and supported values.
  - **Key Tests:**
    - Tests case sensitivity for various intervals (e.g., `1h` vs `1H`), confirming OKX's requirement for uppercase for `H`, `D`, `W`, `M`.
    - Tests that the `1s` interval is rejected by the API with error code `51000` ("Parameter bar error").
    - Validates that only officially supported intervals are accepted by the API.
  - **Supported Intervals:** `1m`, `3m`, `5m`, `15m`, `30m`, `1H`, `2H`, `4H`, `6H`, `12H`, `1D`, `1W`, `1M` (and UTC variants like `1Hutc`, `4Hutc`).

## Core Components and Utilities

The test suite relies on several core components and utilities within the codebase:

- **API Endpoints:** Tests interact with `https://www.okx.com/api/v5/market/candles` and `https://www.okx.com/api/v5/market/history-candles`.
- **`src/ckvd/utils/market_constraints.py`:** Provides enums and functions for market types, data providers, chart types, intervals, symbol formatting, and validation, ensuring tests use consistent and correct parameters.
- **`src/ckvd/utils/logger_setup.py`:** Used for logging output within the tests, often utilizing `rich` for formatted console output.
- **`httpx` Library:** Used for making asynchronous HTTP requests to the OKX API.
- **Retry Logic:** A `retry_request` helper function (implemented using `tenacity` in the actual API client, but simulated with a loop in the tests) is used to handle transient network issues with retries.

## Running the Tests

All test files are designed as self-executable Python scripts. You can run individual tests directly from the terminal:

```bash
./tests/okx/<test_file_name>.py
```

For example, to run the symbol tests:

```bash
./tests/okx/okx_symbol_test.py
```

Alternatively, you can use `pytest` to discover and run all tests in the directory:

```bash
pytest tests/okx/
```

## Test Results

Test results are printed to the console using formatted tables provided by the `rich` library, making it easy to read and analyze the outcome of each test case. The output typically includes:

- Status of each test (PASS/FAIL/ERROR).
- Details of the parameters used.
- API response codes and messages.
- Number of records returned.
- Specific findings related to the test objective (e.g., overlap status, latency, data consistency details, validation results, availability).

## Adding New Tests

When contributing new tests to this suite:

1. Create a new executable Python file (`#!/usr/bin/env python3`) in the `tests/okx/` directory.
2. Import necessary modules, including `httpx`, relevant utilities from the `utils` directory (`market_constraints`, `logger_setup`), and `rich` for output.
3. Define test functions that clearly target specific API behaviors, parameters, or edge cases.
4. Use the `retry_request` helper for making API calls to handle retries consistently.
5. Format test output using `rich.print` and `rich.table.Table` for clear reporting.
6. Ensure tests are self-contained and can be run independently or via `pytest`.
7. Update this `CLAUDE.md` file to include a description of the new test file and the areas it covers.
