# Bybit Kline Data Fetching - Duplicate Test

This directory contains a Python script (`fetch_data_batch_test.py`) designed to investigate potential duplicate data when fetching historical Bybit kline data in small, consecutive batches using the REST API.

## The Problem

When fetching historical kline data from the Bybit API (`/v5/market/kline`) in multiple requests, especially with smaller `limit` sizes, a naive approach of using the timestamp of the oldest kline from the previous batch as the `end` time for the next request can lead to duplicate data points.

Our tests with odd batch sizes (3, 5, and 7) for 5-minute and 15-minute intervals consistently showed duplicates in the combined dataset.

## Technical Insight

The Bybit API's `/v5/market/kline` endpoint returns data points _up to and including_ the timestamp specified in the `end` parameter. When fetching historical data by iterating backwards in time, if the `end` time of the current request is set exactly to the timestamp of the oldest kline received in the _previous_ request, that oldest kline will be included again as the _newest_ kline in the current response.

## The Solution

To avoid duplicates when fetching historical data in batches, the `end` timestamp for the next request should be set to the timestamp of the oldest kline received in the current batch **minus one interval duration**.

The `fetch_data_batch_test.py` script implements this corrected logic. It calculates the timestamp for the next request by subtracting the interval duration (converted to milliseconds) from the timestamp of the last kline in the current batch (which is the oldest, as the API returns data in reverse chronological order).

## Script Details

- **File:** `fetch_data_batch_test.py`
- **Purpose:** To fetch multiple consecutive batches of kline data using specified limit sizes and check for duplicates in the combined dataset.
- **Libraries Used:** `typer` for command-line interface, `httpx` for API requests, `rich` for formatted output.

## How to Run

1.  Navigate to the script directory in your terminal: `cd playground/bybit/non-overlap/`
2.  Ensure the script has execute permissions (if running directly): `chmod +x fetch_data_batch_test.py` (Alternatively, run with `python fetch_data_batch_test.py ...`)
3.  Run the script using the `python` interpreter, providing the required arguments:

    ```bash
    python fetch_data_batch_test.py --category <market_category> --symbol <trading_pair> --interval <interval> --num-batches <number_of_batches> --limit <limit1> --limit <limit2> ...
    ```

    **Example:**

    ```bash
    python fetch_data_batch_test.py --category inverse --symbol BTCUSD --interval 5 --num-batches 10 --limit 3 --limit 5 --limit 7
    ```

    Replace `<market_category>`, `<trading_pair>`, `<interval>`, `<number_of_batches>`, and `<limitX>` with your desired values. The script currently supports `interval` values of "5" and "15".

## Results

Running the script with the corrected `end` timestamp calculation for subsequent batches confirms that no duplicates are found in the combined data, even when using small or "odd" batch sizes like 3, 5, or 7. This validates the method for reliable historical data pagination from the Bybit API.
