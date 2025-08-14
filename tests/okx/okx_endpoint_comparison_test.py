#!/usr/bin/env python3

import json
import time
from datetime import datetime

import httpx
from rich import print
from rich.console import Console
from rich.table import Table

from data_source_manager.utils.config import MIN_RECORDS_FOR_COMPARISON
from data_source_manager.utils.loguru_setup import logger

# API constants
OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"

# Test parameters
SPOT_INSTRUMENT = "BTC-USDT"
SWAP_INSTRUMENT = "BTC-USD-SWAP"
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
MAX_LIMIT = 100  # Max limit for OKX API
INTERVALS = ["1m", "3m", "5m", "15m", "30m", "1H", "4H", "1D", "1W", "1M"]


def retry_request(url, params=None, max_retries=MAX_RETRIES):
    """Make HTTP request with retry logic."""
    for attempt in range(max_retries):
        try:
            response = httpx.get(url, params=params, timeout=10.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.critical(f"All {max_retries} attempts failed for URL: {url}")
                raise

    return None


def test_boundary_overlap(instrument="BTC-USDT", interval="1m", limit=10):
    """
    Test 1: Boundary overlap test
    Confirm whether candles and history-candles endpoints overlap or are strictly partitioned.
    """
    # Step 1: Get the earliest available data from candles endpoint
    candles_params = {"instId": instrument, "bar": interval, "limit": limit}
    candles_data = retry_request(CANDLES_ENDPOINT, candles_params)

    if not candles_data.get("data"):
        return {
            "test": "boundary_overlap",
            "status": "error",
            "message": "Failed to get data from candles endpoint",
        }

    # Get the oldest timestamp from candles endpoint (timestamps are in ms)
    # Note: OKX returns data in reverse order (newest first)
    oldest_candle = candles_data["data"][-1]
    oldest_timestamp = int(oldest_candle[0])  # timestamp is the first field

    # Step 2: Try to get the same timestamp from history-candles endpoint
    history_params = {
        "instId": instrument,
        "bar": interval,
        "limit": limit,
        "before": oldest_timestamp + 1,  # Add 1 to include the oldest timestamp
    }

    history_data = retry_request(HISTORY_CANDLES_ENDPOINT, history_params)

    if not history_data.get("data"):
        return {
            "test": "boundary_overlap",
            "status": "no_overlap",
            "message": "No data from history-candles with the same timestamp",
        }

    # Check if we found any matching timestamps
    matching_entries = []
    for candle in candles_data["data"]:
        candle_timestamp = candle[0]
        for history_candle in history_data["data"]:
            if history_candle[0] == candle_timestamp:
                matching_entries.append(
                    {
                        "timestamp": candle_timestamp,
                        "candles_data": candle,
                        "history_data": history_candle,
                    }
                )

    return {
        "test": "boundary_overlap",
        "status": "success",
        "matching_count": len(matching_entries),
        "overlap_exists": len(matching_entries) > 0,
        "matching_entries": matching_entries[:3],  # Limit to first 3 for readability
    }


def test_latency_freshness(instrument="BTC-USDT", interval="1m", limit=1):
    """
    Test 2: Latency/freshness test
    Compare how far behind history-candles is from real-time compared to candles endpoint.
    """
    # Get the latest data from candles endpoint
    candles_params = {"instId": instrument, "bar": interval, "limit": limit}
    candles_data = retry_request(CANDLES_ENDPOINT, candles_params)

    if not candles_data.get("data"):
        return {
            "test": "latency_freshness",
            "status": "error",
            "message": "Failed to get data from candles endpoint",
        }

    # Get the newest timestamp from candles endpoint
    newest_candle_timestamp = int(
        candles_data["data"][0][0]
    )  # First candle, first field (timestamp)

    # Get the latest data from history-candles endpoint
    history_params = {"instId": instrument, "bar": interval, "limit": limit}
    history_data = retry_request(HISTORY_CANDLES_ENDPOINT, history_params)

    if not history_data.get("data"):
        return {
            "test": "latency_freshness",
            "status": "error",
            "message": "Failed to get data from history-candles endpoint",
        }

    # Get the newest timestamp from history-candles endpoint
    newest_history_timestamp = int(
        history_data["data"][0][0]
    )  # First candle, first field (timestamp)

    # Calculate time difference
    time_diff_ms = newest_candle_timestamp - newest_history_timestamp
    time_diff_seconds = time_diff_ms / 1000
    time_diff_minutes = time_diff_seconds / 60
    time_diff_hours = time_diff_minutes / 60

    return {
        "test": "latency_freshness",
        "status": "success",
        "candles_timestamp": newest_candle_timestamp,
        "candles_time": datetime.fromtimestamp(newest_candle_timestamp / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "history_timestamp": newest_history_timestamp,
        "history_time": datetime.fromtimestamp(
            newest_history_timestamp / 1000
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "difference_ms": time_diff_ms,
        "difference_seconds": time_diff_seconds,
        "difference_minutes": time_diff_minutes,
        "difference_hours": time_diff_hours,
    }


def test_data_consistency(instrument="BTC-USDT", interval="1m", timestamp=None):
    """
    Test 3: Data consistency
    Check if the same timestamp returns identical OHLCV data across both endpoints.
    """
    # If no timestamp provided, get one from the latest history-candles data
    if timestamp is None:
        history_params = {"instId": instrument, "bar": interval, "limit": 1}
        history_data = retry_request(HISTORY_CANDLES_ENDPOINT, history_params)

        if not history_data.get("data"):
            return {
                "test": "data_consistency",
                "status": "error",
                "message": "Failed to get data from history-candles endpoint",
            }

        timestamp = int(history_data["data"][0][0])

    # Query candles endpoint with the timestamp
    candles_params = {
        "instId": instrument,
        "bar": interval,
        "limit": 1,
        "before": timestamp + 1,  # Add 1 to include the target timestamp
    }
    candles_data = retry_request(CANDLES_ENDPOINT, candles_params)

    # Query history-candles endpoint with the same timestamp
    history_params = {
        "instId": instrument,
        "bar": interval,
        "limit": 1,
        "before": timestamp + 1,  # Add 1 to include the target timestamp
    }
    history_data = retry_request(HISTORY_CANDLES_ENDPOINT, history_params)

    # Check if we got data from both endpoints
    candles_entry = None
    for candle in candles_data.get("data", []):
        if int(candle[0]) == timestamp:
            candles_entry = candle
            break

    history_entry = None
    for candle in history_data.get("data", []):
        if int(candle[0]) == timestamp:
            history_entry = candle
            break

    if not candles_entry or not history_entry:
        return {
            "test": "data_consistency",
            "status": "no_match",
            "message": "Could not find matching timestamp in both endpoints",
            "timestamp": timestamp,
            "candles_found": candles_entry is not None,
            "history_found": history_entry is not None,
        }

    # Compare the data
    consistent = candles_entry == history_entry

    differences = {}
    if not consistent:
        for i, (c_val, h_val) in enumerate(zip(candles_entry, history_entry)):
            if c_val != h_val:
                field_names = [
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "volCcy",
                    "volCcyQuote",
                    "confirm",
                ]
                field_name = field_names[i] if i < len(field_names) else f"field_{i}"
                differences[field_name] = {"candles": c_val, "history": h_val}

    return {
        "test": "data_consistency",
        "status": "success",
        "timestamp": timestamp,
        "time": datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S"),
        "consistent": consistent,
        "candles_data": candles_entry,
        "history_data": history_entry,
        "differences": differences,
    }


def test_timestamp_handling(instrument="BTC-USDT", interval="1m", limit=5):
    """
    Test 4: Timestamp handling
    Validate if 'after'/'before' parameters are inclusive/exclusive in both endpoints.
    """
    # Get a reference timestamp
    ref_params = {"instId": instrument, "bar": interval, "limit": limit}
    ref_data = retry_request(CANDLES_ENDPOINT, ref_params)

    if not ref_data.get("data") or len(ref_data["data"]) < MIN_RECORDS_FOR_COMPARISON:
        return {
            "test": "timestamp_handling",
            "status": "error",
            "message": "Failed to get enough reference data",
        }

    # Get a middle timestamp to test with
    middle_idx = len(ref_data["data"]) // 2
    test_timestamp = int(ref_data["data"][middle_idx][0])

    # Test 'before' parameter (should include timestamps <= before)
    before_candles_params = {
        "instId": instrument,
        "bar": interval,
        "limit": limit,
        "before": test_timestamp,
    }
    before_candles_data = retry_request(CANDLES_ENDPOINT, before_candles_params)

    before_history_params = {
        "instId": instrument,
        "bar": interval,
        "limit": limit,
        "before": test_timestamp,
    }
    before_history_data = retry_request(HISTORY_CANDLES_ENDPOINT, before_history_params)

    # Test 'after' parameter (should include timestamps >= after)
    after_candles_params = {
        "instId": instrument,
        "bar": interval,
        "limit": limit,
        "after": test_timestamp,
    }
    after_candles_data = retry_request(CANDLES_ENDPOINT, after_candles_params)

    after_history_params = {
        "instId": instrument,
        "bar": interval,
        "limit": limit,
        "after": test_timestamp,
    }
    after_history_data = retry_request(HISTORY_CANDLES_ENDPOINT, after_history_params)

    # Check if test_timestamp is included in the 'before' results
    before_candles_includes = any(
        int(candle[0]) == test_timestamp
        for candle in before_candles_data.get("data", [])
    )
    before_history_includes = any(
        int(candle[0]) == test_timestamp
        for candle in before_history_data.get("data", [])
    )

    # Check if test_timestamp is included in the 'after' results
    after_candles_includes = any(
        int(candle[0]) == test_timestamp
        for candle in after_candles_data.get("data", [])
    )
    after_history_includes = any(
        int(candle[0]) == test_timestamp
        for candle in after_history_data.get("data", [])
    )

    return {
        "test": "timestamp_handling",
        "status": "success",
        "test_timestamp": test_timestamp,
        "time": datetime.fromtimestamp(test_timestamp / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "before_parameter": {
            "candles_includes_timestamp": before_candles_includes,
            "history_includes_timestamp": before_history_includes,
            "consistent": before_candles_includes == before_history_includes,
        },
        "after_parameter": {
            "candles_includes_timestamp": after_candles_includes,
            "history_includes_timestamp": after_history_includes,
            "consistent": after_candles_includes == after_history_includes,
        },
    }


def test_backfill_depth(instrument="BTC-USDT", interval="1D", max_days=365):
    """
    Test 5: Backfill depth
    Check how far back history-candles can go by progressively requesting older data.
    """
    results = []
    current_timestamp = None
    oldest_timestamp = None
    day_count = 0

    # Start with current time
    while day_count < max_days:
        # For first iteration, don't use an 'after' parameter
        if current_timestamp is None:
            params = {"instId": instrument, "bar": interval, "limit": 100}
        else:
            # For subsequent iterations, use the oldest timestamp from previous iteration
            params = {
                "instId": instrument,
                "bar": interval,
                "limit": 100,
                "after": current_timestamp,
            }

        try:
            data = retry_request(HISTORY_CANDLES_ENDPOINT, params)

            if not data.get("data") or len(data["data"]) == 0:
                break

            # OKX returns data in reverse order (newest first)
            # So the oldest candle is the last one in the array
            oldest_candle = data["data"][-1]
            current_timestamp = int(oldest_candle[0])

            if oldest_timestamp is None:
                newest_timestamp = int(data["data"][0][0])
                oldest_timestamp = current_timestamp

            # Record the result
            results.append(
                {
                    "batch": day_count + 1,
                    "oldest_time": datetime.fromtimestamp(
                        current_timestamp / 1000
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    "oldest_timestamp": current_timestamp,
                    "candles_returned": len(data["data"]),
                }
            )

            # Increment day count
            day_count += 1

        except Exception as e:
            logger.error(f"Error during backfill test: {e}")
            results.append({"batch": day_count + 1, "error": str(e)})
            break

    return {
        "test": "backfill_depth",
        "status": "success" if day_count > 0 else "error",
        "total_batches": day_count,
        "instrument": instrument,
        "interval": interval,
        "oldest_data": results[-1] if results else None,
        "newest_data": results[0] if results else None,
        "time_span_days": (
            (newest_timestamp - oldest_timestamp) / (1000 * 60 * 60 * 24)
            if oldest_timestamp and newest_timestamp
            else None
        ),
        "results": results,
    }


def print_results_table(title, data):
    """Print results in a formatted table."""
    console = Console()
    print(f"\n[bold cyan]{title}[/bold cyan]")

    if isinstance(data, dict):
        # Handle dictionary output
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Key", style="dim")
        table.add_column("Value")

        for key, value in data.items():
            if isinstance(value, dict) or isinstance(value, list):
                value_str = json.dumps(value, indent=2)
            else:
                value_str = str(value)
            table.add_row(key, value_str)

        console.print(table)

    elif isinstance(data, list):
        # Handle list output
        if not data:
            console.print("[italic]No data returned[/italic]")
            return

        table = Table(show_header=True, header_style="bold magenta")

        # Add columns based on first item's keys
        for key in data[0].keys():
            table.add_column(key)

        # Add rows
        for item in data:
            row_values = []
            for key in data[0].keys():
                value = item.get(key, "")
                if isinstance(value, dict) or isinstance(value, list):
                    value = json.dumps(value, indent=2)
                row_values.append(str(value))
            table.add_row(*row_values)

        console.print(table)


def run_all_tests(instrument="BTC-USDT", interval="1m"):
    """Run all tests with a single instrument and interval."""
    print("[bold green]Running OKX Endpoint Comparison Tests[/bold green]")
    print(f"[bold]Instrument:[/bold] {instrument}, [bold]Interval:[/bold] {interval}")

    # Test 1: Boundary Overlap
    boundary_result = test_boundary_overlap(instrument, interval)
    print_results_table("1. Boundary Overlap Test", boundary_result)

    # Test 2: Latency/Freshness
    latency_result = test_latency_freshness(instrument, interval)
    print_results_table("2. Latency/Freshness Test", latency_result)

    # Test 3: Data Consistency
    consistency_result = test_data_consistency(instrument, interval)
    print_results_table("3. Data Consistency Test", consistency_result)

    # Test 4: Timestamp Handling
    timestamp_result = test_timestamp_handling(instrument, interval)
    print_results_table("4. Timestamp Handling Test", timestamp_result)

    # Test 5: Backfill Depth (use a larger interval like 1D to avoid too many requests)
    backfill_result = test_backfill_depth(instrument, "1D", max_days=30)
    print_results_table("5. Backfill Depth Test", backfill_result)


def main():
    """Main function to run tests."""
    print("[bold green]OKX Endpoint Comparison Tests[/bold green]")
    print(
        "Testing differences between /market/candles and /market/history-candles endpoints"
    )

    # Run all tests with default parameters
    run_all_tests(SPOT_INSTRUMENT, "1m")


if __name__ == "__main__":
    main()
