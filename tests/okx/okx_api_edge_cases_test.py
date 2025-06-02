#!/usr/bin/env python3

import json
import time
from datetime import datetime, timedelta

import httpx
from rich import print
from rich.console import Console
from rich.table import Table

from utils.loguru_setup import logger

# API constants
OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"

# Test parameters
SPOT_INSTRUMENT = "BTC-USDT"
SWAP_INSTRUMENT = "BTC-USD-SWAP"
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds


def retry_request(url, params=None, max_retries=MAX_RETRIES):
    """Make HTTP request with retry logic."""
    for attempt in range(max_retries):
        try:
            response = httpx.get(url, params=params, timeout=10.0)
            response.raise_for_status()
            return {
                "status_code": response.status_code,
                "data": response.json(),
            }
        except Exception as e:
            logger.error(f"Request failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.critical(f"All {max_retries} attempts failed for URL: {url}")
                return {
                    "status_code": -1,
                    "error": str(e),
                }

    return None


def test_limit_constraints():
    """
    Test limit parameter constraints.
    Official documentation says limit is 100 (default) but our code uses 300 as max.
    This test verifies the actual behavior.
    """
    test_limits = [1, 50, 100, 200, 300, 400, 500]
    results = []

    for limit in test_limits:
        params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": limit}
        response = retry_request(CANDLES_ENDPOINT, params)

        if "error" in response:
            results.append(
                {
                    "limit": limit,
                    "status": "error",
                    "error": response["error"],
                    "records_returned": 0,
                }
            )
        else:
            data = response["data"]
            records_count = len(data.get("data", []))
            results.append(
                {
                    "limit": limit,
                    "status": "success",
                    "records_returned": records_count,
                    "api_code": data.get("code"),
                    "api_message": data.get("msg", ""),
                }
            )

    # Same test for history-candles
    history_results = []
    for limit in test_limits:
        # Use a timestamp 30 days ago to ensure data availability
        timestamp = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        params = {
            "instId": SPOT_INSTRUMENT,
            "bar": "1m",
            "limit": limit,
            "after": timestamp,
        }
        response = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        if "error" in response:
            history_results.append(
                {
                    "limit": limit,
                    "status": "error",
                    "error": response["error"],
                    "records_returned": 0,
                }
            )
        else:
            data = response["data"]
            records_count = len(data.get("data", []))
            history_results.append(
                {
                    "limit": limit,
                    "status": "success",
                    "records_returned": records_count,
                    "api_code": data.get("code"),
                    "api_message": data.get("msg", ""),
                }
            )

    return {"candles_limits": results, "history_candles_limits": history_results}


def test_invalid_instrument():
    """Test the API behavior with invalid instrument IDs."""
    invalid_instruments = [
        "BTCUSDT",  # No hyphen
        "BTC-INVALID",  # Invalid quote currency
        "INVALID-USDT",  # Invalid base currency
        "",  # Empty string
    ]

    results = []
    for instrument in invalid_instruments:
        params = {"instId": instrument, "bar": "1m", "limit": 10}
        response = retry_request(CANDLES_ENDPOINT, params)

        if "error" in response:
            results.append(
                {
                    "instrument": instrument,
                    "status": "client_error",
                    "error": response["error"],
                }
            )
        else:
            data = response["data"]
            status = "success" if data.get("code") == "0" else "api_error"
            results.append(
                {
                    "instrument": instrument,
                    "status": status,
                    "api_code": data.get("code"),
                    "api_message": data.get("msg", ""),
                    "records_returned": len(data.get("data", [])),
                }
            )

    # Same test for history-candles
    history_results = []
    for instrument in invalid_instruments:
        timestamp = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        params = {"instId": instrument, "bar": "1m", "limit": 10, "after": timestamp}
        response = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        if "error" in response:
            history_results.append(
                {
                    "instrument": instrument,
                    "status": "client_error",
                    "error": response["error"],
                }
            )
        else:
            data = response["data"]
            status = "success" if data.get("code") == "0" else "api_error"
            history_results.append(
                {
                    "instrument": instrument,
                    "status": status,
                    "api_code": data.get("code"),
                    "api_message": data.get("msg", ""),
                    "records_returned": len(data.get("data", [])),
                }
            )

    return {
        "candles_invalid_instruments": results,
        "history_invalid_instruments": history_results,
    }


def test_invalid_intervals():
    """Test the API behavior with invalid interval values."""
    # Mix of valid and invalid intervals
    test_intervals = [
        "1m",  # Valid
        "1s",  # Invalid - OKX doesn't support 1 second
        "2h",  # Invalid format - should be 2H
        "1d",  # Invalid format - should be 1D
        "5M",  # Invalid - 5 month doesn't exist
        "invalid",  # Invalid - gibberish
    ]

    results = []
    for interval in test_intervals:
        params = {"instId": SPOT_INSTRUMENT, "bar": interval, "limit": 10}
        response = retry_request(CANDLES_ENDPOINT, params)

        if "error" in response:
            results.append(
                {
                    "interval": interval,
                    "status": "client_error",
                    "error": response["error"],
                }
            )
        else:
            data = response["data"]
            status = "success" if data.get("code") == "0" else "api_error"
            results.append(
                {
                    "interval": interval,
                    "status": status,
                    "api_code": data.get("code"),
                    "api_message": data.get("msg", ""),
                    "records_returned": len(data.get("data", [])),
                }
            )

    # Same test for history-candles
    history_results = []
    for interval in test_intervals:
        timestamp = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        params = {
            "instId": SPOT_INSTRUMENT,
            "bar": interval,
            "limit": 10,
            "after": timestamp,
        }
        response = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        if "error" in response:
            history_results.append(
                {
                    "interval": interval,
                    "status": "client_error",
                    "error": response["error"],
                }
            )
        else:
            data = response["data"]
            status = "success" if data.get("code") == "0" else "api_error"
            history_results.append(
                {
                    "interval": interval,
                    "status": status,
                    "api_code": data.get("code"),
                    "api_message": data.get("msg", ""),
                    "records_returned": len(data.get("data", [])),
                }
            )

    return {
        "candles_invalid_intervals": results,
        "history_invalid_intervals": history_results,
    }


def test_timestamp_edge_cases():
    """Test edge cases with timestamp parameters."""
    now_ms = int(datetime.now().timestamp() * 1000)
    far_future_ms = now_ms + (365 * 24 * 60 * 60 * 1000)  # 1 year in the future
    far_past_ms = 1420070400000  # 2015-01-01

    test_cases = [
        {"name": "future_timestamp", "params": {"after": far_future_ms}},
        {"name": "very_old_timestamp", "params": {"after": far_past_ms}},
        {"name": "now_timestamp", "params": {"before": now_ms}},
        {
            "name": "before_older_than_after",
            "params": {"before": far_past_ms, "after": now_ms},
        },
    ]

    results = []
    for case in test_cases:
        params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": 10, **case["params"]}
        response = retry_request(CANDLES_ENDPOINT, params)

        if "error" in response:
            results.append(
                {
                    "case": case["name"],
                    "status": "client_error",
                    "error": response["error"],
                    "params": case["params"],
                }
            )
        else:
            data = response["data"]
            status = "success" if data.get("code") == "0" else "api_error"
            results.append(
                {
                    "case": case["name"],
                    "status": status,
                    "api_code": data.get("code"),
                    "api_message": data.get("msg", ""),
                    "records_returned": len(data.get("data", [])),
                    "params": case["params"],
                }
            )

    # Same test for history-candles
    history_results = []
    for case in test_cases:
        params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": 10, **case["params"]}
        response = retry_request(HISTORY_CANDLES_ENDPOINT, params)

        if "error" in response:
            history_results.append(
                {
                    "case": case["name"],
                    "status": "client_error",
                    "error": response["error"],
                    "params": case["params"],
                }
            )
        else:
            data = response["data"]
            status = "success" if data.get("code") == "0" else "api_error"
            history_results.append(
                {
                    "case": case["name"],
                    "status": status,
                    "api_code": data.get("code"),
                    "api_message": data.get("msg", ""),
                    "records_returned": len(data.get("data", [])),
                    "params": case["params"],
                }
            )

    return {
        "candles_timestamp_edge_cases": results,
        "history_timestamp_edge_cases": history_results,
    }


def test_missing_required_parameters():
    """Test API behavior when required parameters are missing."""
    test_cases = [
        {"name": "missing_instId", "params": {"bar": "1m", "limit": 10}},
        {"name": "missing_bar", "params": {"instId": SPOT_INSTRUMENT, "limit": 10}},
        {"name": "empty_instId", "params": {"instId": "", "bar": "1m", "limit": 10}},
    ]

    results = []
    for case in test_cases:
        response = retry_request(CANDLES_ENDPOINT, case["params"])

        if "error" in response:
            results.append(
                {
                    "case": case["name"],
                    "status": "client_error",
                    "error": response["error"],
                    "params": case["params"],
                }
            )
        else:
            data = response["data"]
            status = "success" if data.get("code") == "0" else "api_error"
            results.append(
                {
                    "case": case["name"],
                    "status": status,
                    "api_code": data.get("code"),
                    "api_message": data.get("msg", ""),
                    "records_returned": len(data.get("data", [])),
                    "params": case["params"],
                }
            )

    # Same test for history-candles
    history_results = []
    for case in test_cases:
        response = retry_request(HISTORY_CANDLES_ENDPOINT, case["params"])

        if "error" in response:
            history_results.append(
                {
                    "case": case["name"],
                    "status": "client_error",
                    "error": response["error"],
                    "params": case["params"],
                }
            )
        else:
            data = response["data"]
            status = "success" if data.get("code") == "0" else "api_error"
            history_results.append(
                {
                    "case": case["name"],
                    "status": status,
                    "api_code": data.get("code"),
                    "api_message": data.get("msg", ""),
                    "records_returned": len(data.get("data", [])),
                    "params": case["params"],
                }
            )

    return {
        "candles_missing_params": results,
        "history_missing_params": history_results,
    }


def print_results_table(title, data):
    """Print results in a formatted table."""
    console = Console()
    print(f"\n[bold cyan]{title}[/bold cyan]")

    if isinstance(data, dict):
        # If data has nested dictionaries with lists
        for key, value in data.items():
            if isinstance(value, list):
                print(f"\n[bold yellow]{key}[/bold yellow]")

                if not value:
                    console.print("[italic]No data returned[/italic]")
                    continue

                table = Table(show_header=True, header_style="bold magenta")

                # Add columns based on first item's keys
                for col_key in value[0].keys():
                    table.add_column(col_key)

                # Add rows
                for item in value:
                    row_values = []
                    for col_key in value[0].keys():
                        val = item.get(col_key, "")
                        if isinstance(val, dict) or isinstance(val, list):
                            val = json.dumps(val, indent=2)
                        row_values.append(str(val))
                    table.add_row(*row_values)

                console.print(table)
            elif isinstance(value, dict):
                # Recursively print nested dictionaries
                print_results_table(key, value)
            else:
                # Print simple key-value pairs
                print(f"[bold]{key}:[/bold] {value}")

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


def main():
    """Run edge case tests for OKX API endpoints."""
    print("[bold green]OKX API Edge Cases Tests[/bold green]")
    print("Testing edge cases and parameter validation for OKX API endpoints")

    # Test 1: Limit constraints
    print("\n[bold blue]1. Testing Limit Parameter Constraints[/bold blue]")
    limit_results = test_limit_constraints()
    print_results_table("Limit Parameter Tests", limit_results)

    # Test 2: Invalid instruments
    print("\n[bold blue]2. Testing Invalid Instrument IDs[/bold blue]")
    instrument_results = test_invalid_instrument()
    print_results_table("Invalid Instrument Tests", instrument_results)

    # Test 3: Invalid intervals
    print("\n[bold blue]3. Testing Invalid Intervals[/bold blue]")
    interval_results = test_invalid_intervals()
    print_results_table("Invalid Interval Tests", interval_results)

    # Test 4: Timestamp edge cases
    print("\n[bold blue]4. Testing Timestamp Edge Cases[/bold blue]")
    timestamp_results = test_timestamp_edge_cases()
    print_results_table("Timestamp Edge Case Tests", timestamp_results)

    # Test 5: Missing required parameters
    print("\n[bold blue]5. Testing Missing Required Parameters[/bold blue]")
    param_results = test_missing_required_parameters()
    print_results_table("Missing Parameter Tests", param_results)

    print("\n[bold green]Edge Case Tests Complete![/bold green]")


if __name__ == "__main__":
    main()
