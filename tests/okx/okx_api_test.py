#!/usr/bin/env python3

import time
from datetime import datetime, timedelta

import httpx
from rich import print
from rich.console import Console
from rich.table import Table

from utils.logger_setup import logger

OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"

# Test parameters
SPOT_INSTRUMENT = "BTC-USDT"
SWAP_INSTRUMENT = "BTC-USD-SWAP"
INTERVALS = [
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1H",
    "2H",
    "4H",
    "6H",
    "12H",
    "1D",
    "1W",
    "1M",
]
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds


def retry_request(url, params=None, max_retries=MAX_RETRIES):
    """Make HTTP request with retry logic."""
    for attempt in range(max_retries):
        try:
            response = httpx.get(url, params=params, timeout=10.0)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Request failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.critical(f"All {max_retries} attempts failed for URL: {url}")
                raise

    return None


def test_candles_endpoint(instrument, interval, limit=100):
    """Test the candles endpoint for a specific instrument and interval."""
    params = {"instId": instrument, "bar": interval, "limit": limit}

    try:
        data = retry_request(CANDLES_ENDPOINT, params)
        return {
            "status": "success" if data.get("code") == "0" else "error",
            "code": data.get("code"),
            "message": data.get("msg"),
            "count": len(data.get("data", [])),
            "sample": data.get("data", [])[0] if data.get("data") else None,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "sample": None,
        }


def test_history_candles_endpoint(instrument, interval, limit=100):
    """Test the history candles endpoint for a specific instrument and interval."""
    # Set timestamp to 30 days ago
    timestamp = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)

    params = {"instId": instrument, "bar": interval, "limit": limit, "after": timestamp}

    try:
        data = retry_request(HISTORY_CANDLES_ENDPOINT, params)
        return {
            "status": "success" if data.get("code") == "0" else "error",
            "code": data.get("code"),
            "message": data.get("msg"),
            "count": len(data.get("data", [])),
            "sample": data.get("data", [])[0] if data.get("data") else None,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "count": 0,
            "sample": None,
        }


def test_max_limit():
    """Test the maximum limit of records that can be returned."""
    limits_to_test = [100, 200, 300, 400, 500]
    results = []

    for limit in limits_to_test:
        result = test_candles_endpoint(SPOT_INSTRUMENT, "1m", limit)
        results.append(
            {
                "requested_limit": limit,
                "actual_count": result["count"],
            }
        )

    return results


def print_results_table(title, results):
    """Print results in a formatted table."""
    console = Console()
    table = Table(title=title)

    # Add columns based on the first result's keys
    if results and len(results) > 0:
        for key in results[0].keys():
            table.add_column(key)

        # Add rows
        for result in results:
            table.add_row(*[str(result.get(key, "")) for key in results[0].keys()])

    console.print(table)


def main():
    print("[bold green]OKX API Test[/bold green]")

    # Test 1: Candles endpoint for SPOT
    print("\n[bold blue]Testing Candles Endpoint for SPOT (BTC-USDT)[/bold blue]")
    spot_results = []
    for interval in INTERVALS:
        result = test_candles_endpoint(SPOT_INSTRUMENT, interval)
        spot_results.append(
            {
                "instrument": SPOT_INSTRUMENT,
                "interval": interval,
                "status": result["status"],
                "count": result["count"],
            }
        )
    print_results_table("SPOT Candles Results", spot_results)

    # Test 2: Candles endpoint for SWAP
    print("\n[bold blue]Testing Candles Endpoint for SWAP (BTC-USD-SWAP)[/bold blue]")
    swap_results = []
    for interval in INTERVALS:
        result = test_candles_endpoint(SWAP_INSTRUMENT, interval)
        swap_results.append(
            {
                "instrument": SWAP_INSTRUMENT,
                "interval": interval,
                "status": result["status"],
                "count": result["count"],
            }
        )
    print_results_table("SWAP Candles Results", swap_results)

    # Test 3: History candles endpoint
    print("\n[bold blue]Testing History Candles Endpoint[/bold blue]")
    history_results = []
    for instrument in [SPOT_INSTRUMENT, SWAP_INSTRUMENT]:
        result = test_history_candles_endpoint(instrument, "1D")
        history_results.append(
            {
                "instrument": instrument,
                "interval": "1D",
                "status": result["status"],
                "count": result["count"],
            }
        )
    print_results_table("History Candles Results", history_results)

    # Test 4: Max limit test
    print("\n[bold blue]Testing Maximum Limit[/bold blue]")
    limit_results = test_max_limit()
    print_results_table("Max Limit Test", limit_results)

    # Data format analysis for SPOT
    print("\n[bold blue]Data Format Analysis (SPOT)[/bold blue]")
    result = test_candles_endpoint(SPOT_INSTRUMENT, "1m", 1)
    if result["sample"]:
        print("[bold]Sample data structure:[/bold]")
        for i, field in enumerate(result["sample"]):
            print(f"Field {i}: {field} ({type(field).__name__})")

    print("\n[bold green]Test Complete![/bold green]")


if __name__ == "__main__":
    main()
