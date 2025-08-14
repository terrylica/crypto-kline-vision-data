#!/usr/bin/env python3

import json
import time
from datetime import datetime, timedelta

import httpx
from rich import print
from rich.console import Console
from rich.table import Table

from data_source_manager.utils.loguru_setup import logger

# API constants
OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"

# Test parameters
SPOT_INSTRUMENT = "BTC-USDT"
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


def test_interval_case_sensitivity():
    """
    Test if interval parameters are case-sensitive for both endpoints.
    OKX documentation suggests capital H/D/W/M for larger intervals.
    """
    # Test pairs with mixed case
    test_intervals = [
        # Standard format, Mixed case, Official format
        ("1m", "1M", "1m"),  # Should be lowercase for minute
        ("1h", "1H", "1H"),  # Should be uppercase for hour
        ("4h", "4H", "4H"),  # Should be uppercase for hour
        ("1d", "1D", "1D"),  # Should be uppercase for day
        ("1w", "1W", "1W"),  # Should be uppercase for week
        ("1M", "1m", "1M"),  # Should be uppercase for month
    ]

    results = []
    for original, mixed_case, official in test_intervals:
        # Test with candles endpoint
        candles_original = retry_request(
            CANDLES_ENDPOINT, {"instId": SPOT_INSTRUMENT, "bar": original, "limit": 1}
        )
        candles_mixed = retry_request(
            CANDLES_ENDPOINT, {"instId": SPOT_INSTRUMENT, "bar": mixed_case, "limit": 1}
        )
        candles_official = retry_request(
            CANDLES_ENDPOINT, {"instId": SPOT_INSTRUMENT, "bar": official, "limit": 1}
        )

        # Test with history-candles endpoint
        timestamp = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
        history_original = retry_request(
            HISTORY_CANDLES_ENDPOINT,
            {
                "instId": SPOT_INSTRUMENT,
                "bar": original,
                "limit": 1,
                "after": timestamp,
            },
        )
        history_mixed = retry_request(
            HISTORY_CANDLES_ENDPOINT,
            {
                "instId": SPOT_INSTRUMENT,
                "bar": mixed_case,
                "limit": 1,
                "after": timestamp,
            },
        )
        history_official = retry_request(
            HISTORY_CANDLES_ENDPOINT,
            {
                "instId": SPOT_INSTRUMENT,
                "bar": official,
                "limit": 1,
                "after": timestamp,
            },
        )

        # Process results
        results.append(
            {
                "interval": original,
                "mixed_case": mixed_case,
                "official": official,
                "candles_original_status": (
                    "success"
                    if "data" in candles_original
                    and candles_original["data"].get("code") == "0"
                    else "error"
                ),
                "candles_mixed_status": (
                    "success"
                    if "data" in candles_mixed
                    and candles_mixed["data"].get("code") == "0"
                    else "error"
                ),
                "candles_official_status": (
                    "success"
                    if "data" in candles_official
                    and candles_official["data"].get("code") == "0"
                    else "error"
                ),
                "history_original_status": (
                    "success"
                    if "data" in history_original
                    and history_original["data"].get("code") == "0"
                    else "error"
                ),
                "history_mixed_status": (
                    "success"
                    if "data" in history_mixed
                    and history_mixed["data"].get("code") == "0"
                    else "error"
                ),
                "history_official_status": (
                    "success"
                    if "data" in history_official
                    and history_official["data"].get("code") == "0"
                    else "error"
                ),
            }
        )

    return results


def test_one_second_interval():
    """
    Test the alleged 1-second interval support in the history-candles endpoint.
    Our previous tests showed the history-candles endpoint might support 1s interval.
    """
    # Test 1s interval with both endpoints
    candles_result = retry_request(
        CANDLES_ENDPOINT, {"instId": SPOT_INSTRUMENT, "bar": "1s", "limit": 10}
    )

    # Test with different timestamps to ensure it's not just a fluke
    timestamp_yesterday = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
    timestamp_week_ago = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)
    timestamp_month_ago = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)

    history_results = []
    for timestamp, label in [
        (timestamp_yesterday, "1 day ago"),
        (timestamp_week_ago, "1 week ago"),
        (timestamp_month_ago, "1 month ago"),
    ]:
        history_result = retry_request(
            HISTORY_CANDLES_ENDPOINT,
            {"instId": SPOT_INSTRUMENT, "bar": "1s", "limit": 10, "after": timestamp},
        )

        # Check if we got valid data
        has_data = False
        if (
            "data" in history_result
            and history_result["data"].get("code") == "0"
            and len(history_result["data"].get("data", [])) > 0
        ):
            has_data = True

        history_results.append(
            {
                "timestamp_period": label,
                "timestamp_ms": timestamp,
                "success": "data" in history_result
                and history_result["data"].get("code") == "0",
                "error_message": (
                    history_result["data"].get("msg", "")
                    if "data" in history_result
                    else str(history_result.get("error", ""))
                ),
                "has_data": has_data,
                "record_count": (
                    len(history_result["data"].get("data", []))
                    if "data" in history_result
                    else 0
                ),
            }
        )

    return {
        "candles_1s": {
            "success": "data" in candles_result
            and candles_result["data"].get("code") == "0",
            "error_message": (
                candles_result["data"].get("msg", "")
                if "data" in candles_result
                else str(candles_result.get("error", ""))
            ),
            "record_count": (
                len(candles_result["data"].get("data", []))
                if "data" in candles_result
                else 0
            ),
        },
        "history_candles_1s": history_results,
    }


def print_results_table(title, data):
    """Print results in a formatted table."""
    console = Console()
    print(f"\n[bold cyan]{title}[/bold cyan]")

    if isinstance(data, list):
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
                elif key.endswith("_status"):
                    # Highlight success/error
                    value = (
                        f"[green]{value}[/green]"
                        if value == "success"
                        else f"[red]{value}[/red]"
                    )
                row_values.append(str(value))
            table.add_row(*row_values)

        console.print(table)
    elif isinstance(data, dict):
        # For dictionaries with "history_candles_1s"
        if "history_candles_1s" in data and isinstance(
            data["history_candles_1s"], list
        ):
            # Print candles_1s first
            candles_info = data["candles_1s"]
            print("\n[bold yellow]candles_1s[/bold yellow]")
            table = Table(show_header=True, header_style="bold magenta")
            for key in candles_info.keys():
                table.add_column(key)

            row_values = []
            for key in candles_info.keys():
                value = candles_info.get(key, "")
                row_values.append(str(value))
            table.add_row(*row_values)
            console.print(table)

            # Print history_candles_1s
            print("\n[bold yellow]history_candles_1s[/bold yellow]")
            history_results = data["history_candles_1s"]
            if history_results:
                table = Table(show_header=True, header_style="bold magenta")
                for key in history_results[0].keys():
                    table.add_column(key)

                for item in history_results:
                    row_values = []
                    for key in history_results[0].keys():
                        value = item.get(key, "")
                        if key == "success":
                            value = (
                                f"[green]{value}[/green]"
                                if value
                                else f"[red]{value}[/red]"
                            )
                        elif key == "has_data":
                            value = (
                                f"[green]{value}[/green]"
                                if value
                                else f"[red]{value}[/red]"
                            )
                        row_values.append(str(value))
                    table.add_row(*row_values)
                console.print(table)
        else:
            # Regular dictionary
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Key", style="dim")
            table.add_column("Value")

            for key, value in data.items():
                table.add_row(key, str(value))

            console.print(table)


def main():
    """Run interval validation tests for OKX API endpoints."""
    print("[bold green]OKX Interval Validation Tests[/bold green]")

    # Test 1: Case sensitivity
    print("\n[bold blue]1. Testing Interval Case Sensitivity[/bold blue]")
    case_results = test_interval_case_sensitivity()
    print_results_table("Interval Case Sensitivity Results", case_results)

    # Test 2: 1-second interval support
    print("\n[bold blue]2. Testing 1-Second Interval Support[/bold blue]")
    second_results = test_one_second_interval()
    print_results_table("1-Second Interval Results", second_results)

    print("\n[bold green]Interval Validation Tests Complete![/bold green]")


if __name__ == "__main__":
    main()
