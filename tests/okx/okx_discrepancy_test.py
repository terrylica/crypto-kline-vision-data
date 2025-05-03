#!/usr/bin/env python3

import time
from datetime import datetime, timedelta

import httpx
from rich import print
from rich.console import Console
from rich.table import Table

from utils.config import SECONDS_IN_HOUR
from utils.logger_setup import logger

# Set logger level to WARNING to reduce verbosity
logger.setLevel("WARNING")

# Constants
OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"
SPOT_INSTRUMENT = "BTC-USDT"
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

MS_IN_HOUR = SECONDS_IN_HOUR * 1000  # Milliseconds in an hour
MS_IN_MINUTE = 60000  # Milliseconds in a minute


def retry_request(url, params=None, max_retries=MAX_RETRIES):
    """Make HTTP request with retry logic."""
    for attempt in range(max_retries):
        try:
            response = httpx.get(url, params=params, timeout=10.0)
            response.raise_for_status()
            return {
                "status_code": response.status_code,
                "data": response.json(),
                "raw_response": response,
            }
        except Exception as e:
            logger.error(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.critical(f"All {max_retries} attempts failed for URL: {url}")
                return {
                    "status_code": -1,
                    "error": str(e),
                }
    return None


def print_results_table(title, data):
    """Print results in a formatted table."""
    console = Console()
    print(f"\n[bold cyan]{title}[/bold cyan]")

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

            # Format boolean values with color
            if isinstance(value, bool):
                value = f"[green]{value}[/green]" if value else f"[red]{value}[/red]"

            row_values.append(str(value))

        table.add_row(*row_values)

    console.print(table)


def test_bar_parameter_requirement():
    """
    Test if the 'bar' parameter is truly optional and defaults to '1m'.
    This tests whether 'bar' parameter is required as stated in docs or if it's
    actually optional as the OKX Data Guide suggests.
    """
    print("\n[bold blue]Testing 'bar' Parameter Requirement[/bold blue]")

    # Test cases
    test_cases = [
        {
            "case": "With bar parameter",
            "params": {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": 5},
        },
        {
            "case": "Without bar parameter",
            "params": {"instId": SPOT_INSTRUMENT, "limit": 5},
        },
    ]

    results = []

    # Test both endpoints
    for endpoint_name, endpoint_url in [
        ("candles", CANDLES_ENDPOINT),
        ("history-candles", HISTORY_ENDPOINT),
    ]:
        for test_case in test_cases:
            # For history endpoint, add timestamp
            params = test_case["params"].copy()
            if endpoint_name == "history-candles":
                # Use a timestamp 7 days ago
                params["after"] = int(
                    (datetime.now() - timedelta(days=7)).timestamp() * 1000
                )

            # Make the request
            response = retry_request(endpoint_url, params)

            # Process response
            success = False
            error_message = ""
            data_count = 0
            interval = ""

            if response and "data" in response:
                if response["data"].get("code") == "0":
                    success = True
                    data_count = len(response["data"].get("data", []))

                    # Check first data point to infer interval if data exists
                    if data_count > 0 and len(response["data"]["data"][0]) > 0:
                        # Take first two data points to infer interval if available
                        if data_count > 1:
                            timestamp1 = int(response["data"]["data"][0][0])
                            timestamp2 = int(response["data"]["data"][1][0])
                            interval_ms = abs(timestamp2 - timestamp1)
                            interval = (
                                f"{interval_ms / MS_IN_MINUTE:.1f}m"
                                if interval_ms < MS_IN_HOUR
                                else f"{interval_ms / MS_IN_HOUR:.1f}H"
                            )
                        else:
                            interval = "Cannot infer (need 2+ points)"
                else:
                    error_message = response["data"].get("msg", "")
            else:
                error_message = str(response.get("error", "Unknown error"))

            results.append(
                {
                    "endpoint": endpoint_name,
                    "test_case": test_case["case"],
                    "success": success,
                    "error_message": error_message,
                    "data_points": data_count,
                    "inferred_interval": interval,
                }
            )

            status = (
                "[green]✅ Success[/green]"
                if success
                else f"[red]❌ Failed: {error_message}[/red]"
            )
            print(
                f"{endpoint_name} - {test_case['case']}: {status} (Records: {data_count})"
            )

    return results


def test_one_second_interval_availability():
    """
    Test the actual availability window of 1-second interval data.
    The guide mentions "~20 days" but testing showed "up to approximately one month".
    """
    print("\n[bold blue]Testing 1-Second Interval Data Availability[/bold blue]")

    # Test data availability at different historical points
    current_time = datetime.now()
    test_periods = [
        {"name": "Today", "days_back": 0},
        {"name": "Yesterday", "days_back": 1},
        {"name": "1 week ago", "days_back": 7},
        {"name": "2 weeks ago", "days_back": 14},
        {"name": "3 weeks ago", "days_back": 21},
        {"name": "4 weeks ago", "days_back": 28},
        {"name": "5 weeks ago", "days_back": 35},
        {"name": "6 weeks ago", "days_back": 42},
    ]

    results = []

    # Test data availability on both endpoints
    for endpoint_name, endpoint_url in [
        ("candles", CANDLES_ENDPOINT),
        ("history-candles", HISTORY_ENDPOINT),
    ]:
        for period in test_periods:
            test_time = current_time - timedelta(days=period["days_back"])
            test_timestamp = int(test_time.timestamp() * 1000)

            params = {"instId": SPOT_INSTRUMENT, "bar": "1s", "limit": 5}
            if endpoint_name == "history-candles":
                params["after"] = test_timestamp

            # Make the request
            response = retry_request(endpoint_url, params)

            # Process response
            success = False
            data_count = 0
            error_message = ""

            if response and "data" in response:
                if response["data"].get("code") == "0":
                    data_count = len(response["data"].get("data", []))
                    success = data_count > 0
                else:
                    error_message = response["data"].get("msg", "")
            else:
                error_message = str(response.get("error", "Unknown error"))

            results.append(
                {
                    "endpoint": endpoint_name,
                    "period": period["name"],
                    "days_back": period["days_back"],
                    "timestamp": test_timestamp,
                    "success": success,
                    "error_message": error_message,
                    "data_count": data_count,
                }
            )

            status = (
                f"[green]✅ Available ({data_count} records)[/green]"
                if success
                else f"[red]❌ Not available: {error_message}[/red]"
            )
            print(
                f"{endpoint_name} - {period['name']} ({test_time.strftime('%Y-%m-%d')}): {status}"
            )

    return results


def test_historical_data_availability():
    """
    Test the actual availability of 1D (daily) historical data across both endpoints.
    This verifies the earliest dates claimed in the documentation.
    """
    print("\n[bold blue]Testing Historical 1D Data Availability[/bold blue]")

    # Key dates to test from the documentation
    test_dates = [
        {"name": "Candles earliest (May 21, 2021)", "date": datetime(2021, 5, 21)},
        {"name": "Candles earliest - 1 day", "date": datetime(2021, 5, 20)},
        {"name": "History earliest (Oct 10, 2017)", "date": datetime(2017, 10, 10)},
        {"name": "History earliest - 1 day", "date": datetime(2017, 10, 9)},
    ]

    results = []

    # Test both endpoints
    for endpoint_name, endpoint_url in [
        ("candles", CANDLES_ENDPOINT),
        ("history-candles", HISTORY_ENDPOINT),
    ]:
        for date_info in test_dates:
            test_timestamp = int(date_info["date"].timestamp() * 1000)

            params = {"instId": SPOT_INSTRUMENT, "bar": "1D", "limit": 5}
            if endpoint_name == "history-candles":
                params["after"] = test_timestamp

            # Make the request
            response = retry_request(endpoint_url, params)

            # Process response
            success = False
            data_count = 0
            error_message = ""
            earliest_date = None

            if response and "data" in response:
                if response["data"].get("code") == "0":
                    data = response["data"].get("data", [])
                    data_count = len(data)
                    success = data_count > 0

                    if success and data_count > 0:
                        # Get earliest timestamp in response
                        earliest_timestamp = min([int(record[0]) for record in data])
                        earliest_date = datetime.fromtimestamp(
                            earliest_timestamp / 1000
                        ).strftime("%Y-%m-%d")
                else:
                    error_message = response["data"].get("msg", "")
            else:
                error_message = str(response.get("error", "Unknown error"))

            results.append(
                {
                    "endpoint": endpoint_name,
                    "test_date": date_info["name"],
                    "date": date_info["date"].strftime("%Y-%m-%d"),
                    "timestamp": test_timestamp,
                    "success": success,
                    "data_count": data_count,
                    "earliest_date_in_response": earliest_date,
                    "error_message": error_message,
                }
            )

            status = (
                f"[green]✅ Available ({data_count} records)[/green]"
                if success
                else f"[red]❌ Not available: {error_message}[/red]"
            )
            date_info_str = (
                f"Earliest date in response: {earliest_date}" if earliest_date else ""
            )
            print(
                f"{endpoint_name} - {date_info['name']} ({date_info['date'].strftime('%Y-%m-%d')}): {status} {date_info_str}"
            )

    return results


def test_endpoint_record_limits():
    """
    Test the actual record limits per request for both endpoints.
    The documentation states limits of 300 for candles and 100 for history-candles.
    """
    print("\n[bold blue]Testing Endpoint Record Limits[/bold blue]")

    # Test various limit values
    test_limits = [1, 50, 99, 100, 101, 200, 299, 300, 301, 500]

    results = []

    # Test both endpoints
    for endpoint_name, endpoint_url in [
        ("candles", CANDLES_ENDPOINT),
        ("history-candles", HISTORY_ENDPOINT),
    ]:
        for limit in test_limits:
            params = {"instId": SPOT_INSTRUMENT, "bar": "1m", "limit": limit}
            if endpoint_name == "history-candles":
                # Use a timestamp 7 days ago to ensure data availability
                params["after"] = int(
                    (datetime.now() - timedelta(days=7)).timestamp() * 1000
                )

            # Make the request
            response = retry_request(endpoint_url, params)

            # Process response
            success = False
            records_returned = 0
            error_message = ""

            if response and "data" in response:
                if response["data"].get("code") == "0":
                    success = True
                    records_returned = len(response["data"].get("data", []))
                else:
                    error_message = response["data"].get("msg", "")
            else:
                error_message = str(response.get("error", "Unknown error"))

            results.append(
                {
                    "endpoint": endpoint_name,
                    "requested_limit": limit,
                    "success": success,
                    "records_returned": records_returned,
                    "error_message": error_message,
                }
            )

            status = (
                "[green]✅ Success[/green]"
                if success
                else f"[red]❌ Failed: {error_message}[/red]"
            )
            print(
                f"{endpoint_name} - Requested limit {limit}: {status} (Returned: {records_returned})"
            )

    return results


def main():
    console = Console()

    # Test 1: Bar parameter requirement
    console.print("\n[bold]TEST 1: Bar Parameter Requirement[/bold]")
    console.print(
        "Testing if 'bar' parameter is truly optional as claimed in the guide..."
    )
    bar_results = test_bar_parameter_requirement()
    print_results_table("Bar Parameter Requirement Results", bar_results)

    # Test 2: 1-second interval availability
    console.print("\n[bold]TEST 2: 1-Second Interval Availability[/bold]")
    console.print("Testing how far back 1-second data is actually available...")
    second_results = test_one_second_interval_availability()
    print_results_table("1-Second Interval Availability Results", second_results)

    # Test 3: Historical data availability
    console.print("\n[bold]TEST 3: Historical 1D Data Availability[/bold]")
    console.print("Testing the earliest available data for 1D interval...")
    history_results = test_historical_data_availability()
    print_results_table("Historical 1D Data Availability Results", history_results)

    # Test 4: Endpoint record limits
    console.print("\n[bold]TEST 4: Endpoint Record Limits[/bold]")
    console.print("Testing maximum records per request for both endpoints...")
    limit_results = test_endpoint_record_limits()
    print_results_table("Endpoint Record Limits Results", limit_results)

    # Summary and conclusion
    console.print("\n[bold green]=========== TEST SUMMARY ===========[/bold green]")

    # Bar parameter
    bar_optional = any(
        r["success"] and r["test_case"] == "Without bar parameter" for r in bar_results
    )
    console.print(
        f"1. Bar parameter optional: [{'green' if bar_optional else 'red'}]{bar_optional}[/{'green' if bar_optional else 'red'}]"
    )

    # 1-second availability
    max_days_back = 0
    for r in second_results:
        if r["endpoint"] == "history-candles" and r["success"]:
            max_days_back = max(max_days_back, r["days_back"])
    console.print(
        f"2. Maximum days back for 1-second data: [green]{max_days_back} days[/green]"
    )

    # Historical 1D data
    candles_earliest = "Not found"
    history_earliest = "Not found"
    for r in history_results:
        if r["success"]:
            if r["endpoint"] == "candles" and "May 21, 2021" in r["test_date"]:
                candles_earliest = r["date"]
            elif (
                r["endpoint"] == "history-candles" and "Oct 10, 2017" in r["test_date"]
            ):
                history_earliest = r["date"]

    console.print(
        f"3. Earliest 1D data - candles endpoint: [green]{candles_earliest}[/green]"
    )
    console.print(
        f"   Earliest 1D data - history endpoint: [green]{history_earliest}[/green]"
    )

    # Record limits
    candles_max = 0
    history_max = 0
    for r in limit_results:
        if r["success"]:
            if r["endpoint"] == "candles":
                candles_max = max(candles_max, r["records_returned"])
            elif r["endpoint"] == "history-candles":
                history_max = max(history_max, r["records_returned"])

    console.print(
        f"4. Maximum records - candles endpoint: [green]{candles_max}[/green]"
    )
    console.print(
        f"   Maximum records - history endpoint: [green]{history_max}[/green]"
    )


if __name__ == "__main__":
    main()
