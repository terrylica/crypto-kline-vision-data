#!/usr/bin/env python3

import time
from datetime import datetime, timedelta

import httpx
from rich import print
from rich.console import Console
from rich.table import Table

from utils.config import MEDIUM_HISTORY_DAYS, SHORT_HISTORY_DAYS
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

# Test all intervals we want to check
ALL_INTERVALS = ["1m", "3m", "5m", "15m", "30m", "1H", "2H", "4H", "6H", "12H", "1D"]
CURRENT_DATE = datetime.now()

# Time window parameters for searching historical data
KNOWN_START_DATE = datetime(2017, 10, 1)  # Known approximate start date for OKX history


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


def has_data(endpoint, instrument, interval, timestamp):
    """Check if data exists at a specific timestamp."""
    params = {
        "instId": instrument,
        "bar": interval,
        "limit": 1,
        "after": timestamp,
    }

    result = retry_request(endpoint, params)

    if (
        result
        and "data" in result
        and result["data"].get("code") == "0"
        and len(result["data"].get("data", [])) > 0
    ):
        return True, result

    return False, result


def binary_search_earliest_data(endpoint, instrument, interval, start_date, end_date):
    """
    Use binary search to find the earliest available data point.

    Args:
        endpoint: API endpoint URL
        instrument: Trading pair symbol
        interval: Time interval for candles
        start_date: Earliest possible date to check
        end_date: Latest possible date to check

    Returns:
        The earliest timestamp where data is available
    """
    print(f"Starting binary search between {start_date} and {end_date}")

    start_ms = int(start_date.timestamp() * 1000)
    end_ms = int(end_date.timestamp() * 1000)

    earliest_with_data = None

    # First, verify the end date has data
    has_end_data, _ = has_data(endpoint, instrument, interval, end_ms)
    if not has_end_data:
        print(f"No data found at end date: {end_date}")
        return None

    # Begin binary search
    while start_ms <= end_ms:
        mid_ms = start_ms + (end_ms - start_ms) // 2
        mid_date = datetime.fromtimestamp(mid_ms / 1000)

        print(f"Checking date: {mid_date}")
        has_data_at_mid, _ = has_data(endpoint, instrument, interval, mid_ms)

        if has_data_at_mid:
            # Data exists at mid, so the earliest date is either this one or before it
            earliest_with_data = mid_date
            end_ms = mid_ms - 1  # Search earlier
        else:
            # No data at mid, the earliest date must be after this
            start_ms = mid_ms + 1  # Search later

    return earliest_with_data


def verify_exact_earliest_date(endpoint, instrument, earliest_estimate, interval):
    """Verify the exact earliest date by checking days before and after the estimate."""
    if not earliest_estimate:
        return None

    # For all intervals, check a range of days around the earliest estimate
    check_range = 7  # Check a week before and after
    results = []

    for days in range(-check_range, check_range + 1):
        check_date = earliest_estimate + timedelta(days=days)
        check_ms = int(check_date.timestamp() * 1000)

        has_data_result, response = has_data(endpoint, instrument, interval, check_ms)

        data_points = 0
        timestamp = None
        if (
            has_data_result
            and "data" in response
            and len(response["data"].get("data", [])) > 0
        ):
            timestamp = response["data"]["data"][0][0]  # First datapoint timestamp
            data_points = len(response["data"]["data"])

        results.append(
            {
                "date": check_date.strftime("%Y-%m-%d"),
                "has_data": has_data_result,
                "timestamp": timestamp,
                "data_points": data_points,
            }
        )

        print(
            f"Date {check_date.strftime('%Y-%m-%d')}: {'Has data' if has_data_result else 'No data'}"
        )

    return results


def test_historical_depth_from_now(instrument, interval):
    """
    Test how far back from current time each endpoint can go for the specified interval.
    This test starts from current time and works backward in increments to
    find the point at which data is no longer available.
    """
    print(
        f"\n[bold blue]Testing Historical Depth From Current Time for {interval}[/bold blue]"
    )

    # Define the time ranges to test (in days back from now)
    days_to_test = [1, 7, 30, 60, 90, 180, 365, 730]

    current_time = datetime.now()
    results = []

    # Test both endpoints
    for endpoint_name, endpoint_url in [
        ("candles", CANDLES_ENDPOINT),
        ("history-candles", HISTORY_ENDPOINT),
    ]:
        print(f"\n[bold cyan]Testing {endpoint_name} endpoint[/bold cyan]")
        last_day_with_data = None

        for days_back in days_to_test:
            test_date = current_time - timedelta(days=days_back)
            test_ms = int(test_date.timestamp() * 1000)

            has_data_result, response = has_data(
                endpoint_url, instrument, interval, test_ms
            )

            status = "✅ Available" if has_data_result else "❌ Not available"
            print(f"{days_back} days ago ({test_date.strftime('%Y-%m-%d')}): {status}")

            results.append(
                {
                    "endpoint": endpoint_name,
                    "interval": interval,
                    "days_ago": days_back,
                    "date": test_date.strftime("%Y-%m-%d"),
                    "has_data": has_data_result,
                    "data_points": (
                        len(response["data"].get("data", [])) if has_data_result else 0
                    ),
                }
            )

            if has_data_result:
                last_day_with_data = days_back

        if last_day_with_data:
            print(
                f"[bold green]The {endpoint_name} endpoint has {interval} data going back at least {last_day_with_data} days[/bold green]"
            )
        else:
            print(
                f"[bold red]No {interval} data found in the {endpoint_name} endpoint for any tested period[/bold red]"
            )

    return results


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


def test_candles_endpoint_recent_window(instrument, interval):
    """
    Use a smart algorithm to determine exactly when data becomes available in the candles endpoint.
    Starts from current time and narrows down the window where data transitions from available to not available.
    """
    print(
        f"\n[bold blue]Testing {interval} Data Availability Window for Candles Endpoint[/bold blue]"
    )

    current_time = datetime.now()

    # First, verify that we can get current data
    current_timestamp = int(current_time.timestamp() * 1000)

    # Adjust the check for different intervals - look back appropriate amount based on interval
    interval_minutes = {
        "1m": 1,
        "3m": 3,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1H": 60,
        "2H": 120,
        "4H": 240,
        "6H": 360,
        "12H": 720,
        "1D": 1440,
    }
    minutes_to_look_back = interval_minutes.get(interval, 1)

    has_current_data, _ = has_data(
        CANDLES_ENDPOINT,
        instrument,
        interval,
        current_timestamp - (minutes_to_look_back * 60000),
    )  # Look back based on interval

    if not has_current_data:
        print(
            f"[bold red]Cannot get current {interval} data from candles endpoint. Test cannot proceed.[/bold red]"
        )
        return None

    print(f"[green]Current {interval} data is available from candles endpoint[/green]")

    # Start with binary search to find approximate boundary
    # Initial window: now to 30 days ago (adjust for longer intervals)
    available_point = current_time

    # For longer intervals, extend initial search window
    search_days = 90 if interval in ["1D", "12H", "6H", "4H", "2H", "1H"] else 30

    unavailable_point = current_time - timedelta(days=search_days)

    # First check if data is unavailable at the search_days point
    timestamp_back = int(unavailable_point.timestamp() * 1000)
    has_data_back, _ = has_data(CANDLES_ENDPOINT, instrument, interval, timestamp_back)

    if has_data_back:
        print(
            f"[yellow]Data is available {search_days} days ago. Extending search window...[/yellow]"
        )
        # Try 3x days ago
        unavailable_point = current_time - timedelta(days=search_days * 3)
        timestamp_extended = int(unavailable_point.timestamp() * 1000)
        has_data_extended, _ = has_data(
            CANDLES_ENDPOINT, instrument, interval, timestamp_extended
        )

        if has_data_extended:
            print(
                "[yellow]Data is available at extended search window. This is unexpected. Moving to hourly search.[/yellow]"
            )
        else:
            print(
                f"[green]Found rough boundary: data available at {search_days} days ago but not at {search_days * 3} days ago[/green]"
            )
    else:
        print(
            f"[green]Found rough boundary: data available now but not {search_days} days ago[/green]"
        )

    # Binary search to narrow down the window
    # For higher intervals, we'll use a coarser precision to be more efficient
    precision_hours = 1
    if interval in ["1D", "12H", "6H", "4H"]:
        precision_hours = 12
    elif interval in ["2H", "1H"]:
        precision_hours = 4

    while (
        available_point - unavailable_point
    ).total_seconds() > precision_hours * 3600:  # Narrow down based on interval
        mid_point = available_point - (available_point - unavailable_point) / 2
        mid_timestamp = int(mid_point.timestamp() * 1000)

        has_mid_data, _ = has_data(
            CANDLES_ENDPOINT, instrument, interval, mid_timestamp
        )

        if has_mid_data:
            print(f"Data available at {mid_point}")
            available_point = mid_point
        else:
            print(f"Data NOT available at {mid_point}")
            unavailable_point = mid_point

    # Calculate how far back from current time based on the available_point
    time_difference = current_time - available_point
    days = time_difference.days
    hours, remainder = divmod(time_difference.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    earliest_available_timestamp = int(available_point.timestamp() * 1000)
    print(
        f"[bold green]The candles endpoint provides {interval} data from approximately {available_point} onwards[/bold green]"
    )
    print(
        f"[bold green]Oldest available timestamp: {earliest_available_timestamp}[/bold green]"
    )
    print(
        f"[bold green]The candles endpoint provides {interval} data going back approximately {days} days, {hours} hours, and {minutes} minutes[/bold green]"
    )

    return {
        "interval": interval,
        "oldest_timestamp": earliest_available_timestamp,
        "oldest_date": available_point,
        "days_back": days + (hours / 24) + (minutes / 1440),  # Convert to decimal days
    }


def find_earliest_data_for_interval(instrument, interval):
    """Find the earliest available data for a given interval using history-candles endpoint."""
    print(
        f"\n[bold blue]Finding Earliest Available {interval} Data in history-candles[/bold blue]"
    )

    # Do binary search to find earliest available data
    earliest_date = binary_search_earliest_data(
        HISTORY_ENDPOINT, instrument, interval, KNOWN_START_DATE, CURRENT_DATE
    )

    if earliest_date:
        print(
            f"[bold green]Earliest estimated date with {interval} data: {earliest_date}[/bold green]"
        )

        # Verify the exact date
        verification_results = verify_exact_earliest_date(
            HISTORY_ENDPOINT, instrument, earliest_date, interval
        )

        if verification_results:
            print_results_table(
                f"Date Verification Results for {interval}", verification_results
            )

            # Find the earliest date with data
            earliest_with_data = None
            for result in verification_results:
                if result["has_data"] and (
                    earliest_with_data is None
                    or datetime.strptime(result["date"], "%Y-%m-%d")
                    < datetime.strptime(earliest_with_data["date"], "%Y-%m-%d")
                ):
                    earliest_with_data = result

            if earliest_with_data:
                print(
                    f"[bold green]Earliest verified date with {interval} data: {earliest_with_data['date']}[/bold green]"
                )
                return earliest_with_data
    else:
        print(f"[bold red]Failed to find earliest date with {interval} data[/bold red]")

    return None


def main():
    """Test the historical depth of OKX's candles endpoints for multiple intervals."""
    print(
        "[bold green]OKX Candles Historical Depth Test for Multiple Intervals[/bold green]"
    )

    # We'll store results for all intervals to update documentation
    all_results = {"candles_availability": {}, "history_candles_earliest_dates": {}}

    # First test - For each interval, check how far back candles endpoint goes
    for interval in ALL_INTERVALS:
        print(f"\n[bold yellow]===== Testing {interval} interval =====[/bold yellow]")

        # Test window of availability for candles endpoint
        candles_window = test_candles_endpoint_recent_window(SPOT_INSTRUMENT, interval)
        if candles_window:
            all_results["candles_availability"][interval] = candles_window

        # Test historical depth from now for this interval
        historical_depth_results = test_historical_depth_from_now(
            SPOT_INSTRUMENT, interval
        )
        if historical_depth_results:
            print_results_table(
                f"Historical Depth From Current Time Results ({interval})",
                historical_depth_results,
            )

        # Find earliest data in history-candles for this interval
        earliest_data = find_earliest_data_for_interval(SPOT_INSTRUMENT, interval)
        if earliest_data:
            all_results["history_candles_earliest_dates"][interval] = earliest_data

    # Summary of results for all intervals
    print("\n[bold blue]=== SUMMARY OF RESULTS ===[/bold blue]")

    # Candles endpoint availability
    print("\n[bold cyan]Candles Endpoint Availability Summary[/bold cyan]")
    for interval, data in all_results["candles_availability"].items():
        if isinstance(data, dict):
            days_back = data.get("days_back", 0)
            availability = "✅ Available"
            if days_back < SHORT_HISTORY_DAYS:
                availability += f" (last ~{days_back:.1f} days only)"
            elif days_back < MEDIUM_HISTORY_DAYS:
                availability += f" (from ~{days_back:.1f} days ago)"
            else:
                availability += f" (historic data from ~{days_back:.1f} days ago)"
            print(f"- {interval}: {availability}")
        else:
            print(f"- {interval}: ❌ Not available historically")

    # History-candles endpoint availability
    print("\n[bold cyan]History-Candles Endpoint Earliest Date Summary[/bold cyan]")
    for interval, data in all_results["history_candles_earliest_dates"].items():
        if isinstance(data, dict) and data.get("has_data"):
            earliest_date = data.get("date")
            print(f"- {interval}: ✅ Available (from {earliest_date})")
        else:
            print(f"- {interval}: ❌ Not available historically")

    print("\n[bold green]OKX Historical Depth Test Complete![/bold green]")


if __name__ == "__main__":
    main()
