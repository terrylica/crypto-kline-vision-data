#!/usr/bin/env python3

import time
from datetime import datetime, timedelta

import httpx
from rich import print
from rich.console import Console
from rich.table import Table

from data_source_manager.utils.loguru_setup import logger

# Set logger level to WARNING to reduce verbosity
logger.setLevel("WARNING")

# Constants
OKX_API_BASE_URL = "https://www.okx.com/api/v5"
CANDLES_ENDPOINT = f"{OKX_API_BASE_URL}/market/candles"
HISTORY_ENDPOINT = f"{OKX_API_BASE_URL}/market/history-candles"
SPOT_INSTRUMENT = "BTC-USDT"
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# Interval we're testing
TEST_INTERVAL = "1s"  # Testing 1-second interval
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


def test_recent_availability(instrument, interval):
    """
    Test if very recent data is available for the 1s interval.
    This checks the last hour, last 10 minutes, and last minute.
    """
    print(f"\n[bold blue]Testing Recent {interval} Data Availability[/bold blue]")

    current_time = datetime.now()
    test_windows = [
        ("Last minute", 1),
        ("Last 10 minutes", 10),
        ("Last hour", 60),
        ("Last 6 hours", 6 * 60),
        ("Last 24 hours", 24 * 60),
    ]

    results = []

    for window_name, minutes in test_windows:
        test_time = current_time - timedelta(minutes=minutes)
        test_timestamp = int(test_time.timestamp() * 1000)

        # Test both endpoints
        for endpoint_name, endpoint_url in [
            ("candles", CANDLES_ENDPOINT),
            ("history-candles", HISTORY_ENDPOINT),
        ]:
            has_data_result, response = has_data(
                endpoint_url, instrument, interval, test_timestamp
            )

            data_count = 0
            if has_data_result and "data" in response:
                data_count = len(response["data"].get("data", []))

            results.append(
                {
                    "window": window_name,
                    "endpoint": endpoint_name,
                    "timestamp": test_timestamp,
                    "datetime": test_time,
                    "has_data": has_data_result,
                    "data_points": data_count,
                }
            )

            status = "✅ Available" if has_data_result else "❌ Not available"
            print(f"{window_name} ({test_time}) - {endpoint_name}: {status}")

            # If data is available, also check the data structure
            if (
                has_data_result
                and "data" in response
                and len(response["data"].get("data", [])) > 0
            ):
                data_item = response["data"]["data"][0]
                print(f"  Sample data: {data_item}")

    # Use assertion to make sure we got at least some results for pytest
    assert len(results) > 0, "No results collected during test"
    # Return results for direct script execution
    return results


def test_historical_timepoints(instrument, interval):
    """
    Test availability of 1s data at specific historical timepoints
    Covers key dates to check if 1s data was ever available
    """
    print(
        f"\n[bold blue]Testing Historical {interval} Data at Key Timepoints[/bold blue]"
    )

    # Test a range of historical points
    test_dates = [
        ("Today", datetime.now()),
        ("Yesterday", datetime.now() - timedelta(days=1)),
        ("Last week", datetime.now() - timedelta(days=7)),
        ("Last month", datetime.now() - timedelta(days=30)),
        ("Six months ago", datetime.now() - timedelta(days=180)),
        ("One year ago", datetime.now() - timedelta(days=365)),
        ("Two years ago", datetime.now() - timedelta(days=730)),
        ("Three years ago", datetime.now() - timedelta(days=1095)),
        ("Four years ago", datetime.now() - timedelta(days=1460)),
    ]

    results = []

    for name, test_date in test_dates:
        test_timestamp = int(test_date.timestamp() * 1000)

        # Test both endpoints
        for endpoint_name, endpoint_url in [
            ("candles", CANDLES_ENDPOINT),
            ("history-candles", HISTORY_ENDPOINT),
        ]:
            has_data_result, response = has_data(
                endpoint_url, instrument, interval, test_timestamp
            )

            results.append(
                {
                    "timepoint": name,
                    "endpoint": endpoint_name,
                    "date": test_date.strftime("%Y-%m-%d"),
                    "has_data": has_data_result,
                    "data_points": (
                        len(response["data"].get("data", [])) if has_data_result else 0
                    ),
                }
            )

            status = "✅ Available" if has_data_result else "❌ Not available"
            print(
                f"{name} ({test_date.strftime('%Y-%m-%d')}) - {endpoint_name}: {status}"
            )

    # Use assertion to make sure we got at least some results for pytest
    assert len(results) > 0, "No results collected during test"
    # Return results for direct script execution
    return results


def test_hourly_availability_today(instrument, interval):
    """
    Test 1s data availability for multiple hours throughout the current day
    to check for any time-based patterns in data availability.
    """
    print(f"\n[bold blue]Testing Hourly {interval} Data Availability Today[/bold blue]")

    current_time = datetime.now()
    start_of_day = datetime(current_time.year, current_time.month, current_time.day)

    results = []

    # Check every 3 hours throughout today
    for hour in range(0, 24, 3):
        # Skip future hours
        test_time = start_of_day + timedelta(hours=hour)
        if test_time > current_time:
            continue

        test_timestamp = int(test_time.timestamp() * 1000)

        # Test both endpoints
        for endpoint_name, endpoint_url in [
            ("candles", CANDLES_ENDPOINT),
            ("history-candles", HISTORY_ENDPOINT),
        ]:
            has_data_result, response = has_data(
                endpoint_url, instrument, interval, test_timestamp
            )

            results.append(
                {
                    "hour": f"{hour:02d}:00",
                    "endpoint": endpoint_name,
                    "timestamp": test_timestamp,
                    "datetime": test_time,
                    "has_data": has_data_result,
                    "data_points": (
                        len(response["data"].get("data", [])) if has_data_result else 0
                    ),
                }
            )

            status = "✅ Available" if has_data_result else "❌ Not available"
            print(f"Hour {hour:02d}:00 ({test_time}) - {endpoint_name}: {status}")

    # Use assertion to make sure we got at least some results for pytest
    assert len(results) > 0, "No results collected during test"
    # Return results for direct script execution
    return results


def test_rapid_consecutive_calls(instrument, interval, num_calls=5):
    """
    Test rapid consecutive calls to check for any rate limiting or
    inconsistencies in data availability for 1s data.
    """
    print(
        f"\n[bold blue]Testing Rapid Consecutive Calls for {interval} Data[/bold blue]"
    )

    results = []
    current_time = datetime.now()
    test_timestamp = int(current_time.timestamp() * 1000)

    # Test both endpoints
    for endpoint_name, endpoint_url in [
        ("candles", CANDLES_ENDPOINT),
        ("history-candles", HISTORY_ENDPOINT),
    ]:
        print(f"Testing {endpoint_name} endpoint with {num_calls} consecutive calls...")

        for i in range(1, num_calls + 1):
            start_time = time.time()
            has_data_result, response = has_data(
                endpoint_url, instrument, interval, test_timestamp
            )
            end_time = time.time()
            response_time = end_time - start_time

            data_points = (
                len(response["data"].get("data", [])) if has_data_result else 0
            )

            results.append(
                {
                    "call_number": i,
                    "endpoint": endpoint_name,
                    "has_data": has_data_result,
                    "response_time": response_time,
                    "data_points": data_points,
                }
            )

            status = "✅ Success" if has_data_result else "❌ Failed"
            print(
                f"Call {i}: {status} - Response time: {response_time:.2f}s - Data points: {data_points}"
            )

    # Use assertion to make sure we got at least some results for pytest
    assert len(results) > 0, "No results collected during test"
    # Return results for direct script execution
    return results


def find_history_cutoff_date(instrument, interval):
    """
    Use binary search to find the exact cutoff date where historical 1s data
    stops being available on the history-candles endpoint.
    """
    print(
        f"\n[bold blue]Finding Exact Cutoff Date for {interval} Data Availability[/bold blue]"
    )

    # Initial test showed data is available for ~30 days but not for 6 months
    # So we'll search between 20-60 days ago to find the exact boundary
    current_time = datetime.now()
    earliest_with_data = current_time - timedelta(days=20)  # We know data exists here
    latest_without_data = current_time - timedelta(
        days=60
    )  # We know data doesn't exist here

    # First confirm our assumptions
    test_timestamp_recent = int(earliest_with_data.timestamp() * 1000)
    has_recent_data, _ = has_data(
        HISTORY_ENDPOINT, instrument, interval, test_timestamp_recent
    )

    test_timestamp_old = int(latest_without_data.timestamp() * 1000)
    has_old_data, _ = has_data(
        HISTORY_ENDPOINT, instrument, interval, test_timestamp_old
    )

    if has_recent_data and not has_old_data:
        print(
            "[green]Initial assumptions confirmed: data available at 20 days ago, not available at 60 days ago[/green]"
        )
    else:
        print(
            "[yellow]Initial assumptions incorrect. Adjusting search window...[/yellow]"
        )

        if not has_recent_data:
            # If no data at 20 days, try closer to now
            for days in [10, 5, 2, 1]:
                test_date = current_time - timedelta(days=days)
                test_timestamp = int(test_date.timestamp() * 1000)
                has_data_result, _ = has_data(
                    HISTORY_ENDPOINT, instrument, interval, test_timestamp
                )

                if has_data_result:
                    earliest_with_data = test_date
                    print(f"[green]Found data at {days} days ago[/green]")
                    break

        if has_old_data:
            # If data at 60 days, try further back
            for days in [90, 120, 180, 365]:
                test_date = current_time - timedelta(days=days)
                test_timestamp = int(test_date.timestamp() * 1000)
                has_data_result, _ = has_data(
                    HISTORY_ENDPOINT, instrument, interval, test_timestamp
                )

                if not has_data_result:
                    latest_without_data = test_date
                    print(f"[green]Found no data at {days} days ago[/green]")
                    break

    # Begin binary search to find exact cutoff
    print(
        f"Beginning binary search between {earliest_with_data.date()} and {latest_without_data.date()}"
    )

    cutoff_date = None
    while (latest_without_data - earliest_with_data).days > 1:
        mid_point = earliest_with_data + (latest_without_data - earliest_with_data) / 2
        mid_timestamp = int(mid_point.timestamp() * 1000)

        print(f"Testing cutoff at {mid_point.date()}")
        has_mid_data, _ = has_data(
            HISTORY_ENDPOINT, instrument, interval, mid_timestamp
        )

        if has_mid_data:
            earliest_with_data = mid_point
            print(f"  Data available at {mid_point.date()}")
        else:
            latest_without_data = mid_point
            print(f"  No data available at {mid_point.date()}")

    # The cutoff is the point where we go from having data to not having data
    cutoff_date = earliest_with_data
    days_of_data = (current_time - cutoff_date).days

    print(f"[bold green]Found cutoff date: {cutoff_date.date()}[/bold green]")
    print(
        f"[bold green]1s data is available for approximately {days_of_data} days[/bold green]"
    )

    # Verify a few days around the cutoff
    verification_results = []
    for offset in range(-2, 3):
        verify_date = cutoff_date + timedelta(days=offset)
        verify_timestamp = int(verify_date.timestamp() * 1000)
        has_verify_data, _ = has_data(
            HISTORY_ENDPOINT, instrument, interval, verify_timestamp
        )

        verification_results.append(
            {
                "date": verify_date.date(),
                "offset_from_cutoff": offset,
                "has_data": has_verify_data,
            }
        )

        status = "✅ Available" if has_verify_data else "❌ Not available"
        print(
            f"Verification {offset} days from cutoff ({verify_date.date()}): {status}"
        )

    return {
        "cutoff_date": cutoff_date.date(),
        "days_of_data": days_of_data,
        "verification": verification_results,
    }


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


def main():
    """Test the availability of 1-second interval data on OKX candles endpoints."""
    print("[bold green]OKX 1-Second Interval Data Availability Test[/bold green]")
    print(f"Running tests for {SPOT_INSTRUMENT} with {TEST_INTERVAL} interval")

    all_results = {}

    # Test 1: Recent 1s data availability
    recent_results = test_recent_availability(SPOT_INSTRUMENT, TEST_INTERVAL)
    all_results["recent_availability"] = recent_results
    print_results_table("Recent 1s Data Availability Results", recent_results)

    # Test 2: Historical 1s data at key points in time
    historical_results = test_historical_timepoints(SPOT_INSTRUMENT, TEST_INTERVAL)
    all_results["historical_timepoints"] = historical_results
    print_results_table("Historical 1s Data Availability Results", historical_results)

    # Test 3: Hourly availability today
    hourly_results = test_hourly_availability_today(SPOT_INSTRUMENT, TEST_INTERVAL)
    all_results["hourly_availability"] = hourly_results
    print_results_table("Hourly 1s Data Availability Today", hourly_results)

    # Test 4: Rapid consecutive calls to check consistency
    consistency_results = test_rapid_consecutive_calls(SPOT_INSTRUMENT, TEST_INTERVAL)
    all_results["rapid_consecutive_calls"] = consistency_results
    print_results_table("Rapid Consecutive Calls Results", consistency_results)

    # Test 5: Find exact cutoff date for history-candles endpoint
    # Only run this if we've confirmed data is available in recent history
    recent_history_available = any(
        r["has_data"] for r in recent_results if r["endpoint"] == "history-candles"
    )
    if recent_history_available:
        cutoff_results = find_history_cutoff_date(SPOT_INSTRUMENT, TEST_INTERVAL)
        all_results["cutoff_date"] = cutoff_results
        if isinstance(cutoff_results.get("verification"), list):
            print_results_table(
                "Cutoff Date Verification", cutoff_results["verification"]
            )

    # Summary of findings
    print("\n[bold blue]=== SUMMARY OF FINDINGS FOR 1S INTERVAL DATA ====[/bold blue]")

    # Check if 1s is available at all in recent data
    recent_candles_available = any(
        r["has_data"] for r in recent_results if r["endpoint"] == "candles"
    )
    recent_history_available = any(
        r["has_data"] for r in recent_results if r["endpoint"] == "history-candles"
    )

    # Check if 1s is available at all in historical data
    historical_candles_available = any(
        r["has_data"] for r in historical_results if r["endpoint"] == "candles"
    )
    historical_history_available = any(
        r["has_data"] for r in historical_results if r["endpoint"] == "history-candles"
    )

    print("\n[bold cyan]1-Second Data Availability Summary[/bold cyan]")
    print(
        f"- Candles endpoint (recent): {'✅ Available' if recent_candles_available else '❌ Not available'}"
    )
    print(
        f"- History-candles endpoint (recent): {'✅ Available' if recent_history_available else '❌ Not available'}"
    )
    print(
        f"- Candles endpoint (historical): {'✅ Available' if historical_candles_available else '❌ Not available'}"
    )
    print(
        f"- History-candles endpoint (historical): {'✅ Available' if historical_history_available else '❌ Not available'}"
    )

    # Display cutoff date if we found it
    if "cutoff_date" in all_results:
        cutoff = all_results["cutoff_date"]
        print("\n[bold cyan]Data Retention Boundary[/bold cyan]")
        print(f"- Cutoff date: {cutoff['cutoff_date']}")
        print(
            f"- Days of 1s data available: approximately {cutoff['days_of_data']} days"
        )

    # Overall availability determination
    print("\n[bold green]Conclusion:[/bold green]")
    if not any(
        [
            recent_candles_available,
            recent_history_available,
            historical_candles_available,
            historical_history_available,
        ]
    ):
        print(
            "[bold red]1-second interval data is NOT available on any OKX endpoint tested.[/bold red]"
        )
    else:
        if recent_candles_available:
            print(
                "[bold green]1-second interval data is available on the candles endpoint for recent data.[/bold green]"
            )
        if recent_history_available:
            print(
                "[bold green]1-second interval data is available on the history-candles endpoint for recent data.[/bold green]"
            )
        if historical_candles_available:
            print(
                "[bold green]1-second interval data is available on the candles endpoint for historical data.[/bold green]"
            )
        if historical_history_available:
            print(
                "[bold green]1-second interval data is available on the history-candles endpoint for historical data.[/bold green]"
            )

        if "cutoff_date" in all_results:
            cutoff = all_results["cutoff_date"]
            print(
                f"[bold green]1-second data is available from {cutoff['cutoff_date']} to present (approximately {cutoff['days_of_data']} days).[/bold green]"
            )


if __name__ == "__main__":
    main()
