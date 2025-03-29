#!/usr/bin/env python3
"""
Consolidated HTTP Client Benchmark and Best Practices

This script provides:
1. Comprehensive benchmarking of different HTTP client approaches for Binance Vision data
2. Reference implementations of best practices identified through testing
3. Both traditional check-then-download and optimized download-first approaches

Key best practices demonstrated:
- Use curl_cffi for optimal performance and lower CPU usage
- Implement download-first approach for faster file availability checking
- Use concurrent downloads for multiple files
- Configure proper timeouts without unnecessary retry logic
"""

import asyncio
import time
import statistics
import argparse
import os
import sys
from datetime import datetime, timedelta
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import shutil
from contextlib import contextmanager

# Suppress specific CURL warnings
warnings.filterwarnings("ignore", message=".*SSLKEYLOGFILE.*")

# Import HTTP clients
import aiohttp

# Try to import curl_cffi - our recommended client
try:
    import curl_cffi.requests as curl_requests

    CURL_CFFI_AVAILABLE = True
except ImportError:
    CURL_CFFI_AVAILABLE = False
    print("curl_cffi not available. Install with: pip install curl-cffi")
    print("Some benchmark features will be limited.")

# Command-line arguments
parser = argparse.ArgumentParser(
    description="Benchmark HTTP clients and demonstrate best practices"
)
parser.add_argument(
    "--mode",
    default="benchmark",
    choices=["benchmark", "compare-approaches", "demo-best-practice"],
    help="Benchmark mode to run",
)
parser.add_argument(
    "--market",
    default="spot",
    choices=["spot", "um", "cm"],
    help="Market type: spot, um (USDT-Margined Futures), or cm (Coin-Margined Futures)",
)
parser.add_argument(
    "--symbol", default="BTCUSDT", help="Trading pair symbol for date testing"
)
parser.add_argument(
    "--interval",
    default="1h",
    choices=[
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
    ],
    help="Time interval for date testing",
)
parser.add_argument(
    "--days", type=int, default=5, help="Number of days to check backward"
)
parser.add_argument(
    "--iterations", type=int, default=3, help="Number of iterations for benchmarking"
)
parser.add_argument(
    "--timeout", type=float, default=3.0, help="Request timeout in seconds"
)
parser.add_argument(
    "--concurrent", type=int, default=4, help="Number of concurrent downloads"
)
parser.add_argument(
    "--output-dir",
    default=None,
    help="Directory for downloads (defaults to temporary directory)",
)
parser.add_argument("--verbose", action="store_true", help="Show detailed output")
args = parser.parse_args()


# -------------------------- UTILITY FUNCTIONS --------------------------


def get_base_url(market_type, symbol, interval):
    """Get the base URL for data download based on market type."""
    if market_type == "spot":
        return f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{interval}"
    elif market_type == "um":
        return f"https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{interval}"
    elif market_type == "cm":
        return f"https://data.binance.vision/data/futures/cm/daily/klines/{symbol}/{interval}"
    else:
        raise ValueError(f"Invalid market type: {market_type}")


def get_test_dates(days_back=5):
    """Get a list of dates to test, from most recent backward."""
    current_date = datetime.utcnow()
    return [
        (current_date - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(days_back + 1)
    ]


def fmt_time(seconds):
    """Format time in an appropriate scale (ms or s)."""
    if seconds < 0.1:
        return f"{seconds * 1000:.2f}ms"
    return f"{seconds:.4f}s"


@contextmanager
def get_output_dir(specified_dir=None):
    """Create and manage output directory for downloads."""
    if specified_dir:
        os.makedirs(specified_dir, exist_ok=True)
        yield specified_dir
        # Don't clean up specified directory
    else:
        temp_dir = tempfile.mkdtemp()
        try:
            yield temp_dir
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


# -------------------------- BEST PRACTICE IMPLEMENTATIONS --------------------------


def download_first_curl_cffi(url, output_path=None, timeout=3.0):
    """Best practice implementation using curl_cffi with download-first approach."""
    if not CURL_CFFI_AVAILABLE:
        return False, None, 0

    start_time = time.time()
    success = False

    try:
        response = curl_requests.get(url, timeout=timeout)

        if response.status_code == 200:
            if output_path:
                with open(output_path, "wb") as f:
                    f.write(response.content)
            success = True

    except Exception as e:
        if args.verbose:
            print(f"curl_cffi error: {e}")

    elapsed = time.time() - start_time
    return success, response.content if success else None, elapsed


def check_latest_date_download_first(
    market_type, symbol, interval, max_days_back=5, timeout=3.0, output_dir=None
):
    """Find latest available date using download-first approach (recommended)."""
    base_url = get_base_url(market_type, symbol, interval)
    dates = get_test_dates(max_days_back)

    start_time = time.time()

    for date in dates:
        filename = f"{symbol}-{interval}-{date}.zip"
        url = f"{base_url}/{filename}"

        if args.verbose:
            print(f"Checking {date} using download-first approach...")

        output_path = os.path.join(output_dir, filename) if output_dir else None
        success, content, elapsed = download_first_curl_cffi(url, output_path, timeout)

        if success:
            total_time = time.time() - start_time
            return date, total_time, len(content) if content else 0

    total_time = time.time() - start_time
    return None, total_time, 0


def download_multiple_files_concurrent(urls, output_dir, max_concurrent=4, timeout=3.0):
    """Download multiple files concurrently using curl_cffi (recommended approach)."""
    if not CURL_CFFI_AVAILABLE:
        print("curl_cffi is required for concurrent downloads")
        return []

    os.makedirs(output_dir, exist_ok=True)
    successful_downloads = []

    def download_single_file(url):
        try:
            filename = os.path.basename(url)
            output_path = os.path.join(output_dir, filename)

            success, _, elapsed = download_first_curl_cffi(url, output_path, timeout)
            if success:
                return output_path, elapsed
            return None, elapsed
        except Exception as e:
            if args.verbose:
                print(f"Error downloading {url}: {e}")
            return None, 0

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        future_to_url = {
            executor.submit(download_single_file, url): url for url in urls
        }

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result, elapsed = future.result()
                if result:
                    successful_downloads.append(result)
                    if args.verbose:
                        print(
                            f"Downloaded {os.path.basename(url)} in {fmt_time(elapsed)}"
                        )
            except Exception as e:
                if args.verbose:
                    print(f"Error processing {url}: {e}")

    total_time = time.time() - start_time
    return successful_downloads, total_time


# -------------------------- LEGACY APPROACH IMPLEMENTATIONS --------------------------


async def check_url_aiohttp(url, timeout):
    """Traditional HEAD-based URL checking with aiohttp."""
    start_time = time.time()
    success = False

    async with aiohttp.ClientSession() as client:
        try:
            # First try HEAD
            async with client.head(url, timeout=timeout) as response:
                success = response.status == 200

        except Exception as e:
            if args.verbose:
                print(f"aiohttp error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


def check_url_curl_cffi(url, timeout):
    """Traditional HEAD-based URL checking with curl_cffi."""
    if not CURL_CFFI_AVAILABLE:
        return False, 0

    start_time = time.time()
    success = False

    try:
        response = curl_requests.head(url, timeout=timeout)
        success = response.status_code == 200
    except Exception as e:
        if args.verbose:
            print(f"curl_cffi error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


async def check_latest_date_traditional(
    market_type, symbol, interval, max_days_back=5, timeout=3.0, client="curl_cffi"
):
    """Find latest available date using traditional check-then-download approach."""
    base_url = get_base_url(market_type, symbol, interval)
    dates = get_test_dates(max_days_back)

    start_time = time.time()

    for date in dates:
        filename = f"{symbol}-{interval}-{date}.zip"
        url = f"{base_url}/{filename}"

        if args.verbose:
            print(f"Checking {date} using {client}...")

        if client == "aiohttp":
            success, elapsed = await check_url_aiohttp(url, timeout)
        else:  # curl_cffi
            success, elapsed = check_url_curl_cffi(url, timeout)

        if success:
            total_time = time.time() - start_time
            return date, total_time

    total_time = time.time() - start_time
    return None, total_time


# -------------------------- BENCHMARK FUNCTIONS --------------------------


async def compare_approaches(market_type, symbol, interval, max_days_back, timeout):
    """Compare different URL checking approaches."""
    print("\n" + "=" * 80)
    print("COMPARING URL CHECKING APPROACHES")
    print("=" * 80)
    print(f"Testing for {market_type}/{symbol}/{interval} over {max_days_back} days")
    print(f"Timeout: {timeout}s")
    print("-" * 80)

    # Track results
    results = []

    # Test traditional approach with curl_cffi
    print("\nTesting traditional check-then-download approach with curl_cffi...")
    date, elapsed = await check_latest_date_traditional(
        market_type, symbol, interval, max_days_back, timeout, "curl_cffi"
    )
    results.append(("Traditional (curl_cffi)", date, elapsed))
    print(f"  Result: {date or 'Not found'} in {fmt_time(elapsed)}")

    # Test traditional approach with aiohttp
    print("\nTesting traditional check-then-download approach with aiohttp...")
    date, elapsed = await check_latest_date_traditional(
        market_type, symbol, interval, max_days_back, timeout, "aiohttp"
    )
    results.append(("Traditional (aiohttp)", date, elapsed))
    print(f"  Result: {date or 'Not found'} in {fmt_time(elapsed)}")

    # Test download-first approach (recommended)
    print("\nTesting download-first approach with curl_cffi (recommended)...")
    with get_output_dir(args.output_dir) as output_dir:
        date, elapsed, size = check_latest_date_download_first(
            market_type, symbol, interval, max_days_back, timeout, output_dir
        )
    results.append(("Download-first (curl_cffi)", date, elapsed))
    print(f"  Result: {date or 'Not found'} in {fmt_time(elapsed)}")
    if date and size:
        print(f"  File size: {size/1024:.1f} KB")

    # Print comparison
    print("\n" + "-" * 80)
    print("RESULTS COMPARISON")
    print("-" * 80)

    # Sort by speed
    results.sort(key=lambda x: x[2])

    print(f"{'Approach':30} {'Date Found':15} {'Time':12}")
    print("-" * 60)
    for approach, date, elapsed in results:
        print(f"{approach:30} {date or 'Not found':15} {fmt_time(elapsed):12}")

    # Calculate speedup
    fastest = results[0]
    for approach, date, elapsed in results[1:]:
        speedup = (elapsed / fastest[2] - 1) * 100
        print(f"{fastest[0]} is {speedup:.1f}% faster than {approach}")


async def benchmark_url_checks(market_type, symbol, interval, iterations, timeout):
    """Benchmark URL availability checks."""
    print("\n" + "=" * 80)
    print("BENCHMARKING URL AVAILABILITY CHECKS")
    print("=" * 80)
    print(f"Testing for {market_type}/{symbol}/{interval}")
    print(f"Iterations: {iterations}, Timeout: {timeout}s")
    print("-" * 80)

    # Get a date we know should exist (yesterday)
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    filename = f"{symbol}-{interval}-{yesterday}.zip"
    base_url = get_base_url(market_type, symbol, interval)
    url = f"{base_url}/{filename}"

    print(f"\nTesting URL: {url}")

    # Results storage
    results = {"aiohttp_head": [], "curl_cffi_head": [], "curl_cffi_get": []}

    # Test HEAD with aiohttp
    print("\nBenchmarking aiohttp (HEAD)...")
    for i in range(iterations):
        success, elapsed = await check_url_aiohttp(url, timeout)
        results["aiohttp_head"].append(elapsed)
        print(
            f"  Iteration {i+1}: {'Success' if success else 'Failed'} in {fmt_time(elapsed)}"
        )

    # Test HEAD with curl_cffi
    if CURL_CFFI_AVAILABLE:
        print("\nBenchmarking curl_cffi (HEAD)...")
        for i in range(iterations):
            success, elapsed = check_url_curl_cffi(url, timeout)
            results["curl_cffi_head"].append(elapsed)
            print(
                f"  Iteration {i+1}: {'Success' if success else 'Failed'} in {fmt_time(elapsed)}"
            )

    # Test GET with curl_cffi (download-first approach)
    if CURL_CFFI_AVAILABLE:
        print("\nBenchmarking curl_cffi (GET/download-first)...")
        for i in range(iterations):
            success, _, elapsed = download_first_curl_cffi(url, None, timeout)
            results["curl_cffi_get"].append(elapsed)
            print(
                f"  Iteration {i+1}: {'Success' if success else 'Failed'} in {fmt_time(elapsed)}"
            )

    # Print summary statistics
    print("\n" + "-" * 80)
    print("BENCHMARK SUMMARY")
    print("-" * 80)

    print(f"{'Approach':25} {'Avg Time':12} {'Min Time':12} {'Max Time':12}")
    print("-" * 65)

    for name, times in results.items():
        if not times:
            continue

        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)

        print(
            f"{name:25} {fmt_time(avg_time):12} {fmt_time(min_time):12} {fmt_time(max_time):12}"
        )


async def benchmark_concurrent_downloads(
    market_type, symbol, intervals, days_back, concurrent, timeout
):
    """Benchmark concurrent downloads performance."""
    print("\n" + "=" * 80)
    print("BENCHMARKING CONCURRENT DOWNLOADS")
    print("=" * 80)
    print(f"Testing for {market_type}/{symbol} with {concurrent} concurrent downloads")
    print(f"Timeout: {timeout}s")
    print("-" * 80)

    # Generate URLs for multiple intervals
    urls = []
    available_dates = {}

    # First find which dates exist for each interval
    for interval in intervals:
        base_url = get_base_url(market_type, symbol, interval)
        dates = get_test_dates(days_back)

        for date in dates:
            filename = f"{symbol}-{interval}-{date}.zip"
            url = f"{base_url}/{filename}"

            if CURL_CFFI_AVAILABLE:
                success, _ = check_url_curl_cffi(url, timeout)
                if success:
                    urls.append(url)
                    available_dates[interval] = date
                    break

    if not urls:
        print(
            "No valid URLs found to download. Try with different symbol or intervals."
        )
        return

    print(f"\nFound {len(urls)} files to download:")
    for interval, date in available_dates.items():
        print(f"  {symbol}-{interval}-{date}.zip")

    # Benchmark sequential downloads
    print("\nBenchmarking sequential downloads...")
    with get_output_dir(args.output_dir) as output_dir:
        start_time = time.time()
        successful = []

        for url in urls:
            filename = os.path.basename(url)
            output_path = os.path.join(output_dir, filename)
            success, _, elapsed = download_first_curl_cffi(url, output_path, timeout)

            if success:
                successful.append(output_path)
                print(f"  Downloaded {filename} in {fmt_time(elapsed)}")

        sequential_time = time.time() - start_time
        print(f"  Total time for sequential downloads: {fmt_time(sequential_time)}")
        print(f"  Successfully downloaded {len(successful)}/{len(urls)} files")

    # Benchmark concurrent downloads
    print("\nBenchmarking concurrent downloads...")
    with get_output_dir(args.output_dir) as output_dir:
        successful, concurrent_time = download_multiple_files_concurrent(
            urls, output_dir, concurrent, timeout
        )
        print(f"  Total time for concurrent downloads: {fmt_time(concurrent_time)}")
        print(f"  Successfully downloaded {len(successful)}/{len(urls)} files")

    # Calculate speedup
    if sequential_time > 0 and concurrent_time > 0:
        speedup = (sequential_time / concurrent_time - 1) * 100
        print(
            f"\nConcurrent downloads were {speedup:.1f}% faster than sequential downloads"
        )


def demo_best_practice(market_type, symbol, interval, days_back, timeout):
    """Demonstrate the recommended best practice implementation."""
    print("\n" + "=" * 80)
    print("BEST PRACTICE IMPLEMENTATION DEMO")
    print("=" * 80)
    print(
        "This demonstrates our recommended best practice for working with Binance Vision data"
    )
    print("-" * 80)

    # Test finding the latest date
    print("\nFinding the latest available date...")
    with get_output_dir(args.output_dir) as output_dir:
        date, elapsed, size = check_latest_date_download_first(
            market_type, symbol, interval, days_back, timeout, output_dir
        )

    if date:
        print(f"✓ Found latest date: {date} in {fmt_time(elapsed)}")
        print(f"✓ File size: {size/1024:.1f} KB")

        # Show the full implementation
        print("\nHere's the recommended implementation:")
        print("-" * 80)
        print(
            '''
# Best practice implementation for finding and downloading data
import os
import curl_cffi.requests as curl_requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_latest_data(symbol, interval, max_days_back=5, output_dir="downloads"):
    """Get the latest available data file using download-first approach."""
    os.makedirs(output_dir, exist_ok=True)
    current_date = datetime.utcnow()
    
    for i in range(max_days_back + 1):
        check_date = (current_date - timedelta(days=i)).strftime("%Y-%m-%d")
        url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{check_date}.zip"
        output_path = os.path.join(output_dir, f"{symbol}-{interval}-{check_date}.zip")
        
        try:
            # Attempt direct download without checking first
            response = curl_requests.get(url, timeout=3.0)
            
            if response.status_code == 200:
                # File exists and download was successful
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                return check_date, output_path
                
        except Exception:
            # Continue to next date on error
            continue
            
    return None, None

def download_multiple_files(url_list, output_dir="downloads", max_concurrent=4):
    """Download multiple files concurrently using curl_cffi."""
    os.makedirs(output_dir, exist_ok=True)
    successful_downloads = []
    
    def download_single_file(url):
        try:
            filename = os.path.basename(url)
            output_path = os.path.join(output_dir, filename)
            
            response = curl_requests.get(url, timeout=3.0)
            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                return output_path
        except Exception:
            return None
    
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        future_to_url = {executor.submit(download_single_file, url): url for url in url_list}
        
        for future in as_completed(future_to_url):
            result = future.result()
            if result:
                successful_downloads.append(result)
    
    return successful_downloads
'''
        )
    else:
        print(f"✗ No data found for the last {days_back} days")


# -------------------------- MAIN EXECUTION --------------------------


async def main():
    # Check that curl_cffi is available
    if not CURL_CFFI_AVAILABLE:
        print(
            "\nWARNING: curl_cffi is not installed, which is required for optimal performance."
        )
        print("Install it with: pip install curl-cffi")

    print("=" * 80)
    print("HTTP CLIENT BENCHMARK AND BEST PRACTICES")
    print("=" * 80)

    if args.mode == "compare-approaches":
        await compare_approaches(
            args.market, args.symbol, args.interval, args.days, args.timeout
        )
    elif args.mode == "benchmark":
        # Benchmark URL checks
        await benchmark_url_checks(
            args.market, args.symbol, args.interval, args.iterations, args.timeout
        )

        # Benchmark downloads with different intervals
        intervals = ["1h", "4h", "1d"]
        intervals = [i for i in intervals if i != args.interval]
        intervals.insert(0, args.interval)  # Put the specified interval first
        intervals = intervals[:3]  # Limit to 3 intervals

        await benchmark_concurrent_downloads(
            args.market,
            args.symbol,
            intervals,
            args.days,
            args.concurrent,
            args.timeout,
        )
    elif args.mode == "demo-best-practice":
        demo_best_practice(
            args.market, args.symbol, args.interval, args.days, args.timeout
        )

    print("\n" + "=" * 80)
    print("COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(0)
