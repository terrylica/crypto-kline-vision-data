#!/usr/bin/env python3
"""
Download-First Benchmark for HTTP Clients

This script benchmarks an alternative approach for working with Binance Vision data:
instead of checking if a file exists before downloading, it attempts to download directly
and measures how quickly it can detect failures.

The hypothesis is that for file-existence checking, a direct download attempt might be
more efficient than a separate HEAD request followed by a download.
"""

import asyncio
import time
import statistics
import argparse
import os
from datetime import datetime, timedelta
import warnings

# Suppress specific CURL warnings
warnings.filterwarnings("ignore", message=".*SSLKEYLOGFILE.*")

# Import HTTP clients
import aiohttp

try:
    import curl_cffi.requests as curl_requests
except ImportError:
    curl_requests = None
    print("curl_cffi not available. Install with: pip install curl-cffi")

# Command-line arguments
parser = argparse.ArgumentParser(
    description="Benchmark 'download-first' vs 'check-then-download' approaches"
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
    "--iterations", type=int, default=5, help="Number of iterations for benchmarking"
)
parser.add_argument(
    "--timeout", type=float, default=3.0, help="Request timeout in seconds"
)
parser.add_argument("--verbose", action="store_true", help="Show detailed output")
parser.add_argument(
    "--output-dir", default="./temp_downloads", help="Directory for temporary downloads"
)
parser.add_argument(
    "--simulate-failure",
    action="store_true",
    help="Simulate failure by trying future dates",
)
args = parser.parse_args()


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


def get_test_date():
    """Get the test date - either yesterday (for success) or tomorrow (for failure)."""
    if args.simulate_failure:
        # Use tomorrow's date to simulate a failure
        return (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # Use yesterday's date which should exist
        return (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")


# -------------------------- CHECK THEN DOWNLOAD IMPLEMENTATIONS --------------------------


async def check_then_download_aiohttp(
    base_url, date, symbol, interval, timeout, output_dir
):
    """Check if file exists, then download it using aiohttp."""
    filename = f"{symbol}-{interval}-{date}.zip"
    url = f"{base_url}/{filename}"
    output_path = os.path.join(output_dir, filename)

    start_time = time.time()
    success = False
    file_exists = False

    # First check if the file exists
    async with aiohttp.ClientSession() as session:
        try:
            # Try HEAD request first
            async with session.head(url, timeout=timeout) as response:
                file_exists = response.status == 200

            # If file exists, download it
            if file_exists:
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        content = await response.read()
                        with open(output_path, "wb") as f:
                            f.write(content)
                        success = True
        except Exception as e:
            success = False
            if args.verbose:
                print(f"aiohttp error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


async def check_then_download_curl_cffi(
    base_url, date, symbol, interval, timeout, output_dir
):
    """Check if file exists, then download it using curl_cffi."""
    if curl_requests is None:
        return False, 0

    filename = f"{symbol}-{interval}-{date}.zip"
    url = f"{base_url}/{filename}"
    output_path = os.path.join(output_dir, filename)

    start_time = time.time()
    success = False
    file_exists = False

    try:
        # First check if the file exists
        response = curl_requests.head(url, timeout=timeout)
        file_exists = response.status_code == 200

        # If file exists, download it
        if file_exists:
            response = curl_requests.get(url, timeout=timeout)
            if response.status_code == 200:
                with open(output_path, "wb") as f:
                    f.write(response.content)
                success = True
    except Exception as e:
        success = False
        if args.verbose:
            print(f"curl_cffi error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


# -------------------------- DOWNLOAD FIRST IMPLEMENTATIONS --------------------------


async def download_first_aiohttp(base_url, date, symbol, interval, timeout, output_dir):
    """Attempt to download directly without checking, using aiohttp."""
    filename = f"{symbol}-{interval}-{date}.zip"
    url = f"{base_url}/{filename}"
    output_path = os.path.join(output_dir, filename)

    start_time = time.time()
    success = False

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    content = await response.read()
                    with open(output_path, "wb") as f:
                        f.write(content)
                    success = True
        except Exception as e:
            success = False
            if args.verbose:
                print(f"aiohttp error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


async def download_first_curl_cffi(
    base_url, date, symbol, interval, timeout, output_dir
):
    """Attempt to download directly without checking, using curl_cffi."""
    if curl_requests is None:
        return False, 0

    filename = f"{symbol}-{interval}-{date}.zip"
    url = f"{base_url}/{filename}"
    output_path = os.path.join(output_dir, filename)

    start_time = time.time()
    success = False

    try:
        response = curl_requests.get(url, timeout=timeout)
        if response.status_code == 200:
            with open(output_path, "wb") as f:
                f.write(response.content)
            success = True
    except Exception as e:
        success = False
        if args.verbose:
            print(f"curl_cffi error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


# -------------------------- SMALL FILES DOWNLOADING IMPLEMENTATIONS --------------------------


async def download_small_aiohttp(
    base_url, date, symbol, interval, timeout, output_dir, bytes_limit=1024
):
    """Download only the first few bytes to quickly determine if file exists, using aiohttp."""
    filename = f"{symbol}-{interval}-{date}.zip"
    url = f"{base_url}/{filename}"
    output_path = os.path.join(output_dir, filename)

    start_time = time.time()
    success = False

    headers = {"Range": f"bytes=0-{bytes_limit-1}"}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=timeout, headers=headers) as response:
                if (
                    response.status == 206 or response.status == 200
                ):  # Partial Content or OK
                    # Just read a small part to verify it exists
                    content = await response.content.read(bytes_limit)
                    success = len(content) > 0
                    # We could save this partial content and resume later if needed
        except Exception as e:
            success = False
            if args.verbose:
                print(f"aiohttp small download error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


async def download_small_curl_cffi(
    base_url, date, symbol, interval, timeout, output_dir, bytes_limit=1024
):
    """Download only the first few bytes to quickly determine if file exists, using curl_cffi."""
    if curl_requests is None:
        return False, 0

    filename = f"{symbol}-{interval}-{date}.zip"
    url = f"{base_url}/{filename}"
    output_path = os.path.join(output_dir, filename)

    start_time = time.time()
    success = False

    headers = {"Range": f"bytes=0-{bytes_limit-1}"}

    try:
        response = curl_requests.get(url, timeout=timeout, headers=headers)
        if (
            response.status_code == 206 or response.status_code == 200
        ):  # Partial Content or OK
            success = len(response.content) > 0
            # We could save this partial content and resume later if needed
    except Exception as e:
        success = False
        if args.verbose:
            print(f"curl_cffi small download error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


# -------------------------- BENCHMARK FUNCTIONS --------------------------


async def run_benchmarks(
    base_url, date, symbol, interval, iterations, timeout, output_dir
):
    """Run all benchmarks and compare results."""
    print(f"\nBenchmarking for {symbol}-{interval}-{date}")
    print(f"Simulating {'failure' if args.simulate_failure else 'success'} scenario")
    print("-" * 80)

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Results storage
    results = {}

    # Run benchmarks for each approach and client
    for client in ["aiohttp", "curl_cffi"]:
        if client == "curl_cffi" and curl_requests is None:
            print(f"Skipping {client} benchmarks (not installed)")
            continue

        for approach in ["check_then_download", "download_first", "download_small"]:
            times = []
            successes = 0

            func_name = f"{approach}_{client}"
            func = globals().get(func_name)

            if not func:
                print(f"Function {func_name} not found, skipping")
                continue

            print(f"\nRunning {approach} with {client}...")

            for i in range(iterations):
                success, elapsed = await func(
                    base_url, date, symbol, interval, timeout, output_dir
                )
                times.append(elapsed)
                if success:
                    successes += 1

                print(
                    f"  Iteration {i+1}: {'Success' if success else 'Failed'} in {elapsed:.4f}s"
                )

                # Small delay between iterations
                await asyncio.sleep(0.1)

            # Calculate statistics
            avg_time = sum(times) / len(times)
            success_rate = (successes / iterations) * 100

            results[func_name] = {
                "avg_time": avg_time,
                "min_time": min(times),
                "max_time": max(times),
                "success_rate": success_rate,
                "std_dev": statistics.stdev(times) if len(times) > 1 else 0,
            }

            print(f"  Average: {avg_time:.4f}s, Success rate: {success_rate:.1f}%")

    # Print comparative results
    print("\n" + "=" * 80)
    print("BENCHMARK RESULTS SUMMARY")
    print("=" * 80)

    # Sort by average time
    sorted_results = sorted(results.items(), key=lambda x: x[1]["avg_time"])

    print(
        f"{'Approach':30} {'Avg Time (s)':15} {'Success Rate (%)':15} {'Min (s)':10} {'Max (s)':10}"
    )
    print("-" * 80)

    for name, data in sorted_results:
        print(
            f"{name:30} {data['avg_time']:<15.4f} {data['success_rate']:<15.1f} {data['min_time']:<10.4f} {data['max_time']:<10.4f}"
        )

    # Calculate and show speed comparisons
    if len(sorted_results) > 1:
        print("\nSpeed Comparisons:")
        fastest = sorted_results[0]
        for name, data in sorted_results[1:]:
            speedup = (data["avg_time"] / fastest[1]["avg_time"]) - 1
            print(f"{fastest[0]} is {speedup*100:.1f}% faster than {name}")


async def main():
    # Set up the benchmark parameters
    base_url = get_base_url(args.market, args.symbol, args.interval)
    date = get_test_date()

    print("=" * 80)
    print("DOWNLOAD-FIRST vs CHECK-THEN-DOWNLOAD BENCHMARK")
    print("=" * 80)
    print(f"\nTesting URL: {base_url}/{args.symbol}-{args.interval}-{date}.zip")
    print(f"Timeout: {args.timeout}s, Iterations: {args.iterations}")

    # Run the benchmarks
    await run_benchmarks(
        base_url,
        date,
        args.symbol,
        args.interval,
        args.iterations,
        args.timeout,
        args.output_dir,
    )

    # Clean up temp downloads if needed
    # Uncomment to automatically clean up downloads
    # import shutil
    # shutil.rmtree(args.output_dir, ignore_errors=True)

    print("\n" + "=" * 80)
    print("BENCHMARK COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user.")
        import sys

        sys.exit(0)
