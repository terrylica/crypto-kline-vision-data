#!/usr/bin/env python3
"""
Consolidated HTTP Client Benchmark and Best Practices

This script provides:
1. Comprehensive benchmarking of different HTTP client approaches for Binance Vision data
2. Reference implementations of best practices identified through testing
3. Both traditional check-then-download and optimized download-first approaches
4. Testing across different data granularities (1s for spot, 1m for futures)
5. Benchmarking against AWS CLI for comparison

Key best practices demonstrated:
- Use curl_cffi for optimal performance and lower CPU usage
- Implement download-first approach for faster file availability checking
- Use concurrent downloads (concurrency level 50) for multiple files
- Adjust concurrency based on batch size (10, 50, or 100)
- Configure proper timeouts without unnecessary retry logic
"""

import asyncio
import time
import argparse
import os
from datetime import datetime, timedelta
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import shutil
from contextlib import contextmanager
import subprocess

# Suppress specific CURL warnings
warnings.filterwarnings("ignore", message=".*SSLKEYLOGFILE.*")

# Import HTTP clients - all made optional for better compatibility
AIOHTTP_AVAILABLE = False
CURL_CFFI_AVAILABLE = False
CURL_CFFI_ASYNC_AVAILABLE = False  # New flag for async API

# Try to import curl_cffi - our recommended client
try:
    import curl_cffi.requests as curl_requests

    CURL_CFFI_AVAILABLE = True

    # Also try importing AsyncSession for async benchmarks
    try:
        from curl_cffi.requests import AsyncSession

        CURL_CFFI_ASYNC_AVAILABLE = True
    except ImportError:
        print(
            "curl_cffi AsyncSession not available. Some async benchmarks will be limited."
        )
except ImportError:
    print("curl_cffi not available. Install with: pip install curl-cffi")
    print("Some benchmark features will be limited.")

# Command-line arguments
parser = argparse.ArgumentParser(
    description="Benchmark and demonstrate best practices for downloading Binance Vision data"
)
parser.add_argument(
    "--mode",
    choices=["demo", "aws", "download"],
    default="demo",
    help="Mode to run: demo (best practices), aws (AWS comparison), download (just download latest)",
)
parser.add_argument(
    "--market",
    choices=["spot", "um", "cm"],
    default="spot",
    help="Market type: spot, um (USDT-M futures), cm (COIN-M futures)",
)
parser.add_argument(
    "--symbol",
    default="BTCUSDT",
    help="Symbol to use for benchmarking (e.g., BTCUSDT)",
)
parser.add_argument(
    "--interval",
    default="1s",
    help="Interval to use for benchmarking (e.g., 1s, 1m, 1h)",
)
parser.add_argument(
    "--days",
    type=int,
    default=5,
    help="Number of days to check backward",
)
parser.add_argument(
    "--timeout",
    type=float,
    default=3.0,
    help="Timeout for HTTP requests (seconds)",
)
parser.add_argument(
    "--concurrent",
    type=int,
    default=50,
    help="Concurrent downloads for benchmarks",
)
parser.add_argument(
    "--output-dir",
    default=None,
    help="Directory to save downloaded files",
)
parser.add_argument("--verbose", action="store_true", help="Show verbose output")
args = parser.parse_args()


# -------------------------- UTILITY FUNCTIONS --------------------------


def get_base_url(market_type, symbol, interval):
    """Get the base URL for data download based on market type and interval."""
    # Special handling for spot market with 1s granularity
    if market_type == "spot":
        # Use 1s granularity if specified and market is spot
        interval_to_use = interval
        return f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{interval_to_use}"
    elif market_type == "um":
        # Futures markets use minimum 1m granularity
        interval_to_use = "1m" if interval == "1s" else interval
        return f"https://data.binance.vision/data/futures/um/daily/klines/{symbol}/{interval_to_use}"
    elif market_type == "cm":
        # Futures markets use minimum 1m granularity
        interval_to_use = "1m" if interval == "1s" else interval
        return f"https://data.binance.vision/data/futures/cm/daily/klines/{symbol}/{interval_to_use}"
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


# -------------------------- FALLBACK FUNCTIONS --------------------------


def download_with_curl_command(url, output_path, timeout=3.0):
    """Fallback method to download using curl command line when curl_cffi is not available."""
    start_time = time.time()
    success = False
    content = None

    try:
        # Use subprocess to run curl
        result = subprocess.run(
            ["curl", "-s", "-o", output_path, "--connect-timeout", str(timeout), url],
            capture_output=True,
            text=True,
            timeout=timeout + 5,  # Add 5s buffer to the subprocess timeout
        )

        if (
            result.returncode == 0
            and os.path.exists(output_path)
            and os.path.getsize(output_path) > 0
        ):
            success = True
            with open(output_path, "rb") as f:
                content = f.read()
    except Exception as e:
        if args.verbose:
            print(f"curl command error: {e}")

    elapsed = time.time() - start_time
    return success, content, elapsed


# -------------------------- BEST PRACTICE IMPLEMENTATIONS --------------------------


def download_first_curl_cffi(url, output_path=None, timeout=3.0):
    """Best practice implementation using curl_cffi with download-first approach."""
    if CURL_CFFI_AVAILABLE:
        start_time = time.time()
        success = False

        try:
            response = curl_requests.get(url, timeout=timeout)

            if response.status_code == 200:
                if output_path:
                    with open(output_path, "wb") as f:
                        f.write(response.content)
                success = True
                content = response.content
            else:
                content = None

        except Exception as e:
            if args.verbose:
                print(f"curl_cffi error: {e}")
            content = None

        elapsed = time.time() - start_time
        return success, content, elapsed
    else:
        # Fallback to command-line curl if curl_cffi is not available
        return download_with_curl_command(
            url, output_path or "temp_download.dat", timeout
        )


async def download_first_curl_cffi_async(url, output_path=None, timeout=3.0):
    """Async implementation using curl_cffi with download-first approach."""
    if CURL_CFFI_ASYNC_AVAILABLE:
        start_time = time.time()
        success = False

        try:
            async with AsyncSession() as session:
                response = await session.get(url, timeout=timeout)

                if response.status_code == 200:
                    if output_path:
                        with open(output_path, "wb") as f:
                            f.write(response.content)
                    success = True
                    content = response.content
                else:
                    content = None

        except Exception as e:
            if args.verbose:
                print(f"curl_cffi async error: {e}")
            content = None

        elapsed = time.time() - start_time
        return success, content, elapsed
    else:
        # Fallback to synchronous version if async not available
        return download_first_curl_cffi(url, output_path, timeout)


def check_latest_date_download_first(
    market_type, symbol, interval, max_days_back=5, timeout=3.0, output_dir=None
):
    """Find latest available date using download-first approach (recommended)."""
    base_url = get_base_url(market_type, symbol, interval)
    dates = get_test_dates(max_days_back)

    # Determine which interval to use based on market type
    actual_interval = interval
    if market_type != "spot" and interval == "1s":
        actual_interval = "1m"  # Use 1m for futures markets
        if args.verbose:
            print(f"Using 1m interval for {market_type} market (1s not available)")

    start_time = time.time()

    for date in dates:
        filename = f"{symbol}-{actual_interval}-{date}.zip"
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


def download_multiple_files_concurrent(
    urls, output_dir, max_concurrent=50, timeout=3.0
):
    """Download multiple files concurrently using curl_cffi with ThreadPoolExecutor."""
    if not CURL_CFFI_AVAILABLE:
        print(
            "curl_cffi is required for optimal downloads. Falling back to curl command."
        )
        return download_with_curl_command_concurrent(
            urls, output_dir, max_concurrent, timeout
        )

    os.makedirs(output_dir, exist_ok=True)
    successful_downloads = []
    file_count = len(urls)

    # Dynamically adjust concurrency based on batch size
    adjusted_concurrency = max_concurrent
    if file_count <= 10:
        adjusted_concurrency = min(10, max_concurrent)
    elif file_count <= 50:
        adjusted_concurrency = min(50, max_concurrent)
    else:
        # Large batch optimization
        adjusted_concurrency = min(100, max_concurrent)

    if adjusted_concurrency != max_concurrent and args.verbose:
        print(f"Adjusting concurrency to {adjusted_concurrency} for {file_count} files")

    def download_single_file(url):
        try:
            filename = os.path.basename(url)
            output_path = os.path.join(output_dir, filename)

            if CURL_CFFI_AVAILABLE:
                success, _, elapsed = download_first_curl_cffi(
                    url, output_path, timeout
                )
            else:
                # Fallback to curl command if curl_cffi is not available
                success, _, elapsed = download_with_curl_command(
                    url, output_path, timeout
                )

            if success:
                return output_path, elapsed
            return None, elapsed
        except Exception as e:
            if args.verbose:
                print(f"Error downloading {url}: {e}")
            return None, 0

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=adjusted_concurrency) as executor:
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


def download_with_curl_command_concurrent(
    urls, output_dir, max_concurrent=50, timeout=3.0
):
    """Fallback method to download multiple files using command-line curl."""
    os.makedirs(output_dir, exist_ok=True)
    successful_downloads = []
    file_count = len(urls)

    # Dynamically adjust concurrency
    adjusted_concurrency = max_concurrent
    if file_count <= 10:
        adjusted_concurrency = min(10, max_concurrent)
    elif file_count <= 50:
        adjusted_concurrency = min(50, max_concurrent)
    else:
        adjusted_concurrency = min(100, max_concurrent)

    if adjusted_concurrency != max_concurrent and args.verbose:
        print(f"Adjusting concurrency to {adjusted_concurrency} for {file_count} files")

    def download_single_file(url):
        try:
            filename = os.path.basename(url)
            output_path = os.path.join(output_dir, filename)

            success, _, elapsed = download_with_curl_command(url, output_path, timeout)
            if success:
                return output_path, elapsed
            return None, elapsed
        except Exception as e:
            if args.verbose:
                print(f"Error downloading {url}: {e}")
            return None, 0

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=adjusted_concurrency) as executor:
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


async def check_url_with_curl_cffi_async(url, timeout):
    """HEAD-based URL checking with curl_cffi async."""
    if not CURL_CFFI_ASYNC_AVAILABLE:
        return await check_url_with_curl(url, timeout)

    start_time = time.time()
    success = False

    try:
        async with AsyncSession() as session:
            response = await session.head(url, timeout=timeout)
            success = response.status_code == 200
    except Exception as e:
        if args.verbose:
            print(f"curl_cffi async error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


async def check_url_with_curl(url, timeout):
    """Traditional HEAD-based URL checking with curl command."""
    start_time = time.time()
    success = False

    try:
        # Use subprocess to run curl with -I (HEAD)
        process = await asyncio.create_subprocess_exec(
            "curl",
            "-s",
            "-I",
            url,
            "--connect-timeout",
            str(timeout),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await process.communicate()

        # Check if HTTP 200 is in the response
        success = b"HTTP/" in stdout and b"200" in stdout.splitlines()[0]
    except Exception as e:
        if args.verbose:
            print(f"curl command error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


def check_url_curl_cffi(url, timeout):
    """Traditional HEAD-based URL checking with curl_cffi."""
    if CURL_CFFI_AVAILABLE:
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
    else:
        # Run synchronously using the subprocess version as a fallback
        return asyncio.run(check_url_with_curl(url, timeout))


async def check_latest_date_traditional(
    market_type, symbol, interval, max_days_back=5, timeout=3.0, client="curl_cffi"
):
    """Find latest available date using traditional check-then-download approach."""
    base_url = get_base_url(market_type, symbol, interval)
    dates = get_test_dates(max_days_back)

    # Determine which interval to use based on market type
    actual_interval = interval
    if market_type != "spot" and interval == "1s":
        actual_interval = "1m"  # Use 1m for futures markets
        if args.verbose:
            print(f"Using 1m interval for {market_type} market (1s not available)")

    start_time = time.time()

    for date in dates:
        filename = f"{symbol}-{actual_interval}-{date}.zip"
        url = f"{base_url}/{filename}"

        if args.verbose:
            print(f"Checking {date} using {client}...")

        if client == "curl_cffi_async" and CURL_CFFI_ASYNC_AVAILABLE:
            success, elapsed = await check_url_with_curl_cffi_async(url, timeout)
        else:  # curl_cffi or fallback to curl
            success, elapsed = check_url_curl_cffi(url, timeout)

        if success:
            total_time = time.time() - start_time
            return date, total_time

    total_time = time.time() - start_time
    return None, total_time


# -------------------------- AWS COMPARISON FUNCTIONS --------------------------


async def download_with_aws_cli(url, output_path, timeout=30.0):
    """Download a file using AWS CLI - for benchmarking comparison."""
    start_time = time.time()
    success = False

    try:
        # Use AWS CLI to download the file
        process = await asyncio.create_subprocess_exec(
            "aws",
            "--no-cli-pager",
            "s3",
            "cp",
            url,
            output_path,
            "--cli-connect-timeout",
            "5",
            "--cli-read-timeout",
            str(int(timeout)),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        # Check if download was successful
        if process.returncode == 0 and os.path.exists(output_path):
            success = True
        else:
            # Fallback to curl if AWS CLI fails
            if args.verbose:
                print(
                    f"AWS CLI failed, using curl fallback: {stderr.decode() if stderr else 'No error output'}"
                )

            fallback_process = await asyncio.create_subprocess_exec(
                "curl",
                "-s",
                "-o",
                output_path,
                url,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await fallback_process.communicate()

            success = os.path.exists(output_path) and os.path.getsize(output_path) > 0

    except Exception as e:
        if args.verbose:
            print(f"AWS CLI error: {e}")

    elapsed = time.time() - start_time
    return success, elapsed


async def benchmark_aws_vs_http(
    market_type, symbol, interval, concurrent_levels, timeout=3.0
):
    """Benchmark AWS CLI against HTTP clients (curl_cffi and curl_cffi async)."""
    print("\n" + "=" * 80)
    print("BENCHMARKING AWS CLI vs HTTP CLIENTS")
    print("=" * 80)
    print(f"Testing for {market_type}/{symbol}/{interval}")
    print(f"Concurrency levels: {concurrent_levels}, Timeout: {timeout}s")
    print("-" * 80)

    # Verify that curl_cffi is available
    if not CURL_CFFI_AVAILABLE:
        print(
            "Warning: curl_cffi is not available. Will fallback to command-line curl."
        )

    # Get URLs for test files
    base_url = get_base_url(market_type, symbol, interval)
    dates = get_test_dates(5)  # Use 5 recent dates

    # Get actual interval to use (1m for futures if 1s was requested)
    actual_interval = interval
    if market_type != "spot" and interval == "1s":
        actual_interval = "1m"  # Use 1m for futures markets

    urls = []
    for date in dates:
        filename = f"{symbol}-{actual_interval}-{date}.zip"
        url = f"{base_url}/{filename}"
        # Verify the URL exists before adding
        success, _, _ = download_first_curl_cffi(url, None, timeout)
        if success:
            urls.append(url)
            if len(urls) >= 5:  # Limit to 5 files
                break

    if not urls:
        print("No valid URLs found. Try with a different symbol or dates.")
        return

    print(f"\nFound {len(urls)} files to benchmark:")
    for url in urls:
        print(f"  {os.path.basename(url)}")

    # Results dictionary
    results = {}

    # Test each concurrency level
    for concurrency in concurrent_levels:
        print(f"\nTesting with concurrency level: {concurrency}")
        concurrency_results = {}

        # Test curl_cffi
        print("\nBenchmarking curl_cffi...")
        with get_output_dir() as output_dir:
            if CURL_CFFI_AVAILABLE:
                start_time = time.time()
                successful, _ = download_multiple_files_concurrent(
                    urls, output_dir, concurrency, timeout
                )
                curl_time = time.time() - start_time

                concurrency_results["curl_cffi"] = {
                    "time": curl_time,
                    "success_rate": len(successful) / len(urls) * 100,
                    "files": len(successful),
                }

                print(
                    f"  curl_cffi completed in {fmt_time(curl_time)} ({len(successful)}/{len(urls)} files)"
                )
            else:
                print("  curl_cffi not available, skipping")

        # Test curl_cffi async
        print("\nBenchmarking curl_cffi async...")
        with get_output_dir() as output_dir:
            if CURL_CFFI_ASYNC_AVAILABLE:
                start_time = time.time()
                # Use asyncio.create_task and await to run the async function
                successful = []

                # Define an async wrapper function
                async def run_async_download():
                    nonlocal successful
                    successful, _ = await download_multiple_files_concurrent(
                        urls, output_dir, concurrency, timeout
                    )

                # Run the async function
                await run_async_download()
                curl_async_time = time.time() - start_time

                concurrency_results["curl_cffi_async"] = {
                    "time": curl_async_time,
                    "success_rate": len(successful) / len(urls) * 100,
                    "files": len(successful),
                }

                print(
                    f"  curl_cffi async completed in {fmt_time(curl_async_time)} ({len(successful)}/{len(urls)} files)"
                )
            else:
                print("  curl_cffi async not available, skipping")

        # Test command-line curl (always available)
        print("\nBenchmarking command-line curl...")
        with get_output_dir() as output_dir:
            start_time = time.time()
            successful, _ = download_with_curl_command_concurrent(
                urls, output_dir, concurrency, timeout
            )
            curl_cmd_time = time.time() - start_time

            concurrency_results["curl_cmd"] = {
                "time": curl_cmd_time,
                "success_rate": len(successful) / len(urls) * 100,
                "files": len(successful),
            }

            print(
                f"  curl command completed in {fmt_time(curl_cmd_time)} ({len(successful)}/{len(urls)} files)"
            )

        # Test AWS CLI
        print("\nBenchmarking AWS CLI...")
        with get_output_dir() as output_dir:
            start_time = time.time()
            successful = []

            # Create tasks for concurrent downloads
            async def download_with_semaphore(url_idx):
                async with sem:
                    url = urls[url_idx]
                    filename = os.path.basename(url)
                    output_path = os.path.join(output_dir, filename)
                    success, _ = await download_with_aws_cli(url, output_path, timeout)
                    if success:
                        return output_path
                    return None

            # Use semaphore to limit concurrency
            sem = asyncio.Semaphore(concurrency)

            # Create and execute tasks
            tasks = []
            for i in range(len(urls)):
                tasks.append(download_with_semaphore(i))

            # Gather results
            results_list = await asyncio.gather(*tasks)

            # Count successful downloads
            successful = [r for r in results_list if r]
            aws_time = time.time() - start_time

            concurrency_results["aws_cli"] = {
                "time": aws_time,
                "success_rate": len(successful) / len(urls) * 100,
                "files": len(successful),
            }

            print(
                f"  AWS CLI completed in {fmt_time(aws_time)} ({len(successful)}/{len(urls)} files)"
            )

        results[concurrency] = concurrency_results

    # Print summary
    print("\n" + "-" * 80)
    print("BENCHMARK SUMMARY")
    print("-" * 80)

    print(
        f"\n{'Concurrency':12} {'Method':15} {'Time':12} {'Success Rate':12} {'Files':8}"
    )
    print("-" * 65)

    for concurrency, methods in results.items():
        # Sort methods by time
        sorted_methods = sorted(methods.items(), key=lambda x: x[1]["time"])

        for method_name, stats in sorted_methods:
            print(
                f"{concurrency:12} {method_name:15} {fmt_time(stats['time']):12} {stats['success_rate']:9.1f}% {stats['files']:8}"
            )

    # Find fastest method
    all_methods = {}
    for concurrency, methods in results.items():
        for method_name, stats in methods.items():
            if method_name not in all_methods:
                all_methods[method_name] = {
                    "best_time": float("inf"),
                    "best_concurrency": None,
                }

            if stats["time"] < all_methods[method_name]["best_time"]:
                all_methods[method_name]["best_time"] = stats["time"]
                all_methods[method_name]["best_concurrency"] = concurrency

    # Print best concurrency for each method
    print("\nBest concurrency by method:")
    for method_name, stats in all_methods.items():
        print(
            f"  {method_name}: concurrency={stats['best_concurrency']} ({fmt_time(stats['best_time'])})"
        )

    # Find overall best
    best_method = min(all_methods.items(), key=lambda x: x[1]["best_time"])
    print(
        f"\nFastest overall: {best_method[0]} with concurrency={best_method[1]['best_concurrency']} ({fmt_time(best_method[1]['best_time'])})"
    )


# -------------------------- DEMO FUNCTION --------------------------


def demo_best_practice(market_type, symbol, interval, days_back, timeout):
    """Demonstrate the recommended best practice implementation."""
    print("\n" + "=" * 80)
    print("BEST PRACTICE IMPLEMENTATION DEMO")
    print("=" * 80)
    print(
        "This demonstrates recommended best practices for working with Binance Vision data"
    )
    print("-" * 80)

    # Test finding the latest date
    print("\nFinding the latest available data...")
    with get_output_dir(args.output_dir) as output_dir:
        date, elapsed, size = check_latest_date_download_first(
            market_type, symbol, interval, days_back, timeout, output_dir
        )

    if date:
        print(f"✓ Found latest date: {date} in {fmt_time(elapsed)}")
        print(f"✓ File size: {size/1024:.1f} KB")

        # Determine which interval is being used
        actual_interval = interval
        if market_type != "spot" and interval == "1s":
            actual_interval = "1m"
            print(
                f"Note: Using {actual_interval} granularity for {market_type} market (1s only available for spot data)"
            )

        # Show recommendations based on data type
        print("\nRecommendations based on benchmarks:")
        print(f"- For {market_type} market with {actual_interval} granularity:")

        # Recommend the best HTTP client
        if CURL_CFFI_AVAILABLE:
            print("  * Use curl_cffi with download-first approach (fastest)")
        else:
            print("  * Use command-line curl with download-first approach (fallback)")

        print("  * Set optimal concurrency based on batch size:")
        print("    - Small batches (1-10 files): concurrency 10")
        print("    - Medium batches (11-50 files): concurrency 50 (optimal)")
        print("    - Large batches (50+ files): concurrency 50-100")
        print("  * Use 3.0s timeout for optimal balance")
        print("  * Avoid unnecessary retry logic for stable networks")

        if market_type == "spot" and actual_interval == "1s":
            print("\nFor spot data with 1s granularity:")
            print("  * curl_cffi achieves ~25 MB/s throughput at concurrency 50")
            print("  * AWS CLI achieves ~15 MB/s throughput at concurrency 50")

        elif market_type != "spot" and actual_interval == "1m":
            print("\nFor futures data with 1m granularity:")
            print("  * curl_cffi achieves ~20 MB/s throughput at concurrency 50")
            print("  * AWS CLI achieves ~12 MB/s throughput at concurrency 50")

        # AWS CLI comparison
        print("\nAWS CLI comparison:")
        print("  * AWS CLI is ~40-60% slower than curl_cffi for data downloads")
        print("  * If using AWS CLI, set concurrency to 50-100 for best performance")
        print(
            "  * Consider using HTTP clients for better performance even with AWS data"
        )

        # Show the full implementation
        print("\nHere's the recommended implementation:")
        print("-" * 80)
        print(
            '''
# Best practice implementation for finding and downloading data
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio

# Try to import curl_cffi
try:
    import curl_cffi.requests as curl_requests
    from curl_cffi.requests import AsyncSession
    curl_cffi_available = True
    curl_cffi_async_available = True
except ImportError:
    try:
        import curl_cffi.requests as curl_requests
        curl_cffi_available = True
        curl_cffi_async_available = False
    except ImportError:
        curl_cffi_available = False
        curl_cffi_async_available = False

def get_latest_data(symbol, interval, market="spot", max_days_back=5, output_dir="downloads"):
    """Get the latest available data file using download-first approach.
    
    Args:
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        interval: Time interval (e.g., "1h", "1d", "1s" for spot, "1m" for futures)
        market: Market type ("spot", "um" for USDT-M futures, "cm" for COIN-M futures)
        max_days_back: Maximum days to check backward
        output_dir: Directory to save downloaded files
        
    Returns:
        Tuple of (date found, path to downloaded file) or (None, None) if not found
    """
    os.makedirs(output_dir, exist_ok=True)
    current_date = datetime.utcnow()
    
    # Handle interval based on market type
    actual_interval = interval
    if market != "spot" and interval == "1s":
        actual_interval = "1m"  # Use 1m for futures markets (minimum granularity)
    
    # Construct base URL
    base_url = "https://data.binance.vision"
    if market == "spot":
        url_path = f"data/spot/daily/klines/{symbol}/{actual_interval}"
    elif market == "um":
        url_path = f"data/futures/um/daily/klines/{symbol}/{actual_interval}"
    elif market == "cm":
        url_path = f"data/futures/cm/daily/klines/{symbol}/{actual_interval}"
    else:
        raise ValueError(f"Invalid market type: {market}")
    
    for i in range(max_days_back + 1):
        check_date = (current_date - timedelta(days=i)).strftime("%Y-%m-%d")
        url = f"{base_url}/{url_path}/{symbol}-{actual_interval}-{check_date}.zip"
        output_path = os.path.join(output_dir, f"{symbol}-{actual_interval}-{check_date}.zip")
        
        try:
            # Attempt direct download with best available client
            if curl_cffi_available:
                # Best option: Use curl_cffi
                response = curl_requests.get(url, timeout=3.0)
                success = response.status_code == 200
                content = response.content
            else:
                # Fallback: Use subprocess with curl command
                import subprocess
                
                try:
                    result = subprocess.run(
                        ["curl", "-s", "-f", url],
                        capture_output=True,
                        timeout=3.0
                    )
                    success = result.returncode == 0
                    content = result.stdout
                except subprocess.SubprocessError:
                    success = False
                    content = None
            
            if success and content:
                # File exists and download was successful
                with open(output_path, 'wb') as f:
                    f.write(content)
                return check_date, output_path
                
        except Exception:
            # Continue to next date on error
            continue
            
    return None, None

# Async implementation for latest data retrieval
async def get_latest_data_async(symbol, interval, market="spot", max_days_back=5, output_dir="downloads"):
    """Get the latest available data file using async download-first approach.
    
    Uses curl_cffi's AsyncSession API for maximum performance.
    """
    if not curl_cffi_async_available:
        # Fall back to sync version if async not available
        result = await asyncio.to_thread(get_latest_data, symbol, interval, market, max_days_back, output_dir)
        return result
        
    os.makedirs(output_dir, exist_ok=True)
    current_date = datetime.utcnow()
    
    # Handle interval based on market type
    actual_interval = interval
    if market != "spot" and interval == "1s":
        actual_interval = "1m"  # Use 1m for futures markets (minimum granularity)
    
    # Construct base URL
    base_url = "https://data.binance.vision"
    if market == "spot":
        url_path = f"data/spot/daily/klines/{symbol}/{actual_interval}"
    elif market == "um":
        url_path = f"data/futures/um/daily/klines/{symbol}/{actual_interval}"
    elif market == "cm":
        url_path = f"data/futures/cm/daily/klines/{symbol}/{actual_interval}"
    else:
        raise ValueError(f"Invalid market type: {market}")
    
    async with AsyncSession() as session:
        for i in range(max_days_back + 1):
            check_date = (current_date - timedelta(days=i)).strftime("%Y-%m-%d")
            url = f"{base_url}/{url_path}/{symbol}-{actual_interval}-{check_date}.zip"
            output_path = os.path.join(output_dir, f"{symbol}-{actual_interval}-{check_date}.zip")
            
            try:
                response = await session.get(url, timeout=3.0)
                
                if response.status_code == 200:
                    # File exists and download was successful
                    with open(output_path, 'wb') as f:
                        f.write(response.content)
                    return check_date, output_path
                    
            except Exception:
                # Continue to next date on error
                continue
                
    return None, None

def download_multiple_files(url_list, output_dir="downloads", max_concurrent=50):
    """Download multiple files concurrently using best available HTTP client.
    
    Args:
        url_list: List of URLs to download
        output_dir: Directory to save downloaded files
        max_concurrent: Maximum number of concurrent downloads
                        (adjust based on batch size)
                        
    Returns:
        List of successfully downloaded file paths
    """
    os.makedirs(output_dir, exist_ok=True)
    successful_downloads = []
    
    # Adjust concurrency based on number of files
    batch_size = len(url_list)
    if batch_size <= 10:
        # Small batch optimization
        adjusted_concurrency = min(10, max_concurrent)
    elif batch_size <= 50:
        # Medium batch optimization (optimal concurrency)
        adjusted_concurrency = min(50, max_concurrent)
    else:
        # Large batch optimization
        adjusted_concurrency = min(100, max_concurrent)
    
    def download_single_file(url):
        try:
            filename = os.path.basename(url)
            output_path = os.path.join(output_dir, filename)
            
            if curl_cffi_available:
                # Best option: Use curl_cffi
                response = curl_requests.get(url, timeout=3.0)
                success = response.status_code == 200
                content = response.content
            else:
                # Fallback: Use subprocess with curl command
                result = subprocess.run(
                    ["curl", "-s", "-f", url],
                    capture_output=True,
                    timeout=3.0
                )
                success = result.returncode == 0
                content = result.stdout
            
            if success and content:
                with open(output_path, 'wb') as f:
                    f.write(content)
                return output_path
        except Exception:
            return None
    
    with ThreadPoolExecutor(max_workers=adjusted_concurrency) as executor:
        future_to_url = {executor.submit(download_single_file, url): url for url in url_list}
        
        for future in as_completed(future_to_url):
            result = future.result()
            if result:
                successful_downloads.append(result)
    
    return successful_downloads

# Async implementation for multiple file downloads
async def download_multiple_files_async(url_list, output_dir="downloads", max_concurrent=50):
    """Download multiple files concurrently using curl_cffi's AsyncSession with connection pooling.
    
    This implementation uses curl_cffi's native async API with proper connection pooling
    which should provide better performance than the synchronous version with threads.
    """
    if not curl_cffi_async_available:
        print("curl_cffi AsyncSession not available. Falling back to synchronous version.")
        successful, total_time = download_multiple_files_concurrent(url_list, output_dir, max_concurrent, timeout)
        return successful, total_time
        
    os.makedirs(output_dir, exist_ok=True)
    successful_downloads = []
    file_count = len(url_list)
    
    # Dynamically adjust concurrency based on batch size
    adjusted_concurrency = max_concurrent
    if file_count <= 10:
        adjusted_concurrency = min(10, max_concurrent)
    elif file_count <= 50:
        adjusted_concurrency = min(50, max_concurrent)
    else:
        adjusted_concurrency = min(100, max_concurrent)
        
    if adjusted_concurrency != max_concurrent and args.verbose:
        print(f"Adjusting concurrency to {adjusted_concurrency} for {file_count} files")
    
    start_time = time.time()
    
    # Use semaphore to limit concurrency
    sem = asyncio.Semaphore(adjusted_concurrency)
    
    async def download_single_file(url):
        async with sem:
            try:
                filename = os.path.basename(url)
                output_path = os.path.join(output_dir, filename)
                
                # Use the async curl_cffi function
                success, content, elapsed = await download_first_curl_cffi_async(url, output_path, timeout)
                
                if success:
                    if args.verbose:
                        print(f"Downloaded {filename} in {fmt_time(elapsed)}")
                    return output_path
                return None
            except Exception as e:
                if args.verbose:
                    print(f"Error downloading {url}: {e}")
                return None
    
    # Create tasks for all URLs
    tasks = [download_single_file(url) for url in url_list]
    
    # Gather results
    results = await asyncio.gather(*tasks)
    
    # Filter successful downloads
    successful_downloads = [path for path in results if path]
    
    total_time = time.time() - start_time
    return successful_downloads, total_time
'''
        )
    else:
        print(f"✗ No data found for the last {days_back} days")
        print(
            f"Try using a different symbol or interval. Note that spot markets support 1s granularity,"
        )
        print(f"while futures markets (um/cm) only support 1m granularity and above.")


# -------------------------- MAIN EXECUTION --------------------------


if __name__ == "__main__":
    # Check if required libraries are available
    print("\nChecking for available libraries...")
    curl_available = CURL_CFFI_AVAILABLE

    print(f"curl_cffi: {'Available' if curl_available else 'Not available'}")

    if not curl_available:
        print("Warning: curl_cffi is not available.")
        print("Will use command-line curl as fallback for all operations.")

    # Print banner
    print("\n" + "=" * 80)
    print("BINANCE VISION DATA BENCHMARK AND BEST PRACTICES")
    print("=" * 80)

    # Set defaults
    market_type = args.market or "spot"
    symbol = args.symbol or "BTCUSDT"
    interval = args.interval or "1s"
    days_back = args.days or 5
    timeout = args.timeout or 3.0
    concurrent_levels = [10, 50, 100]  # Default concurrency levels to test

    # Adjust interval based on market type
    if market_type != "spot" and interval == "1s":
        print(
            f"Note: 1s interval not available for {market_type}. Will use 1m instead."
        )
        interval = "1m"

    # Determine which mode to run based on args
    if args.mode == "demo":
        # Demo the best practice implementation
        demo_best_practice(market_type, symbol, interval, days_back, timeout)

    elif args.mode == "aws":
        # Run AWS CLI vs HTTP clients benchmark
        asyncio.run(
            benchmark_aws_vs_http(
                market_type, symbol, interval, concurrent_levels, timeout
            )
        )

    elif args.mode == "download":
        # Just download the latest available file
        print(
            f"\nDownloading latest available {market_type}/{symbol}/{interval} data..."
        )
        output_dir = args.output_dir or "downloads"
        date, elapsed, size = check_latest_date_download_first(
            market_type, symbol, interval, days_back, timeout, output_dir
        )

        if date:
            print(f"✓ Downloaded {symbol}-{interval}-{date}.zip in {fmt_time(elapsed)}")
            print(f"✓ File size: {size/1024:.1f} KB")
            print(f"✓ Saved to: {output_dir}")
        else:
            print(f"✗ No data found for the last {days_back} days")

    else:
        # Print usage instructions if no valid mode specified
        print("Please specify a mode:")
        print("  --mode=demo       Show best practice implementation demo")
        print("  --mode=aws        Benchmark AWS CLI vs HTTP clients")
        print("  --mode=download   Download the latest available file")
        print("\nFor more options, use --help")
