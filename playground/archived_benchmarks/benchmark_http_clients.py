#!/usr/bin/env python3
import asyncio
import time
import argparse
import statistics
from datetime import datetime, timedelta
import functools
import warnings

# Suppress specific CURL warnings
warnings.filterwarnings("ignore", message=".*SSLKEYLOGFILE.*")

# Import HTTP clients for comparison
import httpx
import aiohttp

try:
    import tls_client
    import curl_cffi.requests as curl_requests
    from functools import wraps, partial
except ImportError:
    print(
        "NOTE: tls_client and/or curl_cffi are not installed. Run: pip install tls-client curl-cffi"
    )
    tls_client = None
    curl_requests = None

# Command-line arguments
parser = argparse.ArgumentParser(
    description="Benchmark HTTP clients for Binance Vision data"
)
parser.add_argument(
    "--market",
    default="spot",
    choices=["spot", "um", "cm"],
    help="Market type: spot, um (USDT-Margined Futures), or cm (Coin-Margined Futures)",
)
parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair symbol")
parser.add_argument(
    "--interval",
    default="1m",
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
    help="Time interval",
)
parser.add_argument("--days-back", type=int, default=1, help="Days back to check")
parser.add_argument(
    "--iterations", type=int, default=10, help="Number of iterations for each client"
)
parser.add_argument(
    "--timeout", type=float, default=5.0, help="Request timeout in seconds"
)
parser.add_argument("--url", help="Direct URL to test (optional)")
parser.add_argument("--verbose", action="store_true", help="Show detailed output")
parser.add_argument(
    "--method",
    default="head",
    choices=["head", "get", "both"],
    help="HTTP method to test (head, get, or both)",
)
parser.add_argument(
    "--clients",
    nargs="+",
    default=["httpx", "aiohttp", "tls_client", "curl_cffi"],
    help="HTTP clients to benchmark",
)
args = parser.parse_args()


def get_base_url(market_type, symbol):
    """Get the base URL for data download based on market type."""
    if market_type == "spot":
        return f"https://data.binance.vision/data/spot/daily/klines/{symbol}"
    elif market_type == "um":
        return f"https://data.binance.vision/data/futures/um/daily/klines/{symbol}"
    elif market_type == "cm":
        return f"https://data.binance.vision/data/futures/cm/daily/klines/{symbol}"
    else:
        raise ValueError(f"Invalid market type: {market_type}")


def get_test_url():
    """Get URL for testing based on command line arguments."""
    if args.url:
        return args.url

    # Calculate date to check
    check_date = (datetime.utcnow() - timedelta(days=args.days_back)).strftime(
        "%Y-%m-%d"
    )
    base_url = get_base_url(args.market, args.symbol)
    return f"{base_url}/{args.interval}/{args.symbol}-{args.interval}-{check_date}.zip"


# Wrap synchronous functions to make them async-compatible
def async_wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


# HTTPX implementation - HEAD
async def check_with_httpx_head(url, timeout):
    """Check URL availability using HTTPX with HEAD method."""
    start_time = time.time()
    success = False
    status_code = None

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.head(url, follow_redirects=True)
            status_code = response.status_code
            success = status_code == 200
    except Exception as e:
        if args.verbose:
            print(f"HTTPX HEAD Error: {e}")

    elapsed = time.time() - start_time
    return success, status_code, elapsed


# HTTPX implementation - GET
async def check_with_httpx_get(url, timeout):
    """Check URL availability using HTTPX with GET method."""
    start_time = time.time()
    success = False
    status_code = None

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            # Stream to avoid downloading the entire file
            response = await client.get(url, follow_redirects=True)
            status_code = response.status_code
            success = status_code == 200
            # Immediately close to avoid downloading the entire file
            response.close()
    except Exception as e:
        if args.verbose:
            print(f"HTTPX GET Error: {e}")

    elapsed = time.time() - start_time
    return success, status_code, elapsed


# AIOHTTP implementation - HEAD
async def check_with_aiohttp_head(url, timeout):
    """Check URL availability using AIOHTTP with HEAD method."""
    start_time = time.time()
    success = False
    status_code = None

    try:
        # Configure timeout
        timeout_obj = aiohttp.ClientTimeout(total=timeout)

        async with aiohttp.ClientSession(timeout=timeout_obj) as session:
            async with session.head(url, allow_redirects=True) as response:
                status_code = response.status
                success = status_code == 200
    except Exception as e:
        if args.verbose:
            print(f"AIOHTTP HEAD Error: {e}")

    elapsed = time.time() - start_time
    return success, status_code, elapsed


# AIOHTTP implementation - GET
async def check_with_aiohttp_get(url, timeout):
    """Check URL availability using AIOHTTP with GET method."""
    start_time = time.time()
    success = False
    status_code = None

    try:
        # Configure timeout
        timeout_obj = aiohttp.ClientTimeout(total=timeout)

        async with aiohttp.ClientSession(timeout=timeout_obj) as session:
            async with session.get(url, allow_redirects=True) as response:
                status_code = response.status
                success = status_code == 200
                # Read a small chunk to ensure connection is established
                await response.read(10)
    except Exception as e:
        if args.verbose:
            print(f"AIOHTTP GET Error: {e}")

    elapsed = time.time() - start_time
    return success, status_code, elapsed


# TLS-CLIENT implementation - HEAD
async def check_with_tls_client_head(url, timeout):
    """Check URL availability using TLS-CLIENT with HEAD method."""
    if tls_client is None:
        return False, None, 0

    start_time = time.time()
    success = False
    status_code = None

    try:
        # Create a TLS client session
        session = tls_client.Session(
            client_identifier="chrome_110", random_tls_extension_order=True
        )

        # Use async wrapper for the synchronous method
        head_async = async_wrap(session.head)

        # Set timeout
        response = await head_async(url, timeout=timeout, allow_redirects=True)
        status_code = response.status_code
        success = status_code == 200

    except Exception as e:
        if args.verbose:
            print(f"TLS-CLIENT HEAD Error: {e}")

    elapsed = time.time() - start_time
    return success, status_code, elapsed


# TLS-CLIENT implementation - GET
async def check_with_tls_client_get(url, timeout):
    """Check URL availability using TLS-CLIENT with GET method."""
    if tls_client is None:
        return False, None, 0

    start_time = time.time()
    success = False
    status_code = None

    try:
        # Create a TLS client session
        session = tls_client.Session(
            client_identifier="chrome_110", random_tls_extension_order=True
        )

        # Use async wrapper for the synchronous method
        get_async = async_wrap(session.get)

        # Set timeout and stream to avoid downloading the entire file
        response = await get_async(url, timeout=timeout, allow_redirects=True)
        status_code = response.status_code
        success = status_code == 200

    except Exception as e:
        if args.verbose:
            print(f"TLS-CLIENT GET Error: {e}")

    elapsed = time.time() - start_time
    return success, status_code, elapsed


# CURL-CFFI implementation - HEAD
async def check_with_curl_cffi_head(url, timeout):
    """Check URL availability using CURL-CFFI with HEAD method."""
    if curl_requests is None:
        return False, None, 0

    start_time = time.time()
    success = False
    status_code = None

    try:
        # Need to use async client session for asynchronous requests
        async with curl_requests.AsyncSession() as session:
            response = await session.head(url, timeout=timeout, follow_redirects=True)
            status_code = response.status_code
            success = status_code == 200
    except Exception as e:
        if args.verbose:
            print(f"CURL-CFFI HEAD Error: {e}")

    elapsed = time.time() - start_time
    return success, status_code, elapsed


# CURL-CFFI implementation - GET
async def check_with_curl_cffi_get(url, timeout):
    """Check URL availability using CURL-CFFI with GET method."""
    if curl_requests is None:
        return False, None, 0

    start_time = time.time()
    success = False
    status_code = None

    try:
        # Need to use async client session for asynchronous requests
        async with curl_requests.AsyncSession() as session:
            response = await session.get(url, timeout=timeout, follow_redirects=True)
            status_code = response.status_code
            success = status_code == 200
    except Exception as e:
        if args.verbose:
            print(f"CURL-CFFI GET Error: {e}")

    elapsed = time.time() - start_time
    return success, status_code, elapsed


# Test functions to run the benchmark
async def run_benchmark(client_type, method, url, iterations, timeout):
    """Run benchmark for a specific client and method."""
    results = []

    # Select the right function to call
    check_function = None
    if client_type == "httpx":
        if method == "head":
            check_function = check_with_httpx_head
        else:  # method == "get"
            check_function = check_with_httpx_get
    elif client_type == "aiohttp":
        if method == "head":
            check_function = check_with_aiohttp_head
        else:  # method == "get"
            check_function = check_with_aiohttp_get
    elif client_type == "tls_client":
        if method == "head":
            check_function = check_with_tls_client_head
        else:  # method == "get"
            check_function = check_with_tls_client_get
    elif client_type == "curl_cffi":
        if method == "head":
            check_function = check_with_curl_cffi_head
        else:  # method == "get"
            check_function = check_with_curl_cffi_get

    if check_function is None:
        print(f"Client {client_type} is not available")
        return results

    for i in range(iterations):
        success, status_code, elapsed = await check_function(url, timeout)
        results.append(elapsed)

        if args.verbose:
            print(
                f"{client_type.upper()} {method.upper()} Iteration {i+1}/{iterations}: {elapsed:.4f}s (Status: {status_code})"
            )

        # Small delay between requests to avoid rate limiting
        await asyncio.sleep(0.1)

    return results


# Helper function to calculate and print statistics
def print_statistics(name, times):
    """Calculate and print statistics for the benchmark results."""
    if not times:
        print(f"{name}: No successful results")
        return

    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)
    median_time = statistics.median(times)
    stdev = statistics.stdev(times) if len(times) > 1 else 0

    print(f"\n{name} Statistics:")
    print(f"  Iterations:  {len(times)}")
    print(f"  Average:     {avg_time:.4f}s")
    print(f"  Minimum:     {min_time:.4f}s")
    print(f"  Maximum:     {max_time:.4f}s")
    print(f"  Median:      {median_time:.4f}s")
    print(f"  Std Dev:     {stdev:.4f}s")

    return {
        "avg": avg_time,
        "min": min_time,
        "max": max_time,
        "median": median_time,
        "stdev": stdev,
    }


# Compare results and print summary
def compare_results(client_stats):
    """Compare results between multiple clients and print summary."""
    if not client_stats:
        return

    # Find the fastest client
    fastest_client = min(
        client_stats.items(), key=lambda x: x[1]["avg"] if x[1] else float("inf")
    )
    fastest_name, fastest_stats = fastest_client

    if not fastest_stats:
        return

    print("\nPERFORMANCE COMPARISON:")
    print(f"Fastest client: {fastest_name} (Average: {fastest_stats['avg']:.4f}s)")

    # Compare each client to the fastest
    for name, stats in client_stats.items():
        if name != fastest_name and stats:
            diff_percent = ((stats["avg"] - fastest_stats["avg"]) / stats["avg"]) * 100
            print(f"{fastest_name} is faster than {name} by {diff_percent:.2f}%")


async def main():
    url = get_test_url()
    print(f"Benchmarking URL: {url}")
    print(f"HTTP Method(s): {args.method.upper()}")
    print(f"Clients to test: {', '.join(args.clients)}")
    print(f"Iterations: {args.iterations}")
    print(f"Timeout: {args.timeout}s")
    print("-" * 50)

    # Check if libraries are available
    clients_to_test = []
    for client in args.clients:
        if client == "tls_client" and tls_client is None:
            print("Warning: tls_client library is not installed, skipping")
        elif client == "curl_cffi" and curl_requests is None:
            print("Warning: curl_cffi library is not installed, skipping")
        else:
            clients_to_test.append(client)

    # Determine which methods to benchmark
    methods_to_test = []
    if args.method == "both":
        methods_to_test = ["head", "get"]
    else:
        methods_to_test = [args.method]

    results = {}

    # Run benchmarks for each method
    for method in methods_to_test:
        print(f"\n=== Testing {method.upper()} Method ===")

        method_results = {}

        # Run benchmarks for each client
        for client in clients_to_test:
            print(f"\nRunning {client.upper()} {method.upper()} benchmark...")
            client_times = await run_benchmark(
                client, method, url, args.iterations, args.timeout
            )
            method_results[client] = client_times

        # Store results for this method
        results[method] = method_results

    # Calculate and print stats for each method
    print("\n" + "=" * 50)
    print("BENCHMARK RESULTS")
    print("=" * 50)

    for method in methods_to_test:
        print(f"\n--- {method.upper()} Method Results ---")

        # Calculate statistics for each client
        client_stats = {}
        for client in clients_to_test:
            if client in results[method]:
                client_stat = print_statistics(
                    f"{client.upper()} {method.upper()}", results[method][client]
                )
                client_stats[f"{client.upper()} {method.upper()}"] = client_stat

        # Compare results for this method
        compare_results(client_stats)

    # Overall summary if testing both methods
    if args.method == "both":
        print("\n" + "=" * 50)
        print("OVERALL SUMMARY")
        print("=" * 50)

        # Combine results for all methods
        overall_results = {}
        for client in clients_to_test:
            all_times = []
            for method in methods_to_test:
                if client in results[method]:
                    all_times.extend(results[method][client])
            overall_results[client] = all_times

        # Calculate overall statistics
        overall_stats = {}
        for client in clients_to_test:
            if overall_results[client]:
                client_stat = print_statistics(
                    f"{client.upper()} OVERALL", overall_results[client]
                )
                overall_stats[f"{client.upper()} OVERALL"] = client_stat

        # Compare overall results
        compare_results(overall_stats)


if __name__ == "__main__":
    asyncio.run(main())
