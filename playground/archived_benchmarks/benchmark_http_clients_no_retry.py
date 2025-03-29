#!/usr/bin/env python3
import asyncio
import time
import argparse
import statistics
from datetime import datetime, timedelta
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
parser = argparse.ArgumentParser(description="Benchmark HTTP clients (no retry)")
parser.add_argument(
    "--url",
    default="https://data.binance.vision/data/spot/daily/klines/BTCUSDT/4h/BTCUSDT-4h-2025-03-28.zip",
    help="URL to test (default: a Binance data URL)",
)
parser.add_argument(
    "--iterations", type=int, default=20, help="Number of iterations for each client"
)
parser.add_argument(
    "--timeout", type=float, default=2.0, help="Request timeout in seconds"
)
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
parser.add_argument("--verbose", action="store_true", help="Show detailed output")
args = parser.parse_args()


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

        # TLS client doesn't support timeout parameter
        response = await head_async(url, allow_redirects=True)
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

        # TLS client doesn't support timeout parameter
        response = await get_async(url, allow_redirects=True)
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
            response = await session.head(url, timeout=timeout)
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
            response = await session.get(url, timeout=timeout)
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
    success_count = 0

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
        return results, 0

    for i in range(iterations):
        success, status_code, elapsed = await check_function(url, timeout)
        results.append(elapsed)
        if success:
            success_count += 1

        if args.verbose:
            print(
                f"{client_type.upper()} {method.upper()} Iteration {i+1}/{iterations}: {elapsed:.4f}s (Status: {status_code})"
            )

        # Small delay between requests to avoid rate limiting
        await asyncio.sleep(0.1)

    return results, success_count


# Helper function to calculate and print statistics
def print_statistics(name, times, success_rate):
    """Calculate and print statistics for the benchmark results."""
    if not times:
        print(f"{name}: No successful results")
        return None

    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)
    median_time = statistics.median(times)
    stdev = statistics.stdev(times) if len(times) > 1 else 0

    print(f"\n{name} Statistics:")
    print(f"  Iterations:   {len(times)}")
    print(f"  Success Rate: {success_rate}%")
    print(f"  Average:      {avg_time:.4f}s")
    print(f"  Minimum:      {min_time:.4f}s")
    print(f"  Maximum:      {max_time:.4f}s")
    print(f"  Median:       {median_time:.4f}s")
    print(f"  Std Dev:      {stdev:.4f}s")

    return {
        "avg": avg_time,
        "min": min_time,
        "max": max_time,
        "median": median_time,
        "stdev": stdev,
        "success_rate": success_rate,
    }


# Compare results and print summary
def compare_results(client_stats):
    """Compare results between multiple clients and print summary."""
    if not client_stats:
        return

    # Find the fastest client with 100% success rate
    successful_clients = {
        name: stats
        for name, stats in client_stats.items()
        if stats and stats["success_rate"] == 100
    }

    if successful_clients:
        fastest_client = min(successful_clients.items(), key=lambda x: x[1]["avg"])
    else:
        # If no 100% success rate clients, just find the fastest
        fastest_client = min(
            client_stats.items(), key=lambda x: x[1]["avg"] if x[1] else float("inf")
        )

    fastest_name, fastest_stats = fastest_client

    if not fastest_stats:
        return

    print("\nPERFORMANCE COMPARISON:")
    print(
        f"Fastest client: {fastest_name} (Average: {fastest_stats['avg']:.4f}s, Success: {fastest_stats['success_rate']}%)"
    )

    # Compare each client to the fastest
    for name, stats in client_stats.items():
        if name != fastest_name and stats:
            diff_percent = ((stats["avg"] - fastest_stats["avg"]) / stats["avg"]) * 100
            print(f"{fastest_name} is faster than {name} by {diff_percent:.2f}%")


async def main():
    url = args.url
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
    success_rates = {}

    # Run benchmarks for each method
    for method in methods_to_test:
        print(f"\n=== Testing {method.upper()} Method ===")

        method_results = {}
        method_success_rates = {}

        # Run benchmarks for each client
        for client in clients_to_test:
            print(f"\nRunning {client.upper()} {method.upper()} benchmark...")
            client_times, success_count = await run_benchmark(
                client, method, url, args.iterations, args.timeout
            )
            method_results[client] = client_times
            method_success_rates[client] = (success_count / args.iterations) * 100

        # Store results for this method
        results[method] = method_results
        success_rates[method] = method_success_rates

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
                    f"{client.upper()} {method.upper()}",
                    results[method][client],
                    success_rates[method][client],
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
        overall_success_rates = {}
        for client in clients_to_test:
            all_times = []
            total_success = 0
            total_requests = 0
            for method in methods_to_test:
                if client in results[method]:
                    all_times.extend(results[method][client])
                    success_rate = success_rates[method][client]
                    method_success = int(success_rate * args.iterations / 100)
                    total_success += method_success
                    total_requests += args.iterations

            overall_results[client] = all_times
            if total_requests > 0:
                overall_success_rates[client] = (total_success / total_requests) * 100
            else:
                overall_success_rates[client] = 0

        # Calculate overall statistics
        overall_stats = {}
        for client in clients_to_test:
            if overall_results[client]:
                client_stat = print_statistics(
                    f"{client.upper()} OVERALL",
                    overall_results[client],
                    overall_success_rates[client],
                )
                overall_stats[f"{client.upper()} OVERALL"] = client_stat

        # Compare overall results
        compare_results(overall_stats)


if __name__ == "__main__":
    asyncio.run(main())
