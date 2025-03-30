#!/usr/bin/env python
"""Simple benchmark script to verify Vision API performance improvements.

This script measures the download performance of the VisionDataClient
with the optimized settings and compares it with baseline metrics.
"""

import asyncio
import time
from datetime import datetime, timedelta
import os
from pathlib import Path

from core.vision_data_client import VisionDataClient
from utils.time_utils import enforce_utc_timezone


async def benchmark_vision_download(
    symbol="BTCUSDT",
    interval="1s",
    days_back=3,
    download_count=5,
):
    """Benchmark the VisionDataClient download performance.

    Args:
        symbol: Symbol to download data for
        interval: Time interval to download
        days_back: How many days back to fetch data
        download_count: Number of times to repeat the download for averaging
    """
    print(f"Benchmarking VisionDataClient with {symbol} {interval} data")
    print(f"Testing {download_count} downloads for averaging")

    # Calculate the date range - using a small window for benchmarking
    end_time = enforce_utc_timezone(datetime.utcnow() - timedelta(days=days_back))
    start_time = end_time - timedelta(hours=6)  # 6-hour window

    print(f"Date range: {start_time.isoformat()} to {end_time.isoformat()}")

    # Run the benchmark
    download_times = []

    for i in range(download_count):
        print(f"\nDownload test {i+1}/{download_count}...")

        # Create a new client for each test
        async with VisionDataClient(symbol, interval) as client:
            # Measure the download time
            start = time.time()
            result = await client.fetch(start_time, end_time)
            elapsed = time.time() - start

            # Record the result
            download_times.append(elapsed)
            print(f"Download completed in {elapsed:.2f}s - got {len(result)} records")

    # Calculate and display statistics
    avg_time = sum(download_times) / len(download_times)
    min_time = min(download_times)
    max_time = max(download_times)

    print("\nBenchmark Results:")
    print(f"  Average download time: {avg_time:.2f}s")
    print(f"  Minimum download time: {min_time:.2f}s")
    print(f"  Maximum download time: {max_time:.2f}s")

    # Calculate throughput metrics
    if len(result) > 0:
        data_points_per_second = len(result) / avg_time
        print(f"  Data points per second: {data_points_per_second:.2f}")

    return avg_time, min_time, max_time


if __name__ == "__main__":
    asyncio.run(benchmark_vision_download())
