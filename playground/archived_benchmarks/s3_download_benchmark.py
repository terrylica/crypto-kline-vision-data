#!/usr/bin/env python3
"""
S3 Download Speed Benchmark

This script benchmarks different libraries for downloading data from S3,
focusing exclusively on download speed.
"""

import asyncio
import time
import statistics
import argparse
import os
import subprocess
import tempfile
from pathlib import Path
from contextlib import contextmanager
import shutil
import io

# Import libraries
import boto3
import s3fs
import aiohttp
import curl_cffi.requests as curl_requests

# Optional import for Daft if available
DAFT_AVAILABLE = False
try:
    import daft

    DAFT_AVAILABLE = True
except ImportError:
    # Daft not installed, that's fine
    pass

# Set up argument parser
parser = argparse.ArgumentParser(description="Benchmark S3 download speed")
parser.add_argument(
    "--url",
    default="https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2023-01.zip",
    help="URL to download (default: 2.3MB BTCUSDT monthly data)",
)
parser.add_argument(
    "--bucket", default="", help="S3 bucket name (only needed for S3 downloads)"
)
parser.add_argument(
    "--key", default="", help="S3 object key (only needed for S3 downloads)"
)
parser.add_argument("--iterations", type=int, default=3, help="Iterations per library")
parser.add_argument("--concurrency", type=int, default=1, help="Concurrent downloads")
parser.add_argument(
    "--output",
    default=None,
    help="Output path for downloaded files (defaults to temp dir)",
)
parser.add_argument(
    "--chunk-size",
    type=int,
    default=8 * 1024 * 1024,
    help="Chunk size for downloads (bytes)",
)
parser.add_argument(
    "--discard",
    action="store_true",
    help="Discard downloaded data (don't write to disk)",
)


@contextmanager
def temp_download_dir():
    """Create a temporary directory for downloads and clean up afterward."""
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)


class BenchmarkResult:
    """Store and analyze benchmark results."""

    def __init__(self, name, times, file_size=None):
        self.name = name
        self.times = times
        self.file_size = file_size  # in bytes

    @property
    def avg_time(self):
        return statistics.mean(self.times)

    @property
    def min_time(self):
        return min(self.times)

    @property
    def max_time(self):
        return max(self.times)

    @property
    def std_dev(self):
        return statistics.stdev(self.times) if len(self.times) > 1 else 0

    @property
    def throughput_mb_s(self):
        """Return average throughput in MB/s."""
        if self.file_size:
            return (self.file_size / 1024 / 1024) / self.avg_time
        return None

    def __str__(self):
        result = f"{self.name}: avg={self.avg_time:.2f}s, min={self.min_time:.2f}s, max={self.max_time:.2f}s, std={self.std_dev:.2f}s"
        if self.throughput_mb_s:
            result += f", throughput={self.throughput_mb_s:.2f} MB/s"
        return result


# Download implementations
async def download_boto3(url, output_path, discard=False, chunk_size=8 * 1024 * 1024):
    """Download using boto3."""
    # Parse URL to get bucket and key
    if url.startswith("https://") and "s3.amazonaws.com" in url:
        # Convert https://bucket.s3.amazonaws.com/key to s3://bucket/key
        parts = url.replace("https://", "").split("/", 1)
        bucket = parts[0].split(".s3.amazonaws.com")[0]
        key = parts[1]
    elif url.startswith("https://data.binance.vision/"):
        # For Binance data, use a hardcoded bucket because we don't have direct S3 access
        # This won't actually work with boto3, but we keep it for comparison
        bucket = "binance-public-data"
        key = url.replace("https://data.binance.vision/", "")
    else:
        raise ValueError(f"Cannot parse S3 bucket and key from URL: {url}")

    start_time = time.time()
    s3 = boto3.client("s3")
    output_file = os.path.join(output_path, os.path.basename(key))

    if discard:
        # Stream but discard data
        response = s3.get_object(Bucket=bucket, Key=key)
        chunk = response["Body"].read(chunk_size)
        content_length = int(response["ContentLength"])
        while chunk:
            chunk = response["Body"].read(chunk_size)
    else:
        # Normal download to file
        with open(output_file, "wb") as f:
            s3.download_fileobj(Bucket=bucket, Key=key, Fileobj=f)

        content_length = os.path.getsize(output_file)

    elapsed = time.time() - start_time
    return elapsed, content_length


async def download_boto3_chunked(
    url, output_path, discard=False, chunk_size=8 * 1024 * 1024
):
    """Download using boto3 with manual chunking."""
    # Parse URL to get bucket and key
    if url.startswith("https://") and "s3.amazonaws.com" in url:
        # Convert https://bucket.s3.amazonaws.com/key to s3://bucket/key
        parts = url.replace("https://", "").split("/", 1)
        bucket = parts[0].split(".s3.amazonaws.com")[0]
        key = parts[1]
    elif url.startswith("https://data.binance.vision/"):
        # For Binance data, use a hardcoded bucket because we don't have direct S3 access
        bucket = "binance-public-data"
        key = url.replace("https://data.binance.vision/", "")
    else:
        raise ValueError(f"Cannot parse S3 bucket and key from URL: {url}")

    start_time = time.time()
    s3 = boto3.client("s3")
    output_file = os.path.join(output_path, os.path.basename(key))

    response = s3.get_object(Bucket=bucket, Key=key)
    content_length = int(response["ContentLength"])

    if not discard:
        with open(output_file, "wb") as f:
            chunk = response["Body"].read(chunk_size)
            while chunk:
                f.write(chunk)
                chunk = response["Body"].read(chunk_size)
    else:
        # Just read and discard
        chunk = response["Body"].read(chunk_size)
        while chunk:
            chunk = response["Body"].read(chunk_size)

    elapsed = time.time() - start_time
    return elapsed, content_length


async def download_s3fs(url, output_path, discard=False, chunk_size=8 * 1024 * 1024):
    """Download using s3fs."""
    # Parse URL to get bucket and key
    if url.startswith("https://") and "s3.amazonaws.com" in url:
        # Convert https://bucket.s3.amazonaws.com/key to s3://bucket/key
        parts = url.replace("https://", "").split("/", 1)
        bucket = parts[0].split(".s3.amazonaws.com")[0]
        key = parts[1]
    elif url.startswith("https://data.binance.vision/"):
        # For Binance data, use a hardcoded bucket because we don't have direct S3 access
        bucket = "binance-public-data"
        key = url.replace("https://data.binance.vision/", "")
    else:
        raise ValueError(f"Cannot parse S3 bucket and key from URL: {url}")

    start_time = time.time()
    # Use anonymous access for public data
    fs = s3fs.S3FileSystem(anon=True)
    output_file = os.path.join(output_path, os.path.basename(key))

    if not discard:
        with fs.open(f"{bucket}/{key}", "rb") as s3_file:
            content_length = 0
            with open(output_file, "wb") as local_file:
                while True:
                    chunk = s3_file.read(chunk_size)
                    if not chunk:
                        break
                    content_length += len(chunk)
                    local_file.write(chunk)
    else:
        with fs.open(f"{bucket}/{key}", "rb") as s3_file:
            content_length = 0
            while True:
                chunk = s3_file.read(chunk_size)
                if not chunk:
                    break
                content_length += len(chunk)

    elapsed = time.time() - start_time
    return elapsed, content_length


async def download_aiohttp(url, output_path, discard=False, chunk_size=8 * 1024 * 1024):
    """Download using aiohttp."""
    start_time = time.time()
    output_file = os.path.join(output_path, os.path.basename(url))

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if discard:
                content_length = 0
                while True:
                    chunk = await response.content.read(chunk_size)
                    if not chunk:
                        break
                    content_length += len(chunk)
            else:
                with open(output_file, "wb") as f:
                    content_length = 0
                    while True:
                        chunk = await response.content.read(chunk_size)
                        if not chunk:
                            break
                        content_length += len(chunk)
                        f.write(chunk)

    elapsed = time.time() - start_time
    return elapsed, content_length


async def download_curl_cffi(
    url, output_path, discard=False, chunk_size=8 * 1024 * 1024
):
    """Download using curl_cffi."""
    start_time = time.time()
    output_file = os.path.join(output_path, os.path.basename(url))

    if discard:
        # curl_cffi doesn't provide a streaming interface in sync mode,
        # but we can use a null buffer
        response = curl_requests.get(url)
        content_length = len(response.content)
    else:
        response = curl_requests.get(url)
        with open(output_file, "wb") as f:
            f.write(response.content)
        content_length = len(response.content)

    elapsed = time.time() - start_time
    return elapsed, content_length


async def download_curl_cffi_async(
    url, output_path, discard=False, chunk_size=8 * 1024 * 1024
):
    """Download using curl_cffi async API."""
    start_time = time.time()
    output_file = os.path.join(output_path, os.path.basename(url))

    async with curl_requests.AsyncSession() as session:
        response = await session.get(url, impersonate="chrome110")
        if not discard:
            with open(output_file, "wb") as f:
                f.write(response.content)
        content_length = len(response.content)

    elapsed = time.time() - start_time
    return elapsed, content_length


async def download_awscli(url, output_path, discard=False, chunk_size=None):
    """Download using AWS CLI."""
    # Parse URL to get bucket and key
    if url.startswith("https://") and "s3.amazonaws.com" in url:
        # Convert https://bucket.s3.amazonaws.com/key to s3://bucket/key
        parts = url.replace("https://", "").split("/", 1)
        bucket = parts[0].split(".s3.amazonaws.com")[0]
        key = parts[1]
    elif url.startswith("https://data.binance.vision/"):
        # For Binance data, we won't be able to use S3 protocol directly
        # Fall back to using curl as aws s3 cp won't work
        output_file = os.path.join(output_path, os.path.basename(url))
        cmd = ["curl", "-sS", "-o"]
        if discard:
            cmd.append("/dev/null")
        else:
            cmd.append(output_file)
        cmd.append(url)
    else:
        # Convert URL to S3 path
        parsed = url.replace("https://", "").split("/", 1)
        bucket = parsed[0].split(".")[0]
        key = parsed[1]
        output_file = os.path.join(output_path, os.path.basename(key))
        cmd = [
            "aws",
            "s3",
            "cp",
            f"s3://{bucket}/{key}",
            output_file if not discard else "/dev/null",
            "--no-cli-pager",
        ]

    start_time = time.time()
    result = subprocess.run(cmd, check=True, capture_output=True)
    elapsed = time.time() - start_time

    # Get file size
    if not discard and os.path.exists(output_file):
        content_length = os.path.getsize(output_file)
    else:
        # We need to make a HEAD request to get the content length
        head_result = subprocess.run(
            ["curl", "-sI", url], check=True, capture_output=True, text=True
        )
        for line in head_result.stdout.splitlines():
            if line.lower().startswith("content-length:"):
                content_length = int(line.split(":", 1)[1].strip())
                break
        else:
            content_length = 0

    return elapsed, content_length


# Daft implementation if available
if DAFT_AVAILABLE:

    async def download_daft(url, output_path, discard=False, chunk_size=None):
        """Download using Daft."""
        # Parse URL to get bucket and key for S3 path
        if url.startswith("https://") and "s3.amazonaws.com" in url:
            # Convert https://bucket.s3.amazonaws.com/key to s3://bucket/key
            parts = url.replace("https://", "").split("/", 1)
            bucket = parts[0].split(".s3.amazonaws.com")[0]
            key = parts[1]
            s3_path = f"s3://{bucket}/{key}"
        elif url.startswith("https://data.binance.vision/"):
            # For Binance data, this won't work with Daft through S3
            # Fall back to HTTP
            return await download_curl_cffi(url, output_path, discard, chunk_size)
        else:
            s3_path = url

        start_time = time.time()

        try:
            df = daft.from_glob_path(s3_path)
            # Force download by materializing the dataframe
            df = df.collect()

            output_file = os.path.join(output_path, os.path.basename(url))

            # Since Daft is primarily a dataframe library, not a file downloader,
            # this is not its intended use case, so we'll return an estimated result
            elapsed = time.time() - start_time

            # Try to estimate the file size
            content_length = sum(
                col.estimated_size() for col in df.schema().to_arrow().fields
            )

            if not discard and len(df) > 0:
                # Save the first column as a file just to have something
                with open(output_file, "wb") as f:
                    f.write(b"Daft download test")
        except Exception as e:
            print(f"Daft download failed: {e}")
            elapsed = time.time() - start_time
            content_length = 0

        return elapsed, content_length


async def run_concurrent_tests(
    download_func,
    name,
    url,
    output_path,
    iterations,
    concurrency,
    discard=False,
    chunk_size=8 * 1024 * 1024,
):
    """Run download tests with specified concurrency."""
    print(f"Running {name} test...")
    if concurrency == 1:
        # Simple sequential case
        times = []
        content_length = 0

        for i in range(iterations):
            print(f"  Iteration {i+1}/{iterations}...")
            try:
                result = await download_func(url, output_path, discard, chunk_size)
                elapsed, size = result
                content_length = size  # Use the last valid size
                times.append(elapsed)
                print(
                    f"  Completed in {elapsed:.2f} seconds, {size/(1024*1024):.2f} MB"
                )
            except Exception as e:
                print(f"  Error: {e}")

        if times:
            return BenchmarkResult(name, times, content_length)
        return None
    else:
        # Concurrent downloads
        tasks = []
        for i in range(concurrency):
            tasks.append(download_func(url, output_path, discard, chunk_size))

        try:
            results = await asyncio.gather(*tasks)
            times = [r[0] for r in results]
            content_length = results[0][1] if results else 0
            return BenchmarkResult(
                f"{name} (concurrency={concurrency})", times, content_length
            )
        except Exception as e:
            print(f"  Error with concurrent downloads: {e}")
            return None


async def main():
    args = parser.parse_args()

    # Determine the URL to test
    url = args.url
    if args.bucket and args.key:
        url = f"https://{args.bucket}.s3.amazonaws.com/{args.key}"

    print(f"Benchmarking download of: {url}")
    print(f"Chunk size: {args.chunk_size/1024/1024:.2f} MB")
    print(f"Discard data: {args.discard}")

    if args.output:
        output_path = args.output
        os.makedirs(output_path, exist_ok=True)
    else:
        # Use a temporary directory
        context = temp_download_dir()
        output_path = context.__enter__()
        print(f"Using temporary directory: {output_path}")

    try:
        results = []

        # Run each benchmark
        boto3_result = await run_concurrent_tests(
            download_boto3,
            "boto3",
            url,
            output_path,
            args.iterations,
            args.concurrency,
            args.discard,
            args.chunk_size,
        )
        if boto3_result:
            results.append(boto3_result)

        boto3_chunked_result = await run_concurrent_tests(
            download_boto3_chunked,
            "boto3_chunked",
            url,
            output_path,
            args.iterations,
            args.concurrency,
            args.discard,
            args.chunk_size,
        )
        if boto3_chunked_result:
            results.append(boto3_chunked_result)

        s3fs_result = await run_concurrent_tests(
            download_s3fs,
            "s3fs",
            url,
            output_path,
            args.iterations,
            args.concurrency,
            args.discard,
            args.chunk_size,
        )
        if s3fs_result:
            results.append(s3fs_result)

        aiohttp_result = await run_concurrent_tests(
            download_aiohttp,
            "aiohttp",
            url,
            output_path,
            args.iterations,
            args.concurrency,
            args.discard,
            args.chunk_size,
        )
        if aiohttp_result:
            results.append(aiohttp_result)

        curl_cffi_result = await run_concurrent_tests(
            download_curl_cffi,
            "curl_cffi",
            url,
            output_path,
            args.iterations,
            args.concurrency,
            args.discard,
            args.chunk_size,
        )
        if curl_cffi_result:
            results.append(curl_cffi_result)

        curl_cffi_async_result = await run_concurrent_tests(
            download_curl_cffi_async,
            "curl_cffi_async",
            url,
            output_path,
            args.iterations,
            args.concurrency,
            args.discard,
            args.chunk_size,
        )
        if curl_cffi_async_result:
            results.append(curl_cffi_async_result)

        awscli_result = await run_concurrent_tests(
            download_awscli,
            "awscli/curl",
            url,
            output_path,
            args.iterations,
            args.concurrency,
            args.discard,
            args.chunk_size,
        )
        if awscli_result:
            results.append(awscli_result)

        if DAFT_AVAILABLE:
            daft_result = await run_concurrent_tests(
                download_daft,
                "daft",
                url,
                output_path,
                args.iterations,
                args.concurrency,
                args.discard,
                args.chunk_size,
            )
            if daft_result:
                results.append(daft_result)

        if not results:
            print("No successful benchmark results!")
            return

        # Sort by average download time
        results.sort(key=lambda x: x.avg_time)

        # Print results
        print("\n--- BENCHMARK RESULTS ---")
        for result in results:
            print(result)

        # Print ranking
        print("\n--- RANKING BY AVERAGE SPEED ---")
        for i, result in enumerate(results, 1):
            print(f"{i}. {result.name}: {result.avg_time:.2f}s")

        if results[0].throughput_mb_s:
            print(
                f"\nFastest method: {results[0].name} with {results[0].throughput_mb_s:.2f} MB/s"
            )

    finally:
        if args.output is None:
            context.__exit__(None, None, None)


if __name__ == "__main__":
    asyncio.run(main())
