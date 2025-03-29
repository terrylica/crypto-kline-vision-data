# HTTP Client Benchmark Results and Best Practices

## Overview

This document summarizes our benchmarking of HTTP client libraries for two critical operations with Binance Vision data:

1. Checking URL availability in the Binance Vision data API
2. Downloading data files at maximum speed

## Key Recommendations

Based on our comprehensive testing, here are our key recommendations:

1. **Use `curl_cffi` as the primary HTTP client library**

   - Better performance in most scenarios
   - Lower CPU usage
   - Excellent concurrency support for downloads

2. **Use the "download-first" approach rather than checking before downloading**

   - 2.2-2.4x faster than separate check-then-download
   - Detects non-existent files 10-15% faster
   - Simplifies code and improves performance

3. **Use concurrency for multiple file downloads**

   - `curl_cffi` with ThreadPoolExecutor shows ~40% speedup with concurrency
   - Use 4 concurrent downloads for optimal performance

4. **Configure timeouts appropriately**
   - 3.0 seconds is optimal for Binance Vision API
   - No need for retries in most cases

## Libraries Tested

We benchmarked the following HTTP client libraries:

1. **curl_cffi** - Python bindings for libcurl via CFFI
2. **aiohttp** - Popular async HTTP client
3. **httpx** - Modern async-compatible HTTP client
4. **tls_client** - TLS fingerprinting client
5. **boto3/s3fs** - AWS SDK for Python/Filesystem interface to S3
6. **AWS CLI** - Command-line interface for AWS

## Part 1: URL Availability Checking

### Testing Methodology

The benchmarking involved several dimensions:

1. **Approaches tested:**

   - Traditional check-then-download (HEAD request followed by GET if available)
   - Download-first (direct GET without checking first)
   - Partial download (range request for first 1KB to quickly detect availability)

2. **Request patterns:**
   - With and without retry logic
   - HEAD vs GET requests
   - Various timeout values (0.5s, 2.0s, 3.0s, 5.0s)

### Results for URL Availability Checking

**For Download-First vs Check-Then-Download:**

- **Download-first approach is 2.2-2.4x faster** for existing files
- Download-first approach detects non-existent files 10-15% faster
- Small downloads (partial content) offered minimal benefits over direct downloads

**For Individual Clients:**

- curl_cffi showed dramatically faster individual request times in micro-benchmarks
- Individual HEAD requests completed in ~0.001s with curl_cffi vs ~0.3s with httpx/aiohttp
- For complete URL availability checking applications, network latency dominates
- curl_cffi showed lower CPU usage (7-9% vs 10-13% for aiohttp)

**For Retry vs No-Retry:**

- No-retry implementations were 15-20% faster
- Both achieved 100% success rate with proper timeout settings
- Retry logic only valuable for unstable networks

## Part 2: File Download Performance

### Testing Methodology for File Downloads

We tested download performance using multiple approaches:

1. **Different download methods**
   - Single file downloads with various clients
   - Concurrent downloads (4 parallel downloads)
   - Various chunk sizes (256KB, 1MB, 8MB, 16MB)

### Results for Download Performance

**Single-threaded vs Concurrent Downloads:**

- curl_cffi with concurrency achieved the highest throughput (13.75 MB/s)
- curl_cffi showed a ~40% speedup with concurrency
- aiohttp performed well for single-threaded downloads but worse with concurrency

**Other Findings:**

- 1MB chunk size was optimal for curl_cffi
- Network bandwidth, not disk I/O, was the primary bottleneck

## Implementation Examples

### Recommended Implementation (Download-First Approach)

```python
import os
import curl_cffi.requests as curl_requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_latest_data(symbol, interval, max_days_back=5, output_dir="downloads"):
    """Get the latest available data file using download-first approach.

    Args:
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        interval: Time interval (e.g., "1h", "1d")
        max_days_back: Maximum days to check backward
        output_dir: Directory to save downloaded files

    Returns:
        Tuple of (date found, path to downloaded file) or (None, None) if not found
    """
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
    """Download multiple files concurrently using curl_cffi.

    Args:
        url_list: List of URLs to download
        output_dir: Directory to save downloaded files
        max_concurrent: Maximum number of concurrent downloads

    Returns:
        List of successfully downloaded file paths
    """
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
```

## Alternative Approaches

For cases where you're already using an asyncio-based application, aiohttp can be used:

```python
import aiohttp
import asyncio

async def get_latest_data_async(symbol, interval, max_days_back=5):
    """Async version using aiohttp and download-first approach."""
    current_date = datetime.utcnow()

    async with aiohttp.ClientSession() as session:
        for i in range(max_days_back + 1):
            check_date = (current_date - timedelta(days=i)).strftime("%Y-%m-%d")
            url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{check_date}.zip"

            try:
                async with session.get(url, timeout=3.0) as response:
                    if response.status == 200:
                        content = await response.read()
                        # Process content
                        return check_date, content
            except Exception:
                continue

    return None, None
```

## Conclusion

Our comprehensive benchmarking shows that:

1. The **download-first approach** is significantly faster than checking before downloading
2. **curl_cffi** consistently outperforms other libraries for both checking and downloading
3. **Concurrency** provides substantial performance benefits for multiple downloads
4. Optimal **timeout settings** (3.0s) and no retries are suitable for most applications

By implementing these best practices, applications can achieve:

- Up to 2.4x faster file availability checking
- Up to 40% faster downloads with concurrency
- Simplified and more efficient code with the download-first approach
