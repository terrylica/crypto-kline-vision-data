# Archived HTTP Client Benchmarks

This directory contains the original benchmark scripts and implementations used during the HTTP client performance testing and optimization process.

## Contents

1. `benchmark_http_clients.py` - Original benchmark with retry logic
2. `benchmark_http_clients_no_retry.py` - Benchmark without retry logic
3. `Retrievethelatestdate_curl_cffi.py` - curl_cffi implementation with retry
4. `Retrievethelatestdate_cffi_no_retry.py` - curl_cffi implementation without retry
5. `Retrievethelatestdate_no_retry.py` - aiohttp implementation without retry
6. `test_curl_cffi.py` - Simple test for curl_cffi
7. `test_tls_client.py` - Simple test for tls_client
8. `s3_download_benchmark.py` - Benchmark for S3/download performance comparison
9. `Retrievethelatestdate.py` - httpx implementation for checking latest dates
10. `download_first_benchmark.py` - Benchmark comparing download-first vs check-then-download approaches

## Purpose

These scripts are preserved for historical reference. For active use, refer to the current best practice implementation in:

- `../benchmark_best_practice.py` - Consolidated benchmark script with the best practices

## Documentation

For a comprehensive summary of the findings from all these tests, see:

- `../../docs/http_client_benchmark_summary.md`
