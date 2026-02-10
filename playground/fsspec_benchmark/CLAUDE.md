# fsspec vs Traditional ZIP Handling Benchmark

**Hub**: [playground/](../CLAUDE.md) | **Root**: [CLAUDE.md](../../CLAUDE.md)

Benchmarking tools comparing `fsspec` vs traditional `zipfile`+`tempfile` for processing Binance Vision API data.

## Overview

The benchmark focuses on comparing two approaches to handling ZIP files containing cryptocurrency market data:

1. **Traditional Method**: Uses Python's built-in `zipfile` module to extract files to a temporary directory using `tempfile`, then reads the data with `pandas`.
2. **fsspec Method**: Uses the `fsspec` library to read directly from ZIP files without explicit extraction.

Both methods are also tested with SHA-256 checksum validation to ensure data integrity.

## Key Findings

After extensive benchmarking (20 runs with 10 warmup iterations per configuration), we found:

- The fsspec method consistently outperformed the traditional approach across all test configurations
- Without checksum validation: Performance improvements ranged from 2% to 32%
- With checksum validation: Performance improvements ranged from 1% to 68%
- Even for small files (hourly data with only 24 rows), fsspec was 20% faster without checksum and 68% faster with checksum validation
- The highest performance gain (32%) was observed with ARBUSDT 1-minute data
- For detailed results and implementation recommendations, see [benchmark_results.md](benchmark_results.md)

## Features

- Multiple test configurations across different symbols, intervals, and market types
- Configurable warmup runs to stabilize performance before measurement
- Statistical analysis using median values for more reliable comparisons
- Memory usage profiling (optional)
- SHA-256 checksum validation benchmarking
- System information gathering for reproducibility
- JSON export of detailed results
- Rich console output with color-coded results

## Requirements

The benchmark requires the following packages:

```bash
pip install fsspec httpx pandas rich typer pendulum psutil
```

Note: The `fsspec` library is already included in the project dependencies (as seen in the `.devcontainer/Dockerfile`).

## Usage

### Running Comprehensive Benchmarks

To run benchmarks across multiple configurations:

```bash
./benchmark_vision_data.py benchmark-all --runs 20 --warmup 10 --output results.json
```

To include checksum validation in the benchmarks:

```bash
./benchmark_vision_data.py benchmark-all --runs 20 --warmup 10 --checksum --output results_with_checksum.json
```

This will:

- Run tests across multiple configurations
- Perform 10 warmup runs before each test (discarded)
- Perform 20 timed runs for each method
- Validate SHA-256 checksums if the `--checksum` flag is provided
- Output detailed statistics for each test case
- Save complete results to the specified JSON file

### Running a Single Benchmark

To run a benchmark for a specific configuration:

```bash
./benchmark_vision_data.py benchmark --symbol BTCUSDT --interval 1m --date 2023-12-01 --market spot --runs 20 --warmup 10
```

To enable checksum validation:

```bash
./benchmark_vision_data.py benchmark --symbol BTCUSDT --interval 1m --date 2023-12-01 --market spot --runs 20 --warmup 10 --checksum
```

### Comparing Checksum Performance

To directly compare performance with and without checksum validation:

```bash
./benchmark_vision_data.py benchmark-checksum --symbol BTCUSDT --interval 1m --date 2023-12-01 --runs 10 --warmup 5 --output checksum_comparison.json
```

This will run benchmarks both with and without checksum validation and provide a direct comparison of the performance impact.

### Command Line Arguments

#### For `benchmark-all` Command

- `--runs`, `-r`: Number of benchmark runs to perform (default: 15)
- `--warmup`, `-w`: Number of warmup runs to perform (default: 3)
- `--output`, `-o`: Output JSON file for detailed results (optional)
- `--memory`, `-m`: Measure memory usage (may affect timing results)
- `--checksum`, `-c`: Enable SHA-256 checksum validation

#### For `benchmark` Command

- `--symbol`, `-s`: Symbol to download data for (default: BTCUSDT)
- `--interval`, `-i`: Kline interval (default: 1m)
- `--date`, `-d`: Date in YYYY-MM-DD format (default: 2023-12-01)
- `--market`, `-m`: Market type (spot, um, cm) (default: spot)
- `--runs`, `-r`: Number of benchmark runs to perform (default: 15)
- `--warmup`, `-w`: Number of warmup runs to perform (default: 3)
- `--memory`: Measure memory usage (may affect timing results)
- `--checksum`, `-c`: Enable SHA-256 checksum validation

#### For `benchmark-checksum` Command

- `--symbol`, `-s`: Symbol to download data for (default: BTCUSDT)
- `--interval`, `-i`: Kline interval (default: 1m)
- `--date`, `-d`: Date in YYYY-MM-DD format (default: 2023-12-01)
- `--market`, `-m`: Market type (spot, um, cm) (default: spot)
- `--runs`, `-r`: Number of benchmark runs to perform (default: 15)
- `--warmup`, `-w`: Number of warmup runs to perform (default: 3)
- `--memory`: Measure memory usage (may affect timing results)
- `--output`, `-o`: Output JSON file for detailed results (optional)

## Interpreting Results

The benchmark provides several metrics:

- **Median Time**: The median execution time across all runs (primary comparison metric)
- **Average Time**: The average execution time across all runs
- **Standard Deviation**: Variation in execution times
- **Memory Usage**: Increase in memory during execution (when measuring memory)
- **Speedup Factor**: Ratio of faster method to slower method
- **Checksum Overhead**: Percentage increase in processing time when validating checksums

A summary table shows all test cases with color-coded winners:

- Green indicates cases where fsspec is faster (currently all test cases)

## Implementation Details

The benchmark closely simulates the actual production code in `VisionDataClient._download_file()` method:

1. Downloads the ZIP file from Binance Vision API
2. Optionally validates the SHA-256 checksum against the published checksum file
3. Uses the same file processing logic but with precise timing around the critical operations
4. Implements both methods with the same input/output behavior

### Checksum Validation

The benchmark implements SHA-256 checksum validation:

1. Downloads the checksum file (with .CHECKSUM extension) from Binance Vision API
2. Computes the SHA-256 hash of the downloaded ZIP file
3. Compares the computed hash with the expected hash
4. For fair benchmarking, the checksum file is downloaded once before timing starts

## Adding New Tests

To add new test configurations, edit the `test_cases` list in the `benchmark_all` command.
