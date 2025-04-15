# fsspec vs Traditional ZIP Handling Benchmark

This directory contains comprehensive benchmarking tools for comparing the performance of `fsspec` against traditional `zipfile` and `tempfile` methods when processing Binance Vision API data.

## Overview

The benchmark focuses on comparing two approaches to handling ZIP files containing cryptocurrency market data:

1. **Traditional Method**: Uses Python's built-in `zipfile` module to extract files to a temporary directory using `tempfile`, then reads the data with `pandas`.
2. **fsspec Method**: Uses the `fsspec` library to read directly from ZIP files without explicit extraction.

## Key Findings

After extensive benchmarking (20 runs with 10 warmup iterations per configuration), we found:

- The fsspec method consistently outperformed the traditional approach across all test configurations
- Performance improvements ranged from 2% to 32%
- Even for small files (hourly data with only 24 rows), fsspec was 20% faster
- The highest performance gain (32%) was observed with ARBUSDT 1-minute data
- For detailed results and implementation recommendations, see [benchmark_results.md](benchmark_results.md)

## Features

- Multiple test configurations across different symbols, intervals, and market types
- Configurable warmup runs to stabilize performance before measurement
- Statistical analysis using median values for more reliable comparisons
- Memory usage profiling (optional)
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

This will:

- Run tests across multiple configurations
- Perform 10 warmup runs before each test (discarded)
- Perform 20 timed runs for each method
- Output detailed statistics for each test case
- Save complete results to `results.json`

### Running a Single Benchmark

To run a benchmark for a specific configuration:

```bash
./benchmark_vision_data.py benchmark --symbol BTCUSDT --interval 1m --date 2023-12-01 --market spot --runs 20 --warmup 10
```

### Command Line Arguments

#### For `benchmark-all` Command

- `--runs`, `-r`: Number of benchmark runs to perform (default: 15)
- `--warmup`, `-w`: Number of warmup runs to perform (default: 3)
- `--output`, `-o`: Output JSON file for detailed results (optional)
- `--memory`, `-m`: Measure memory usage (may affect timing results)

#### For `benchmark` Command

- `--symbol`, `-s`: Symbol to download data for (default: BTCUSDT)
- `--interval`, `-i`: Kline interval (default: 1m)
- `--date`, `-d`: Date in YYYY-MM-DD format (default: 2023-12-01)
- `--market`, `-m`: Market type (spot, um, cm) (default: spot)
- `--runs`, `-r`: Number of benchmark runs to perform (default: 15)
- `--warmup`, `-w`: Number of warmup runs to perform (default: 3)
- `--memory`: Measure memory usage (may affect timing results)

## Interpreting Results

The benchmark provides several metrics:

- **Median Time**: The median execution time across all runs (primary comparison metric)
- **Average Time**: The average execution time across all runs
- **Standard Deviation**: Variation in execution times
- **Memory Usage**: Increase in memory during execution (when measuring memory)
- **Speedup Factor**: Ratio of faster method to slower method

A summary table shows all test cases with color-coded winners:

- Green indicates cases where fsspec is faster (currently all test cases)

## Implementation Details

The benchmark closely simulates the actual production code in `VisionDataClient._download_file()` method:

1. Downloads the ZIP file from Binance Vision API
2. Uses the same file processing logic but with precise timing around the critical operations
3. Implements both methods with the same input/output behavior

## Adding New Tests

To add new test configurations, edit the `test_cases` list in the `benchmark_all` command.
