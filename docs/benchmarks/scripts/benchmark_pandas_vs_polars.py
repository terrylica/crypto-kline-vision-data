#!/usr/bin/env python3
"""Performance benchmark: Pandas FCP path vs Polars Pipeline path.

This script compares performance between:
1. Legacy pandas-based FCP processing (USE_POLARS_PIPELINE=False)
2. New Polars pipeline processing (USE_POLARS_PIPELINE=True)

Tests various data request sizes to measure:
- Execution time
- Peak memory usage
"""

import gc
import importlib
import os
import time
import tracemalloc
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

# Set environment variables BEFORE importing CKVD
os.environ["CKVD_LOG_LEVEL"] = "ERROR"  # Suppress logs during benchmarks


class BenchmarkResult(NamedTuple):
    """Result from a single benchmark run."""

    scenario: str
    use_polars: bool
    rows: int
    time_seconds: float
    peak_memory_mb: float
    data_source: str


def run_benchmark(
    symbol: str,
    days: int,
    interval_str: str,
    use_polars_pipeline: bool,
    scenario_name: str,
) -> BenchmarkResult:
    """Run a single benchmark with specified configuration.

    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")
        days: Number of days of data to fetch
        interval_str: Interval string (e.g., "1h", "1m")
        use_polars_pipeline: Whether to use Polars pipeline
        scenario_name: Name for this scenario

    Returns:
        BenchmarkResult with timing and memory metrics
    """
    # Force garbage collection before benchmark
    gc.collect()

    # Set feature flag
    os.environ["CKVD_USE_POLARS_PIPELINE"] = str(use_polars_pipeline).lower()

    # Start memory tracking
    tracemalloc.start()

    # Import fresh to pick up env var
    # Note: We need to reload the config module to pick up env var changes
    from ckvd.utils import config

    importlib.reload(config)

    from ckvd import DataProvider, CryptoKlineVisionData, Interval, MarketType

    # Map interval string to enum
    interval_map = {
        "1m": Interval.MINUTE_1,
        "5m": Interval.MINUTE_5,
        "15m": Interval.MINUTE_15,
        "1h": Interval.HOUR_1,
        "4h": Interval.HOUR_4,
        "1d": Interval.DAY_1,
    }
    interval = interval_map[interval_str]

    # Time range
    end_time = datetime.now(timezone.utc) - timedelta(days=3)  # Avoid too recent
    start_time = end_time - timedelta(days=days)

    # Run benchmark
    start = time.perf_counter()

    manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
    df = manager.get_data(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
    )
    manager.close()

    elapsed = time.perf_counter() - start

    # Get memory stats
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    rows = len(df) if df is not None else 0
    data_source = "unknown"
    if df is not None and "_data_source" in df.columns:
        sources = df["_data_source"].value_counts().to_dict()
        data_source = ", ".join(f"{k}:{v}" for k, v in sources.items())
    elif df is not None:
        data_source = "cache/mixed"

    return BenchmarkResult(
        scenario=scenario_name,
        use_polars=use_polars_pipeline,
        rows=rows,
        time_seconds=elapsed,
        peak_memory_mb=peak / 1024 / 1024,
        data_source=data_source,
    )


def format_results(results: list[BenchmarkResult]) -> str:
    """Format benchmark results as a table."""
    lines = [
        "",
        "=" * 100,
        "PERFORMANCE BENCHMARK: Pandas FCP Path vs Polars Pipeline",
        "=" * 100,
        "",
        f"{'Scenario':<25} {'Engine':<10} {'Rows':>10} {'Time (s)':>12} {'Memory (MB)':>14} {'Speedup':>10}",
        "-" * 100,
    ]

    # Group by scenario to calculate speedup
    scenarios = {}
    for r in results:
        if r.scenario not in scenarios:
            scenarios[r.scenario] = {}
        scenarios[r.scenario][r.use_polars] = r

    for _scenario_name, runs in scenarios.items():
        pandas_result = runs.get(False)
        polars_result = runs.get(True)

        if pandas_result:
            speedup = "-"
            lines.append(
                f"{pandas_result.scenario:<25} {'Pandas':<10} {pandas_result.rows:>10,} "
                f"{pandas_result.time_seconds:>12.3f} {pandas_result.peak_memory_mb:>14.2f} {speedup:>10}"
            )

        if polars_result:
            if pandas_result and pandas_result.time_seconds > 0:
                speedup_val = pandas_result.time_seconds / polars_result.time_seconds
                speedup = f"{speedup_val:.2f}x"
            else:
                speedup = "-"
            lines.append(
                f"{polars_result.scenario:<25} {'Polars':<10} {polars_result.rows:>10,} "
                f"{polars_result.time_seconds:>12.3f} {polars_result.peak_memory_mb:>14.2f} {speedup:>10}"
            )

        lines.append("-" * 100)

    # Summary
    pandas_results = [r for r in results if not r.use_polars]
    polars_results = [r for r in results if r.use_polars]

    if pandas_results and polars_results:
        avg_pandas_time = sum(r.time_seconds for r in pandas_results) / len(pandas_results)
        avg_polars_time = sum(r.time_seconds for r in polars_results) / len(polars_results)
        avg_pandas_mem = sum(r.peak_memory_mb for r in pandas_results) / len(pandas_results)
        avg_polars_mem = sum(r.peak_memory_mb for r in polars_results) / len(polars_results)

        lines.extend(
            [
                "",
                "SUMMARY",
                "-" * 50,
                f"Average Time  - Pandas: {avg_pandas_time:.3f}s, Polars: {avg_polars_time:.3f}s",
                f"Average Memory - Pandas: {avg_pandas_mem:.2f}MB, Polars: {avg_polars_mem:.2f}MB",
                f"Overall Speedup: {avg_pandas_time / avg_polars_time:.2f}x",
                f"Memory Improvement: {((avg_pandas_mem - avg_polars_mem) / avg_pandas_mem * 100):.1f}%",
            ]
        )

    return "\n".join(lines)


def main():
    """Run all benchmarks."""
    print("Starting performance benchmarks...")
    print("This will take several minutes as it fetches real data.\n")

    # Define scenarios: (name, symbol, days, interval)
    # Note: Using hourly intervals as 1m has Vision API compatibility issues
    scenarios = [
        ("Small (1d, 1h)", "BTCUSDT", 1, "1h"),
        ("Medium (7d, 1h)", "BTCUSDT", 7, "1h"),
        ("Large (30d, 1h)", "BTCUSDT", 30, "1h"),
        ("XL (90d, 1h)", "BTCUSDT", 90, "1h"),
        ("XXL (180d, 1h)", "BTCUSDT", 180, "1h"),
    ]

    results = []

    for scenario_name, symbol, days, interval in scenarios:
        print(f"\nRunning: {scenario_name}")

        # Run with Pandas first
        print("  - Pandas FCP path...")
        pandas_result = run_benchmark(symbol, days, interval, False, scenario_name)
        results.append(pandas_result)
        print(f"    {pandas_result.rows:,} rows in {pandas_result.time_seconds:.2f}s")

        # Run with Polars
        print("  - Polars Pipeline...")
        polars_result = run_benchmark(symbol, days, interval, True, scenario_name)
        results.append(polars_result)
        print(f"    {polars_result.rows:,} rows in {polars_result.time_seconds:.2f}s")

    # Print formatted results
    print(format_results(results))

    # Save results to file
    output_file = "/Users/terryli/eon/crypto-kline-vision-data/tmp/benchmark_results.txt"
    with open(output_file, "w") as f:
        f.write(format_results(results))
        f.write(f"\n\nBenchmark completed at: {datetime.now(timezone.utc).isoformat()}\n")
    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
