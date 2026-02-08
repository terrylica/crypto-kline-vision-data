#!/usr/bin/env python3
"""Performance benchmark: Polars Streaming Engine vs In-Memory Engine.

This script compares performance between:
1. Polars with streaming engine: .collect(engine='streaming')
2. Polars with in-memory engine: .collect() (default)

Tests various data request sizes to measure:
- Execution time
- Peak memory usage
"""

import gc
import os
import time
import tracemalloc
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

import polars as pl

# Set environment variables BEFORE importing CKVD
os.environ["CKVD_LOG_LEVEL"] = "ERROR"  # Suppress logs during benchmarks


class BenchmarkResult(NamedTuple):
    """Result from a single benchmark run."""

    scenario: str
    use_streaming: bool
    rows: int
    time_seconds: float
    peak_memory_mb: float


def create_test_lazyframe(num_rows: int) -> pl.LazyFrame:
    """Create a test LazyFrame simulating OHLCV data.

    Args:
        num_rows: Number of rows to generate

    Returns:
        LazyFrame with OHLCV-like data
    """
    base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # Create data
    data = {
        "open_time": [base_time + timedelta(hours=i) for i in range(num_rows)],
        "open": [42000.0 + (i % 100) for i in range(num_rows)],
        "high": [42100.0 + (i % 100) for i in range(num_rows)],
        "low": [41900.0 + (i % 100) for i in range(num_rows)],
        "close": [42050.0 + (i % 100) for i in range(num_rows)],
        "volume": [1000.0 + (i % 500) for i in range(num_rows)],
        "_data_source": ["CACHE" for _ in range(num_rows)],
    }

    return pl.LazyFrame(data)


def run_benchmark(
    num_rows: int,
    use_streaming: bool,
    scenario_name: str,
    num_iterations: int = 3,
) -> BenchmarkResult:
    """Run a single benchmark with specified configuration.

    Args:
        num_rows: Number of rows in test data
        use_streaming: Whether to use streaming engine
        scenario_name: Name for this scenario
        num_iterations: Number of iterations to average

    Returns:
        BenchmarkResult with timing and memory metrics
    """
    times = []
    peak_memories = []

    for _ in range(num_iterations):
        # Force garbage collection before benchmark
        gc.collect()

        # Create fresh LazyFrame
        lf = create_test_lazyframe(num_rows)

        # Apply some typical operations
        lf = lf.filter(pl.col("volume") > 500)
        lf = lf.with_columns([
            (pl.col("high") - pl.col("low")).alias("range"),
            ((pl.col("close") - pl.col("open")) / pl.col("open") * 100).alias("pct_change"),
        ])
        lf = lf.sort("open_time")

        # Start memory tracking
        tracemalloc.start()

        # Run benchmark
        start = time.perf_counter()

        if use_streaming:
            df = lf.collect(engine="streaming")
        else:
            df = lf.collect()

        elapsed = time.perf_counter() - start

        # Get memory stats
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        times.append(elapsed)
        peak_memories.append(peak / 1024 / 1024)

        # Clean up
        del df
        del lf
        gc.collect()

    # Average results
    avg_time = sum(times) / len(times)
    avg_memory = sum(peak_memories) / len(peak_memories)

    return BenchmarkResult(
        scenario=scenario_name,
        use_streaming=use_streaming,
        rows=num_rows,
        time_seconds=avg_time,
        peak_memory_mb=avg_memory,
    )


def run_ipc_file_benchmark(
    num_rows: int,
    use_streaming: bool,
    scenario_name: str,
) -> BenchmarkResult:
    """Run benchmark simulating IPC file reads with scan_ipc.

    Args:
        num_rows: Number of rows in test data
        use_streaming: Whether to use streaming engine
        scenario_name: Name for this scenario

    Returns:
        BenchmarkResult with timing and memory metrics
    """
    import tempfile
    from pathlib import Path

    # Force garbage collection
    gc.collect()

    # Create temp IPC file
    df = pl.DataFrame({
        "open_time": [datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i) for i in range(num_rows)],
        "open": [42000.0 + (i % 100) for i in range(num_rows)],
        "high": [42100.0 + (i % 100) for i in range(num_rows)],
        "low": [41900.0 + (i % 100) for i in range(num_rows)],
        "close": [42050.0 + (i % 100) for i in range(num_rows)],
        "volume": [1000.0 + (i % 500) for i in range(num_rows)],
    })

    with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as f:
        temp_path = Path(f.name)
        df.write_ipc(temp_path)

    # Start memory tracking
    tracemalloc.start()

    start = time.perf_counter()

    # Use scan_ipc (lazy) with predicate pushdown
    lf = pl.scan_ipc(temp_path)
    lf = lf.filter(pl.col("volume") > 500)

    if use_streaming:
        result_df = lf.collect(engine="streaming")
    else:
        result_df = lf.collect()

    elapsed = time.perf_counter() - start

    # Get memory stats
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    rows = len(result_df)

    # Cleanup
    temp_path.unlink()

    return BenchmarkResult(
        scenario=scenario_name,
        use_streaming=use_streaming,
        rows=rows,
        time_seconds=elapsed,
        peak_memory_mb=peak / 1024 / 1024,
    )


def format_results(results: list[BenchmarkResult]) -> str:
    """Format benchmark results as a table."""
    lines = [
        "",
        "=" * 105,
        "PERFORMANCE BENCHMARK: Polars Streaming Engine vs In-Memory Engine",
        "=" * 105,
        "",
        f"{'Scenario':<25} {'Engine':<12} {'Rows':>12} {'Time (ms)':>12} {'Memory (MB)':>14} {'Speedup':>10}",
        "-" * 105,
    ]

    # Group by scenario to calculate speedup
    scenarios = {}
    for r in results:
        if r.scenario not in scenarios:
            scenarios[r.scenario] = {}
        scenarios[r.scenario][r.use_streaming] = r

    for _scenario_name, runs in scenarios.items():
        inmemory_result = runs.get(False)
        streaming_result = runs.get(True)

        if inmemory_result:
            speedup = "-"
            lines.append(
                f"{inmemory_result.scenario:<25} {'In-Memory':<12} {inmemory_result.rows:>12,} "
                f"{inmemory_result.time_seconds * 1000:>12.2f} {inmemory_result.peak_memory_mb:>14.2f} {speedup:>10}"
            )

        if streaming_result:
            if inmemory_result and inmemory_result.time_seconds > 0:
                speedup_val = inmemory_result.time_seconds / streaming_result.time_seconds
                speedup = f"{speedup_val:.2f}x"
            else:
                speedup = "-"
            lines.append(
                f"{streaming_result.scenario:<25} {'Streaming':<12} {streaming_result.rows:>12,} "
                f"{streaming_result.time_seconds * 1000:>12.2f} {streaming_result.peak_memory_mb:>14.2f} {speedup:>10}"
            )

        lines.append("-" * 105)

    # Summary
    inmemory_results = [r for r in results if not r.use_streaming]
    streaming_results = [r for r in results if r.use_streaming]

    if inmemory_results and streaming_results:
        avg_inmemory_time = sum(r.time_seconds for r in inmemory_results) / len(inmemory_results)
        avg_streaming_time = sum(r.time_seconds for r in streaming_results) / len(streaming_results)
        avg_inmemory_mem = sum(r.peak_memory_mb for r in inmemory_results) / len(inmemory_results)
        avg_streaming_mem = sum(r.peak_memory_mb for r in streaming_results) / len(streaming_results)

        lines.extend(
            [
                "",
                "SUMMARY",
                "-" * 50,
                f"Average Time   - In-Memory: {avg_inmemory_time * 1000:.2f}ms, Streaming: {avg_streaming_time * 1000:.2f}ms",
                f"Average Memory - In-Memory: {avg_inmemory_mem:.2f}MB, Streaming: {avg_streaming_mem:.2f}MB",
            ]
        )

        if avg_streaming_time > 0:
            lines.append(f"Overall Speedup: {avg_inmemory_time / avg_streaming_time:.2f}x")

        if avg_inmemory_mem > 0:
            mem_diff = ((avg_inmemory_mem - avg_streaming_mem) / avg_inmemory_mem * 100)
            lines.append(f"Memory Difference: {mem_diff:.1f}%")

    return "\n".join(lines)


def main():
    """Run all benchmarks."""
    print("=" * 70)
    print("BENCHMARK: Polars Streaming Engine vs In-Memory Engine")
    print("=" * 70)

    results = []

    # Part 1: Synthetic data benchmarks
    print("\n[PART 1] Synthetic Data Benchmarks (3 iterations averaged)")
    print("-" * 50)

    synthetic_scenarios = [
        ("Tiny (1K rows)", 1_000),
        ("Small (10K rows)", 10_000),
        ("Medium (100K rows)", 100_000),
        ("Large (500K rows)", 500_000),
        ("XL (1M rows)", 1_000_000),
    ]

    for scenario_name, num_rows in synthetic_scenarios:
        print(f"\nRunning: {scenario_name}")

        # In-memory engine
        print("  - In-Memory engine...")
        inmemory_result = run_benchmark(num_rows, False, scenario_name)
        results.append(inmemory_result)
        print(f"    {inmemory_result.rows:,} rows in {inmemory_result.time_seconds * 1000:.2f}ms, {inmemory_result.peak_memory_mb:.2f}MB")

        # Streaming engine
        print("  - Streaming engine...")
        streaming_result = run_benchmark(num_rows, True, scenario_name)
        results.append(streaming_result)
        print(f"    {streaming_result.rows:,} rows in {streaming_result.time_seconds * 1000:.2f}ms, {streaming_result.peak_memory_mb:.2f}MB")

    # Part 2: Real cache read benchmarks
    print("\n" + "=" * 70)
    print("[PART 2] Real Cache Read Benchmarks")
    print("-" * 50)

    cache_scenarios = [
        ("Cache 7d", 7),
        ("Cache 30d", 30),
        ("Cache 90d", 90),
        ("Cache 180d", 180),
    ]

    for scenario_name, days in cache_scenarios:
        print(f"\nRunning: {scenario_name}")

        # In-memory engine
        print("  - In-Memory engine...")
        inmemory_result = run_cache_read_benchmark(days, False, scenario_name)
        if inmemory_result:
            results.append(inmemory_result)
            print(f"    {inmemory_result.rows:,} rows in {inmemory_result.time_seconds * 1000:.2f}ms, {inmemory_result.peak_memory_mb:.2f}MB")
        else:
            print("    Cache not available")

        # Streaming engine
        print("  - Streaming engine...")
        streaming_result = run_cache_read_benchmark(days, True, scenario_name)
        if streaming_result:
            results.append(streaming_result)
            print(f"    {streaming_result.rows:,} rows in {streaming_result.time_seconds * 1000:.2f}ms, {streaming_result.peak_memory_mb:.2f}MB")
        else:
            print("    Cache not available")

    # Print formatted results
    print(format_results(results))

    # Save results to file
    output_file = "/Users/terryli/eon/crypto-kline-vision-data/tmp/benchmark_streaming_results.txt"
    with open(output_file, "w") as f:
        f.write(format_results(results))
        f.write(f"\n\nBenchmark completed at: {datetime.now(timezone.utc).isoformat()}\n")
    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
