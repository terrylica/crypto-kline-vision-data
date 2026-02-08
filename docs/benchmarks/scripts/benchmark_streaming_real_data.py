#!/usr/bin/env python3
"""Performance benchmark: Streaming vs In-Memory Engine with REAL data.

This script tests the Polars `.collect(engine='streaming')` vs `.collect()` (in-memory)
using actual CKVD data fetches to show real-world performance differences.

KEY INSIGHT:
- Both paths still use FCP (Cache → Vision → REST) for data retrieval
- The difference is HOW Polars processes the data after retrieval:
  * In-Memory: Load all data, then process (higher peak memory)
  * Streaming: Process in batches (lower peak memory, sometimes faster)
"""

import gc
import importlib
import os
import time
import tracemalloc
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

# Set environment variables BEFORE importing CKVD
os.environ["CKVD_LOG_LEVEL"] = "ERROR"


class BenchmarkResult(NamedTuple):
    """Result from a single benchmark run."""

    scenario: str
    engine: str
    rows: int
    time_seconds: float
    peak_memory_mb: float


def run_dsm_benchmark(
    days: int,
    use_streaming: bool,
    scenario_name: str,
) -> BenchmarkResult:
    """Run benchmark using full CKVD data fetch.

    This tests the complete FCP flow with streaming or in-memory collect.
    """
    # Set streaming preference via environment variable
    os.environ["CKVD_USE_POLARS_STREAMING"] = str(use_streaming).lower()

    # Reload config to pick up env var
    from ckvd.utils import config
    importlib.reload(config)

    from ckvd import DataProvider, CryptoKlineVisionData, Interval, MarketType

    gc.collect()

    # Time range (historical)
    end_time = datetime.now(timezone.utc) - timedelta(days=3)
    start_time = end_time - timedelta(days=days)

    # Start memory tracking
    tracemalloc.start()
    start = time.perf_counter()

    # Create manager with Polars pipeline enabled
    manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

    # Fetch data - internally uses PolarsDataPipeline with streaming or not
    df = manager.get_data(
        symbol="BTCUSDT",
        start_time=start_time,
        end_time=end_time,
        interval=Interval.HOUR_1,
        return_polars=True,
    )

    manager.close()

    elapsed = time.perf_counter() - start
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    rows = len(df) if df is not None else 0
    engine = "Streaming" if use_streaming else "In-Memory"

    return BenchmarkResult(
        scenario=scenario_name,
        engine=engine,
        rows=rows,
        time_seconds=elapsed,
        peak_memory_mb=peak / 1024 / 1024,
    )


def run_synthetic_polars_benchmark(
    num_rows: int,
    use_streaming: bool,
    scenario_name: str,
    num_iterations: int = 3,
) -> BenchmarkResult:
    """Run benchmark with synthetic data to isolate streaming engine difference.

    This creates LazyFrames and compares collect() engines directly.
    """
    import polars as pl

    times = []
    peak_memories = []

    for _ in range(num_iterations):
        gc.collect()

        # Create LazyFrame with OHLCV-like data
        base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
        data = {
            "open_time": [base_time + timedelta(hours=i) for i in range(num_rows)],
            "open": [42000.0 + (i % 100) for i in range(num_rows)],
            "high": [42100.0 + (i % 100) for i in range(num_rows)],
            "low": [41900.0 + (i % 100) for i in range(num_rows)],
            "close": [42050.0 + (i % 100) for i in range(num_rows)],
            "volume": [1000.0 + (i % 500) for i in range(num_rows)],
            "_data_source": ["CACHE" for _ in range(num_rows)],
        }
        lf = pl.LazyFrame(data)

        # Apply typical FCP merge operations
        lf = lf.filter(pl.col("volume") > 500)
        lf = lf.with_columns([
            (pl.col("high") - pl.col("low")).alias("range"),
            ((pl.col("close") - pl.col("open")) / pl.col("open") * 100).alias("pct_change"),
        ])
        lf = lf.sort("open_time")

        tracemalloc.start()
        start = time.perf_counter()

        df = lf.collect(engine="streaming") if use_streaming else lf.collect()

        elapsed = time.perf_counter() - start
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        times.append(elapsed)
        peak_memories.append(peak / 1024 / 1024)

        del df
        del lf

    avg_time = sum(times) / len(times)
    avg_memory = sum(peak_memories) / len(peak_memories)
    engine = "Streaming" if use_streaming else "In-Memory"

    return BenchmarkResult(
        scenario=scenario_name,
        engine=engine,
        rows=num_rows,
        time_seconds=avg_time,
        peak_memory_mb=avg_memory,
    )


def format_results(results: list[BenchmarkResult], title: str) -> str:
    """Format benchmark results as a table."""
    lines = [
        "",
        "=" * 100,
        title,
        "=" * 100,
        "",
        f"{'Scenario':<25} {'Engine':<12} {'Rows':>12} {'Time (ms)':>12} {'Memory (MB)':>14}",
        "-" * 100,
    ]

    # Group by scenario
    scenarios = {}
    for r in results:
        if r.scenario not in scenarios:
            scenarios[r.scenario] = []
        scenarios[r.scenario].append(r)

    for _scenario_name, runs in scenarios.items():
        for r in runs:
            lines.append(
                f"{r.scenario:<25} {r.engine:<12} {r.rows:>12,} "
                f"{r.time_seconds * 1000:>12.2f} {r.peak_memory_mb:>14.2f}"
            )
        lines.append("-" * 100)

    # Summary
    inmemory = [r for r in results if r.engine == "In-Memory"]
    streaming = [r for r in results if r.engine == "Streaming"]

    if inmemory and streaming:
        avg_inmem_time = sum(r.time_seconds for r in inmemory) / len(inmemory)
        avg_stream_time = sum(r.time_seconds for r in streaming) / len(streaming)
        avg_inmem_mem = sum(r.peak_memory_mb for r in inmemory) / len(inmemory)
        avg_stream_mem = sum(r.peak_memory_mb for r in streaming) / len(streaming)

        lines.extend([
            "",
            "SUMMARY",
            "-" * 50,
            f"Avg Time   - In-Memory: {avg_inmem_time * 1000:.2f}ms, Streaming: {avg_stream_time * 1000:.2f}ms",
            f"Avg Memory - In-Memory: {avg_inmem_mem:.2f}MB, Streaming: {avg_stream_mem:.2f}MB",
        ])

        if avg_stream_time > 0 and avg_inmem_time > 0:
            if avg_stream_time < avg_inmem_time:
                lines.append(f"Streaming is {avg_inmem_time / avg_stream_time:.2f}x faster")
            else:
                lines.append(f"In-Memory is {avg_stream_time / avg_inmem_time:.2f}x faster")

        if avg_inmem_mem > 0:
            mem_savings = (avg_inmem_mem - avg_stream_mem) / avg_inmem_mem * 100
            if mem_savings > 0:
                lines.append(f"Streaming uses {mem_savings:.1f}% less memory")
            else:
                lines.append(f"In-Memory uses {-mem_savings:.1f}% less memory")

    return "\n".join(lines)


def main():
    """Run all benchmarks."""
    print("=" * 70)
    print("BENCHMARK: Streaming vs In-Memory Engine")
    print("=" * 70)
    print()
    print("KEY INSIGHT:")
    print("Both approaches use the same FCP (Cache → Vision → REST) flow.")
    print("The streaming engine affects HOW Polars processes data:")
    print("  - In-Memory: Load all data into memory, then process")
    print("  - Streaming: Process data in batches (lower peak memory)")
    print()

    all_output = []

    # Part 1: Synthetic data (isolated streaming engine test)
    print("\n[PART 1] Synthetic Data - Isolating Streaming Engine Effect")
    print("-" * 60)

    synthetic_results = []
    synthetic_scenarios = [
        ("10K rows", 10_000),
        ("100K rows", 100_000),
        ("500K rows", 500_000),
        ("1M rows", 1_000_000),
        ("2M rows", 2_000_000),
    ]

    for scenario_name, num_rows in synthetic_scenarios:
        print(f"\nRunning: {scenario_name}")

        inmem = run_synthetic_polars_benchmark(num_rows, False, scenario_name)
        stream = run_synthetic_polars_benchmark(num_rows, True, scenario_name)

        synthetic_results.extend([inmem, stream])
        print(f"  In-Memory: {inmem.time_seconds * 1000:.2f}ms, {inmem.peak_memory_mb:.2f}MB")
        print(f"  Streaming: {stream.time_seconds * 1000:.2f}ms, {stream.peak_memory_mb:.2f}MB")

    output1 = format_results(synthetic_results, "PART 1: Synthetic Data - Streaming Engine Comparison")
    print(output1)
    all_output.append(output1)

    # Part 2: Real CKVD data fetch
    print("\n\n[PART 2] Real CKVD Data Fetch - Full FCP Flow")
    print("-" * 60)

    dsm_results = []
    dsm_scenarios = [
        ("7 days", 7),
        ("30 days", 30),
        ("90 days", 90),
        ("180 days", 180),
    ]

    for scenario_name, days in dsm_scenarios:
        print(f"\nRunning: {scenario_name}")

        inmem = run_dsm_benchmark(days, False, scenario_name)
        stream = run_dsm_benchmark(days, True, scenario_name)

        dsm_results.extend([inmem, stream])
        print(f"  In-Memory: {inmem.rows:,} rows, {inmem.time_seconds * 1000:.2f}ms, {inmem.peak_memory_mb:.2f}MB")
        print(f"  Streaming: {stream.rows:,} rows, {stream.time_seconds * 1000:.2f}ms, {stream.peak_memory_mb:.2f}MB")

    output2 = format_results(dsm_results, "PART 2: Real CKVD Data - Full FCP Flow with Streaming")
    print(output2)
    all_output.append(output2)

    # Save all results
    output_file = "/Users/terryli/eon/crypto-kline-vision-data/tmp/benchmark_streaming_complete.txt"
    with open(output_file, "w") as f:
        f.write("\n\n".join(all_output))
        f.write(f"\n\nBenchmark completed at: {datetime.now(timezone.utc).isoformat()}\n")
    print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()
