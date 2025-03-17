#!/usr/bin/env python
"""Integration tests for cache performance benchmarking.

Focus areas:
1. Basic cache hit/miss performance
2. Memory usage patterns
3. Data consistency verification
4. Cache vs. no-cache timing comparisons
"""

import pytest
import arrow
import pandas as pd
import time
import psutil
import os
from datetime import timedelta, datetime
from typing import Tuple, Dict, List, Any, AsyncGenerator
import pytest_asyncio

from utils.logger_setup import get_logger
from core.data_source_manager import DataSourceManager
from utils.market_constraints import Interval, MarketType

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)

# Test configuration
TEST_SYMBOL = "BTCUSDT"  # Use BTC for reliable data
TEST_INTERVAL = Interval.SECOND_1  # Focus on 1-second data

# Time constants for tests
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)


def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def log_performance_metrics(
    operation: str,
    start_time: float,
    end_time: float,
    start_memory: float,
    end_memory: float,
    df: pd.DataFrame,
) -> None:
    """Log detailed performance metrics for an operation."""
    duration = end_time - start_time
    memory_change = end_memory - start_memory

    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info(f"║ 📊 PERFORMANCE METRICS - {operation}")
    logger.info(
        "╠════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ ⏱️  Timing Metrics:")
    logger.info(f"║   • Total Duration: {duration:.4f} seconds")
    logger.info(f"║   • Per Record: {(duration * 1000 / len(df)):.4f} ms/record")
    logger.info("║")
    logger.info("║ 💾 Memory Metrics:")
    logger.info(f"║   • Initial Memory: {start_memory:.2f} MB")
    logger.info(f"║   • Final Memory: {end_memory:.2f} MB")
    logger.info(f"║   • Memory Change: {memory_change:+.2f} MB")
    logger.info("║")
    logger.info("║ 📈 Data Metrics:")
    logger.info(f"║   • Records Processed: {len(df):,}")
    logger.info(f"║   • Memory per Record: {(memory_change / len(df)):.4f} MB/record")
    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )


async def perform_data_fetch(
    manager: DataSourceManager,
    start_time: datetime,
    end_time: datetime,
    use_cache: bool,
) -> Tuple[pd.DataFrame, float, float, float, float]:
    """Perform data fetch and measure performance metrics."""
    start_memory = get_memory_usage()
    start_time_perf = time.perf_counter()

    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=start_time,
        end_time=end_time,
        use_cache=use_cache,
    )

    end_time_perf = time.perf_counter()
    end_memory = get_memory_usage()

    return df, start_time_perf, end_time_perf, start_memory, end_memory


@pytest_asyncio.fixture
async def manager() -> AsyncGenerator[DataSourceManager, None]:
    """Create DataSourceManager instance with fresh components."""
    async with DataSourceManager(market_type=MarketType.SPOT) as mgr:
        yield mgr


@pytest.mark.real
@pytest.mark.asyncio
async def test_basic_cache_performance(manager: DataSourceManager) -> None:
    """Test basic cache performance with cold and warm cache scenarios."""
    logger.info("")
    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 🧪 TEST CASE: Basic Cache Performance")
    logger.info(
        "╠════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 🎯 MOTIVATION:")
    logger.info(
        "║   Measuring and comparing performance characteristics between cache hits and misses"
    )
    logger.info(
        "║   to understand the performance impact of caching in typical usage scenarios."
    )
    logger.info("║")
    logger.info("║ 📋 TEST SEQUENCE:")
    logger.info("║   1. Cold cache fetch (cache miss)")
    logger.info("║   2. Warm cache fetch (cache hit)")
    logger.info("║   3. Performance comparison")
    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )

    # Test parameters
    base_time = arrow.utcnow().shift(days=-2)
    start_time = base_time.datetime
    end_time = base_time.shift(minutes=5).datetime

    # Cold cache fetch (cache miss)
    logger.info("Performing cold cache fetch (cache miss)...")
    df_cold, start_cold, end_cold, start_mem_cold, end_mem_cold = (
        await perform_data_fetch(manager, start_time, end_time, use_cache=True)
    )
    log_performance_metrics(
        "Cold Cache Fetch", start_cold, end_cold, start_mem_cold, end_mem_cold, df_cold
    )

    # Warm cache fetch (cache hit)
    logger.info("\nPerforming warm cache fetch (cache hit)...")
    df_warm, start_warm, end_warm, start_mem_warm, end_mem_warm = (
        await perform_data_fetch(manager, start_time, end_time, use_cache=True)
    )
    log_performance_metrics(
        "Warm Cache Fetch", start_warm, end_warm, start_mem_warm, end_mem_warm, df_warm
    )

    # Compare results
    cold_duration = end_cold - start_cold
    warm_duration = end_warm - start_warm
    speedup = cold_duration / warm_duration if warm_duration > 0 else float("inf")

    logger.info("")
    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 📊 CACHE PERFORMANCE COMPARISON")
    logger.info(
        "╠════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info(f"║ • Cold Cache Duration: {cold_duration:.4f} seconds")
    logger.info(f"║ • Warm Cache Duration: {warm_duration:.4f} seconds")
    logger.info(f"║ • Cache Speedup Factor: {speedup:.2f}x")
    logger.info("║")
    logger.info("║ Memory Impact:")
    logger.info(
        f"║ • Cold Cache Memory Change: {end_mem_cold - start_mem_cold:+.2f} MB"
    )
    logger.info(
        f"║ • Warm Cache Memory Change: {end_mem_warm - start_mem_warm:+.2f} MB"
    )
    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )

    # Verify data consistency
    pd.testing.assert_frame_equal(
        df_cold,
        df_warm,
        check_dtype=True,
        check_index_type=True,
        check_column_type=True,
    )


@pytest.mark.real
@pytest.mark.asyncio
async def test_cache_vs_no_cache_comparison(manager: DataSourceManager) -> None:
    """Compare performance between cached and non-cached data retrieval."""
    logger.info("")
    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 🧪 TEST CASE: Cache vs. No-Cache Comparison")
    logger.info(
        "╠════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 🎯 MOTIVATION:")
    logger.info(
        "║   Directly comparing performance characteristics between cached and non-cached data retrieval"
    )
    logger.info("║   to quantify the benefits and overhead of caching.")
    logger.info("║")
    logger.info("║ 📋 TEST SEQUENCE:")
    logger.info("║   1. No-cache fetch")
    logger.info("║   2. Cached fetch")
    logger.info("║   3. Performance comparison")
    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )

    # Test parameters
    base_time = arrow.utcnow().shift(days=-2)
    start_time = base_time.datetime
    end_time = base_time.shift(minutes=5).datetime

    # No-cache fetch
    logger.info("Performing no-cache fetch...")
    df_no_cache, start_no_cache, end_no_cache, start_mem_no_cache, end_mem_no_cache = (
        await perform_data_fetch(manager, start_time, end_time, use_cache=False)
    )
    log_performance_metrics(
        "No-Cache Fetch",
        start_no_cache,
        end_no_cache,
        start_mem_no_cache,
        end_mem_no_cache,
        df_no_cache,
    )

    # Cache fetch
    logger.info("\nPerforming cached fetch...")
    df_cache, start_cache, end_cache, start_mem_cache, end_mem_cache = (
        await perform_data_fetch(manager, start_time, end_time, use_cache=True)
    )
    log_performance_metrics(
        "Cached Fetch", start_cache, end_cache, start_mem_cache, end_mem_cache, df_cache
    )

    # Compare results
    no_cache_duration = end_no_cache - start_no_cache
    cache_duration = end_cache - start_cache
    performance_diff = ((no_cache_duration - cache_duration) / no_cache_duration) * 100

    logger.info("")
    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 📊 CACHE VS. NO-CACHE COMPARISON")
    logger.info(
        "╠════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info(f"║ • No-Cache Duration: {no_cache_duration:.4f} seconds")
    logger.info(f"║ • Cache Duration: {cache_duration:.4f} seconds")
    logger.info(f"║ • Performance Improvement: {performance_diff:+.2f}%")
    logger.info("║")
    logger.info("║ Memory Impact:")
    logger.info(
        f"║ • No-Cache Memory Change: {end_mem_no_cache - start_mem_no_cache:+.2f} MB"
    )
    logger.info(f"║ • Cache Memory Change: {end_mem_cache - start_mem_cache:+.2f} MB")
    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )

    # Verify data consistency
    pd.testing.assert_frame_equal(
        df_no_cache,
        df_cache,
        check_dtype=True,
        check_index_type=True,
        check_column_type=True,
    )


@pytest.mark.real
@pytest.mark.asyncio
async def test_geometric_range_performance(manager: DataSourceManager) -> None:
    """Test cache performance with geometrically increasing data ranges."""
    logger.info("")
    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 🧪 TEST CASE: Geometric Range Performance")
    logger.info(
        "╠════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 🎯 MOTIVATION:")
    logger.info(
        "║   Analyzing how cache performance scales with geometrically increasing data ranges"
    )
    logger.info(
        "║   to understand the relationship between data size and performance benefits."
    )
    logger.info("║")
    logger.info("║ 📋 TEST SEQUENCE:")
    logger.info("║   1. Test with 5-minute range")
    logger.info("║   2. Test with 15-minute range")
    logger.info("║   3. Test with 30-minute range")
    logger.info("║   4. Test with 1-hour range")
    logger.info("║   5. Performance scaling analysis")
    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )

    # Test ranges in minutes
    ranges = [5, 15, 30, 60]
    base_time = arrow.utcnow().shift(days=-2)
    results: List[Dict[str, Any]] = []

    for minutes in ranges:
        logger.info(f"\nTesting {minutes}-minute range...")
        start_time = base_time.datetime
        end_time = base_time.shift(minutes=minutes).datetime

        # Cold cache fetch
        logger.info(f"Performing cold cache fetch for {minutes}-minute range...")
        df_cold, start_cold, end_cold, start_mem_cold, end_mem_cold = (
            await perform_data_fetch(manager, start_time, end_time, use_cache=True)
        )
        cold_duration = end_cold - start_cold
        cold_memory = end_mem_cold - start_mem_cold
        log_performance_metrics(
            f"Cold Cache Fetch ({minutes}min)",
            start_cold,
            end_cold,
            start_mem_cold,
            end_mem_cold,
            df_cold,
        )

        # Warm cache fetch
        logger.info(f"Performing warm cache fetch for {minutes}-minute range...")
        df_warm, start_warm, end_warm, start_mem_warm, end_mem_warm = (
            await perform_data_fetch(manager, start_time, end_time, use_cache=True)
        )
        warm_duration = end_warm - start_warm
        warm_memory = end_mem_warm - start_mem_warm
        log_performance_metrics(
            f"Warm Cache Fetch ({minutes}min)",
            start_warm,
            end_warm,
            start_mem_warm,
            end_mem_warm,
            df_warm,
        )

        # Store results
        results.append(
            {
                "range_minutes": minutes,
                "records": len(df_cold),
                "cold_duration": cold_duration,
                "warm_duration": warm_duration,
                "cold_memory": cold_memory,
                "warm_memory": warm_memory,
                "speedup": (
                    cold_duration / warm_duration if warm_duration > 0 else float("inf")
                ),
                "memory_efficiency": (
                    cold_memory / warm_memory if warm_memory > 0 else float("inf")
                ),
            }
        )

        # Verify data consistency
        pd.testing.assert_frame_equal(
            df_cold,
            df_warm,
            check_dtype=True,
            check_index_type=True,
            check_column_type=True,
        )

    # Log scaling analysis
    logger.info("")
    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 📊 PERFORMANCE SCALING ANALYSIS")
    logger.info(
        "╠════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info(
        "║ Range  Records  Cold(s)  Warm(s)  Speedup  Cold Mem(MB)  Warm Mem(MB)  Mem.Eff"
    )
    logger.info(
        "║ ─────────────────────────────────────────────────────────────────────────────"
    )

    for result in results:
        range_min: int = result["range_minutes"]
        record_count: int = result["records"]
        cold_dur: float = result["cold_duration"]
        warm_dur: float = result["warm_duration"]
        speedup_val: float = result["speedup"]
        cold_mem: float = result["cold_memory"]
        warm_mem: float = result["warm_memory"]
        mem_eff: float = result["memory_efficiency"]

        logger.info(
            f"║ {range_min:3}m  {record_count:7}  {cold_dur:7.3f}  "
            f"{warm_dur:7.3f}  {speedup_val:7.2f}x  {cold_mem:11.2f}  "
            f"{warm_mem:11.2f}  {mem_eff:7.2f}x"
        )

    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════"
    )

    # Calculate and log performance trends
    record_sizes: List[int] = [r["records"] for r in results]
    speedups: List[float] = [r["speedup"] for r in results]
    mem_effs: List[float] = [r["memory_efficiency"] for r in results]

    logger.info("")
    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 📈 PERFORMANCE TRENDS")
    logger.info(
        "╠════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info(f"║ • Records Growth: {', '.join(f'{s:,}' for s in record_sizes)}")
    logger.info(f"║ • Speedup Trend: {', '.join(f'{s:.2f}x' for s in speedups)}")
    logger.info(
        f"║ • Memory Efficiency Trend: {', '.join(f'{m:.2f}x' for m in mem_effs)}"
    )
    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════"
    )
