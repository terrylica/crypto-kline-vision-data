# Performance Benchmarks

**Hub**: [docs/](../CLAUDE.md) | **Root**: [CLAUDE.md](../../CLAUDE.md)

Comprehensive performance benchmarks for Crypto Kline Vision Data v2.0+.

## Executive Summary

| Comparison                 | Winner    | Speedup      | Memory Savings |
| -------------------------- | --------- | ------------ | -------------- |
| **Polars vs Pandas FCP**   | Polars    | 6.35x faster | 92.1% less     |
| **Streaming vs In-Memory** | Streaming | 1.42x faster | 49.5% less     |

## Benchmarks

### 1. Polars Pipeline vs Pandas FCP Path

Compares the new Polars-based FCP pipeline against the legacy Pandas path.

**Script**: [scripts/benchmark_pandas_vs_polars.py](scripts/benchmark_pandas_vs_polars.py)
**Results**: [results/benchmark_results.txt](results/benchmark_results.txt)

| Scenario    | Rows  | Pandas Time | Polars Time | Speedup | Pandas Memory | Polars Memory |
| ----------- | ----- | ----------- | ----------- | ------- | ------------- | ------------- |
| Small (1d)  | 24    | 0.353s      | 0.030s      | 11.64x  | 64.74 MB      | 0.62 MB       |
| Medium (7d) | 168   | 0.041s      | 0.033s      | 1.24x   | 0.62 MB       | 0.62 MB       |
| Large (30d) | 720   | 0.105s      | 0.062s      | 1.69x   | 1.02 MB       | 0.62 MB       |
| XL (90d)    | 2,160 | 1.774s      | 0.168s      | 10.53x  | 5.38 MB       | 1.49 MB       |
| XXL (180d)  | 4,320 | 1.991s      | 0.377s      | 5.29x   | 6.58 MB       | 2.82 MB       |

**Key Findings**:

- Cold start (first request) shows dramatic improvement due to avoiding pandas DataFrame initialization overhead
- Polars excels at scale - lazy evaluation and streaming engine shine with larger datasets
- Memory improvement ranges from 39% to 99% depending on scenario

### 2. Streaming Engine vs In-Memory Engine

Compares Polars `.collect(engine='streaming')` vs `.collect()` (in-memory).

**Script**: [scripts/benchmark_streaming_real_data.py](scripts/benchmark_streaming_real_data.py)
**Results**: [results/benchmark_streaming_complete.txt](results/benchmark_streaming_complete.txt)

#### Part 1: Synthetic Data (Isolated Streaming Test)

| Scenario  | In-Memory | Streaming | Result           |
| --------- | --------- | --------- | ---------------- |
| 10K rows  | 5.51ms    | 3.50ms    | Streaming faster |
| 100K rows | 1.33ms    | 1.33ms    | Equal            |
| 500K rows | 3.67ms    | 4.00ms    | In-Memory faster |
| 1M rows   | 7.49ms    | 9.29ms    | In-Memory faster |
| 2M rows   | 18.17ms   | 20.51ms   | In-Memory faster |

**Summary**: In-Memory is 1.07x faster for pure CPU-bound operations

#### Part 2: Real CKVD Data (Full FCP Flow)

| Scenario | In-Memory | Streaming | Speedup   |
| -------- | --------- | --------- | --------- |
| 7 days   | 422.80ms  | 40.65ms   | **10.4x** |
| 30 days  | 94.77ms   | 83.66ms   | 1.13x     |
| 90 days  | 249.35ms  | 240.45ms  | 1.04x     |
| 180 days | 645.25ms  | 629.11ms  | 1.03x     |

**Summary**: Streaming is 1.42x faster, uses 49.5% less memory

**Key Insight**: The streaming engine excels in I/O-bound scenarios (real FCP data retrieval) rather than pure CPU-bound synthetic operations.

## Why Polars + Streaming is Default

1. **Lazy Evaluation**: Operations are optimized and combined before execution
2. **Predicate Pushdown**: Filters applied at scan time, not after loading
3. **Streaming Engine**: `engine='streaming'` processes data in batches
4. **Zero-Copy Operations**: Arrow IPC files read directly without copying
5. **No Index Overhead**: Unlike pandas, Polars doesn't maintain a separate index

## Configuration

### Default (Recommended)

The Polars pipeline is always active — no configuration needed.
The `USE_POLARS_PIPELINE` flag was removed (see CHANGELOG for details).

### For Maximum Performance

```python
# Use return_polars=True for zero-copy output
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start,
    end_time=end,
    interval=Interval.HOUR_1,
    return_polars=True  # Skip pandas conversion
)
```

### Legacy Pandas Path

The legacy pandas FCP path has been removed. The Polars pipeline is unconditional.

## Running Benchmarks

```bash
# Pandas vs Polars comparison
uv run -p 3.13 python docs/benchmarks/scripts/benchmark_pandas_vs_polars.py

# Streaming vs In-Memory comparison
uv run -p 3.13 python docs/benchmarks/scripts/benchmark_streaming_real_data.py
```

## Test Environment

- **CKVD Version**: 2.0.2
- **Platform**: macOS darwin/arm64
- **Python**: 3.13
- **Date**: 2026-02-05

## Related

- [Performance Summary](PERFORMANCE_SUMMARY.md) - Detailed executive summary
- [FCP Protocol](/docs/adr/2025-01-30-failover-control-protocol.md) - Architecture decision record
- [Memory Efficiency Plan](/.claude/plans/gleaming-frolicking-engelbart.md) - Implementation plan
