# Overview

This document details the timestamp behavior of the Binance API, specifically focusing on 1-second kline data retrieval. Understanding these behaviors is crucial for accurate data collection and processing.

## Key Behaviors

### 1. Millisecond Precision

#### Timestamp Alignment

- The API automatically aligns timestamps to second boundaries
- Start timestamps are rounded up to the next second
- End timestamps are rounded down to the last complete second
- Millisecond components are handled as follows:
  - `000` ms: Exact second boundary
  - `001-999` ms: Rounded to nearest second

#### Bar Boundaries

- Open time: Always at exact second boundary (`.000` ms)
- Close time: Always at `.999` ms of the same second
- Example:

  ```python
  bar_open = "2024-01-01 00:00:00.000"
  bar_close = "2024-01-01 00:00:00.999"
  ```

### 2. Timestamp Inclusivity

#### Request Windows

- Start time is inclusive (`>=`)
- End time is inclusive (`<=`)
- Minimum window: 1 second
- Zero-length windows (start = end) return no data

#### Edge Cases

- Single millisecond before second: Included in next second's bar
- Single millisecond after second: Included in current second's bar
- Consecutive milliseconds across second boundary: Split into separate bars

### 3. Bar Duration

#### Standard Behavior

- Each bar represents exactly 1 second
- Bar duration = 999 milliseconds (from `.000` to `.999`)
- No gaps between consecutive bars
- Example sequence:

   ```csv
   Bar 1: 00:00:00.000 -> 00:00:00.999
   Bar 2: 00:00:01.000 -> 00:00:01.999
   Bar 3: 00:00:02.000 -> 00:00:02.999
   ```

#### Special Cases

- Last bar of request window may be incomplete
- Real-time data may have partial bars
- Historical data always has complete bars

### 4. Chunk Handling

#### Chunk Size Calculation

- Standard chunk: 1000 records
- Last chunk may be partial
- Chunk boundaries must align with second boundaries
- Example:

  ```python
  # For a 2500-second window:
  chunk1 = records[0:1000]     # 1000 records
  chunk2 = records[1000:2000]  # 1000 records
  chunk3 = records[2000:2500]  # 500 records
  ```

## Implementation Considerations

### 1. Time Window Validation

```python
def validate_time_window(start_time: datetime, end_time: datetime) -> Tuple[datetime, datetime]:
    # Ensure UTC timezone
    start_time = start_time.astimezone(timezone.utc)
    end_time = end_time.astimezone(timezone.utc)
    
    # Align to second boundaries
    aligned_start = start_time.replace(microsecond=0) + timedelta(seconds=1)
    aligned_end = end_time.replace(microsecond=0)
    
    return aligned_start, aligned_end
```

### 2. Chunk Size Calculation

```python
def calculate_chunk_size(start_ms: int, end_ms: int) -> int:
    time_span = end_ms - start_ms
    return min(1000, time_span // 1000)  # Convert ms to seconds
```

## Best Practices

1. **Time Alignment**
   - Always align request windows to second boundaries
   - Account for millisecond rounding behavior
   - Validate aligned timestamps before requests

2. **Chunk Management**
   - Calculate exact chunk sizes including partial chunks
   - Verify chunk boundaries align with seconds
   - Handle last chunk separately

3. **Data Validation**
   - Verify bar completeness
   - Check for gaps between bars
   - Validate open/close time relationships

4. **Error Handling**
   - Handle timezone conversions explicitly
   - Account for partial bars in real-time data
   - Validate timestamp ranges before requests

## Common Pitfalls

1. **Incorrect Time Window Calculation**
   - Not accounting for inclusive end times
   - Ignoring millisecond precision
   - Assuming zero-length windows return data

2. **Chunk Size Miscalculation**
   - Not handling partial last chunks
   - Incorrect boundary calculations
   - Ignoring millisecond components

3. **Data Quality Issues**
   - Not validating bar completeness
   - Missing gaps in data
   - Incorrect timestamp comparisons

## References

- [Binance API Documentation](https://binance-docs.github.io/apidocs/spot/en/)
- [Time Alignment Utilities](../utils/time_alignment.py)
- [Market Constraints](../utils/market_constraints.py)
- [Timestamp Behavior Tests](../tests/test_binance_timestamp_behavior.py)
