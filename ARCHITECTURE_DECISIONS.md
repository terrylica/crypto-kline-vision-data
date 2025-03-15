# Architecture Decision Records

Technical findings and investigation results for the Binance Data Service module.

## ADR-001: Range Request Behavior on Binance Vision API

**Date**: 2024-03-19

### Test Results

```bash
# Test 1: HEAD request shows accept-ranges header
$ curl -I "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s/BTCUSDT-1s-2024-03-18.zip"
Response: accept-ranges: bytes present

# Test 2: Range request returns full file
$ curl -H "Range: bytes=1000000-2000000" -I "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s/BTCUSDT-1s-2024-03-18.zip"
Response: HTTP 200 instead of expected 206
Full content-length returned
```

### Technical Implications

1. Range requests return full file despite accept-ranges header
2. No partial content responses (HTTP 206) observed

### Implementation Note

Current implementation uses full file downloads. Range-based resumption not tested further.

## ADR Template for Future Decisions

**Date**: YYYY-MM-DD

### Test Results - Template

- What was tested?
- What were the exact responses?
- Include raw output

### Technical Implications - Template

- What are the direct technical consequences?

### Implementation Note - Template

- Current implementation approach
- What wasn't tested

## ADR-007: Timestamp Format Evolution in Binance Vision Data

### Context

During testing with 2025 data, we discovered that Binance Vision's timestamp format has evolved:

- Pre-2025: Timestamps are in milliseconds (13 digits)
- 2025 onwards: Timestamps are in microseconds (16 digits)

Additionally, we found consistent patterns in timestamp precision:

1. `open_time` values:
   - Always on second boundaries (microseconds = 0)
   - Format: YYYYMMDD HH:MM:SS.000000
   - Used as the canonical index in DataFrames

2. `close_time` values:
   - Full microsecond precision (ending in 999999)
   - Format: YYYYMMDD HH:MM:SS.999999
   - Consistently 0.999999 seconds after open_time
   - Used to verify microsecond precision support

### Decision

1. Implement automatic timestamp format detection based on digit count:
   - 13 digits -> milliseconds
   - 16 digits -> microseconds

2. Use `close_time` for microsecond precision validation since it contains the full precision.

3. Add validation functions:
   - `detect_timestamp_unit(sample_ts: int | str) -> TimestampUnit`
   - `validate_timestamp_unit(unit: TimestampUnit) -> None`

4. Preserve the natural precision patterns:
   - Keep `open_time` on second boundaries for index
   - Maintain full microsecond precision in `close_time`

### Consequences

#### Positive

- Handles both pre-2025 and 2025+ data formats seamlessly
- Preserves microsecond precision where available
- Automatic format detection reduces maintenance burden
- Validation ensures data integrity
- Natural alignment with Binance's data structure (second-aligned opens, precise closes)

#### Negative

- Additional overhead from format detection
- Need to maintain backward compatibility
- Must handle both precision levels in analysis code
- Potential confusion about different precision levels between open and close times

### Implementation Notes

1. Detection happens at data load time
2. Uses first row timestamp as format indicator
3. Validates format before conversion
4. Logs detected format for monitoring
5. Part of core validation pipeline
6. Maintains precision patterns:
   - Index (`open_time`) always on second boundaries
   - `close_time` preserves full microsecond precision

#### Test Coverage

New test file `test_vision_architecture.py` includes:

1. Unit tests for format detection
2. Unit tests for timestamp validation
3. Integration tests with actual data formats
4. Error case handling tests
5. Verification of precision patterns:
   - Second-aligned opens
   - Microsecond-precise closes
   - Consistent 0.999999s time deltas

#### Additional Testing Insights

1. Year Boundary Behavior:
   - Verified clean transition at 2024-2025 boundary
   - Last row of 2024 (23:59:59) uses 13-digit format
   - First row of 2025 (00:00:00) uses 16-digit format
   - No data gaps or overlaps at boundary

2. Data Consistency:
   - Early 2024 (January): 13-digit timestamps
   - Late 2024 (December): 13-digit timestamps
   - Early 2025 (January): 16-digit timestamps
   - Consistent format within each year

3. Price Data Integrity:
   - No anomalies in price data across format change
   - OHLCV values maintain precision
   - Trade counts and volumes unaffected

4. Performance Impact:
   - Format detection adds negligible overhead
   - No significant impact on data loading speed
   - Memory footprint unchanged by precision increase

### Related

- ADR-001: Range Request Behavior on Binance Vision API
