# Troubleshooting Guide

Common issues and solutions for Crypto Kline Vision Data.

## Quick Diagnostics

```bash
# Check imports
uv run -p 3.13 python -c "from ckvd import CryptoKlineVisionData; print('OK')"

# Check cache status
mise run cache:stats

# Enable debug logging
CKVD_LOG_LEVEL=DEBUG uv run -p 3.13 python your_script.py
```

## Common Issues

### Empty DataFrame Returned

**Symptoms**: `get_data()` returns DataFrame with 0 rows.

**Causes**:

1. Wrong symbol format for market type
2. Requesting future timestamps
3. Date range with no trading data

**Solutions**:

```python
# Check symbol format - raises ValueError with suggestion if invalid
from ckvd import MarketType
from ckvd.utils.market_constraints import validate_symbol_for_market_type

try:
    validate_symbol_for_market_type("BTCUSDT", MarketType.FUTURES_COIN)
except ValueError as e:
    print(e)  # "Invalid symbol format... Try using 'BTCUSD_PERP' instead."

# Check time range is in past
from datetime import datetime, timezone
assert end_time <= datetime.now(timezone.utc), "Cannot request future data"
```

### HTTP 403 Forbidden

**Symptoms**: Vision or REST API returns 403 error.

**Causes**:

1. Requesting data for future timestamps
2. Symbol doesn't exist on exchange
3. IP banned (rare)

**Solutions**:

- Verify timestamps are UTC and in the past
- Check symbol exists on Binance
- Wait 15 minutes if IP banned

### HTTP 429 Rate Limited

**Symptoms**: REST API returns 429 error.

**Causes**: Exceeded weight/minute limit (Spot: 6,000 / Futures: 2,400).

**Solutions**:

- Wait 60 seconds before retrying
- Use Vision API for bulk historical data
- Enable caching to reduce API calls

### Naive Datetime Errors

**Symptoms**: `TypeError: can't compare offset-naive and offset-aware datetimes`

**Cause**: Using `datetime.now()` instead of `datetime.now(timezone.utc)`.

**Solution**:

```python
# Wrong
from datetime import datetime
now = datetime.now()

# Correct
from datetime import datetime, timezone
now = datetime.now(timezone.utc)
```

### Import Errors

**Symptoms**: `ModuleNotFoundError: No module named 'ckvd'`

**Solutions**:

```bash
# Reinstall in editable mode
uv pip install -e ".[dev]"

# Or sync dependencies
uv sync --dev
```

### Cache Corruption

**Symptoms**: Unexpected data, partial results, or read errors from cache.

**Solutions**:

```bash
# Clear cache
mise run cache:clear

# Or manually (macOS path via platformdirs)
rm -rf ~/Library/Caches/crypto-kline-vision-data
```

## Debug Mode

Enable verbose logging to see FCP decisions:

```python
import os
os.environ["CKVD_LOG_LEVEL"] = "DEBUG"

from ckvd import CryptoKlineVisionData, DataProvider, MarketType

# Now get_data() logs:
# DEBUG - Cache hit for 2024-01-01
# DEBUG - Cache miss for 2024-01-02, trying Vision
# DEBUG - Vision API downloaded 2024-01-02
# DEBUG - REST fallback for 2024-01-03 (recent data)
```

## FCP Source Verification

Force specific data source for debugging by passing `enforce_source` to `get_data()`:

```python
from datetime import datetime, timedelta, timezone
from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval
from ckvd.core.sync.ckvd_types import DataSource

manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

end = datetime.now(timezone.utc)
start = end - timedelta(days=7)

# Force Vision only (skip cache)
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start,
    end_time=end,
    interval=Interval.HOUR_1,
    enforce_source=DataSource.VISION
)

# Force REST only (skip cache and Vision)
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start,
    end_time=end,
    interval=Interval.HOUR_1,
    enforce_source=DataSource.REST
)

# Force cache only (offline mode - no API calls)
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start,
    end_time=end,
    interval=Interval.HOUR_1,
    enforce_source=DataSource.CACHE
)

manager.close()
```

## Cache Toggle Issues

### Cache Not Disabled by Environment Variable

**Symptoms**: `CKVD_ENABLE_CACHE=false` set but cache still active.

**Causes**:

1. Env var set **after** importing CKVD
2. Typo in variable name or value

**Solutions**:

```python
# Set BEFORE importing CKVD
import os
os.environ["CKVD_ENABLE_CACHE"] = "false"

from ckvd import CryptoKlineVisionData  # Now picks up env var

# Accepted disable values (case-insensitive): "false", "0", "no"
```

### enforce_source=CACHE + use_cache=False Contradiction

**Symptoms**: `RuntimeError: Cannot use enforce_source=DataSource.CACHE when use_cache=False`

**Cause**: Requesting cache-only mode on a manager with cache disabled.

**Solution**: Use a compatible combination:

| `enforce_source` | `use_cache=False` | Result                       |
| ---------------- | ----------------- | ---------------------------- |
| `AUTO`           | Works             | Vision -> REST (skips cache) |
| `VISION`         | Works             | Vision API only              |
| `REST`           | Works             | REST API only                |
| `CACHE`          | **RuntimeError**  | Logical contradiction        |

### Cache Disabled Unexpectedly

**Symptoms**: Data always fetched from API, no cache files created.

**Diagnostic**:

```python
# Check if env var is set somewhere
import os
print(f"CKVD_ENABLE_CACHE = {os.environ.get('CKVD_ENABLE_CACHE', '(not set)')}")

# Check manager state
manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
print(f"use_cache = {manager.use_cache}")
manager.close()
```

See the [README.md](/README.md#logging-control) for logging and cache environment variables.

## Streaming Issues

Streaming is a separate channel from FCP (Cache→Vision→REST). Issues below are specific to real-time kline updates via WebSocket.

### Missing [streaming] Extras

**Symptoms**: `ModuleNotFoundError: No module named 'websockets'` or other streaming dependencies when calling `create_stream()`.

**Cause**: Package installed without streaming dependencies.

**Solutions**:

```bash
# Install with streaming extras
pip install 'crypto-kline-vision-data[streaming]'

# Or with uv
uv add 'crypto-kline-vision-data[streaming]'
```

### StreamConnectionError

**Symptoms**: WebSocket connection fails immediately or during subscription.

**Causes**:

1. Network connectivity issue
2. Incorrect WebSocket URL
3. Binance server temporarily unavailable

**Solutions**:

- Check network connectivity
- Verify Binance API status (<https://status.binance.com>)
- Automatic retry with backoff is built-in (5 attempts via stamina library)

### StreamReconnectExhaustedError

**Symptoms**: `StreamReconnectExhaustedError` after attempting reconnection.

**Cause**: 5 reconnect attempts all failed — prolonged network outage or Binance downtime.

**Solution**:

- Restart the streaming session
- Check Binance status and network connectivity
- Increase `max_reconnect_attempts` in `create_stream()` if transient failures expected

### Dropped Messages

**Symptoms**: `stream.dropped_count > 0` after iterating over updates.

**Cause**: Message queue (default 1000) filled faster than consumer processes them.

**Solutions**:

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType

manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

# Increase queue size
async with manager.create_stream(queue_maxsize=5000) as stream:
    await stream.subscribe("BTCUSDT", "1h")
    # Process updates faster (avoid blocking operations in loop)
    async for update in stream:
        process_quickly(update)
```

### No Updates Despite Confirmed Subscription

**Symptoms**: Subscription confirmed but no `KlineUpdate` events received.

**Cause**: `confirmed_only=True` (default) filters out mid-candle updates. Market is open with no closed candles in the subscribed interval.

**Solutions**:

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType

manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

# See mid-candle updates (default confirmed_only=True)
async with manager.create_stream(confirmed_only=False) as stream:
    await stream.subscribe("BTCUSDT", "1h")
    async for update in stream:
        print(update)

# Or wait for candle close:
# 1h interval closes every hour at :00 UTC
# 5m interval closes every 5 minutes past the hour (e.g., :05, :10, :15...)
```

### stream_data_sync() Blocks Forever

**Symptoms**: Synchronous streaming method `stream_data_sync()` never returns.

**Cause**: By design — streaming continues indefinitely until interrupt.

**Solutions**:

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType

manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

# Option 1: KeyboardInterrupt (Ctrl+C)
# stream_data_sync() runs an event loop and handles SIGINT

# Option 2: Max updates counter
count = 0
for update in manager.stream_data_sync("BTCUSDT", "1h"):
    print(update)
    count += 1
    if count >= 1000:
        break

# Option 3: Time-based exit
import time
start = time.time()
for update in manager.stream_data_sync("BTCUSDT", "1h"):
    if time.time() - start > 300:  # 5 minutes
        break

manager.close()
```

## Telemetry Issues

### No events.jsonl File Created

**Symptoms**: Running an example produces no `examples/logs/events.jsonl`.

**Causes**:

1. Example not using `_telemetry.py` (pre-telemetry example)
2. File permission issue on `examples/logs/` directory

**Solutions**:

```bash
# Verify telemetry output
uv run -p 3.13 python examples/quick_start.py
cat examples/logs/events.jsonl | jq . | head -5
```

### Telemetry Stops After CryptoKlineVisionData.create()

**Symptoms**: Events stop appearing in `events.jsonl` after manager creation.

**Cause**: CKVD's `loguru_setup.py` calls `logger.remove()` which destroys all sinks.

**Solution**: This is handled automatically by `ResilientLogger` in `_telemetry.py`. Ensure you use `init_telemetry()` (not raw loguru) in examples. See [examples/CLAUDE.md](/examples/CLAUDE.md#resilientlogger) for details.

### Correlating Events Across Runs

Use `trace_id` to group events from a single run, and `span_id` to match fetch start/complete pairs:

```bash
# All events from one run
cat examples/logs/events.jsonl | jq 'select(.trace_id == "YOUR_TRACE_ID")'

# Match fetch spans
cat examples/logs/events.jsonl | jq 'select(.span_id == "cb72ce8f")'
```

## Reconciliation Issues

Reconciliation is opt-in (`reconciliation_enabled=True`) and automatically backfills klines missed during WebSocket disconnects via REST API.

### Reconciliation Not Triggering

**Symptoms**: Gaps in stream data after reconnect, no backfill.

**Causes**:

1. `reconciliation_enabled=False` (default — must opt in)
2. No `last_confirmed_open_time` established (disconnect before any closed candle)
3. Cooldown active from a recent reconciliation

**Solutions**:

```python
# Enable reconciliation
stream = manager.create_stream(reconciliation_enabled=True)

# Check stats
async with stream:
    await stream.subscribe("BTCUSDT", "1h")
    async for update in stream:
        if stream.reconciliation_stats:
            print(f"Requests: {stream.reconciliation_stats.total_requests}")
            print(f"Backfilled: {stream.reconciliation_stats.total_backfilled}")
```

### Too Many REST Calls (Rate Limiting)

**Symptoms**: `StreamReconciliationError` with cause `RateLimitError` in details.

**Cause**: Frequent disconnects triggering reconciliation faster than cooldown allows.

**Solutions**:

```python
# Increase cooldown (default 30s)
config = StreamConfig(
    market_type=MarketType.FUTURES_USDT,
    reconciliation_enabled=True,
    reconciliation_cooldown_seconds=60.0,  # 60s between REST calls per symbol
)
```

### Large Gaps Capped

**Symptoms**: Partial backfill — only some missing klines recovered.

**Cause**: Gap exceeds `reconciliation_max_gap_intervals` (default 1440 = 1 day of 1m candles).

**Solution**: Increase `reconciliation_max_gap_intervals` if longer gaps are expected (e.g., weekend outages).

## Getting Help

1. Check [FCP Protocol Reference](skills/ckvd-usage/references/fcp-protocol.md)
2. Review [Market Types](skills/ckvd-usage/references/market-types.md)
3. Enable debug logging and check output
4. Use the fcp-debugger agent or ckvd-fcp-monitor skill in Claude Code
