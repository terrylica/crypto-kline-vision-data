---
name: ckvd-fcp-monitor
description: Monitor and diagnose FCP (Failover Control Protocol) behavior, cache health, and data source performance. TRIGGERS - FCP issues, cache problems, slow data, Vision API errors, REST fallback, data source debugging.
argument-hint: "[symbol] [market-type]"
user-invocable: true
context: fork
allowed-tools: Read, Bash, Grep, Glob
adr: docs/adr/2025-01-30-failover-control-protocol.md
---

# FCP Monitor

Monitor and diagnose Failover Control Protocol behavior for: $ARGUMENTS

## Quick Diagnostics

```bash
# Check FCP behavior for a symbol
uv run -p 3.13 python docs/skills/ckvd-usage/scripts/diagnose_fcp.py BTCUSDT futures_usdt 1h

# Check cache health
uv run -p 3.13 python docs/skills/ckvd-fcp-monitor/scripts/cache_health.py

# Monitor FCP source distribution
uv run -p 3.13 python docs/skills/ckvd-fcp-monitor/scripts/fcp_stats.py --symbol BTCUSDT
```

## FCP Decision Flow

```
Request
    │
    ▼
┌─────────────────┐
│ 1. Cache Check  │ ──── Hit (99%) ───▶ Return (~1ms)
└─────────────────┘
    │ Miss (1%)
    ▼
┌─────────────────┐
│ 2. Vision API   │ ──── OK (95%) ────▶ Cache + Return (~2s)
│    (S3 bulk)    │
└─────────────────┘
    │ Fail (5%)
    ▼
┌─────────────────┐
│ 3. REST API     │ ──── OK (99%) ────▶ Return (~200ms)
│    (real-time)  │
└─────────────────┘
    │ Fail (1%)
    ▼
Raise DataSourceError
```

## Common Issues

### Cache Not Being Used

**Symptoms**: Always hitting REST, slow performance

**Diagnostics**:

```bash
# Check cache directory exists and has data
ls -la ~/.cache/ckvd/binance/futures_usdt/klines/daily/BTCUSDT/1h/

# Verify cache permissions
stat ~/.cache/ckvd/
```

**Solutions**:

1. Verify cache directory permissions (should be writable)
2. Check if requested interval matches cached interval
3. Ensure date range overlaps with cached data

### Vision API 403 Errors

**Symptoms**: "403 Forbidden" in logs, fallback to REST

**Cause**: Binance Vision API has regional restrictions

**Solution**: FCP handles this automatically. No action needed.

### Rate Limit Errors

**Symptoms**: 429 responses, `RateLimitError`

**Solutions**:

```python
# Use smaller date ranges
for chunk in date_chunks(start, end, chunk_days=7):
    df = manager.get_data(symbol="BTCUSDT", start_time=chunk[0], end_time=chunk[1])
    time.sleep(0.5)  # Rate-limit friendly

# Or use lower frequency intervals
df = manager.get_data(symbol="BTCUSDT", interval=Interval.HOUR_1)  # vs MINUTE_1
```

## Performance Optimization

### Cache Warm-Up

Pre-populate cache for frequently accessed symbols:

```python
from datetime import datetime, timedelta, timezone
from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval

symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

end = datetime.now(timezone.utc) - timedelta(days=2)  # Avoid recent data
start = end - timedelta(days=365)

for symbol in symbols:
    print(f"Warming cache for {symbol}...")
    df = manager.get_data(symbol=symbol, start_time=start, end_time=end, interval=Interval.HOUR_1)
    print(f"  Cached {len(df)} bars")

manager.close()
```

### Batch Fetching

Fetch multiple symbols efficiently:

```python
from concurrent.futures import ThreadPoolExecutor

def fetch_symbol(symbol: str) -> tuple[str, int]:
    manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
    df = manager.get_data(symbol=symbol, start_time=start, end_time=end, interval=Interval.HOUR_1)
    manager.close()
    return symbol, len(df)

with ThreadPoolExecutor(max_workers=3) as executor:
    results = list(executor.map(fetch_symbol, symbols))
```

## Scripts

| Script            | Purpose                         |
| ----------------- | ------------------------------- |
| `cache_health.py` | Check cache directory health    |
| `fcp_stats.py`    | Monitor FCP source distribution |
| `warm_cache.py`   | Pre-populate cache for symbols  |

## Related

- @src/CLAUDE.md - FCP implementation, cache patterns, exception hierarchy
- @docs/skills/ckvd-usage/references/debugging.md - General debugging
- @./references/evolution-log.md - Skill improvement history
