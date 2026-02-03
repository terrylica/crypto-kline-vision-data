---
adr: docs/adr/2025-01-30-failover-control-protocol.md
paths:
  - "src/data_source_manager/core/sync/**/*.py"
  - "src/data_source_manager/core/providers/**/*.py"
  - "docs/skills/dsm-fcp-monitor/**/*"
  - "tests/**/test_fcp*.py"
---

# FCP Protocol Rules

Failover Control Protocol (FCP) implementation guidelines.

## Priority Order

```
1. Cache (~1ms)     - Local Arrow files
2. Vision (~1-5s)   - Binance S3 historical data
3. REST (~100-500ms) - Live Binance API
```

## When Each Source Is Used

| Source | When Used                             | Latency    |
| ------ | ------------------------------------- | ---------- |
| Cache  | Data exists locally                   | ~1ms       |
| Vision | Historical data (>48h old)            | ~1-5s      |
| REST   | Recent data (<48h), live, or fallback | ~100-500ms |

## FCP Decision Logic

```python
def get_data(symbol, start, end, interval):
    # 1. Check cache first
    cached = cache_manager.get(symbol, start, end, interval)
    if cached is not None:
        return cached

    # 2. Try Vision for historical data
    if end < (now - timedelta(hours=48)):
        try:
            vision_data = vision_handler.fetch(...)
            cache_manager.put(vision_data)  # Populate cache
            return vision_data
        except VisionError:
            pass  # Fall through to REST

    # 3. REST API for recent/live data
    rest_data = rest_client.fetch(...)
    if is_complete_day(rest_data):
        cache_manager.put(rest_data)  # Cache if complete
    return rest_data
```

## Cache Population Rules

**DO cache**:

- Complete days from Vision API
- Complete days from REST API (historical)

**DON'T cache**:

- Partial days (still accumulating)
- Future timestamps
- Error responses
- Data less than 48h old (may be incomplete)

## Debugging FCP Behavior

Enable debug logging:

```python
import os
# Set before importing DSM
os.environ["DSM_LOG_LEVEL"] = "DEBUG"

from data_source_manager import DataSourceManager, DataProvider, MarketType

# Or configure after creation
manager = DataSourceManager.create(
    DataProvider.BINANCE,
    MarketType.FUTURES_USDT,
    log_level="DEBUG",
    suppress_http_debug=False,  # Show detailed HTTP logs
)

# Or reconfigure at runtime
manager.reconfigure_logging(log_level="DEBUG")
```

Check FCP decisions:

```bash
# Run diagnostic script
uv run -p 3.13 python docs/skills/dsm-usage/scripts/diagnose_fcp.py BTCUSDT FUTURES_USDT 1h
```

## Common FCP Issues

| Symptom             | Cause                 | Solution                      |
| ------------------- | --------------------- | ----------------------------- |
| Always hitting REST | Cache miss            | Check cache path, permissions |
| Vision 403          | Data too recent       | FCP handles automatically     |
| Slow performance    | Large date range      | Split into chunks             |
| Stale data          | Cache not invalidated | Clear cache manually          |

## Tracking Data Sources

The `_data_source` column tracks where each record came from:

```python
# Enable source tracking (default: True)
df = manager.get_data(
    "BTCUSDT", start_time, end_time, Interval.HOUR_1,
    include_source_info=True
)

# Check data sources used
if "_data_source" in df.columns:
    sources = df["_data_source"].value_counts()
    print(f"Data sources: {sources.to_dict()}")
    # Example output: {'CACHE': 100, 'VISION': 50, 'REST': 10}
```
