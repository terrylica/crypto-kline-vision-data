---
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
import logging
logging.getLogger("data_source_manager").setLevel(logging.DEBUG)

# Or with structlog
import structlog
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG)
)
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
| Vision 403          | IP not whitelisted    | Use REST fallback             |
| Slow performance    | Large date range      | Split into chunks             |
| Stale data          | Cache not invalidated | Clear cache manually          |

## FCP Metrics

Track FCP source distribution:

```python
# Manager exposes metrics after fetch
stats = manager.get_stats()
print(f"Cache hits: {stats.cache_hits}")
print(f"Vision fetches: {stats.vision_fetches}")
print(f"REST fetches: {stats.rest_fetches}")
```
