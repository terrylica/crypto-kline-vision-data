---
name: debug-fcp
description: Debug Failover Control Protocol data retrieval issues
disable-model-invocation: true
---

# Debug FCP Issues

Diagnose why data retrieval isn't working as expected.

## Arguments

$ARGUMENTS should be the symbol to debug, e.g., `BTCUSDT`

## Steps

1. **Enable debug logging**:

```python
import os
os.environ["DSM_LOG_LEVEL"] = "DEBUG"
```

1. **Check cache location**:

```bash
ls -la ~/.cache/data_source_manager/binance/
```

1. **Test each source individually**:

```python
from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval
from data_source_manager.core.sync.data_source_manager import DataSource
from datetime import datetime, timedelta, timezone

symbol = "$ARGUMENTS" or "BTCUSDT"
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=3)

# Test Cache only
try:
    manager = DataSourceManager.create(
        DataProvider.BINANCE, MarketType.FUTURES_USDT,
        enforce_source=DataSource.CACHE
    )
    df = manager.get_data(symbol, start_time, end_time, Interval.HOUR_1)
    print(f"✓ Cache: {len(df)} bars")
    manager.close()
except Exception as e:
    print(f"✗ Cache failed: {e}")

# Test Vision only
try:
    manager = DataSourceManager.create(
        DataProvider.BINANCE, MarketType.FUTURES_USDT,
        enforce_source=DataSource.VISION
    )
    df = manager.get_data(symbol, start_time, end_time, Interval.HOUR_1)
    print(f"✓ Vision: {len(df)} bars")
    manager.close()
except Exception as e:
    print(f"✗ Vision failed: {e}")

# Test REST only
try:
    manager = DataSourceManager.create(
        DataProvider.BINANCE, MarketType.FUTURES_USDT,
        enforce_source=DataSource.REST
    )
    df = manager.get_data(symbol, start_time, end_time, Interval.HOUR_1)
    print(f"✓ REST: {len(df)} bars")
    manager.close()
except Exception as e:
    print(f"✗ REST failed: {e}")
```

## Common Issues

| Symptom               | Likely Cause           | Fix                                   |
| --------------------- | ---------------------- | ------------------------------------- |
| Cache returns nothing | New symbol/interval    | Normal - will populate on first fetch |
| Vision 403            | Too recent data (<48h) | Use REST for recent data              |
| REST rate limited     | Too many requests      | Wait 1 minute, check weight usage     |
| Empty DataFrame       | Wrong symbol format    | Check market type matches symbol      |
