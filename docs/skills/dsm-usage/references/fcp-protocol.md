# Failover Control Protocol (FCP) Reference

Detailed documentation for the FCP data retrieval strategy.

## Overview

FCP automatically retrieves market data from the best available source:

```
┌─────────────────────────────────────────────────────────┐
│                   Request: get_data()                   │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│              1. CHECK LOCAL CACHE                       │
│  - Arrow files in ~/.cache/data_source_manager/         │
│  - Fastest (~1ms lookup)                                │
│  - Returns immediately if complete                      │
└────────────────────────┬────────────────────────────────┘
                         │ (if gaps)
                         ▼
┌─────────────────────────────────────────────────────────┐
│              2. VISION API                              │
│  - Binance Vision on AWS S3                             │
│  - Bulk historical data (~1-5s per day)                 │
│  - ~48h delay from market close                         │
│  - No rate limits                                       │
└────────────────────────┬────────────────────────────────┘
                         │ (if gaps remain)
                         ▼
┌─────────────────────────────────────────────────────────┐
│              3. REST API                                │
│  - Real-time fallback                                   │
│  - Rate limited (6000 weight/min)                       │
│  - 1000 candles per request max                         │
│  - Used for recent data not in Vision                   │
└─────────────────────────────────────────────────────────┘
```

## Forcing Data Sources

The `enforce_source` parameter is passed to `get_data()`, not `create()`:

```python
from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval
from data_source_manager.core.sync.data_source_manager import DataSource
from datetime import datetime, timedelta, timezone

manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

end = datetime.now(timezone.utc)
start = end - timedelta(days=7)

# Force Vision API only (skip cache)
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start,
    end_time=end,
    interval=Interval.HOUR_1,
    enforce_source=DataSource.VISION
)

# Force REST API only
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start,
    end_time=end,
    interval=Interval.HOUR_1,
    enforce_source=DataSource.REST
)

# Force cache only (offline mode)
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start,
    end_time=end,
    interval=Interval.HOUR_1,
    enforce_source=DataSource.CACHE
)

manager.close()
```

## Cache Structure

```
~/.cache/data_source_manager/
└── binance/
    ├── spot/
    │   └── klines/
    │       └── daily/
    │           └── BTCUSDT/
    │               └── 1h/
    │                   ├── 2024-01-01.arrow
    │                   ├── 2024-01-02.arrow
    │                   └── ...
    └── futures_usdt/
        └── klines/
            └── daily/
                └── BTCUSDT/
                    └── 1h/
                        └── ...
```

## Performance Characteristics

| Source | Latency   | Throughput   | Best For                    |
| ------ | --------- | ------------ | --------------------------- |
| Cache  | ~1ms      | Unlimited    | Repeated queries, backtests |
| Vision | 1-5s/day  | High         | Historical bulk downloads   |
| REST   | 100-500ms | Rate limited | Real-time, recent data      |

## Gap Detection

FCP automatically detects gaps in data and fills them:

```python
# Request 30 days of data
df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)

# FCP internally:
# 1. Checks cache: Days 1-25 present
# 2. Vision API: Downloads days 26-28
# 3. REST API: Gets days 29-30 (too recent for Vision)
# 4. Merges all data, caches new data
```

## Debugging FCP

Enable debug logging to see FCP decisions:

```python
import os
os.environ["DSM_LOG_LEVEL"] = "DEBUG"

# Now get_data() will log:
# DEBUG - Cache hit for 2024-01-01
# DEBUG - Cache miss for 2024-01-02, trying Vision
# DEBUG - Vision API downloaded 2024-01-02
# DEBUG - REST fallback for 2024-01-03 (recent data)
```
