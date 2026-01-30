---
name: dsm-usage
description: Fetch market data using DataSourceManager with Failover Control Protocol (cache → Vision API → REST API)
---

# DataSourceManager Usage

Fetch cryptocurrency market data with automatic failover between data sources.

## Quick Start

```python
from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval
from datetime import datetime, timedelta, timezone

# Create manager for USDT-margined futures
manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

# Fetch data with automatic failover (cache → Vision → REST)
# IMPORTANT: Always use UTC timezone-aware datetimes
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=7)

df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start_time,
    end_time=end_time,
    interval=Interval.HOUR_1
)

print(f"Loaded {len(df)} bars")
manager.close()
```

## Market Types

| MarketType     | Description                     |
| -------------- | ------------------------------- |
| `SPOT`         | Spot market                     |
| `FUTURES_USDT` | USDT-margined perpetual futures |
| `FUTURES_COIN` | Coin-margined futures           |

## Intervals

Common intervals: `MINUTE_1`, `MINUTE_5`, `MINUTE_15`, `HOUR_1`, `HOUR_4`, `DAY_1`

## High-Level API

For simpler use cases, use `fetch_market_data`:

```python
from data_source_manager import fetch_market_data, MarketType, Interval
from datetime import datetime, timedelta, timezone

df = fetch_market_data(
    symbol="BTCUSDT",
    market_type=MarketType.FUTURES_USDT,
    interval=Interval.HOUR_1,
    start_time=datetime.now(timezone.utc) - timedelta(days=30),
    end_time=datetime.now(timezone.utc)
)
```

## Failover Control Protocol

Data retrieval follows this priority:

1. **Cache** - Local Arrow files (~1ms)
2. **Vision API** - Binance S3 bulk data (~1-5s)
3. **REST API** - Real-time fallback (~100-500ms)

Recent data (~48h) typically not in Vision API, falls through to REST.
