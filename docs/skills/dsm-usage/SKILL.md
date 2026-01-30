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

## Key Concepts

| Concept       | Quick Reference                                     |
| ------------- | --------------------------------------------------- |
| Market Types  | SPOT, FUTURES_USDT, FUTURES_COIN                    |
| Intervals     | MINUTE_1, MINUTE_5, HOUR_1, HOUR_4, DAY_1           |
| FCP Priority  | Cache (~1ms) → Vision (~1-5s) → REST (~100-500ms)   |
| Symbol Format | BTCUSDT (spot/futures), BTCUSD_PERP (coin-margined) |

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

## Examples

Practical code examples:

- @examples/basic-fetch.md - Single/multiple symbols, market types
- @examples/error-handling.md - Exception patterns, validation, retries

## Detailed References

For deeper information, see:

- @references/market-types.md - Detailed market type documentation
- @references/intervals.md - Complete interval reference
- @references/fcp-protocol.md - FCP architecture and debugging
