---
name: dsm-usage
description: Fetch market data using DataSourceManager with Failover Control Protocol (cache → Vision API → REST API). TRIGGERS - fetch market data, use DSM, access Binance, get klines, OHLCV data, DataSourceManager API.
argument-hint: "[symbol] [market-type]"
user-invocable: true
allowed-tools: Read, Bash
---

# DataSourceManager Usage

Fetch market data for: $ARGUMENTS

Use automatic failover between data sources.

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
from data_source_manager import fetch_market_data, DataProvider, MarketType, Interval, ChartType
from datetime import datetime, timedelta, timezone

df, elapsed_time, records_count = fetch_market_data(
    provider=DataProvider.BINANCE,
    market_type=MarketType.FUTURES_USDT,
    chart_type=ChartType.KLINES,
    symbol="BTCUSDT",
    interval=Interval.HOUR_1,
    start_time=datetime.now(timezone.utc) - timedelta(days=30),
    end_time=datetime.now(timezone.utc)
)
print(f"Loaded {records_count} bars in {elapsed_time:.2f}s")
```

## Examples

Practical code examples:

- @examples/basic-fetch.md - Single/multiple symbols, market types
- @examples/error-handling.md - Exception patterns, validation, retries

## Helper Scripts

Utility scripts for common operations:

```bash
# Validate symbol format
uv run -p 3.13 python docs/skills/dsm-usage/scripts/validate_symbol.py BTCUSDT FUTURES_COIN

# Check cache status
uv run -p 3.13 python docs/skills/dsm-usage/scripts/check_cache.py BTCUSDT futures_usdt 1h

# Diagnose FCP behavior (with debug logging)
uv run -p 3.13 python docs/skills/dsm-usage/scripts/diagnose_fcp.py BTCUSDT futures_usdt 1h --days 3
```

## Detailed References

For deeper information, see:

- @references/market-types.md - Detailed market type documentation
- @references/intervals.md - Complete interval reference
- @references/fcp-protocol.md - FCP architecture and debugging
- @references/debugging.md - Debugging techniques and troubleshooting
