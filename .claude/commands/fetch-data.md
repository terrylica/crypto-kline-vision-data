---
name: fetch-data
description: Fetch market data using DataSourceManager
disable-model-invocation: true
---

# Fetch Market Data

Fetch cryptocurrency market data with automatic failover.

## Arguments

$ARGUMENTS should be in format: `SYMBOL DAYS [INTERVAL]`

Example: `BTCUSDT 7 1h` or just `ETHUSDT 30`

## Implementation

```python
from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval
from datetime import datetime, timedelta, timezone

# Parse arguments
args = "$ARGUMENTS".split()
symbol = args[0] if args else "BTCUSDT"
days = int(args[1]) if len(args) > 1 else 7
interval_str = args[2] if len(args) > 2 else "1h"

# Map interval string to Interval enum
interval_map = {
    "1m": Interval.MINUTE_1,
    "5m": Interval.MINUTE_5,
    "15m": Interval.MINUTE_15,
    "1h": Interval.HOUR_1,
    "4h": Interval.HOUR_4,
    "1d": Interval.DAY_1,
}
interval = interval_map.get(interval_str, Interval.HOUR_1)

# Determine market type from symbol
if symbol.endswith("_PERP"):
    market_type = MarketType.FUTURES_COIN
elif symbol.endswith("USDT"):
    market_type = MarketType.FUTURES_USDT
else:
    market_type = MarketType.SPOT

# Create manager and fetch
manager = DataSourceManager.create(DataProvider.BINANCE, market_type)
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=days)

df = manager.get_data(
    symbol=symbol,
    start_time=start_time,
    end_time=end_time,
    interval=interval
)

print(f"✓ Loaded {len(df)} bars of {symbol} ({interval_str}) data")
print(f"  Date range: {df.index[0]} to {df.index[-1]}")
print(df.head())

manager.close()
```

Run this with: `uv run -p 3.13 python -c "..."` using the code above.
