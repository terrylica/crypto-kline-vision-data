# Basic Data Fetching

Minimal examples for common data retrieval tasks.

## Single Symbol, Last Week

```python
from datetime import datetime, timedelta, timezone

from data_source_manager import DataSourceManager, DataProvider, Interval, MarketType

# Create manager
manager = DataSourceManager.create(
    DataProvider.BINANCE,
    MarketType.FUTURES_USDT
)

# Time range
end = datetime.now(timezone.utc)
start = end - timedelta(days=7)

# Fetch data
df = manager.get_data(
    symbol="BTCUSDT",
    interval=Interval.HOUR_1,
    start_time=start,
    end_time=end
)

print(f"Rows: {len(df)}, Columns: {list(df.columns)}")
```

## Multiple Symbols

```python
symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
data = {}

for symbol in symbols:
    df = manager.get_data(
        symbol=symbol,
        interval=Interval.HOUR_1,
        start_time=start,
        end_time=end
    )
    data[symbol] = df
    print(f"{symbol}: {len(df)} rows")
```

## Different Market Types

```python
# Spot market
spot_manager = DataSourceManager.create(
    DataProvider.BINANCE,
    MarketType.SPOT
)

# USDT-margined futures
usdt_manager = DataSourceManager.create(
    DataProvider.BINANCE,
    MarketType.FUTURES_USDT
)

# Coin-margined futures (different symbol format!)
coin_manager = DataSourceManager.create(
    DataProvider.BINANCE,
    MarketType.FUTURES_COIN
)

# Note: BTCUSD_PERP, not BTCUSDT
df = coin_manager.get_data(
    symbol="BTCUSD_PERP",  # Coin-margined format
    interval=Interval.HOUR_1,
    start_time=start,
    end_time=end
)
```

## Available Intervals

```python
from data_source_manager import Interval

# Common intervals
Interval.MINUTE_1    # 1 minute
Interval.MINUTE_5    # 5 minutes
Interval.MINUTE_15   # 15 minutes
Interval.HOUR_1      # 1 hour
Interval.HOUR_4      # 4 hours
Interval.DAY_1       # 1 day
```

## Returned DataFrame Structure

```python
# Index: open_time (UTC datetime)
# Columns: open, high, low, close, volume (all float64)

df.index.name  # "open_time"
df.columns     # ["open", "high", "low", "close", "volume"]

# First row
print(df.iloc[0])
```

## Related

- [Market Types Reference](../references/market-types.md)
- [Intervals Reference](../references/intervals.md)
- [FCP Protocol](../references/fcp-protocol.md)
