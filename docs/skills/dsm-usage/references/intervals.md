# Intervals Reference

Detailed documentation for kline/candlestick intervals supported by DataSourceManager.

## Interval Enum Values

```python
from data_source_manager import Interval

# Sub-minute (Spot only)
Interval.SECOND_1    # 1s - Spot market only, not available in futures

# Minutes
Interval.MINUTE_1    # 1m
Interval.MINUTE_3    # 3m
Interval.MINUTE_5    # 5m
Interval.MINUTE_15   # 15m
Interval.MINUTE_30   # 30m

# Hours
Interval.HOUR_1      # 1h
Interval.HOUR_2      # 2h
Interval.HOUR_4      # 4h
Interval.HOUR_6      # 6h
Interval.HOUR_8      # 8h
Interval.HOUR_12     # 12h

# Days and above
Interval.DAY_1       # 1d
Interval.DAY_3       # 3d
Interval.WEEK_1      # 1w
Interval.MONTH_1     # 1M
```

## Interval Support by Market Type

| Interval  | SPOT | FUTURES_USDT | FUTURES_COIN |
| --------- | ---- | ------------ | ------------ |
| SECOND_1  | ✅   | ❌           | ❌           |
| MINUTE_1  | ✅   | ✅           | ✅           |
| MINUTE_5  | ✅   | ✅           | ✅           |
| MINUTE_15 | ✅   | ✅           | ✅           |
| HOUR_1    | ✅   | ✅           | ✅           |
| HOUR_4    | ✅   | ✅           | ✅           |
| DAY_1     | ✅   | ✅           | ✅           |

## Vision API vs REST API Availability

**Vision API** (bulk historical):

- Data available ~48 hours after market close
- All intervals except SECOND_1 available in daily files
- More efficient for large historical requests

**REST API** (real-time):

- All intervals available
- Rate limited (6000 weight/minute)
- Maximum 1000 candles per request

## Converting Between Intervals

```python
from data_source_manager.utils.market_constraints import Interval

# Get interval in seconds
interval_sec = Interval.HOUR_1.to_seconds()  # 3600

# Get string representation for API calls
interval_str = Interval.HOUR_1.value  # "1h"
```

## Bars Per Day

| Interval | Bars/Day |
| -------- | -------- |
| 1s       | 86,400   |
| 1m       | 1,440    |
| 5m       | 288      |
| 15m      | 96       |
| 1h       | 24       |
| 4h       | 6        |
| 1d       | 1        |
