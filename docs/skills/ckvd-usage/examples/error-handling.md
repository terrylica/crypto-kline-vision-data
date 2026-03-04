# Error Handling Examples

Proper error handling patterns for CKVD operations.

## Basic Try/Except Pattern

```python
from datetime import datetime, timedelta, timezone

from ckvd import CryptoKlineVisionData, DataProvider, Interval, MarketType
from ckvd.utils.for_core.rest_exceptions import RateLimitError
from ckvd.utils.for_core.vision_exceptions import DataNotAvailableError

manager = CryptoKlineVisionData.create(
    DataProvider.BINANCE,
    MarketType.FUTURES_USDT
)

end = datetime.now(timezone.utc)
start = end - timedelta(days=7)

try:
    df = manager.get_data(
        symbol="BTCUSDT",
        interval=Interval.HOUR_1,
        start_time=start,
        end_time=end
    )
except RateLimitError as e:
    # REST API rate limited - wait and retry
    print(f"Rate limited, wait 60s: {e}")
except DataNotAvailableError as e:
    # Vision API doesn't have this data (e.g., too recent)
    print(f"Vision unavailable: {e}. Details: {e.details}")
except Exception as e:
    # Log unexpected errors with context
    print(f"Unexpected error fetching BTCUSDT: {type(e).__name__}: {e}")
    raise
```

## Validate Symbol Format Before Fetch

```python
from ckvd.utils.market_constraints import validate_symbol_for_market_type

symbol = "BTCUSDT"
market_type = MarketType.FUTURES_COIN

try:
    validate_symbol_for_market_type(symbol, market_type)
except ValueError as e:
    print(f"Invalid symbol: {e}")
    # e.g., "Invalid symbol format... Try using 'BTCUSD_PERP' instead."
    symbol = "BTCUSD_PERP"
```

## Handle Empty Results

```python
df = manager.get_data(
    symbol="BTCUSDT",
    interval=Interval.HOUR_1,
    start_time=start,
    end_time=end
)

if df is None or len(df) == 0:
    print("No data returned. Possible causes:")
    print("1. Symbol doesn't exist")
    print("2. Date range has no trading data")
    print("3. Requesting future timestamps")
    return

# Validate before use
assert df.index.is_monotonic_increasing, "Unsorted timestamps"
assert not df.index.has_duplicates, "Duplicate timestamps"
```

## Timeout Handling

```python
# CKVD uses httpx internally with retry logic
# The retry_count parameter controls how many times failed requests are retried

from ckvd import CryptoKlineVisionData, DataProvider, MarketType

# Create manager with higher retry count for unreliable networks
manager = CryptoKlineVisionData.create(
    DataProvider.BINANCE,
    MarketType.FUTURES_USDT,
    retry_count=5  # Number of retry attempts (default is 3)
)
```

## Retry Pattern

```python
import time

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

for attempt in range(MAX_RETRIES):
    try:
        df = manager.get_data(
            symbol="BTCUSDT",
            interval=Interval.HOUR_1,
            start_time=start,
            end_time=end
        )
        break  # Success
    except RateLimitError:
        if attempt < MAX_RETRIES - 1:
            print(f"Rate limited, retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
        else:
            raise
```

## Related

- [FCP Protocol](../references/fcp-protocol.md) - Fallback behavior
- [Troubleshooting](../../../TROUBLESHOOTING.md) - Common issues
