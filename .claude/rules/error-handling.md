---
paths:
  - "src/data_source_manager/**/*.py"
  - "tests/**/*.py"
---

# Error Handling Rules

Error handling patterns specific to Data Source Manager.

## Exception Hierarchy

```
DSMError (base)
├── DataSourceError
│   ├── DataNotFoundError     # No data available for requested range
│   ├── RateLimitError        # API rate limit exceeded
│   └── ConnectionError       # Network connectivity issues
├── ValidationError
│   ├── InvalidSymbolError    # Symbol not valid for market type
│   ├── InvalidIntervalError  # Unsupported interval
│   └── InvalidTimeRangeError # Start > End, or future dates
└── CacheError
    ├── CacheReadError        # Failed to read from cache
    └── CacheWriteError       # Failed to write to cache
```

## FCP Error Flow

```
Request → Cache → Vision → REST → Error
           │         │        │
           │         │        └── raise DataSourceError
           │         └── 403/404 → try REST
           └── miss → try Vision
```

## Error Handling Patterns

### Always Catch Specific Exceptions

```python
# ✅ CORRECT
from data_source_manager.exceptions import DataNotFoundError, RateLimitError

try:
    df = manager.get_data(symbol="BTCUSDT", ...)
except DataNotFoundError:
    logger.warning(f"No data for {symbol} in range")
    df = pl.DataFrame()  # Empty fallback
except RateLimitError:
    logger.warning("Rate limited, backing off...")
    time.sleep(60)
    df = manager.get_data(symbol="BTCUSDT", ...)

# ❌ WRONG - Silent failure
try:
    df = manager.get_data(symbol="BTCUSDT", ...)
except:  # Bare except
    pass  # Silent failure - NEVER do this
```

### Rate Limit Handling

Binance rate limits:

- REST API: 1200 requests/minute (weight varies by endpoint)
- Vision API: No rate limit (S3 bucket)

```python
# FCP automatically retries with backoff
# For manual handling:
from data_source_manager.utils.rate_limiter import RateLimiter

limiter = RateLimiter(requests_per_minute=1200)
await limiter.wait()  # Blocks if rate exceeded
```

### Timeout Configuration

```python
# ✅ Always specify timeouts for external calls
manager = DataSourceManager.create(
    DataProvider.BINANCE,
    MarketType.FUTURES_USDT,
    timeout=30.0  # seconds
)

# httpx default timeout is 5.0 seconds
# Vision API calls may take longer for large files
```

## Common Error Scenarios

| Scenario              | Error                 | Recovery                     |
| --------------------- | --------------------- | ---------------------------- |
| Invalid symbol        | InvalidSymbolError    | Validate before calling      |
| No historical data    | DataNotFoundError     | Check symbol listing date    |
| API down              | ConnectionError       | FCP falls back automatically |
| Rate limited          | RateLimitError        | Wait and retry               |
| Future date requested | InvalidTimeRangeError | Use current UTC time         |

## Logging Errors

```python
import structlog
logger = structlog.get_logger()

# Include context for debugging
logger.error(
    "fetch_failed",
    symbol=symbol,
    market_type=market_type.value,
    interval=interval.value,
    error=str(e),
    exc_info=True
)
```
