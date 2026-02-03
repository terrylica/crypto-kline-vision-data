---
paths:
  - "src/data_source_manager/**/*.py"
  - "tests/**/*.py"
---

# Error Handling Rules

Error handling patterns specific to Data Source Manager.

## Exception Hierarchy

```
REST API Exceptions (rest_exceptions.py):
RestAPIError (base)
├── RateLimitError        # API rate limit exceeded (429)
├── HTTPError             # HTTP errors with status code
├── APIError              # API-specific error codes
├── NetworkError          # Network connectivity issues
├── RestTimeoutError      # Request timeout
└── JSONDecodeError       # JSON parsing failures

Vision API Exceptions (vision_exceptions.py):
VisionAPIError (base)
├── DataFreshnessError        # Data too recent for Vision API
├── ChecksumVerificationError # Checksum validation failed
└── DownloadFailedError       # File download failed

UnsupportedIntervalError (ValueError)  # Interval not supported by market type
```

## FCP Error Flow

```
Request → Cache → Vision → REST → Error
           │         │        │
           │         │        └── raise RestAPIError
           │         └── VisionAPIError → try REST
           └── miss → try Vision
```

## Error Handling Patterns

### Always Catch Specific Exceptions

```python
# ✅ CORRECT - Use actual exception classes
from data_source_manager.utils.for_core.rest_exceptions import RateLimitError, RestAPIError
from data_source_manager.utils.for_core.vision_exceptions import VisionAPIError
import pandas as pd

try:
    df = manager.get_data(symbol="BTCUSDT", ...)
except RateLimitError as e:
    logger.warning(f"Rate limited: {e}, retry after {e.retry_after}s")
    time.sleep(e.retry_after or 60)
    df = manager.get_data(symbol="BTCUSDT", ...)
except VisionAPIError:
    logger.warning("Vision API failed, will use REST fallback")
    # FCP handles this automatically
except RestAPIError as e:
    logger.error(f"REST API error: {e}")
    df = pd.DataFrame()  # Empty fallback

# ❌ WRONG - Silent failure
try:
    df = manager.get_data(symbol="BTCUSDT", ...)
except:  # Bare except
    pass  # Silent failure - NEVER do this
```

### Symbol Validation

```python
# Validate symbols to get helpful error messages
from data_source_manager.utils.market_constraints import validate_symbol_for_market_type

try:
    validate_symbol_for_market_type("BTCUSDT", MarketType.FUTURES_COIN)
except ValueError as e:
    # Error includes suggestion: "Try using 'BTCUSD_PERP' instead."
    logger.error(f"Invalid symbol: {e}")
```

### Rate Limit Handling

Binance rate limits:

- REST API: 6000 weight per minute (weight varies by endpoint)
- Vision API: No rate limit (S3 bucket)

```python
# FCP automatically handles retries
# For manual rate limit handling:
from data_source_manager.utils.for_core.rest_exceptions import RateLimitError

try:
    df = manager.get_data(...)
except RateLimitError as e:
    wait_time = e.retry_after or 60
    time.sleep(wait_time)
    df = manager.get_data(...)  # Retry
```

### Timeout Configuration

```python
# DSM uses configurable retry_count for network operations
manager = DataSourceManager.create(
    DataProvider.BINANCE,
    MarketType.FUTURES_USDT,
    retry_count=3,  # Number of retries for failed requests
)

# httpx is used internally with appropriate timeouts
# Vision API calls may take longer for large files
```

## Common Error Scenarios

| Scenario              | Error            | Recovery                            |
| --------------------- | ---------------- | ----------------------------------- |
| Invalid symbol        | ValueError       | Use validate_symbol_for_market_type |
| No historical data    | Empty DataFrame  | Check symbol listing date           |
| Vision API down       | VisionAPIError   | FCP falls back to REST              |
| REST API rate limited | RateLimitError   | Wait and retry                      |
| Future date requested | ValueError       | Use current UTC time                |
| Network timeout       | RestTimeoutError | Increase retry_count                |

## Logging Errors

```python
from data_source_manager.utils.loguru_setup import logger

# Include context for debugging
logger.error(
    f"fetch_failed symbol={symbol} market_type={market_type.value} "
    f"interval={interval.value} error={e}"
)

# Or use structured logging with loguru
logger.bind(symbol=symbol, market_type=market_type.value).error(f"Fetch failed: {e}")
```
