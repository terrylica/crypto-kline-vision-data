---
paths:
  - "src/data_source_manager/core/providers/binance/**/*.py"
  - "tests/integration/**/*.py"
---

# Binance API Rules

Rules for working with Binance market data APIs.

## Rate Limits

- REST API: 6000 weight per minute
- Vision API: No rate limits (bulk S3 access)
- Each klines request: ~1-5 weight depending on limit

## Symbol Formats

| Market Type  | Format           | Example     |
| ------------ | ---------------- | ----------- |
| SPOT         | `{BASE}{QUOTE}`  | BTCUSDT     |
| FUTURES_USDT | `{BASE}{QUOTE}`  | BTCUSDT     |
| FUTURES_COIN | `{BASE}USD_PERP` | BTCUSD_PERP |

## Timestamp Handling

- All timestamps from Binance are Unix milliseconds (UTC)
- `open_time` represents the **start** of the candle period
- Vision API data has ~48h delay from market close
- REST API provides real-time data

## Error Handling

```python
# Common HTTP status codes
# 403 - Future timestamp requested (data not available yet)
# 429 - Rate limit exceeded (wait 1 minute)
# 418 - IP banned (check for violations)
```

## Best Practices

1. Always use timezone-aware datetime: `datetime.now(timezone.utc)`
2. Validate symbol format before requests
3. Use FCP priority: Cache → Vision → REST
4. Set explicit timeout on all HTTP requests
