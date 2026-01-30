---
paths:
  - "src/data_source_manager/core/sync/**/*.py"
  - "src/data_source_manager/core/cache/**/*.py"
  - "tests/**/test_cache*.py"
---

# Caching Patterns Rules

Guidelines for DSM cache operations.

## Cache Location

Default: `~/.cache/data_source_manager/`

```
~/.cache/data_source_manager/
└── binance/
    ├── spot/
    │   └── klines/daily/{SYMBOL}/{INTERVAL}/
    ├── futures_usdt/
    │   └── klines/daily/{SYMBOL}/{INTERVAL}/
    └── futures_coin/
        └── klines/daily/{SYMBOL}/{INTERVAL}/
```

## Cache File Format

**Apache Arrow (.arrow)** files with:

- One file per day
- Memory-mapped reads for speed
- Atomic writes to prevent corruption

## Cache Key Pattern

```
{provider}/{market_type}/klines/daily/{symbol}/{interval}/{date}.arrow
```

Example:

```
binance/futures_usdt/klines/daily/BTCUSDT/1h/2024-01-15.arrow
```

## Cache Invalidation

Cache files are immutable once written. To refresh:

```bash
# Clear specific symbol
rm -rf ~/.cache/data_source_manager/binance/futures_usdt/klines/daily/BTCUSDT/

# Clear all cache
mise run cache:clear
```

## Never Cache

- Future timestamps (data not yet available)
- Today's incomplete data (still updating)
- Error responses

## Cache Read Pattern

```python
# FCP priority: Cache → Vision → REST
# Cache is always checked first for performance
```

## Partial Day Handling

Recent data (<48h) may not be in Vision API. FCP handles this automatically by falling back to REST API.
