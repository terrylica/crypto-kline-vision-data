# Examples

Runnable examples demonstrating Crypto Kline Vision Data functionality.

## Quick Start

```bash
# Basic usage (BTCUSDT hourly data)
uv run -p 3.13 python examples/quick_start.py
```

## Example Categories

### Basic Usage

| Example                | Description                  |
| ---------------------- | ---------------------------- |
| `quick_start.py`       | Minimal FCP usage example    |
| `sync/README.md`       | Synchronous CLI demo         |
| `lib_module/README.md` | Library integration patterns |

### Advanced Patterns

| Example                                | Description                    |
| -------------------------------------- | ------------------------------ |
| `ckvd_lazy_initialization_demo.py`     | Lazy manager initialization    |
| `clean_feature_engineering_example.py` | Feature engineering pipeline   |
| `ckvd_logging_demo.py`                 | Logging configuration patterns |

### Logging

| Example                         | Description                    |
| ------------------------------- | ------------------------------ |
| `loguru_demo.py`                | Loguru integration             |
| `default_logging_comparison.py` | Compare logging configurations |

## Running Examples

All examples use Python 3.13:

```bash
# Run any example
uv run -p 3.13 python examples/<example_name>.py

# Run with debug logging
CKVD_LOG_LEVEL=DEBUG uv run -p 3.13 python examples/quick_start.py
```

## Key Patterns

### Always Use UTC

```python
from datetime import datetime, timezone, timedelta

# ✅ CORRECT - timezone-aware UTC
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=7)

# ❌ WRONG - naive datetime
end_time = datetime.now()  # No timezone info!
```

### Always Close Manager

```python
manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
try:
    df = manager.get_data(symbol="BTCUSDT", ...)
finally:
    manager.close()  # Always cleanup
```

### Symbol Format by Market Type

| Market Type  | Symbol Format | Example       |
| ------------ | ------------- | ------------- |
| SPOT         | BTCUSDT       | `ETHUSDT`     |
| FUTURES_USDT | BTCUSDT       | `SOLUSDT`     |
| FUTURES_COIN | BTCUSD_PERP   | `ETHUSD_PERP` |

## Related Documentation

- [CLAUDE.md](/CLAUDE.md) - Main project reference
- [docs/skills/ckvd-usage/](/docs/skills/ckvd-usage/) - Usage skill with examples
- [docs/TROUBLESHOOTING.md](/docs/TROUBLESHOOTING.md) - Common issues
