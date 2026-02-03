---
adr: docs/adr/2025-01-30-failover-control-protocol.md
paths:
  - "src/data_source_manager/core/providers/binance/**/*.py"
  - "src/data_source_manager/utils/market_constraints.py"
  - "examples/**/*.py"
  - "tests/integration/**/*.py"
---

# Symbol Format Rules

Symbol format varies by market type. Using the wrong format causes empty DataFrames.

## Quick Reference

| Market Type  | Symbol Format | Example     |
| ------------ | ------------- | ----------- |
| SPOT         | BASEUSDT      | BTCUSDT     |
| FUTURES_USDT | BASEUSDT      | BTCUSDT     |
| FUTURES_COIN | BASEUSD_PERP  | BTCUSD_PERP |

## Common Mistakes

```python
# WRONG: Using USDT format for coin-margined
manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_COIN)
df = manager.get_data(symbol="BTCUSDT", ...)  # Returns empty DataFrame!

# CORRECT: Use USD_PERP format for coin-margined
df = manager.get_data(symbol="BTCUSD_PERP", ...)
```

## Validation Helper

Always validate symbols before fetching:

```python
from data_source_manager.utils.market_constraints import (
    validate_symbol_for_market_type,
    get_market_symbol_format,
    MarketType,
)

# Validate symbol - raises ValueError if invalid
try:
    validate_symbol_for_market_type("BTCUSDT", MarketType.FUTURES_COIN)
except ValueError as e:
    print(f"Invalid symbol: {e}")
    # Error message includes suggestion, e.g.:
    # "Invalid symbol format for FUTURES_COIN market: 'BTCUSDT'.
    #  FUTURES_COIN symbols should end with '_PERP' for perpetual contracts.
    #  Try using 'BTCUSD_PERP' instead."

# Or use get_market_symbol_format to auto-convert
correct_symbol = get_market_symbol_format("BTCUSDT", MarketType.FUTURES_COIN)
# Returns: "BTCUSD_PERP"
```

## Full Symbol Patterns

### SPOT / FUTURES_USDT

```
{BASE}{QUOTE}
```

Examples: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT

### FUTURES_COIN (Coin-Margined Perpetual)

```
{BASE}USD_PERP
```

Examples: BTCUSD_PERP, ETHUSD_PERP, ADAUSD_PERP

### FUTURES_COIN (Quarterly Contracts)

```
{BASE}USD_{EXPIRY}
```

Examples: BTCUSD_240927, ETHUSD_241227

## Diagnostic: Empty DataFrame

If `get_data()` returns empty DataFrame, first check:

1. **Symbol format matches market type**
2. **Date range is in the past** (not future)
3. **Symbol exists on exchange** (check Binance website)

```bash
# Quick validation via REST API
curl -s "https://api.binance.com/api/v3/exchangeInfo?symbol=BTCUSDT" | jq '.symbols[0].status'
```
