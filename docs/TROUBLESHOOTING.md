# Troubleshooting Guide

Common issues and solutions for Data Source Manager.

## Quick Diagnostics

```bash
# Check imports
uv run -p 3.13 python -c "from data_source_manager import DataSourceManager; print('OK')"

# Check cache status
du -sh ~/.cache/data_source_manager

# Enable debug logging
DSM_LOG_LEVEL=DEBUG uv run -p 3.13 python your_script.py
```

## Common Issues

### Empty DataFrame Returned

**Symptoms**: `get_data()` returns DataFrame with 0 rows.

**Causes**:

1. Wrong symbol format for market type
2. Requesting future timestamps
3. Date range with no trading data

**Solutions**:

```python
# Check symbol format - raises ValueError with suggestion if invalid
from data_source_manager import MarketType
from data_source_manager.utils.market_constraints import validate_symbol_for_market_type

try:
    validate_symbol_for_market_type("BTCUSDT", MarketType.FUTURES_COIN)
except ValueError as e:
    print(e)  # "Invalid symbol format... Try using 'BTCUSD_PERP' instead."

# Check time range is in past
from datetime import datetime, timezone
assert end_time <= datetime.now(timezone.utc), "Cannot request future data"
```

### HTTP 403 Forbidden

**Symptoms**: Vision or REST API returns 403 error.

**Causes**:

1. Requesting data for future timestamps
2. Symbol doesn't exist on exchange
3. IP banned (rare)

**Solutions**:

- Verify timestamps are UTC and in the past
- Check symbol exists on Binance
- Wait 15 minutes if IP banned

### HTTP 429 Rate Limited

**Symptoms**: REST API returns 429 error.

**Causes**: Exceeded 6000 weight/minute limit.

**Solutions**:

- Wait 60 seconds before retrying
- Use Vision API for bulk historical data
- Enable caching to reduce API calls

### Naive Datetime Errors

**Symptoms**: `TypeError: can't compare offset-naive and offset-aware datetimes`

**Cause**: Using `datetime.now()` instead of `datetime.now(timezone.utc)`.

**Solution**:

```python
# Wrong
from datetime import datetime
now = datetime.now()

# Correct
from datetime import datetime, timezone
now = datetime.now(timezone.utc)
```

### Import Errors

**Symptoms**: `ModuleNotFoundError: No module named 'data_source_manager'`

**Solutions**:

```bash
# Reinstall in editable mode
uv pip install -e ".[dev]"

# Or sync dependencies
uv sync --dev
```

### Cache Corruption

**Symptoms**: Unexpected data, partial results, or read errors from cache.

**Solutions**:

```bash
# Clear cache
mise run cache:clear

# Or manually
rm -rf ~/.cache/data_source_manager
```

## Debug Mode

Enable verbose logging to see FCP decisions:

```python
import os
os.environ["DSM_LOG_LEVEL"] = "DEBUG"

from data_source_manager import DataSourceManager, DataProvider, MarketType

# Now get_data() logs:
# DEBUG - Cache hit for 2024-01-01
# DEBUG - Cache miss for 2024-01-02, trying Vision
# DEBUG - Vision API downloaded 2024-01-02
# DEBUG - REST fallback for 2024-01-03 (recent data)
```

## FCP Source Verification

Force specific data source for debugging by passing `enforce_source` to `get_data()`:

```python
from datetime import datetime, timedelta, timezone
from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval
from data_source_manager.core.sync.dsm_types import DataSource

manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)

end = datetime.now(timezone.utc)
start = end - timedelta(days=7)

# Force Vision only (skip cache)
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start,
    end_time=end,
    interval=Interval.HOUR_1,
    enforce_source=DataSource.VISION
)

# Force REST only (skip cache and Vision)
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start,
    end_time=end,
    interval=Interval.HOUR_1,
    enforce_source=DataSource.REST
)

# Force cache only (offline mode - no API calls)
df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start,
    end_time=end,
    interval=Interval.HOUR_1,
    enforce_source=DataSource.CACHE
)

manager.close()
```

## Getting Help

1. Check [FCP Protocol Reference](skills/dsm-usage/references/fcp-protocol.md)
2. Review [Market Types](skills/dsm-usage/references/market-types.md)
3. Enable debug logging and check output
4. Run `/debug-fcp SYMBOL` command in Claude Code
