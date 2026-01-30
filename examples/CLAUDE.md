# Examples Directory

Context-specific instructions for working with DSM examples.

**Parent context**: See [/CLAUDE.md](/CLAUDE.md) for project-wide conventions.

---

## Quick Start

```bash
# Run any example
uv run -p 3.13 python examples/quick_start.py

# Run with debug logging
DEBUG=1 uv run -p 3.13 python examples/dsm_logging_demo.py
```

---

## Directory Structure

| Directory     | Purpose                         |
| ------------- | ------------------------------- |
| `sync/`       | Synchronous CLI demos           |
| `lib_module/` | Library usage patterns          |
| `tutorials/`  | Step-by-step learning materials |

---

## Example Conventions

1. **Always use UTC datetimes**: `datetime.now(timezone.utc)`
2. **Always close managers**: `manager.close()` or use context managers
3. **Include helpful comments**: Examples are documentation
4. **Keep examples self-contained**: Minimal external dependencies

---

## Common Patterns

### Basic Fetch

```python
from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval
from datetime import datetime, timedelta, timezone

manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
end = datetime.now(timezone.utc)
start = end - timedelta(days=7)

df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
print(f"Fetched {len(df)} bars")
manager.close()
```

### With Error Handling

```python
from data_source_manager.core.errors import DataSourceError, RateLimitError

try:
    df = manager.get_data(symbol, start, end, interval)
except RateLimitError:
    print("Rate limited - wait and retry")
except DataSourceError as e:
    print(f"Data source error: {e}")
finally:
    manager.close()
```

---

## Related

- @docs/skills/dsm-usage/SKILL.md - Full API usage guide
- @docs/skills/dsm-usage/examples/ - More detailed examples
