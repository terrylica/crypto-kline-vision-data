# Examples Directory

Context-specific instructions for working with DSM examples.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [src/](../src/CLAUDE.md) | [tests/](../tests/CLAUDE.md) | [docs/](../docs/CLAUDE.md)

---

## Quick Start

```bash
# Run any example
uv run -p 3.13 python examples/quick_start.py

# Run with debug logging
DSM_LOG_LEVEL=DEBUG uv run -p 3.13 python examples/dsm_logging_demo.py
```

---

## Directory Structure

| Directory     | Purpose                |
| ------------- | ---------------------- |
| `sync/`       | Synchronous CLI demos  |
| `lib_module/` | Library usage patterns |
| Root files    | Quick start and demos  |

**Root-level examples**: `quick_start.py`, `dsm_logging_demo.py`, `dsm_lazy_initialization_demo.py`, `clean_feature_engineering_example.py`

---

## Example Conventions

1. **Use package imports**: `from data_source_manager import ...` (NOT relative imports)
2. **Always use UTC datetimes**: `datetime.now(timezone.utc)`
3. **Always close managers**: `manager.close()` or use context managers
4. **Include helpful comments**: Examples are documentation
5. **Keep examples self-contained**: Minimal external dependencies
6. **No duplicate demos**: Check existing examples before creating new ones

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
from data_source_manager.utils.for_core.rest_exceptions import RateLimitError, RestAPIError

try:
    df = manager.get_data(symbol, start, end, interval)
except RateLimitError:
    print("Rate limited - wait and retry")
except RestAPIError as e:
    print(f"REST API error: {e}")
finally:
    manager.close()
```

---

## Related

- @docs/skills/dsm-usage/SKILL.md - Full API usage guide
- @docs/skills/dsm-usage/examples/ - More detailed examples
