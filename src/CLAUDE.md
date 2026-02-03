# Source Code Directory

Context-specific instructions for working with DSM source code.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [tests/](../tests/CLAUDE.md) | [docs/](../docs/CLAUDE.md) | [examples/](../examples/CLAUDE.md)

---

## Package Structure

```
src/data_source_manager/
├── __init__.py              # Public API exports
├── core/
│   ├── sync/
│   │   ├── data_source_manager.py  # Main DSM class with FCP
│   │   ├── dsm_types.py            # DataSource, DataSourceConfig
│   │   └── dsm_lib.py              # High-level functions
│   └── providers/
│       └── binance/
│           ├── vision_data_client.py   # Vision API (S3)
│           ├── rest_data_client.py     # REST API
│           └── cache_manager.py        # Arrow cache
└── utils/
    ├── market_constraints.py    # Enums and validation
    ├── loguru_setup.py          # Logging configuration
    └── for_core/                # Internal utilities
        ├── rest_exceptions.py   # REST API exceptions
        └── vision_exceptions.py # Vision API exceptions
```

---

## Key Classes

| Class               | Location                           | Purpose                   |
| ------------------- | ---------------------------------- | ------------------------- |
| `DataSourceManager` | `core/sync/data_source_manager.py` | Main entry point with FCP |
| `DataSourceConfig`  | `core/sync/dsm_types.py`           | Configuration dataclass   |
| `DataSource`        | `core/sync/dsm_types.py`           | Data source enum          |
| `DataProvider`      | `utils/market_constraints.py`      | Provider enum (BINANCE)   |
| `MarketType`        | `utils/market_constraints.py`      | Market type enum          |
| `Interval`          | `utils/market_constraints.py`      | Timeframe interval enum   |

---

## FCP Implementation (core/sync/data_source_manager.py)

The Failover Control Protocol orchestrates data retrieval:

```
1. Cache check (Arrow files) → Fast path (~1ms)
2. Vision API (S3) → Bulk historical (~1-5s)
3. REST API → Real-time fallback (~100-500ms)
```

Key methods:

- `get_data()` - Main entry point, implements FCP
- `_get_from_cache()` - Check local Arrow cache
- `_fetch_from_vision()` - Fetch from Binance Vision
- `_fetch_from_rest()` - Fall back to REST API

---

## Code Patterns

### Exception Handling

```python
# CORRECT: Specific exceptions
from data_source_manager.utils.for_core.rest_exceptions import RateLimitError, RestAPIError
from data_source_manager.utils.for_core.vision_exceptions import VisionAPIError

try:
    df = manager.get_data(...)
except RateLimitError:
    logger.warning("Rate limited, retrying...")
except (RestAPIError, VisionAPIError) as e:
    logger.error(f"Data fetch failed: {e}")
    raise
```

### Timestamp Handling

```python
# CORRECT: Always UTC
from datetime import datetime, timezone

now = datetime.now(timezone.utc)
start = now - timedelta(days=7)
```

### HTTP Requests

```python
# CORRECT: Always with timeout
response = httpx.get(url, timeout=30)
```

---

## Modification Guidelines

1. **Never use bare `except:`** - Always catch specific exceptions
2. **Always use UTC datetimes** - `datetime.now(timezone.utc)`
3. **Always add HTTP timeouts** - Explicit `timeout=` parameter
4. **Match symbol format to market type** - BTCUSDT vs BTCUSD_PERP
5. **Close managers** - Always call `manager.close()`

---

## Related

- @.claude/rules/fcp-protocol.md - FCP decision logic
- @.claude/rules/error-handling.md - Exception patterns
- @docs/adr/2025-01-30-failover-control-protocol.md - FCP architecture
