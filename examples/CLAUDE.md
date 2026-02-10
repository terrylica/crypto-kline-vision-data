# Examples Directory

Context-specific instructions for working with CKVD examples.

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [src/](../src/CLAUDE.md) | [tests/](../tests/CLAUDE.md) | [docs/](../docs/CLAUDE.md) | [scripts/](../scripts/CLAUDE.md) | [playground/](../playground/CLAUDE.md)

---

## Quick Start

```bash
# Run any example (direct or via mise)
uv run -p 3.13 python examples/quick_start.py
mise run demo:quickstart

# Run with debug logging
CKVD_LOG_LEVEL=DEBUG uv run -p 3.13 python examples/ckvd_logging_demo.py
```

---

## Directory Structure

| Directory  | Purpose                                     |
| ---------- | ------------------------------------------- |
| `sync/`    | Synchronous examples (datetime, 1s testing) |
| Root files | Quick start, logging, lazy init demos       |

**Root-level examples**: `quick_start.py`, `ckvd_logging_demo.py`, `ckvd_lazy_initialization_demo.py`, `clean_feature_engineering_example.py`, `ckvd_cache_control_example.py`

---

## Example Conventions

1. **Use package imports**: `from ckvd import ...` (NOT relative imports)
2. **Always use UTC datetimes**: `datetime.now(timezone.utc)`
3. **Always close managers**: `manager.close()` or use context managers
4. **Emit structured NDJSON telemetry**: Use `_telemetry.py` — no `print()` or `rich`
5. **Keep examples self-contained**: Minimal external dependencies
6. **No duplicate demos**: Check existing examples before creating new ones

---

## NDJSON Telemetry

All examples emit structured NDJSON to `examples/logs/events.jsonl` via `_telemetry.py`. No `print()` or `rich` in examples.

### Shared Module: `_telemetry.py`

| Export           | Purpose                                                                         |
| ---------------- | ------------------------------------------------------------------------------- |
| `init_telemetry` | Add NDJSON + console sinks, bind provenance, return logger                      |
| `timed_span`     | Context manager emitting `*_started`/`*_completed`/`*_failed` with `latency_ms` |

### Domain Event Types

| Category          | Events                                                                                                    | When                                    |
| ----------------- | --------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| Lifecycle         | `session_started`, `session_completed`, `section_started`                                                 | Run boundaries, demo section markers    |
| CKVD Operations   | `manager_creating`, `manager_created`, `fetch_started`, `fetch_completed`, `fetch_failed`, `fetch_detail` | Factory creation, FCP fetch lifecycle   |
| Analysis          | `validation_result`, `feature_computed`, `data_sample`                                                    | Data quality checks, computed features  |
| Config/Benchmarks | `config_documented`, `config_state`, `benchmark_result`, `profiling_hint`                                 | Configuration capture, perf measurement |

### Trace Correlation

- **`trace_id`** (16-char hex): Groups all events from a single example run. Generated once per `init_telemetry()` call.
- **`span_id`** (8-char hex): Links a `timed_span`'s `*_started`/`*_completed`/`*_failed` triplet. Useful for matching fetch start with its completion and latency.

### ResilientLogger

CKVD's `loguru_setup.py` calls `logger.remove()` during `CryptoKlineVisionData.create()`, which destroys all loguru sinks — including telemetry. The `ResilientLogger` wrapper in `_telemetry.py` auto-heals by checking `_loguru_logger._core.handlers` before every emit and re-adding sinks if missing. This is why examples must use `init_telemetry()`, not raw loguru.

### Known Limitation

`fetch_completed` events include `latency_ms` but do not report which FCP source (CACHE, VISION, REST) was used. To see source distribution, check the `_data_source` column in the returned DataFrame (see `sync/ckvd_datetime_example.py` for an example).

### Pattern

```python
from _telemetry import init_telemetry, timed_span

tlog = init_telemetry("example_name")

tlog.bind(event_type="manager_created", venue="binance").info("Created")

with timed_span(tlog, "fetch", symbol="BTCUSDT", interval="1h"):
    df = manager.get_data(...)
```

### Subdirectory Import

Files in `sync/` need a sys.path adjustment:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _telemetry import init_telemetry, timed_span
```

---

## Common Patterns

### Basic Fetch

```python
from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval
from datetime import datetime, timedelta, timezone

manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
end = datetime.now(timezone.utc)
start = end - timedelta(days=7)

df = manager.get_data("BTCUSDT", start, end, Interval.HOUR_1)
print(f"Fetched {len(df)} bars")
manager.close()
```

### With Error Handling

```python
from ckvd.utils.for_core.rest_exceptions import RateLimitError, RestAPIError

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

- @docs/skills/ckvd-usage/SKILL.md - Full API usage guide
- @docs/skills/ckvd-usage/examples/ - More detailed examples
