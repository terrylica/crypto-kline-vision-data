---
paths:
  - "src/data_source_manager/**/*.py"
  - "examples/**/*.py"
  - "tests/**/*.py"
---

# Timestamp Handling Rules

Critical rules for timestamp handling in Data Source Manager.

## Always UTC

```python
# ✅ CORRECT
from datetime import datetime, timezone
now = datetime.now(timezone.utc)

# ❌ WRONG - Naive datetime
now = datetime.now()  # No timezone info!
```

## open_time Semantics

The `open_time` column represents the **start** of the candle period:

```
open_time = 2024-01-15 14:00:00 UTC
interval  = 1h

This candle covers: 14:00:00 - 14:59:59 UTC
```

## Conversion Patterns

```python
# Pandas timestamp to datetime
dt = pd.Timestamp('2024-01-15 14:00:00+00:00').to_pydatetime()

# Unix milliseconds to datetime
ms = 1705330800000
dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

# datetime to Unix milliseconds
ms = int(dt.timestamp() * 1000)
```

## DataFrame Index

Market data DataFrames should have:

- Index: `open_time` (timezone-aware, UTC)
- Monotonic increasing: `df.index.is_monotonic_increasing == True`
- No duplicates: `df.index.has_duplicates == False`

## Common Pitfalls

1. **Mixing timezones**: Never mix UTC and local time
2. **Naive comparison**: `datetime.now()` vs `df.index[0]` may fail
3. **Boundary errors**: End of period vs start of next period
