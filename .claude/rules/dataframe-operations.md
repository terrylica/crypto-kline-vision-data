---
paths:
  - "src/data_source_manager/**/*.py"
  - "examples/**/*.py"
  - "tests/**/*.py"
---

# DataFrame Operations Rules

Guidelines for working with market data DataFrames in DSM.

## Library Preference

**Polars preferred over Pandas** for new code.

```python
# Preferred
import polars as pl
df = pl.DataFrame(data)

# Legacy (acceptable for compatibility)
import pandas as pd
df = pd.DataFrame(data)
```

## Index Conventions

Market data DataFrames use `open_time` as the index:

```python
# Expected structure
df.index.name == "open_time"
df.index.is_monotonic_increasing == True
df.index.tz == timezone.utc  # or "UTC"
```

## Column Names

Standard OHLCV columns (lowercase):

| Column | Type    | Description    |
| ------ | ------- | -------------- |
| open   | float64 | Opening price  |
| high   | float64 | Highest price  |
| low    | float64 | Lowest price   |
| close  | float64 | Closing price  |
| volume | float64 | Trading volume |

## Validation Before Use

Always validate DataFrames from external sources:

```python
def validate_ohlcv(df):
    assert len(df) > 0, "Empty DataFrame"
    assert df.index.is_monotonic_increasing, "Unsorted timestamps"
    assert not df.index.has_duplicates, "Duplicate timestamps"
    assert (df["high"] >= df["low"]).all(), "Invalid high/low"
    assert (df["volume"] >= 0).all(), "Negative volume"
```

## Memory Efficiency

For large datasets:

```python
# Use Arrow for disk storage
df.to_parquet("data.parquet")

# Use mmap for reading
df = pl.read_parquet("data.parquet", memory_map=True)
```
