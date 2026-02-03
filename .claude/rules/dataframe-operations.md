---
paths:
  - "src/data_source_manager/**/*.py"
  - "examples/**/*.py"
  - "tests/**/*.py"
---

# DataFrame Operations Rules

Guidelines for working with market data DataFrames in DSM.

## Library Preference

**DSM public API returns Pandas DataFrames** for backward compatibility.

```python
# DSM returns pd.DataFrame
from data_source_manager import DataSourceManager
df = manager.get_data(...)  # Returns pd.DataFrame

# Internal utilities may use Polars for performance
# Cache uses Apache Arrow for storage
```

**Note**: Polars is used internally for some operations, but the public `get_data()`
API returns `pd.DataFrame` to maintain compatibility with existing consumer code.

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
# Use Arrow for disk storage (pandas)
df.to_parquet("data.parquet")

# For reading cached data, DSM uses memory-mapped Arrow files internally
# Direct parquet reading with pandas:
df = pd.read_parquet("data.parquet")
```
