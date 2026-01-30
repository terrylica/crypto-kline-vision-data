---
name: validate-data
description: Validate data integrity from a DataFrame
---

# Validate Data Integrity

Run data quality checks on fetched market data.

## Checks Performed

1. **No gaps** in timestamp sequence for the given interval
2. **No duplicates** in the index
3. **Monotonic increasing** timestamps
4. **UTC timezone** awareness
5. **Reasonable values** for OHLCV columns

## Usage

After fetching data with `/fetch-data`, run this to validate.

## Implementation

```python
import pandas as pd
from datetime import timezone

def validate_dataframe(df: pd.DataFrame, interval_minutes: int = 60) -> list[str]:
    """Validate a market data DataFrame.

    Args:
        df: DataFrame with open_time index and OHLCV columns
        interval_minutes: Expected interval in minutes

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    # Check 1: Not empty
    if len(df) == 0:
        errors.append("❌ DataFrame is empty")
        return errors

    # Check 2: Index is monotonic increasing
    if not df.index.is_monotonic_increasing:
        errors.append("❌ Timestamps not in ascending order")

    # Check 3: No duplicate indices
    if df.index.has_duplicates:
        dup_count = df.index.duplicated().sum()
        errors.append(f"❌ Found {dup_count} duplicate timestamps")

    # Check 4: Timezone aware (UTC)
    if df.index.tz is None:
        errors.append("⚠️ Index is not timezone-aware (should be UTC)")
    elif str(df.index.tz) != "UTC":
        errors.append(f"⚠️ Index timezone is {df.index.tz}, expected UTC")

    # Check 5: Check for gaps
    expected_delta = pd.Timedelta(minutes=interval_minutes)
    actual_deltas = df.index.to_series().diff().dropna()
    gaps = actual_deltas[actual_deltas > expected_delta * 1.5]
    if len(gaps) > 0:
        errors.append(f"❌ Found {len(gaps)} gaps in timestamp sequence")

    # Check 6: OHLCV sanity
    if 'high' in df.columns and 'low' in df.columns:
        invalid_hl = (df['high'] < df['low']).sum()
        if invalid_hl > 0:
            errors.append(f"❌ Found {invalid_hl} bars where high < low")

    if 'volume' in df.columns:
        neg_vol = (df['volume'] < 0).sum()
        if neg_vol > 0:
            errors.append(f"❌ Found {neg_vol} bars with negative volume")

    if not errors:
        print("✅ All validation checks passed")
        print(f"   Rows: {len(df)}")
        print(f"   Range: {df.index[0]} to {df.index[-1]}")
    else:
        for error in errors:
            print(error)

    return errors

# Run validation on 'df' if it exists in scope
# validate_dataframe(df, interval_minutes=60)
```

Copy the function above and call `validate_dataframe(df)` on your DataFrame.
