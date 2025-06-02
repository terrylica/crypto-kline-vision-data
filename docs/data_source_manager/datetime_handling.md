# DateTime Handling in Data Source Manager

This document explains the datetime handling and DataFrame consistency enhancements in the Data Source Manager (DSM).

## Core Principles

The Data Source Manager now follows these core principles for datetime handling:

1. **Consistent DateTime Representation**:

   - All timestamps are timezone-aware in UTC
   - `open_time` is available as both an index and a column in returned DataFrames
   - Missing timestamps are represented with NaN values (via reindexing)

2. **Standard Output Format**:

   - All DataFrame columns have consistent data types
   - Column names follow a standardized naming convention
   - Timestamps are in millisecond precision (matching REST API)

3. **Gap Detection and Reporting**:
   - Missing data segments are identified and reported
   - Complete time series reindexing ensures regular intervals

## Using the Data Source Manager

### Core API Pattern

```python
from datetime import datetime, timezone
from core.sync.data_source_manager import DataSourceManager
from utils.market_constraints import DataProvider, MarketType, Interval

# Create a manager
manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

# Fetch data with timezone-aware datetimes
start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
end_time = datetime(2023, 1, 10, tzinfo=timezone.utc)

df = manager.get_data(
    symbol="BTCUSDT",
    start_time=start_time,
    end_time=end_time,
    interval=Interval.MINUTE_1
)

# The resulting DataFrame will have:
# 1. A DatetimeIndex named 'open_time' with timezone-aware UTC datetimes
# 2. An 'open_time' column with the same values as the index
# 3. All expected timestamps in the range, with NaN values for missing data
```

### Working with Returned DataFrames

#### Understanding the Structure

The returned DataFrame has a consistent structure:

- **Index**: `DatetimeIndex` named 'open_time' with timezone-aware UTC datetimes
- **Timestamps**: `open_time` column with UTC datetime objects
- **Numeric Data**: All price and volume columns are floating-point numbers
- **Trade Counts**: The 'count' column is an integer
- **Source Information**: The '\_data_source' column shows the data source for each row

#### Checking for Complete Data

You can verify data completeness using the provided utility:

```python
from utils.dataframe_utils import verify_data_completeness

is_complete, gaps = verify_data_completeness(
    df,
    start_time,
    end_time,
    interval="1m"
)

if not is_complete:
    print(f"Found {len(gaps)} gaps in the data")
    for gap_start, gap_end in gaps:
        print(f"Gap from {gap_start} to {gap_end}")
```

#### Working with Window-Based Calculations

For calculations that require a minimum amount of data:

```python
from utils.for_core.dsm_utilities import check_window_data_completeness

# Check if we have enough data for a 24-period calculation (80% minimum)
has_enough_data, completeness_pct = check_window_data_completeness(
    df,
    window_size=24,
    min_required_pct=80.0
)

if has_enough_data:
    # Proceed with calculation
    result = df['close'].rolling(24).mean()
else:
    print(f"Not enough data available (only {completeness_pct:.1f}%)")
```

## Utility Functions

### Datetime Handling

The `utils/for_core/dsm_utilities.py` module provides several utility functions:

#### `ensure_consistent_timezone`

Ensures datetime objects have a consistent timezone (UTC):

```python
from utils.for_core.dsm_utilities import ensure_consistent_timezone

# Convert naive datetime to timezone-aware
aware_dt = ensure_consistent_timezone(naive_dt)

# Convert string to timezone-aware datetime
aware_dt = ensure_consistent_timezone("2023-01-01T00:00:00")
```

#### `safe_timestamp_comparison`

Safely compares timestamps of different formats:

```python
from utils.for_core.dsm_utilities import safe_timestamp_comparison

# Compare millisecond timestamp to datetime
result = safe_timestamp_comparison(
    1640995200000,  # milliseconds
    datetime(2022, 1, 1, tzinfo=timezone.utc)
)
```

#### `safely_reindex_dataframe`

Creates a complete time series with regular intervals:

```python
from utils.for_core.dsm_utilities import safely_reindex_dataframe

# Reindex with 1-minute intervals, filling forward
complete_df = safely_reindex_dataframe(
    df,
    start_time,
    end_time,
    interval="1m",
    fill_method="ffill"  # Optional
)
```

## Best Practices

1. **Always Use Timezone-Aware Datetimes**:

   ```python
   from datetime import datetime, timezone

   # Good
   dt = datetime(2023, 1, 1, tzinfo=timezone.utc)

   # Avoid
   dt = datetime(2023, 1, 1)  # naive datetime
   ```

2. **Check Data Completeness Before Analysis**:

   ```python
   # Verify data completeness before proceeding
   is_complete, gaps = verify_data_completeness(df, start_time, end_time, interval)
   if not is_complete:
       # Handle missing data appropriately
   ```

3. **Use Defensive Programming with Window Functions**:

   ```python
   # Check window size before calculation
   if len(df) >= 24:  # Minimum window size
       # Proceed with calculation
   else:
       # Handle insufficient data
   ```

4. **Standardize Your Own DataFrames**:

   ```python
   from utils.dataframe_utils import standardize_dataframe

   # Standardize your custom DataFrame to match DSM format
   df = standardize_dataframe(df)
   ```

5. **Implement Error Handling for API Calls**:
   ```python
   try:
       df = manager.get_data(symbol, start_time, end_time, interval)
       if df.empty:
           # Handle empty result
   except Exception as e:
       # Handle exception
   ```

## Troubleshooting

### Common Issues

1. **Timezone Warnings**:

   - Warning: "Converting tz-naive pandas.DatetimeIndex to tz-aware"
   - Solution: Always use timezone-aware datetimes

2. **Type Comparison Errors**:

   - Error: "Cannot compare dtypes int64 and datetime64"
   - Solution: Use `safe_timestamp_comparison` for mixed type comparisons

3. **Missing Data Issues**:

   - Warning: "Data incomplete: X/Y timestamps missing"
   - Solution: Use `safely_reindex_dataframe` to fill gaps or check completeness before analysis

4. **NaN Values in Results**:
   - Observation: DataFrame contains NaN values
   - Explanation: These represent timestamps where data couldn't be retrieved from any source
   - Solution: Use `df.fillna()` methods or handle NaNs explicitly in your analysis

### Diagnostic Checks

To diagnose DataFrame issues:

```python
# Check the DataFrame structure
print(f"Index type: {type(df.index)}")
print(f"Index timezone: {df.index.tz}")
print(f"First timestamp: {df.index[0]}")

# Check for missing values
print(f"Missing values: {df.isna().sum()}")

# Check data sources used
if '_data_source' in df.columns:
    print(f"Data sources: {df['_data_source'].value_counts()}")
```
