# Time Utilities (time_utils.py)

This module provides centralized time-related utilities for handling datetime objects, timezone conversions, interval calculations, and time boundary alignment. These utilities are essential for consistent time handling across the codebase.

## Key Functions

### Timezone Handling

- **`enforce_utc_timezone(dt: datetime) -> datetime`**
  - Ensures datetime objects are timezone-aware and in UTC
  - Returns a new datetime object if the input is naive or in a different timezone

### Time Validation

- **`validate_dates(start_time: datetime, end_time: datetime) -> None`**
  - Validates that start_time is before end_time
  - Ensures both datetimes are timezone-aware
  - Raises ValueError if validation fails

### Interval Calculations

- **`get_interval_micros(interval: Union[str, MarketInterval]) -> int`**

  - Gets interval in microseconds
  - Supports both string and MarketInterval enum input

- **`get_interval_seconds(interval: Union[str, MarketInterval]) -> int`**

  - Gets interval in seconds
  - Useful for API calls and time calculations

- **`get_interval_timedelta(interval: Union[str, MarketInterval]) -> timedelta`**

  - Gets interval as a timedelta object
  - Convenient for datetime arithmetic

- **`get_interval_floor(dt: datetime, interval: Union[str, MarketInterval]) -> datetime`**

  - Gets floor datetime aligned to interval boundary
  - Useful for standardizing start times

- **`get_interval_ceiling(dt: datetime, interval: Union[str, MarketInterval]) -> datetime`**
  - Gets ceiling datetime aligned to interval boundary
  - Useful for standardizing end times

### Bar Analysis

- **`get_bar_close_time(bar_open_time: datetime, interval: Union[str, MarketInterval]) -> datetime`**

  - Calculates exact close time for a bar given its open time
  - Takes interval into account

- **`is_bar_complete(bar_open_time: datetime, interval: Union[str, MarketInterval], current_time: Optional[datetime] = None) -> bool`**
  - Determines if a bar should be complete based on current time
  - Useful for real-time data processing

### DataFrame Operations

- **`filter_dataframe_by_time(df: pd.DataFrame, start_time: datetime, end_time: datetime) -> pd.DataFrame`**
  - Filters DataFrame to include only rows within the specified time range
  - Handles inclusive start and end times

### API Boundary Alignment

- **`align_time_boundaries(start_time: datetime, end_time: datetime, interval: Union[str, MarketInterval]) -> tuple[datetime, datetime]`**

  - Aligns start and end times to interval boundaries
  - Ensures consistent alignment with API expectations

- **`estimate_record_count(start_time: datetime, end_time: datetime, interval: Union[str, MarketInterval]) -> int`**

  - Estimates number of records for a given time range and interval
  - Useful for API request planning and validation

- **`vision_api_time_window_alignment(window_size: int) -> timedelta`**

  - Provides the appropriate time window for Vision API based on window size
  - Helps in preventing excessive data requests

- **`align_vision_api_to_rest(start_time: datetime, end_time: datetime, interval: Union[str, MarketInterval]) -> Dict[str, Any]`**
  - Aligns Vision API request boundaries to REST API expectations
  - Returns a dictionary with aligned boundaries and record count

## Usage Examples

```python
# Timezone handling
from datetime import datetime
from utils.time_utils import enforce_utc_timezone

dt = datetime(2023, 1, 1, 12, 0, 0)  # Naive datetime
dt_utc = enforce_utc_timezone(dt)  # Now timezone-aware

# Time window validation
from utils.validation import DataValidation
from datetime import datetime, timezone, timedelta

start = datetime(2023, 1, 1, tzinfo=timezone.utc)
end = start + timedelta(days=7)
DataValidation.validate_time_window(start, end)  # Validates the time window

# Interval calculations
from utils.time_utils import get_interval_floor, get_interval_ceiling
from utils.market_constraints import Interval

dt = datetime(2023, 1, 1, 12, 30, 45, tzinfo=timezone.utc)
interval = Interval.HOUR_1

floor_dt = get_interval_floor(dt, interval)  # 2023-01-01 12:00:00+00:00
ceiling_dt = get_interval_ceiling(dt, interval)  # 2023-01-01 13:00:00+00:00

# DataFrame filtering
import pandas as pd
from utils.time_utils import filter_dataframe_by_time

df = pd.DataFrame(...)  # DataFrame with datetime index
filtered_df = filter_dataframe_by_time(df, start_time, end_time)
```
