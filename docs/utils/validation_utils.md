# Validation Utilities (validation_utils.py) [DEPRECATED]

This module provides centralized validation utilities for ensuring data integrity, including DataFrame validation, API boundary validation, and cache validation. These utilities help maintain consistency and reliability across the codebase.

> **IMPORTANT**: This module is now deprecated. Please use the `utils.validation` module and its `DataValidation` and `DataFrameValidator` classes instead.

## Key Components

### Constants and Shared Types

- **`ERROR_TYPES`**: Dictionary of standardized error types for validation failures
- **`CacheValidationError`**: NamedTuple for structured validation error reporting
- **`ValidationOptions`**: Dataclass for configuring validation operations

### Basic Validation Functions

- **`validate_dates(start_time: datetime, end_time: datetime) -> None`** [DEPRECATED]
  - Validates that datetimes are in proper order and timezone-aware
  - Raises ValueError if validation fails
  - **Now available as `DataValidation.validate_dates`**

### Data Availability Validation

- **`validate_data_availability(start_time: datetime, end_time: datetime, consolidation_delay: timedelta = timedelta(hours=48)) -> None`**

  - Validates if data should be available for a given time range
  - Logs warnings for potentially incomplete data

- **`is_data_likely_available(target_date: datetime, consolidation_delay: timedelta = timedelta(hours=48)) -> bool`**
  - Checks if data is likely available for a specified date
  - Returns boolean indicating availability

### DataFrame Validation

- **`validate_dataframe(df: pd.DataFrame) -> None`** [DEPRECATED]

  - Validates DataFrame structure and integrity
  - Checks index type, timezone awareness, column presence, etc.
  - Raises ValueError if validation fails
  - **Now available as `DataFrameValidator.validate_dataframe`**

- **`format_dataframe(df: pd.DataFrame, output_dtypes: Dict[str, str] = OUTPUT_DTYPES) -> pd.DataFrame`** [DEPRECATED]
  - Formats DataFrame to ensure consistent structure
  - Handles index conversion, timezone standardization, etc.
  - Returns formatted DataFrame
  - **Now available as `DataFrameValidator.format_dataframe`**

### File Validation

- **`validate_cache_integrity(file_path: Union[str, Path], min_size: int = MIN_VALID_FILE_SIZE, max_age: timedelta = MAX_CACHE_AGE) -> Optional[Dict[str, Any]]`**

  - Validates cache file integrity
  - Checks existence, size, and age
  - Returns error information or None if valid

- **`calculate_checksum(file_path: Path) -> str`**
  - Calculates SHA-256 checksum of a file
  - Returns hexadecimal checksum string

### API Validation

#### ApiValidator Class [DEPRECATED]

The `ApiValidator` class provides methods for validating data against Binance API behavior.

- **`__init__(self, api_boundary_validator: Optional[ApiBoundaryValidator] = None)`**

  - Initializes the validator with an optional ApiBoundaryValidator

- **`validate_api_time_range(self, start_time: datetime, end_time: datetime, interval: Union[str, Interval], symbol: str = "BTCUSDT") -> bool`**

  - Validates if a time range is valid for the API
  - Returns boolean indicating validity

- **`get_api_aligned_boundaries(self, start_time: datetime, end_time: datetime, interval: Union[str, Interval], symbol: str = "BTCUSDT") -> Dict[str, Any]`**

  - Gets API-aligned boundaries for a time range
  - Returns dictionary with boundary information

- **`does_data_range_match_api_response(self, df: pd.DataFrame, start_time: datetime, end_time: datetime, interval: Interval, symbol: str = "BTCUSDT") -> bool`**
  - Checks if DataFrame matches what API would return
  - Returns boolean indicating match

### Comprehensive Data Validation

#### DataValidator Class [DEPRECATED]

The `DataValidator` class provides comprehensive data validation including structure and API alignment.

- **`__init__(self, api_validator: Optional[ApiValidator] = None)`**

  - Initializes the validator with an optional ApiValidator

- **`validate_data(self, df: pd.DataFrame, options: ValidationOptions = None) -> Optional[CacheValidationError]`**

  - Validates data structure and content
  - Returns ValidationError if invalid, None if valid

- **`align_data_to_api_boundaries(self, df: pd.DataFrame, start_time: datetime, end_time: datetime, interval: Interval, symbol: str = TEST_SYMBOL) -> pd.DataFrame`**
  - Aligns data to match API boundaries
  - Returns aligned DataFrame

## Usage Examples

```python
# Basic validation - USE UPDATED MODULES
from utils.validation import DataValidation

# Validate a time window
from datetime import datetime, timezone, timedelta
start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
end_time = datetime(2023, 1, 2, tzinfo=timezone.utc)
DataValidation.validate_time_window(start_time, end_time)

# DataFrame validation - USE UPDATED MODULES
import pandas as pd
from utils.validation import DataFrameValidator

# Create a DataFrame
df = pd.DataFrame(...)

# Validate structure
DataFrameValidator.validate_dataframe(df)  # Raises ValueError if invalid

# Format to ensure consistent structure
formatted_df = DataFrameValidator.format_dataframe(df)
```
