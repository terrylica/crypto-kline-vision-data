# Validation Utilities (validation_utils.py)

This module provides centralized validation utilities for ensuring data integrity, including DataFrame validation, API boundary validation, and cache validation. These utilities help maintain consistency and reliability across the codebase.

## Key Components

### Constants and Shared Types

- **`ERROR_TYPES`**: Dictionary of standardized error types for validation failures
- **`CacheValidationError`**: NamedTuple for structured validation error reporting
- **`ValidationOptions`**: Dataclass for configuring validation operations

### Basic Validation Functions

- **`validate_dates(start_time: datetime, end_time: datetime) -> None`**

  - Validates that datetimes are in proper order and timezone-aware
  - Raises ValueError if validation fails

- **`validate_time_window(start_time: datetime, end_time: datetime) -> None`**

  - Validates time window against maximum allowed range
  - Calls validate_dates first
  - Raises ValueError if validation fails

- **`validate_time_range(start_time: Optional[datetime], end_time: Optional[datetime]) -> tuple[Optional[datetime], Optional[datetime]]`**

  - Normalizes and validates time range parameters
  - Returns normalized start and end times

- **`validate_interval(interval: str, market_type: str = "SPOT") -> None`**

  - Validates that an interval string is valid for the specified market type
  - Raises ValueError for invalid intervals

- **`validate_symbol_format(symbol: str, market_type: str = "SPOT") -> None`**
  - Validates trading pair symbol format
  - Raises ValueError for invalid symbols

### Data Availability Validation

- **`validate_data_availability(start_time: datetime, end_time: datetime, consolidation_delay: timedelta = timedelta(hours=48)) -> None`**

  - Validates if data should be available for a given time range
  - Logs warnings for potentially incomplete data

- **`is_data_likely_available(target_date: datetime, consolidation_delay: timedelta = timedelta(hours=48)) -> bool`**
  - Checks if data is likely available for a specified date
  - Returns boolean indicating availability

### DataFrame Validation

- **`validate_dataframe(df: pd.DataFrame) -> None`**

  - Validates DataFrame structure and integrity
  - Checks index type, timezone awareness, column presence, etc.
  - Raises ValueError if validation fails

- **`format_dataframe(df: pd.DataFrame, output_dtypes: Dict[str, str] = OUTPUT_DTYPES) -> pd.DataFrame`**
  - Formats DataFrame to ensure consistent structure
  - Handles index conversion, timezone standardization, etc.
  - Returns formatted DataFrame

### File Validation

- **`validate_cache_integrity(file_path: Union[str, Path], min_size: int = MIN_VALID_FILE_SIZE, max_age: timedelta = MAX_CACHE_AGE) -> Optional[Dict[str, Any]]`**

  - Validates cache file integrity
  - Checks existence, size, and age
  - Returns error information or None if valid

- **`calculate_checksum(file_path: Path) -> str`**
  - Calculates SHA-256 checksum of a file
  - Returns hexadecimal checksum string

### API Validation

#### ApiValidator Class

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

#### DataValidator Class

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
# Basic validation
from utils.validation_utils import validate_symbol_format, validate_interval

# Validate a trading pair symbol
validate_symbol_format("BTCUSDT")  # Passes
validate_symbol_format("btcusdt")  # Raises ValueError - should be uppercase

# Validate an interval
validate_interval("1m", "SPOT")  # Passes
validate_interval("1s", "FUTURES")  # Raises ValueError - 1s not available for futures

# DataFrame validation
import pandas as pd
from utils.validation_utils import validate_dataframe, format_dataframe

# Create a DataFrame
df = pd.DataFrame(...)

# Validate structure
validate_dataframe(df)  # Raises ValueError if invalid

# Format to ensure consistent structure
formatted_df = format_dataframe(df)

# API validation
from datetime import datetime, timezone
from utils.validation_utils import ApiValidator
from utils.api_boundary_validator import ApiBoundaryValidator
from utils.market_constraints import MarketType, Interval

# Create validators
api_boundary_validator = ApiBoundaryValidator(MarketType.SPOT)
api_validator = ApiValidator(api_boundary_validator)

# Check if time range is valid
start_time = datetime(2023, 1, 1, tzinfo=timezone.utc)
end_time = datetime(2023, 1, 2, tzinfo=timezone.utc)
interval = Interval.HOUR_1

is_valid = await api_validator.validate_api_time_range(start_time, end_time, interval)

# Get API-aligned boundaries
boundaries = await api_validator.get_api_aligned_boundaries(start_time, end_time, interval)

# Comprehensive data validation
from utils.validation_utils import DataValidator, ValidationOptions

# Create validator
data_validator = DataValidator(api_validator)

# Configure validation options
options = ValidationOptions(
    allow_empty=False,
    start_time=start_time,
    end_time=end_time,
    interval=interval,
    symbol="BTCUSDT"
)

# Validate data
validation_error = await data_validator.validate_data(df, options)
if validation_error is None:
    print("Data is valid!")
else:
    print(f"Validation failed: {validation_error.message}")
```
