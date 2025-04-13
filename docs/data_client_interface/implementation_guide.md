# DataClientInterface Implementation Guide

## Overview

This guide provides comprehensive instructions for implementing the `DataClientInterface` consistently across different data clients. Adhering to these guidelines ensures that all data clients work seamlessly with the `DataSourceManager` and maintain consistent behavior.

## Interface Requirements

The `DataClientInterface` defines the contract that all data clients must fulfill:

```python
class DataClientInterface(ABC):
    @property
    @abstractmethod
    def provider(self) -> DataProvider: ...

    @property
    @abstractmethod
    def chart_type(self) -> ChartType: ...

    @property
    @abstractmethod
    def symbol(self) -> str: ...

    @property
    @abstractmethod
    def interval(self) -> Union[str, object]: ...

    @abstractmethod
    def fetch(self, symbol: str, interval: str, start_time: datetime,
              end_time: datetime, **kwargs) -> pd.DataFrame: ...

    @abstractmethod
    def is_data_available(self, start_time: datetime, end_time: datetime) -> bool: ...

    @abstractmethod
    def create_empty_dataframe(self) -> pd.DataFrame: ...

    @abstractmethod
    def validate_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]: ...

    @abstractmethod
    def close(self) -> None: ...
```

## Implementation Guidelines

### 1. Parameter Standardization

All implementations should handle parameters consistently:

#### Required Parameters

- The `fetch` method's parameters (`symbol`, `interval`, `start_time`, `end_time`) should be treated as required
- If implementations allow fallback to instance defaults, they should document this behavior explicitly

#### Parameter Validation

- All implementations should validate input parameters before processing
- Validation should check for correct types, value ranges, and relationships (e.g., start_time < end_time)
- Implementations should provide clear error messages for invalid inputs

#### Example Parameter Validation

```python
def fetch(self, symbol: str, interval: str, start_time: datetime, end_time: datetime, **kwargs):
    # Validate symbol
    if not isinstance(symbol, str) or not symbol:
        # Either raise an error or fall back to a default
        symbol = self._symbol
        logger.debug(f"Using default symbol: {symbol}")

    # Validate interval
    if not isinstance(interval, str) or not interval:
        # Either raise an error or fall back to a default
        interval = self._interval
        logger.debug(f"Using default interval: {interval}")

    # Validate times
    if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
        raise ValueError("Start and end times must be datetime objects")

    if start_time >= end_time:
        raise ValueError(f"Start time {start_time} must be before end time {end_time}")
```

### 2. Return Type Consistency

All implementations should ensure consistent return types:

#### DataFrame Structure

- Return DataFrames should have consistent column names and types based on chart type
- Index should be properly set according to standards (usually a DatetimeIndex named 'open_time')
- Empty DataFrames should still maintain the correct structure

#### Error Handling

- Implementations should handle errors gracefully and return appropriate error messages
- The `validate_data` method should return a tuple of (is_valid, error_message)

### 3. Documentation Standards

All implementations should have thorough documentation:

#### Method Documentation

- Each method should have a docstring explaining its purpose, parameters, and return values
- Special behaviors or exceptions should be documented
- Parameter validation and default values should be clearly explained

#### Example Documentation

```python
def fetch(self, symbol: str, interval: str, start_time: datetime, end_time: datetime, **kwargs):
    """Fetch data for a specific time range.

    This method retrieves data from the source based on the provided parameters.
    If symbol is not provided or is empty, it falls back to the instance's default symbol.

    Args:
        symbol: Trading pair symbol (e.g., "BTCUSDT")
        interval: Time interval (e.g., "1m", "1h")
        start_time: Start time for data retrieval (timezone-aware datetime)
        end_time: End time for data retrieval (timezone-aware datetime)
        **kwargs: Additional parameters specific to this implementation

    Returns:
        DataFrame with fetched data, properly structured with consistent column names

    Raises:
        ValueError: If time parameters are invalid
        RuntimeError: If data cannot be fetched due to service issues
    """
```

### 4. Error Handling Consistency

All implementations should handle errors consistently:

#### Logging

- Use appropriate log levels (`debug`, `info`, `warning`, `error`) for different situations
- Include relevant details in error messages

#### Recovery

- Implement retry logic where appropriate
- When errors occur, either recover gracefully or provide clear error messages

### 5. Resource Management

All implementations should manage resources properly:

#### Connection Handling

- The `close` method should release all resources held by the client
- Consider implementing context manager support (`__enter__` and `__exit__` methods)
- Ensure HTTP clients are properly closed

## Testing Recommendations

When implementing a new data client:

1. Test parameter validation by providing various invalid inputs
2. Test empty data handling
3. Test error handling by simulating network failures
4. Test resource cleanup
5. Test with the DataSourceManager to ensure integration works correctly

## Current Implementations

For reference, review these existing implementations:

- `RestDataClient`: For fetching real-time data from REST APIs
- `VisionDataClient`: For fetching historical data from file-based sources
- `BinanceFundingRateClient`: For fetching specialized funding rate data

## Common Issues to Avoid

1. **Inconsistent Parameter Handling**: Don't make required parameters optional in one implementation but required in another
2. **Improper Resource Cleanup**: Always ensure the `close()` method releases all resources
3. **Insufficient Error Handling**: Don't assume network requests will always succeed
4. **Poor Documentation**: Don't leave special behaviors undocumented
5. **Type Inconsistencies**: Don't return DataFrames with inconsistent structures or column names
