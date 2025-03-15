# Pandas Deprecation Standards

## Overview

This document provides standardized rules for handling deprecated pandas operations, particularly focusing on Timedelta string formats and other common deprecation warnings.

### Motivation

1. **Consistency**: Ensure consistent handling of pandas operations across the codebase
2. **Future-proofing**: Prevent deprecation warnings by using the latest pandas standards
3. **Maintainability**: Centralize all deprecation-related rules in one place
4. **Documentation**: Provide clear guidance on proper usage patterns

### Key Areas Addressed

1. Timedelta string formats
2. DataFrame operation standards
3. Date/time handling conventions

## Time Unit Standards

### Valid Time Units

The following time units are compliant with pandas>=2.1.0:

| Unit | Pandas Format | Description |
|------|--------------|-------------|
| Second | "s" | Seconds |
| Minute | "min" | Minutes |
| Hour | "h" | Hours |
| Day | "D" | Days |
| Week | "W" | Weeks |
| Month | "M" | Months |
| Year | "Y" | Years |

### Market Interval Mappings

When converting market intervals to pandas time units:

```python
INTERVAL_MAP = {
    "1s": "s",  # 1 second
    "1m": "min",  # 1 minute
    "3m": "min",  # 3 minutes
    "5m": "min",  # 5 minutes
    "15m": "min", # 15 minutes
    "30m": "min", # 30 minutes
    "1h": "h",   # 1 hour
    "2h": "h",   # 2 hours
    "4h": "h",   # 4 hours
    "6h": "h",   # 6 hours
    "8h": "h",   # 8 hours
    "12h": "h",  # 12 hours
    "1d": "D",   # 1 day
    "3d": "D",   # 3 days
    "1w": "W",   # 1 week
    "1M": "M"    # 1 month
}
```

## Best Practices

### Timedelta Creation

::: {.callout-warning}

#### Deprecated

```python
# Don't use string formats directly
td = pd.Timedelta("1 day")
td = pd.Timedelta("24 hours")
```

:::

::: {.callout-tip}

#### Recommended

```python
# Use explicit unit specifications
td = pd.Timedelta(days=1)
td = pd.Timedelta(hours=24)
```

:::

### Interval Parsing

When parsing intervals:

1. Always validate the input format
2. Use explicit numeric values and units
3. Handle edge cases gracefully

Example:

```python
def parse_interval(interval: str) -> pd.Timedelta:
    """Parse interval string to pandas Timedelta."""
    if not interval:
        raise ValueError("Interval string cannot be empty")
    
    # Extract numeric value and unit
    numeric = ""
    for char in interval:
        if char.isdigit():
            numeric += char
        else:
            unit = interval[len(numeric):]
            break
    else:
        raise ValueError(f"No time unit found in interval: {interval}")
    
    value = int(numeric)
    if unit not in INTERVAL_MAP:
        raise ValueError(f"Invalid time unit: {unit}")
    
    return pd.Timedelta(value, unit=INTERVAL_MAP[unit])
```

### Validation Rules

1. Interval values must be integers
2. Values must be between 1 and 1000
3. Units must match pandas-compliant formats
4. Empty or malformed intervals should raise explicit errors

## Error Handling

### Common Errors

1. **IntervalParseError**: Invalid interval format
2. **ValueError**: Invalid numeric values
3. **TypeError**: Wrong type for interval value

### Error Messages

Error messages should be:

1. Descriptive of the issue
2. Include the problematic value
3. Suggest correct format if possible

Example:

```python
try:
    interval = parse_interval("1x")  # Invalid unit
except ValueError as e:
    logger.error(f"Invalid interval format: 1x. Valid units are: s, min, h, D, W, M, Y")
```

## Migration Guide

When updating existing code:

1. Scan for deprecated string formats
2. Replace with explicit Timedelta constructors
3. Update any custom parsing logic
4. Add validation for all interval inputs

### Common Migration Patterns

| Old Pattern | New Pattern |
|------------|-------------|
| `pd.Timedelta("1D")` | `pd.Timedelta(days=1)` |
| `pd.Timedelta("60min")` | `pd.Timedelta(minutes=60)` |
| `pd.Timedelta("3600s")` | `pd.Timedelta(seconds=3600)` |

## Testing Standards

Tests should verify:

1. Correct parsing of all valid intervals
2. Proper error handling for invalid inputs
3. Consistent behavior with pandas operations
4. Compatibility with market interval conversions

Example test cases:

```python
def test_interval_parsing():
    assert parse_interval("1s").total_seconds() == 1
    assert parse_interval("5min").total_seconds() == 300
    assert parse_interval("2h").total_seconds() == 7200
    
    with pytest.raises(ValueError):
        parse_interval("")  # Empty string
    with pytest.raises(ValueError):
        parse_interval("1x")  # Invalid unit
    with pytest.raises(ValueError):
        parse_interval("0s")  # Zero value
```
