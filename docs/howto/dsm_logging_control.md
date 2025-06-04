# DSM Logging Control Guide

## Overview

The Data Source Manager (DSM) provides comprehensive logging control to address the common issue of cluttered console output during feature engineering workflows. This guide shows how to configure DSM logging levels to achieve clean, professional output.

## Problem Statement

When using DSM in feature engineering components, extensive logging output clutters the console:

```
[2024-06-04 10:15:23] INFO: DSM Cache: Checking cache for SOLUSDT_1s_2022-05-04_16:00:00_2022-05-04_16:15:00
[2024-06-04 10:15:23] DEBUG: FCP Utils: Fetching data from Binance API
[2024-06-04 10:15:23] INFO: DSM Cache: Cache miss, fetching from source
[2024-06-04 10:15:24] DEBUG: DataFrame Utils: Processing 900 records
[2024-06-04 10:15:24] INFO: DSM Cache: Storing data in cache
... (hundreds of similar lines)
```

This forces developers to add boilerplate logging suppression code in every component.

## Solution: Configurable Logging Levels

DSM provides multiple ways to control logging levels without any code changes to existing DSM usage.

### Method 1: Environment Variable Control (Recommended)

The simplest and most effective approach:

```bash
# Clean output for feature engineering (suppress all non-critical DSM logs)
export DSM_LOG_LEVEL=CRITICAL

# Normal development with basic info
export DSM_LOG_LEVEL=INFO

# Detailed debugging
export DSM_LOG_LEVEL=DEBUG
```

#### Usage Example

```python
# Clean feature engineering code - no boilerplate needed!
import os
os.environ["DSM_LOG_LEVEL"] = "CRITICAL"

from core.sync.data_source_manager import DataSourceManager
from utils.market_constraints import DataProvider, MarketType, Interval

# Create DSM instance - minimal logging
dsm = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

# Fetch data - clean output
data = dsm.get_data(
    symbol="SOLUSDT",
    start_time=start_time,
    end_time=end_time,
    interval=Interval.MINUTE_1,
)

# Only your feature engineering logs are visible!
print(f"✓ Feature engineering complete: {len(data)} records processed")
```

### Method 2: Programmatic Control

Configure logging directly in your code:

```python
from utils.loguru_setup import logger

# Set log level before importing DSM components
logger.configure_level("CRITICAL")

# Now import and use DSM with suppressed logging
from core.sync.data_source_manager import DataSourceManager
```

### Method 3: Global Configuration

Set a global logging policy for your entire application:

```python
import os

# Configure at application startup
os.environ["DSM_LOG_LEVEL"] = "CRITICAL"

# All subsequent DSM usage will respect this setting
from core.sync.data_source_manager import DataSourceManager
```

## Logging Level Reference

| Level      | What You See                                                | Use Case                                         |
| ---------- | ----------------------------------------------------------- | ------------------------------------------------ |
| `CRITICAL` | Only critical errors (connection failures, data corruption) | **Feature engineering workflows** - clean output |
| `ERROR`    | Errors that don't stop execution + critical (**default**)   | **Production monitoring and normal usage**       |
| `WARNING`  | Data quality warnings, cache misses + errors                | Development with some visibility                 |
| `INFO`     | Basic operation info + warnings                             | Detailed development and debugging               |
| `DEBUG`    | Detailed debugging info + all above                         | Deep debugging and troubleshooting               |

## Affected Components

The logging control applies to all DSM components:

- **Core DSM** (`core.sync.data_source_manager`)
- **Cache utilities** (`utils.for_core.dsm_cache_utils`)
- **FCP utilities** (`utils.for_core.dsm_fcp_utils`)
- **DataFrame utilities** (`utils.dataframe_utils`)
- **API utilities** (`utils.for_core.dsm_api_utils`)
- **All other DSM-related modules**

## Before and After Comparison

### Before (Cluttered Output)

```python
# Required 15+ lines of boilerplate in every file
import logging
from loguru import logger as loguru_logger

# Suppress ALL logging including DSM cache and FCP logs
logging.getLogger().setLevel(logging.CRITICAL)
logging.getLogger("core").setLevel(logging.CRITICAL)
logging.getLogger("core.sync").setLevel(logging.CRITICAL)
logging.getLogger("core.sync.data_source_manager").setLevel(logging.CRITICAL)
logging.getLogger("utils").setLevel(logging.CRITICAL)
logging.getLogger("utils.for_core").setLevel(logging.CRITICAL)
logging.getLogger("utils.for_core.dsm_cache_utils").setLevel(logging.CRITICAL)
logging.getLogger("utils.for_core.dsm_fcp_utils").setLevel(logging.CRITICAL)
logging.getLogger("utils.dataframe_utils").setLevel(logging.CRITICAL)

# Suppress loguru logs from DSM
loguru_logger.remove()
loguru_logger.add(lambda _: None)

# Finally use DSM
from core.sync.data_source_manager import DataSourceManager
```

### After (Clean Solution)

```python
# Clean, simple feature engineering code
import os
os.environ["DSM_LOG_LEVEL"] = "CRITICAL"

from core.sync.data_source_manager import DataSourceManager
from utils.market_constraints import DataProvider, Interval, MarketType

# No more logging boilerplate needed!
dsm = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
data = dsm.get_data(
    symbol="SOLUSDT",
    start_time=start_time,
    end_time=end_time,
    interval=Interval.SECOND_1,
)
# Clean output - only your feature engineering logs visible
```

## Advanced Configuration

### File Logging

Enable file logging with automatic rotation:

```bash
export DSM_LOG_FILE="./logs/dsm.log"
export DSM_LOG_LEVEL="DEBUG"
```

### Disable Colors

For environments that don't support colored output:

```bash
export DSM_DISABLE_COLORS="true"
```

### Session-Specific Logging

For temporary debugging sessions:

```python
from utils.loguru_setup import configure_session_logging

# Configure timestamped log files
main_log, error_log, timestamp = configure_session_logging("my_session", "DEBUG")
print(f"Logs: {main_log} and {error_log}")
```

## Demo and Testing

Try the interactive demo to see logging control in action:

```bash
# Basic demo
python examples/dsm_logging_demo.py

# Test different log levels
python examples/dsm_logging_demo.py --log-level CRITICAL --test-dsm
python examples/dsm_logging_demo.py --log-level DEBUG --test-dsm

# Show all features
python examples/dsm_logging_demo.py --show-all

# Environment variable control
DSM_LOG_LEVEL=CRITICAL python examples/dsm_logging_demo.py --test-dsm
```

## Migration Guide

### From Old Logging Suppression

If you have existing code with logging suppression boilerplate:

1. **Remove all logging suppression code** (15+ lines of `logging.getLogger().setLevel()` calls)
2. **Add single environment variable** at the top of your file:
   ```python
   import os
   os.environ["DSM_LOG_LEVEL"] = "CRITICAL"
   ```
3. **Import DSM normally** - no other changes needed

### Gradual Migration

You can migrate gradually:

1. **Start with one component** - add `DSM_LOG_LEVEL=CRITICAL` to one feature engineering script
2. **Verify clean output** - ensure only your application logs appear
3. **Apply to other components** - use the same pattern in other scripts
4. **Remove old boilerplate** - clean up the old logging suppression code

## Best Practices

### For Feature Engineering

```python
# At the top of feature engineering scripts
import os
os.environ["DSM_LOG_LEVEL"] = "CRITICAL"

# Your feature engineering code with clean output
```

### For Development

```python
# Use INFO level for normal development
import os
os.environ["DSM_LOG_LEVEL"] = "INFO"

# Or use DEBUG for troubleshooting
# os.environ["DSM_LOG_LEVEL"] = "DEBUG"
```

### For Production

```bash
# Set in your deployment environment
export DSM_LOG_LEVEL=ERROR

# Or in your application configuration
DSM_LOG_LEVEL=WARNING
```

## Troubleshooting

### Logging Not Suppressed

If DSM logs still appear after setting `DSM_LOG_LEVEL=CRITICAL`:

1. **Check environment variable**: `echo $DSM_LOG_LEVEL`
2. **Set before import**: Ensure you set the environment variable before importing DSM
3. **Verify effective level**:
   ```python
   from utils.loguru_setup import logger
   print(f"Effective level: {logger.getEffectiveLevel()}")
   ```

### Need Temporary Debug Output

To temporarily enable debug output without changing code:

```bash
# Override environment variable for one run
DSM_LOG_LEVEL=DEBUG python your_script.py
```

### Mixed Logging Systems

If your application uses different logging systems:

```python
# Configure DSM logging
import os
os.environ["DSM_LOG_LEVEL"] = "CRITICAL"

# Configure your application logging separately
import logging
logging.basicConfig(level=logging.INFO)

# Both systems work independently
```

## Implementation Details

### Technical Implementation

- **Centralized Logger**: All DSM components use `utils.loguru_setup.logger`
- **Environment Variable**: `DSM_LOG_LEVEL` is checked at import time
- **Cleaner Default**: Default level is now `ERROR` for quieter operation by default
- **Performance**: Loguru provides better performance than standard logging
- **Thread Safe**: All logging operations are thread-safe

### Supported Environment Variables

| Variable             | Purpose                | Default | Example                                         |
| -------------------- | ---------------------- | ------- | ----------------------------------------------- |
| `DSM_LOG_LEVEL`      | Set log level          | `ERROR` | `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG` |
| `DSM_LOG_FILE`       | Enable file logging    | None    | `./logs/dsm.log`                                |
| `DSM_DISABLE_COLORS` | Disable colored output | `false` | `true`, `false`                                 |

## Benefits Summary

✅ **No Boilerplate**: Eliminates 15+ lines of logging suppression code  
✅ **Clean Output**: Professional console output for feature engineering  
✅ **Easy Control**: Single environment variable controls all DSM logging  
✅ **Backward Compatible**: Existing code works unchanged  
✅ **Configurable**: Different log levels for different use cases  
✅ **Performance**: Better performance with loguru  
✅ **Professional**: Clean, focused output for production workflows

The DSM logging control solution provides exactly what was requested: simple, effective control over DSM logging output without requiring any changes to existing DSM usage patterns.
