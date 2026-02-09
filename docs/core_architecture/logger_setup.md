# Loguru Setup - Centralized Logging for Crypto Kline Vision Data

The `src/ckvd/utils/loguru_setup.py` module provides a simple, powerful loguru-based logging system that addresses user complaints about log level control in the CKVD package. It features environment variable configuration, automatic log rotation, and rich formatting support.

## Key Features

- **Simple Configuration**: Control log level via environment variable or API
- **Better Performance**: Loguru is faster than Python's standard logging
- **Auto Rotation**: Built-in log file rotation and compression
- **Rich Formatting**: Colored output with module/function info
- **Method Chaining**: Fluent API for configuration
- **Drop-in Replacement**: Same API as the old logger

## Basic Usage

```python
from ckvd.utils.loguru_setup import logger

# Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
logger.configure_level("INFO")

# All standard logging calls work
logger.debug("Debug message")   # Only shown at DEBUG level
logger.info("Info message")     # Shown at INFO level and below
logger.warning("Warning")       # Shown at WARNING level and below
logger.error("Error")           # Always shown
logger.critical("Critical")     # Always shown

# Rich formatting with colors
logger.info("Status: <green>SUCCESS</green>")
logger.error("Error: <red>FAILED</red>")
```

## Environment Variable Control

The easiest way to control logging is via environment variables:

```bash
# Suppress CKVD logs for clean output (feature engineering workflows)
export CKVD_LOG_LEVEL=CRITICAL

# Normal development with info-level logging
export CKVD_LOG_LEVEL=INFO

# Detailed debugging
export CKVD_LOG_LEVEL=DEBUG

# Optional: Log to file with automatic rotation
export CKVD_LOG_FILE=./logs/ckvd.log

# Optional: Disable colored output
export CKVD_DISABLE_COLORS=true

# Run your application
python your_script.py
```

### Default Behavior

When no environment variable is set, the default log level is `ERROR`. This provides quieter operation by default, showing only errors and critical messages.

## CKVD Logging Suppression for Feature Engineering

**Problem**: CKVD produces extensive logging that clutters console output during feature engineering workflows.

**Solution**: Use `CKVD_LOG_LEVEL=CRITICAL` to suppress all non-critical CKVD logs:

```python
# Clean feature engineering code - no boilerplate needed!
import os
os.environ["CKVD_LOG_LEVEL"] = "CRITICAL"

from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval

# Create CKVD instance - minimal logging
ckvd = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)

# Fetch data - clean output, only your logs visible
data = ckvd.get_data(
    symbol="SOLUSDT",
    start_time=start_time,
    end_time=end_time,
    interval=Interval.MINUTE_1,
)
# Clean output - no more cluttered CKVD logs!
```

**Benefits**:

- **No Boilerplate**: Eliminates 15+ lines of logging suppression code
- **Clean Output**: Professional console output for feature engineering
- **Easy Control**: Single environment variable controls all CKVD logging
- **Cleaner Default**: Default ERROR level provides quieter operation

## Programmatic Configuration

For more control, use the programmatic API:

```python
from ckvd.utils.loguru_setup import logger

# Set log level
logger.configure_level("DEBUG")

# Enable file logging with automatic rotation
logger.configure_file("./logs/ckvd.log")

# Disable colors (useful for CI/CD or file output)
logger.disable_colors(True)

# Method chaining
logger.configure_level("INFO").configure_file("./logs/app.log").info("Logging configured")
```

### Configuration Methods

| Method                | Description                   | Returns         |
| --------------------- | ----------------------------- | --------------- |
| `configure_level()`   | Set log level                 | Self (chaining) |
| `configure_file()`    | Set log file path             | Self (chaining) |
| `disable_colors()`    | Enable/disable colored output | Self (chaining) |
| `setLevel()`          | Compatibility with old logger | Self (chaining) |
| `getEffectiveLevel()` | Get current log level         | str             |
| `isEnabledFor()`      | Check if level is enabled     | bool            |

## Session Logging

For session-specific logging with timestamped files:

```python
from ckvd.utils.loguru_setup import configure_session_logging

# Creates timestamped log files for a session
main_log, error_log, timestamp = configure_session_logging(
    session_name="data_fetch",
    log_level="DEBUG"
)

# Logs are written to:
# - logs/data_fetch_logs/data_fetch_20240115_143022.log (all messages)
# - logs/error_logs/data_fetch_errors_20240115_143022.log (ERROR/CRITICAL only)
```

## Log File Features

When file logging is enabled:

- **Automatic Rotation**: Files rotate at 10 MB
- **Retention**: Logs kept for 1 week
- **Compression**: Rotated logs are compressed to `.zip`
- **Error Separation**: Session logging creates separate error-only files

## Rich Formatting

Loguru supports inline markup for colored output:

```python
# Color tags
logger.info("Status: <green>SUCCESS</green>")
logger.warning("Warning: <yellow>CAUTION</yellow>")
logger.error("Error: <red>FAILED</red>")

# Style tags
logger.info("<bold>Bold text</bold>")
logger.info("<italic>Italic text</italic>")
logger.info("<underline>Underlined</underline>")

# Combinations
logger.info("<bold><green>Success!</green></bold>")
```

## Advanced Features

### Context Binding

Add context to log messages:

```python
# Bind context for subsequent messages
context_logger = logger.bind(symbol="BTCUSDT", market="FUTURES_USDT")
context_logger.info("Fetching data")  # Includes bound context
```

### Exception Logging

Log exceptions with full traceback:

```python
try:
    risky_operation()
except Exception:
    logger.exception("Operation failed")
    # Automatically includes traceback
```

### Logger Options

Fine-tune individual log calls:

```python
# Skip depth for accurate caller info
logger.opt(depth=1).info("Message from wrapper")

# Lazy evaluation for expensive operations
logger.opt(lazy=True).debug("{}", expensive_function)
```

## Log Format

The default log format includes:

```
2024-01-15 14:30:22.123 | INFO     | module:function:42 - Message
```

Components:

- **Timestamp**: `YYYY-MM-DD HH:mm:ss.SSS`
- **Level**: Padded to 8 characters
- **Location**: `module:function:line`
- **Message**: The log message

## Example Program

```python
#!/usr/bin/env python3
import argparse
from ckvd.utils.loguru_setup import logger

def main():
    parser = argparse.ArgumentParser(description="Demo")
    parser.add_argument(
        "--level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO)"
    )
    args = parser.parse_args()

    # Configure logging
    logger.configure_level(args.level)

    # Log messages at different levels
    logger.debug("Detailed debug information")
    logger.info(f"Log level set to: {args.level}")
    logger.warning("This is a warning")
    logger.error("This error message is always visible")
    logger.critical("Critical system failure")

    # Rich formatting
    logger.info("Status: <green>SUCCESS</green>")
    logger.info("Progress: <cyan>50%</cyan> complete")

    # Method chaining
    logger.configure_level("DEBUG").debug("Now showing debug messages")

if __name__ == "__main__":
    main()
```

## Running the Demo

```bash
# Run the CKVD logging demo
uv run -p 3.13 python examples/ckvd_logging_demo.py

# Run with different log levels via environment variable
CKVD_LOG_LEVEL=DEBUG uv run -p 3.13 python examples/ckvd_logging_demo.py
CKVD_LOG_LEVEL=CRITICAL uv run -p 3.13 python examples/ckvd_logging_demo.py

# Clean feature engineering example (suppressed logging)
uv run -p 3.13 python examples/clean_feature_engineering_example.py
```

## Best Practices

1. **Use Environment Variables**: Set `CKVD_LOG_LEVEL` for easy control without code changes
2. **Default to ERROR**: Keep production quiet by default
3. **Use DEBUG for Development**: Enable verbose logging during development
4. **Rich Formatting for Readability**: Use color tags for important status messages
5. **File Logging for Production**: Enable file logging for persistent records
6. **Method Chaining**: Configure logger fluently for cleaner code

## Architecture Notes

- Built on [loguru](https://github.com/Delgan/loguru), a modern Python logging library
- Global singleton pattern via `CKVDLogger` class
- Thread-safe by design (loguru handles thread safety)
- Automatic exception formatting with backtraces
- Lazy message formatting for performance

## Related

- [Migration Guide](../howto/loguru_migration.md) - Detailed migration instructions
- [README.md](/README.md#logging-control) - Overview in main README
- [examples/ckvd_logging_demo.py](/examples/ckvd_logging_demo.py) - CKVD logging demo
- [examples/clean_feature_engineering_example.py](/examples/clean_feature_engineering_example.py) - Clean logging for feature engineering
