# Loguru Logger Usage

The DSM package uses loguru for logging. This provides better log level control and improved logging experience.

## Why Loguru?

Loguru provides:

- **Simple log level control**: Just set `DSM_LOG_LEVEL=DEBUG`
- **Better performance**: More efficient than Python's standard logging
- **Automatic log rotation**: Built-in file rotation and compression
- **Rich formatting**: Beautiful colored output with module/function info
- **Easy configuration**: Environment variables or simple API calls

## Basic Usage

```python
from data_source_manager.utils.loguru_setup import logger

# Standard logging calls
logger.debug("Debug message")
logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
logger.critical("Critical message")
```

## Configuration

### Environment Variables

The easiest way to control logging:

```bash
# Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
export DSM_LOG_LEVEL=DEBUG

# Optional: Log to file with automatic rotation
export DSM_LOG_FILE=./logs/dsm.log

# Optional: Disable colors
export DSM_DISABLE_COLORS=true
```

### Programmatic Configuration

```python
from data_source_manager.utils.loguru_setup import logger

# Set log level
logger.configure_level("DEBUG")

# Enable file logging with rotation
logger.configure_file("./logs/dsm.log")

# Disable colors
logger.disable_colors(True)

# Method chaining works
logger.configure_level("INFO").configure_file("./logs/app.log")
```

## Examples

### Basic Usage

```python
from data_source_manager.utils.loguru_setup import logger

# Set log level for your session
logger.configure_level("DEBUG")

# Use rich formatting
logger.info("Status: <green>SUCCESS</green>")
logger.warning("Warning: <yellow>CAUTION</yellow>")
logger.error("Error: <red>FAILED</red>")

# All standard methods work
logger.debug("Detailed debug info")
logger.exception("Error with traceback")
```

### Environment-based Configuration

```bash
# In your shell or .env file
export DSM_LOG_LEVEL=INFO
export DSM_LOG_FILE=./logs/dsm.log

# Now just import and use
python -c "
from data_source_manager.utils.loguru_setup import logger
logger.info('Configured from environment!')
"
```

### Advanced Features

```python
from data_source_manager.utils.loguru_setup import logger

# Bind context to all log messages
contextual_logger = logger.bind(user_id=123, session="abc")
contextual_logger.info("User action")  # Includes user_id and session

# Use loguru's advanced options
logger.opt(colors=True).info("Force <red>colored</red> output")
```

## Testing the Migration

After migration, test your logging:

```python
from data_source_manager.utils.loguru_setup import logger

# Test different levels
logger.configure_level("DEBUG")
logger.debug("This should appear")

logger.configure_level("ERROR")
logger.debug("This should NOT appear")
logger.error("This should appear")

# Test rich formatting
logger.info("Status: <green>All good!</green>")
```

## Troubleshooting

### Import Errors

If you get import errors, ensure loguru is installed:

```bash
pip install loguru>=0.7.3
```

### Log Level Not Working

Make sure you're setting the level correctly:

```python
# Correct
logger.configure_level("DEBUG")

# Also correct
import os
os.environ["DSM_LOG_LEVEL"] = "DEBUG"
```

### Colors Not Showing

Check your terminal supports colors and:

```python
# Force enable colors
logger.disable_colors(False)

# Or via environment
export DSM_DISABLE_COLORS=false
```

## Rollback

If you need to rollback, restore from the backup files:

```bash
# Find backup files
find . -name "*.py.backup"

# Restore a specific file
mv core/sync/data_source_manager.py.backup core/sync/data_source_manager.py

# Or restore all files
find . -name "*.py.backup" -exec sh -c 'mv "$1" "${1%.backup}"' _ {} \;
```

## Benefits After Migration

- **Easier log control**: `export DSM_LOG_LEVEL=DEBUG` vs complex logging configuration
- **Better performance**: Loguru is faster than standard logging
- **Automatic file rotation**: No more manual log file management
- **Rich output**: Beautiful colored logs with module/function info
- **Same API**: All existing logging calls work unchanged

The migration preserves all existing functionality while providing much better control and user experience!
