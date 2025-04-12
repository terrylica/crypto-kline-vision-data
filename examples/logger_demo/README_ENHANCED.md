# Enhanced Logging System

The `logger_setup.py` module provides a comprehensive logging system that eliminates the need to directly import the standard Python `logging` module. This document explains how to use the enhanced features.

## Basic Usage

```python
# Import the logger at the top of your Python file
from utils.logger_setup import logger
from rich import print  # Optional, for enhanced console output

# Now you can use the logger in your code
logger.info("This is an information message")
logger.warning("This is a warning message")
logger.error("This is an error message")
```

## Setting Up the Root Logger

```python
# Configure the root logger at the beginning of your application
logger.setup_root(level="DEBUG")  # Configure with desired log level
logger.show_filename(True)  # Show file and line information
logger.use_rich(True)  # Use rich formatting (if available)
```

## Adding File Handlers Without Importing 'logging'

Instead of:

```python
import logging
file_handler = logging.FileHandler("my_log_file.log")
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)
```

You can now simply use:

```python
# Add a file handler with a single call
logger.add_file_handler(
    "my_log_file.log",
    level="DEBUG",
    mode="w",  # 'w' to overwrite, 'a' to append
    formatter_pattern="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
```

## Accessing Standard Logging Levels

Instead of:

```python
import logging
if level >= logging.ERROR:
    # Do something
```

You can use:

```python
if level >= logger.ERROR:
    # Do something
```

Available level constants:

- `logger.DEBUG`
- `logger.INFO`
- `logger.WARNING`
- `logger.ERROR`
- `logger.CRITICAL`

## Method Chaining for Concise Logging

The logger supports method chaining for concise logging operations:

```python
# Configure and log in a single chain
logger.setup_root(level="DEBUG").show_filename(True).info("Application starting").debug("Debug details")

# Add a file handler and continue logging
logger.add_file_handler("debug.log").info("Logging to file enabled")
```

## Error Logging

```python
# Enable dedicated error logging to a separate file
logger.enable_error_logging("error_log.txt")

# Now all ERROR and CRITICAL messages go to both the console and the error log file
logger.error("This error is captured in the error log file")
```

## Advanced Features

```python
# Get a standard logger instance for a specific name
custom_logger = logger.get_logger("custom_component")

# Create a custom formatter
formatter = logger.create_formatter("%(asctime)s - %(message)s")
```

## Best Practices

1. Import only `logger` from `utils.logger_setup` at the top of each file
2. Configure the root logger only once at the application entry point
3. Use method chaining for concise setup and logging
4. Prefer the logger's built-in methods over importing the standard logging module
5. Use appropriate logging levels for different message types

## Example Files

- `main_example_enhanced.py`: Demonstrates all the enhanced logging features
- `main_example_debug.py`: Shows standard logger usage with DEBUG level
- `main_example_info.py`: Shows standard logger usage with INFO level
- `test_modules.py`: Contains helper functions that use the logger

## Comparing with Standard Approach

The standard approach (as seen in `main_example_debug.py`) requires:

- Importing the logger
- Configuring the logger with `setLevel()`
- Setting up file handlers separately (requires importing `logging` when needed)

The enhanced approach (as seen in `main_example_enhanced.py`) offers:

- Method chaining for clean, concise configuration
- Built-in helper methods to avoid importing the standard logging module
- Integrated file handler setup with a single method call
- Access to logging levels as constants on the logger object

Run both examples to see the difference:

```bash
python examples/logger_demo/main_example_debug.py
python examples/logger_demo/main_example_enhanced.py
```
