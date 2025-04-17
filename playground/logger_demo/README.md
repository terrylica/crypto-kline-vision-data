# Logger Demo

This directory contains a demonstration of the smart print functionality and module-specific logging provided by the `utils/logger_setup.py` module.

## Smart Print Demo

The `simple_print_demo.py` script showcases how to use the logger's smart print functionality, which enhances Python's built-in `print()` function with:

1. Rich formatting capabilities (colors, styles, tables, etc.)
2. Log level awareness (output suppression at ERROR and CRITICAL levels)
3. Proper rendering of rich objects (tables, panels, progress bars)

### Running the Demo

```bash
# Run with INFO level (shows all print statements)
python examples/logger_demo/simple_print_demo.py --level INFO

# Run with ERROR level (hides non-essential print statements)
python examples/logger_demo/simple_print_demo.py --level ERROR
```

### Key Features

- Single import approach: `from utils.logger_setup import logger`
- Enhanced print statements with rich formatting
- Automatic suppression of visual output at higher log levels
- Consistent interface for both logging and console output
- Automatic module detection for proper source identification in logs
- Method chaining support for cleaner, more readable code
- Hierarchical control allowing parent scripts to control logging in imported modules

## Hierarchical Control Example

The logger system allows a parent script to control the logging behavior of all imported modules:

```python
# In your main.py (run this first)
from utils.logger_setup import logger

# Set global log level and enable smart print before importing modules
logger.setLevel("ERROR")
logger.enable_smart_print(True)

# Now import other modules
import module_a
import module_b

# All print statements from module_a and module_b will automatically
# respect the ERROR log level (they will be suppressed)
```

This hierarchical control extends to both regular logging calls and smart print statements across your entire application.

## Documentation

For comprehensive documentation on the logger setup and smart print functionality, please refer to:

[Logger Setup Documentation](../../docs/core_architecture/logger_setup.md)

This documentation includes:

- Detailed explanation of all features including smart print and proxying
- Code examples for various use cases
- Best practices for integrating with your application
- Architecture notes on the implementation details
