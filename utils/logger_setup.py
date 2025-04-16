#!/usr/bin/env python3
"""
Advanced Logging System for Binance Data Services

This module provides a sophisticated logging system that extends Python's standard
logging facilities with additional features:

Key Features:
- Automatic module name detection in log messages
- Source file and line tracking for accurate debugging
- Method chaining support for cleaner code
- Rich-powered logging for improved readability in terminals
- Dedicated timeout and error logging subsystems
- Smart print functionality that respects log levels

The central component is the `logger` object which serves as a drop-in replacement
for the standard logging module with enhanced capabilities.

Basic Usage Examples:
    # Import the logger
    from utils.logger_setup import logger

    # Basic logging with auto-detected module name
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")

    # Chainable logging API
    logger.info("First message").debug("Second message")

    # Rich formatting (if available)
    logger.info("Status: [bold green]SUCCESS[/bold green]")

Advanced Usage:
    # Configure log level
    logger.setLevel(logger.DEBUG)  # Or using string: "DEBUG"

    # Switch between rich and standard logging
    logger.use_rich(True)

    # Log timeouts to a dedicated file
    log_timeout("database_query", 5.0, details={"query_id": "abc123"})

    # Enable error logging to a separate file
    enable_error_logging("/path/to/errors.log")

    # Make print statements respect log levels
    enable_smart_print()
"""

import logging, os
import inspect
from colorlog import ColoredFormatter
import traceback
from pathlib import Path
import builtins
import sys
import pendulum
import time

try:
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.traceback import install as install_rich_traceback

    RICH_AVAILABLE = True
    # Install rich traceback handling
    install_rich_traceback(show_locals=True)
except ImportError:
    RICH_AVAILABLE = False

# Default log level from environment or INFO
DEFAULT_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Global state tracking
_root_configured = False
_module_loggers = {}
_use_rich = os.environ.get("USE_RICH_LOGGING", "true").lower() in ("true", "1", "yes")
_show_filename = True  # Always show filename

# Timeout logging
_timeout_log_file = os.environ.get(
    "TIMEOUT_LOG_FILE", "./logs/timeout_incidents/timeout_log.txt"
)
_timeout_logger_configured = False
_timeout_logger = None

# Error logging
_error_log_file = os.environ.get("ERROR_LOG_FILE", "./logs/error_logs/error_log.txt")
_error_logger_configured = False
_error_logger = None
_error_logging_enabled = False

# Default color scheme
DEFAULT_LOG_COLORS = {
    "DEBUG": "cyan",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red,bg_white",
}

# Format strings
# Base format without filename
FORMAT_BASE = "%(log_color)s%(levelname)-8s%(reset)s %(name)s: %(message)s"

# Format with custom file/line information from our proxy
CUSTOM_FORMAT_WITH_FILENAME = "%(log_color)s%(levelname)-8s%(reset)s %(name)s: %(message)s%(blue)s [%(cyan)s%(source_file)s%(blue)s:%(yellow)s%(source_line)s%(blue)s]%(reset)s"

# Current format
FORMAT = CUSTOM_FORMAT_WITH_FILENAME

# Rich format (Rich handles the styling)
RICH_FORMAT = "%(message)s"

# Rich console instance
if RICH_AVAILABLE:
    console = Console(highlight=False)  # Disable syntax highlighting to preserve markup

# Rich console instance for the module
_console = None


# Custom logger that properly tracks caller information
class CustomLogger(logging.Logger):
    """
    Custom logger class that correctly identifies the actual source of log messages.

    This class overrides the standard findCaller method to properly identify where
    log messages are actually coming from in the application code, rather than showing
    the logging infrastructure's files (like logger_setup.py) as the source.

    This is especially important when:
    1. Using the show_filename feature to display source file information
    2. Working with proxy loggers or wrapper functions
    3. Building logging frameworks that should be "invisible" to the end user

    The findCaller method skips through frames in the logging module and this module
    to find the actual application code that initiated the logging call.
    """

    def findCaller(self, stack_info=False, stacklevel=1):
        """
        Find the stack frame of the caller.

        This customizes the stack level search to skip past the LoggerProxy class
        and standard logging infrastructure to find the actual application caller.
        This ensures that when filename display is enabled, the correct source file
        is shown rather than the logging infrastructure files.

        Args:
            stack_info (bool): If True, collect stack trace information
            stacklevel (int): How many frames to skip

        Returns:
            tuple: (filename, line number, function name, stack info)
        """
        # Get current stack frames
        try:
            # Get the stack frames
            frame_records = inspect.stack()

            # Skip frames based on stacklevel (default is 1 in logging)
            # We need to skip more frames to account for the logger proxy and other infrastructure
            adjusted_level = stacklevel + 4  # Skip extra frames for our infrastructure

            if len(frame_records) <= adjusted_level:
                return "(unknown file)", 0, "(unknown function)", None

            # Get the relevant frame based on adjusted stacklevel
            caller_frame = frame_records[adjusted_level]

            # Extract file, line, and function information
            fn = caller_frame.filename
            lno = caller_frame.lineno
            func = caller_frame.function

            # Get stack info if requested
            sinfo = None
            if stack_info:
                sinfo = "".join(
                    traceback.format_stack(frame_records[adjusted_level][0])
                )

            return fn, lno, func, sinfo

        except Exception:
            return "(unknown file)", 0, "(unknown function)", None


# Register our custom logger class
logging.setLoggerClass(CustomLogger)


def _get_module_logger(name=None, level=None, setup_root=False, use_rich=None):
    """
    INTERNAL USE ONLY: Retrieve or create a logger instance with appropriate configuration.

    This function is for internal use by the LoggerProxy only.
    External code should use the 'logger' directly.
    """
    # Auto-detect caller's module name if not provided
    if name is None:
        frame = (
            inspect.currentframe().f_back.f_back
        )  # Extra level for _get_module_logger caller
        module = inspect.getmodule(frame)
        name = module.__name__ if module else "__main__"

    # Configure root logger if requested
    if setup_root:
        if use_rich is None:
            use_rich = _use_rich
        _setup_root_logger(level, use_rich=use_rich)

    # Return cached logger if already created for this module
    if name in _module_loggers:
        return _module_loggers[name]

    logger_instance = logging.getLogger(name)

    # If not using as a module within a configured app,
    # set up minimal logging for this module
    if not _root_configured and not logger_instance.handlers:
        log_level = (level or DEFAULT_LEVEL).upper()
        logger_instance.setLevel(log_level)
    elif level:
        # Allow level override when explicitly requested
        logger_instance.setLevel(level.upper())

    # Cache the logger
    _module_loggers[name] = logger_instance

    return logger_instance


def _setup_root_logger(level=None, use_rich=None):
    """
    Configure the root logger with specified level and colorized output.
    """
    global _root_configured, _use_rich, FORMAT

    # Determine whether to use rich
    if use_rich is None:
        use_rich = _use_rich
    else:
        _use_rich = use_rich

    root_logger = logging.getLogger()
    root_logger.handlers.clear()  # Clear existing handlers

    # Set level
    log_level = (level or DEFAULT_LEVEL).upper()
    level_int = getattr(logging, log_level)
    root_logger.setLevel(level_int)

    # Set up handler based on preference and availability
    if use_rich and RICH_AVAILABLE:
        # Use Rich handler with explicit filename display
        handler = RichHandler(
            console=console,
            rich_tracebacks=True,
            markup=True,  # Enable markup
            show_time=False,
            show_path=False,  # We'll handle file path display ourselves
            enable_link_path=True,
            highlighter=None,  # Disable syntax highlighting to preserve rich markup
        )

        # For Rich, we'll directly modify the message in the LoggerProxy
        formatter = logging.Formatter("%(message)s")
    else:
        # Use colorlog handler (fallback or default)
        handler = logging.StreamHandler()
        formatter = ColoredFormatter(
            FORMAT,
            log_colors=DEFAULT_LOG_COLORS,
            secondary_log_colors={
                "filename": {
                    "DEBUG": "cyan",
                    "INFO": "cyan",
                    "WARNING": "cyan",
                    "ERROR": "cyan",
                    "CRITICAL": "cyan",
                },
                "lineno": {
                    "DEBUG": "yellow",
                    "INFO": "yellow",
                    "WARNING": "yellow",
                    "ERROR": "yellow",
                    "CRITICAL": "yellow",
                },
            },
            style="%",
            reset=True,
        )

    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    # Update existing loggers
    for name in logging.root.manager.loggerDict:
        logging.getLogger(name).setLevel(level_int)

    _root_configured = True
    return root_logger


def use_rich_logging(enable=True, level=None):
    """
    Enable or disable Rich logging.

    Parameters:
        enable (bool): True to enable Rich logging, False to use standard colorlog.
        level (str, optional): Logging level to set when reconfiguring.

    Returns:
        bool: True if Rich logging was enabled, False otherwise.
    """
    global _use_rich

    if enable and not RICH_AVAILABLE:
        tmp_logger = _get_module_logger()
        tmp_logger.warning(
            "Rich library not available. Install with 'pip install rich'"
        )
        return False

    _use_rich = enable
    _setup_root_logger(level=level, use_rich=enable)
    return enable


# Create a proxy logger object that automatically detects the calling module
class LoggerProxy:
    """
    Proxy implementation providing automatic module detection and method chaining.
    """

    def __init__(self):
        """Initialize the LoggerProxy with root logger configuration"""
        # Configure root logger with default settings if not already configured
        if not _root_configured:
            _setup_root_logger(level=DEFAULT_LEVEL, use_rich=_use_rich)

        # Initialize console (lazy loading)
        self._console = None

    def __getattr__(self, name):
        """
        Dynamic attribute resolution with runtime module detection.
        """
        # Handle standard logging methods
        if name in (
            "debug",
            "info",
            "warning",
            "warn",
            "error",
            "critical",
            "exception",
        ):
            # Get caller's module and source information
            frame = inspect.currentframe().f_back
            module = inspect.getmodule(frame)
            module_name = module.__name__ if module else "__main__"

            # Get exact caller information
            file_path = frame.f_code.co_filename
            line_no = frame.f_lineno

            # Get the appropriate logger for the module
            logger_instance = _get_module_logger(module_name)

            # Create a wrapper with explicit source location handling
            def log_wrapper(*args, **kwargs):
                # If extra is not provided, create it
                if "extra" not in kwargs:
                    kwargs["extra"] = {}

                # Add specific file and line information to make it available to formatters
                source_file = os.path.basename(file_path)
                kwargs["extra"]["source_file"] = source_file
                kwargs["extra"]["source_line"] = line_no

                # For Rich logging, append file/line info directly to the message
                if _use_rich and RICH_AVAILABLE and args:
                    # Format for the appended file info - match the style of standard logger
                    file_info = f" [blue][[cyan]{source_file}[/cyan]:[yellow]{line_no}[/yellow][blue]][/blue]"

                    # Make sure args is mutable (convert from tuple to list)
                    args_list = list(args)

                    # Append to the first argument (the message) if it's a string
                    # Be careful not to break existing rich markup
                    if isinstance(args_list[0], str):
                        # Preserve rich markup by adding file info at the end without modifying existing markup
                        args_list[0] = f"{args_list[0]}{file_info}"
                        args = tuple(args_list)

                # Don't use stacklevel as we're directly providing file/line info
                if "stacklevel" in kwargs:
                    del kwargs["stacklevel"]

                # Call the actual log method
                log_method = getattr(logger_instance, name)
                log_method(*args, **kwargs)

                # Return self for method chaining
                return self

            return log_wrapper

        # For any other attributes, use this module's logger
        tmp_logger = _get_module_logger()
        return getattr(tmp_logger, name)

    def setup_root(self, level=None, use_rich=None):
        """
        Configure the root logger with specified options.

        Parameters:
            level (str, optional): Logging level to set
            use_rich (bool, optional): Whether to use Rich logging

        Returns:
            LoggerProxy: Self reference for method chaining
        """
        _setup_root_logger(level=level, use_rich=use_rich)
        return self

    def setLevel(self, level, configure_root=True):
        """
        Set the logging level with method chaining support.

        Parameters:
            level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            configure_root (bool): Whether to also configure the root logger

        Returns:
            LoggerProxy: Self reference for method chaining support.
        """
        # Allow string or int levels
        level_str = level if isinstance(level, str) else logging._levelToName.get(level)

        # Check if the level is valid
        if not hasattr(logging, level_str):
            valid_levels = list(logging._levelToName.values())
            raise ValueError(
                f"Invalid log level: {level_str}. Valid levels are: {valid_levels}"
            )

        # Set up root logger if requested (usually first setup)
        if configure_root:
            _setup_root_logger(level=level_str, use_rich=_use_rich)

        # Get corresponding module
        frame = inspect.currentframe()
        module = inspect.getmodule(frame.f_back) if frame else None
        module_name = module.__name__ if module else "__main__"

        # Set level for the specific module logger
        module_logger = _get_module_logger(module_name)
        module_logger.setLevel(level_str)

        # Return self for chaining
        return self

    def use_rich(self, enable=True, level=None):
        """
        Enable or disable Rich logging with method chaining support.

        Parameters:
            enable (bool): True to enable Rich logging, False to use standard colorlog.
            level (str, optional): Logging level to set when reconfiguring.

        Returns:
            LoggerProxy: Self reference for method chaining support.
        """
        use_rich_logging(enable=enable, level=level)
        return self

    def log_timeout(self, operation, timeout_value, details=None):
        """
        Log a timeout event to the centralized timeout log.

        This method logs timeout events both to the standard logger and to a dedicated
        timeout log file for easier analysis of performance issues.

        Parameters:
            operation (str): Description of the operation that timed out
            timeout_value (float): The timeout value in seconds that was breached
            details (dict, optional): Additional details about the operation

        Returns:
            LoggerProxy: Self reference for method chaining support.
        """
        # Get caller's module
        frame = inspect.currentframe().f_back
        module = inspect.getmodule(frame)
        module_name = module.__name__ if module else "__main__"

        # Use the global log_timeout function
        log_timeout(operation, timeout_value, module_name, details)

        # Return self for chaining
        return self

    def set_timeout_log_file(self, path):
        """
        Set the file path for timeout logging with method chaining support.

        Parameters:
            path (str): Path to the log file

        Returns:
            LoggerProxy: Self reference for method chaining support.
        """
        set_timeout_log_file(path)
        return self

    def enable_error_logging(self, error_log_file=None):
        """Enable logging of all errors, warnings, and critical messages to a dedicated file.

        Args:
            error_log_file (str, optional): Path to the error log file.

        Returns:
            LoggerProxy: Self reference for method chaining
        """
        enable_error_logging(error_log_file)
        return self

    def set_error_log_file(self, path):
        """Set the file path for error logging with method chaining support.

        Args:
            path (str): Path to the log file

        Returns:
            LoggerProxy: Self reference for method chaining support.
        """
        set_error_log_file(path)
        return self

    def add_file_handler(
        self,
        file_path,
        level="DEBUG",
        mode="w",
        formatter_pattern=None,
        strip_rich_markup=True,
    ):
        """Add a file handler to the root logger for outputting logs to a file.

        This method eliminates the need to import the standard logging module
        when setting up file-based logging.

        Args:
            file_path (str): Path to the log file
            level (str): Logging level for this handler (DEBUG, INFO, etc.)
            mode (str): File mode ('w' for write/overwrite, 'a' for append)
            formatter_pattern (str): Optional custom formatter pattern
                                     Defaults to "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            strip_rich_markup (bool): Whether to strip Rich markup tags from log messages

        Returns:
            LoggerProxy: Self reference for method chaining
        """
        # Ensure parent directory exists
        file_path = Path(file_path)
        if file_path.parent and not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)

        # Create file handler
        handler = logging.FileHandler(str(file_path), mode=mode)

        # Set level
        level_str = level.upper() if isinstance(level, str) else level
        handler.setLevel(level_str)

        # Create formatter
        if formatter_pattern is None:
            formatter_pattern = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(formatter_pattern)
        handler.setFormatter(formatter)

        # Add a filter to strip Rich markup if requested
        if strip_rich_markup:
            import re

            rich_markup_pattern = re.compile(r"\[(.*?)\]")

            class RichMarkupStripper(logging.Filter):
                def filter(self, record):
                    if isinstance(record.msg, str):
                        # Remove Rich markup tags
                        record.msg = rich_markup_pattern.sub("", record.msg)

                        # Also strip markup from any values in the record.args tuple if it exists
                        if record.args and isinstance(record.args, tuple):
                            args_list = list(record.args)
                            for i, arg in enumerate(args_list):
                                if isinstance(arg, str):
                                    args_list[i] = rich_markup_pattern.sub("", arg)
                            record.args = tuple(args_list)

                        # Also strip markup from any extra values
                        if hasattr(record, "source_file") and isinstance(
                            record.source_file, str
                        ):
                            record.source_file = rich_markup_pattern.sub(
                                "", record.source_file
                            )
                        if hasattr(record, "source_line") and isinstance(
                            record.source_line, str
                        ):
                            record.source_line = rich_markup_pattern.sub(
                                "", record.source_line
                            )
                    return True

            handler.addFilter(RichMarkupStripper())

        # Add to root logger
        logging.getLogger().addHandler(handler)

        # Log the file handler setup using the logger itself
        self.info(f"Added file handler: {file_path}")

        return self

    def get_logger(self, name=None):
        """Get a logger instance for the specified name.

        This provides access to the standard logging.getLogger() functionality
        without requiring direct import of the logging module.

        Args:
            name (str, optional): Logger name. If None, uses the caller's module name.

        Returns:
            logging.Logger: The requested logger instance
        """
        if name is None:
            frame = inspect.currentframe().f_back
            module = inspect.getmodule(frame)
            name = module.__name__ if module else "__main__"

        return logging.getLogger(name)

    def create_formatter(self, pattern):
        """Create a log formatter with the specified pattern.

        This provides access to logging.Formatter without requiring direct import
        of the logging module.

        Args:
            pattern (str): Formatter pattern string

        Returns:
            logging.Formatter: Configured formatter instance
        """
        return logging.Formatter(pattern)

    def should_show_rich_output(self):
        """
        Determine if rich Progress and print should be displayed based on log level.

        Returns:
            bool: True if the current log level allows rich output (DEBUG, INFO, WARNING),
                  False if the current log level is ERROR or CRITICAL.
        """
        # Get the root logger level
        root_level = logging.getLogger().level

        # Allow rich output for DEBUG, INFO, WARNING levels
        # Suppress rich output for ERROR and CRITICAL levels
        return root_level < logging.ERROR

    def print(self, *args, **kwargs):
        """
        Print using rich.print only if the current log level allows rich output.

        NOTE: This method is provided for backward compatibility. For new code,
        prefer using logger.console.print() which provides better rich object
        rendering and is always available regardless of log level.

        This function behaves like rich.print but respects the log level settings:
        - When log level is DEBUG, INFO, or WARNING: prints normally
        - When log level is ERROR or CRITICAL: suppresses output

        Args:
            *args: Positional arguments to pass to rich.print
            **kwargs: Keyword arguments to pass to rich.print
        """
        if self.should_show_rich_output():
            # Import locally to avoid circular imports
            from rich.console import Console

            # Use console for better rendering of rich objects
            console = Console()
            for arg in args:
                console.print(arg)

    def progress(self, *args, **kwargs):
        """
        Create a rich.progress.Progress instance only if the current log level allows rich output.

        This function returns a Progress object when the log level is DEBUG, INFO, or WARNING,
        but returns a no-op context manager when the log level is ERROR or CRITICAL.

        Args:
            *args: Positional arguments to pass to rich.progress.Progress
            **kwargs: Keyword arguments to pass to rich.progress.Progress

        Returns:
            Context manager: Either a Progress object or a no-op context manager
        """
        if self.should_show_rich_output():
            # Import locally to avoid circular imports
            from rich.progress import Progress

            return Progress(*args, **kwargs)
        else:
            # Return a no-op context manager when output should be suppressed
            class NoOpProgress:
                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc_val, exc_tb):
                    pass

                def add_task(self, *args, **kwargs):
                    return 0

                def update(self, *args, **kwargs):
                    pass

            return NoOpProgress()

    def enable_smart_print(self, enabled=True):
        """
        Enable or disable the smart print feature that makes all print statements
        respect log level settings.

        When enabled, the built-in print function is monkey-patched to use logger.console.print,
        making it respect the current log level (prints for DEBUG, INFO, WARNING; suppresses for ERROR, CRITICAL)
        while providing the best rich object rendering available.

        Args:
            enabled (bool): Whether to enable (True) or disable (False) smart print

        Returns:
            LoggerProxy: Self reference for method chaining
        """
        if enabled:
            # Store original print function if not already stored
            if not hasattr(builtins, "_original_print"):
                builtins._original_print = builtins.print

            # Replace built-in print with a function that uses logger.console.print
            def smart_print(*args, **kwargs):
                if self.should_show_rich_output():
                    # Use our shared console for consistent rendering
                    # Extract file and end parameters as they're not supported by console.print
                    file = kwargs.pop("file", None)
                    end = kwargs.pop("end", None)

                    # If output is being redirected to a file (like in exception handling),
                    # use the original print function
                    if (
                        file is not None
                        and file is not sys.stdout
                        and file is not sys.stderr
                    ):
                        if hasattr(builtins, "_original_print"):
                            builtins._original_print(*args, **kwargs)
                        return

                    # Otherwise use the console to print
                    self.console.print(*args, **kwargs)

            # Replace the built-in print
            builtins.print = smart_print

            # Use debug level message to not appear in higher log levels
            self.debug("Smart print enabled - print statements now respect log level")

            # Always show this message regardless of level
            if logging.getLogger().level >= logging.ERROR:
                # For ERROR and CRITICAL, use the original print function to show a message
                if hasattr(builtins, "_original_print"):
                    builtins._original_print(
                        "Smart print enabled - print output will be suppressed at current log level"
                    )
        else:
            # Restore original print if we have it stored
            if hasattr(builtins, "_original_print"):
                builtins.print = builtins._original_print
                self.debug("Smart print disabled - print statements restored to normal")

        return self

    # Constants to expose logging levels without importing logging
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

    @property
    def console(self):
        """
        Get a rich.console.Console instance for direct rendering of rich objects.

        This property provides access to a shared Console instance that can be used
        for rendering rich objects directly, regardless of log level.

        Examples:
            # Print a rich table directly
            table = Table(title="Data")
            table.add_column("Name")
            table.add_row("Example")
            logger.console.print(table)

            # Print formatted text
            logger.console.print("[bold red]Important message[/bold red]")

        Returns:
            rich.console.Console: A Console instance for direct rendering
        """
        global _console

        # Lazy initialize console
        if _console is None:
            # Import locally to avoid circular imports
            from rich.console import Console

            _console = Console(
                highlight=False
            )  # Disable syntax highlighting to preserve markup

        return _console


# Create the auto-detecting logger proxy for conventional syntax
logger = LoggerProxy()

# Enable rich logging by default for best experience
if RICH_AVAILABLE:
    use_rich_logging(True)

# Enable smart print by default to support rich object rendering
logger.enable_smart_print(True)

# Add these to __all__ to make them available when importing
__all__ = [
    "logger",
    "show_filename",
    "use_rich_logging",
    "log_timeout",
    "set_timeout_log_file",
    "enable_error_logging",
    "set_error_log_file",
    "get_error_log_file",
    "enable_smart_print",
]

# Test logger if run directly
if __name__ == "__main__":
    # Test the proxy logger with conventional syntax
    print("\nTesting logger with file/line detection:")
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    logger.critical("Critical message")

    # Test with different logging levels
    print("\nTesting logger with different levels:")
    logger.setLevel("WARNING")
    logger.debug("Debug message (should not appear)")
    logger.info("Info message (should not appear)")
    logger.warning("Warning message (should appear)")

    # Reset to DEBUG
    logger.setLevel("DEBUG")

    # Test rich logging if available
    if RICH_AVAILABLE:
        print("\nTesting Rich logging:")
        logger.use_rich(True)
        logger.info("Info with [bold green]Rich formatting[/bold green]")

        try:
            1 / 0
        except Exception as e:
            logger.exception("Exception with Rich traceback")

        # Switch back to colorlog
        print("\nSwitching back to colorlog:")
        logger.use_rich(False)
    else:
        print("\nRich library not installed, skipping Rich logging tests")

    # Test method chaining
    print("\nTesting method chaining:")
    logger.debug("First message").info("Second message").warning("Third message")

    # Create and test a temporary file
    test_file = "temp_logger_test.py"
    with open(test_file, "w") as f:
        f.write(
            """#!/usr/bin/env python3
from utils.logger_setup import logger

def test_from_another_file():
    logger.info("Log from another file")
    
if __name__ == "__main__":
    test_from_another_file()
"""
        )

    # Make it executable
    os.chmod(test_file, 0o755)

    # Run the test file
    print("\nTesting logging from another file:")
    os.system(f"python {test_file}")

    # Clean up
    os.remove(test_file)


def _configure_timeout_logger():
    """Configure the dedicated logger for timeout events.

    This creates a separate file-based logger specifically for logging timeout events,
    which can be used for analyzing performance issues.
    """
    global _timeout_logger, _timeout_logger_configured, _timeout_log_file

    if _timeout_logger_configured:
        return _timeout_logger

    # Create directory if it doesn't exist
    log_dir = os.path.dirname(_timeout_log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # Set up file handler
    timeout_logger = logging.getLogger("timeout_logger")
    timeout_logger.setLevel(logging.INFO)

    # Create a FileHandler that appends to the timeout log file
    handler = logging.FileHandler(_timeout_log_file, mode="a")

    # Create a formatter that includes timestamp, module name, and message
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)

    # Add the handler to the logger
    timeout_logger.handlers.clear()
    timeout_logger.addHandler(handler)

    # Set flag and store logger
    _timeout_logger = timeout_logger
    _timeout_logger_configured = True

    return timeout_logger


def log_timeout(
    operation: str, timeout_value: float, module_name: str = None, details: dict = None
):
    """Log a timeout event to the dedicated timeout log file.

    This function provides a centralized way to log timeout events, making it easier
    to analyze performance issues related to timeouts.

    Args:
        operation: Description of the operation that timed out
        timeout_value: The timeout value in seconds that was breached
        module_name: Optional module name (detected automatically if not provided)
        details: Optional dictionary with additional details about the operation
    """
    global _timeout_logger

    # Configure logger if needed
    if not _timeout_logger_configured:
        _configure_timeout_logger()

    # Auto-detect module name if not provided
    if module_name is None:
        frame = inspect.currentframe().f_back
        module = inspect.getmodule(frame)
        module_name = module.__name__ if module else "__main__"

    # Format the details
    details_str = ""
    if details:
        details_str = " | " + " | ".join([f"{k}={v}" for k, v in details.items()])

    # Create the log message
    message = f"TIMEOUT EXCEEDED: {operation} (limit: {timeout_value}s){details_str}"

    # Log to both the timeout log and the standard log
    _timeout_logger.error(message)

    # Get the module-specific logger and log there too
    module_logger = _get_module_logger(module_name)
    module_logger.error(message)

    # Return True to indicate the message was logged
    return True


def set_timeout_log_file(path: str):
    """Set the file path for timeout logging.

    Args:
        path: Path to the log file

    Returns:
        bool: True if successful
    """
    global _timeout_log_file, _timeout_logger_configured, _timeout_logger

    _timeout_log_file = path

    # Reset the logger so it will be reconfigured with the new path
    if _timeout_logger_configured:
        _timeout_logger_configured = False
        _timeout_logger = None

    return True


# Function to configure and enable error logging
def _configure_error_logger():
    """Configure the dedicated logger for error, warning, and critical events.

    This creates a separate file-based logger specifically for logging error level
    and above events, which can be used for monitoring and troubleshooting.
    """
    global _error_logger, _error_logger_configured, _error_log_file

    if _error_logger_configured:
        return _error_logger

    # Create directory if it doesn't exist
    log_dir = os.path.dirname(_error_log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # Set up file handler
    error_logger = logging.getLogger("error_logger")
    error_logger.setLevel(logging.WARNING)  # Capture WARNING and above
    error_logger.propagate = False  # Don't propagate to the root logger

    # Create a FileHandler that appends to the error log file
    handler = logging.FileHandler(_error_log_file, mode="a")
    handler.setLevel(logging.WARNING)  # Only WARNING, ERROR, and CRITICAL

    # Create a formatter that includes timestamp, module name, and message
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s (%(filename)s:%(lineno)d)",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    # Add a filter to strip Rich markup
    import re

    rich_markup_pattern = re.compile(r"\[(.*?)\]")

    class RichMarkupStripper(logging.Filter):
        def filter(self, record):
            if isinstance(record.msg, str):
                # Remove Rich markup tags
                record.msg = rich_markup_pattern.sub("", record.msg)

                # Also strip markup from any values in the record.args tuple if it exists
                if record.args and isinstance(record.args, tuple):
                    args_list = list(record.args)
                    for i, arg in enumerate(args_list):
                        if isinstance(arg, str):
                            args_list[i] = rich_markup_pattern.sub("", arg)
                    record.args = tuple(args_list)
            return True

    handler.addFilter(RichMarkupStripper())

    # Add the handler to the logger
    error_logger.handlers.clear()
    error_logger.addHandler(handler)

    # Set flag and store logger
    _error_logger = error_logger
    _error_logger_configured = True

    return error_logger


# Function to enable error logging for all modules
def enable_error_logging(error_log_file=None):
    """Enable logging of all errors, warnings, and critical messages to a dedicated file.

    This configures a separate logger that captures all WARNING, ERROR, and CRITICAL
    level messages from all modules and writes them to a centralized log file.

    Args:
        error_log_file (str, optional): Path to the error log file.
                                       If None, uses the default path.

    Returns:
        bool: True if successful
    """
    global _error_log_file, _error_logger_configured, _error_logger, _error_logging_enabled

    # Update log file path if provided
    if error_log_file:
        _error_log_file = error_log_file
        # Reset the logger so it will be reconfigured with the new path
        if _error_logger_configured:
            _error_logger_configured = False
            _error_logger = None

    # Configure error logger
    _configure_error_logger()

    # Set up root logger handler to forward error messages
    if _root_configured:
        root_logger = logging.getLogger()

        # Add filter to only forward WARNING and above
        class ErrorFilter(logging.Filter):
            def filter(self, record):
                return record.levelno >= logging.WARNING

        # Create a handler that sends to error logger
        handler = logging.Handler()
        handler.setLevel(logging.WARNING)
        handler.addFilter(ErrorFilter())

        # Create the formatter
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s (%(filename)s:%(lineno)d)",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        # Custom emit method to send to error log file
        def custom_emit(record):
            if _error_logger:
                _error_logger.handle(record)

        handler.emit = custom_emit

        # Add handler to root logger
        root_logger.addHandler(handler)

    _error_logging_enabled = True
    return True


def get_error_log_file():
    """Get the current error log file path.

    Returns:
        str: Path to the error log file
    """
    return _error_log_file


def set_error_log_file(path):
    """Set the file path for error logging.

    Args:
        path (str): Path to the log file

    Returns:
        bool: True if successful
    """
    global _error_log_file, _error_logger_configured, _error_logger

    _error_log_file = path

    # Reset the logger so it will be reconfigured with the new path
    if _error_logger_configured:
        _error_logger_configured = False
        _error_logger = None

    return True


# Add to the global scope
def enable_smart_print(enabled=True):
    """
    Enable or disable the smart print feature that makes all print statements
    respect log level settings.

    When enabled, the built-in print function is monkey-patched to use rich print,
    making it respect the current log level (prints for DEBUG, INFO, WARNING; suppresses for ERROR, CRITICAL).

    This is a global function that calls logger.enable_smart_print().

    Args:
        enabled (bool): Whether to enable (True) or disable (False) smart print

    Returns:
        bool: True if successful
    """
    logger.enable_smart_print(enabled)
    return True


def configure_session_logging(session_name, log_level="DEBUG"):
    """
    Configure comprehensive session logging with timestamp-based files.

    This function:
    1. Creates necessary log directories
    2. Generates timestamped log files
    3. Sets up file handlers for regular logs and errors
    4. Returns paths to log files for reference

    Args:
        session_name (str): Name of the session (used in log filenames)
        log_level (str): Logging level to use

    Returns:
        tuple: (main_log_path, error_log_path, timestamp) for reference
    """
    # Generate timestamp for consistent filenames
    timestamp = pendulum.now("UTC").format("YYYYMMDD_HHmmss")

    # Create log directories
    main_log_dir = Path(f"logs/{session_name}_logs")
    error_log_dir = Path("logs/error_logs")

    main_log_dir.mkdir(parents=True, exist_ok=True)
    error_log_dir.mkdir(parents=True, exist_ok=True)

    # Define log paths
    main_log_path = main_log_dir / f"{session_name}_{timestamp}.log"
    error_log_path = error_log_dir / f"{session_name}_errors_{timestamp}.log"

    # Configure logging
    logger.setLevel(log_level)
    logger.add_file_handler(
        str(main_log_path), level=log_level, mode="w", strip_rich_markup=True
    )
    logger.enable_error_logging(str(error_log_path))

    # Verify log files exist
    # Wait a short time for file handlers to flush
    time.sleep(0.1)
    # Check and log file status
    main_exists = Path(main_log_path).exists()
    error_exists = Path(error_log_path).exists()
    main_size = Path(main_log_path).stat().st_size if main_exists else 0
    error_size = Path(error_log_path).stat().st_size if error_exists else 0

    # Use original print to ensure this message gets through regardless of log level
    if hasattr(builtins, "_original_print"):
        builtins._original_print(
            f"Main log created: {main_exists}, size: {main_size} bytes"
        )
        builtins._original_print(
            f"Error log created: {error_exists}, size: {error_size} bytes"
        )

    # Log initialization
    logger.info(f"Session logging initialized for {session_name}")
    logger.debug(f"Log level: {log_level}")
    logger.debug(f"Main log: {main_log_path}")
    logger.debug(f"Error log: {error_log_path}")

    return str(main_log_path), str(error_log_path), timestamp
