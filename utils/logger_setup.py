import logging, os
import inspect
from colorlog import ColoredFormatter
import traceback

try:
    from rich.console import Console
    from rich.logging import RichHandler

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Default log level from environment or INFO
DEFAULT_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Global state tracking
_root_configured = False
_module_loggers = {}
_use_rich = os.environ.get("USE_RICH_LOGGING", "").lower() in ("true", "1", "yes")
_show_filename = os.environ.get("SHOW_LOG_FILENAME", "").lower() in ("true", "1", "yes")

# Timeout logging
_timeout_log_file = os.environ.get(
    "TIMEOUT_LOG_FILE", "./logs/timeout_incidents/timeout_log.txt"
)
_timeout_logger_configured = False
_timeout_logger = None

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
# Format with filename - moved to the right end of the line to match Rich style
FORMAT_WITH_FILENAME = "%(log_color)s%(levelname)-8s%(reset)s %(name)s: %(message)s%(log_color)s %(filename)25s:%(lineno)-4d%(reset)s"
# Current format based on show_filename setting
FORMAT = FORMAT_WITH_FILENAME if _show_filename else FORMAT_BASE

# Rich format (simpler as Rich handles the styling)
RICH_FORMAT_BASE = "%(message)s"
# Rich format for filename display - no square brackets since Rich will add its own formatting
RICH_FORMAT_WITH_FILENAME = "%(message)s"  # Let Rich handle the file path display
RICH_FORMAT = RICH_FORMAT_WITH_FILENAME if _show_filename else RICH_FORMAT_BASE

# Rich console instance
if RICH_AVAILABLE:
    console = Console()


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
            stacklevel (int): How many frames to skip (unused, kept for compatibility)

        Returns:
            tuple: (filename, line number, function name, stack info)
        """
        # Start from the frame of the caller of this method
        frame = inspect.currentframe()

        # Skip this frame
        if frame:
            frame = frame.f_back

        # Initialize values
        fn, lno, func, sinfo = "(unknown file)", 0, "(unknown function)", None

        # Find frame that's not in the logging system
        while frame:
            module_name = frame.f_globals.get("__name__", "")
            if not (module_name == "logging" or module_name == __name__):
                # This is the original caller
                fn = frame.f_code.co_filename
                lno = frame.f_lineno
                func = frame.f_code.co_name
                if stack_info:
                    sinfo = self._get_stack_info(frame)
                break
            frame = frame.f_back

        return fn, lno, func, sinfo

    def _get_stack_info(self, frame):
        """
        Get formatted stack information for the given frame.

        Args:
            frame: The stack frame to format

        Returns:
            str: Formatted stack trace information
        """
        stack = traceback.extract_stack(frame)
        if stack:
            return "".join(traceback.format_list(stack))
        return None


# Register our custom logger class
logging.setLoggerClass(CustomLogger)


# DEPRECATED: This function is intentionally disabled to enforce the use of 'logger' instead
def get_logger(*args, **kwargs):
    """
    DEPRECATED: Do not use this function. Import and use 'logger' directly.

    Example:
        from utils.logger_setup import logger

        # Then use directly:
        logger.info("Your message")
    """
    raise DeprecationWarning(
        "get_logger() is deprecated and has been disabled. "
        "Please use 'from utils.logger_setup import logger' instead and access logging methods directly."
    )


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


def _setup_root_logger(level=None, use_rich=None, show_filename=None):
    """
    Configure the root logger with specified level and colorized output.

    Establishes the hierarchy root configuration that affects all loggers.
    Clears any existing handlers to prevent duplicate log entries and
    applies consistent formatting across the logging system.

    Parameters:
        level (str, optional): Logging level threshold (DEBUG, INFO, WARNING, ERROR, CRITICAL).
                               If None, uses DEFAULT_LEVEL from environment or configuration.
        use_rich (bool, optional): When True, uses Rich for logging formatting.
                                  If None, uses the value from USE_RICH_LOGGING env var.
        show_filename (bool, optional): When True, includes filename and line number in log output.
                                       If None, uses the value from the global _show_filename.

    Returns:
        logging.Logger: Configured root logger instance.
    """
    global _root_configured, _use_rich, _show_filename, FORMAT, RICH_FORMAT

    # Determine whether to use rich
    if use_rich is None:
        use_rich = _use_rich
    else:
        _use_rich = use_rich

    # Determine whether to show filename
    if show_filename is not None:
        _show_filename = show_filename
        # Update format strings based on current setting
        FORMAT = FORMAT_WITH_FILENAME if _show_filename else FORMAT_BASE
        RICH_FORMAT = RICH_FORMAT_WITH_FILENAME if _show_filename else RICH_FORMAT_BASE

    root_logger = logging.getLogger()
    root_logger.handlers.clear()  # Clear existing handlers

    # Set level
    log_level = (level or DEFAULT_LEVEL).upper()
    level_int = getattr(logging, log_level)
    root_logger.setLevel(level_int)

    # Set up handler based on preference and availability
    if use_rich and RICH_AVAILABLE:
        # Use Rich handler
        handler = RichHandler(
            console=console,
            rich_tracebacks=True,
            markup=True,
            show_time=False,
            show_path=_show_filename,  # Show path if filename display is enabled
            enable_link_path=_show_filename,  # Enable clickable links when filenames are shown
        )
        formatter = logging.Formatter(RICH_FORMAT)
    else:
        # Use colorlog handler (fallback or default)
        handler = logging.StreamHandler()
        formatter = ColoredFormatter(
            FORMAT,
            log_colors=DEFAULT_LOG_COLORS,
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

    Configures the root logger to use Rich for enhanced console logging with
    proper formatting for structured data, exceptions, and other rich content.

    Parameters:
        enable (bool): True to enable Rich logging, False to use standard colorlog.
        level (str, optional): Logging level to set when reconfiguring.
                              If None, maintains current level.

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


def show_filename(enable=True, level=None):
    """
    Enable or disable showing filename and line number in log messages.

    Configures the root logger to include source file information in log output.

    Parameters:
        enable (bool): True to show filename and line number, False to hide.
        level (str, optional): Logging level to set when reconfiguring.
                              If None, maintains current level.

    Returns:
        bool: Current show_filename setting
    """
    global _show_filename

    _show_filename = enable
    _setup_root_logger(level=level, show_filename=enable)
    return enable


# Create a proxy logger object that automatically detects the calling module
class LoggerProxy:
    """
    Proxy implementation providing automatic module detection and method chaining.

    Implements the Proxy design pattern to provide transparent access to module-specific
    loggers without requiring explicit logger acquisition. Uses runtime introspection
    to determine the calling module and delegates to the appropriate logger instance.

    Additionally implements the Fluent Interface pattern through method chaining,
    allowing sequential logging operations and configuration with a concise syntax.
    """

    def __init__(self):
        """Initialize the LoggerProxy with root logger configuration"""
        # Configure root logger with default settings if not already configured
        if not _root_configured:
            _setup_root_logger(level=DEFAULT_LEVEL, use_rich=_use_rich)

    def __getattr__(self, name):
        """
        Dynamic attribute resolution with runtime module detection.

        Intercepts standard logging method calls (debug, info, etc.) and
        delegates them to the appropriate module-specific logger. Returns
        a callable wrapper that preserves the proxy instance for method chaining.

        Parameters:
            name (str): Attribute name being accessed, typically a logging method.

        Returns:
            callable: Wrapper function for logging methods or direct attribute access.
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
            # Get caller's module (skip this frame)
            frame = inspect.currentframe().f_back
            module = inspect.getmodule(frame)
            module_name = module.__name__ if module else "__main__"

            # Get the appropriate logger for the module
            logger_instance = _get_module_logger(module_name)

            # Create a wrapper that calls the log method and returns self for chaining
            def log_wrapper(*args, **kwargs):
                log_method = getattr(logger_instance, name)
                log_method(*args, **kwargs)
                return self

            return log_wrapper

        # For any other attributes, use this module's logger
        tmp_logger = _get_module_logger()
        return getattr(tmp_logger, name)

    def setup_root(self, level=None, use_rich=None, show_filename=None):
        """
        Configure the root logger with specified options.

        Provides a cleaner interface for setting up the root logger with various options.

        Parameters:
            level (str, optional): Logging level to set
            use_rich (bool, optional): Whether to use Rich logging
            show_filename (bool, optional): Whether to show filenames in log output

        Returns:
            LoggerProxy: Self reference for method chaining
        """
        _setup_root_logger(level=level, use_rich=use_rich, show_filename=show_filename)
        return self

    def setLevel(self, level, configure_root=True):
        """
        Set the logging level with method chaining support.

        Simplifies setting the logging level while supporting method chaining.

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
            # Use logging.getLogger directly to avoid circular reference
            # root_logger = logging.getLogger("root")
            # root_logger.debug(f"Logger root configured with level {level_str}")

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

    def show_filename(self, enable=True, level=None):
        """
        Enable or disable showing filename in log output with method chaining support.

        Parameters:
            enable (bool): True to show filename and line number, False to hide.
            level (str, optional): Logging level to set when reconfiguring.

        Returns:
            LoggerProxy: Self reference for method chaining support.
        """
        show_filename(enable=enable, level=level)
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


# Create the auto-detecting logger proxy for conventional syntax
logger = LoggerProxy()

# Hide the get_logger function from imports
__all__ = [
    "logger",
    "show_filename",
    "use_rich_logging",
    "log_timeout",
    "set_timeout_log_file",
]

# Test logger if run directly
if __name__ == "__main__":
    # Test the proxy logger with conventional syntax
    print("\nTesting conventional logger.xxx() syntax:")
    logger.debug("Debug using conventional syntax")
    logger.info("Info using conventional syntax")
    logger.warning("Warning using conventional syntax")
    logger.error("Error using conventional syntax")
    logger.critical("Critical using conventional syntax")

    # Test the conventional setLevel method
    print("\nTesting conventional logger.setLevel() method:")
    logger.setLevel("WARNING")
    logger.debug("This debug message should NOT appear")
    logger.info("This info message should NOT appear")
    logger.warning("This warning message SHOULD appear")

    # Test filename display
    print("\nTesting filename display:")
    logger.show_filename(True)
    logger.info("This message should show filename")
    logger.show_filename(False)
    logger.info("This message should NOT show filename")

    # Test rich logging if available
    if RICH_AVAILABLE:
        print("\nTesting Rich logging:")
        logger.use_rich(True)
        logger.debug("Debug message with [bold blue]Rich[/bold blue] formatting")
        logger.info("Info with Rich: [green]success[/green]")
        logger.warning(
            "Warning with data",
            extra={"data": {"complex": True, "nested": {"value": 42}}},
        )

        # Test rich logging with filename
        print("\nTesting Rich logging with filename:")
        logger.show_filename(True)
        logger.info("This message should show filename with Rich formatting")
        logger.show_filename(False)
        logger.info("This message should NOT show filename with Rich formatting")

        try:
            1 / 0
        except Exception as e:
            logger.exception("Exception with Rich traceback")

        # Switch back to colorlog
        print("\nSwitching back to colorlog:")
        logger.use_rich(False)
    else:
        print("\nRich library not installed, skipping Rich logging tests")

    # Test method chaining with all levels
    print("\nTesting method chaining with all levels:")
    logger.debug("Debug chain").info("Info chain").warning("Warning chain").error(
        "Error chain"
    ).critical("Critical chain")

    # Test method chaining with setLevel
    print("\nTesting method chaining with setLevel:")
    logger.setLevel("DEBUG").debug("This chained debug message SHOULD appear")

    # Test method chaining with show_filename
    print("\nTesting method chaining with show_filename:")
    logger.show_filename(True).info("This message should show filename (chained)")

    # Try to use the deprecated get_logger function - should raise an error
    try:
        get_logger()
    except Exception as e:
        print(f"\nSuccessfully disabled get_logger(): {e}")


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
