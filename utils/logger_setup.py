import logging, os
import inspect
import warnings
from colorlog import ColoredFormatter

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

# Default color scheme
DEFAULT_LOG_COLORS = {
    "DEBUG": "cyan",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red,bg_white",
}

# Simple format
FORMAT = "%(log_color)s%(levelname)-8s%(reset)s %(name)s: %(message)s"
# Rich format (simpler as Rich handles the styling)
RICH_FORMAT = "%(message)s"

# Rich console instance
if RICH_AVAILABLE:
    console = Console()


def get_logger(name=None, level=None, setup_root=False, use_rich=None):
    """
    Retrieve or create a logger instance with appropriate configuration.

    Implements module-specific logger acquisition with optional root logger configuration.
    Maintains singleton logger instances via caching mechanism to prevent duplicate
    handler configuration and ensure consistent behavior across modules.

    Parameters:
        name (str, optional): Logger name, typically __name__. If None, automatically
                              derived from calling module via stack introspection.
        level (str, optional): Logging level threshold for message filtering.
                               One of: DEBUG, INFO, WARNING, ERROR, CRITICAL.
        setup_root (bool): When True, configures the root logger with specified level,
                           affecting all loggers in the hierarchy.
        use_rich (bool, optional): When True, uses rich for logging formatting.
                                   If None, uses the value from USE_RICH_LOGGING env var.

    Returns:
        logging.Logger: Configured logger instance with appropriate level and handlers.
    """
    # Auto-detect caller's module name if not provided
    if name is None:
        frame = inspect.currentframe().f_back
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

    logger = logging.getLogger(name)

    # If not using as a module within a configured app,
    # set up minimal logging for this module
    if not _root_configured and not logger.handlers:
        log_level = (level or DEFAULT_LEVEL).upper()
        logger.setLevel(log_level)
    elif level:
        # Allow level override when explicitly requested
        logger.setLevel(level.upper())

    # Cache the logger
    _module_loggers[name] = logger

    return logger


def _setup_root_logger(level=None, use_rich=None):
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

    Returns:
        logging.Logger: Configured root logger instance.
    """
    global _root_configured, _use_rich

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
        # Use Rich handler
        handler = RichHandler(
            console=console,
            rich_tracebacks=True,
            markup=True,
            show_time=False,
            show_path=False,
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
        logger = get_logger()
        logger.warning("Rich library not available. Install with 'pip install rich'")
        return False

    _use_rich = enable
    _setup_root_logger(level=level, use_rich=enable)
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
            frame = inspect.currentframe().f_back
            module = inspect.getmodule(frame)
            module_name = module.__name__ if module else "__main__"
            logger = get_logger(module_name)

            # Create a wrapper that calls the log method and returns self for chaining
            def log_wrapper(*args, **kwargs):
                log_method = getattr(logger, name)
                log_method(*args, **kwargs)
                return self

            return log_wrapper

        # For any other attributes, use this module's logger
        logger = get_logger()
        return getattr(logger, name)

    def setLevel(self, level, configure_root=True):
        """
        Conventional style method to set logging level, matching standard Logger API.

        Sets the level for the current module's logger and optionally
        configures the root logger. Maintains proxy behavior and method chaining.

        Parameters:
            level (str or int): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                               or corresponding integer value.
            configure_root (bool): When True (default), also configures the root logger
                                  and propagates level setting to all loggers.

        Returns:
            LoggerProxy: Self reference for method chaining support.
        """
        # Check if environment variable takes precedence
        env_level = os.environ.get("LOG_LEVEL")
        if env_level:
            level = env_level

        # Normalize level format
        if isinstance(level, str):
            level_str = level.upper()
        else:
            level_str = level

        # Configure the root logger if requested
        if configure_root:
            _setup_root_logger(level=level_str)

        # Get caller's module name
        frame = inspect.currentframe().f_back
        module = inspect.getmodule(frame)
        module_name = module.__name__ if module else "__main__"

        # Set level for the specific module logger (already done for all if root configured)
        # but ensure the specific module has the level set
        logger = get_logger(module_name)
        logger.setLevel(level_str)

        # Return self for chaining
        return self

    def setup(self, level=None):
        """
        [DEPRECATED] Use setLevel() instead.

        Configure the logging hierarchy with specified level threshold.

        Establishes the root configuration and propagates settings to all loggers.
        Respects environment variable configuration (LOG_LEVEL) which takes precedence
        over programmatically specified levels.

        Parameters:
            level (str, optional): Logging level threshold (DEBUG, INFO, WARNING, ERROR, CRITICAL).
                                   If None, uses environment variable or default.

        Returns:
            LoggerProxy: Self reference for method chaining support.
        """
        warnings.warn(
            "setup() is deprecated, use setLevel() instead",
            DeprecationWarning,
            stacklevel=2,
        )

        # If no level is provided, use the default
        if level is None:
            level = DEFAULT_LEVEL

        # Use the new setLevel method with root configuration
        return self.setLevel(level, configure_root=True)

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


# Create the auto-detecting logger proxy for conventional syntax
logger = LoggerProxy()

# Configure root logger with simple format by default
if not _root_configured:
    _setup_root_logger(level=DEFAULT_LEVEL, use_rich=_use_rich)

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
        try:
            1 / 0
        except Exception as e:
            logger.exception("Exception with Rich traceback")

        # Switch back to colorlog
        print("\nSwitching back to colorlog:")
        logger.use_rich(False)
    else:
        print("\nRich library not installed, skipping Rich logging tests")

    # Test deprecated setup method
    print("\nTesting deprecated setup method (should show warning):")
    logger.setup(level="INFO")
    logger.debug("This debug message should NOT appear")
    logger.info("This info message SHOULD appear")

    # Test method chaining with all levels
    print("\nTesting method chaining with all levels:")
    logger.debug("Debug chain").info("Info chain").warning("Warning chain").error(
        "Error chain"
    ).critical("Critical chain")

    # Test method chaining with setLevel
    print("\nTesting method chaining with setLevel:")
    logger.setLevel("DEBUG").debug("This chained debug message SHOULD appear")
