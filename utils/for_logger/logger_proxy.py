#!/usr/bin/env python3
"""
Logger proxy implementation.

This module provides the LoggerProxy class, which is the main interface for the
logger system, providing automatic module detection and method chaining.
"""

import inspect
import logging
import os
from pathlib import Path

from utils.for_logger.console_utils import (
    create_rich_progress,
    get_console,
    should_show_rich_output,
)
from utils.for_logger.console_utils import (
    enable_smart_print as _enable_smart_print,
)

# Global rich availability flag
try:
    pass

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class ConsoleState:
    """Singleton class that manages console state, avoiding global variables."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._console = None
        return cls._instance

    @property
    def console(self):
        return self._console

    @console.setter
    def console(self, value):
        self._console = value


# Create singleton instance
_console_state = ConsoleState()


class LoggerProxy:
    """
    Proxy implementation providing automatic module detection and method chaining.
    """

    def __init__(self, get_module_logger_fn, setup_root_logger_fn=None, use_rich=None):
        """
        Initialize the LoggerProxy.

        Args:
            get_module_logger_fn (callable): Function to get a module logger
            setup_root_logger_fn (callable, optional): Function to set up the root logger
            use_rich (bool, optional): Whether to use rich logging
        """
        self._get_module_logger = get_module_logger_fn
        self._setup_root_logger = setup_root_logger_fn
        self._use_rich = use_rich

        # Configure root logger with default settings if setup function is provided
        if self._setup_root_logger:
            self._setup_root_logger()

    def __getattr__(self, name):
        """
        Dynamic attribute resolution with runtime module detection.

        Args:
            name (str): Attribute name

        Returns:
            callable: Function that delegates to the appropriate logger method
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
            logger_instance = self._get_module_logger(module_name)

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
                if self._use_rich and RICH_AVAILABLE and args:
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
                kwargs.pop("stacklevel", None)

                # Call the actual log method
                log_method = getattr(logger_instance, name)
                log_method(*args, **kwargs)

                # Return self for method chaining
                return self

            return log_wrapper

        # For any other attributes, use this module's logger
        tmp_logger = self._get_module_logger()
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
        if self._setup_root_logger:
            self._setup_root_logger(level=level, use_rich=use_rich)
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
            raise ValueError(f"Invalid log level: {level_str}. Valid levels are: {valid_levels}")

        # Set up root logger if requested (usually first setup)
        if configure_root and self._setup_root_logger:
            self._setup_root_logger(level=level_str, use_rich=self._use_rich)

        # Get corresponding module
        frame = inspect.currentframe()
        module = inspect.getmodule(frame.f_back) if frame else None
        module_name = module.__name__ if module else "__main__"

        # Set level for the specific module logger
        module_logger = self._get_module_logger(module_name)
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
        from utils.for_logger.logger_setup_utils import use_rich_logging

        self._use_rich = use_rich_logging(enable=enable, level=level, setup_root_logger=self._setup_root_logger)
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

        # Import locally to avoid circular imports
        from utils.for_logger.timeout_logger import log_timeout as _log_timeout

        # Use the log_timeout function with module name
        _log_timeout(operation, timeout_value, module_name, details, self._get_module_logger)

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
        from utils.for_logger.timeout_logger import (
            set_timeout_log_file as _set_timeout_log_file,
        )

        _set_timeout_log_file(path)
        return self

    def enable_error_logging(self, error_log_file=None):
        """Enable logging of all errors, warnings, and critical messages to a dedicated file.

        Args:
            error_log_file (str, optional): Path to the error log file.

        Returns:
            LoggerProxy: Self reference for method chaining
        """
        from utils.for_logger.error_logger import (
            enable_error_logging as _enable_error_logging,
        )

        root_configured = hasattr(logging.getLogger(), "_root_configured")
        _enable_error_logging(error_log_file, root_configured)
        return self

    def set_error_log_file(self, path):
        """Set the file path for error logging with method chaining support.

        Args:
            path (str): Path to the log file

        Returns:
            LoggerProxy: Self reference for method chaining support.
        """
        from utils.for_logger.error_logger import (
            set_error_log_file as _set_error_log_file,
        )

        _set_error_log_file(path)
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
            from utils.for_logger.formatters import RichMarkupStripper

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
        return should_show_rich_output()

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
            # Use console for better rendering of rich objects
            self.console.print(*args, **kwargs)

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
        return create_rich_progress(*args, **kwargs)

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
        _enable_smart_print(enabled, self.console)
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
        # Lazy initialize console
        if _console_state.console is None:
            _console_state.console = get_console(highlight=False)

        return _console_state.console
