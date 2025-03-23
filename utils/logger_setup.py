import logging, os
from colorlog import ColoredFormatter
import warnings
import traceback

# Default log level from environment
console_log_level = os.environ.get("LOG_LEVEL", "INFO").upper()

# Do not configure root logger with basicConfig as it can interfere with pytest


def get_logger(name: str, level: str = None, show_path: bool = None) -> logging.Logger:
    """
    Get a logger configured with colored output that works with pytest's caplog.

    Args:
        name: The name of the logger
        level: The log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        show_path: Whether to show the logger name/path in logs

    Returns:
        A configured logger instance
    """
    logger = logging.getLogger(name)
    log_level = level.upper() if level else console_log_level
    logger.setLevel(log_level)

    # Only add a handler if the logger doesn't already have one
    # This prevents duplicate handlers when get_logger is called multiple times
    if not logger.handlers:
        # Create a handler that writes log messages to stderr
        handler = logging.StreamHandler()

        # Create colored formatter for console output
        console_format = "%(levelname)-8s"
        if show_path is None or show_path:
            console_format += " %(name)s:"
        console_format += " %(message)s"

        # Create a ColoredFormatter
        formatter = ColoredFormatter(
            console_format,
            datefmt="[%X]",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
            style="%",
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # IMPORTANT: Leave propagate as True for pytest caplog to work
    # caplog captures logs from the root logger, so our messages need to propagate
    logger.propagate = True

    return logger


logger = get_logger(__name__)


def setup_warning_logging():
    """Configure the warnings module to log using our custom logger"""

    def custom_warning_handler(
        message, category, filename, lineno, file=None, line=None
    ):
        tb = "".join(traceback.format_stack(limit=5))
        logger.warning(
            f"{category.__name__}: {message} at {filename}:{lineno}\nStack trace:\n{tb}"
        )

    warnings.showwarning = custom_warning_handler


# Test the logger setup
if __name__ == "__main__":
    test_logger = get_logger("test_logger", level="DEBUG")
    test_logger.debug("This is a debug message")
    test_logger.info("This is an info message")
    test_logger.warning("This is a warning message")
    test_logger.error("This is an error message")
    try:
        raise ValueError("This is a test exception")
    except Exception as e:
        test_logger.exception("An exception occurred:")
