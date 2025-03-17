import logging, os
from rich.logging import RichHandler
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, FloatPrompt
from rich import print
from rich.traceback import install
import warnings
import traceback
install(show_locals=True)

console = Console()
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(message)s",
    datefmt="[%X]",
    handlers=[
        RichHandler(
            console=console,
            rich_tracebacks=True,
            markup=True,
            show_path=True,
            omit_repeated_times=True,
            log_time_format="[%X]"
        )
    ],
    force=True
)

def get_logger(name: str, level: str = None, show_path: bool = None, rich_tracebacks: bool = True) -> logging.Logger:
    logger = logging.getLogger(name)
    if level:
        logger.setLevel(level.upper())
    
    # Always use RichHandler with rich_tracebacks enabled
    handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        markup=True,
        show_path=show_path if show_path is not None else True,
        omit_repeated_times=True,
        log_time_format="[%X]"
    )
    logger.handlers = [handler]  # Replace any existing handlers
    logger.propagate = False

    # Add some debug logging
    logger.debug(f"Logger '{name}' configured with level={logger.level}, show_path={show_path}, rich_tracebacks=True")
    
    return logger

logger = get_logger(__name__)

def setup_warning_logging():
    def custom_warning_handler(message, category, filename, lineno, file=None, line=None):
        tb = ''.join(traceback.format_stack(limit=5))
        logger.warning(f"{category.__name__}: {message} at {filename}:{lineno}\nStack trace:\n{tb}")
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
