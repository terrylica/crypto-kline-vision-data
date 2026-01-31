#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Logger setup utilities.

This module provides utilities for setting up and configuring loggers.
"""

import inspect
import logging
import os
from collections.abc import Callable
from typing import Any

try:
    from rich.console import Console
    from rich.logging import RichHandler

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

# Default log level from environment or INFO
DEFAULT_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


def get_module_logger(
    name: str | None = None,
    level: str | None = None,
    module_loggers: dict[str, logging.Logger] | None = None,
    setup_root: bool = False,
    use_rich: bool | None = None,
    setup_root_logger: Callable[..., Any] | None = None,
) -> logging.Logger:
    """Retrieve or create a logger instance with appropriate configuration.

    Args:
        name: Logger name. If None, auto-detected from caller
        level: Logging level
        module_loggers: Dictionary to cache module loggers
        setup_root: Whether to set up the root logger
        use_rich: Whether to use rich logging
        setup_root_logger: Function to set up the root logger

    Returns:
        logging.Logger: The requested logger instance
    """
    if module_loggers is None:
        module_loggers = {}

    # Auto-detect caller's module name if not provided
    if name is None:
        frame = inspect.currentframe().f_back.f_back  # Extra level for caller
        module = inspect.getmodule(frame)
        name = module.__name__ if module else "__main__"

    # Configure root logger if requested
    if setup_root and setup_root_logger:
        setup_root_logger(level, use_rich=use_rich)

    # Return cached logger if already created for this module
    if name in module_loggers:
        return module_loggers[name]

    logger_instance = logging.getLogger(name)

    # If not using as a module within a configured app,
    # set up minimal logging for this module
    root_configured = hasattr(logging.getLogger(), "_root_configured")
    if not root_configured and not logger_instance.handlers:
        log_level = (level or DEFAULT_LEVEL).upper()
        logger_instance.setLevel(log_level)
    elif level:
        # Allow level override when explicitly requested
        logger_instance.setLevel(level.upper())

    # Cache the logger
    module_loggers[name] = logger_instance

    return logger_instance


def setup_rich_handler(console: Any = None) -> Any:
    """Set up a RichHandler for rich logging.

    Args:
        console: Console to use (rich.console.Console)

    Returns:
        RichHandler: Configured RichHandler
    """
    if not RICH_AVAILABLE:
        raise ImportError("Rich library is not available. Install with 'pip install rich'")

    if console is None:
        console = Console(highlight=False)  # Disable syntax highlighting to preserve markup

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
    handler.setFormatter(formatter)

    return handler


def use_rich_logging(
    enable: bool = True,
    level: str | None = None,
    setup_root_logger: Callable[..., Any] | None = None,
) -> bool:
    """Enable or disable Rich logging.

    Parameters:
        enable: True to enable Rich logging, False to use standard colorlog.
        level: Logging level to set when reconfiguring.
        setup_root_logger: Function to set up the root logger

    Returns:
        bool: True if Rich logging was enabled, False otherwise.
    """
    if enable and not RICH_AVAILABLE:
        tmp_logger = logging.getLogger(__name__)
        tmp_logger.warning("Rich library not available. Install with 'pip install rich'")
        return False

    if setup_root_logger:
        setup_root_logger(level=level, use_rich=enable)

    return enable
