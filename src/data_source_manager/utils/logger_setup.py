#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Legacy Logger Module - Deprecated.

.. deprecated:: 1.0.1
    This module is deprecated. Use ``loguru_setup`` instead::

        # Old (deprecated):
        from data_source_manager.utils.logger_setup import logger

        # New (recommended):
        from data_source_manager.utils.loguru_setup import logger

This module re-exports from ``loguru_setup`` for backward compatibility.
All functionality is now available in the simpler loguru-based system.

Migration Guide:
    See docs/howto/loguru_migration.md for details.
"""

import warnings

# Re-export everything from loguru_setup for backward compatibility
from data_source_manager.utils.loguru_setup import (
    DSMLogger,
    LOG_FORMAT,
    SIMPLE_FORMAT,
    configure_file,
    configure_level,
    configure_session_logging,
    disable_colors,
    logger,
)

# Emit deprecation warning on import
warnings.warn(
    "data_source_manager.utils.logger_setup is deprecated. "
    "Use data_source_manager.utils.loguru_setup instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "LOG_FORMAT",
    "SIMPLE_FORMAT",
    "DSMLogger",
    "configure_file",
    "configure_level",
    "configure_session_logging",
    "disable_colors",
    "logger",
]
