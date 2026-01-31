#!/usr/bin/env python
"""Network-related exception classes.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from network_utils.py for modularity
"""

__all__ = [
    "DownloadException",
    "DownloadStalledException",
    "RateLimitException",
]


class DownloadException(Exception):
    """Base exception for download-related errors."""


class DownloadStalledException(DownloadException):
    """Raised when a download appears to have stalled."""


class RateLimitException(DownloadException):
    """Raised when rate limits are hit during downloads."""
