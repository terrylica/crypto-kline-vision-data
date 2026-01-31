#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Add D107 __init__ docstrings
"""Custom exceptions for REST API related operations.

This module defines specialized exceptions to provide more precise error handling
for REST API requests. By using custom exceptions, calling code can catch specific
error types and handle them appropriately.
"""

from data_source_manager.utils.loguru_setup import logger


class RestAPIError(Exception):
    """Base exception for all REST API related errors."""

    def __init__(self, message="REST API error occurred"):
        """Initialize RestAPIError with an error message.

        Args:
            message: Error description.
        """
        self.message = message
        super().__init__(self.message)
        logger.error(f"RestAPIError: {message}")


class RateLimitError(RestAPIError):
    """Exception raised when rate limited by the REST API."""

    def __init__(self, retry_after=None, message="Rate limited by REST API"):
        """Initialize RateLimitError with retry information.

        Args:
            retry_after: Seconds to wait before retrying.
            message: Error description.
        """
        self.retry_after = retry_after
        message_with_retry = f"{message} (retry after {retry_after}s)" if retry_after is not None else message
        super().__init__(f"RateLimitError: {message_with_retry}")


class HTTPError(RestAPIError):
    """Exception raised when an HTTP error occurs."""

    def __init__(self, status_code, message=None):
        """Initialize HTTPError with status code.

        Args:
            status_code: HTTP status code.
            message: Error description.
        """
        self.status_code = status_code
        message = message or f"HTTP error {status_code}"
        super().__init__(f"HTTPError: {message}")


class APIError(RestAPIError):
    """Exception raised when the API returns an error code."""

    def __init__(self, code, message=None):
        """Initialize APIError with error code.

        Args:
            code: API error code.
            message: Error description.
        """
        self.code = code
        message = message or f"API error code {code}"
        super().__init__(f"APIError: {message}")


class NetworkError(RestAPIError):
    """Exception raised when a network error occurs during REST API requests."""

    def __init__(self, message="Network error during REST API request"):
        """Initialize NetworkError with error message.

        Args:
            message: Error description.
        """
        super().__init__(f"NetworkError: {message}")


class RestTimeoutError(RestAPIError):
    """Exception raised when a REST API request times out."""

    def __init__(self, message="REST API request timed out"):
        """Initialize RestTimeoutError with error message.

        Args:
            message: Error description.
        """
        super().__init__(f"RestTimeoutError: {message}")


# Backwards compatibility alias (deprecated)
TimeoutError = RestTimeoutError


class JSONDecodeError(RestAPIError):
    """Exception raised when unable to decode JSON response from REST API."""

    def __init__(self, message="Failed to decode JSON response from REST API"):
        """Initialize JSONDecodeError with error message.

        Args:
            message: Error description.
        """
        super().__init__(f"JSONDecodeError: {message}")
