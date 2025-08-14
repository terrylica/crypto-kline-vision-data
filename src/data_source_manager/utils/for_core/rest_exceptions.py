#!/usr/bin/env python3
"""
Custom exceptions for REST API related operations.

This module defines specialized exceptions to provide more precise error handling
for REST API requests. By using custom exceptions, calling code can catch specific
error types and handle them appropriately.
"""

from data_source_manager.utils.loguru_setup import logger


class RestAPIError(Exception):
    """Base exception for all REST API related errors."""

    def __init__(self, message="REST API error occurred"):
        self.message = message
        super().__init__(self.message)
        logger.error(f"RestAPIError: {message}")


class RateLimitError(RestAPIError):
    """Exception raised when rate limited by the REST API."""

    def __init__(self, retry_after=None, message="Rate limited by REST API"):
        self.retry_after = retry_after
        message_with_retry = f"{message} (retry after {retry_after}s)" if retry_after is not None else message
        super().__init__(f"RateLimitError: {message_with_retry}")


class HTTPError(RestAPIError):
    """Exception raised when an HTTP error occurs."""

    def __init__(self, status_code, message=None):
        self.status_code = status_code
        message = message or f"HTTP error {status_code}"
        super().__init__(f"HTTPError: {message}")


class APIError(RestAPIError):
    """Exception raised when the API returns an error code."""

    def __init__(self, code, message=None):
        self.code = code
        message = message or f"API error code {code}"
        super().__init__(f"APIError: {message}")


class NetworkError(RestAPIError):
    """Exception raised when a network error occurs during REST API requests."""

    def __init__(self, message="Network error during REST API request"):
        super().__init__(f"NetworkError: {message}")


class TimeoutError(RestAPIError):
    """Exception raised when a REST API request times out."""

    def __init__(self, message="REST API request timed out"):
        super().__init__(f"TimeoutError: {message}")


class JSONDecodeError(RestAPIError):
    """Exception raised when unable to decode JSON response from REST API."""

    def __init__(self, message="Failed to decode JSON response from REST API"):
        super().__init__(f"JSONDecodeError: {message}")
