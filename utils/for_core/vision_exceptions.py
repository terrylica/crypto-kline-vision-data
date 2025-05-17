#!/usr/bin/env python3
"""
Custom exceptions for Binance Vision API related operations.

This module defines specialized exceptions to provide more precise error handling
for the Binance Vision API component. By using custom exceptions, calling code can
catch specific error types and handle them appropriately.
"""

from utils.logger_setup import logger


class VisionAPIError(Exception):
    """Base exception for all Vision API related errors."""

    def __init__(self, message="Binance Vision API error occurred"):
        self.message = message
        super().__init__(self.message)
        logger.error(f"VisionAPIError: {message}")


class UnsupportedIntervalError(ValueError):
    """Exception raised when an interval is not supported by a market type."""

    def __init__(self, message="The specified interval is not supported by this market type"):
        self.message = message
        super().__init__(self.message)
        logger.error(f"UnsupportedIntervalError: {message}")


class DataFreshnessError(VisionAPIError):
    """Exception raised when data is too fresh for Vision API."""

    def __init__(self, message="Data is too fresh for Vision API"):
        super().__init__(f"DataFreshnessError: {message}")


class ChecksumVerificationError(VisionAPIError):
    """Exception raised when checksum verification fails."""

    def __init__(self, message="Checksum verification failed"):
        super().__init__(f"ChecksumVerificationError: {message}")


class DownloadFailedError(VisionAPIError):
    """Exception raised when file download fails."""

    def __init__(self, message="Failed to download file from Vision API"):
        super().__init__(f"DownloadFailedError: {message}")
