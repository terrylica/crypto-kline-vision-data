#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Custom exceptions for WebSocket streaming operations.

This module defines specialized exceptions for the streaming layer.
All exceptions carry a `.details` dict (default `{}`) for machine-parseable
error context, following the same pattern as RestAPIError (GitHub #23).

CRITICAL: StreamingError inherits Exception directly (NOT ValueError).
Reason: FCP's error handler wraps ValueError → RuntimeError, which would
silently mangle streaming exceptions if they inherited ValueError.
"""

from __future__ import annotations

from typing import Any

from ckvd.utils.loguru_setup import logger


class StreamingError(Exception):
    """Base exception for all WebSocket streaming errors.

    Attributes:
        message: Human-readable error message.
        details: Machine-parseable error context (dict, default ``{}``).

    NOTE: Inherits Exception directly, NOT ValueError.
    FCP's error handler wraps ValueError → RuntimeError, so streaming
    exceptions must not inherit ValueError or they will be mangled.
    """

    def __init__(self, message: str = "Streaming error occurred", *, details: dict[str, Any] | None = None) -> None:
        """Initialize StreamingError with an error message.

        Args:
            message: Error description.
            details: Machine-parseable context (symbol, interval, url, etc.).
        """
        self.message = message
        self.details: dict[str, Any] = details or {}
        super().__init__(self.message)
        logger.error(f"StreamingError: {message}")


class StreamConnectionError(StreamingError):
    """Exception raised when WebSocket connection or handshake fails."""

    def __init__(self, message: str = "WebSocket connection failed", **kwargs: Any) -> None:
        """Initialize StreamConnectionError.

        Args:
            message: Error description.
            **kwargs: Passed to StreamingError (e.g. ``details=...``).
        """
        super().__init__(f"StreamConnectionError: {message}", **kwargs)


class StreamSubscriptionError(StreamingError):
    """Exception raised when subscribe/unsubscribe operation fails."""

    def __init__(self, message: str = "Stream subscription failed", **kwargs: Any) -> None:
        """Initialize StreamSubscriptionError.

        Args:
            message: Error description.
            **kwargs: Passed to StreamingError (e.g. ``details=...``).
        """
        super().__init__(f"StreamSubscriptionError: {message}", **kwargs)


class StreamReconnectExhaustedError(StreamingError):
    """Exception raised when maximum reconnection attempts are exceeded."""

    def __init__(self, message: str = "Stream reconnect attempts exhausted", **kwargs: Any) -> None:
        """Initialize StreamReconnectExhaustedError.

        Args:
            message: Error description.
            **kwargs: Passed to StreamingError (e.g. ``details={'attempts': N}``).
        """
        super().__init__(f"StreamReconnectExhaustedError: {message}", **kwargs)


class StreamTimeoutError(StreamingError):
    """Exception raised when WebSocket read or ping times out."""

    def __init__(self, message: str = "Stream connection timed out", **kwargs: Any) -> None:
        """Initialize StreamTimeoutError.

        Args:
            message: Error description.
            **kwargs: Passed to StreamingError (e.g. ``details=...``).
        """
        super().__init__(f"StreamTimeoutError: {message}", **kwargs)


class StreamMessageParseError(StreamingError):
    """Exception raised when JSON decoding or schema validation fails for a stream message."""

    def __init__(self, message: str = "Failed to parse stream message", **kwargs: Any) -> None:
        """Initialize StreamMessageParseError.

        Args:
            message: Error description.
            **kwargs: Passed to StreamingError (e.g. ``details={'raw': raw_bytes}``).
        """
        super().__init__(f"StreamMessageParseError: {message}", **kwargs)


class StreamBackpressureError(StreamingError):
    """Exception raised when the consumer is too slow and the queue limit is exceeded.

    This is a soft error — by default KlineStream drops newest messages when full
    (drop-newest backpressure). This exception is raised when the caller explicitly
    requests strict mode (raise_on_full=True).
    """

    def __init__(self, message: str = "Stream queue full — consumer too slow", **kwargs: Any) -> None:
        """Initialize StreamBackpressureError.

        Args:
            message: Error description.
            **kwargs: Passed to StreamingError (e.g. ``details={'queue_size': N}``).
        """
        super().__init__(f"StreamBackpressureError: {message}", **kwargs)


class StreamReconciliationError(StreamingError):
    """Exception raised when REST backfill during reconciliation fails.

    Raised by Reconciler when the fetch_fn (typically manager.get_data)
    encounters an error while backfilling gaps detected after reconnect.
    """

    def __init__(self, message: str = "Stream reconciliation failed", **kwargs: Any) -> None:  # SSoT-OK: exception default msg
        """Initialize StreamReconciliationError.

        Args:
            message: Error description.
            **kwargs: Passed to StreamingError (e.g. ``details={'symbol': ..., 'gap_intervals': N}``).
        """
        super().__init__(f"StreamReconciliationError: {message}", **kwargs)


class StreamGapDetectedError(StreamingError):
    """Exception raised when a gap is detected in the kline stream.

    Informational exception signaling that klines were missed between the
    last confirmed open_time and the current update. The Reconciler uses
    this internally; consumers may see it if reconciliation is disabled.
    """

    def __init__(self, message: str = "Gap detected in kline stream", **kwargs: Any) -> None:  # SSoT-OK: exception default msg
        """Initialize StreamGapDetectedError.

        Args:
            message: Error description.
            **kwargs: Passed to StreamingError (e.g. ``details={'gap_start': ..., 'gap_end': ...}``).
        """
        super().__init__(f"StreamGapDetectedError: {message}", **kwargs)
