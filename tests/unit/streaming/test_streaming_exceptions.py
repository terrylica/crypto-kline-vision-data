# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Unit tests for streaming exceptions (T18).

Covers:
- StreamingError is NOT a ValueError (FCP would wrap it → RuntimeError)
- All subclasses are catchable as StreamingError
- .details dict always present and never None
- .details defaults to {} when not provided
- .message attribute
- StreamReconnectExhaustedError .details has attempts/max_attempts
- Each exception has distinct class identity
"""

import pytest

from ckvd.utils.for_core.streaming_exceptions import (
    StreamBackpressureError,
    StreamConnectionError,
    StreamingError,
    StreamMessageParseError,
    StreamReconnectExhaustedError,
    StreamSubscriptionError,
    StreamTimeoutError,
)


class TestStreamingErrorNotValueError:
    """CRITICAL: StreamingError must NOT inherit ValueError.

    FCP wraps ValueError → RuntimeError. Streaming errors must not
    be swallowed by that path.
    """

    def test_not_value_error(self):
        assert not issubclass(StreamingError, ValueError)

    def test_is_exception(self):
        assert issubclass(StreamingError, Exception)

    def test_connection_error_not_value_error(self):
        assert not issubclass(StreamConnectionError, ValueError)

    def test_subscription_error_not_value_error(self):
        assert not issubclass(StreamSubscriptionError, ValueError)

    def test_reconnect_error_not_value_error(self):
        assert not issubclass(StreamReconnectExhaustedError, ValueError)


class TestStreamingErrorHierarchy:
    """All subclasses are catchable as StreamingError."""

    @pytest.mark.parametrize("exc_class", [
        StreamConnectionError,
        StreamSubscriptionError,
        StreamReconnectExhaustedError,
        StreamTimeoutError,
        StreamMessageParseError,
        StreamBackpressureError,
    ])
    def test_subclass_caught_as_streaming_error(self, exc_class):
        with pytest.raises(StreamingError):
            raise exc_class("test")

    def test_base_streaming_error_caught_as_exception(self):
        with pytest.raises(Exception):
            raise StreamingError("test")


class TestStreamingErrorDetails:
    """All exceptions carry .details dict — never None."""

    def test_details_default_empty_dict(self):
        exc = StreamingError("test")
        assert exc.details == {}
        assert isinstance(exc.details, dict)

    def test_details_never_none(self):
        exc = StreamingError("test", details=None)  # None → {}
        assert exc.details is not None
        assert exc.details == {}

    def test_details_preserved(self):
        exc = StreamingError("test", details={"url": "wss://example.com", "code": 1006})
        assert exc.details["url"] == "wss://example.com"
        assert exc.details["code"] == 1006

    def test_connection_error_details(self):
        exc = StreamConnectionError("connect failed", details={"cause": "OSError"})
        assert exc.details["cause"] == "OSError"

    def test_subscription_error_details(self):
        exc = StreamSubscriptionError("subscribe failed", details={"stream": "btcusdt@kline_1h"})
        assert exc.details["stream"] == "btcusdt@kline_1h"

    def test_reconnect_error_details(self):
        exc = StreamReconnectExhaustedError("exhausted", details={"attempts": 5, "max_attempts": 5})
        assert exc.details["attempts"] == 5
        assert exc.details["max_attempts"] == 5

    def test_timeout_error_details(self):
        exc = StreamTimeoutError("timeout", details={"timeout_sec": 10.0})
        assert exc.details["timeout_sec"] == 10.0

    def test_parse_error_details(self):
        exc = StreamMessageParseError("parse failed", details={"raw_preview": "not-json"})
        assert exc.details["raw_preview"] == "not-json"

    def test_backpressure_error_details(self):
        exc = StreamBackpressureError("queue full", details={"dropped": 42})
        assert exc.details["dropped"] == 42


class TestStreamingErrorMessage:
    """Exception message attribute."""

    def test_message_attribute(self):
        exc = StreamingError("connection dropped")
        assert exc.message == "connection dropped"

    def test_str_representation(self):
        exc = StreamingError("connection dropped")
        assert "connection dropped" in str(exc)

    def test_default_message_not_empty(self):
        exc = StreamingError()
        assert exc.message  # non-empty default

    def test_connection_error_str(self):
        exc = StreamConnectionError("WebSocket connect failed: OSError")
        assert "WebSocket connect failed" in str(exc)


class TestStreamingErrorDistinctClasses:
    """Each exception class has distinct identity."""

    def test_connection_vs_subscription(self):
        assert StreamConnectionError is not StreamSubscriptionError

    def test_reconnect_vs_timeout(self):
        assert StreamReconnectExhaustedError is not StreamTimeoutError

    def test_parse_vs_backpressure(self):
        assert StreamMessageParseError is not StreamBackpressureError

    def test_all_distinct(self):
        classes = [
            StreamingError,
            StreamConnectionError,
            StreamSubscriptionError,
            StreamReconnectExhaustedError,
            StreamTimeoutError,
            StreamMessageParseError,
            StreamBackpressureError,
        ]
        assert len(set(classes)) == len(classes)
