#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""StreamClient: Protocol for WebSocket kline stream providers.

Follows the same structural pattern as DataClientInterface
(ckvd.core.providers.binance.data_client_interface) but as a
runtime_checkable Protocol for duck-typing provider implementations.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

from ckvd.core.streaming.kline_update import KlineUpdate


@runtime_checkable
class StreamClient(Protocol):
    """Protocol for WebSocket kline stream providers.

    Any object that implements these methods satisfies the protocol
    (structural subtyping / duck-typing). Use ``isinstance(obj, StreamClient)``
    at runtime to check conformance.

    Implementations:
        BinanceStreamClient — ckvd.core.providers.binance.binance_stream_client
    """

    async def connect(self) -> None:
        """Establish the WebSocket connection.

        Should be called before subscribe(). Raises StreamConnectionError on failure.
        """
        ...

    async def subscribe(self, symbol: str, interval: str) -> None:
        """Subscribe to the kline stream for a symbol + interval.

        Args:
            symbol: Trading pair (e.g. "BTCUSDT").
            interval: Candle interval string (e.g. "1h").

        Raises:
            StreamSubscriptionError: If subscription fails.
        """
        ...

    async def unsubscribe(self, symbol: str, interval: str) -> None:
        """Unsubscribe from a kline stream.

        Args:
            symbol: Trading pair to unsubscribe.
            interval: Candle interval to unsubscribe.
        """
        ...

    def messages(self) -> AsyncIterator[KlineUpdate]:
        """Async iterator yielding parsed KlineUpdate events.

        Yields:
            KlineUpdate for each incoming WebSocket message.

        Raises:
            StreamConnectionError: If connection drops unexpectedly.
            StreamMessageParseError: If a message cannot be parsed.
        """
        ...

    async def close(self) -> None:
        """Gracefully close the WebSocket connection and clean up resources."""
        ...

    @property
    def is_connected(self) -> bool:
        """True if the WebSocket connection is currently active."""
        ...
