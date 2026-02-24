#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""KlineStream: async context manager and iterator for WebSocket kline data.

Main entry point for async streaming consumers. Wraps a StreamClient with:
- asyncio.Queue(maxsize=N) drop-newest backpressure
- k.x gate: confirmed_only=True filters intermediate candle updates
- Gap tracking: last_confirmed_open_time per (symbol, interval) pair
- Async context manager protocol for resource cleanup
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import TYPE_CHECKING

from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.core.streaming.stream_config import StreamConfig

if TYPE_CHECKING:
    from ckvd.core.streaming.stream_client import StreamClient


class KlineStream:
    """Async context manager and iterator for WebSocket kline updates.

    Provides backpressure-aware streaming with the k.x finalization gate.

    Usage (async):
        async with KlineStream(config, client) as stream:
            await stream.subscribe("BTCUSDT", "1h")
            async for update in stream:
                # Only finalized candles (k.x=True) by default
                print(update.close)

    Backpressure policy:
        Drop-newest: when queue is full, the incoming message is discarded.
        Old data (already queued) is preserved over new data because the
        consumer needs to catch up to its current position first.

    Gap tracking:
        last_confirmed_open_time[(symbol, interval)] → latest finalized open_time.
        On reconnect, callers can query this to determine what data to backfill.
    """

    def __init__(self, config: StreamConfig, client: StreamClient) -> None:
        """Initialize KlineStream.

        Args:
            config: Streaming configuration (compression=None, confirmed_only, etc.).
            client: Provider-specific WebSocket client implementing StreamClient.
        """
        self._config = config
        self._client = client
        self._queue: asyncio.Queue[KlineUpdate] = asyncio.Queue(maxsize=config.queue_maxsize)
        self._last_confirmed_open_time: dict[tuple[str, str], datetime] = {}
        self._dropped_count: int = 0

    # -------------------------------------------------------------------------
    # Context manager
    # -------------------------------------------------------------------------

    async def __aenter__(self) -> KlineStream:
        """Connect the underlying WebSocket client."""
        await self._client.connect()
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the underlying WebSocket client, even on exception."""
        await self._client.close()

    # -------------------------------------------------------------------------
    # Subscription
    # -------------------------------------------------------------------------

    async def subscribe(self, symbol: str, interval: str) -> None:
        """Subscribe to a kline stream for symbol+interval.

        Args:
            symbol: Trading pair (e.g. "BTCUSDT").
            interval: Candle interval string (e.g. "1h").
        """
        await self._client.subscribe(symbol, interval)

    async def unsubscribe(self, symbol: str, interval: str) -> None:
        """Unsubscribe from a kline stream.

        Args:
            symbol: Trading pair to unsubscribe.
            interval: Candle interval to unsubscribe.
        """
        await self._client.unsubscribe(symbol, interval)

    # -------------------------------------------------------------------------
    # Async iteration
    # -------------------------------------------------------------------------

    def __aiter__(self) -> AsyncGenerator[KlineUpdate, None]:
        """Return async generator of KlineUpdate events."""
        return self._consume()

    async def _consume(self) -> AsyncGenerator[KlineUpdate, None]:
        """Internal async generator — filters, queues, and yields updates."""
        async for update in self._client.messages():
            # k.x gate: skip intermediate candle updates when confirmed_only=True
            if self._config.confirmed_only and not update.is_closed:
                continue

            # Track last confirmed open_time for gap detection on reconnect
            if update.is_closed:
                key = (update.symbol, update.interval)
                prev = self._last_confirmed_open_time.get(key)
                if prev is None or update.open_time > prev:
                    self._last_confirmed_open_time[key] = update.open_time

            # Drop-newest backpressure: discard current message when queue is full
            # (preserve existing queued data; consumer must catch up)
            try:
                self._queue.put_nowait(update)
            except asyncio.QueueFull:
                self._dropped_count += 1
                continue

            # Yield from queue (ensures ordering)
            yield self._queue.get_nowait()

    # -------------------------------------------------------------------------
    # Observability
    # -------------------------------------------------------------------------

    def last_confirmed_open_time(self, symbol: str, interval: str) -> datetime | None:
        """Return the latest confirmed open_time for a symbol+interval pair.

        Used for gap detection after reconnect: if this is non-None, data from
        this timestamp onwards may need to be backfilled via FCP.

        Args:
            symbol: Trading pair.
            interval: Candle interval string.

        Returns:
            UTC datetime of the latest finalized candle, or None if no candles
            have been confirmed yet.
        """
        return self._last_confirmed_open_time.get((symbol, interval))

    @property
    def dropped_count(self) -> int:
        """Total number of messages dropped due to queue backpressure."""
        return self._dropped_count

    @property
    def queue_size(self) -> int:
        """Current number of items waiting in the queue."""
        return self._queue.qsize()
