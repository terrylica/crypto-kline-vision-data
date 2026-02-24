#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""BinanceStreamClient: WebSocket kline stream provider for Binance.

Implements the StreamClient protocol using websockets v16 + orjson.
Supports SPOT, FUTURES_USDT, and FUTURES_COIN market types with
correct per-market WS endpoints and stream limits.

Key design decisions:
- compression=None: Binance does not support per-message deflate.
  Omitting this (defaulting to zlib) wastes CPU decompressing nothing.
- orjson.loads: 3-10x faster than stdlib json.loads for kline payloads.
- stamina retry: production-grade exponential backoff for connect().
- Stream limits: COIN-M = 200, SPOT/USDT-M = 1024 streams/connection.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import orjson
import stamina
import websockets
from websockets.exceptions import ConnectionClosed

from ckvd.core.streaming.connection_manager import ConnectionMachine
from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.core.streaming.stream_config import StreamConfig
from ckvd.utils.for_core.streaming_exceptions import (
    StreamConnectionError,
    StreamMessageParseError,
    StreamSubscriptionError,
)
from ckvd.utils.loguru_setup import logger
from ckvd.utils.market_constraints import MarketType

# ------------------------------------------------------------------
# Per-market-type WebSocket base URLs
# ------------------------------------------------------------------
_WS_URLS: dict[MarketType, str] = {
    MarketType.SPOT: "wss://stream.binance.com:9443/ws",
    MarketType.FUTURES_USDT: "wss://fstream.binance.com/ws",
    # CRITICAL: COIN-M uses binancefuture.com (NOT binance.com)
    MarketType.FUTURES_COIN: "wss://dstream.binancefuture.com/ws",
}

# Binance-imposed stream limits per connection
_MAX_STREAMS: dict[MarketType, int] = {
    MarketType.SPOT: 1024,
    MarketType.FUTURES_USDT: 1024,
    MarketType.FUTURES_COIN: 200,  # stricter limit for COIN-M
}


def _stream_name(symbol: str, interval: str) -> str:
    """Return Binance stream name for a symbol+interval pair.

    Args:
        symbol: Trading pair (e.g. "BTCUSDT").
        interval: Candle interval string (e.g. "1h").

    Returns:
        Stream identifier (e.g. "btcusdt@kline_1h").
    """
    return f"{symbol.lower()}@kline_{interval}"


class BinanceStreamClient:
    """WebSocket kline stream client for Binance.

    Implements the StreamClient protocol. Use via KlineStream context manager:

        config = StreamConfig(market_type=MarketType.FUTURES_USDT)
        client = BinanceStreamClient(config)
        async with KlineStream(config, client) as stream:
            await stream.subscribe("BTCUSDT", "1h")
            async for update in stream:
                print(update.close)

    Attributes:
        config: Immutable StreamConfig (compression=None enforced).
        is_connected: True when WebSocket is active.
    """

    def __init__(self, config: StreamConfig) -> None:
        """Initialize BinanceStreamClient.

        Args:
            config: StreamConfig with market_type, ping settings, etc.
                    compression=None is enforced by StreamConfig validator.
        """
        self._config = config
        self._ws: websockets.ClientConnection | None = None
        self._subscriptions: set[tuple[str, str]] = set()
        self._fsm = ConnectionMachine(max_attempts=config.max_reconnect_attempts)

    # -------------------------------------------------------------------------
    # StreamClient protocol implementation
    # -------------------------------------------------------------------------

    @stamina.retry(on=StreamConnectionError, attempts=5)
    async def connect(self) -> None:
        """Establish WebSocket connection with stamina retry.

        Uses compression=None (Binance does not support per-message deflate).
        ping_interval and ping_timeout are set from StreamConfig.

        Raises:
            StreamConnectionError: After all retry attempts are exhausted.
        """
        url = _WS_URLS.get(self._config.market_type)
        if url is None:
            raise StreamConnectionError(
                f"No WebSocket URL for market type {self._config.market_type}",
                details={"market_type": str(self._config.market_type)},
            )

        self._fsm.connect()
        try:
            self._ws = await websockets.connect(
                url,
                compression=None,  # CRITICAL: Binance doesn't support deflate
                ping_interval=self._config.ping_interval,
                ping_timeout=self._config.ping_timeout,
                open_timeout=10.0,
            )
            self._fsm.handshake_ok()
            logger.debug(f"BinanceStreamClient connected to {url}")
        except (OSError, ConnectionClosed, TimeoutError) as exc:
            self._fsm.handshake_fail()
            raise StreamConnectionError(
                f"WebSocket connect failed: {exc}",
                details={"url": url, "cause": type(exc).__name__},
            ) from exc

    async def subscribe(self, symbol: str, interval: str) -> None:
        """Subscribe to a kline stream for symbol+interval.

        Sends a Binance WebSocket SUBSCRIBE request.

        Args:
            symbol: Trading pair (e.g. "BTCUSDT").
            interval: Candle interval string (e.g. "1h").

        Raises:
            StreamSubscriptionError: If the connection is not active or
                the subscription limit would be exceeded.
            StreamConnectionError: If the WebSocket is not connected.
        """
        if self._ws is None:
            raise StreamConnectionError("Not connected — call connect() first")

        max_streams = _MAX_STREAMS.get(self._config.market_type, 1024)
        if len(self._subscriptions) >= max_streams:
            raise StreamSubscriptionError(
                f"Stream limit {max_streams} reached for {self._config.market_type}",
                details={"limit": max_streams, "current": len(self._subscriptions)},
            )

        stream = _stream_name(symbol, interval)
        self._fsm.subscribe()
        try:
            payload = orjson.dumps({
                "method": "SUBSCRIBE",
                "params": [stream],
                "id": len(self._subscriptions) + 1,
            })
            await self._ws.send(payload)
            self._subscriptions.add((symbol, interval))
            self._fsm.subscribed()
            logger.debug(f"Subscribed to {stream}")
        except (OSError, ConnectionClosed) as exc:
            self._fsm.subscribe_fail()
            raise StreamSubscriptionError(
                f"Subscribe failed for {stream}: {exc}",
                details={"stream": stream, "cause": type(exc).__name__},
            ) from exc

    async def unsubscribe(self, symbol: str, interval: str) -> None:
        """Unsubscribe from a kline stream.

        Args:
            symbol: Trading pair to unsubscribe.
            interval: Candle interval to unsubscribe.
        """
        if self._ws is None:
            return
        stream = _stream_name(symbol, interval)
        try:
            payload = orjson.dumps({
                "method": "UNSUBSCRIBE",
                "params": [stream],
                "id": 0,
            })
            await self._ws.send(payload)
            self._subscriptions.discard((symbol, interval))
            logger.debug(f"Unsubscribed from {stream}")
        except (OSError, ConnectionClosed):
            pass  # Already disconnected; cleanup is best-effort

    async def messages(self) -> AsyncIterator[KlineUpdate]:  # type: ignore[override]
        """Async iterator yielding parsed KlineUpdate events.

        Parses Binance kline messages using orjson (3-10x faster than json).
        Non-kline messages (e.g. subscription ACKs) are silently skipped.

        Yields:
            KlineUpdate for each incoming kline WebSocket message.

        Raises:
            StreamConnectionError: If the WebSocket connection drops.
            StreamMessageParseError: If a message cannot be parsed.
        """
        if self._ws is None:
            raise StreamConnectionError("Not connected — call connect() first")

        try:
            async for raw_bytes in self._ws:
                try:
                    data = orjson.loads(raw_bytes)
                except orjson.JSONDecodeError as exc:
                    raise StreamMessageParseError(
                        f"orjson decode failed: {exc}",
                        details={"raw_preview": str(raw_bytes)[:100]},
                    ) from exc

                # Skip subscription ACKs and other non-kline messages
                if data.get("e") != "kline":
                    continue

                try:
                    yield KlineUpdate.from_binance_ws(data)
                except KeyError as exc:
                    raise StreamMessageParseError(
                        f"Missing field in kline payload: {exc}",
                        details={"missing_key": str(exc)},
                    ) from exc

        except ConnectionClosed as exc:
            raise StreamConnectionError(
                f"WebSocket connection closed unexpectedly: {exc}",
                details={"code": exc.rcvd.code if exc.rcvd else None},
            ) from exc

    async def close(self) -> None:
        """Gracefully close the WebSocket connection."""
        if self._ws is not None:
            try:
                await self._ws.close()
            except (OSError, ConnectionClosed, asyncio.TimeoutError):
                pass  # Best-effort close
            finally:
                self._ws = None
                self._subscriptions.clear()
                self._fsm.close()
                logger.debug("BinanceStreamClient closed")

    @property
    def is_connected(self) -> bool:
        """True if the WebSocket connection is currently active."""
        return self._ws is not None and not self._ws.closed

    @property
    def subscription_count(self) -> int:
        """Number of currently active stream subscriptions."""
        return len(self._subscriptions)

    @property
    def max_streams(self) -> int:
        """Maximum streams allowed for this market type."""
        return _MAX_STREAMS.get(self._config.market_type, 1024)
