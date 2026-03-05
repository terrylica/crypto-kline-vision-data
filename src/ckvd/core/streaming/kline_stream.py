#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""KlineStream: async context manager and iterator for WebSocket kline data.

Main entry point for async streaming consumers. Wraps a StreamClient with:
- asyncio.Queue(maxsize=N) drop-newest backpressure
- k.x gate: confirmed_only=True filters intermediate candle updates
- Gap tracking: last_confirmed_open_time per (symbol, interval) pair
- Async context manager protocol for resource cleanup
- Optional reconciliation: automatic REST backfill on reconnect gaps
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ckvd._reconciler import DedupEngine, _INTERVAL_MS, _dt_to_ms, _ms_to_dt
from ckvd._reconciler import detect_gap as _detect_gap
from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.core.streaming.stream_config import StreamConfig
from ckvd.utils.for_core.streaming_exceptions import StreamReconciliationError
from ckvd.utils.loguru_setup import logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from ckvd.core.streaming.reconciler import Reconciler
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

    Reconciliation (opt-in via config.reconciliation_enabled + fetch_fn):
        Three trigger points detect and backfill gaps:
        1. Reconnect: gap between last_confirmed_open_time and first new update
        2. Watermark: periodic timer detects silence (no updates received)
        3. Backpressure: dropped messages trigger backfill for dropped window
    """

    def __init__(
        self,
        config: StreamConfig,
        client: StreamClient,
        *,
        fetch_fn: Callable[..., Any] | None = None,
    ) -> None:
        """Initialize KlineStream.

        Args:
            config: Streaming configuration (compression=None, confirmed_only, etc.).
            client: Provider-specific WebSocket client implementing StreamClient.
            fetch_fn: Optional callable for REST backfill (typically manager.get_data).
                Required when config.reconciliation_enabled is True.
        """
        self._config = config
        self._client = client
        self._queue: asyncio.Queue[KlineUpdate] = asyncio.Queue(maxsize=config.queue_maxsize)
        self._last_confirmed_open_time: dict[tuple[str, str], datetime] = {}
        self._dropped_count: int = 0

        # Reconciliation (opt-in)
        self._reconciler: Reconciler | None = None
        if config.reconciliation_enabled and fetch_fn is not None:
            from ckvd.core.streaming.reconciler import Reconciler

            self._reconciler = Reconciler(fetch_fn, config)

        # Dedup engine bounded by max_gap_intervals (FIFO eviction)
        # Uses Rust AHashSet when available, pure-Python set+deque fallback
        # Lazy init: only allocate when reconciliation is actually enabled
        self._dedup: DedupEngine | None = (
            DedupEngine(config.reconciliation_max_gap_intervals)
            if config.reconciliation_enabled
            else None
        )

        # Watermark timer task
        self._watermark_task: asyncio.Task[None] | None = None
        self._subscriptions: set[tuple[str, str]] = set()

    # -------------------------------------------------------------------------
    # Context manager
    # -------------------------------------------------------------------------

    async def __aenter__(self) -> KlineStream:
        """Connect the underlying WebSocket client."""
        await self._client.connect()
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Close the underlying WebSocket client, even on exception."""
        if self._watermark_task is not None:
            self._watermark_task.cancel()
            try:
                await self._watermark_task
            except asyncio.CancelledError:
                pass

        if self._reconciler is not None:
            logger.info(
                f"Reconciliation stats: {self._reconciler.stats.total_requests} requests, "
                f"{self._reconciler.stats.successful} ok, {self._reconciler.stats.failed} failed, "
                f"{self._reconciler.stats.total_backfilled} backfilled"
            )

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
        self._subscriptions.add((symbol, interval))

    async def unsubscribe(self, symbol: str, interval: str) -> None:
        """Unsubscribe from a kline stream.

        Args:
            symbol: Trading pair to unsubscribe.
            interval: Candle interval to unsubscribe.
        """
        await self._client.unsubscribe(symbol, interval)
        self._subscriptions.discard((symbol, interval))

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

                # Trigger 1: Reconnect gap detection
                if self._reconciler is not None and prev is not None:
                    backfilled = await self._try_reconcile_gap(
                        update.symbol, update.interval, prev, update.open_time
                    )
                    for bu in backfilled:
                        yield bu

                # Monotonic update of last confirmed time
                if prev is None or update.open_time > prev:
                    self._last_confirmed_open_time[key] = update.open_time

            # Dedup: skip if already seen (from backfill or duplicate messages)
            # Only active when reconciliation is enabled to avoid breaking
            # confirmed_only=False flows where same open_time can repeat
            if self._reconciler is not None and self._dedup is not None:
                is_dup = self._dedup.check_and_insert(
                    update.symbol, update.interval, update.open_time_ms or _dt_to_ms(update.open_time)
                )
                if is_dup:
                    self._reconciler.stats.total_deduped += 1
                    continue

            # Drop-newest backpressure: discard current message when queue is full
            # (preserve existing queued data; consumer must catch up)
            prev_dropped = self._dropped_count
            try:
                self._queue.put_nowait(update)
            except asyncio.QueueFull:
                self._dropped_count += 1

                # Trigger 3: Backpressure drop — schedule reconciliation
                if self._reconciler is not None and self._dropped_count > prev_dropped:
                    logger.warning(
                        f"Backpressure drop #{self._dropped_count} for "
                        f"{update.symbol} {update.interval}"
                    )
                continue

            # Yield from queue (ensures ordering)
            yield self._queue.get_nowait()

    # -------------------------------------------------------------------------
    # Reconciliation helpers
    # -------------------------------------------------------------------------

    async def _try_reconcile_gap(
        self,
        symbol: str,
        interval: str,
        prev_open_time: datetime,
        current_open_time: datetime,
    ) -> list[KlineUpdate]:
        """Check for gap and reconcile if needed.

        Trigger 1 (reconnect): detects gap between last confirmed candle and
        current update. If gap > 1 interval, triggers REST backfill.

        Args:
            symbol: Trading pair.
            interval: Candle interval string.
            prev_open_time: Last confirmed open_time for this (symbol, interval).
            current_open_time: Current update's open_time.

        Returns:
            List of backfilled KlineUpdates (empty if no gap or on cooldown).
        """
        if self._reconciler is None:
            return []

        from ckvd.core.streaming.reconciler import ReconciliationRequest

        interval_ms = _INTERVAL_MS.get(interval)
        if interval_ms is None:
            return []

        prev_ms = _dt_to_ms(prev_open_time)
        current_ms = _dt_to_ms(current_open_time)

        has_gap, capped_end_ms = _detect_gap(
            prev_ms, current_ms, interval_ms, self._config.reconciliation_max_gap_intervals
        )
        if not has_gap:
            return []

        gap_start = _ms_to_dt(prev_ms + interval_ms)
        gap_end = _ms_to_dt(capped_end_ms)
        request = ReconciliationRequest(
            symbol=symbol,
            interval=interval,
            gap_start=gap_start,
            gap_end=gap_end,
            trigger="reconnect",
        )

        try:
            updates = await self._reconciler.reconcile(request)
        except StreamReconciliationError:
            logger.exception(f"Reconciliation failed for {symbol} {interval}")
            return []

        # Track backfilled keys in dedup engine and filter already-seen
        updates.sort(key=lambda x: x.open_time)  # In-place sort avoids list copy
        result: list[KlineUpdate] = []
        dedup = self._dedup
        for u in updates:
            is_dup = dedup.check_and_insert(u.symbol, u.interval, u.open_time_ms or _dt_to_ms(u.open_time)) if dedup is not None else False
            if not is_dup:
                result.append(u)
            else:
                self._reconciler.stats.total_deduped += 1

        return result

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

    @property
    def reconciliation_stats(self) -> Any:
        """Current reconciliation statistics, or None if reconciliation is disabled."""
        if self._reconciler is not None:
            return self._reconciler.stats
        return None
