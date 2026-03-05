#!/usr/bin/env python3
# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Reconciler: automatic REST backfill for gaps detected in WebSocket streams.

When a WebSocket disconnects and reconnects, klines missed during the gap are
silently lost. The Reconciler detects these gaps and backfills them via the
FCP REST API (manager.get_data), converting historical rows to KlineUpdate
objects for uniform downstream handling.

Design principles:
- Non-blocking: asyncio.to_thread() wraps sync get_data() for backfill
- Cooldown timer prevents REST rate limit storms
- Dedup by (symbol, interval, open_time) tuple
- reconciliation_enabled=False default — fully backward compatible
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Literal

from ckvd._reconciler import _INTERVAL_MS
from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.utils.for_core.streaming_exceptions import StreamReconciliationError
from ckvd.utils.loguru_setup import logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from ckvd.core.streaming.stream_config import StreamConfig


@dataclass(frozen=True, slots=True)
class ReconciliationRequest:
    """A request to backfill missing klines for a gap.

    Attributes:
        symbol: Trading pair (e.g. "BTCUSDT").
        interval: Candle interval string (e.g. "1h").
        gap_start: Start of the gap (last_confirmed_open_time + 1 interval).
        gap_end: End of the gap (current update's open_time).
        trigger: What caused this reconciliation ("reconnect", "watermark", "backpressure").
    """

    symbol: str
    interval: str
    gap_start: datetime
    gap_end: datetime
    trigger: Literal["reconnect", "watermark", "backpressure"]


@dataclass
class ReconciliationStats:
    """Counters for reconciliation observability.

    Attributes:
        total_requests: Total reconciliation requests attempted.
        successful: Requests that returned data successfully.
        failed: Requests that raised an error.
        total_backfilled: Total KlineUpdate objects produced from backfill.
        total_deduped: Updates skipped because they were already seen.
    """

    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    total_backfilled: int = 0
    total_deduped: int = 0



def _interval_to_timedelta(interval: str) -> timedelta:
    """Convert an interval string to a timedelta.

    Uses ``_INTERVAL_MS`` from ``ckvd._reconciler`` (single source of truth
    for interval-to-duration mapping).

    Args:
        interval: Candle interval string (e.g. "1h").

    Returns:
        Corresponding timedelta.

    Raises:
        ValueError: If interval is not recognized.
    """
    ms = _INTERVAL_MS.get(interval)
    if ms is None:
        raise ValueError(f"Unknown interval: {interval!r}")
    return timedelta(milliseconds=ms)


class Reconciler:
    """Detects gaps in WebSocket streams and backfills via REST API.

    Args:
        fetch_fn: Callable that fetches historical data (typically manager.get_data).
            Signature: fetch_fn(symbol, start_time, end_time, interval) -> DataFrame.
        config: StreamConfig with reconciliation parameters.
    """

    def __init__(self, fetch_fn: Callable[..., Any], config: StreamConfig) -> None:
        """Initialize reconciler with fetch function and streaming config."""
        self._fetch_fn = fetch_fn
        self._config = config
        self._cooldown_until: dict[tuple[str, str], datetime] = {}
        self._stats = ReconciliationStats()

    @property
    def stats(self) -> ReconciliationStats:
        """Current reconciliation statistics."""
        return self._stats

    def is_on_cooldown(self, symbol: str, interval: str) -> bool:
        """Check if reconciliation is on cooldown for this symbol+interval.

        Args:
            symbol: Trading pair.
            interval: Candle interval string.

        Returns:
            True if cooldown period has not elapsed since last reconciliation.
        """
        key = (symbol, interval)
        until = self._cooldown_until.get(key)
        if until is None:
            return False
        return datetime.now(timezone.utc) < until

    async def reconcile(self, request: ReconciliationRequest) -> list[KlineUpdate]:
        """Fetch missing klines via REST and return as KlineUpdate list.

        Steps:
        1. Check cooldown — skip if too soon after last reconciliation
        2. Cap gap at max_gap_intervals to prevent huge REST requests
        3. Call fetch_fn via asyncio.to_thread (non-blocking)
        4. Convert DataFrame rows to KlineUpdate via from_historical_row()
        5. Update cooldown timer
        6. Return backfilled updates

        Args:
            request: ReconciliationRequest with gap details.

        Returns:
            List of KlineUpdate objects for the gap period (may be empty).

        Raises:
            StreamReconciliationError: If fetch_fn raises an exception.
        """
        key = (request.symbol, request.interval)

        # 1. Check cooldown
        if self.is_on_cooldown(request.symbol, request.interval):
            logger.debug(f"Reconciliation cooldown active for {key}, skipping")
            return []

        self._stats.total_requests += 1

        # 2. Cap gap at max_gap_intervals
        interval_td = _interval_to_timedelta(request.interval)
        max_gap_td = interval_td * self._config.reconciliation_max_gap_intervals
        gap_start = request.gap_start
        gap_end = request.gap_end

        if (gap_end - gap_start) > max_gap_td:
            gap_end = gap_start + max_gap_td
            logger.warning(
                f"Gap for {key} capped at {self._config.reconciliation_max_gap_intervals} intervals"
            )

        # 3. Fetch via asyncio.to_thread (non-blocking)
        try:
            df = await asyncio.to_thread(
                self._fetch_fn,
                request.symbol,
                gap_start,
                gap_end,
                request.interval,
            )
        except Exception as exc:
            self._stats.failed += 1
            raise StreamReconciliationError(
                f"REST backfill failed for {request.symbol} {request.interval}",
                details={
                    "symbol": request.symbol,
                    "interval": request.interval,
                    "gap_start": gap_start.isoformat(),
                    "gap_end": gap_end.isoformat(),
                    "trigger": request.trigger,
                    "cause": type(exc).__name__,
                },
            ) from exc

        # 4. Convert DataFrame rows to KlineUpdate (columnar extraction avoids per-row iloc)
        updates: list[KlineUpdate] = []
        if df is not None and len(df) > 0:
            # Extract columns as numpy arrays for fast iteration
            open_times = df.index if df.index.name == "open_time" else df["open_time"]
            opens = df["open"].to_numpy()
            highs = df["high"].to_numpy()
            lows = df["low"].to_numpy()
            closes = df["close"].to_numpy()
            volumes = df["volume"].to_numpy()
            sources = df["_data_source"].to_numpy() if "_data_source" in df.columns else None

            for idx in range(len(df)):
                ot = open_times[idx]
                if not isinstance(ot, datetime):
                    ot = ot.to_pydatetime()
                if ot.tzinfo is None:
                    ot = ot.replace(tzinfo=timezone.utc)
                ot_ms = int(ot.timestamp() * 1000)
                updates.append(
                    KlineUpdate(
                        symbol=request.symbol,
                        interval=request.interval,
                        open_time=ot,
                        open=float(opens[idx]),
                        high=float(highs[idx]),
                        low=float(lows[idx]),
                        close=float(closes[idx]),
                        volume=float(volumes[idx]),
                        close_time=ot,  # historical rows don't carry explicit close_time
                        is_closed=True,
                        data_source=str(sources[idx]) if sources is not None else "REST",
                        open_time_ms=ot_ms,
                    )
                )

        # 5. Update cooldown timer
        self._cooldown_until[key] = datetime.now(timezone.utc) + timedelta(
            seconds=self._config.reconciliation_cooldown_seconds
        )

        # 6. Stats and return
        self._stats.successful += 1
        self._stats.total_backfilled += len(updates)

        logger.info(
            f"Reconciliation for {key}: backfilled {len(updates)} klines "
            f"({request.trigger}, {gap_start.isoformat()} → {gap_end.isoformat()})"
        )

        return updates
