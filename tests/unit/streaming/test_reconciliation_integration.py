# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Phase 3 tests: KlineStream reconciliation integration.

Covers:
- Reconnect gap triggers reconciliation (mock fetch_fn)
- Backfilled updates are yielded in chronological order
- Duplicate updates are deduplicated via dedup_key()
- No reconciliation when reconciliation_enabled=False (backward compat)
- No reconciliation when fetch_fn=None
- Cooldown prevents rapid-fire reconciliation
- _seen_keys bounded at max_gap_intervals entries
- Backpressure drop triggers reconciliation scheduling
- Reconciliation error logged but doesn't crash stream
- reconciliation_stats property exposed
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ckvd.core.streaming.kline_stream import KlineStream
from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.core.streaming.stream_config import StreamConfig
from ckvd.utils.market_constraints import MarketType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_MS = 1_700_000_000_000  # 2023-11-14T22:13:20 UTC
_HOUR_MS = 3_600_000


def _make_update(
    *,
    symbol: str = "BTCUSDT",  # SSoT-OK: test fixture default
    interval: str = "1h",  # SSoT-OK: test fixture default
    is_closed: bool = True,
    open_time_ms: int = _BASE_MS,
    close: float = 36800.0,
) -> KlineUpdate:
    """Build a KlineUpdate for testing."""
    return KlineUpdate(
        symbol=symbol,
        interval=interval,
        open_time=datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc),
        open=36500.0,
        high=37000.0,
        low=36400.0,
        close=close,
        volume=1500.0,
        close_time=datetime.fromtimestamp((open_time_ms + _HOUR_MS - 1) / 1000, tz=timezone.utc),
        is_closed=is_closed,
    )


def _make_config(*, reconciliation_enabled: bool = True, **overrides) -> StreamConfig:
    """Build a StreamConfig with reconciliation enabled by default."""
    defaults = {
        "market_type": MarketType.FUTURES_USDT,
        "reconciliation_enabled": reconciliation_enabled,
        "reconciliation_cooldown_seconds": 0.0,
        "reconciliation_max_gap_intervals": 1440,
        "queue_maxsize": 1000,
    }
    defaults.update(overrides)
    return StreamConfig(**defaults)


class _FakeClient:
    """Minimal StreamClient that yields a predetermined sequence."""

    def __init__(self, updates: list[KlineUpdate]) -> None:
        self._updates = updates
        self._connected = False

    async def connect(self) -> None:
        self._connected = True

    async def subscribe(self, symbol: str, interval: str) -> None:
        pass

    async def unsubscribe(self, symbol: str, interval: str) -> None:
        pass

    async def messages(self) -> AsyncIterator[KlineUpdate]:
        for u in self._updates:
            yield u

    async def close(self) -> None:
        self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected


def _make_ohlcv_df(
    rows: int, base_time: datetime, interval_td: timedelta = timedelta(hours=1)
) -> pd.DataFrame:
    """Create a sample OHLCV DataFrame matching CKVD output format."""
    times = [base_time + interval_td * i for i in range(rows)]
    return pd.DataFrame(
        {
            "open": [36500.0 + i * 100 for i in range(rows)],
            "high": [37000.0 + i * 100 for i in range(rows)],
            "low": [36400.0 + i * 100 for i in range(rows)],
            "close": [36800.0 + i * 100 for i in range(rows)],
            "volume": [1500.0] * rows,
        },
        index=pd.DatetimeIndex(times, name="open_time"),
    )


def _make_stream(
    updates: list[KlineUpdate],
    *,
    reconciliation_enabled: bool = True,
    fetch_fn: MagicMock | None = None,
    **config_overrides,
) -> KlineStream:
    """Build a KlineStream with a fake client."""
    config = _make_config(reconciliation_enabled=reconciliation_enabled, **config_overrides)
    client = _FakeClient(updates)
    return KlineStream(config, client, fetch_fn=fetch_fn)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestReconciliationDisabled:
    """Backward compatibility: no reconciliation when disabled."""

    @pytest.mark.asyncio
    async def test_no_reconciliation_when_disabled(self):
        """reconciliation_enabled=False should not trigger any backfill."""
        # Gap of 3 hours
        updates = [
            _make_update(open_time_ms=_BASE_MS),
            _make_update(open_time_ms=_BASE_MS + 4 * _HOUR_MS),  # 4h gap
        ]
        fetch_fn = MagicMock(return_value=pd.DataFrame())
        stream = _make_stream(updates, reconciliation_enabled=False, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        assert len(received) == 2
        fetch_fn.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_reconciliation_when_no_fetch_fn(self):
        """No fetch_fn should not trigger any backfill even if enabled."""
        updates = [
            _make_update(open_time_ms=_BASE_MS),
            _make_update(open_time_ms=_BASE_MS + 4 * _HOUR_MS),
        ]
        stream = _make_stream(updates, reconciliation_enabled=True, fetch_fn=None)

        async with stream:
            received = [u async for u in stream]

        assert len(received) == 2


class TestReconnectGapTrigger:
    """Trigger 1: gap detection on reconnect."""

    @pytest.mark.asyncio
    async def test_gap_triggers_reconciliation(self):
        """3-hour gap between candles should trigger REST backfill."""
        # Candle at T0, then candle at T0+4h (gap of T0+1h, T0+2h, T0+3h)
        updates = [
            _make_update(open_time_ms=_BASE_MS),
            _make_update(open_time_ms=_BASE_MS + 4 * _HOUR_MS),
        ]
        gap_start = datetime.fromtimestamp((_BASE_MS + _HOUR_MS) / 1000, tz=timezone.utc)
        backfill_df = _make_ohlcv_df(3, gap_start)
        fetch_fn = MagicMock(return_value=backfill_df)

        stream = _make_stream(updates, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        # 1 live + 3 backfilled + 1 live = 5
        assert len(received) == 5
        fetch_fn.assert_called_once()

    @pytest.mark.asyncio
    async def test_backfilled_updates_in_chronological_order(self):
        """Backfilled updates should appear in chronological order before the live update."""
        updates = [
            _make_update(open_time_ms=_BASE_MS),
            _make_update(open_time_ms=_BASE_MS + 3 * _HOUR_MS),
        ]
        gap_start = datetime.fromtimestamp((_BASE_MS + _HOUR_MS) / 1000, tz=timezone.utc)
        backfill_df = _make_ohlcv_df(2, gap_start)
        fetch_fn = MagicMock(return_value=backfill_df)

        stream = _make_stream(updates, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        # First candle, then 2 backfilled, then last candle
        times = [u.open_time for u in received]
        assert times == sorted(times), f"Not chronological: {times}"

    @pytest.mark.asyncio
    async def test_no_gap_no_reconciliation(self):
        """Consecutive candles (1h apart) should not trigger reconciliation."""
        updates = [
            _make_update(open_time_ms=_BASE_MS),
            _make_update(open_time_ms=_BASE_MS + _HOUR_MS),
            _make_update(open_time_ms=_BASE_MS + 2 * _HOUR_MS),
        ]
        fetch_fn = MagicMock(return_value=pd.DataFrame())

        stream = _make_stream(updates, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        assert len(received) == 3
        fetch_fn.assert_not_called()


class TestDedupMerge:
    """Deduplication of backfilled updates with live stream."""

    @pytest.mark.asyncio
    async def test_duplicate_deduped(self):
        """Backfill overlapping with live data should be deduplicated."""
        # Live: T0, T4h  (gap at T1h, T2h, T3h)
        updates = [
            _make_update(open_time_ms=_BASE_MS),
            _make_update(open_time_ms=_BASE_MS + 4 * _HOUR_MS),
        ]
        # Backfill returns T0-T3h (overlaps with T0 live)
        gap_start = datetime.fromtimestamp(_BASE_MS / 1000, tz=timezone.utc)
        backfill_df = _make_ohlcv_df(4, gap_start)
        fetch_fn = MagicMock(return_value=backfill_df)

        stream = _make_stream(updates, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        # T0 live + T1h,T2h,T3h backfilled (T0 backfill deduped) + T4h live = 5
        assert len(received) == 5

        # Verify dedup stat
        assert stream.reconciliation_stats is not None
        assert stream.reconciliation_stats.total_deduped >= 1

    @pytest.mark.asyncio
    async def test_seen_keys_bounded(self):
        """_seen_keys should not grow beyond max_gap_intervals."""
        # Create many unique candles that exceed max_gap_intervals
        max_intervals = 10
        updates = [
            _make_update(open_time_ms=_BASE_MS + i * _HOUR_MS) for i in range(max_intervals + 5)
        ]
        fetch_fn = MagicMock(return_value=pd.DataFrame())

        stream = _make_stream(
            updates,
            fetch_fn=fetch_fn,
            reconciliation_max_gap_intervals=max_intervals,
        )

        async with stream:
            _ = [u async for u in stream]

        # Dedup engine should be bounded
        assert len(stream._dedup) <= max_intervals


class TestCooldownIntegration:
    """Cooldown prevents rapid-fire reconciliation."""

    @pytest.mark.asyncio
    async def test_cooldown_limits_reconciliation(self):
        """Only the first gap should trigger reconciliation within cooldown window."""
        # Two gaps for same symbol: T0→T3h, T3h→T6h
        updates = [
            _make_update(open_time_ms=_BASE_MS),
            _make_update(open_time_ms=_BASE_MS + 3 * _HOUR_MS),
            _make_update(open_time_ms=_BASE_MS + 6 * _HOUR_MS),
        ]
        fetch_fn = MagicMock(return_value=pd.DataFrame())

        stream = _make_stream(
            updates,
            fetch_fn=fetch_fn,
            reconciliation_cooldown_seconds=60.0,  # 60s cooldown
        )

        async with stream:
            _ = [u async for u in stream]

        # Only first gap reconciled; second blocked by cooldown
        assert fetch_fn.call_count == 1


class TestReconciliationErrorHandling:
    """Reconciliation errors should not crash the stream."""

    @pytest.mark.asyncio
    async def test_fetch_error_does_not_crash(self):
        """Reconciliation error should be logged, not propagated."""
        updates = [
            _make_update(open_time_ms=_BASE_MS),
            _make_update(open_time_ms=_BASE_MS + 4 * _HOUR_MS),
        ]
        fetch_fn = MagicMock(side_effect=RuntimeError("REST timeout"))

        stream = _make_stream(updates, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        # Both live updates should still be yielded despite reconciliation failure
        assert len(received) == 2
        assert stream.reconciliation_stats.failed == 1


class TestReconciliationStatsProperty:
    """reconciliation_stats property on KlineStream."""

    @pytest.mark.asyncio
    async def test_stats_none_when_disabled(self):
        """Stats should be None when reconciliation is disabled."""
        stream = _make_stream([], reconciliation_enabled=False)
        assert stream.reconciliation_stats is None

    @pytest.mark.asyncio
    async def test_stats_available_when_enabled(self):
        """Stats should be available when reconciliation is enabled."""
        fetch_fn = MagicMock(return_value=pd.DataFrame())
        stream = _make_stream([], fetch_fn=fetch_fn, reconciliation_enabled=True)
        assert stream.reconciliation_stats is not None
        assert stream.reconciliation_stats.total_requests == 0

    @pytest.mark.asyncio
    async def test_stats_zero_with_no_gaps(self):
        """Stats should be zero when no gaps detected."""
        updates = [
            _make_update(open_time_ms=_BASE_MS + i * _HOUR_MS) for i in range(5)
        ]
        fetch_fn = MagicMock(return_value=pd.DataFrame())
        stream = _make_stream(updates, fetch_fn=fetch_fn)

        async with stream:
            _ = [u async for u in stream]

        assert stream.reconciliation_stats.total_requests == 0
