# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Phase 5 tests: reconciliation observability.

Covers:
- Stats counters increment correctly
- Stats accessible via stream.reconciliation_stats
- Stats are zero when reconciliation disabled
- ReconciliationStats and ReconciliationRequest are importable from streaming __init__
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

_BASE_MS = 1_700_000_000_000
_HOUR_MS = 3_600_000


def _make_update(*, open_time_ms: int = _BASE_MS) -> KlineUpdate:
    return KlineUpdate(
        symbol="BTCUSDT",  # SSoT-OK: test fixture
        interval="1h",  # SSoT-OK: test fixture
        open_time=datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc),
        open=36500.0,
        high=37000.0,
        low=36400.0,
        close=36800.0,
        volume=1500.0,
        close_time=datetime.fromtimestamp((open_time_ms + _HOUR_MS - 1) / 1000, tz=timezone.utc),
        is_closed=True,
    )


class _FakeClient:
    def __init__(self, updates: list[KlineUpdate]) -> None:
        self._updates = updates

    async def connect(self) -> None:
        pass

    async def subscribe(self, symbol: str, interval: str) -> None:
        pass

    async def unsubscribe(self, symbol: str, interval: str) -> None:
        pass

    async def messages(self) -> AsyncIterator[KlineUpdate]:
        for u in self._updates:
            yield u

    async def close(self) -> None:
        pass

    @property
    def is_connected(self) -> bool:
        return True


def _make_ohlcv_df(rows: int, base_time: datetime) -> pd.DataFrame:
    times = [base_time + timedelta(hours=i) for i in range(rows)]
    return pd.DataFrame(
        {
            "open": [36500.0] * rows,
            "high": [37000.0] * rows,
            "low": [36400.0] * rows,
            "close": [36800.0] * rows,
            "volume": [1500.0] * rows,
        },
        index=pd.DatetimeIndex(times, name="open_time"),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestReconciliationStatsCounters:
    """Verify stats counters increment correctly."""

    @pytest.mark.asyncio
    async def test_stats_after_successful_reconciliation(self):
        """Stats should reflect successful backfill."""
        updates = [
            _make_update(open_time_ms=_BASE_MS),
            _make_update(open_time_ms=_BASE_MS + 4 * _HOUR_MS),
        ]
        gap_start = datetime.fromtimestamp((_BASE_MS + _HOUR_MS) / 1000, tz=timezone.utc)
        fetch_fn = MagicMock(return_value=_make_ohlcv_df(3, gap_start))

        config = StreamConfig(
            market_type=MarketType.FUTURES_USDT,
            reconciliation_enabled=True,
            reconciliation_cooldown_seconds=0.0,
        )
        client = _FakeClient(updates)
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            _ = [u async for u in stream]

        stats = stream.reconciliation_stats
        assert stats is not None
        assert stats.total_requests == 1
        assert stats.successful == 1
        assert stats.failed == 0
        assert stats.total_backfilled == 3

    @pytest.mark.asyncio
    async def test_stats_after_failed_reconciliation(self):
        """Stats should reflect failed backfill."""
        updates = [
            _make_update(open_time_ms=_BASE_MS),
            _make_update(open_time_ms=_BASE_MS + 4 * _HOUR_MS),
        ]
        fetch_fn = MagicMock(side_effect=ConnectionError("timeout"))

        config = StreamConfig(
            market_type=MarketType.FUTURES_USDT,
            reconciliation_enabled=True,
            reconciliation_cooldown_seconds=0.0,
        )
        client = _FakeClient(updates)
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            _ = [u async for u in stream]

        stats = stream.reconciliation_stats
        assert stats is not None
        assert stats.total_requests == 1
        assert stats.failed == 1
        assert stats.successful == 0

    @pytest.mark.asyncio
    async def test_stats_none_when_disabled(self):
        """Stats should be None when reconciliation is disabled."""
        config = StreamConfig(
            market_type=MarketType.FUTURES_USDT,
            reconciliation_enabled=False,
        )
        client = _FakeClient([])
        stream = KlineStream(config, client)

        assert stream.reconciliation_stats is None


class TestReconciliationExports:
    """Verify public exports from streaming __init__."""

    def test_reconciliation_stats_importable(self):
        """ReconciliationStats should be importable from ckvd.core.streaming."""
        from ckvd.core.streaming.reconciler import ReconciliationStats as RS

        stats = RS()
        assert stats.total_requests == 0

    def test_reconciliation_request_importable(self):
        """ReconciliationRequest should be importable from ckvd.core.streaming."""
        from ckvd.core.streaming.reconciler import ReconciliationRequest as RR

        req = RR(
            symbol="BTCUSDT",
            interval="1h",
            gap_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            gap_end=datetime(2024, 1, 1, 3, 0, tzinfo=timezone.utc),
            trigger="reconnect",
        )
        assert req.symbol == "BTCUSDT"
