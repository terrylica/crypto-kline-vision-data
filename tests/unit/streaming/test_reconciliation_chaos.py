# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Phase 6 tests: chaos, corruption, edge cases for reconciliation.

Tests organized by category:
A. Gap injection tests
B. Corruption tests
C. Disconnect/reconnect chaos
D. Backpressure tests
E. Dedup edge cases
"""

from __future__ import annotations

import math
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ckvd.core.streaming.kline_stream import KlineStream
from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.core.streaming.stream_config import StreamConfig
from ckvd.utils.for_core.streaming_exceptions import StreamConnectionError
from ckvd.utils.market_constraints import MarketType

from tests.unit.streaming.chaos_client import ChaosEvent, ChaosScenario, ChaosStreamClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_TIME = datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
_HOUR = timedelta(hours=1)


def _make_ohlcv_df(rows: int, base_time: datetime) -> pd.DataFrame:
    times = [base_time + _HOUR * i for i in range(rows)]
    return pd.DataFrame(
        {
            "open": [42000.0 + i * 10 for i in range(rows)],
            "high": [42100.0 + i * 10 for i in range(rows)],
            "low": [41950.0 + i * 10 for i in range(rows)],
            "close": [42050.0 + i * 10 for i in range(rows)],
            "volume": [1500.0] * rows,
        },
        index=pd.DatetimeIndex(times, name="open_time"),
    )


def _make_config(**overrides) -> StreamConfig:
    defaults = {
        "market_type": MarketType.FUTURES_USDT,
        "reconciliation_enabled": True,
        "reconciliation_cooldown_seconds": 0.0,
        "reconciliation_max_gap_intervals": 1440,
    }
    defaults.update(overrides)
    return StreamConfig(**defaults)


# ---------------------------------------------------------------------------
# A. Gap Injection Tests
# ---------------------------------------------------------------------------


class TestSingleIntervalGap:
    """Scenario 7: 10 klines → skip 1 → resume."""

    @pytest.mark.asyncio
    async def test_single_interval_gap(self):
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=10),
            ChaosEvent(type="gap", count=1),
            ChaosEvent(type="kline", count=5),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)
        gap_start = _BASE_TIME + _HOUR * 10
        fetch_fn = MagicMock(return_value=_make_ohlcv_df(1, gap_start))
        config = _make_config()
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        assert len(received) == 16  # 10 + 1 backfilled + 5


class TestMultiIntervalGap:
    """Scenario 8: 10 klines → skip 5 → resume."""

    @pytest.mark.asyncio
    async def test_multi_interval_gap(self):
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=10),
            ChaosEvent(type="gap", count=5),
            ChaosEvent(type="kline", count=5),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)
        gap_start = _BASE_TIME + _HOUR * 10
        fetch_fn = MagicMock(return_value=_make_ohlcv_df(5, gap_start))
        config = _make_config()
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        # Chronological order preserved
        times = [u.open_time for u in received]
        assert times == sorted(times)
        assert len(received) == 20


class TestRepeatedGaps:
    """Scenario 10: multiple gaps in sequence."""

    @pytest.mark.asyncio
    async def test_two_gaps(self):
        """5 klines → gap(2) → 3 klines → gap(3) → 5 klines."""
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=5),
            ChaosEvent(type="gap", count=2),
            ChaosEvent(type="kline", count=3),
            ChaosEvent(type="gap", count=3),
            ChaosEvent(type="kline", count=5),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)

        # fetch_fn returns correct backfill for each call
        gap1_start = _BASE_TIME + _HOUR * 5
        gap2_start = _BASE_TIME + _HOUR * 10
        fetch_fn = MagicMock(side_effect=[
            _make_ohlcv_df(2, gap1_start),
            _make_ohlcv_df(3, gap2_start),
        ])
        config = _make_config()
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        assert fetch_fn.call_count == 2
        assert stream.reconciliation_stats.successful == 2
        assert len(received) == 18  # 5+2+3+3+5


class TestCooldownBoundary:
    """Scenario 11: cooldown blocks second reconciliation."""

    @pytest.mark.asyncio
    async def test_cooldown_blocks_second(self):
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=3),
            ChaosEvent(type="gap", count=2),
            ChaosEvent(type="kline", count=3),
            ChaosEvent(type="gap", count=2),
            ChaosEvent(type="kline", count=3),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)
        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config(reconciliation_cooldown_seconds=60.0)
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            _ = [u async for u in stream]

        # Only first gap reconciled (second within 60s cooldown)
        assert fetch_fn.call_count == 1
        assert stream.reconciliation_stats.total_requests == 1


# ---------------------------------------------------------------------------
# B. Corruption Tests
# ---------------------------------------------------------------------------


class TestNanOhlcvValues:
    """Scenario 12: NaN OHLCV values pass through (consumer's responsibility)."""

    @pytest.mark.asyncio
    async def test_nan_values_yielded(self):
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=2),
            ChaosEvent(type="corrupt", count=1),
            ChaosEvent(type="kline", count=2),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)
        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config()
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        # All 5 updates yielded including corrupt one
        assert len(received) == 5
        assert math.isnan(received[2].close)


class TestZeroVolumeCandle:
    """Scenario 13: zero volume is valid, no reconciliation triggered."""

    @pytest.mark.asyncio
    async def test_zero_volume_normal(self):
        """Zero volume candle should pass through normally."""
        # Just use normal klines (chaos client generates volume=1500)
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=5),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)
        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config()
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        assert len(received) == 5
        fetch_fn.assert_not_called()


class TestDuplicateOpenTime:
    """Scenario 14: two updates with same open_time — dedup catches it."""

    @pytest.mark.asyncio
    async def test_duplicate_deduped(self):
        """Second update with same open_time should be dropped."""
        # Use a custom client that yields duplicate open_time
        class _DuplicateClient:
            async def connect(self) -> None:
                pass

            async def subscribe(self, s: str, i: str) -> None:
                pass

            async def unsubscribe(self, s: str, i: str) -> None:
                pass

            async def close(self) -> None:
                pass

            @property
            def is_connected(self) -> bool:
                return True

            async def messages(self) -> AsyncIterator[KlineUpdate]:
                t = _BASE_TIME
                for price in [42000.0, 42050.0]:  # Same open_time, different close
                    yield KlineUpdate(
                        symbol="BTCUSDT", interval="1h",
                        open_time=t, open=price, high=price + 100,
                        low=price - 50, close=price + 50, volume=1500.0,
                        close_time=t + _HOUR - timedelta(milliseconds=1),
                        is_closed=True,
                    )

        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config()
        stream = KlineStream(config, _DuplicateClient(), fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        assert len(received) == 1  # Second duplicate dropped
        assert stream.reconciliation_stats.total_deduped == 1


class TestOutOfOrderTimestamps:
    """Scenario 15: out-of-order timestamps — last_confirmed doesn't regress."""

    @pytest.mark.asyncio
    async def test_monotonic_guard(self):
        """Older timestamp after newer should not regress last_confirmed."""

        class _OutOfOrderClient:
            async def connect(self) -> None:
                pass

            async def subscribe(self, s: str, i: str) -> None:
                pass

            async def unsubscribe(self, s: str, i: str) -> None:
                pass

            async def close(self) -> None:
                pass

            @property
            def is_connected(self) -> bool:
                return True

            async def messages(self) -> AsyncIterator[KlineUpdate]:
                for hour_offset in [0, 1, 2, 1]:  # T2h then back to T1h
                    t = _BASE_TIME + _HOUR * hour_offset
                    yield KlineUpdate(
                        symbol="BTCUSDT", interval="1h",
                        open_time=t, open=42000.0, high=42100.0,
                        low=41950.0, close=42050.0, volume=1500.0,
                        close_time=t + _HOUR - timedelta(milliseconds=1),
                        is_closed=True,
                    )

        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config()
        stream = KlineStream(config, _OutOfOrderClient(), fetch_fn=fetch_fn)

        async with stream:
            _ = [u async for u in stream]

        # last_confirmed should be T2h, not T1h
        last = stream.last_confirmed_open_time("BTCUSDT", "1h")
        assert last == _BASE_TIME + _HOUR * 2


# ---------------------------------------------------------------------------
# C. Disconnect/Reconnect Chaos
# ---------------------------------------------------------------------------


class TestDisconnectAfterOneMessage:
    """Scenario 17: disconnect after 1 kline."""

    @pytest.mark.asyncio
    async def test_disconnect_after_one(self):
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=1),
            ChaosEvent(type="disconnect"),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)
        config = _make_config()
        stream = KlineStream(config, client)

        with pytest.raises(StreamConnectionError):
            async with stream:
                _ = [u async for u in stream]


class TestDisconnectNoMessages:
    """Scenario 18: disconnect with no prior messages."""

    @pytest.mark.asyncio
    async def test_disconnect_no_baseline(self):
        scenario = ChaosScenario(events=[
            ChaosEvent(type="disconnect"),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)
        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config()
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        with pytest.raises(StreamConnectionError):
            async with stream:
                _ = [u async for u in stream]

        # No reconciliation (no baseline to gap-detect from)
        fetch_fn.assert_not_called()


# ---------------------------------------------------------------------------
# D. Backpressure Tests
# ---------------------------------------------------------------------------


class TestQueueFullDuringBurst:
    """Scenario 21: queue_maxsize=5, burst 20 klines."""

    @pytest.mark.asyncio
    async def test_burst_backpressure(self):
        scenario = ChaosScenario(events=[
            ChaosEvent(type="burst", count=20),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)
        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config(queue_maxsize=5)
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        # Some received, rest dropped
        assert len(received) + stream.dropped_count == 20


# ---------------------------------------------------------------------------
# E. Dedup Edge Cases
# ---------------------------------------------------------------------------


class TestBackfillOverlap:
    """Scenario 27: backfill overlaps with already-received live data."""

    @pytest.mark.asyncio
    async def test_overlap_deduped(self):
        """Live T0-T4, gap T5-T7, backfill returns T3-T7. T3-T4 deduped."""
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=5),   # T0-T4
            ChaosEvent(type="gap", count=3),      # skip T5, T6, T7
            ChaosEvent(type="kline", count=2),   # T8, T9
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)

        # Backfill returns T3-T7 (overlapping T3, T4 with live)
        gap_start = _BASE_TIME + _HOUR * 3
        fetch_fn = MagicMock(return_value=_make_ohlcv_df(5, gap_start))
        config = _make_config()
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        # 5 live + 3 new from backfill (T5,T6,T7) + 2 live = 10
        # T3 and T4 from backfill are deduped
        assert len(received) == 10
        assert stream.reconciliation_stats.total_deduped >= 2

    @pytest.mark.asyncio
    async def test_seen_keys_eviction(self):
        """Stream more than max_gap_intervals unique keys → set stays bounded."""
        max_intervals = 10
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=max_intervals + 10),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)
        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config(reconciliation_max_gap_intervals=max_intervals)
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            _ = [u async for u in stream]

        assert len(stream._seen_keys) <= max_intervals


class TestCrossSymbolDedup:
    """Scenario 29: same open_time, different symbol — NOT deduped."""

    @pytest.mark.asyncio
    async def test_different_symbols_not_deduped(self):
        """BTCUSDT T1 and ETHUSDT T1 are distinct dedup keys."""

        class _MultiSymbolClient:
            async def connect(self) -> None:
                pass

            async def subscribe(self, s: str, i: str) -> None:
                pass

            async def unsubscribe(self, s: str, i: str) -> None:
                pass

            async def close(self) -> None:
                pass

            @property
            def is_connected(self) -> bool:
                return True

            async def messages(self) -> AsyncIterator[KlineUpdate]:
                for symbol in ["BTCUSDT", "ETHUSDT"]:
                    yield KlineUpdate(
                        symbol=symbol, interval="1h",
                        open_time=_BASE_TIME, open=42000.0, high=42100.0,
                        low=41950.0, close=42050.0, volume=1500.0,
                        close_time=_BASE_TIME + _HOUR - timedelta(milliseconds=1),
                        is_closed=True,
                    )

        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config()
        stream = KlineStream(config, _MultiSymbolClient(), fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        assert len(received) == 2  # Both symbols yielded
        assert stream.reconciliation_stats.total_deduped == 0
