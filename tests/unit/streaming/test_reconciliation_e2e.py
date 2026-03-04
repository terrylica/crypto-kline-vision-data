# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Phase 6 tests: E2E reconciliation scenarios using ChaosStreamClient.

Covers:
1. Full reconnect cycle: klines → gap → backfill → klines
2. Large gap capped at max_gap_intervals
3. Graceful degradation: REST returns empty DataFrame
4. REST backfill raises exception: stream continues
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ckvd.core.streaming.kline_stream import KlineStream
from ckvd.core.streaming.stream_config import StreamConfig
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
# E2E scenarios
# ---------------------------------------------------------------------------


class TestFullReconnectCycle:
    """Scenario 1: klines → gap → backfill → more klines."""

    @pytest.mark.asyncio
    async def test_gap_backfilled_seamlessly(self):
        """5 klines → 3h gap → 5 more klines = all 13 present."""
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=5),   # T0-T4h
            ChaosEvent(type="gap", count=3),      # skip T5h, T6h, T7h
            ChaosEvent(type="kline", count=5),   # T8h-T12h
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)

        # Backfill returns the 3 missing klines
        gap_start = _BASE_TIME + _HOUR * 5
        fetch_fn = MagicMock(return_value=_make_ohlcv_df(3, gap_start))

        config = _make_config()
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        # 5 live + 3 backfilled + 5 live = 13
        assert len(received) == 13

        # Verify chronological order
        times = [u.open_time for u in received]
        assert times == sorted(times)

        # Stats
        assert stream.reconciliation_stats.successful == 1
        assert stream.reconciliation_stats.total_backfilled == 3


class TestLargeGapCapped:
    """Scenario 3: gap of 2000 intervals capped at max_gap_intervals."""

    @pytest.mark.asyncio
    async def test_gap_capped(self):
        """Large gap should be capped; fetch_fn called with capped range."""
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=1),
            ChaosEvent(type="gap", count=2000),  # Huge gap
            ChaosEvent(type="kline", count=1),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)

        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config(reconciliation_max_gap_intervals=10)
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            _ = [u async for u in stream]

        # Verify fetch_fn was called with capped gap_end
        # detect_gap caps at prev + max_gap * interval = base + 10h
        args = fetch_fn.call_args[0]
        gap_end = args[2]
        expected_capped = _BASE_TIME + _HOUR * 10  # prev + max_gap_intervals * interval
        assert gap_end == expected_capped


class TestGracefulDegradation:
    """Scenario 5: REST returns empty DataFrame, stream continues."""

    @pytest.mark.asyncio
    async def test_empty_backfill_continues(self):
        """Empty REST response should not crash; gap remains but stream continues."""
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=3),
            ChaosEvent(type="gap", count=2),
            ChaosEvent(type="kline", count=3),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)

        fetch_fn = MagicMock(return_value=pd.DataFrame())
        config = _make_config()
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        # 3 + 0 backfilled + 3 = 6 (gap not filled)
        assert len(received) == 6
        assert stream.reconciliation_stats.successful == 1
        assert stream.reconciliation_stats.total_backfilled == 0


class TestRestBackfillException:
    """Scenario 6: REST backfill raises, stream continues."""

    @pytest.mark.asyncio
    async def test_backfill_error_continues(self):
        """REST error should be logged, not propagated; stats show failed."""
        scenario = ChaosScenario(events=[
            ChaosEvent(type="kline", count=3),
            ChaosEvent(type="gap", count=2),
            ChaosEvent(type="kline", count=3),
        ])
        client = ChaosStreamClient(scenario, base_time=_BASE_TIME)

        fetch_fn = MagicMock(side_effect=ConnectionError("network down"))
        config = _make_config()
        stream = KlineStream(config, client, fetch_fn=fetch_fn)

        async with stream:
            received = [u async for u in stream]

        assert len(received) == 6
        assert stream.reconciliation_stats.failed == 1
        assert stream.reconciliation_stats.successful == 0
