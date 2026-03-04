# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Phase 2 tests: Reconciler with non-blocking REST backfill.

Covers:
- reconcile() calls fetch_fn with correct args via asyncio.to_thread
- Gap capped at max_gap_intervals when too large
- Cooldown prevents repeated reconciliation within window
- is_on_cooldown() returns correct state
- Empty DataFrame from fetch_fn returns empty list
- from_historical_row() conversion produces valid KlineUpdates
- ReconciliationRequest is frozen and has correct fields
- Stats track reconciliation count, success, failure
- Error in fetch_fn raises StreamReconciliationError with details
- _interval_to_timedelta converts known intervals
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ckvd.core.streaming.reconciler import (
    ReconciliationRequest,
    ReconciliationStats,
    Reconciler,
    _interval_to_timedelta,
)
from ckvd.core.streaming.stream_config import StreamConfig
from ckvd.utils.for_core.streaming_exceptions import StreamReconciliationError
from ckvd.utils.market_constraints import MarketType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> StreamConfig:
    """Build a StreamConfig with reconciliation enabled."""
    defaults = {
        "market_type": MarketType.FUTURES_USDT,
        "reconciliation_enabled": True,
        "reconciliation_cooldown_seconds": 30.0,
        "reconciliation_max_gap_intervals": 1440,
    }
    defaults.update(overrides)
    return StreamConfig(**defaults)


def _make_request(
    *,
    symbol: str = "BTCUSDT",  # SSoT-OK: test fixture default
    interval: str = "1h",  # SSoT-OK: test fixture default
    gap_hours: int = 3,
    trigger: str = "reconnect",  # SSoT-OK: test fixture default
) -> ReconciliationRequest:
    """Build a ReconciliationRequest with sensible defaults."""
    gap_start = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
    gap_end = gap_start + timedelta(hours=gap_hours)
    return ReconciliationRequest(
        symbol=symbol,
        interval=interval,
        gap_start=gap_start,
        gap_end=gap_end,
        trigger=trigger,
    )


def _make_ohlcv_df(rows: int = 3, base_time: datetime | None = None) -> pd.DataFrame:
    """Create a sample OHLCV DataFrame matching CKVD output format."""
    if base_time is None:
        base_time = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
    times = [base_time + timedelta(hours=i) for i in range(rows)]
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


# ---------------------------------------------------------------------------
# ReconciliationRequest tests
# ---------------------------------------------------------------------------


class TestReconciliationRequest:
    """Tests for ReconciliationRequest dataclass."""

    def test_frozen(self):
        """ReconciliationRequest should be immutable."""
        req = _make_request()
        with pytest.raises(AttributeError):
            req.symbol = "ETHUSDT"  # type: ignore[misc]

    def test_fields_correct(self):
        """All fields should be accessible."""
        req = _make_request(symbol="ETHUSDT", interval="5m", trigger="watermark")
        assert req.symbol == "ETHUSDT"
        assert req.interval == "5m"
        assert req.trigger == "watermark"
        assert isinstance(req.gap_start, datetime)
        assert isinstance(req.gap_end, datetime)

    def test_gap_end_after_start(self):
        """Gap end should be after gap start."""
        req = _make_request(gap_hours=5)
        assert req.gap_end > req.gap_start


# ---------------------------------------------------------------------------
# ReconciliationStats tests
# ---------------------------------------------------------------------------


class TestReconciliationStats:
    """Tests for ReconciliationStats."""

    def test_defaults_zero(self):
        """All counters should start at 0."""
        stats = ReconciliationStats()
        assert stats.total_requests == 0
        assert stats.successful == 0
        assert stats.failed == 0
        assert stats.total_backfilled == 0
        assert stats.total_deduped == 0

    def test_mutable(self):
        """Stats should be mutable (not frozen)."""
        stats = ReconciliationStats()
        stats.total_requests = 5
        assert stats.total_requests == 5


# ---------------------------------------------------------------------------
# _interval_to_timedelta tests
# ---------------------------------------------------------------------------


class TestIntervalToTimedelta:
    """Tests for _interval_to_timedelta."""

    @pytest.mark.parametrize(
        ("interval", "expected"),
        [
            ("1m", timedelta(minutes=1)),
            ("5m", timedelta(minutes=5)),
            ("1h", timedelta(hours=1)),
            ("4h", timedelta(hours=4)),
            ("1d", timedelta(days=1)),
        ],
    )
    def test_known_intervals(self, interval: str, expected: timedelta):
        """Known intervals should convert correctly."""
        assert _interval_to_timedelta(interval) == expected

    def test_unknown_interval_raises(self):
        """Unknown interval should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown interval"):
            _interval_to_timedelta("2w")


# ---------------------------------------------------------------------------
# Reconciler tests
# ---------------------------------------------------------------------------


class TestReconciler:
    """Tests for Reconciler core logic."""

    @pytest.mark.asyncio
    async def test_reconcile_calls_fetch_fn(self):
        """reconcile() should call fetch_fn with correct arguments."""
        mock_fetch = MagicMock(return_value=_make_ohlcv_df(3))
        config = _make_config()
        reconciler = Reconciler(mock_fetch, config)
        request = _make_request(gap_hours=3)

        await reconciler.reconcile(request)

        mock_fetch.assert_called_once()
        args = mock_fetch.call_args[0]
        assert args[0] == "BTCUSDT"
        assert args[1] == request.gap_start
        assert args[2] == request.gap_end
        assert args[3] == "1h"

    @pytest.mark.asyncio
    async def test_reconcile_returns_kline_updates(self):
        """reconcile() should convert DataFrame rows to KlineUpdates."""
        mock_fetch = MagicMock(return_value=_make_ohlcv_df(3))
        config = _make_config()
        reconciler = Reconciler(mock_fetch, config)
        request = _make_request(gap_hours=3)

        updates = await reconciler.reconcile(request)

        assert len(updates) == 3
        for u in updates:
            assert u.symbol == "BTCUSDT"
            assert u.interval == "1h"
            assert u.is_closed is True

    @pytest.mark.asyncio
    async def test_reconcile_empty_dataframe(self):
        """Empty DataFrame should return empty list."""
        mock_fetch = MagicMock(return_value=pd.DataFrame())
        config = _make_config()
        reconciler = Reconciler(mock_fetch, config)
        request = _make_request()

        updates = await reconciler.reconcile(request)

        assert updates == []

    @pytest.mark.asyncio
    async def test_reconcile_none_dataframe(self):
        """None DataFrame should return empty list."""
        mock_fetch = MagicMock(return_value=None)
        config = _make_config()
        reconciler = Reconciler(mock_fetch, config)
        request = _make_request()

        updates = await reconciler.reconcile(request)

        assert updates == []

    @pytest.mark.asyncio
    async def test_gap_capped_at_max_intervals(self):
        """Large gap should be capped at max_gap_intervals."""
        mock_fetch = MagicMock(return_value=pd.DataFrame())
        config = _make_config(reconciliation_max_gap_intervals=10)
        reconciler = Reconciler(mock_fetch, config)

        # Request a 100-hour gap (100 intervals for 1h), but cap is 10
        request = _make_request(gap_hours=100)
        await reconciler.reconcile(request)

        args = mock_fetch.call_args[0]
        gap_end_called = args[2]
        expected_capped_end = request.gap_start + timedelta(hours=10)
        assert gap_end_called == expected_capped_end

    @pytest.mark.asyncio
    async def test_cooldown_prevents_repeat(self):
        """Cooldown should prevent repeated reconciliation."""
        mock_fetch = MagicMock(return_value=_make_ohlcv_df(1))
        config = _make_config(reconciliation_cooldown_seconds=60.0)
        reconciler = Reconciler(mock_fetch, config)
        request = _make_request()

        # First call succeeds
        result1 = await reconciler.reconcile(request)
        assert len(result1) == 1

        # Second call within cooldown returns empty
        result2 = await reconciler.reconcile(request)
        assert result2 == []
        assert mock_fetch.call_count == 1  # Only called once

    @pytest.mark.asyncio
    async def test_is_on_cooldown_state(self):
        """is_on_cooldown() should reflect cooldown state."""
        mock_fetch = MagicMock(return_value=pd.DataFrame())
        config = _make_config(reconciliation_cooldown_seconds=60.0)
        reconciler = Reconciler(mock_fetch, config)

        assert reconciler.is_on_cooldown("BTCUSDT", "1h") is False

        await reconciler.reconcile(_make_request())

        assert reconciler.is_on_cooldown("BTCUSDT", "1h") is True

    @pytest.mark.asyncio
    async def test_cooldown_per_symbol_interval(self):
        """Cooldown should be per (symbol, interval) pair."""
        mock_fetch = MagicMock(return_value=pd.DataFrame())
        config = _make_config(reconciliation_cooldown_seconds=60.0)
        reconciler = Reconciler(mock_fetch, config)

        await reconciler.reconcile(_make_request(symbol="BTCUSDT", interval="1h"))

        assert reconciler.is_on_cooldown("BTCUSDT", "1h") is True
        assert reconciler.is_on_cooldown("ETHUSDT", "1h") is False
        assert reconciler.is_on_cooldown("BTCUSDT", "5m") is False

    @pytest.mark.asyncio
    async def test_stats_success(self):
        """Stats should track successful reconciliation."""
        mock_fetch = MagicMock(return_value=_make_ohlcv_df(5))
        config = _make_config(reconciliation_cooldown_seconds=0.0)
        reconciler = Reconciler(mock_fetch, config)

        await reconciler.reconcile(_make_request())

        assert reconciler.stats.total_requests == 1
        assert reconciler.stats.successful == 1
        assert reconciler.stats.failed == 0
        assert reconciler.stats.total_backfilled == 5

    @pytest.mark.asyncio
    async def test_stats_failure(self):
        """Stats should track failed reconciliation."""
        mock_fetch = MagicMock(side_effect=RuntimeError("REST timeout"))
        config = _make_config()
        reconciler = Reconciler(mock_fetch, config)

        with pytest.raises(StreamReconciliationError):
            await reconciler.reconcile(_make_request())

        assert reconciler.stats.total_requests == 1
        assert reconciler.stats.successful == 0
        assert reconciler.stats.failed == 1

    @pytest.mark.asyncio
    async def test_fetch_error_raises_reconciliation_error(self):
        """fetch_fn exception should be wrapped in StreamReconciliationError."""
        mock_fetch = MagicMock(side_effect=ConnectionError("network down"))
        config = _make_config()
        reconciler = Reconciler(mock_fetch, config)

        with pytest.raises(StreamReconciliationError) as exc_info:
            await reconciler.reconcile(_make_request())

        assert exc_info.value.details["symbol"] == "BTCUSDT"
        assert exc_info.value.details["cause"] == "ConnectionError"
        assert exc_info.value.details["trigger"] == "reconnect"

    @pytest.mark.asyncio
    async def test_zero_cooldown_allows_repeat(self):
        """Cooldown of 0 seconds should allow immediate repeat."""
        mock_fetch = MagicMock(return_value=_make_ohlcv_df(1))
        config = _make_config(reconciliation_cooldown_seconds=0.0)
        reconciler = Reconciler(mock_fetch, config)
        request = _make_request()

        await reconciler.reconcile(request)
        await reconciler.reconcile(request)

        assert mock_fetch.call_count == 2
        assert reconciler.stats.total_requests == 2
