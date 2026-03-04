# ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""Phase 1 tests: reconciliation foundation — exceptions, config fields, dedup_key.

Covers:
- StreamReconciliationError and StreamGapDetectedError exist and inherit StreamingError
- Exception .details dict works correctly
- StreamConfig new reconciliation fields have correct defaults
- StreamConfig with reconciliation fields is still frozen/immutable
- KlineUpdate.dedup_key() returns correct tuple
- dedup_key() is consistent for identical updates
- dedup_key() differs for different symbols/intervals/times
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ckvd.core.streaming.kline_update import KlineUpdate
from ckvd.core.streaming.stream_config import StreamConfig
from ckvd.utils.for_core.streaming_exceptions import (
    StreamGapDetectedError,
    StreamReconciliationError,
    StreamingError,
)
from ckvd.utils.market_constraints import MarketType


# ---------------------------------------------------------------------------
# Exception tests
# ---------------------------------------------------------------------------


class TestStreamReconciliationError:
    """Tests for StreamReconciliationError."""

    def test_inherits_streaming_error(self):
        """StreamReconciliationError should be a subclass of StreamingError."""
        assert issubclass(StreamReconciliationError, StreamingError)

    def test_default_message(self):
        """Default message should mention reconciliation."""
        exc = StreamReconciliationError()
        assert "reconciliation" in str(exc).lower()

    def test_custom_message(self):
        """Custom message is preserved."""
        exc = StreamReconciliationError("REST backfill timeout")
        assert "REST backfill timeout" in str(exc)

    def test_details_dict_default_empty(self):
        """Details should default to empty dict, never None."""
        exc = StreamReconciliationError()
        assert exc.details == {}
        assert isinstance(exc.details, dict)

    def test_details_dict_with_values(self):
        """Details should carry machine-parseable context."""
        details = {"symbol": "BTCUSDT", "gap_intervals": 5, "trigger": "reconnect"}
        exc = StreamReconciliationError("backfill failed", details=details)
        assert exc.details["symbol"] == "BTCUSDT"
        assert exc.details["gap_intervals"] == 5

    def test_catchable_as_streaming_error(self):
        """Should be catchable as StreamingError."""
        with pytest.raises(StreamingError):
            raise StreamReconciliationError("test")


class TestStreamGapDetectedError:
    """Tests for StreamGapDetectedError."""

    def test_inherits_streaming_error(self):
        """StreamGapDetectedError should be a subclass of StreamingError."""
        assert issubclass(StreamGapDetectedError, StreamingError)

    def test_default_message(self):
        """Default message should mention gap."""
        exc = StreamGapDetectedError()
        assert "gap" in str(exc).lower()

    def test_details_dict_default_empty(self):
        """Details should default to empty dict."""
        exc = StreamGapDetectedError()
        assert exc.details == {}

    def test_details_dict_with_gap_info(self):
        """Details carry gap start/end for machine parsing."""
        gap_start = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
        gap_end = datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)
        exc = StreamGapDetectedError(
            "3h gap detected",
            details={"gap_start": gap_start.isoformat(), "gap_end": gap_end.isoformat(), "gap_intervals": 3},
        )
        assert exc.details["gap_intervals"] == 3

    def test_not_value_error(self):
        """StreamGapDetectedError must NOT inherit ValueError (FCP safety)."""
        assert not issubclass(StreamGapDetectedError, ValueError)


# ---------------------------------------------------------------------------
# StreamConfig reconciliation fields
# ---------------------------------------------------------------------------


class TestStreamConfigReconciliation:
    """Tests for StreamConfig reconciliation fields."""

    def test_default_reconciliation_disabled(self):
        """Reconciliation should be disabled by default (backward compat)."""
        config = StreamConfig(market_type=MarketType.FUTURES_USDT)
        assert config.reconciliation_enabled is False

    def test_default_watermark_factor(self):
        """Default watermark factor should be 2.0."""
        config = StreamConfig(market_type=MarketType.FUTURES_USDT)
        assert config.reconciliation_watermark_factor == 2.0

    def test_default_max_gap_intervals(self):
        """Default max gap intervals should be 1440 (1 day of 1m candles)."""
        config = StreamConfig(market_type=MarketType.FUTURES_USDT)
        assert config.reconciliation_max_gap_intervals == 1440

    def test_default_cooldown_seconds(self):
        """Default cooldown should be 30 seconds."""
        config = StreamConfig(market_type=MarketType.FUTURES_USDT)
        assert config.reconciliation_cooldown_seconds == 30.0

    def test_reconciliation_enabled_explicit(self):
        """Can explicitly enable reconciliation."""
        config = StreamConfig(
            market_type=MarketType.FUTURES_USDT,
            reconciliation_enabled=True,
        )
        assert config.reconciliation_enabled is True

    def test_custom_reconciliation_values(self):
        """All reconciliation fields can be customized."""
        config = StreamConfig(
            market_type=MarketType.FUTURES_USDT,
            reconciliation_enabled=True,
            reconciliation_watermark_factor=3.0,
            reconciliation_max_gap_intervals=720,
            reconciliation_cooldown_seconds=60.0,
        )
        assert config.reconciliation_watermark_factor == 3.0
        assert config.reconciliation_max_gap_intervals == 720
        assert config.reconciliation_cooldown_seconds == 60.0

    def test_frozen_immutability(self):
        """StreamConfig should still be frozen with new fields."""
        config = StreamConfig(market_type=MarketType.FUTURES_USDT, reconciliation_enabled=True)
        with pytest.raises(AttributeError):
            config.reconciliation_enabled = False  # type: ignore[misc]

    def test_invalid_watermark_factor_wrong_type(self):
        """Watermark factor must be a number."""
        with pytest.raises(TypeError):
            StreamConfig(market_type=MarketType.FUTURES_USDT, reconciliation_watermark_factor="fast")  # type: ignore[arg-type]

    def test_invalid_max_gap_intervals_wrong_type(self):
        """Max gap intervals must be an int."""
        with pytest.raises(TypeError):
            StreamConfig(market_type=MarketType.FUTURES_USDT, reconciliation_max_gap_intervals=1.5)  # type: ignore[arg-type]

    def test_cooldown_zero_allowed(self):
        """Cooldown of 0 is valid (no cooldown)."""
        config = StreamConfig(market_type=MarketType.FUTURES_USDT, reconciliation_cooldown_seconds=0.0)
        assert config.reconciliation_cooldown_seconds == 0.0


# ---------------------------------------------------------------------------
# KlineUpdate.dedup_key()
# ---------------------------------------------------------------------------


class TestDedupKey:
    """Tests for KlineUpdate.dedup_key()."""

    def _make_update(
        self,
        *,
        symbol: str = "BTCUSDT",
        interval: str = "1h",
        open_time_ms: int = 1_700_000_000_000,
    ) -> KlineUpdate:
        """Build a KlineUpdate for testing."""
        return KlineUpdate(
            symbol=symbol,
            interval=interval,
            open_time=datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc),
            open=36500.0,
            high=37000.0,
            low=36400.0,
            close=36800.0,
            volume=1500.0,
            close_time=datetime.fromtimestamp((open_time_ms + 3_599_999) / 1000, tz=timezone.utc),
            is_closed=True,
        )

    def test_returns_tuple(self):
        """dedup_key() should return a 3-tuple."""
        update = self._make_update()
        key = update.dedup_key()
        assert isinstance(key, tuple)
        assert len(key) == 3

    def test_correct_values(self):
        """dedup_key() should contain (symbol, interval, open_time)."""
        update = self._make_update(symbol="ETHUSDT", interval="5m", open_time_ms=1_700_000_000_000)
        key = update.dedup_key()
        assert key[0] == "ETHUSDT"
        assert key[1] == "5m"
        assert key[2] == datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)

    def test_consistent_for_identical_updates(self):
        """Two identical KlineUpdates should have the same dedup_key."""
        u1 = self._make_update()
        u2 = self._make_update()
        assert u1.dedup_key() == u2.dedup_key()

    def test_differs_for_different_symbol(self):
        """Different symbols should produce different dedup_keys."""
        u1 = self._make_update(symbol="BTCUSDT")
        u2 = self._make_update(symbol="ETHUSDT")
        assert u1.dedup_key() != u2.dedup_key()

    def test_differs_for_different_interval(self):
        """Different intervals should produce different dedup_keys."""
        u1 = self._make_update(interval="1h")
        u2 = self._make_update(interval="5m")
        assert u1.dedup_key() != u2.dedup_key()

    def test_differs_for_different_open_time(self):
        """Different open_times should produce different dedup_keys."""
        u1 = self._make_update(open_time_ms=1_700_000_000_000)
        u2 = self._make_update(open_time_ms=1_700_003_600_000)
        assert u1.dedup_key() != u2.dedup_key()

    def test_hashable_for_set_use(self):
        """dedup_key() should be usable as a set/dict key."""
        u1 = self._make_update()
        u2 = self._make_update(symbol="ETHUSDT")
        keys = {u1.dedup_key(), u2.dedup_key()}
        assert len(keys) == 2

    def test_same_open_time_different_close_same_key(self):
        """Updates with same (symbol, interval, open_time) but different close prices share a key."""
        u1 = KlineUpdate(
            symbol="BTCUSDT", interval="1h",
            open_time=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
            open=36500.0, high=37000.0, low=36400.0, close=36800.0, volume=1500.0,
            close_time=datetime(2024, 1, 15, 12, 59, 59, tzinfo=timezone.utc),
            is_closed=True,
        )
        u2 = KlineUpdate(
            symbol="BTCUSDT", interval="1h",
            open_time=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
            open=36500.0, high=37000.0, low=36400.0, close=37200.0, volume=2000.0,
            close_time=datetime(2024, 1, 15, 12, 59, 59, tzinfo=timezone.utc),
            is_closed=True,
        )
        assert u1.dedup_key() == u2.dedup_key()
