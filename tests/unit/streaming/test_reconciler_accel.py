# ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
"""Tests for Rust-accelerated reconciler components (DedupEngine + detect_gap).

Validates both backends (Rust AHashSet and Python set+deque) produce identical
results, and tests the Python bridge in _reconciler.py.
"""

from __future__ import annotations


from ckvd._reconciler import (
    BACKEND,
    DedupEngine,
    _INTERVAL_MS,
    _PythonDedupEngine,
    _dt_to_ms,
    _ms_to_dt,
    _python_detect_gap,
    detect_gap,
)


# ---------------------------------------------------------------------------
# Backend detection
# ---------------------------------------------------------------------------


class TestBackendDetection:
    """BACKEND flag reports which engine is active."""

    def test_backend_is_string(self):
        assert isinstance(BACKEND, str)

    def test_backend_is_rust_or_python(self):
        assert BACKEND in ("rust", "python")


# ---------------------------------------------------------------------------
# DedupEngine (both backends)
# ---------------------------------------------------------------------------


class TestDedupEngineInsertAndCheck:
    """check_and_insert returns True for duplicates, False for new keys."""

    def test_new_key_returns_false(self):
        engine = DedupEngine(10)
        assert engine.check_and_insert("BTCUSDT", "1h", 1000) is False

    def test_duplicate_returns_true(self):
        engine = DedupEngine(10)
        engine.check_and_insert("BTCUSDT", "1h", 1000)
        assert engine.check_and_insert("BTCUSDT", "1h", 1000) is True

    def test_different_symbol_not_duplicate(self):
        engine = DedupEngine(10)
        engine.check_and_insert("BTCUSDT", "1h", 1000)
        assert engine.check_and_insert("ETHUSDT", "1h", 1000) is False

    def test_different_interval_not_duplicate(self):
        engine = DedupEngine(10)
        engine.check_and_insert("BTCUSDT", "1h", 1000)
        assert engine.check_and_insert("BTCUSDT", "4h", 1000) is False

    def test_different_time_not_duplicate(self):
        engine = DedupEngine(10)
        engine.check_and_insert("BTCUSDT", "1h", 1000)
        assert engine.check_and_insert("BTCUSDT", "1h", 2000) is False

    def test_len_tracks_size(self):
        engine = DedupEngine(10)
        assert len(engine) == 0
        engine.check_and_insert("BTCUSDT", "1h", 1000)
        assert len(engine) == 1
        engine.check_and_insert("ETHUSDT", "1h", 1000)
        assert len(engine) == 2

    def test_duplicate_does_not_increase_len(self):
        engine = DedupEngine(10)
        engine.check_and_insert("BTCUSDT", "1h", 1000)
        engine.check_and_insert("BTCUSDT", "1h", 1000)  # dup
        assert len(engine) == 1


class TestDedupEngineFIFOEviction:
    """Bounded capacity with FIFO eviction."""

    def test_evicts_oldest_at_capacity(self):
        engine = DedupEngine(3)
        engine.check_and_insert("A", "1h", 1)
        engine.check_and_insert("B", "1h", 2)
        engine.check_and_insert("C", "1h", 3)
        # At capacity — inserting D evicts A
        engine.check_and_insert("D", "1h", 4)
        assert len(engine) == 3
        # A was evicted, so re-inserting is not a dup
        assert engine.check_and_insert("A", "1h", 1) is False

    def test_stays_bounded(self):
        engine = DedupEngine(5)
        for i in range(100):
            engine.check_and_insert("SYM", "1h", i)
        assert len(engine) == 5

    def test_contains_without_insert(self):
        engine = DedupEngine(10)
        engine.check_and_insert("BTCUSDT", "1h", 1000)
        assert engine.contains("BTCUSDT", "1h", 1000) is True
        assert engine.contains("ETHUSDT", "1h", 1000) is False

    def test_clear(self):
        engine = DedupEngine(10)
        engine.check_and_insert("BTCUSDT", "1h", 1000)
        engine.clear()
        assert len(engine) == 0
        assert engine.contains("BTCUSDT", "1h", 1000) is False


# ---------------------------------------------------------------------------
# detect_gap (both backends)
# ---------------------------------------------------------------------------

HOUR_MS = 3_600_000


class TestDetectGap:
    """Gap detection with capping."""

    def test_no_gap_consecutive(self):
        has_gap, _ = detect_gap(0, HOUR_MS, HOUR_MS, 1440)
        assert has_gap is False

    def test_no_gap_same_time(self):
        has_gap, _ = detect_gap(HOUR_MS, HOUR_MS, HOUR_MS, 1440)
        assert has_gap is False

    def test_gap_detected(self):
        # 3-hour gap with 1h interval
        has_gap, capped_end = detect_gap(0, 3 * HOUR_MS, HOUR_MS, 1440)
        assert has_gap is True
        assert capped_end == 3 * HOUR_MS

    def test_gap_capped(self):
        # 2000-interval gap, capped at 1440
        has_gap, capped_end = detect_gap(0, 2000 * HOUR_MS, HOUR_MS, 1440)
        assert has_gap is True
        assert capped_end == 1440 * HOUR_MS

    def test_two_intervals_is_gap(self):
        has_gap, _ = detect_gap(0, 2 * HOUR_MS, HOUR_MS, 1440)
        assert has_gap is True

    def test_negative_gap_no_gap(self):
        # Out-of-order (current before previous)
        has_gap, _ = detect_gap(5 * HOUR_MS, 3 * HOUR_MS, HOUR_MS, 1440)
        assert has_gap is False

    def test_small_max_gap(self):
        has_gap, capped_end = detect_gap(0, 100 * HOUR_MS, HOUR_MS, 5)
        assert has_gap is True
        assert capped_end == 5 * HOUR_MS


# ---------------------------------------------------------------------------
# Backend parity: Rust and Python produce identical results
# ---------------------------------------------------------------------------


class TestBackendParity:
    """DedupEngine and detect_gap produce same results in both backends."""

    def test_dedup_parity(self):
        """Rust and Python DedupEngine agree on all operations."""
        rust_or_active = DedupEngine(5)
        python = _PythonDedupEngine(5)

        keys = [
            ("BTCUSDT", "1h", 1000),
            ("ETHUSDT", "1h", 2000),
            ("BTCUSDT", "4h", 1000),
            ("BTCUSDT", "1h", 1000),  # duplicate
            ("SOLUSDT", "1m", 3000),
            ("ADAUSDT", "1d", 4000),
            ("XRPUSDT", "15m", 5000),  # triggers eviction
            ("BTCUSDT", "1h", 1000),  # was evicted, should be new again
        ]

        for sym, interval, ts in keys:
            r = rust_or_active.check_and_insert(sym, interval, ts)
            p = python.check_and_insert(sym, interval, ts)
            assert r == p, f"Mismatch for ({sym}, {interval}, {ts}): active={r}, python={p}"

        assert len(rust_or_active) == len(python)

    def test_detect_gap_parity(self):
        """Rust and Python detect_gap agree on all cases."""
        cases = [
            (0, HOUR_MS, HOUR_MS, 1440),  # consecutive
            (0, 3 * HOUR_MS, HOUR_MS, 1440),  # gap
            (0, 2000 * HOUR_MS, HOUR_MS, 1440),  # capped gap
            (5 * HOUR_MS, 3 * HOUR_MS, HOUR_MS, 1440),  # out-of-order
            (0, 2 * HOUR_MS, HOUR_MS, 10),  # small max_gap
            (0, 100 * HOUR_MS, HOUR_MS, 5),  # capped at 5
        ]

        for prev, curr, interval, max_gap in cases:
            r = detect_gap(prev, curr, interval, max_gap)
            p = _python_detect_gap(prev, curr, interval, max_gap)
            assert r == p, f"Mismatch for ({prev}, {curr}, {interval}, {max_gap}): active={r}, python={p}"


# ---------------------------------------------------------------------------
# Interval mapping
# ---------------------------------------------------------------------------


class TestIntervalMapping:
    """_INTERVAL_MS covers all expected intervals."""

    EXPECTED_INTERVALS = [
        "1s", "1m", "3m", "5m", "15m", "30m",
        "1h", "2h", "4h", "6h", "8h", "12h",
        "1d", "3d", "1w", "1M",
    ]

    def test_all_intervals_present(self):
        for interval in self.EXPECTED_INTERVALS:
            assert interval in _INTERVAL_MS, f"Missing interval: {interval}"

    def test_interval_values_positive(self):
        for interval, ms in _INTERVAL_MS.items():
            assert ms > 0, f"{interval} has non-positive ms: {ms}"

    def test_1h_is_3600000(self):
        assert _INTERVAL_MS["1h"] == 3_600_000

    def test_1d_is_86400000(self):
        assert _INTERVAL_MS["1d"] == 86_400_000

    def test_1m_is_60000(self):
        assert _INTERVAL_MS["1m"] == 60_000


# ---------------------------------------------------------------------------
# Datetime conversion helpers
# ---------------------------------------------------------------------------


class TestDatetimeConversion:
    """_dt_to_ms and _ms_to_dt are inverses."""

    def test_roundtrip(self):
        from datetime import datetime, timezone

        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        ms = _dt_to_ms(dt)
        assert _ms_to_dt(ms) == dt

    def test_epoch_zero(self):
        from datetime import datetime, timezone

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        assert _dt_to_ms(epoch) == 0

    def test_ms_to_dt_utc(self):
        from datetime import timezone

        dt = _ms_to_dt(0)
        assert dt.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# KlineStream integration with DedupEngine
# ---------------------------------------------------------------------------


class TestKlineStreamDedupIntegration:
    """KlineStream uses DedupEngine instead of set+deque."""

    def test_stream_has_dedup_attribute(self):
        from unittest.mock import MagicMock

        from ckvd.core.streaming.kline_stream import KlineStream
        from ckvd.core.streaming.stream_config import StreamConfig
        from ckvd.utils.market_constraints import MarketType

        config = StreamConfig(market_type=MarketType.FUTURES_USDT)
        client = MagicMock()
        stream = KlineStream(config, client)
        assert hasattr(stream, "_dedup")
        assert not hasattr(stream, "_seen_keys")

    def test_dedup_engine_type(self):
        from unittest.mock import MagicMock

        from ckvd.core.streaming.kline_stream import KlineStream
        from ckvd.core.streaming.stream_config import StreamConfig
        from ckvd.utils.market_constraints import MarketType

        config = StreamConfig(market_type=MarketType.FUTURES_USDT)
        client = MagicMock()
        stream = KlineStream(config, client)
        # Should be an instance of DedupEngine (Rust or Python)
        assert hasattr(stream._dedup, "check_and_insert")
        assert hasattr(stream._dedup, "contains")
        assert hasattr(stream._dedup, "clear")
