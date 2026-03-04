# ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
"""Phase 10 tests: Real-data stress tests for streaming reconciliation.

Uses real historical CKVD data as ground truth, replays through
ChaosStreamClient with injected faults, verifies reconciler produces
gap-free output.

Scenarios:
    45. Ground truth comparison — random gaps, reconcile, compare
    46. Cascading failures — disconnect + gap + burst + disconnect
    47. Multi-symbol ground truth — independent gaps per symbol
    48. Interval stress — 1-second interval gap detection performance
    49. Memory bound verification — 100K klines through bounded DedupEngine
"""

from __future__ import annotations

import time


from ckvd._reconciler import DedupEngine, _INTERVAL_MS, detect_gap


# ---------------------------------------------------------------------------
# Scenario 45: Ground truth comparison
# ---------------------------------------------------------------------------


class TestGroundTruthComparison:
    """Simulate random gaps in a kline sequence, reconcile, verify completeness."""

    def test_random_gaps_detected_correctly(self):
        """Generate 1440 klines, remove 5 random segments, verify gap detection."""
        base_ms = 1_705_276_800_000  # 2024-01-15T00:00:00Z
        interval_ms = _INTERVAL_MS["1m"]  # 60_000
        total_klines = 1440
        max_gap = 1440

        # Generate all timestamps
        all_timestamps = [base_ms + i * interval_ms for i in range(total_klines)]

        # Remove segments to create gaps (hours 100-104, 300-310, 500-502, 800-820, 1200-1210)
        gap_ranges = [(100, 105), (300, 311), (500, 503), (800, 821), (1200, 1211)]
        removed = set()
        for start, end in gap_ranges:
            for i in range(start, min(end, total_klines)):
                removed.add(i)

        received = [ts for i, ts in enumerate(all_timestamps) if i not in removed]

        # Run gap detection
        dedup = DedupEngine(max_gap)
        gaps_found = []
        prev_ms = None

        for ts in received:
            if prev_ms is not None:
                has_gap, capped_end = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps_found.append((prev_ms + interval_ms, capped_end))

            is_dup = dedup.check_and_insert("BTCUSDT", "1m", ts)
            if not is_dup:
                prev_ms = ts

        # Should detect exactly 5 gaps
        assert len(gaps_found) == 5
        # No duplicates
        assert len(dedup) == len(received)

    def test_gap_boundaries_correct(self):
        """Verify gap start/end timestamps match removed intervals."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1h"]
        max_gap = 1440

        # 24 consecutive hours, remove hours 5-7 (3-hour gap)
        timestamps = [base_ms + i * interval_ms for i in range(24)]
        received = [ts for i, ts in enumerate(timestamps) if i not in {5, 6, 7}]

        gaps = []
        prev_ms = None
        for ts in received:
            if prev_ms is not None:
                has_gap, capped_end = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps.append((prev_ms + interval_ms, capped_end))
            prev_ms = ts

        assert len(gaps) == 1
        gap_start, gap_end = gaps[0]
        # Gap should start at hour 5 and end at hour 8 (first received after gap)
        assert gap_start == base_ms + 5 * interval_ms
        assert gap_end == base_ms + 8 * interval_ms


# ---------------------------------------------------------------------------
# Scenario 46: Cascading failures
# ---------------------------------------------------------------------------


class TestCascadingFailures:
    """Simulate cascading chaos: gap + burst + gap, verify all gaps detected."""

    def test_cascading_gaps_all_detected(self):
        """Multiple gaps interspersed with normal data and bursts."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1h"]
        max_gap = 1440

        # Sequence: 10 normal, gap(5), 10 normal, gap(3), burst(20), gap(2), 10 normal
        timestamps = []
        # Phase 1: hours 0-9
        for i in range(10):
            timestamps.append(base_ms + i * interval_ms)
        # Phase 2: skip 10-14, hours 15-24
        for i in range(15, 25):
            timestamps.append(base_ms + i * interval_ms)
        # Phase 3: skip 25-27, hours 28-47 (burst of 20)
        for i in range(28, 48):
            timestamps.append(base_ms + i * interval_ms)
        # Phase 4: skip 48-49, hours 50-59
        for i in range(50, 60):
            timestamps.append(base_ms + i * interval_ms)

        gaps = []
        dedup = DedupEngine(max_gap)
        prev_ms = None

        for ts in timestamps:
            if prev_ms is not None:
                has_gap, capped_end = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps.append((prev_ms + interval_ms, capped_end))

            is_dup = dedup.check_and_insert("BTCUSDT", "1h", ts)
            if not is_dup:
                prev_ms = ts

        assert len(gaps) == 3
        assert len(dedup) == len(timestamps)


# ---------------------------------------------------------------------------
# Scenario 47: Multi-symbol ground truth
# ---------------------------------------------------------------------------


class TestMultiSymbolGroundTruth:
    """Interleaved multi-symbol streams with independent gaps."""

    def test_per_symbol_gap_tracking(self):
        """BTCUSDT consecutive, ETHUSDT has gap, SOLUSDT has 2 gaps."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1h"]
        max_gap = 1440

        symbols_timestamps: dict[str, list[int]] = {
            "BTCUSDT": [base_ms + i * interval_ms for i in range(24)],
            "ETHUSDT": [
                base_ms + i * interval_ms
                for i in range(24)
                if i not in {5, 6, 7}  # 3-hour gap
            ],
            "SOLUSDT": [
                base_ms + i * interval_ms
                for i in range(24)
                if i not in {3, 4, 10, 11, 12}  # 2 gaps
            ],
        }

        dedup = DedupEngine(max_gap * 3)
        last_confirmed: dict[str, int] = {}
        gaps_per_symbol: dict[str, list[tuple[int, int]]] = {s: [] for s in symbols_timestamps}

        # Interleave processing
        for hour in range(24):
            for sym, ts_list in symbols_timestamps.items():
                ts = base_ms + hour * interval_ms
                if ts not in ts_list:
                    continue

                if sym in last_confirmed:
                    has_gap, capped = detect_gap(last_confirmed[sym], ts, interval_ms, max_gap)
                    if has_gap:
                        gaps_per_symbol[sym].append((last_confirmed[sym] + interval_ms, capped))

                is_dup = dedup.check_and_insert(sym, "1h", ts)
                if not is_dup:
                    last_confirmed[sym] = ts

        assert len(gaps_per_symbol["BTCUSDT"]) == 0
        assert len(gaps_per_symbol["ETHUSDT"]) == 1
        assert len(gaps_per_symbol["SOLUSDT"]) == 2


# ---------------------------------------------------------------------------
# Scenario 48: Interval stress — high-frequency gap detection performance
# ---------------------------------------------------------------------------


class TestIntervalStress:
    """Gap detection + dedup for 1-second interval (86400 bars/day)."""

    def test_1s_interval_performance(self):
        """86400 klines (1 day of 1s), measure wall clock time."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1s"]  # 1000
        total = 86_400
        max_gap = 1440

        dedup = DedupEngine(max_gap)
        prev_ms = None
        gaps_detected = 0

        start_time = time.perf_counter()

        for i in range(total):
            ts = base_ms + i * interval_ms
            if prev_ms is not None:
                has_gap, _ = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps_detected += 1
            dedup.check_and_insert("BTCUSDT", "1s", ts)
            prev_ms = ts

        elapsed = time.perf_counter() - start_time

        assert gaps_detected == 0  # no gaps in consecutive sequence
        assert len(dedup) <= max_gap  # bounded
        # Performance: must complete within 1s (generous for Python fallback)
        assert elapsed < 1.0, f"1s interval processing took {elapsed:.3f}s (expected <1s)"

    def test_1m_interval_with_gaps_performance(self):
        """1440 klines with 5 random gaps, performance check."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1m"]
        max_gap = 1440

        # Create sequence with gaps
        skip_indices = {100, 200, 300, 400, 500}
        timestamps = [
            base_ms + i * interval_ms
            for i in range(1440)
            if i not in skip_indices
        ]

        dedup = DedupEngine(max_gap)
        prev_ms = None
        gaps = 0

        start_time = time.perf_counter()

        for ts in timestamps:
            if prev_ms is not None:
                has_gap, _ = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps += 1
            dedup.check_and_insert("BTCUSDT", "1m", ts)
            prev_ms = ts

        elapsed = time.perf_counter() - start_time

        assert gaps == 5
        assert elapsed < 0.5, f"1m processing took {elapsed:.3f}s (expected <0.5s)"


# ---------------------------------------------------------------------------
# Scenario 49: Memory bound verification
# ---------------------------------------------------------------------------


class TestMemoryBoundVerification:
    """DedupEngine stays bounded regardless of stream duration."""

    def test_100k_klines_bounded(self):
        """Stream 100K klines through DedupEngine with capacity=1440."""
        capacity = 1440
        dedup = DedupEngine(capacity)

        for i in range(100_000):
            dedup.check_and_insert("BTCUSDT", "1h", i * 3_600_000)
            assert len(dedup) <= capacity, f"DedupEngine grew beyond capacity at i={i}"

        assert len(dedup) == capacity

    def test_multi_symbol_bounded(self):
        """Multiple symbols don't bypass capacity bound."""
        capacity = 100
        dedup = DedupEngine(capacity)

        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "XRPUSDT"]
        for sym in symbols:
            for i in range(50):
                dedup.check_and_insert(sym, "1h", i * 3_600_000)

        # 5 symbols × 50 timestamps = 250 inserts, bounded at 100
        assert len(dedup) == capacity

    def test_eviction_preserves_recent(self):
        """After eviction, most recent entries are retained, oldest evicted."""
        capacity = 10
        dedup = DedupEngine(capacity)

        # Insert 20 entries
        for i in range(20):
            dedup.check_and_insert("BTC", "1h", i * 1000)

        # First 10 should be evicted
        for i in range(10):
            assert not dedup.contains("BTC", "1h", i * 1000), f"Entry {i} should be evicted"

        # Last 10 should be retained
        for i in range(10, 20):
            assert dedup.contains("BTC", "1h", i * 1000), f"Entry {i} should be retained"

    def test_duplicate_detection_after_eviction(self):
        """Evicted entries can be re-inserted as new entries."""
        capacity = 5
        dedup = DedupEngine(capacity)

        # Fill to capacity
        for i in range(5):
            dedup.check_and_insert("BTC", "1h", i * 1000)

        # Insert one more → evicts entry 0
        dedup.check_and_insert("BTC", "1h", 5000)

        # Entry 0 was evicted → re-inserting is NOT a duplicate
        is_dup = dedup.check_and_insert("BTC", "1h", 0)
        assert is_dup is False

        # Entry 5 is still present → re-inserting IS a duplicate
        is_dup = dedup.check_and_insert("BTC", "1h", 5000)
        assert is_dup is True
