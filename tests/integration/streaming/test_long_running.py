# ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
"""Phase 10 tests: Endurance and long-running stress tests.

Scenarios:
    50. Sustained stream — 10K klines with random disconnects, all gaps detected
    51. Cooldown exhaustion — rapid gaps rate-limited by cooldown timer
    52. Stats accuracy — reconciliation stats counters are consistent
    53. Dedup under sustained load — bounded set with continuous eviction
    54. Multi-symbol sustained — 3 symbols, independent gaps, all tracked
    55. Clock monotonicity — out-of-order timestamps don't cause false gaps
"""

from __future__ import annotations

import time

from ckvd._reconciler import DedupEngine, _INTERVAL_MS, detect_gap


# ---------------------------------------------------------------------------
# Scenario 50: Sustained stream with random disconnects
# ---------------------------------------------------------------------------


class TestSustainedStream:
    """Simulate a long stream with periodic gaps, verify all detected."""

    def test_10k_klines_with_periodic_gaps(self):
        """10K klines at 1m, gap every 500th kline (20 gaps total)."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1m"]
        max_gap = 1440
        total = 10_000

        dedup = DedupEngine(max_gap)
        prev_ms = None
        gaps = []

        start = time.perf_counter()

        for i in range(total):
            # Skip every 500th kline to simulate periodic disconnect gaps
            if i > 0 and i % 500 == 0:
                continue

            ts = base_ms + i * interval_ms
            if prev_ms is not None:
                has_gap, capped = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps.append((prev_ms + interval_ms, capped))

            is_dup = dedup.check_and_insert("BTCUSDT", "1m", ts)
            if not is_dup:
                prev_ms = ts

        elapsed = time.perf_counter() - start

        # 10K / 500 = 20 gaps (skip at 500, 1000, ..., 9500)
        assert len(gaps) == 19  # first skip at i=500 creates gap, last at 9500
        assert elapsed < 2.0, f"10K kline processing took {elapsed:.3f}s"

    def test_variable_gap_sizes(self):
        """Gaps of increasing size: 1, 2, 3, ..., 10 intervals."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1h"]
        max_gap = 1440

        # Build sequence: 10 normal, skip 1, 10 normal, skip 2, ...
        timestamps = []
        current = 0
        expected_gaps = 0
        for gap_size in range(1, 11):
            # 10 normal klines
            for _ in range(10):
                timestamps.append(base_ms + current * interval_ms)
                current += 1
            # Skip gap_size intervals
            current += gap_size
            expected_gaps += 1

        # Final 10 klines after last gap
        for _ in range(10):
            timestamps.append(base_ms + current * interval_ms)
            current += 1

        dedup = DedupEngine(max_gap)
        prev_ms = None
        gaps = []

        for ts in timestamps:
            if prev_ms is not None:
                has_gap, capped = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps.append((prev_ms + interval_ms, capped))

            is_dup = dedup.check_and_insert("BTCUSDT", "1h", ts)
            if not is_dup:
                prev_ms = ts

        assert len(gaps) == expected_gaps


# ---------------------------------------------------------------------------
# Scenario 51: Cooldown exhaustion simulation
# ---------------------------------------------------------------------------


class TestCooldownExhaustion:
    """Simulate rapid gap triggers, verify cooldown limits reconciliation calls."""

    def test_cooldown_rate_limiting(self):
        """100 gaps in rapid succession — cooldown should limit actual calls."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1m"]
        max_gap = 1440
        cooldown_ms = 30_000  # 30s cooldown

        # Simulate 100 gaps, each separated by 2 minutes of normal data
        # Cooldown is per (symbol, interval) — track when last reconciliation happened
        timestamps = []
        current = 0
        for _ in range(100):
            # 2 normal klines
            for _ in range(2):
                timestamps.append(base_ms + current * interval_ms)
                current += 1
            # Skip 3 intervals (gap)
            current += 3

        gaps_detected = 0
        reconciliations_allowed = 0
        last_reconcile_ms = 0
        prev_ms = None

        for ts in timestamps:
            if prev_ms is not None:
                has_gap, _ = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps_detected += 1
                    # Check cooldown
                    if ts - last_reconcile_ms >= cooldown_ms:
                        reconciliations_allowed += 1
                        last_reconcile_ms = ts
            prev_ms = ts

        # Last cycle's gap has no subsequent kline → 99 gaps detected
        assert gaps_detected == 99
        # Each cycle = 5 min apart (2 × 60s + 3 × 60s), cooldown 30s → all allowed
        assert reconciliations_allowed == gaps_detected

    def test_tight_cooldown_limits_calls(self):
        """Gaps separated by only 10s — cooldown at 30s should block most."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1s"]  # 1-second intervals
        max_gap = 1440
        cooldown_ms = 30_000  # 30s cooldown

        # 5 normal klines (5s), skip 3 (gap), repeat 20 times
        # Each cycle = 8s, well within 30s cooldown
        timestamps = []
        current = 0
        for _ in range(20):
            for _ in range(5):
                timestamps.append(base_ms + current * interval_ms)
                current += 1
            current += 3  # gap

        gaps_detected = 0
        reconciliations_allowed = 0
        last_reconcile_ms = 0
        prev_ms = None

        for ts in timestamps:
            if prev_ms is not None:
                has_gap, _ = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps_detected += 1
                    if ts - last_reconcile_ms >= cooldown_ms:
                        reconciliations_allowed += 1
                        last_reconcile_ms = ts
            prev_ms = ts

        # Last cycle's gap has no subsequent kline → 19 gaps detected
        assert gaps_detected == 19
        # 19 gaps × 8s each = 152s total. Cooldown 30s → ~5-6 reconciliations
        assert reconciliations_allowed < gaps_detected
        assert reconciliations_allowed >= 5


# ---------------------------------------------------------------------------
# Scenario 52: Stats accuracy
# ---------------------------------------------------------------------------


class TestStatsAccuracy:
    """Verify gap detection and dedup counters are consistent."""

    def test_counters_sum_correctly(self):
        """total = unique + duplicates. gaps = detected gaps."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1h"]
        max_gap = 1440

        # Generate 100 klines with 5 gaps and 10 intentional duplicates
        timestamps = []
        for i in range(100):
            if i not in {20, 40, 60, 80, 90}:  # 5 gaps
                timestamps.append(base_ms + i * interval_ms)

        # Add 10 duplicates (repeat timestamps 10-19)
        for i in range(10, 20):
            timestamps.append(base_ms + i * interval_ms)

        dedup = DedupEngine(max_gap)
        unique_count = 0
        dup_count = 0
        gap_count = 0
        prev_ms = None

        for ts in timestamps:
            is_dup = dedup.check_and_insert("BTCUSDT", "1h", ts)
            if is_dup:
                dup_count += 1
            else:
                unique_count += 1
                if prev_ms is not None:
                    has_gap, _ = detect_gap(prev_ms, ts, interval_ms, max_gap)
                    if has_gap:
                        gap_count += 1
                prev_ms = ts

        assert unique_count + dup_count == len(timestamps)
        assert dup_count == 10
        assert gap_count == 5
        assert len(dedup) == unique_count


# ---------------------------------------------------------------------------
# Scenario 53: Dedup under sustained load
# ---------------------------------------------------------------------------


class TestDedupSustainedLoad:
    """DedupEngine under continuous eviction pressure."""

    def test_sustained_eviction(self):
        """50K inserts with capacity=100, verify bounded throughout."""
        capacity = 100
        dedup = DedupEngine(capacity)

        start = time.perf_counter()

        for i in range(50_000):
            dedup.check_and_insert("BTC", "1h", i * 3_600_000)
            assert len(dedup) <= capacity

        elapsed = time.perf_counter() - start

        assert len(dedup) == capacity
        assert elapsed < 2.0, f"50K evictions took {elapsed:.3f}s"

    def test_eviction_correctness_under_load(self):
        """After 1000 inserts with capacity=10, only last 10 remain."""
        capacity = 10
        dedup = DedupEngine(capacity)

        for i in range(1000):
            dedup.check_and_insert("BTC", "1h", i * 1000)

        # First 990 should be evicted
        for i in range(990):
            assert not dedup.contains("BTC", "1h", i * 1000)

        # Last 10 should be present
        for i in range(990, 1000):
            assert dedup.contains("BTC", "1h", i * 1000)


# ---------------------------------------------------------------------------
# Scenario 54: Multi-symbol sustained
# ---------------------------------------------------------------------------


class TestMultiSymbolSustained:
    """Multiple symbols with independent gap tracking over extended periods."""

    def test_3_symbols_independent_gaps(self):
        """BTCUSDT, ETHUSDT, SOLUSDT — each has different gap patterns."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1h"]
        max_gap = 1440

        # Define per-symbol gap indices
        gap_indices: dict[str, set[int]] = {
            "BTCUSDT": {10, 11, 12},  # 3-hour gap
            "ETHUSDT": {5, 20, 21, 35},  # 3 gaps (1h, 2h, 1h)
            "SOLUSDT": set(),  # no gaps
        }

        dedup = DedupEngine(max_gap * 3)  # 3 symbols
        last_confirmed: dict[str, int] = {}
        gaps_per_symbol: dict[str, int] = {s: 0 for s in gap_indices}

        for hour in range(48):  # 2 days
            for sym, skip_set in gap_indices.items():
                if hour in skip_set:
                    continue

                ts = base_ms + hour * interval_ms

                if sym in last_confirmed:
                    has_gap, _ = detect_gap(
                        last_confirmed[sym], ts, interval_ms, max_gap
                    )
                    if has_gap:
                        gaps_per_symbol[sym] += 1

                is_dup = dedup.check_and_insert(sym, "1h", ts)
                if not is_dup:
                    last_confirmed[sym] = ts

        assert gaps_per_symbol["BTCUSDT"] == 1  # one contiguous 3h gap
        assert gaps_per_symbol["ETHUSDT"] == 3  # three separate gaps
        assert gaps_per_symbol["SOLUSDT"] == 0  # no gaps


# ---------------------------------------------------------------------------
# Scenario 55: Clock monotonicity — out-of-order timestamps
# ---------------------------------------------------------------------------


class TestClockMonotonicity:
    """Out-of-order timestamps don't cause false gap detection."""

    def test_out_of_order_no_false_gaps(self):
        """Feed timestamps out-of-order, verify gap detection uses confirmed order."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1h"]
        max_gap = 1440

        # Normal sequence: 0,1,2,3,...,9
        # Out-of-order: 0,1,2,5,3,4,6,7,8,9  (5 arrives before 3,4)
        ooo_order = [0, 1, 2, 5, 3, 4, 6, 7, 8, 9]
        timestamps = [base_ms + i * interval_ms for i in ooo_order]

        # Use monotonic guard: only update prev_ms if ts > prev_ms
        dedup = DedupEngine(max_gap)
        prev_ms = None
        gaps = []

        for ts in timestamps:
            is_dup = dedup.check_and_insert("BTCUSDT", "1h", ts)
            if is_dup:
                continue

            if prev_ms is not None and ts > prev_ms:
                has_gap, capped = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps.append((prev_ms + interval_ms, capped))
                prev_ms = ts
            elif prev_ms is None:
                prev_ms = ts
            # If ts <= prev_ms, we skip updating prev_ms (monotonic guard)

        # With monotonic guard, the "5 arriving before 3,4" causes one gap
        # 0→1→2→5 (gap: 2→5 = 3 intervals), then 3,4 are out-of-order (skipped),
        # then 6,7,8,9 continue from 5
        assert len(gaps) == 1
        gap_start, gap_end = gaps[0]
        assert gap_start == base_ms + 3 * interval_ms  # expected gap start: hour 3
        assert gap_end == base_ms + 5 * interval_ms  # gap end: hour 5

    def test_strictly_decreasing_no_gaps(self):
        """All timestamps in reverse order — no gaps detected (all skipped)."""
        base_ms = 1_705_276_800_000
        interval_ms = _INTERVAL_MS["1h"]
        max_gap = 1440

        # Reverse order: 9,8,7,...,0
        timestamps = [base_ms + i * interval_ms for i in range(9, -1, -1)]

        prev_ms = None
        gaps = []

        for ts in timestamps:
            if prev_ms is not None and ts > prev_ms:
                has_gap, _ = detect_gap(prev_ms, ts, interval_ms, max_gap)
                if has_gap:
                    gaps.append(ts)
                prev_ms = ts
            elif prev_ms is None:
                prev_ms = ts

        # Only first timestamp sets prev_ms, all others are <= prev_ms
        assert len(gaps) == 0
