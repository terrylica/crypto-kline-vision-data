# ADR: docs/adr/2026-02-24-websocket-streaming-subsystem.md
"""Python bridge for Rust-accelerated reconciler components.

Provides DedupEngine and detect_gap with automatic fallback to pure-Python
implementations when the Rust extension (_reconciler_rs) is not built.

Usage:
    from ckvd._reconciler import DedupEngine, detect_gap, BACKEND

    engine = DedupEngine(max_capacity=1440)
    is_dup = engine.check_and_insert("BTCUSDT", "1h", open_time_ms)

    has_gap, capped_end_ms = detect_gap(prev_ms, current_ms, interval_ms, max_gap)

    print(BACKEND)  # "rust" or "python"
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone


def _dt_to_ms(dt: datetime) -> int:
    """Convert a UTC datetime to milliseconds since epoch."""
    return int(dt.timestamp() * 1000)


def _ms_to_dt(ms: int) -> datetime:
    """Convert milliseconds since epoch to UTC datetime."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


# SSoT-OK: interval mappings are authoritative here for ms conversion
_INTERVAL_MS: dict[str, int] = {
    "1s": 1_000,
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
    "1M": 2_592_000_000,
}


# ---------------------------------------------------------------------------
# Pure-Python fallback
# ---------------------------------------------------------------------------


class _PythonDedupEngine:
    """Pure-Python bounded dedup engine (set + deque FIFO eviction)."""

    __slots__ = ("_max_capacity", "_order", "_seen")

    def __init__(self, max_capacity: int) -> None:
        self._seen: set[tuple[str, str, int]] = set()
        self._order: deque[tuple[str, str, int]] = deque()
        self._max_capacity = max_capacity

    def check_and_insert(self, symbol: str, interval: str, open_time_ms: int) -> bool:
        """Return True if DUPLICATE (already seen), False if new."""
        key = (symbol, interval, open_time_ms)
        if key in self._seen:
            return True

        if len(self._seen) >= self._max_capacity:
            oldest = self._order.popleft()
            self._seen.discard(oldest)

        self._seen.add(key)
        self._order.append(key)
        return False

    def contains(self, symbol: str, interval: str, open_time_ms: int) -> bool:
        """Check if key exists without inserting."""
        return (symbol, interval, open_time_ms) in self._seen

    def __len__(self) -> int:
        return len(self._seen)

    def clear(self) -> None:
        self._seen.clear()
        self._order.clear()


def _python_detect_gap(
    prev_ms: int, current_ms: int, interval_ms: int, max_gap_intervals: int
) -> tuple[bool, int]:
    """Pure-Python gap detection. Returns (has_gap, capped_end_ms)."""
    gap = current_ms - prev_ms
    if gap <= interval_ms:
        return (False, current_ms)

    max_gap_ms = interval_ms * max_gap_intervals
    capped_end = prev_ms + max_gap_ms if gap > max_gap_ms else current_ms
    return (True, capped_end)


# ---------------------------------------------------------------------------
# Backend selection: try Rust, fall back to Python
# ---------------------------------------------------------------------------

try:
    # Try ckvd._reconciler_rs first (in-tree build), then top-level (maturin develop)
    try:
        from ckvd._reconciler_rs import PyDedupEngine as _RustDedupEngine  # type: ignore[import-not-found]
        from ckvd._reconciler_rs import detect_gap as _rust_detect_gap  # type: ignore[import-not-found]
    except ImportError:
        from _reconciler_rs import PyDedupEngine as _RustDedupEngine  # type: ignore[import-not-found]
        from _reconciler_rs import detect_gap as _rust_detect_gap  # type: ignore[import-not-found]

    DedupEngine = _RustDedupEngine
    detect_gap = _rust_detect_gap
    BACKEND: str = "rust"
except ImportError:
    DedupEngine = _PythonDedupEngine  # type: ignore[assignment,misc]
    detect_gap = _python_detect_gap  # type: ignore[assignment]
    BACKEND = "python"


__all__ = [
    "BACKEND",
    "INTERVAL_MS",
    "DedupEngine",
    "detect_gap",
]

# Public alias for the interval mapping
INTERVAL_MS = _INTERVAL_MS
