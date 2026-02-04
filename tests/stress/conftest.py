"""Stress test fixtures and configuration.

Memory monitoring fixtures and shared test utilities for stress tests.
"""

import gc
import tracemalloc
from datetime import datetime, timezone

import pytest


@pytest.fixture
def memory_tracker():
    """Context manager for tracking memory allocation.

    Usage:
        def test_memory(memory_tracker):
            with memory_tracker as tracker:
                # ... operations ...
            assert tracker.peak_mb < 100
    """

    class MemoryTracker:
        def __init__(self):
            self.current = 0
            self.peak = 0
            self.snapshots = []

        def __enter__(self):
            gc.collect()
            tracemalloc.start()
            self.snapshots.append(("start", tracemalloc.take_snapshot()))
            return self

        def __exit__(self, *args):
            self.current, self.peak = tracemalloc.get_traced_memory()
            self.snapshots.append(("end", tracemalloc.take_snapshot()))
            tracemalloc.stop()

        @property
        def peak_mb(self) -> float:
            """Peak memory in megabytes."""
            return self.peak / (1024 * 1024)

        @property
        def current_mb(self) -> float:
            """Current memory in megabytes."""
            return self.current / (1024 * 1024)

        def get_delta_mb(self) -> float:
            """Memory growth between start and end snapshots."""
            if len(self.snapshots) < 2:
                return 0.0
            start = self.snapshots[0][1]
            end = self.snapshots[-1][1]
            diff = end.compare_to(start, "lineno")
            return sum(stat.size_diff for stat in diff) / (1024 * 1024)

    return MemoryTracker()


@pytest.fixture
def historical_time_range():
    """Standard historical time range for stress tests.

    Returns data from 2024-01-01 to avoid hitting recent data edge cases.
    """
    end = datetime(2024, 1, 8, tzinfo=timezone.utc)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return start, end


@pytest.fixture
def large_time_range():
    """Large time range (30 days) for memory pressure tests."""
    end = datetime(2024, 1, 31, tzinfo=timezone.utc)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return start, end


@pytest.fixture
def test_symbols():
    """Standard symbols for multi-symbol stress tests."""
    return [
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "ADAUSDT",
        "DOGEUSDT",
        "DOTUSDT",
        "MATICUSDT",
        "LTCUSDT",
    ]


def pytest_configure(config):
    """Register stress test marker."""
    config.addinivalue_line("markers", "stress: mark test as stress test (may be slow, memory-intensive)")
