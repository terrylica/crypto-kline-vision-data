#!/usr/bin/env python3
"""Unit tests for merge_dataframes edge cases.

Tests validate additional edge cases for the FCP merge logic
beyond those covered in test_merge_dataframes_polars.py.

These tests serve as a regression safety net for Phase 2 Polars pipeline migration.

Copy from: tests/unit/utils/for_core/test_dsm_fcp_utils.py
Task: #81 - Create test_merge_edge_cases.py (10 tests)

ADR: docs/adr/2025-01-30-failover-control-protocol.md
Plan: /Users/terryli/.claude/plans/gleaming-frolicking-engelbart.md
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def base_time():
    """Fixed base time for reproducible tests."""
    return datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)


def make_ohlcv_df(
    base_time: datetime,
    hours: int,
    source: str | None = None,
    offset_hours: int = 0,
    open_base: float = 100.0,
) -> pd.DataFrame:
    """Create a standard OHLCV DataFrame for testing."""
    start = base_time + timedelta(hours=offset_hours)
    timestamps = [start + timedelta(hours=i) for i in range(hours)]

    data = {
        "open_time": timestamps,
        "open": [open_base + i * 10 for i in range(hours)],
        "high": [open_base + 100 + i * 10 for i in range(hours)],
        "low": [open_base - 50 + i * 10 for i in range(hours)],
        "close": [open_base + 50 + i * 10 for i in range(hours)],
        "volume": [1000.0 + i * 100 for i in range(hours)],
    }

    if source is not None:
        data["_data_source"] = [source] * hours

    return pd.DataFrame(data)


# =============================================================================
# Test 1-4: Partial Overlap Edge Cases
# =============================================================================


class TestPartialOverlapMerge:
    """Tests for partial overlap scenarios in merge_dataframes()."""

    def test_merge_partial_overlap_at_start(self, base_time):
        """Partial overlap at start should be handled correctly.

        Pattern: df1 covers hours 0-5, df2 covers hours 3-8
        Result: hours 0-8, df2 wins for hours 3-5 (if higher priority)
        """
        df1 = make_ohlcv_df(base_time, hours=6, source="CACHE", open_base=100.0)
        df2 = make_ohlcv_df(base_time, hours=6, source="REST", offset_hours=3, open_base=200.0)

        result = merge_dataframes([df1, df2])

        # Should have 9 unique timestamps (hours 0-8)
        assert len(result) == 9
        # REST should win for overlapping hours (3-5)
        overlap_mask = result.index >= (base_time + timedelta(hours=3))
        overlap_data = result[overlap_mask]
        assert (overlap_data["_data_source"] == "REST").all()

    def test_merge_partial_overlap_at_end(self, base_time):
        """Partial overlap at end should be handled correctly.

        Pattern: df1 covers hours 3-8, df2 covers hours 0-5
        Result: hours 0-8, df1 (REST) wins for hours 3-5
        """
        df1 = make_ohlcv_df(base_time, hours=6, source="REST", offset_hours=3, open_base=200.0)
        df2 = make_ohlcv_df(base_time, hours=6, source="VISION", open_base=100.0)

        result = merge_dataframes([df1, df2])

        # Should have 9 unique timestamps
        assert len(result) == 9
        # REST should win for overlapping hours
        rest_count = (result["_data_source"] == "REST").sum()
        assert rest_count == 6  # REST provided hours 3-8

    def test_merge_complete_overlap(self, base_time):
        """Complete overlap should keep highest priority only.

        All timestamps overlap, REST should win completely.
        """
        df1 = make_ohlcv_df(base_time, hours=6, source="VISION", open_base=100.0)
        df2 = make_ohlcv_df(base_time, hours=6, source="CACHE", open_base=200.0)
        df3 = make_ohlcv_df(base_time, hours=6, source="REST", open_base=300.0)

        result = merge_dataframes([df1, df2, df3])

        # Only 6 unique timestamps
        assert len(result) == 6
        # REST wins all
        assert (result["_data_source"] == "REST").all()
        assert result.iloc[0]["open"] == 300.0

    def test_merge_interleaved_timestamps(self, base_time):
        """Interleaved (non-contiguous) timestamps should merge correctly."""
        # df1 has hours 0, 2, 4
        df1 = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in [0, 2, 4]],
                "open": [100.0, 120.0, 140.0],
                "high": [110.0, 130.0, 150.0],
                "low": [90.0, 110.0, 130.0],
                "close": [105.0, 125.0, 145.0],
                "volume": [1000.0, 1200.0, 1400.0],
                "_data_source": ["CACHE", "CACHE", "CACHE"],
            }
        )
        # df2 has hours 1, 3, 5
        df2 = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in [1, 3, 5]],
                "open": [210.0, 230.0, 250.0],
                "high": [220.0, 240.0, 260.0],
                "low": [200.0, 220.0, 240.0],
                "close": [215.0, 235.0, 255.0],
                "volume": [2100.0, 2300.0, 2500.0],
                "_data_source": ["REST", "REST", "REST"],
            }
        )

        result = merge_dataframes([df1, df2])

        # Should have 6 unique timestamps
        assert len(result) == 6
        # Should be sorted by open_time
        assert result.index.is_monotonic_increasing


# =============================================================================
# Test 5-7: Gap Filling Edge Cases
# =============================================================================


class TestGapFillingMerge:
    """Tests for gap filling scenarios in merge_dataframes()."""

    def test_merge_with_gap_in_middle(self, base_time):
        """Gap in the middle should be preserved (not filled with duplicates).

        Pattern: df1 covers hours 0-2, df2 covers hours 5-7
        Result: hours 0-2 and 5-7 only (no synthetic data for 3-4)
        """
        df1 = make_ohlcv_df(base_time, hours=3, source="CACHE")
        df2 = make_ohlcv_df(base_time, hours=3, source="REST", offset_hours=5)

        result = merge_dataframes([df1, df2])

        # Should have 6 rows (3 from each, with gap)
        assert len(result) == 6
        # Verify gap exists
        hour_3 = base_time + timedelta(hours=3)
        hour_4 = base_time + timedelta(hours=4)
        assert hour_3 not in result.index
        assert hour_4 not in result.index

    def test_merge_fills_gap_with_third_source(self, base_time):
        """Third source can fill gaps left by first two.

        Pattern: CACHE has 0-2, REST has 5-7, VISION fills 3-4
        """
        df1 = make_ohlcv_df(base_time, hours=3, source="CACHE")
        df2 = make_ohlcv_df(base_time, hours=3, source="REST", offset_hours=5)
        df3 = make_ohlcv_df(base_time, hours=2, source="VISION", offset_hours=3)

        result = merge_dataframes([df1, df2, df3])

        # Should have 8 rows (contiguous 0-7)
        assert len(result) == 8
        # Verify gap is filled
        hour_3 = base_time + timedelta(hours=3)
        hour_4 = base_time + timedelta(hours=4)
        assert hour_3 in result.index
        assert hour_4 in result.index

    def test_merge_large_gap(self, base_time):
        """Large gap between sources should be handled."""
        df1 = make_ohlcv_df(base_time, hours=3, source="CACHE")
        df2 = make_ohlcv_df(base_time, hours=3, source="REST", offset_hours=100)

        result = merge_dataframes([df1, df2])

        # Should have 6 rows
        assert len(result) == 6
        # Sources should be preserved
        cache_count = (result["_data_source"] == "CACHE").sum()
        rest_count = (result["_data_source"] == "REST").sum()
        assert cache_count == 3
        assert rest_count == 3


# =============================================================================
# Test 8-10: Special Column Handling
# =============================================================================


class TestSpecialColumnHandling:
    """Tests for special column handling in merge_dataframes()."""

    def test_merge_mixed_data_source_presence(self, base_time):
        """Mixed _data_source presence should be handled.

        One DataFrame has _data_source, another doesn't.
        """
        df1 = make_ohlcv_df(base_time, hours=3, source="CACHE")
        df2 = make_ohlcv_df(base_time, hours=3, offset_hours=3)  # No source

        result = merge_dataframes([df1, df2])

        assert "_data_source" in result.columns
        # Should have 6 rows
        assert len(result) == 6
        # df2 rows should have "UNKNOWN" source
        unknown_count = (result["_data_source"] == "UNKNOWN").sum()
        assert unknown_count == 3

    def test_merge_standardizes_to_ohlcv_schema(self, base_time):
        """Merge standardizes to OHLCV schema (extra columns may be dropped).

        Note: standardize_dataframe() enforces a consistent OHLCV schema.
        Custom columns are NOT preserved - this is expected behavior.
        """
        df = make_ohlcv_df(base_time, hours=3, source="CACHE")
        df["custom_column"] = [1, 2, 3]

        result = merge_dataframes([df])

        # Standard OHLCV columns are always present
        for col in ["open", "high", "low", "close", "volume"]:
            assert col in result.columns
        # _data_source is preserved
        assert "_data_source" in result.columns
        # Note: custom columns may be dropped by standardize_dataframe

    def test_merge_different_column_orders(self, base_time):
        """DataFrames with different column orders should merge correctly."""
        df1 = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(3)],
                "volume": [1000.0, 1100.0, 1200.0],
                "close": [105.0, 115.0, 125.0],
                "low": [90.0, 100.0, 110.0],
                "high": [110.0, 120.0, 130.0],
                "open": [100.0, 110.0, 120.0],
                "_data_source": ["CACHE", "CACHE", "CACHE"],
            }
        )
        df2 = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(3, 6)],
                "open": [130.0, 140.0, 150.0],
                "high": [140.0, 150.0, 160.0],
                "low": [120.0, 130.0, 140.0],
                "close": [135.0, 145.0, 155.0],
                "volume": [1300.0, 1400.0, 1500.0],
                "_data_source": ["REST", "REST", "REST"],
            }
        )

        result = merge_dataframes([df1, df2])

        # Should merge successfully
        assert len(result) == 6
        # All OHLCV columns should be present
        for col in ["open", "high", "low", "close", "volume"]:
            assert col in result.columns
