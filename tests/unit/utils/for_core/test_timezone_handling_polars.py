#!/usr/bin/env python3
"""Unit tests for timezone handling in Polars operations.

Tests validate that timezone information is correctly preserved during
Polars operations and conversions to/from Pandas.

These tests serve as a regression safety net for Phase 2 Polars pipeline migration.

Copy from: tests/unit/utils/for_core/test_dsm_fcp_utils.py
Task: #79 - Create test_timezone_handling_polars.py (8 tests)

ADR: docs/adr/2025-01-30-failover-control-protocol.md
Plan: /Users/terryli/.claude/plans/gleaming-frolicking-engelbart.md
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl
import pytest


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def base_time_utc():
    """Fixed UTC base time for reproducible tests."""
    return datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def base_time_naive():
    """Fixed naive (no timezone) base time."""
    return datetime(2024, 1, 15, 12, 0, 0)


# =============================================================================
# Test 1-4: UTC Timezone Preservation in Polars
# =============================================================================


class TestPolarsTimezonePreservation:
    """Tests for timezone preservation in Polars operations."""

    def test_polars_datetime_preserves_utc(self, base_time_utc):
        """Polars should preserve UTC timezone information.

        CRITICAL: All market data must be UTC.
        """
        timestamps = [base_time_utc + timedelta(hours=i) for i in range(3)]
        df = pl.DataFrame({"open_time": timestamps, "value": [1.0, 2.0, 3.0]})

        assert df["open_time"].dtype == pl.Datetime("us", "UTC")

    def test_polars_lazyframe_preserves_utc(self, base_time_utc):
        """LazyFrame operations should preserve UTC timezone."""
        timestamps = [base_time_utc + timedelta(hours=i) for i in range(3)]
        df = pl.DataFrame({"open_time": timestamps, "value": [1.0, 2.0, 3.0]})

        lf = df.lazy()
        result = lf.filter(pl.col("value") > 1.0).collect()

        assert result["open_time"].dtype == pl.Datetime("us", "UTC")

    def test_polars_to_pandas_preserves_utc(self, base_time_utc):
        """to_pandas() should preserve UTC timezone."""
        timestamps = [base_time_utc + timedelta(hours=i) for i in range(3)]
        df = pl.DataFrame({"open_time": timestamps})

        pd_df = df.to_pandas()

        # Check pandas timezone
        assert pd_df["open_time"].dt.tz is not None
        assert str(pd_df["open_time"].dt.tz) == "UTC"

    def test_pandas_utc_to_polars_preserves_tz(self, base_time_utc):
        """pl.from_pandas() should preserve UTC from pandas.

        Note: Pandas uses nanoseconds, so Polars inherits ns precision.
        """
        timestamps = pd.DatetimeIndex([base_time_utc + timedelta(hours=i) for i in range(3)], tz="UTC")
        pd_df = pd.DataFrame({"open_time": timestamps})

        pl_df = pl.from_pandas(pd_df)

        # Timezone should be UTC (precision may be ns from pandas)
        assert pl_df["open_time"].dtype.time_zone == "UTC"


# =============================================================================
# Test 5-8: Naive Datetime Handling
# =============================================================================


class TestNaiveDatetimeHandling:
    """Tests for handling naive (no timezone) datetimes."""

    def test_polars_naive_datetime_type(self, base_time_naive):
        """Naive datetimes should have no timezone in Polars."""
        timestamps = [base_time_naive + timedelta(hours=i) for i in range(3)]
        df = pl.DataFrame({"open_time": timestamps})

        # Should be datetime without timezone
        assert df["open_time"].dtype == pl.Datetime("us")
        assert df["open_time"].dtype.time_zone is None

    def test_polars_convert_naive_to_utc(self, base_time_naive):
        """Naive datetime should be convertible to UTC.

        Pattern: df.with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))
        """
        timestamps = [base_time_naive + timedelta(hours=i) for i in range(3)]
        df = pl.DataFrame({"open_time": timestamps})

        df_utc = df.with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

        assert df_utc["open_time"].dtype == pl.Datetime("us", "UTC")

    def test_pandas_naive_to_polars_localize(self, base_time_naive):
        """Naive pandas datetime should be localizable to UTC in Polars.

        Note: Pandas uses nanoseconds, so Polars inherits ns precision.
        """
        timestamps = [base_time_naive + timedelta(hours=i) for i in range(3)]
        pd_df = pd.DataFrame({"open_time": timestamps})

        pl_df = pl.from_pandas(pd_df)

        # Naive in Polars
        assert pl_df["open_time"].dtype.time_zone is None

        # Localize to UTC
        pl_df_utc = pl_df.with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))
        # Check timezone is UTC (precision may be ns from pandas)
        assert pl_df_utc["open_time"].dtype.time_zone == "UTC"

    def test_polars_filter_with_utc_timestamp(self, base_time_utc):
        """Filtering with UTC timestamp should work correctly.

        Pattern used in dsm_cache_utils.py:199-201 for predicate pushdown.
        """
        timestamps = [base_time_utc + timedelta(hours=i) for i in range(24)]
        df = pl.DataFrame({"open_time": timestamps, "value": [float(i) for i in range(24)]})

        # Filter using UTC start/end times
        start = base_time_utc + timedelta(hours=6)
        end = base_time_utc + timedelta(hours=12)

        filtered = df.lazy().filter((pl.col("open_time") >= start) & (pl.col("open_time") <= end)).collect()

        # Should have hours 6-12 inclusive
        assert len(filtered) == 7
        # First timestamp should be hour 6
        assert filtered["open_time"][0] == start
