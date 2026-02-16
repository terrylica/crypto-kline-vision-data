#!/usr/bin/env python3
"""Unit tests for memory efficiency refactoring.

Tests validate that:
1. Polars pipeline is NOT populated when return_polars=False (Phase 2a)
2. Polars pipeline IS populated when return_polars=True (Phase 2a)
3. merge_dataframes() output order is correct after double-sort removal (Phase 1a)
4. Vision data client uses self.interval_obj (Phase 1c)

ADR: docs/adr/2025-01-30-failover-control-protocol.md
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
import polars as pl
import pytest

from ckvd import CryptoKlineVisionData, DataProvider, Interval, MarketType
from ckvd.utils.internal.polars_pipeline import PolarsDataPipeline


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def base_time():
    """Fixed base time in the past for reproducible tests."""
    return datetime(2024, 1, 15, 0, 0, 0, tzinfo=timezone.utc)


def _make_result_df(base_time, hours=12, sources=None):
    """Create a result DataFrame with mixed FCP sources."""
    if sources is None:
        sources = ["CACHE"] * 4 + ["VISION"] * 4 + ["REST"] * 4
    timestamps = [base_time + timedelta(hours=i) for i in range(hours)]
    df = pd.DataFrame(
        {
            "open_time": timestamps,
            "open": [42000.0 + i * 10 for i in range(hours)],
            "high": [42100.0 + i * 10 for i in range(hours)],
            "low": [41900.0 + i * 10 for i in range(hours)],
            "close": [42050.0 + i * 10 for i in range(hours)],
            "volume": [1000.0 + i for i in range(hours)],
            "_data_source": sources,
        }
    )
    return df


# =============================================================================
# Phase 2a: Pipeline guard with return_polars
# =============================================================================


class TestPipelineGuardReturnPolars:
    """Tests that Polars pipeline is only populated when return_polars=True.

    Validates Phase 2a: the `if return_polars` guard prevents wasted
    pandas→Polars conversions for the default pandas output path.
    """

    def test_add_pandas_not_called_when_return_polars_false(self, base_time):
        """When return_polars=False, pipeline.add_pandas should NOT be called.

        We spy on PolarsDataPipeline.add_pandas to verify the guard works.
        """
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
        result_df = _make_result_df(base_time)

        with (
            patch.object(manager, "_get_from_cache", return_value=pd.DataFrame()),
            patch(
                "ckvd.core.sync.crypto_kline_vision_data.process_vision_step",
                side_effect=lambda **kw: (result_df.copy(), []),
            ),
            patch("ckvd.utils.for_core.ckvd_cache_utils.get_cache_lazyframes", return_value=[]),
            patch("ckvd.core.sync.crypto_kline_vision_data.verify_final_data"),
            patch.object(PolarsDataPipeline, "add_pandas", wraps=PolarsDataPipeline.add_pandas) as spy_add_pandas,
        ):
            end = base_time + timedelta(hours=12)
            df = manager.get_data(
                symbol="BTCUSDT",
                start_time=base_time,
                end_time=end,
                interval=Interval.HOUR_1,
                return_polars=False,
            )

            assert isinstance(df, pd.DataFrame)
            # add_pandas should NOT be called — our Phase 2a optimization
            spy_add_pandas.assert_not_called()

        manager.close()

    def test_add_pandas_called_when_return_polars_true(self, base_time):
        """When return_polars=True, pipeline.add_pandas SHOULD be called.

        Ensures the return_polars=True path still populates the pipeline.
        Uses a tracking wrapper to spy on add_pandas calls without breaking self binding.
        """
        manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
        result_df = _make_result_df(base_time)

        # Track add_pandas calls via a wrapper
        add_pandas_calls = []
        original_add_pandas = PolarsDataPipeline.add_pandas

        def tracking_add_pandas(self_pipeline, df, source):
            add_pandas_calls.append(source)
            return original_add_pandas(self_pipeline, df, source)

        with (
            patch.object(manager, "_get_from_cache", return_value=pd.DataFrame()),
            patch(
                "ckvd.core.sync.crypto_kline_vision_data.process_vision_step",
                side_effect=lambda **kw: (result_df.copy(), []),
            ),
            patch("ckvd.utils.for_core.ckvd_cache_utils.get_cache_lazyframes", return_value=[]),
            patch("ckvd.core.sync.crypto_kline_vision_data.verify_final_data"),
            patch.object(PolarsDataPipeline, "add_pandas", new=tracking_add_pandas),
        ):
            end = base_time + timedelta(hours=12)
            result = manager.get_data(
                symbol="BTCUSDT",
                start_time=base_time,
                end_time=end,
                interval=Interval.HOUR_1,
                return_polars=True,
            )

            assert isinstance(result, pl.DataFrame)
            # add_pandas SHOULD be called for Vision data
            assert len(add_pandas_calls) >= 1, (
                f"add_pandas should be called when return_polars=True, got {add_pandas_calls}"
            )
            assert "VISION" in add_pandas_calls, (
                f"Vision data should be added to pipeline, got {add_pandas_calls}"
            )

        manager.close()


# =============================================================================
# Phase 1a: merge_dataframes sort order after double-sort removal
# =============================================================================


class TestMergeDataframesSortOrder:
    """Tests that merge_dataframes() maintains correct sort order.

    After removing the redundant second sort, these tests validate that
    drop_duplicates(keep='last') preserves the sort order from the first sort.
    """

    def test_unsorted_inputs_produce_sorted_output(self, base_time):
        """DataFrames provided in reverse order should still produce sorted output."""
        from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes

        # Create DataFrames in reverse chronological order
        df_late = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6, 12)],
                "open": [200.0 + i * 10 for i in range(6)],
                "high": [210.0 + i * 10 for i in range(6)],
                "low": [190.0 + i * 10 for i in range(6)],
                "close": [205.0 + i * 10 for i in range(6)],
                "volume": [2000.0 + i * 100 for i in range(6)],
                "_data_source": ["REST"] * 6,
            }
        )
        df_early = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6)],
                "open": [100.0 + i * 10 for i in range(6)],
                "high": [110.0 + i * 10 for i in range(6)],
                "low": [90.0 + i * 10 for i in range(6)],
                "close": [105.0 + i * 10 for i in range(6)],
                "volume": [1000.0 + i * 100 for i in range(6)],
                "_data_source": ["CACHE"] * 6,
            }
        )

        # Pass late first, early second
        result = merge_dataframes([df_late, df_early])

        assert result.index.is_monotonic_increasing, "Output should be sorted by open_time ascending"
        assert len(result) == 12

    def test_overlapping_with_priority_after_sort_removal(self, base_time):
        """Overlapping timestamps should still resolve priority correctly.

        After removing the redundant second sort, drop_duplicates(keep='last')
        must still keep the highest-priority source.
        """
        from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes

        # Same timestamps, different sources and values
        df_vision = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6)],
                "open": [100.0] * 6,
                "high": [110.0] * 6,
                "low": [90.0] * 6,
                "close": [105.0] * 6,
                "volume": [1000.0] * 6,
                "_data_source": ["VISION"] * 6,
            }
        )
        df_rest = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6)],
                "open": [200.0] * 6,
                "high": [210.0] * 6,
                "low": [190.0] * 6,
                "close": [205.0] * 6,
                "volume": [2000.0] * 6,
                "_data_source": ["REST"] * 6,
            }
        )

        result = merge_dataframes([df_vision, df_rest])

        # REST should still win (priority 3 > VISION priority 1)
        assert len(result) == 6
        assert (result["_data_source"] == "REST").all()
        assert result["open"].iloc[0] == 200.0
        # Result should still be sorted
        assert result.index.is_monotonic_increasing

    def test_index_is_reset_after_dedup(self, base_time):
        """After dedup with ignore_index=True, index should be clean.

        Validates that ignore_index=True on drop_duplicates produces
        a clean index before standardize_columns sets open_time as index.
        """
        from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes

        df = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6)],
                "open": [100.0 + i for i in range(6)],
                "high": [110.0 + i for i in range(6)],
                "low": [90.0 + i for i in range(6)],
                "close": [105.0 + i for i in range(6)],
                "volume": [1000.0] * 6,
                "_data_source": ["CACHE"] * 6,
            }
        )

        result = merge_dataframes([df])

        # After standardize_columns, open_time should be the index
        assert result.index.name == "open_time"
        assert len(result) == 6


# =============================================================================
# Phase 1c: Vision data client interval_obj reuse
# =============================================================================


class TestVisionClientIntervalObj:
    """Tests that VisionDataClient uses self.interval_obj instead of enum scan."""

    def test_interval_obj_set_during_init(self):
        """VisionDataClient should have interval_obj set from parse_interval()."""
        from ckvd.core.providers.binance.vision_data_client import VisionDataClient

        client = VisionDataClient(
            symbol="BTCUSDT",
            interval="1h",
            market_type="futures_usdt",
        )

        assert client.interval_obj is not None
        assert client.interval_obj == Interval.HOUR_1

    def test_interval_obj_matches_interval_str(self):
        """interval_obj.value should match _interval_str."""
        from ckvd.core.providers.binance.vision_data_client import VisionDataClient

        for interval_str, expected_obj in [
            ("1m", Interval.MINUTE_1),
            ("5m", Interval.MINUTE_5),
            ("1h", Interval.HOUR_1),
            ("4h", Interval.HOUR_4),
            ("1d", Interval.DAY_1),
        ]:
            client = VisionDataClient(
                symbol="BTCUSDT",
                interval=interval_str,
                market_type="spot",
            )
            assert client.interval_obj == expected_obj, (
                f"interval_obj should be {expected_obj} for '{interval_str}'"
            )
