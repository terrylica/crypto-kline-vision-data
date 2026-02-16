# FILE-SIZE-OK: All memory efficiency tests (Round 1 + Round 2) belong together
#!/usr/bin/env python3
"""Unit tests for memory efficiency refactoring.

Round 1 (v4.3.4) tests validate that:
1. Polars pipeline is NOT populated when return_polars=False (Phase 2a)
2. Polars pipeline IS populated when return_polars=True (Phase 2a)
3. merge_dataframes() output order is correct after double-sort removal (Phase 1a)
4. Vision data client uses self.interval_obj (Phase 1c)

Round 2 tests validate that:
5. Funding rate list-collect pattern produces correct DataFrame (Phase 1a)
6. standardize_columns() has open_time as index and is idempotent (Phase 2a)
7. Polars pipeline merge produces monotonically sorted open_time (Phase 1c)
8. add_pandas() with indexed DataFrame preserves open_time column (Phase 2b)

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


# =============================================================================
# Round 2 — Phase 1a: Funding rate list-collect pattern
# =============================================================================


class TestFundingRateListCollect:
    """Tests that funding rate fetching uses list-collect instead of O(n²) concat.

    Validates Phase 1a (Round 2): the inner loop collects dicts into a list,
    and a single DataFrame is built after the loop completes.
    """

    def test_produces_correct_dataframe_with_many_items(self, base_time):
        """List-collect pattern should produce identical output to old concat loop."""
        from unittest.mock import MagicMock

        from ckvd.core.providers.binance.binance_funding_rate_client import BinanceFundingRateClient

        # Create a mock HTTP response with 120 funding rate items
        items = []
        for i in range(120):
            funding_ms = int((base_time + timedelta(hours=8 * i)).timestamp() * 1000)
            items.append({"fundingTime": funding_ms, "fundingRate": f"{0.0001 * (i + 1):.8f}"})

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = items

        client = BinanceFundingRateClient(
            symbol="BTCUSDT",
            market_type=MarketType.FUTURES_USDT,
            use_cache=False,
        )
        # Patch the HTTP client to return our mock
        client._client = MagicMock()
        client._client.get.return_value = mock_response

        end = base_time + timedelta(hours=8 * 120)
        result = client.fetch(
            symbol="BTCUSDT",
            interval="8h",
            start_time=base_time,
            end_time=end,
        )

        assert len(result) == 120
        assert "funding_time" in result.columns
        assert "funding_rate" in result.columns
        assert "symbol" in result.columns
        assert result["symbol"].iloc[0] == "BTCUSDT"
        # Verify sorted
        assert result["funding_time"].is_monotonic_increasing

    def test_produces_correct_dtypes(self, base_time):
        """Funding rate values should be float, not string."""
        from unittest.mock import MagicMock

        from ckvd.core.providers.binance.binance_funding_rate_client import BinanceFundingRateClient

        items = [
            {"fundingTime": int(base_time.timestamp() * 1000), "fundingRate": "0.00010000"},
            {"fundingTime": int((base_time + timedelta(hours=8)).timestamp() * 1000), "fundingRate": "-0.00050000"},
        ]

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = items

        client = BinanceFundingRateClient(
            symbol="BTCUSDT",
            market_type=MarketType.FUTURES_USDT,
            use_cache=False,
        )
        client._client = MagicMock()
        client._client.get.return_value = mock_response

        end = base_time + timedelta(hours=16)
        result = client.fetch("BTCUSDT", "8h", base_time, end)

        assert len(result) == 2
        assert result["funding_rate"].iloc[0] == pytest.approx(0.0001)
        assert result["funding_rate"].iloc[1] == pytest.approx(-0.0005)

    def test_empty_response_returns_empty_dataframe(self, base_time):
        """Empty API response should return empty DataFrame."""
        from unittest.mock import MagicMock

        from ckvd.core.providers.binance.binance_funding_rate_client import BinanceFundingRateClient

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = []

        client = BinanceFundingRateClient(
            symbol="BTCUSDT",
            market_type=MarketType.FUTURES_USDT,
            use_cache=False,
        )
        client._client = MagicMock()
        client._client.get.return_value = mock_response

        end = base_time + timedelta(hours=8)
        result = client.fetch("BTCUSDT", "8h", base_time, end)

        assert result.empty


# =============================================================================
# Round 2 — Phase 2a: standardize_columns NO-OP removal
# =============================================================================


class TestStandardizeColumnsNoOp:
    """Tests that standardize_columns() correctly sets open_time as index.

    Validates Phase 2a (Round 2): after removing the NO-OP reset_index/set_index
    round-trip, open_time should be set as index without appearing as a column.
    """

    def test_open_time_is_index_after_standardization(self, base_time):
        """standardize_columns() should set open_time as index."""
        from ckvd.utils.for_core.ckvd_time_range_utils import standardize_columns

        df = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6)],
                "open": [100.0] * 6,
                "high": [110.0] * 6,
                "low": [90.0] * 6,
                "close": [105.0] * 6,
                "volume": [1000.0] * 6,
            }
        )

        result = standardize_columns(df)

        assert result.index.name == "open_time"
        # open_time should NOT be a regular column (it's the index)
        assert "open_time" not in result.columns

    def test_idempotent_when_called_twice(self, base_time):
        """Calling standardize_columns() twice should produce same result."""
        from ckvd.utils.for_core.ckvd_time_range_utils import standardize_columns

        df = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6)],
                "open": [100.0] * 6,
                "high": [110.0] * 6,
                "low": [90.0] * 6,
                "close": [105.0] * 6,
                "volume": [1000.0] * 6,
            }
        )

        first_pass = standardize_columns(df)
        second_pass = standardize_columns(first_pass)

        assert second_pass.index.name == "open_time"
        assert "open_time" not in second_pass.columns
        assert len(second_pass) == len(first_pass)
        pd.testing.assert_frame_equal(first_pass, second_pass)

    def test_handles_already_indexed_input(self, base_time):
        """standardize_columns() should handle DataFrame with open_time already as index."""
        from ckvd.utils.for_core.ckvd_time_range_utils import standardize_columns

        timestamps = [base_time + timedelta(hours=i) for i in range(6)]
        df = pd.DataFrame(
            {
                "open": [100.0] * 6,
                "high": [110.0] * 6,
                "low": [90.0] * 6,
                "close": [105.0] * 6,
                "volume": [1000.0] * 6,
            },
            index=pd.DatetimeIndex(timestamps, name="open_time", tz="UTC"),
        )

        result = standardize_columns(df)

        assert result.index.name == "open_time"
        assert "open_time" not in result.columns
        assert len(result) == 6


# =============================================================================
# Round 2 — Phase 1c: Polars pipeline sort removal
# =============================================================================


class TestPolarsSecondSortRemoval:
    """Tests that Polars pipeline merge produces correctly sorted output.

    Validates Phase 1c (Round 2): after removing the redundant second
    .sort("open_time"), the output is still monotonically sorted.
    """

    def test_merge_produces_sorted_output(self, base_time):
        """Pipeline merge should produce monotonically sorted open_time."""
        pipeline = PolarsDataPipeline()

        # Add sources in reverse order to stress the sort
        late_lf = pl.LazyFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6, 12)],
                "open": [200.0 + i for i in range(6)],
                "close": [205.0 + i for i in range(6)],
                "_data_source": ["REST"] * 6,
            }
        ).cast({"open_time": pl.Datetime("us", "UTC")})

        early_lf = pl.LazyFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6)],
                "open": [100.0 + i for i in range(6)],
                "close": [105.0 + i for i in range(6)],
                "_data_source": ["CACHE"] * 6,
            }
        ).cast({"open_time": pl.Datetime("us", "UTC")})

        # Add late first, early second
        pipeline.add_source(late_lf, "REST")
        pipeline.add_source(early_lf, "CACHE")

        result = pipeline.collect_polars()

        assert result["open_time"].is_sorted()
        assert len(result) == 12

    def test_priority_resolution_with_single_sort(self, base_time):
        """Priority resolution should work correctly with single sort."""
        pipeline = PolarsDataPipeline()

        # Same timestamps, different sources — REST should win
        vision_lf = pl.LazyFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6)],
                "open": [100.0] * 6,
                "_data_source": ["VISION"] * 6,
            }
        ).cast({"open_time": pl.Datetime("us", "UTC")})

        rest_lf = pl.LazyFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6)],
                "open": [200.0] * 6,
                "_data_source": ["REST"] * 6,
            }
        ).cast({"open_time": pl.Datetime("us", "UTC")})

        pipeline.add_source(vision_lf, "VISION")
        pipeline.add_source(rest_lf, "REST")

        result = pipeline.collect_polars()

        assert len(result) == 6
        assert (result["_data_source"] == "REST").all()
        assert result["open"][0] == 200.0
        assert result["open_time"].is_sorted()


# =============================================================================
# Round 2 — Phase 2b: add_pandas include_index
# =============================================================================


class TestAddPandasIncludeIndex:
    """Tests that add_pandas() preserves open_time via include_index=True.

    Validates Phase 2b (Round 2): when a pandas DataFrame has open_time
    as its index, add_pandas() uses pl.from_pandas(include_index=True)
    instead of df.reset_index() to avoid a copy.
    """

    def test_indexed_dataframe_preserves_open_time(self, base_time):
        """add_pandas() with indexed DataFrame should include open_time column."""
        pipeline = PolarsDataPipeline()

        timestamps = [base_time + timedelta(hours=i) for i in range(6)]
        df = pd.DataFrame(
            {
                "open": [100.0] * 6,
                "high": [110.0] * 6,
                "low": [90.0] * 6,
                "close": [105.0] * 6,
                "volume": [1000.0] * 6,
            },
            index=pd.DatetimeIndex(timestamps, name="open_time", tz="UTC"),
        )

        pipeline.add_pandas(df, "CACHE")

        result = pipeline.collect_polars()
        assert "open_time" in result.columns
        assert len(result) == 6
        assert result["open_time"].is_sorted()

    def test_non_indexed_dataframe_works(self, base_time):
        """add_pandas() with column-based open_time should also work."""
        pipeline = PolarsDataPipeline()

        df = pd.DataFrame(
            {
                "open_time": [base_time + timedelta(hours=i) for i in range(6)],
                "open": [100.0] * 6,
                "volume": [1000.0] * 6,
            }
        )

        pipeline.add_pandas(df, "REST")

        result = pipeline.collect_polars()
        assert "open_time" in result.columns
        assert len(result) == 6

    def test_empty_dataframe_skipped(self):
        """add_pandas() with empty DataFrame should skip it."""
        pipeline = PolarsDataPipeline()

        df = pd.DataFrame()
        pipeline.add_pandas(df, "CACHE")

        assert pipeline.is_empty()
