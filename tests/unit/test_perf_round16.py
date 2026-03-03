"""Performance benchmarks for Round 16: Cleanup & Copy Chain.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md

Validates:
1. standardize_column_names uses single df.rename() instead of per-column copy+drop
2. merge_dataframes filters empty DataFrames before concat
3. Correctness: standardize_column_names produces identical output
4. Correctness: merge_dataframes handles mix of empty and non-empty DataFrames
"""

import timeit
from datetime import datetime, timedelta, timezone

import pandas as pd


def _make_df_with_variant_columns(n_rows: int = 1000) -> pd.DataFrame:
    """Create a DataFrame with variant column names that need standardization."""
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    timestamps = [base + timedelta(hours=h) for h in range(n_rows)]
    return pd.DataFrame(
        {
            "open_time": timestamps,
            "open": [100.0 + h for h in range(n_rows)],
            "high": [101.0 + h for h in range(n_rows)],
            "low": [99.0 + h for h in range(n_rows)],
            "close": [100.5 + h for h in range(n_rows)],
            "volume": [1000.0] * n_rows,
            # Variant columns that need renaming
            "quote_volume": [500.0] * n_rows,
            "trades": list(range(n_rows)),
        }
    )


def _make_ohlcv_df(start: datetime, hours: int, source: str = "UNKNOWN") -> pd.DataFrame:
    """Create a standard OHLCV DataFrame."""
    timestamps = [start + timedelta(hours=h) for h in range(hours)]
    return pd.DataFrame(
        {
            "open_time": timestamps,
            "open": [100.0 + h for h in range(hours)],
            "high": [101.0 + h for h in range(hours)],
            "low": [99.0 + h for h in range(hours)],
            "close": [100.5 + h for h in range(hours)],
            "volume": [1000.0] * hours,
            "_data_source": [source] * hours,
        }
    )


class TestStandardizeColumnNames:
    """Benchmark: single rename() vs per-column copy+drop."""

    def test_rename_faster_than_copy_drop(self):
        """Single rename() should be faster than per-column copy+drop."""
        from ckvd.utils.config import COLUMN_NAME_MAPPING

        df = _make_df_with_variant_columns(5000)

        # Old approach: per-column copy + drop
        def old_approach():
            test_df = df.copy()
            for old_name, new_name in COLUMN_NAME_MAPPING.items():
                if old_name in test_df.columns and new_name not in test_df.columns:
                    test_df[new_name] = test_df[old_name]
                    test_df = test_df.drop(columns=[old_name])
            return test_df

        # New approach: single rename()
        def new_approach():
            test_df = df.copy()
            rename_dict = {
                old: new
                for old, new in COLUMN_NAME_MAPPING.items()
                if old in test_df.columns and new not in test_df.columns
            }
            if rename_dict:
                test_df = test_df.rename(columns=rename_dict)
            return test_df

        iterations = 500
        old_time = timeit.timeit(old_approach, number=iterations)
        new_time = timeit.timeit(new_approach, number=iterations)

        speedup = old_time / new_time
        assert speedup >= 1.0, (
            f"Expected rename to be at least as fast, got {speedup:.2f}x "
            f"(old={old_time:.4f}s, new={new_time:.4f}s)"
        )

    def test_rename_correctness(self):
        """New rename approach should produce identical output."""
        from ckvd.utils.config import COLUMN_NAME_MAPPING

        df = _make_df_with_variant_columns(100)

        # Old approach result
        old_df = df.copy()
        for old_name, new_name in COLUMN_NAME_MAPPING.items():
            if old_name in old_df.columns and new_name not in old_df.columns:
                old_df[new_name] = old_df[old_name]
                old_df = old_df.drop(columns=[old_name])

        # New approach result
        from ckvd.utils.config import standardize_column_names

        new_df = standardize_column_names(df.copy())

        # Same columns (possibly different order)
        assert set(old_df.columns) == set(new_df.columns), (
            f"Column mismatch: old={set(old_df.columns)}, new={set(new_df.columns)}"
        )

        # Same data for each column
        for col in old_df.columns:
            pd.testing.assert_series_equal(
                old_df[col].reset_index(drop=True),
                new_df[col].reset_index(drop=True),
                check_names=False,
            )

    def test_no_per_column_copy_in_source(self):
        """standardize_column_names should use rename(), not per-column copy+drop."""
        import inspect

        from ckvd.utils.config import standardize_column_names

        source = inspect.getsource(standardize_column_names)
        assert "df.rename(columns=" in source, "Should use df.rename(columns=...)"
        assert "df = df.drop(columns=" not in source, "Should NOT use per-column df.drop()"


class TestMergeDataframesEmptyFiltering:
    """Verify merge_dataframes filters empty DataFrames before concat."""

    def test_empty_filtering_in_source(self):
        """merge_dataframes should filter empty DataFrames before processing."""
        import inspect

        from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes

        source = inspect.getsource(merge_dataframes)
        assert "non_empty_dfs" in source or "not df.empty" in source, (
            "merge_dataframes should filter empty DataFrames"
        )

    def test_mixed_empty_nonempty(self):
        """merge_dataframes with mix of empty and non-empty should work."""
        from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        valid_df = _make_ohlcv_df(base, 24, source="REST")
        empty_df = pd.DataFrame()

        result = merge_dataframes([empty_df, valid_df, empty_df])

        assert not result.empty
        # Should have data from the valid DataFrame
        assert len(result) == 24

    def test_all_empty_returns_empty(self):
        """merge_dataframes with all empty DataFrames should return empty."""
        from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes

        result = merge_dataframes([pd.DataFrame(), pd.DataFrame()])

        assert result.empty

    def test_single_nonempty_among_empties(self):
        """Single non-empty DataFrame among empties should be returned directly."""
        from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        valid_df = _make_ohlcv_df(base, 48, source="VISION")

        result = merge_dataframes([pd.DataFrame(), valid_df, pd.DataFrame(), pd.DataFrame()])

        assert not result.empty
        assert len(result) == 48

    def test_empty_filtering_benchmark(self):
        """Filtering empties should avoid unnecessary concat overhead."""
        from ckvd.utils.for_core.ckvd_time_range_utils import merge_dataframes

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        valid_df = _make_ohlcv_df(base, 100, source="CACHE")

        # Create list with many empty DataFrames
        dfs = [pd.DataFrame()] * 50 + [valid_df] + [pd.DataFrame()] * 50

        iterations = 200

        def merge_with_empties():
            return merge_dataframes(dfs)

        elapsed = timeit.timeit(merge_with_empties, number=iterations)
        # Should complete quickly since empties are filtered out
        avg_ms = (elapsed / iterations) * 1000
        assert avg_ms < 50, f"merge_dataframes took {avg_ms:.1f}ms avg, expected < 50ms"


class TestStandardizeColumnsCorrectness:
    """Verify standardize_columns still produces correct output after optimizations."""

    def test_output_has_open_time_index(self):
        """standardize_columns should set open_time as index."""
        from ckvd.utils.for_core.ckvd_time_range_utils import standardize_columns

        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        df = _make_ohlcv_df(base, 24)

        result = standardize_columns(df)

        assert result.index.name == "open_time"

    def test_empty_dataframe_passthrough(self):
        """Empty DataFrame should be returned immediately."""
        from ckvd.utils.for_core.ckvd_time_range_utils import standardize_columns

        result = standardize_columns(pd.DataFrame())
        assert result.empty

    def test_variant_columns_renamed(self):
        """Variant column names should be mapped to canonical names."""
        from ckvd.utils.for_core.ckvd_time_range_utils import standardize_columns

        df = _make_df_with_variant_columns(10)
        result = standardize_columns(df)

        # "quote_volume" should be renamed to "quote_asset_volume"
        assert "quote_volume" not in result.columns
        assert "quote_asset_volume" in result.columns
        # "trades" should be renamed to "count"
        assert "trades" not in result.columns
        assert "count" in result.columns
