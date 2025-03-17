#!/usr/bin/env python
"""Tests for cache functionality in DataSourceManager."""

import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
import asyncio

from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import Interval
from tests.utils.cache_test_utils import corrupt_cache_file

# Using the common temp_cache_dir fixture from conftest.py


@pytest.mark.asyncio
async def test_cache_lifecycle(temp_cache_dir):
    """Test complete cache lifecycle including validation and repair."""
    # Initialize manager with cache
    manager = DataSourceManager(cache_dir=temp_cache_dir, use_cache=True)

    # Test parameters
    symbol = "BTCUSDT"
    interval = Interval.SECOND_1
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(minutes=5)

    # Initial fetch and cache
    df1 = await manager.get_data(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        use_cache=True,
    )
    assert not df1.empty, "Initial fetch should return data"

    # Verify cache stats
    stats = manager.get_cache_stats()
    assert stats["misses"] == 1, "First fetch should be a cache miss"
    assert stats["hits"] == 0, "No cache hits yet"
    assert stats["errors"] == 0, "No cache errors"

    # Fetch again to test cache hit
    df2 = await manager.get_data(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        use_cache=True,
    )
    assert not df2.empty, "Second fetch should return data"
    pd.testing.assert_frame_equal(df1, df2, check_dtype=True)

    # Verify updated stats
    stats = manager.get_cache_stats()
    assert stats["hits"] == 1, "Second fetch should be a cache hit"

    # Test cache validation
    is_valid, error = await manager.validate_cache_integrity(
        symbol=symbol, interval=interval.value, date=start_time
    )
    assert is_valid, f"Cache should be valid, got error: {error}"

    # Test cache repair (force by corrupting cache)
    if manager.cache_manager:  # Type check for linter
        cache_path = manager.cache_manager.get_cache_path(
            symbol, interval.value, start_time
        )
        # Use common utility to corrupt the cache file
        corrupt_cache_file(cache_path)

    # Attempt to fetch corrupted data
    df3 = await manager.get_data(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        use_cache=True,
    )
    assert not df3.empty, "Fetch after repair should return data"
    pd.testing.assert_frame_equal(df1, df3, check_dtype=True)

    # Verify error stats
    stats = manager.get_cache_stats()
    assert stats["errors"] > 0, "Should record cache error from corruption"


@pytest.mark.asyncio
async def test_concurrent_cache_access(temp_cache_dir):
    """Test concurrent cache access patterns."""
    manager = DataSourceManager(cache_dir=temp_cache_dir, use_cache=True)
    symbol = "BTCUSDT"
    interval = Interval.SECOND_1

    # Use a single time window for initial data fetch
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(minutes=10)

    # Initial fetch to populate cache
    df_initial = await manager.get_data(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        use_cache=True,
    )
    assert not df_initial.empty, "Initial fetch should return data"

    # Reset cache stats after initial fetch
    manager._cache_stats = {"hits": 0, "misses": 0, "errors": 0}

    # Define time windows for concurrent access (using subsets of the cached data)
    time_windows = [
        (start_time + timedelta(minutes=i), start_time + timedelta(minutes=i + 1))
        for i in range(5)
    ]

    # Concurrent fetches
    async def fetch_data(start_time, end_time):
        return await manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            use_cache=True,
        )

    # Execute concurrent fetches
    results = await asyncio.gather(*[fetch_data(st, et) for st, et in time_windows])

    # Verify results
    for i, df in enumerate(results):
        assert not df.empty, f"Fetch {i} should return data"
        st, et = time_windows[i]
        expected_subset = df_initial[
            (df_initial.index >= st) & (df_initial.index <= et)
        ]
        pd.testing.assert_frame_equal(
            df.sort_index(), expected_subset.sort_index(), check_dtype=True
        )

    # Verify cache performance
    stats = manager.get_cache_stats()
    assert stats["hits"] == 5, "All fetches should be cache hits"
    assert stats["misses"] == 0, "No cache misses expected"
    assert stats["errors"] == 0, "No cache errors expected"


@pytest.mark.asyncio
async def test_cache_disabled_behavior(temp_cache_dir):
    """Test behavior when cache is disabled."""
    # Test with cache disabled
    manager_no_cache = DataSourceManager(cache_dir=temp_cache_dir, use_cache=False)

    symbol = "BTCUSDT"
    interval = Interval.SECOND_1
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(minutes=5)

    # Multiple fetches should bypass cache
    for _ in range(3):
        df = await manager_no_cache.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            use_cache=True,  # Even if True, should be ignored due to manager setting
        )
        assert not df.empty, "Should fetch data successfully"

    # Verify no cache activity
    stats = manager_no_cache.get_cache_stats()
    assert stats["hits"] == 0, "No cache hits when disabled"
    assert stats["misses"] == 0, "No cache misses when disabled"
    assert stats["errors"] == 0, "No cache errors when disabled"


@pytest.mark.asyncio
async def test_cache_data_integrity(temp_cache_dir):
    """Test data integrity through cache operations."""
    manager = DataSourceManager(cache_dir=temp_cache_dir, use_cache=True)

    symbol = "BTCUSDT"
    interval = Interval.SECOND_1
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(minutes=5)

    # Fetch with different data sources
    df_vision = await manager.get_data(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        enforce_source=DataSource.VISION,
    )

    df_rest = await manager.get_data(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        enforce_source=DataSource.REST,
    )

    # Verify data consistency
    assert df_vision.index.dtype == df_rest.index.dtype, "Index dtype should match"
    assert (
        getattr(df_vision.index, "tz", None) == timezone.utc
    ), "Vision data should be in UTC"
    assert (
        getattr(df_rest.index, "tz", None) == timezone.utc
    ), "REST data should be in UTC"

    # Verify column dtypes
    for col, dtype in DataSourceManager.OUTPUT_DTYPES.items():
        assert str(df_vision[col].dtype) == dtype, f"Vision {col} dtype mismatch"
        assert str(df_rest[col].dtype) == dtype, f"REST {col} dtype mismatch"
