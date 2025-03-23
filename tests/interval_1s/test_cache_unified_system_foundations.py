"""Tests for unified caching system.

System Under Test (SUT):
- core.data_source_manager.DataSourceManager
- core.cache_manager.UnifiedCacheManager
- utils.cache_utils (indirectly)

This test suite verifies the foundational aspects of the unified caching system:

1. Basic cache operations (read/write)
2. Cache directory structure and organization
3. Cache file format and metadata
4. Cache invalidation and error handling
5. Data integrity through cache operations

The unified caching system provides a consistent interface for caching market data
across different data sources, with proper directory structure, metadata tracking,
and data validation to ensure reliability and performance.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from pathlib import Path

from utils.logger_setup import get_logger
from utils.market_constraints import Interval
from core.data_source_manager import DataSourceManager
from tests.utils.cache_test_utils import corrupt_cache_file

logger = get_logger(__name__, "DEBUG", show_path=False)

# Using common temp_cache_dir fixture from conftest.py


@pytest.fixture
def structured_cache_dir(temp_cache_dir):
    """Create structured cache directory with fixed structure."""
    # Create fixed directory structure on top of the temp dir
    (temp_cache_dir / "data").mkdir()
    (temp_cache_dir / "metadata").mkdir()
    return temp_cache_dir


@pytest.fixture
def sample_data():
    """Create sample OHLCV data with random values."""
    now = datetime.now(timezone.utc)
    dates = pd.date_range(
        start=now - timedelta(minutes=5), end=now, freq="1s", tz="UTC"
    )

    return pd.DataFrame(
        {
            "open": np.random.random(len(dates)) * 100 + 20000,
            "high": np.random.random(len(dates)) * 100 + 20100,
            "low": np.random.random(len(dates)) * 100 + 19900,
            "close": np.random.random(len(dates)) * 100 + 20000,
            "volume": np.random.random(len(dates)) * 10,
            "close_time": [int(d.timestamp() * 1000) for d in dates],
            "quote_volume": np.random.random(len(dates)) * 200000,
            "trades": np.random.randint(100, 1000, len(dates)),
            "taker_buy_volume": np.random.random(len(dates)) * 5,
            "taker_buy_quote_volume": np.random.random(len(dates)) * 100000,
        },
        index=dates,
    )


async def test_basic_cache_operations(structured_cache_dir, sample_data):
    """Test basic cache write and read operations."""
    logger.info("Starting basic cache operations test")

    # Initialize manager with cache
    manager = DataSourceManager(cache_dir=structured_cache_dir, use_cache=True)

    # Get current time for data range
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=5)
    end_time = now

    # First fetch - should write to cache
    logger.info("First fetch - should write to cache")
    df1 = await manager.get_data(
        symbol="BTCUSDT",
        start_time=start_time,
        end_time=end_time,
        interval=Interval.SECOND_1,
        use_cache=True,
    )

    # Verify cache structure
    data_dir = structured_cache_dir / "data"
    metadata_dir = structured_cache_dir / "metadata"

    assert data_dir.exists(), "Data directory not created"
    assert metadata_dir.exists(), "Metadata directory not created"

    # Check cache files
    cache_files = list(data_dir.rglob("*.arrow"))
    assert len(cache_files) > 0, "No cache files created"

    # Second fetch - should read from cache
    logger.info("Second fetch - should read from cache")
    df2 = await manager.get_data(
        symbol="BTCUSDT",
        start_time=start_time,
        end_time=end_time,
        interval=Interval.SECOND_1,
        use_cache=True,
    )

    # Log the shapes for debugging
    logger.debug(f"First fetch shape: {df1.shape}, Second fetch shape: {df2.shape}")

    # Remove duplicates from both DataFrames to ensure fair comparison
    df1_clean = (
        df1.reset_index()
        .drop_duplicates(subset=["open_time"])
        .set_index("open_time")
        .sort_index()
    )
    df2_clean = (
        df2.reset_index()
        .drop_duplicates(subset=["open_time"])
        .set_index("open_time")
        .sort_index()
    )

    logger.debug(
        f"After duplicate removal - df1: {df1_clean.shape}, df2: {df2_clean.shape}"
    )

    # Now both DataFrames should have the same number of rows
    # Get the actual time range from both DataFrames to ensure we compare the same time window
    common_start = max(df1_clean.index.min(), df2_clean.index.min())
    common_end = min(df1_clean.index.max(), df2_clean.index.max())
    logger.debug(f"Using common time range: {common_start} to {common_end}")

    # Filter both DataFrames to the common time range
    filtered_df1 = df1_clean[
        (df1_clean.index >= common_start) & (df1_clean.index <= common_end)
    ]
    filtered_df2 = df2_clean[
        (df2_clean.index >= common_start) & (df2_clean.index <= common_end)
    ]

    logger.debug(f"Final shapes - df1: {filtered_df1.shape}, df2: {filtered_df2.shape}")

    # Verify data consistency within the common time range
    pd.testing.assert_frame_equal(
        filtered_df1, filtered_df2, check_dtype=True, check_index_type=True
    )


async def test_cache_directory_structure(structured_cache_dir):
    """Test that cache directory structure follows the simplified pattern."""
    logger.info("Starting cache directory structure test")

    manager = DataSourceManager(cache_dir=structured_cache_dir, use_cache=True)

    # Get current time for data range
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=5)
    end_time = now

    # Fetch data to create cache
    await manager.get_data(
        symbol="BTCUSDT",
        start_time=start_time,
        end_time=end_time,
        interval=Interval.SECOND_1,
        use_cache=True,
    )

    # Expected structure:
    # /data
    #   /BTCUSDT
    #     /1s
    #       /YYYYMM.arrow
    # /metadata
    #   /cache_index.json

    data_dir = structured_cache_dir / "data"
    metadata_dir = structured_cache_dir / "metadata"

    assert data_dir.exists(), "Data directory not created"
    assert metadata_dir.exists(), "Metadata directory not created"
    assert (metadata_dir / "cache_index.json").exists(), "Cache index not created"

    symbol_dir = data_dir / "BTCUSDT"
    assert symbol_dir.exists(), "Symbol directory not created"

    interval_dir = symbol_dir / "1s"
    assert interval_dir.exists(), "Interval directory not created"

    arrow_files = list(interval_dir.glob("*.arrow"))
    assert len(arrow_files) > 0, "No Arrow files created"

    # Verify Arrow file naming
    for arrow_file in arrow_files:
        assert arrow_file.name.endswith(".arrow"), "Invalid file extension"
        date_part = arrow_file.stem
        # Should be in YYYYMM format
        assert (
            len(date_part) == 6
        ), f"Invalid date format in filename: {arrow_file.name}"
        assert (
            date_part.isdigit()
        ), f"Invalid date format in filename: {arrow_file.name}"


async def test_cache_invalidation(structured_cache_dir):
    """Test cache invalidation behavior."""
    logger.info("Starting cache invalidation test")

    manager = DataSourceManager(cache_dir=structured_cache_dir, use_cache=True)

    # Get current time for data range
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=5)
    end_time = now

    # First fetch - create cache
    _df1 = await manager.get_data(
        symbol="BTCUSDT",
        start_time=start_time,
        end_time=end_time,
        interval=Interval.SECOND_1,
        use_cache=True,
    )

    # Corrupt cache file
    cache_files = list((structured_cache_dir / "data").rglob("*.arrow"))
    assert len(cache_files) > 0, "No cache files found"

    # Use common utility to corrupt the cache file
    corrupt_cache_file(cache_files[0])

    # Fetch again - should detect corruption and redownload
    df2 = await manager.get_data(
        symbol="BTCUSDT",
        start_time=start_time,
        end_time=end_time,
        interval=Interval.SECOND_1,
        use_cache=True,
    )

    assert not df2.empty, "Failed to redownload data after cache corruption"
