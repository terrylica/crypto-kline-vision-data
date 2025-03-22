"""Tests for VisionDataClient cache validation with detailed integrity checks.

System Under Test (SUT):
- core.vision_data_client.VisionDataClient
- core.vision_data_client.CacheMetadata
- core.vision_constraints (indirectly)

This test suite verifies the data integrity aspects of the VisionDataClient's caching system:

1. Cache write and read cycle with data validation
2. Arrow file format integrity and structure
3. Data type consistency through cache operations
4. Index name and timezone preservation
5. Proper handling of timestamp formats

Note on Deprecation Warnings:
----------------------------
These tests intentionally use the deprecated direct caching through VisionDataClient
to ensure backward compatibility during the migration period to UnifiedCacheManager.
The warnings are expected and indicate that the deprecation notices are working as intended.
"""

import pytest
import pandas as pd
import pyarrow as pa
from datetime import datetime, timezone
from pathlib import Path
import logging
from typing import cast

from core.vision_data_client import VisionDataClient
from core.vision_constraints import CANONICAL_INDEX_NAME
from utils.cache_validator import VisionCacheManager
from core.cache_manager import UnifiedCacheManager
from tests.utils.cache_test_utils import verify_arrow_format

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Set to DEBUG for more insights
logger = logging.getLogger(__name__)

# Mark deprecation warnings as expected - these warnings indicate proper migration path
pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")

# Now using the common temp_cache_dir fixture from conftest.py


# Using the sample_ohlcv_data fixture from conftest.py instead
@pytest.fixture
def sample_data(sample_ohlcv_data):
    """Adapt sample data to use the canonical index name."""
    df = sample_ohlcv_data.copy()
    df.index.name = CANONICAL_INDEX_NAME
    return df


@pytest.mark.asyncio
async def test_cache_write_read_cycle(temp_cache_dir, sample_data):
    """Test complete cache write and read cycle."""
    logger.info("Starting cache write/read cycle test")
    logger.debug("Initializing VisionDataClient with caching enabled")

    # Initialize client with cache
    client = VisionDataClient(
        symbol="BTCUSDT", interval="1s", cache_dir=temp_cache_dir, use_cache=True
    )

    # Initialize cache manager directly
    cache_manager = UnifiedCacheManager(temp_cache_dir)

    try:
        # Write sample data to cache
        cache_path = temp_cache_dir / "BTCUSDT" / "1s" / "202201.arrow"
        date = datetime(2022, 1, 13, tzinfo=timezone.utc)

        logger.info(f"Writing sample data to cache: {cache_path}")
        logger.debug(f"Sample data shape: {sample_data.shape}")
        logger.debug(f"Sample data columns: {sample_data.columns.tolist()}")
        logger.debug(f"Sample data index: {sample_data.index.name}")
        logger.debug(f"First few rows:\n{sample_data.head()}")

        # Use VisionCacheManager instead of direct client method
        logger.debug("Using VisionCacheManager to save to cache")
        checksum, record_count = await VisionCacheManager.save_to_cache(
            sample_data, cache_path, date
        )
        logger.info(
            f"Cache write complete. Checksum: {checksum}, Records: {record_count}"
        )

        # Verify cache file
        assert cache_path.exists(), "Cache file not created"
        file_size = cache_path.stat().st_size
        logger.debug(f"Cache file size: {file_size} bytes")

        # Verify Arrow format using common utility
        verify_arrow_format(cache_path, index_name=CANONICAL_INDEX_NAME)

        # Read data back from cache using VisionCacheManager
        logger.info("Reading data from cache")
        loaded_df = await VisionCacheManager.load_from_cache(cache_path)
        logger.debug(f"Loaded data shape: {loaded_df.shape}")
        logger.debug(f"Loaded data columns: {loaded_df.columns.tolist()}")
        logger.debug(f"Loaded data index name: {loaded_df.index.name}")
        logger.debug(f"First few rows:\n{loaded_df.head()}")

        # Compare data frames
        logger.info("Comparing DataFrames")
        logger.debug(f"Original dtypes:\n{sample_data.dtypes}")
        logger.debug(f"Loaded dtypes:\n{loaded_df.dtypes}")

        # Ensure index types match
        assert sample_data.index.dtype == loaded_df.index.dtype, "Index dtype mismatch"

        # Compare values
        pd.testing.assert_frame_equal(
            sample_data.reset_index(),  # Include index in comparison
            loaded_df.reset_index(),
            check_dtype=False,  # Allow some type flexibility
        )
        logger.info("Data integrity check passed")

    finally:
        await client.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_fetch_data_with_cache(temp_cache_dir):
    """Test fetching data with caching enabled."""
    logger.info("Starting fetch data with cache test")
    logger.debug("Initializing VisionDataClient with caching enabled")

    # Initialize client with cache
    client = VisionDataClient(
        symbol="BTCUSDT", interval="1s", cache_dir=temp_cache_dir, use_cache=True
    )

    # Initialize cache manager directly if needed
    cache_manager = UnifiedCacheManager(temp_cache_dir)

    try:
        # Fetch data for a time range
        start_time = datetime(2022, 1, 13, 15, 15, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 14, 15, 45, tzinfo=timezone.utc)

        logger.info(f"Fetching data from {start_time} to {end_time}")
        df = await client.fetch(start_time, end_time)

        logger.debug(f"Fetched data shape: {df.shape}")
        logger.debug(f"Fetched data columns: {df.columns.tolist()}")
        logger.debug(f"Data range: {df.index.min()} to {df.index.max()}")
        logger.debug(f"First few rows:\n{df.head()}")

        # Verify data properties
        assert not df.empty, "Fetched data is empty"
        assert df.index.name == CANONICAL_INDEX_NAME, "Index name mismatch"
        assert df.index.is_monotonic_increasing, "Index not sorted"
        assert (
            cast(pd.DatetimeIndex, df.index).tz == timezone.utc
        ), "Index timezone not UTC"

        # Verify cache was created
        cache_files = list(temp_cache_dir.rglob("*.arrow"))
        logger.debug(f"Created cache files: {[f.name for f in cache_files]}")
        assert len(cache_files) > 0, "No cache files created"

        # Verify cache format
        for cache_file in cache_files:
            verify_arrow_format(cache_file, index_name=CANONICAL_INDEX_NAME)

    finally:
        await client.__aexit__(None, None, None)
