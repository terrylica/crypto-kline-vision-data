"""Tests for VisionDataClient cache functionality with detailed validation."""

import pytest
import pandas as pd
import pyarrow as pa
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import shutil
import logging
from typing import cast

from ml_feature_set.binance_data_services.core.vision_data_client import VisionDataClient, CacheMetadata
from ml_feature_set.binance_data_services.core.vision_constraints import CANONICAL_INDEX_NAME

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mark deprecation warnings as expected - these warnings indicate proper migration path
pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning:ml_feature_set.binance_data_services.*")

# Document why we're keeping the deprecated functionality in tests
__doc__ = """
Tests for VisionDataClient cache functionality with detailed validation.

Note on Deprecation Warnings:
----------------------------
These tests intentionally use the deprecated direct caching through VisionDataClient
to ensure backward compatibility during the migration period to UnifiedCacheManager.
The warnings are expected and indicate that the deprecation notices are working as intended.
"""


@pytest.fixture
def temp_cache_dir():
    """Create temporary cache directory."""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_data():
    """Create sample OHLCV data."""
    dates = pd.date_range(start="2022-01-13 15:15:00", end="2022-01-14 15:45:00", freq="15min", tz=timezone.utc)

    df = pd.DataFrame(
        {
            "open": [43798.71] * len(dates),
            "high": [43802.42] * len(dates),
            "low": [43208.00] * len(dates),
            "close": [43312.84] * len(dates),
            "volume": [670.47] * len(dates),
            "close_time": list(range(len(dates))),
            "quote_volume": [29000000.0] * len(dates),
            "trades": [1000] * len(dates),
            "taker_buy_volume": [300.0] * len(dates),
            "taker_buy_quote_volume": [13000000.0] * len(dates),
        },
        index=dates,
    )
    df.index.name = CANONICAL_INDEX_NAME
    return df


def verify_arrow_format(file_path: Path) -> None:
    """Verify Arrow file format in detail."""
    with open(file_path, "rb") as f:
        # Check magic number
        magic = f.read(6)
        logger.info(f"Magic number (hex): {magic.hex()}")
        assert magic == b"ARROW1", f"Invalid magic number: {magic}"

        # Check continuation
        continuation = f.read(2)
        logger.info(f"Continuation: {continuation.hex()}")
        assert continuation == b"\x00\x00", "Invalid continuation bytes"

        # Check metadata length
        metadata_length = int.from_bytes(f.read(4), "little")
        logger.info(f"Metadata length: {metadata_length}")
        # 0xFFFFFFFF (4294967295) is a special value indicating streaming format
        assert metadata_length == 4294967295 or (0 <= metadata_length < 1_000_000), f"Invalid metadata length: {metadata_length}"

        # Read schema
        with pa.ipc.open_file(str(file_path)) as reader:
            schema = reader.schema
            logger.info(f"Schema:\n{schema}")
            assert CANONICAL_INDEX_NAME in str(schema), f"Missing index column: {CANONICAL_INDEX_NAME}"


@pytest.mark.asyncio
async def test_cache_write_read_cycle(temp_cache_dir, sample_data):
    """Test complete cache write and read cycle."""
    logger.info("Starting cache write/read cycle test")

    # Initialize client with cache metadata
    client = VisionDataClient(symbol="BTCUSDT", interval="1s", cache_dir=temp_cache_dir, use_cache=True)
    client.metadata = CacheMetadata(temp_cache_dir)

    try:
        # Write sample data to cache
        cache_path = temp_cache_dir / "BTCUSDT" / "1s" / "202201.arrow"
        date = datetime(2022, 1, 13, tzinfo=timezone.utc)

        logger.info(f"Writing sample data to cache: {cache_path}")
        logger.info(f"Sample data shape: {sample_data.shape}")
        logger.info(f"Sample data columns: {sample_data.columns.tolist()}")
        logger.info(f"Sample data index: {sample_data.index.name}")
        logger.info(f"First few rows:\n{sample_data.head()}")

        checksum, record_count = await client._save_to_cache(sample_data, cache_path, date)
        logger.info(f"Cache write complete. Checksum: {checksum}, Records: {record_count}")

        # Verify cache file
        assert cache_path.exists(), "Cache file not created"
        file_size = cache_path.stat().st_size
        logger.info(f"Cache file size: {file_size} bytes")

        # Verify Arrow format
        verify_arrow_format(cache_path)

        # Read data back from cache
        logger.info("Reading data from cache")
        loaded_df = await client._load_from_cache(cache_path)
        logger.info(f"Loaded data shape: {loaded_df.shape}")
        logger.info(f"Loaded data columns: {loaded_df.columns.tolist()}")
        logger.info(f"Loaded data index name: {loaded_df.index.name}")
        logger.info(f"First few rows:\n{loaded_df.head()}")

        # Compare data frames
        logger.info("Comparing DataFrames")
        logger.info(f"Original dtypes:\n{sample_data.dtypes}")
        logger.info(f"Loaded dtypes:\n{loaded_df.dtypes}")

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

    # Initialize client with cache metadata
    client = VisionDataClient(symbol="BTCUSDT", interval="1s", cache_dir=temp_cache_dir, use_cache=True)
    client.metadata = CacheMetadata(temp_cache_dir)

    try:
        # Fetch data for a time range
        start_time = datetime(2022, 1, 13, 15, 15, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 14, 15, 45, tzinfo=timezone.utc)

        logger.info(f"Fetching data from {start_time} to {end_time}")
        df = await client.fetch(start_time, end_time)

        logger.info(f"Fetched data shape: {df.shape}")
        logger.info(f"Fetched data columns: {df.columns.tolist()}")
        logger.info(f"Data range: {df.index.min()} to {df.index.max()}")
        logger.info(f"First few rows:\n{df.head()}")

        # Verify data properties
        assert not df.empty, "Fetched data is empty"
        assert df.index.name == CANONICAL_INDEX_NAME, "Index name mismatch"
        assert df.index.is_monotonic_increasing, "Index not sorted"
        assert cast(pd.DatetimeIndex, df.index).tz == timezone.utc, "Index timezone not UTC"

        # Verify cache was created
        cache_files = list(temp_cache_dir.rglob("*.arrow"))
        logger.info(f"Created cache files: {[f.name for f in cache_files]}")
        assert len(cache_files) > 0, "No cache files created"

        # Verify cache format
        for cache_file in cache_files:
            verify_arrow_format(cache_file)

    finally:
        await client.__aexit__(None, None, None)
