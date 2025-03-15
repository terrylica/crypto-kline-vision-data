"""Tests for unified caching system."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from pathlib import Path
import tempfile
import shutil

from ml_feature_set.utils.logger_setup import get_logger
from ml_feature_set.binance_data_services.utils.market_constraints import Interval
from ml_feature_set.binance_data_services.core.data_source_manager import DataSourceManager

logger = get_logger(__name__, "DEBUG", show_path=False, rich_tracebacks=True)


@pytest.fixture
def temp_cache_dir():
    """Create temporary cache directory with fixed structure."""
    temp_dir = Path(tempfile.mkdtemp())

    # Create fixed directory structure
    (temp_dir / "data").mkdir()
    (temp_dir / "metadata").mkdir()

    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)


@pytest.fixture
def sample_data():
    """Create sample OHLCV data."""
    now = datetime.now(timezone.utc)
    dates = pd.date_range(start=now - timedelta(minutes=5), end=now, freq="1s", tz="UTC")

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


async def test_basic_cache_operations(temp_cache_dir, sample_data):
    """Test basic cache write and read operations."""
    logger.info("Starting basic cache operations test")

    # Initialize manager with cache
    manager = DataSourceManager(cache_dir=temp_cache_dir, use_cache=True)

    # Get current time for data range
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=5)
    end_time = now

    # First fetch - should write to cache
    logger.info("First fetch - should write to cache")
    df1 = await manager.get_data(symbol="BTCUSDT", start_time=start_time, end_time=end_time, interval=Interval.SECOND_1, use_cache=True)

    # Verify cache structure
    data_dir = temp_cache_dir / "data"
    metadata_dir = temp_cache_dir / "metadata"

    assert data_dir.exists(), "Data directory not created"
    assert metadata_dir.exists(), "Metadata directory not created"

    # Check cache files
    cache_files = list(data_dir.rglob("*.arrow"))
    assert len(cache_files) > 0, "No cache files created"

    # Second fetch - should read from cache
    logger.info("Second fetch - should read from cache")
    df2 = await manager.get_data(symbol="BTCUSDT", start_time=start_time, end_time=end_time, interval=Interval.SECOND_1, use_cache=True)

    # Verify data consistency
    pd.testing.assert_frame_equal(df1, df2)


async def test_cache_directory_structure(temp_cache_dir):
    """Test that cache directory structure follows the simplified pattern."""
    logger.info("Starting cache directory structure test")

    manager = DataSourceManager(cache_dir=temp_cache_dir, use_cache=True)

    # Get current time for data range
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=5)
    end_time = now

    # Fetch data to create cache
    await manager.get_data(symbol="BTCUSDT", start_time=start_time, end_time=end_time, interval=Interval.SECOND_1, use_cache=True)

    # Expected structure:
    # /data
    #   /BTCUSDT
    #     /1s
    #       /YYYYMM.arrow
    # /metadata
    #   /cache_index.json

    data_dir = temp_cache_dir / "data"
    metadata_dir = temp_cache_dir / "metadata"

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
        assert len(date_part) == 6, f"Invalid date format in filename: {arrow_file.name}"
        assert date_part.isdigit(), f"Invalid date format in filename: {arrow_file.name}"


async def test_cache_invalidation(temp_cache_dir):
    """Test cache invalidation behavior."""
    logger.info("Starting cache invalidation test")

    manager = DataSourceManager(cache_dir=temp_cache_dir, use_cache=True)

    # Get current time for data range
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=5)
    end_time = now

    # First fetch - create cache
    _df1 = await manager.get_data(symbol="BTCUSDT", start_time=start_time, end_time=end_time, interval=Interval.SECOND_1, use_cache=True)

    # Corrupt cache file
    cache_files = list((temp_cache_dir / "data").rglob("*.arrow"))
    assert len(cache_files) > 0, "No cache files found"

    with open(cache_files[0], "wb") as f:
        f.write(b"corrupted data")

    # Fetch again - should detect corruption and redownload
    df2 = await manager.get_data(symbol="BTCUSDT", start_time=start_time, end_time=end_time, interval=Interval.SECOND_1, use_cache=True)

    assert not df2.empty, "Failed to redownload data after cache corruption"
