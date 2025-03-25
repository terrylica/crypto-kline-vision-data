#!/usr/bin/env python
"""Test core caching functionality of DataSourceManager.

System Under Test (SUT):
- core.data_source_manager.DataSourceManager
- core.vision_data_client.VisionDataClient
- core.cache_manager.UnifiedCacheManager

This test suite verifies the core caching system in DataSourceManager:

1. The unified caching system works correctly with VisionDataClient
2. Cache files are stored in the correct locations
3. Data is consistent across multiple fetches
"""

import pytest
from datetime import datetime, timezone, timedelta
import tempfile
from pathlib import Path
import pandas as pd

from core.data_source_manager import DataSourceManager, DataSource
from core.vision_data_client import VisionDataClient
from utils.market_constraints import Interval, MarketType
from utils.logger_setup import get_logger

logger = get_logger(__name__, "DEBUG", show_path=False)

# Configure pytest-asyncio
pytest_plugins = ("pytest_asyncio",)
pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="function")
def temp_cache_dir():
    """Create temporary cache directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.mark.asyncio
async def test_unified_caching_through_manager(temp_cache_dir, caplog):
    """Test that caching works through DataSourceManager with UnifiedCacheManager."""
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=1)

    async with DataSourceManager(
        market_type=MarketType.SPOT, cache_dir=temp_cache_dir, use_cache=True
    ) as manager:
        # First fetch - should download and cache
        df1 = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            enforce_source=DataSource.VISION,
        )

        # Check for cache miss in the first fetch
        assert any(
            "Cache miss" in record.message for record in caplog.records
        ), "No cache miss log on first fetch"

        # Check that we successfully cached data
        assert any(
            record.message.startswith("Cached") for record in caplog.records
        ), "No cache creation log messages found"

        # Clear log records before second fetch
        caplog.clear()

        # Second fetch - should use cache
        df2 = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            enforce_source=DataSource.VISION,
        )

        # Check for cache hit in the second fetch
        assert not any(
            "Cache miss" in record.message for record in caplog.records
        ), "Unexpected cache miss on second fetch"

        # Verify both results are identical
        pd.testing.assert_frame_equal(df1, df2)

        # Verify cache files exist
        cache_files = list(temp_cache_dir.rglob("*.arrow"))
        assert len(cache_files) > 0, "No cache files were created"


@pytest.mark.asyncio
async def test_caching_directory_structure(temp_cache_dir, caplog):
    """Test that cache files are stored in the correct directory structure."""
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=1)

    # Create cache directory
    unified_cache_dir = temp_cache_dir / "unified"
    unified_cache_dir.mkdir()

    # Use async context manager to properly manage resources
    async with VisionDataClient(
        symbol="BTCUSDT", interval="1s", use_cache=False
    ) as vision_client:
        # Use through DataSourceManager with unified caching
        async with DataSourceManager(
            market_type=MarketType.SPOT,
            vision_client=vision_client,
            cache_dir=unified_cache_dir,
            use_cache=True,
        ) as manager:
            _df = await manager.get_data(
                symbol="BTCUSDT",
                start_time=start_time,
                end_time=end_time,
                interval=Interval.SECOND_1,
                enforce_source=DataSource.VISION,
            )

            # Verify cache directory structure
            unified_cache_files = list(unified_cache_dir.rglob("*.arrow"))
            assert (
                len(unified_cache_files) > 0
            ), "Data was not cached in unified location"

            # Check for expected directory structure
            # (data/binance/spot/klines/daily/BTCUSDT/1s/...)
            data_dir = unified_cache_dir / "data"
            assert data_dir.exists(), "Data directory not created"

            # Check for the new directory structure components
            exchange_dir = data_dir / "binance"
            assert exchange_dir.exists(), "Exchange directory not created"

            market_type_dir = exchange_dir / "spot"
            assert market_type_dir.exists(), "Market type directory not created"

            data_nature_dir = market_type_dir / "klines"
            assert data_nature_dir.exists(), "Data nature directory not created"

            packaging_dir = data_nature_dir / "daily"
            assert packaging_dir.exists(), "Packaging frequency directory not created"

            btc_dir = packaging_dir / "BTCUSDT"
            assert btc_dir.exists(), "Symbol directory not created"

            interval_dir = btc_dir / "1s"
            assert interval_dir.exists(), "Interval directory not created"

            # Check for cache files in the interval directory
            interval_cache_files = list(interval_dir.glob("*.arrow"))
            assert len(interval_cache_files) > 0, "No cache files in interval directory"

            # Check for cache-related log messages
            assert any(
                record.message.startswith("Cached") for record in caplog.records
            ), "No cache logging messages found"

            # Verify we got successful cache creation logs
            arrow_file_path = str(interval_cache_files[0])
            assert any(
                arrow_file_path.endswith(record.message.split("to ")[-1].strip())
                for record in caplog.records
                if record.message.startswith("Cached") and "to " in record.message
            ), "No log message for cache file creation"
    # The async context manager will properly close all resources here


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
