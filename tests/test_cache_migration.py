#!/usr/bin/env python
"""Test cache migration and deprecation functionality."""

import pytest
import warnings
from datetime import datetime, timezone, timedelta
import tempfile
from pathlib import Path
import pandas as pd

from ml_feature_set.binance_data_services.core.data_source_manager import DataSourceManager, DataSource
from ml_feature_set.binance_data_services.core.vision_data_client import VisionDataClient
from ml_feature_set.binance_data_services.utils.market_constraints import Interval, MarketType
from ml_feature_set.utils.logger_setup import get_logger

logger = get_logger(__name__, "DEBUG", show_path=False, rich_tracebacks=True)

# Configure pytest-asyncio
pytest_plugins = ("pytest_asyncio",)
pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="function")
def temp_cache_dir():
    """Create temporary cache directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.mark.asyncio
async def test_vision_client_deprecation_warning(temp_cache_dir):
    """Test that VisionDataClient emits deprecation warning when using cache directly."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        _client = VisionDataClient(symbol="BTCUSDT", interval="1s", cache_dir=temp_cache_dir, use_cache=True)

        # Verify deprecation warning was emitted
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "Direct caching through VisionDataClient is deprecated" in str(w[0].message)


@pytest.mark.asyncio
async def test_cache_disabled_through_manager(temp_cache_dir):
    """Test that VisionDataClient caching is disabled when used through DataSourceManager."""
    # Create VisionDataClient with caching enabled
    vision_client = VisionDataClient(symbol="BTCUSDT", interval="1s", cache_dir=temp_cache_dir, use_cache=True)

    # Create DataSourceManager with the vision client
    _manager = DataSourceManager(market_type=MarketType.SPOT, vision_client=vision_client, cache_dir=temp_cache_dir, use_cache=True)

    # Verify vision client's caching is disabled
    assert not vision_client.use_cache
    assert vision_client.cache_dir is None


@pytest.mark.asyncio
async def test_cache_settings_restored(temp_cache_dir):
    """Test that VisionDataClient's original cache settings are restored after manager exit."""
    vision_client = VisionDataClient(symbol="BTCUSDT", interval="1s", cache_dir=temp_cache_dir, use_cache=True)

    original_cache_dir = vision_client.cache_dir
    original_use_cache = vision_client.use_cache

    async with DataSourceManager(market_type=MarketType.SPOT, vision_client=vision_client, cache_dir=temp_cache_dir, use_cache=True):
        # Verify settings are modified during manager use
        assert not vision_client.use_cache
        assert vision_client.cache_dir is None

    # Verify original settings are restored
    assert vision_client.cache_dir == original_cache_dir
    assert vision_client.use_cache == original_use_cache


@pytest.mark.asyncio
async def test_unified_caching_through_manager(temp_cache_dir):
    """Test that caching works through DataSourceManager with UnifiedCacheManager."""
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=1)

    async with DataSourceManager(market_type=MarketType.SPOT, cache_dir=temp_cache_dir, use_cache=True) as manager:
        # First fetch - should download and cache
        df1 = await manager.get_data(
            symbol="BTCUSDT", start_time=start_time, end_time=end_time, interval=Interval.SECOND_1, enforce_source=DataSource.VISION
        )

        # Second fetch - should use cache
        df2 = await manager.get_data(
            symbol="BTCUSDT", start_time=start_time, end_time=end_time, interval=Interval.SECOND_1, enforce_source=DataSource.VISION
        )

        # Verify both results are identical
        pd.testing.assert_frame_equal(df1, df2)

        # Verify cache files exist
        cache_files = list(temp_cache_dir.rglob("*.arrow"))
        assert len(cache_files) > 0, "No cache files were created"


@pytest.mark.asyncio
async def test_mixed_caching_scenario(temp_cache_dir):
    """Test scenario with both direct VisionDataClient and DataSourceManager caching."""
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=1)

    # Create separate cache directories
    legacy_cache_dir = temp_cache_dir / "legacy"
    unified_cache_dir = temp_cache_dir / "unified"
    legacy_cache_dir.mkdir()
    unified_cache_dir.mkdir()

    # Create VisionDataClient with legacy caching
    vision_client = VisionDataClient(symbol="BTCUSDT", interval="1s", cache_dir=legacy_cache_dir, use_cache=True)

    # Use through DataSourceManager with unified caching
    async with DataSourceManager(
        market_type=MarketType.SPOT, vision_client=vision_client, cache_dir=unified_cache_dir, use_cache=True
    ) as manager:
        _df = await manager.get_data(
            symbol="BTCUSDT", start_time=start_time, end_time=end_time, interval=Interval.SECOND_1, enforce_source=DataSource.VISION
        )

        # Verify data was cached in unified cache, not legacy cache
        legacy_cache_files = list(legacy_cache_dir.rglob("*.arrow"))
        unified_cache_files = list(unified_cache_dir.rglob("*.arrow"))

        assert len(legacy_cache_files) == 0, "Data was incorrectly cached in legacy location"
        assert len(unified_cache_files) > 0, "Data was not cached in unified location"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
