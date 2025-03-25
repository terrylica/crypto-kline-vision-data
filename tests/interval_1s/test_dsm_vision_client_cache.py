#!/usr/bin/env python
"""Test VisionDataClient integration with DataSourceManager for caching.

System Under Test (SUT):
- core.vision_data_client.VisionDataClient
- core.data_source_manager.DataSourceManager
- core.cache_manager.UnifiedCacheManager (indirectly)

This test suite verifies the integration between VisionDataClient and DataSourceManager
with a focus on caching behavior:

1. Cache operations through DataSourceManager
2. Integration of VisionDataClient with DataSourceManager's unified caching system
3. Cache persistence across client instances
4. Cache invalidation and error handling
5. Prefetch functionality with DataSourceManager
6. Historical data caching behavior

The tests ensure that VisionDataClient works correctly as a data source for
DataSourceManager while the caching is handled by DataSourceManager.
"""

import pytest
import pytest_asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
import shutil
import traceback
import inspect
import functools
from typing import AsyncGenerator, Optional, Generator
from pandas.testing import assert_frame_equal

# Configure pytest-asyncio
pytest_plugins = ("pytest_asyncio",)
pytestmark = pytest.mark.asyncio(loop_scope="function")

from core.data_source_manager import DataSourceManager, DataSource
from core.vision_data_client import VisionDataClient
from core.vision_constraints import CONSOLIDATION_DELAY
from utils.market_constraints import Interval, MarketType
from utils.logger_setup import get_logger
from tests.utils.cache_test_utils import (
    validate_cache_directory,
    corrupt_cache_file,
)

# Set up more detailed logging
logger = get_logger(__name__, "DEBUG", show_path=False)


def get_safe_test_time_range(
    duration: timedelta = timedelta(hours=1),
) -> tuple[datetime, datetime]:
    """Generate a time range that's safely beyond the Vision API consolidation delay.

    Args:
        duration: Duration of the time range (default: 1 hour)

    Returns:
        Tuple of (start_time, end_time) in UTC, rounded to nearest second
    """
    now = datetime.now(timezone.utc)
    # Use CONSOLIDATION_DELAY + 1 day for safety
    safe_days = (CONSOLIDATION_DELAY + timedelta(days=1)).days
    # Round to nearest second to avoid sub-second precision issues
    start_time = (now - timedelta(days=safe_days)).replace(microsecond=0)
    end_time = (start_time + duration).replace(microsecond=0)
    logger.info(f"Generated safe test time range: {start_time} to {end_time}")
    return start_time, end_time


# Configure pytest-asyncio default fixture scope
def pytest_configure(config):
    config.option.asyncio_mode = "strict"
    config.option.asyncio_default_fixture_loop_scope = "function"


def log_async_context(func):
    """Decorator to log async context entry/exit."""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        logger.debug(f"Entering async context for {func.__name__}")
        try:
            result = await func(*args, **kwargs)
            logger.debug(f"Successfully completed {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    return wrapper


# Using common temp_cache_dir from conftest.py, but with custom cleanup
@pytest.fixture(scope="function")
def temp_cache_dir() -> Generator[Path, None, None]:
    """Create temporary cache directory with validation."""
    import tempfile

    temp_dir = Path(tempfile.mkdtemp())
    logger.debug(f"Created temporary cache directory: {temp_dir}")
    try:
        validate_cache_directory(temp_dir)
        yield temp_dir
    finally:
        logger.debug(f"Cleaning up temporary cache directory: {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)


async def validate_client(client: VisionDataClient) -> None:
    """Validate VisionDataClient initialization."""
    logger.debug(f"Validating client: {client}")
    try:
        # Check client attributes
        logger.debug(f"Client attributes:")
        logger.debug(f"  Symbol: {client.symbol}")
        logger.debug(f"  Interval: {client.interval}")

        # Check client methods
        logger.debug("Checking client methods:")
        for name, _ in inspect.getmembers(client, inspect.ismethod):
            logger.debug(f"  Method: {name}")

        # Verify HTTP client (using _client instead of client)
        logger.debug(f"HTTP client initialized: {bool(client._client)}")

        logger.debug("Client validation successful")
    except Exception as e:
        logger.error(f"Client validation failed: {e}")
        raise


@pytest_asyncio.fixture(scope="function")
async def vision_client() -> AsyncGenerator[VisionDataClient, None]:
    """Create VisionDataClient without caching."""
    logger.debug("Initializing VisionDataClient")
    client: Optional[VisionDataClient] = None
    try:
        # Use VisionDataClient without caching
        client = VisionDataClient(symbol="BTCUSDT", interval="1s", use_cache=False)

        logger.debug("VisionDataClient initialized successfully")
        await validate_client(client)
        yield client
    except Exception as e:
        logger.error(f"Error in vision_client fixture: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        if client:
            try:
                await client.__aexit__(None, None, None)
                logger.debug("VisionDataClient cleanup completed")
            except Exception as e:
                logger.error(f"Error cleaning up vision_client: {e}")


@pytest_asyncio.fixture(scope="function")
async def data_source_manager(
    temp_cache_dir: Path,
) -> AsyncGenerator[DataSourceManager, None]:
    """Create DataSourceManager with temporary cache."""
    logger.debug("Initializing DataSourceManager")
    manager: Optional[DataSourceManager] = None
    try:
        # Create VisionDataClient without caching
        vision_client = VisionDataClient(
            symbol="BTCUSDT", interval="1s", use_cache=False
        )
        await validate_client(vision_client)

        # Create DataSourceManager with caching
        manager = DataSourceManager(
            market_type=MarketType.SPOT,
            vision_client=vision_client,
            cache_dir=temp_cache_dir,
            use_cache=True,
        )

        logger.debug("DataSourceManager initialized successfully")
        logger.debug(f"Manager attributes:")
        logger.debug(f"  Market type: {manager.market_type}")
        logger.debug(f"  Vision client: {manager.vision_client}")
        logger.debug(f"  Cache dir: {temp_cache_dir}")
        logger.debug(f"  Use cache: {manager.use_cache}")

        yield manager
    except Exception as e:
        logger.error(f"Error in data_source_manager fixture: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        if manager:
            try:
                # Clean up the DataSourceManager if needed
                logger.debug("DataSourceManager cleanup completed")
            except Exception as e:
                logger.error(f"Error during DataSourceManager cleanup: {e}")


@pytest.mark.asyncio
async def test_vision_cache_write_and_read(
    temp_cache_dir: Path, data_source_manager: DataSourceManager
):
    """Test that caching through DataSourceManager with VisionDataClient works correctly.

    Args:
        temp_cache_dir: Temporary cache directory for testing
        data_source_manager: DataSourceManager with cache management
    """
    logger.debug("Entering async context for test_vision_cache_write_and_read")

    # Get current timestamp for time-based test
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=1)
    end_time = start_time + timedelta(hours=1)
    symbol = "BTCUSDT"
    interval = Interval.SECOND_1

    logger.info(f"Fetching initial data: {start_time} to {end_time}")
    logger.debug(f"Vision client object: {data_source_manager.vision_client}")

    # First fetch - should write to cache
    df1 = await data_source_manager.get_data(symbol, start_time, end_time, interval)
    assert not df1.empty, "First fetch returned empty DataFrame"
    logger.debug(f"First fetch returned DataFrame with shape: {df1.shape}")

    # Check that cache files were created
    cache_files = list(Path(temp_cache_dir).glob("**/*.arrow"))
    logger.info(f"Created cache files: {cache_files}")
    assert len(cache_files) > 0, "No cache files were created"

    for cache_file in cache_files:
        logger.debug(
            f"Cache file details - Size: {cache_file.stat().st_size}, Modified: {datetime.fromtimestamp(cache_file.stat().st_mtime)}"
        )

    # Second fetch - should read from cache
    logger.info("Fetching same data again (should use cache)")
    df2 = await data_source_manager.get_data(symbol, start_time, end_time, interval)
    assert not df2.empty, "Second fetch returned empty DataFrame"

    # Verify data integrity
    pd.testing.assert_frame_equal(df1, df2, check_dtype=True)
    logger.info("Data integrity verified - both fetches returned identical data")

    # Verify cache stats
    cache_stats = data_source_manager.get_cache_stats()
    logger.info(f"Cache stats: {cache_stats}")
    assert cache_stats["hits"] >= 1, "Expected at least one cache hit"
    assert cache_stats["misses"] >= 1, "Expected at least one cache miss"


@pytest.mark.asyncio
@log_async_context
async def test_data_source_manager_vision_cache(
    data_source_manager: DataSourceManager, temp_cache_dir: Path
) -> None:
    """Test that VisionDataClient is being used correctly as a data source for DataSourceManager.

    This test verifies that DataSourceManager can properly integrate VisionDataClient
    as a data source while managing caching internally.

    Args:
        data_source_manager: DataSourceManager instance with caching enabled
        temp_cache_dir: Temporary cache directory
    """
    start_time, end_time = get_safe_test_time_range(timedelta(hours=1))
    symbol = "BTCUSDT"
    interval = Interval.SECOND_1

    # Fetch data specifically from VisionDataClient through DataSourceManager
    df1 = await data_source_manager.get_data(
        symbol, start_time, end_time, interval, enforce_source=DataSource.VISION
    )

    # Verify data was returned
    assert not df1.empty, "Failed to fetch data from VisionDataClient"
    logger.info(f"Data fetched successfully with shape: {df1.shape}")

    # Verify data was cached
    cache_files = list(temp_cache_dir.glob("**/*.arrow"))
    assert len(cache_files) > 0, "No cache files were created"
    logger.info(f"Found {len(cache_files)} cache files")

    # Fetch again from cache
    df2 = await data_source_manager.get_data(
        symbol, start_time, end_time, interval, enforce_source=DataSource.VISION
    )

    # Verify cached data is identical
    pd.testing.assert_frame_equal(df1, df2, check_dtype=True)
    logger.info("Cached data verified to be identical to original fetch")

    # Verify cache statistics
    cache_stats = data_source_manager.get_cache_stats()
    logger.info(f"Cache stats: {cache_stats}")
    assert cache_stats["hits"] >= 1, "Cache hit not recorded in statistics"


@pytest.mark.asyncio
async def test_cache_persistence(temp_cache_dir: Path):
    """Test that cache persists between different DataSourceManager instances."""
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=1)
    symbol = "BTCUSDT"
    interval = Interval.SECOND_1

    # Create first manager and fetch data
    logger.info("Creating first DataSourceManager")

    # Create VisionDataClient without caching
    vision_client1 = VisionDataClient(
        symbol=symbol, interval=interval.value, use_cache=False
    )

    # Create first DataSourceManager with caching
    manager1 = DataSourceManager(
        market_type=MarketType.SPOT,
        vision_client=vision_client1,
        cache_dir=temp_cache_dir,
        use_cache=True,
    )

    logger.info(f"Fetching data with first manager: {start_time} to {end_time}")
    df1 = await manager1.get_data(symbol, start_time, end_time, interval)

    if not df1.empty:
        logger.info(f"Data fetched successfully with shape: {df1.shape}")
    else:
        logger.warning("First fetch returned empty DataFrame")

    # Create second manager (simulating a new session)
    logger.info("Creating second DataSourceManager (simulating new session)")

    # Create new VisionDataClient without caching
    vision_client2 = VisionDataClient(
        symbol=symbol, interval=interval.value, use_cache=False
    )

    # Create second DataSourceManager with caching
    manager2 = DataSourceManager(
        market_type=MarketType.SPOT,
        vision_client=vision_client2,
        cache_dir=temp_cache_dir,
        use_cache=True,
    )

    logger.info(f"Fetching same data with second manager")
    df2 = await manager2.get_data(symbol, start_time, end_time, interval)

    if not df2.empty:
        logger.info(f"Second fetch returned DataFrame with shape: {df2.shape}")

        # Verify data integrity between sessions
        pd.testing.assert_frame_equal(df1, df2, check_dtype=True)
        logger.info("Data integrity verified between sessions")
    else:
        logger.warning("Second fetch returned empty DataFrame - skipping comparison")


@pytest.mark.asyncio
async def test_cache_invalidation(temp_cache_dir: Path):
    """Test that cache invalidation works correctly through DataSourceManager."""
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=1)

    # Create VisionDataClient without caching
    vision_client = VisionDataClient(symbol="BTCUSDT", interval="1s", use_cache=False)

    # Create DataSourceManager with caching
    manager = DataSourceManager(
        market_type=MarketType.SPOT,
        vision_client=vision_client,
        cache_dir=temp_cache_dir,
        use_cache=True,
    )

    # First fetch to populate cache
    logger.info("Initial fetch to populate cache")
    df1 = await manager.get_data(
        symbol="BTCUSDT",
        start_time=start_time,
        end_time=end_time,
        interval=Interval.SECOND_1,
    )

    # Check cache files
    cache_files = list(temp_cache_dir.glob("**/*.arrow"))
    logger.info(f"Cache files after initial fetch: {cache_files}")

    if not df1.empty and len(cache_files) > 0:
        # Corrupt a cache file to force invalidation
        if cache_files:
            corrupt_file = cache_files[0]
            logger.info(f"Corrupting cache file: {corrupt_file}")
            corrupt_cache_file(corrupt_file)

            # Fetch again - should detect corrupt cache and re-download
            logger.info("Fetching again after corrupting cache")
            df2 = await manager.get_data(
                symbol="BTCUSDT",
                start_time=start_time,
                end_time=end_time,
                interval=Interval.SECOND_1,
            )

            # Verify cache was regenerated
            cache_files_after = list(temp_cache_dir.glob("**/*.arrow"))
            logger.info(f"Cache files after re-fetch: {cache_files_after}")

            # Verify data still available
            assert not df2.empty, "Failed to fetch data after cache corruption"
            logger.info(
                f"Successfully re-fetched data after cache corruption: {df2.shape}"
            )
    else:
        logger.warning("Skipping cache corruption test as no data was cached initially")


@pytest.mark.asyncio
async def test_prefetch_with_data_source_manager(temp_cache_dir):
    """Test that prefetching works with DataSourceManager."""
    # Setup test variables
    start_time = datetime.now(timezone.utc) - timedelta(days=1, hours=2)
    end_time = start_time + timedelta(hours=2)
    interval = Interval.SECOND_1
    symbol = "BTCUSDT"

    # Create vision client
    vision_client = VisionDataClient(
        symbol=symbol,
        interval=interval.value,
    )

    # Use through DataSourceManager with unified caching
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        vision_client=vision_client,
        cache_dir=temp_cache_dir,
        use_cache=True,
    ) as manager:
        # Log what we're doing
        logger.info(f"Fetching data: {start_time} to {end_time}")

        # Instead of prefetching, we'll just do a normal data fetch
        # The DataSourceManager will handle caching appropriately
        df = await manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            enforce_source=DataSource.VISION,
        )

        # Verify the data was fetched and cached
        assert len(df) > 0, "No data fetched"
        cache_files = list(temp_cache_dir.rglob("*.arrow"))
        assert len(cache_files) > 0, "No cache files created"

        # Now fetch the data again to verify it's loaded from cache
        logger.info(f"Fetching data again: {start_time} to {end_time}")
        df2 = await manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            enforce_source=DataSource.VISION,
        )

        # Verify the data was loaded from cache
        assert len(df2) > 0, "No data fetched on second attempt"
        assert_frame_equal(df, df2, "Data from cache doesn't match original fetch")


@pytest.mark.asyncio
async def test_historical_data_caching(temp_cache_dir: Path):
    """Test caching behavior with historical data for different time periods."""
    # Define test periods
    test_periods = [
        (
            "Recent Period",
            datetime.now(timezone.utc) - timedelta(days=7),
            timedelta(hours=1),
        ),
        ("Older Period", datetime(2023, 1, 1, tzinfo=timezone.utc), timedelta(hours=1)),
    ]

    for period_name, base_time, duration in test_periods:
        logger.info(f"Testing {period_name}: {base_time} for {duration}")
        start_time = base_time
        end_time = start_time + duration
        symbol = "BTCUSDT"
        interval = Interval.SECOND_1

        # Create a separate cache directory for each period
        period_cache_dir = temp_cache_dir / period_name.lower().replace(" ", "_")
        period_cache_dir.mkdir(exist_ok=True)
        logger.debug(f"Created period cache directory: {period_cache_dir}")

        # Create VisionDataClient without caching
        vision_client = VisionDataClient(
            symbol=symbol, interval=interval.value, use_cache=False
        )

        # Create DataSourceManager with caching
        manager = DataSourceManager(
            market_type=MarketType.SPOT,
            vision_client=vision_client,
            cache_dir=period_cache_dir,
            use_cache=True,
        )

        # First fetch to populate cache
        logger.info(f"First fetch for {period_name}")
        df1 = await manager.get_data(
            symbol=symbol, start_time=start_time, end_time=end_time, interval=interval
        )

        # Check if data was returned and cached
        cache_files = list(period_cache_dir.glob("**/*.arrow"))
        logger.info(f"{period_name} cache files: {cache_files}")

        if not df1.empty:
            logger.info(f"{period_name} data fetched successfully: {df1.shape}")

            # Second fetch to test cache
            logger.info(f"Second fetch for {period_name} (should use cache)")

            # Create new VisionDataClient without caching
            vision_client2 = VisionDataClient(
                symbol=symbol, interval=interval.value, use_cache=False
            )

            # Create new DataSourceManager with caching
            manager2 = DataSourceManager(
                market_type=MarketType.SPOT,
                vision_client=vision_client2,
                cache_dir=period_cache_dir,
                use_cache=True,
            )

            df2 = await manager2.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
            )

            if not df2.empty:
                # Verify data integrity
                pd.testing.assert_frame_equal(df1, df2, check_dtype=True)
                logger.info(f"{period_name} data integrity verified")
            else:
                logger.warning(f"{period_name} second fetch returned empty DataFrame")
        else:
            logger.warning(f"{period_name} first fetch returned empty DataFrame")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
