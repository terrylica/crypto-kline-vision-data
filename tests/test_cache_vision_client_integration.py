#!/usr/bin/env python
"""Test VisionDataClient caching behavior and integration with DataSourceManager."""

import pytest
import pytest_asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
import shutil
import traceback
import inspect
import os
import functools
from typing import AsyncGenerator, Optional, Generator
import asyncio

# Configure pytest-asyncio
pytest_plugins = ("pytest_asyncio",)
pytestmark = pytest.mark.asyncio

from core.data_source_manager import DataSourceManager, DataSource
from core.vision_data_client import VisionDataClient
from core.vision_constraints import CONSOLIDATION_DELAY
from utils.market_constraints import Interval, MarketType
from utils.logger_setup import get_logger
from tests.utils.cache_test_utils import (
    validate_cache_directory,
    corrupt_cache_file,
    wait_for_cache_file_change,
)

# Set up more detailed logging
logger = get_logger(__name__, "DEBUG", show_path=False, rich_tracebacks=True)


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
        logger.debug(f"  Cache dir: {client.cache_dir}")
        logger.debug(f"  Use cache: {client.use_cache}")

        # Check client methods
        logger.debug("Checking client methods:")
        for name, _ in inspect.getmembers(client, inspect.ismethod):
            logger.debug(f"  Method: {name}")

        # Verify HTTP client
        logger.debug(f"HTTP client initialized: {bool(client.client)}")

        logger.debug("Client validation successful")
    except Exception as e:
        logger.error(f"Client validation failed: {e}")
        raise


@pytest_asyncio.fixture(scope="function")
async def vision_client(temp_cache_dir: Path) -> AsyncGenerator[VisionDataClient, None]:
    """Create VisionDataClient with temporary cache."""
    logger.debug("Initializing VisionDataClient")
    client: Optional[VisionDataClient] = None
    try:
        # Instead of direct caching with VisionDataClient
        # client = VisionDataClient(symbol="BTCUSDT", interval="1s", cache_dir=temp_cache_dir, use_cache=True)

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
    """Test that VisionDataClient cache reading and writing works correctly.

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
    logger.debug(f"Second fetch returned DataFrame with shape: {df2.shape}")

    # Verify data integrity
    pd.testing.assert_frame_equal(df1, df2, check_dtype=True, check_index_type=True)
    logger.info("Both fetches returned identical data")

    logger.debug("Successfully completed test_vision_cache_write_and_read")


@pytest.mark.asyncio
@log_async_context
async def test_data_source_manager_vision_cache(
    data_source_manager: DataSourceManager, temp_cache_dir: Path
) -> None:
    """Test that DataSourceManager properly uses Vision API cache."""
    # Test with recent data
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=1)

    logger.info(f"Initial fetch with DataSourceManager: {start_time} to {end_time}")
    logger.debug(f"DataSourceManager object: {data_source_manager}")

    try:
        # First fetch with Vision API enforced
        df1 = await data_source_manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            enforce_source=DataSource.VISION,
        )
        assert not df1.empty, "Initial fetch returned empty DataFrame"
        logger.debug(f"First fetch returned DataFrame with shape: {df1.shape}")

        # Verify cache was created
        cache_files = list(temp_cache_dir.glob("**/*.arrow"))
        assert len(cache_files) > 0, "No cache files were created"
        logger.info(f"Created cache files: {cache_files}")
        for cf in cache_files:
            logger.debug(
                f"Cache file details - Size: {cf.stat().st_size}, Modified: {datetime.fromtimestamp(cf.stat().st_mtime)}"
            )

        # Second fetch - should use cache
        logger.info("Fetching same data again (should use cache)")
        df2 = await data_source_manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            enforce_source=DataSource.VISION,
        )
        logger.debug(f"Second fetch returned DataFrame with shape: {df2.shape}")

        # Verify both results are identical
        pd.testing.assert_frame_equal(df1, df2, check_dtype=True, check_index_type=True)
        logger.info("Both fetches returned identical data")
    except Exception as e:
        logger.error(f"Error in test_data_source_manager_vision_cache: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise


@pytest.mark.asyncio
async def test_cache_persistence(temp_cache_dir: Path):
    """Test that cache persists between different client instances."""
    # Test data from a recent past date (within Vision API availability)
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(hours=1)
    symbol = "BTCUSDT"
    interval = Interval.SECOND_1

    logger.debug("Entering async context for test_cache_persistence")

    # Create first DataSourceManager instance
    original_vision_client = VisionDataClient(
        symbol=symbol, interval=interval.value, use_cache=False
    )
    await validate_client(original_vision_client)

    original_manager = DataSourceManager(
        market_type=MarketType.SPOT,
        vision_client=original_vision_client,
        cache_dir=temp_cache_dir,
        use_cache=True,
    )

    logger.info("Fetching data with first client instance")
    logger.debug(f"Original vision client object: {original_vision_client}")

    # First fetch - should download and cache
    df1 = await original_manager.get_data(symbol, start_time, end_time, interval)
    assert not df1.empty, "Initial fetch returned empty DataFrame"
    logger.debug(f"First fetch returned DataFrame with shape: {df1.shape}")

    # Create second DataSourceManager instance with same cache directory
    logger.debug("Creating new VisionDataClient instance")
    new_vision_client = VisionDataClient(
        symbol=symbol, interval=interval.value, use_cache=False
    )
    await validate_client(new_vision_client)

    new_manager = DataSourceManager(
        market_type=MarketType.SPOT,
        vision_client=new_vision_client,
        cache_dir=temp_cache_dir,
        use_cache=True,
    )

    logger.debug(f"New vision client object: {new_vision_client}")
    # Fetch with new client - should use existing cache
    logger.info("Fetching data with second client instance")
    df2 = await new_manager.get_data(symbol, start_time, end_time, interval)
    assert not df2.empty, "Second fetch returned empty DataFrame"
    logger.debug(f"Second fetch returned DataFrame with shape: {df2.shape}")

    # Verify both results are identical
    pd.testing.assert_frame_equal(df1, df2, check_dtype=True, check_index_type=True)
    logger.info("Both client instances returned identical data")

    logger.debug("Successfully completed test_cache_persistence")


@pytest.mark.asyncio
async def test_cache_invalidation(temp_cache_dir: Path):
    """Test cache invalidation logic in vision API."""
    logger.debug("Entering async context for test_cache_invalidation")

    # First, create a VisionDataClient without caching
    vision_client = VisionDataClient(symbol="BTCUSDT", interval="1s", use_cache=False)
    await validate_client(vision_client)

    # Create a DataSourceManager with caching enabled
    manager = DataSourceManager(
        market_type=MarketType.SPOT,
        vision_client=vision_client,
        cache_dir=temp_cache_dir,
        use_cache=True,
    )

    # Test parameters
    symbol = "BTCUSDT"
    interval = Interval.SECOND_1
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = datetime(2024, 1, 1, 1, tzinfo=timezone.utc)

    logger.info("Initial data fetch")

    # Fetch data - should download and cache
    _ = await manager.get_data(symbol, start_time, end_time, interval)

    # Verify cache file is created
    cache_files = list(temp_cache_dir.glob("**/*.arrow"))
    assert len(cache_files) > 0, "No cache files found"
    logger.info(f"Cache files created: {cache_files}")

    # Get modification time of the cache file
    original_mtime = cache_files[0].stat().st_mtime
    logger.info(f"Original cache file mtime: {datetime.fromtimestamp(original_mtime)}")

    # Add a small delay to ensure modification time would be different
    await asyncio.sleep(1.1)

    # Modify the cache file to simulate tampering
    corrupt_cache_file(cache_files[0])

    # Get new modification time
    new_mtime = cache_files[0].stat().st_mtime
    logger.info(f"Modified cache file mtime: {datetime.fromtimestamp(new_mtime)}")
    assert (
        new_mtime > original_mtime
    ), "Cache file modification time should have changed"

    # Create a new client instance
    new_vision_client = VisionDataClient(
        symbol="BTCUSDT", interval="1s", use_cache=False
    )
    await validate_client(new_vision_client)

    new_manager = DataSourceManager(
        market_type=MarketType.SPOT,
        vision_client=new_vision_client,
        cache_dir=temp_cache_dir,
        use_cache=True,
    )

    # Fetch again - should detect invalid cache and regenerate
    logger.info("Fetching data again with modified cache file")
    df2 = await new_manager.get_data(symbol, start_time, end_time, interval)

    # Verify data was fetched successfully
    assert not df2.empty, "Data should be successfully fetched after invalidating cache"
    assert isinstance(
        df2.index, pd.DatetimeIndex
    ), "DataFrame should have DatetimeIndex"

    # Verify cache was regenerated
    cache_files_after = list(temp_cache_dir.glob("**/*.arrow"))
    assert len(cache_files_after) > 0, "Cache files should exist after regeneration"

    # Final cache file should have different modification time
    final_mtime = cache_files_after[0].stat().st_mtime
    logger.info(f"Regenerated cache file mtime: {datetime.fromtimestamp(final_mtime)}")
    assert final_mtime > new_mtime, "Cache file should have been regenerated"

    logger.debug("Successfully completed test_cache_invalidation")


@pytest.mark.asyncio
@log_async_context
async def test_prefetch_with_cache(vision_client: VisionDataClient) -> None:
    """Test prefetch functionality with caching."""
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_time = start_time + timedelta(days=2)

    logger.info(f"Prefetching data: {start_time} to {end_time}")
    logger.debug(f"Vision client object: {vision_client}")

    try:
        # Prefetch data
        await vision_client.prefetch(start_time, end_time)
        logger.debug("Prefetch completed")

        # Verify data can be fetched from cache
        logger.info("Fetching prefetched data")
        df = await vision_client.fetch(start_time, end_time)
        assert not df.empty, "Failed to fetch prefetched data"
        logger.debug(f"Fetched DataFrame with shape: {df.shape}")
        logger.info(f"Successfully fetched {len(df)} records of prefetched data")

        # Additional verification of data continuity
        time_diffs = df.index.to_series().diff()
        gaps = time_diffs[time_diffs > timedelta(seconds=1)]
        if not gaps.empty:
            logger.warning(f"Found {len(gaps)} gaps in prefetched data")
            for idx, gap in gaps.head().items():
                logger.warning(f"Gap at {idx}: {gap}")

    except Exception as e:
        logger.error(f"Error in test_prefetch_with_cache: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise


@pytest.mark.asyncio
async def test_historical_data_caching(temp_cache_dir: Path):
    """Test caching for different time periods in history."""
    logger.debug("Entering async context for test_historical_data_caching")

    # Test periods in reverse chronological order
    periods = [
        {
            "name": "Early 2024",
            "start": datetime(2024, 1, 1, 15, 15, tzinfo=timezone.utc),
            "end": datetime(2024, 1, 1, 16, 15, tzinfo=timezone.utc),
        },
        {
            "name": "Late 2023",
            "start": datetime(2023, 12, 1, tzinfo=timezone.utc),
            "end": datetime(2023, 12, 1, 1, tzinfo=timezone.utc),
        },
        {
            "name": "Mid 2023",
            "start": datetime(2023, 6, 1, tzinfo=timezone.utc),
            "end": datetime(2023, 6, 1, 1, tzinfo=timezone.utc),
        },
    ]

    symbol = "BTCUSDT"
    interval = Interval.SECOND_1

    for period in periods:
        period_name = period["name"]
        start_time = period["start"]
        end_time = period["end"]

        # Create a unique cache directory for each period
        period_cache_dir = temp_cache_dir / period_name.lower().replace(" ", "_")
        period_cache_dir.mkdir(exist_ok=True)
        logger.debug(f"Created period cache directory: {period_cache_dir}")

        # Create first DataSourceManager instance for this period
        vision_client = VisionDataClient(
            symbol=symbol, interval=interval.value, use_cache=False
        )
        await validate_client(vision_client)

        manager = DataSourceManager(
            market_type=MarketType.SPOT,
            vision_client=vision_client,
            cache_dir=period_cache_dir,
            use_cache=True,
        )

        logger.info(f"\nTesting {period_name} data: {start_time} to {end_time}")

        # First fetch - should download and cache
        logger.info(f"Initial fetch for {period_name}")
        df1 = await manager.get_data(symbol, start_time, end_time, interval)
        assert (
            not df1.empty
        ), f"Initial fetch for {period_name} returned empty DataFrame"
        logger.debug(f"First fetch returned DataFrame with shape: {df1.shape}")

        # Verify cache file was created
        cache_files = list(period_cache_dir.glob("**/*.arrow"))
        assert len(cache_files) > 0, f"No cache files were created for {period_name}"
        logger.info(f"Found cache file: {cache_files[0]}")
        logger.debug(
            f"Cache file details - Size: {cache_files[0].stat().st_size}, Modified: {datetime.fromtimestamp(cache_files[0].stat().st_mtime)}"
        )

        # Create a new DataSourceManager instance for this period
        new_vision_client = VisionDataClient(
            symbol=symbol, interval=interval.value, use_cache=False
        )
        await validate_client(new_vision_client)

        new_manager = DataSourceManager(
            market_type=MarketType.SPOT,
            vision_client=new_vision_client,
            cache_dir=period_cache_dir,
            use_cache=True,
        )

        # Second fetch - should use cache
        logger.info(f"Second fetch for {period_name} (should use cache)")
        df2 = await new_manager.get_data(symbol, start_time, end_time, interval)
        logger.debug(f"Second fetch returned DataFrame with shape: {df2.shape}")

        # Verify results are identical
        pd.testing.assert_frame_equal(df1, df2, check_dtype=True, check_index_type=True)
        logger.info(f"Both fetches for {period_name} returned identical data")

    logger.debug("Successfully completed test_historical_data_caching")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
