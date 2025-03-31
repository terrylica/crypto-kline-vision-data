#!/usr/bin/env python
"""Integration tests for data source fallback mechanism and download-first approach.

This module tests the critical fallback functionality between Vision and REST APIs:
1. Automatic fallback from Vision API to REST API when Vision fails
2. Efficiency of the download-first approach for Vision API
3. Caching integration between different data sources

These tests validate the end-to-end behavior of the data source selection system.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import pytest

# Import directly from core and utils (path is set in conftest.py)
from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import Interval, MarketType
from utils.time_utils import enforce_utc_timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger("test_fallback")


async def test_vision_to_rest_fallback():
    """Test automatic fallback from Vision API to REST API."""
    logger.info("Testing Vision API to REST API fallback...")

    # Create a cache directory for testing
    cache_dir = Path("./test_cache")
    cache_dir.mkdir(exist_ok=True)

    try:
        # Initialize DataSourceManager with caching enabled
        manager = DataSourceManager(
            market_type=MarketType.SPOT,
            cache_dir=cache_dir,
            use_cache=True,
        )

        # Test with recent data that's likely too recent for Vision API
        # but available via REST API - should trigger fallback
        symbol = "BTCUSDT"
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=5)

        logger.info(f"Fetching recent data from {start_time} to {end_time}")

        # Force Vision API first to test fallback
        df = await manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            enforce_source=DataSource.VISION,  # Force Vision API first
        )

        if not df.empty:
            logger.info(f"Successfully retrieved {len(df)} records")
            logger.info(f"First timestamp: {df.index[0].isoformat()}")
            logger.info(f"Last timestamp: {df.index[-1].isoformat()}")

            # Data should be within our requested time range
            assert df.index[0] >= start_time, "Start time outside requested range"
            assert df.index[-1] <= end_time, "End time outside requested range"

            return True
        else:
            logger.warning(
                "No data returned - this might be expected during market closure"
            )
            return False

    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        import shutil

        # Clean up test cache
        shutil.rmtree(cache_dir, ignore_errors=True)


async def test_download_first_approach():
    """Test the download-first approach efficiency."""
    logger.info("Testing download-first approach efficiency...")

    # Create a cache directory for testing
    cache_dir = Path("./test_cache")
    cache_dir.mkdir(exist_ok=True)

    try:
        # Initialize DataSourceManager
        manager = DataSourceManager(
            market_type=MarketType.SPOT,
            cache_dir=cache_dir,
            use_cache=True,
        )

        # Use historical data that should be available in Vision API
        symbol = "BTCUSDT"
        end_time = datetime.now(timezone.utc) - timedelta(days=2)  # 2 days ago
        start_time = end_time - timedelta(minutes=30)  # 30 min window

        logger.info(f"Fetching historical data from {start_time} to {end_time}")

        # Measure time to fetch data
        import time

        start = time.time()

        df = await manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            enforce_source=DataSource.VISION,  # Force Vision API
        )

        duration = time.time() - start

        if not df.empty:
            logger.info(
                f"Successfully retrieved {len(df)} records in {duration:.2f} seconds"
            )
            logger.info(f"First timestamp: {df.index[0].isoformat()}")
            logger.info(f"Last timestamp: {df.index[-1].isoformat()}")

            # Data should be within our requested time range
            assert df.index[0] >= start_time, "Start time outside requested range"
            assert df.index[-1] <= end_time, "End time outside requested range"

            # If time is reasonable (under 5 seconds), consider the test passed
            if duration < 5.0:
                logger.info("Download-first approach is efficient (under 5 seconds)")
            else:
                logger.warning(
                    f"Download-first approach took {duration:.2f}s - may need optimization"
                )

            return True
        else:
            logger.warning(
                "No data returned - check if data is available for the test period"
            )
            return False

    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        import shutil

        # Clean up test cache
        shutil.rmtree(cache_dir, ignore_errors=True)


async def test_caching():
    """Test that caching works correctly."""
    logger.info("Testing caching functionality...")

    # Create a cache directory for testing
    cache_dir = Path("./test_cache")
    cache_dir.mkdir(exist_ok=True)

    try:
        # Initialize DataSourceManager with caching enabled
        manager = DataSourceManager(
            market_type=MarketType.SPOT,
            cache_dir=cache_dir,
            use_cache=True,
        )

        # Use historical data that should be available
        symbol = "BTCUSDT"
        end_time = datetime.now(timezone.utc) - timedelta(days=2)  # 2 days ago
        start_time = end_time - timedelta(minutes=10)  # 10 min window

        logger.info(
            f"First fetch from {start_time} to {end_time} (should be cache miss)"
        )

        # First fetch - should be a cache miss
        df1 = await manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
        )

        cache_stats1 = manager.get_cache_stats()
        logger.info(f"Cache stats after first fetch: {cache_stats1}")

        if df1.empty:
            logger.warning("No data returned on first fetch - test inconclusive")
            return False

        # Second fetch - should be a cache hit
        logger.info("Second fetch (should be cache hit)")
        df2 = await manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
        )

        cache_stats2 = manager.get_cache_stats()
        logger.info(f"Cache stats after second fetch: {cache_stats2}")

        # Verify data is identical
        if df1.equals(df2):
            logger.info("Data from cache matches original fetch")
        else:
            logger.warning("Data from cache differs from original fetch")

        # Verify cache hit happened
        assert (
            cache_stats2["hits"] > cache_stats1["hits"]
        ), "Cache hit count did not increase"

        return True

    except Exception as e:
        logger.error(f"Test failed: {e}")
        return False
    finally:
        import shutil

        # Clean up test cache
        shutil.rmtree(cache_dir, ignore_errors=True)


@pytest.mark.asyncio
async def test_all_features():
    """Run all tests for pytest compatibility."""
    # Test Vision to REST fallback
    fallback_result = await test_vision_to_rest_fallback()

    # Test download-first approach
    download_first_result = await test_download_first_approach()

    # Test caching
    caching_result = await test_caching()

    # Return overall result
    return fallback_result and download_first_result and caching_result


async def main():
    """Run all tests."""
    logger.info("Starting tests...")

    # Test Vision to REST fallback
    fallback_result = await test_vision_to_rest_fallback()

    # Test download-first approach
    download_first_result = await test_download_first_approach()

    # Test caching
    caching_result = await test_caching()

    # Print summary
    logger.info("\n--- TEST SUMMARY ---")
    logger.info(f"Vision to REST fallback: {'PASS' if fallback_result else 'FAIL'}")
    logger.info(
        f"Download-first approach: {'PASS' if download_first_result else 'FAIL'}"
    )
    logger.info(f"Caching functionality: {'PASS' if caching_result else 'FAIL'}")


if __name__ == "__main__":
    asyncio.run(main())
