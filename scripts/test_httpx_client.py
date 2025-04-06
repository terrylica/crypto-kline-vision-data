#!/usr/bin/env python

"""Test script for data retrieval using httpx instead of curl_cffi.

This script tests our improved data retrieval approach using httpx
instead of curl_cffi to ensure it resolves the hanging issues.
"""

import asyncio
import gc
from datetime import datetime, timedelta, timezone
from pathlib import Path

from utils.logger_setup import logger
from utils.market_constraints import Interval, MarketType
from core.data_source_manager import DataSourceManager, DataSource

logger.setLevel("DEBUG")


async def test_rest_with_httpx():
    """Test REST API data retrieval using httpx."""
    logger.info("Testing REST API with httpx client")

    # Create cache directory
    cache_dir = Path("./tmp/cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Define a recent time range to retrieve data for
    now = datetime.now(timezone.utc)
    end_time = now - timedelta(
        hours=48
    )  # Use data from 48 hours ago to ensure availability
    start_time = end_time - timedelta(minutes=30)  # Get 30 minutes of data

    symbol = "BTCUSDT"
    interval = Interval.MINUTE_1

    logger.info(
        f"Retrieving {symbol} {interval.value} data from {start_time} to {end_time}"
    )

    # Use DataSourceManager with httpx client
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,
        use_httpx=True,  # Use httpx instead of curl_cffi
    ) as manager:
        # Force REST API to test our httpx implementation
        df = await manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            enforce_source=DataSource.REST,  # Force REST API
        )

        logger.info(f"Retrieved {len(df)} rows of data using httpx")

        if not df.empty:
            logger.info(f"First row timestamp: {df.index[0]}")
            logger.info(f"Last row timestamp: {df.index[-1]}")

        # Display cache statistics
        cache_stats = manager.get_cache_stats()
        logger.info(f"Cache statistics: {cache_stats}")

    # Force garbage collection to clean up any resources
    gc.collect()

    return df


async def test_vision_with_httpx():
    """Test Vision API data retrieval using httpx."""
    logger.info("Testing Vision API with httpx client")

    # Create cache directory
    cache_dir = Path("./tmp/cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Define a historical time range to retrieve data for
    now = datetime.now(timezone.utc)
    end_time = now - timedelta(days=90)  # Use data from 90 days ago
    start_time = end_time - timedelta(hours=4)  # Get 4 hours of data

    symbol = "BTCUSDT"
    interval = Interval.MINUTE_1

    logger.info(
        f"Retrieving {symbol} {interval.value} data from {start_time} to {end_time}"
    )

    # Use DataSourceManager with httpx client
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,
        use_httpx=True,  # Use httpx instead of curl_cffi
    ) as manager:
        # Force Vision API to test our httpx implementation
        df = await manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            enforce_source=DataSource.VISION,  # Force Vision API
        )

        logger.info(f"Retrieved {len(df)} rows of data using httpx")

        if not df.empty:
            logger.info(f"First row timestamp: {df.index[0]}")
            logger.info(f"Last row timestamp: {df.index[-1]}")

        # Display cache statistics
        cache_stats = manager.get_cache_stats()
        logger.info(f"Cache statistics: {cache_stats}")

    # Force garbage collection to clean up any resources
    gc.collect()

    return df


async def main():
    """Run all tests."""
    logger.info("Starting httpx client tests")

    try:
        # Test REST API with httpx
        rest_df = await test_rest_with_httpx()
        logger.info(f"REST API test {'succeeded' if not rest_df.empty else 'failed'}")

        # Test Vision API with httpx
        vision_df = await test_vision_with_httpx()
        logger.info(
            f"Vision API test {'succeeded' if not vision_df.empty else 'failed'}"
        )

        logger.info("All tests completed successfully")
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        return False

    return True


if __name__ == "__main__":
    # Set up asyncio event loop policy
    import platform

    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Run the tests
    result = asyncio.run(main())

    # Exit with appropriate status
    import sys

    sys.exit(0 if result else 1)
