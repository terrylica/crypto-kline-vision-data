#!/usr/bin/env python
"""Migration guide from deprecated VisionDataClient caching to DataSourceManager."""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import warnings

from utils.logger_setup import get_logger
from utils.market_constraints import Interval, MarketType
from core.vision_data_client import VisionDataClient
from core.data_source_manager import DataSourceManager, DataSource

logger = get_logger(__name__, "INFO", show_path=False)


async def deprecated_approach():
    """
    DEPRECATED: Using direct caching with VisionDataClient.

    This approach will raise deprecation warnings and will be removed in a future version.
    """
    logger.info("DEPRECATED APPROACH: Direct caching with VisionDataClient")
    logger.info("⚠️ This approach will raise deprecation warnings ⚠️")

    # Create cache directory
    cache_dir = Path("./cache/deprecated")
    cache_dir.mkdir(exist_ok=True, parents=True)

    # Define time range
    end_time = datetime(2023, 12, 31, 1, 0, 0, tzinfo=timezone.utc)
    start_time = datetime(2023, 12, 31, 0, 0, 0, tzinfo=timezone.utc)

    logger.info(f"Time range: {start_time} to {end_time}")

    # This will trigger a deprecation warning
    client = VisionDataClient(
        symbol="BTCUSDT",
        interval="1s",
        cache_dir=cache_dir,  # DEPRECATED
        use_cache=True,  # DEPRECATED
    )

    # Using the client to fetch data
    async with client:
        data = await client.fetch(start_time, end_time)

        logger.info(f"Data retrieved: {len(data)} rows")

        if not data.empty:
            logger.info("\nSample data (deprecated approach):")
            print(data.head().to_string())

    logger.info("Completed deprecated approach demonstration")


async def recommended_approach():
    """
    RECOMMENDED: Using DataSourceManager with UnifiedCacheManager.

    This is the current recommended approach for data retrieval with caching.
    """
    logger.info("\nRECOMMENDED APPROACH: DataSourceManager with UnifiedCacheManager")

    # Create cache directory
    cache_dir = Path("./cache/recommended")
    cache_dir.mkdir(exist_ok=True, parents=True)

    # Define the same time range for comparison
    end_time = datetime(2023, 12, 31, 1, 0, 0, tzinfo=timezone.utc)
    start_time = datetime(2023, 12, 31, 0, 0, 0, tzinfo=timezone.utc)

    logger.info(f"Time range: {start_time} to {end_time}")

    # Using DataSourceManager with async context manager
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,  # Enable unified caching
    ) as manager:
        # Fetch data using the manager
        data = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
        )

        logger.info(f"Data retrieved: {len(data)} rows")

        if not data.empty:
            logger.info("\nSample data (recommended approach):")
            print(data.head().to_string())

        # Access cache statistics
        cache_stats = manager.get_cache_stats()
        logger.info(f"\nCache statistics: {cache_stats}")

    logger.info("Completed recommended approach demonstration")


async def hybrid_approach():
    """
    HYBRID: Using VisionDataClient through DataSourceManager.

    This approach might be useful during migration if you have specific
    requirements for using VisionDataClient but want to benefit from the
    unified caching approach.
    """
    logger.info("\nHYBRID APPROACH: VisionDataClient through DataSourceManager")

    # Create cache directory
    cache_dir = Path("./cache/hybrid")
    cache_dir.mkdir(exist_ok=True, parents=True)

    # Create VisionDataClient without caching
    # Note: We disable direct caching as DataSourceManager will handle it
    client = VisionDataClient(
        symbol="BTCUSDT",
        interval="1s",
        use_cache=False,  # Disable direct caching
    )

    # Define time range
    end_time = datetime(2023, 12, 31, 1, 0, 0, tzinfo=timezone.utc)
    start_time = datetime(2023, 12, 31, 0, 0, 0, tzinfo=timezone.utc)

    logger.info(f"Time range: {start_time} to {end_time}")

    # Pass the VisionDataClient to DataSourceManager
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,
        vision_client=client,  # Use the custom VisionDataClient
    ) as manager:
        # DataSourceManager will use the provided client but handle caching
        data = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            enforce_source=DataSource.VISION,  # Force Vision API
        )

        logger.info(f"Data retrieved: {len(data)} rows")

        if not data.empty:
            logger.info("\nSample data (hybrid approach):")
            print(data.head().to_string())

    logger.info("Completed hybrid approach demonstration")


async def main():
    """Run the example functions demonstrating migration path."""
    # You can comment out sections you don't want to run
    with warnings.catch_warnings():
        # For demonstration purposes, we'll show the warnings
        warnings.filterwarnings("always", category=DeprecationWarning)
        await deprecated_approach()

    await recommended_approach()
    await hybrid_approach()

    # Display final migration recommendation
    logger.info("\n" + "=" * 80)
    logger.info("MIGRATION RECOMMENDATION:")
    logger.info("1. Use DataSourceManager with UnifiedCacheManager for all new code")
    logger.info(
        "2. For existing VisionDataClient usage, switch to the recommended approach"
    )
    logger.info("3. If needed temporarily, use the hybrid approach during migration")
    logger.info("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
