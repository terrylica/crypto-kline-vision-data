#!/usr/bin/env python
"""Example of the recommended approach for data retrieval using DataSourceManager."""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd

from utils.logger_setup import get_logger
from utils.market_constraints import Interval, MarketType
from core.data_source_manager import DataSourceManager, DataSource

logger = get_logger(__name__, "INFO", show_path=False)


async def example_fetch_recent_data():
    """Example function to fetch recent data using DataSourceManager."""
    logger.info("Fetching recent Bitcoin data using the recommended approach")

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(exist_ok=True)

    # Define time range (last hour)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    logger.info(f"Time range: {start_time} to {end_time}")

    # Using DataSourceManager (recommended approach with async context manager)
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,  # Enable caching through the unified cache manager
    ) as manager:
        # The manager will automatically:
        # 1. Choose the appropriate data source (REST or Vision API)
        # 2. Handle caching through UnifiedCacheManager
        # 3. Validate and format the data consistently
        df = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
        )

        logger.info(f"Data retrieved: {len(df)} rows")
        logger.info(f"Data shape: {df.shape}")
        logger.info(f"Data columns: {df.columns.tolist()}")

        # Display a sample of the data
        if not df.empty:
            logger.info("\nSample data:")
            print(df.head().to_string())

        # Example of forcing a specific data source
        # You can force REST API for very recent data or testing
        logger.info("\nFetching with forced REST API source:")
        df_rest = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
            enforce_source=DataSource.REST,  # Force REST API
        )

        logger.info(f"REST API data retrieved: {len(df_rest)} rows")


async def example_fetch_historical_data():
    """Example function to fetch historical data using DataSourceManager."""
    logger.info("\nFetching historical Bitcoin data (recommended approach)")

    # Create cache directory
    cache_dir = Path("./cache")
    cache_dir.mkdir(exist_ok=True)

    # Define historical time range (specific date)
    end_time = datetime(2023, 12, 31, 1, 0, 0, tzinfo=timezone.utc)
    start_time = datetime(2023, 12, 31, 0, 0, 0, tzinfo=timezone.utc)

    logger.info(f"Historical time range: {start_time} to {end_time}")

    # Using DataSourceManager
    async with DataSourceManager(
        market_type=MarketType.SPOT,
        cache_dir=cache_dir,
        use_cache=True,
    ) as manager:
        # For historical data, Vision API will automatically be selected
        df = await manager.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.SECOND_1,
        )

        logger.info(f"Historical data retrieved: {len(df)} rows")

        # Display a sample of the data
        if not df.empty:
            logger.info("\nSample historical data:")
            print(df.head().to_string())

        # Access cache statistics
        cache_stats = manager.get_cache_stats()
        logger.info(f"\nCache statistics: {cache_stats}")


async def main():
    """Run the example functions."""
    await example_fetch_recent_data()
    await example_fetch_historical_data()


if __name__ == "__main__":
    asyncio.run(main())
