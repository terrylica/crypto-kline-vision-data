#!/usr/bin/env python
"""Example of the recommended approach for data retrieval using DataSourceManager."""

import asyncio
import signal
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import traceback

from utils.logger_setup import get_logger
from utils.market_constraints import Interval, MarketType
from core.data_source_manager import DataSourceManager, DataSource
from core.vision_data_client import VisionDataClient
from utils.time_utils import validate_time_window
from utils.config import VISION_DATA_DELAY_HOURS

# Set up logging with DEBUG level for detailed diagnostics
logger = get_logger(__name__, "INFO", show_path=True)


def validate_time_range(
    start_time: datetime, end_time: datetime, use_vision: bool = False
) -> None:
    """Validate time range based on data source requirements.

    Args:
        start_time: Start time of data request
        end_time: End time of data request
        use_vision: Whether using Vision API

    Raises:
        ValueError: If time range is invalid for the data source
    """
    now = datetime.now(timezone.utc)

    # Check if we're requesting future data
    if end_time > now:
        raise ValueError(
            f"Cannot fetch data from the future. Current time: {now}, "
            f"Requested end time: {end_time}"
        )

    if use_vision:
        # Vision API has a delay
        min_time = now - timedelta(hours=VISION_DATA_DELAY_HOURS)
        if end_time > min_time:
            raise ValueError(
                f"Vision API data is not available for times newer than {min_time}. "
                f"Requested end time: {end_time}"
            )
    else:
        # REST API is for recent data
        if start_time < now - timedelta(days=7):
            raise ValueError(
                f"REST API is not recommended for data older than 7 days. "
                f"Requested start time: {start_time}"
            )

    # Validate time window
    validate_time_window(start_time, end_time)


async def example_fetch_recent_data():
    """Example function to fetch recent data using DataSourceManager."""
    logger.info("Fetching recent Bitcoin data using the recommended approach")

    # Create cache directory
    cache_dir = Path("./cache")
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Cache directory created/verified at: {cache_dir}")
    except Exception as e:
        logger.error(f"Failed to create cache directory: {e}")
        raise

    # Initialize Vision client
    vision_client = None
    try:
        vision_client = VisionDataClient(
            symbol="BTCUSDT",
            interval="1s",
            use_cache=False,  # Disable direct caching as we use DataSourceManager's cache
        )
        logger.debug("Vision client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Vision client: {e}")
        raise

    # Define time range (recent data that should be available, not in the future)
    now = datetime.now(timezone.utc)
    # Use data from 48 hours ago to ensure Vision API has it
    end_time = now - timedelta(hours=48)
    start_time = end_time - timedelta(hours=1)

    logger.info(f"Time range: {start_time} to {end_time}")

    try:
        # Validate time range for REST API
        validate_time_range(start_time, end_time, use_vision=False)
        logger.debug("Time range validation passed")

        # Using DataSourceManager (recommended approach with async context manager)
        async with DataSourceManager(
            market_type=MarketType.SPOT,
            vision_client=vision_client,  # Add Vision client
            cache_dir=cache_dir,
            use_cache=True,  # Enable caching through the unified cache manager
        ) as manager:
            logger.debug("DataSourceManager initialized successfully")

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

    except ValueError as e:
        logger.error(f"Invalid time range: {e}")
        logger.debug(f"Time range validation failed: {traceback.format_exc()}")
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        logger.debug(f"Unexpected error: {traceback.format_exc()}")
    finally:
        # Clean up Vision client
        if vision_client:
            try:
                await vision_client.__aexit__(None, None, None)
                logger.debug("Vision client cleaned up successfully")
            except Exception as e:
                logger.error(f"Error cleaning up Vision client: {e}")


async def example_fetch_historical_data():
    """Example function to fetch historical data using DataSourceManager."""
    logger.info("\nFetching historical Bitcoin data (recommended approach)")

    # Create cache directory
    cache_dir = Path("./cache")
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Cache directory created/verified at: {cache_dir}")
    except Exception as e:
        logger.error(f"Failed to create cache directory: {e}")
        raise

    # Initialize Vision client
    vision_client = None
    try:
        vision_client = VisionDataClient(
            symbol="BTCUSDT",
            interval="1s",
            use_cache=False,  # Disable direct caching as we use DataSourceManager's cache
        )
        logger.debug("Vision client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Vision client: {e}")
        raise

    # Define historical time range (from 2023 to ensure data exists)
    # Use a date well in the past to guarantee Vision API has the data
    # Using January 15, 2023 as it's a more reliable date for testing
    end_time = datetime(2023, 1, 15, 1, 0, 0, tzinfo=timezone.utc)
    start_time = datetime(2023, 1, 15, 0, 0, 0, tzinfo=timezone.utc)

    logger.info(f"Historical time range: {start_time} to {end_time}")

    try:
        # Validate time range for Vision API
        validate_time_range(start_time, end_time, use_vision=True)
        logger.debug("Time range validation passed")

        # Using DataSourceManager
        async with DataSourceManager(
            market_type=MarketType.SPOT,
            vision_client=vision_client,  # Add Vision client
            cache_dir=cache_dir,
            use_cache=True,
        ) as manager:
            logger.debug("DataSourceManager initialized successfully")

            try:
                # For historical data, Vision API will automatically be selected
                # but we enforce it here to demonstrate error handling
                logger.debug("Attempting to fetch 1-second data from Vision API")
                df = await manager.get_data(
                    symbol="BTCUSDT",
                    start_time=start_time,
                    end_time=end_time,
                    interval=Interval.SECOND_1,
                    enforce_source=DataSource.VISION,  # Enforce Vision API
                )

                logger.info(f"Historical data retrieved: {len(df)} rows")

                # Display a sample of the data
                if not df.empty:
                    logger.info("\nSample historical data:")
                    print(df.head().to_string())

                # Access cache statistics
                cache_stats = manager.get_cache_stats()
                logger.info(f"\nCache statistics: {cache_stats}")
            except Exception as e:
                if "404" in str(e) or "not found" in str(e).lower():
                    logger.error(
                        f"Data not found in Vision API. This could be because:"
                    )
                    logger.error(
                        f"1. The requested date ({start_time.date()}) has no data"
                    )
                    logger.error(f"2. The symbol BTCUSDT wasn't traded on that date")
                    logger.error(f"3. 1-second data isn't available for that date")

                    # Try with a different interval that's more likely to exist
                    logger.info("\nAttempting with 1-minute data instead:")
                    df_minute = await manager.get_data(
                        symbol="BTCUSDT",
                        start_time=start_time,
                        end_time=end_time,
                        interval=Interval.MINUTE_1,
                    )

                    logger.info(f"1-minute data retrieved: {len(df_minute)} rows")
                    if not df_minute.empty:
                        logger.info("\nSample 1-minute data:")
                        print(df_minute.head().to_string())
                elif "checksum" in str(e).lower():
                    logger.error(
                        f"Cache checksum verification failed. This could be because:"
                    )
                    logger.error("1. The downloaded data was corrupted")
                    logger.error("2. The checksum from Vision API is incorrect")
                    logger.error("3. The cache file was modified")

                    # Log additional diagnostic information
                    logger.debug(f"Exception details: {str(e)}")
                    logger.debug(f"Exception type: {type(e)}")
                    logger.debug(f"Full traceback: {traceback.format_exc()}")

                    # Try to repair the cache
                    logger.info("\nAttempting to repair cache...")
                    if await manager.repair_cache("BTCUSDT", "1s", start_time):
                        logger.info(
                            "Cache repair successful, retrying data retrieval..."
                        )
                        df = await manager.get_data(
                            symbol="BTCUSDT",
                            start_time=start_time,
                            end_time=end_time,
                            interval=Interval.SECOND_1,
                            enforce_source=DataSource.VISION,
                        )
                        if not df.empty:
                            logger.info(
                                f"Successfully retrieved {len(df)} rows after cache repair"
                            )
                            print(df.head().to_string())
                    else:
                        logger.error("Cache repair failed")

                        # Try with 1-minute data as a last resort
                        logger.info("\nAttempting with 1-minute data as fallback:")
                        df_minute = await manager.get_data(
                            symbol="BTCUSDT",
                            start_time=start_time,
                            end_time=end_time,
                            interval=Interval.MINUTE_1,
                        )

                        if not df_minute.empty:
                            logger.info(
                                f"Successfully retrieved {len(df_minute)} rows of 1-minute data"
                            )
                            print(df_minute.head().to_string())
                        else:
                            logger.error("Failed to retrieve data with all methods")
                else:
                    raise

    except ValueError as e:
        logger.error(f"Invalid time range: {e}")
        logger.debug(f"Time range validation failed: {traceback.format_exc()}")
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        logger.debug(f"Unexpected error: {traceback.format_exc()}")
    finally:
        # Clean up Vision client
        if vision_client:
            try:
                await vision_client.__aexit__(None, None, None)
                logger.debug("Vision client cleaned up successfully")
            except Exception as e:
                logger.error(f"Error cleaning up Vision client: {e}")


async def example_fetch_same_day_minute_data():
    """Example function to fetch 1-minute data for the current day using DataSourceManager."""
    logger.info("\nFetching 1-minute data for the current day (recommended approach)")

    # Create cache directory
    cache_dir = Path("./cache")
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Cache directory created/verified at: {cache_dir}")
    except Exception as e:
        logger.error(f"Failed to create cache directory: {e}")
        raise

    # Initialize Vision client
    vision_client = None
    try:
        vision_client = VisionDataClient(
            symbol="BTCUSDT",
            interval="1m",  # Using 1-minute interval
            use_cache=False,  # Disable direct caching as we use DataSourceManager's cache
        )
        logger.debug("Vision client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Vision client: {e}")
        raise

    # Define time range for today
    now = datetime.now(timezone.utc)
    # Set end time a few hours in the past to ensure data availability
    end_time = now - timedelta(hours=2)
    # Set start time to the beginning of the same day
    start_time = datetime(
        end_time.year, end_time.month, end_time.day, 0, 0, 0, tzinfo=timezone.utc
    )

    logger.info(f"Same-day time range: {start_time} to {end_time}")

    try:
        # Validate time range for REST API (recent data)
        validate_time_range(start_time, end_time, use_vision=False)
        logger.debug("Time range validation passed")

        # Using DataSourceManager with async context manager
        async with DataSourceManager(
            market_type=MarketType.SPOT,
            vision_client=vision_client,
            cache_dir=cache_dir,
            use_cache=True,  # Enable caching
        ) as manager:
            logger.debug("DataSourceManager initialized successfully")

            # Fetch 1-minute data
            df = await manager.get_data(
                symbol="BTCUSDT",
                start_time=start_time,
                end_time=end_time,
                interval=Interval.MINUTE_1,  # Using 1-minute interval
            )

            logger.info(f"1-minute data retrieved: {len(df)} rows")
            logger.info(f"Data shape: {df.shape}")
            logger.info(f"Data columns: {df.columns.tolist()}")

            # Display a sample of the data
            if not df.empty:
                logger.info("\nSample 1-minute data:")
                print(df.head().to_string())

            # Get cache statistics
            cache_stats = manager.get_cache_stats()
            logger.info(f"\nCache statistics: {cache_stats}")

    except ValueError as e:
        logger.error(f"Invalid time range: {e}")
        logger.debug(f"Time range validation failed: {traceback.format_exc()}")
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        logger.debug(f"Unexpected error: {traceback.format_exc()}")
    finally:
        # Clean up Vision client
        if vision_client:
            try:
                await vision_client.__aexit__(None, None, None)
                logger.debug("Vision client cleaned up successfully")
            except Exception as e:
                logger.error(f"Error cleaning up Vision client: {e}")


async def main():
    """Run the example functions."""
    try:
        # Run all example functions
        await example_fetch_recent_data()  # Run the recent data example
        await example_fetch_historical_data()
        await example_fetch_same_day_minute_data()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.debug(f"Main function error: {traceback.format_exc()}")
        sys.exit(1)


def handle_signals():
    """Set up signal handlers for graceful shutdown."""
    loop = asyncio.get_event_loop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(sig, loop))
        )


async def shutdown(sig, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info(f"Received exit signal {sig.name}...")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]

    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)

    loop.stop()


if __name__ == "__main__":
    # Set up signal handlers
    handle_signals()

    # Run the main function
    asyncio.run(main())
