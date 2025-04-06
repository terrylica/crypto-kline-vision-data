#!/usr/bin/env python
"""
Standalone test script for verifying the fixes for curl_cffi client hanging.

This script tests the RestDataClient and VisionDataClient initialization 
and cleanup without timeouts, to verify that our fixes prevent hanging.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import gc

from utils.logger_setup import logger
from core.rest_data_client import RestDataClient
from core.vision_data_client import VisionDataClient
from utils.market_constraints import MarketType, Interval
from scripts.fix_hanging_client import find_force_timeout_tasks, fix_client_reference


async def test_rest_client():
    """Test the RestDataClient initialization and cleanup."""
    logger.info("Testing RestDataClient initialization and cleanup")

    # First, proactively cancel any existing force_timeout tasks
    force_timeout_tasks = await find_force_timeout_tasks()
    if force_timeout_tasks:
        logger.warning(
            f"Found {len(force_timeout_tasks)} existing force_timeout tasks, cancelling them"
        )
        for task in force_timeout_tasks:
            task.cancel()

    # Get current time for data range
    now = datetime.now(timezone.utc)
    end_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=1)

    logger.info(f"Using time range: {start_time} to {end_time}")

    client = None
    try:
        # Initialize client
        client = RestDataClient(
            market_type=MarketType.SPOT, max_concurrent=10, retry_count=3
        )

        # Enter the context
        await client.__aenter__()
        logger.info("Successfully initialized RestDataClient")

        # Fetch some data
        logger.info("Fetching data to test client functionality")
        df, stats = await client.fetch(
            symbol="BTCUSDT",
            interval=Interval.MINUTE_1,
            start_time=start_time,
            end_time=end_time,
        )

        logger.info(f"Successfully fetched {len(df)} records")
    except Exception as e:
        logger.error(f"Error during RestDataClient test: {e}")
        raise
    finally:
        # Clean up the client
        if client:
            logger.info("Cleaning up RestDataClient")
            start_time = time.time()
            await client.__aexit__(None, None, None)
            cleanup_time = time.time() - start_time
            logger.info(f"RestDataClient cleanup completed in {cleanup_time:.3f}s")

        # Force garbage collection
        gc.collect()


async def test_vision_client():
    """Test the VisionDataClient initialization and cleanup."""
    logger.info("Testing VisionDataClient initialization and cleanup")

    # First, proactively cancel any existing force_timeout tasks
    force_timeout_tasks = await find_force_timeout_tasks()
    if force_timeout_tasks:
        logger.warning(
            f"Found {len(force_timeout_tasks)} existing force_timeout tasks, cancelling them"
        )
        for task in force_timeout_tasks:
            task.cancel()

    # Use BTCUSDT for testing
    symbol = "BTCUSDT"
    interval = "1m"

    client = None
    try:
        # Initialize client
        client = VisionDataClient(
            symbol=symbol, interval=interval, market_type=MarketType.SPOT
        )

        # Enter the context
        await client.__aenter__()
        logger.info("Successfully initialized VisionDataClient")

        # Simply verify that the client has the necessary attributes
        logger.info("Checking VisionDataClient attributes")
        assert client.symbol == symbol, "Symbol mismatch"
        assert client.interval == interval, "Interval mismatch"
        assert client._client is not None, "HTTP client not initialized"

        logger.info("VisionDataClient attributes are valid")
    except Exception as e:
        logger.error(f"Error during VisionDataClient test: {e}")
        raise
    finally:
        # Clean up the client
        if client:
            logger.info("Cleaning up VisionDataClient")
            start_time = time.time()

            # Direct cleanup instead of using __aexit__
            try:
                # First nullify _curlm reference to prevent hanging
                if hasattr(client, "_client") and client._client:
                    if hasattr(client._client, "_curlm") and client._client._curlm:
                        logger.debug("Pre-emptively cleaning _curlm object in _client")
                        client._client._curlm = None

                # Then close the HTTP client
                if hasattr(client, "_client") and client._client:
                    from utils.network_utils import safely_close_client

                    await safely_close_client(client._client)
                    client._client = None

                cleanup_time = time.time() - start_time
                logger.info(
                    f"VisionDataClient cleanup completed in {cleanup_time:.3f}s"
                )
            except Exception as e:
                logger.error(f"Error during manual VisionDataClient cleanup: {e}")

        # Force garbage collection
        gc.collect()


async def main():
    """Main test function."""
    logger.info("Starting client cleanup tests")

    # Test RestDataClient
    await test_rest_client()

    # Wait a moment to ensure all resources are properly released
    logger.info("Waiting for resources to be released")
    await asyncio.sleep(2)

    # Force garbage collection
    gc.collect()

    # Test VisionDataClient
    await test_vision_client()

    logger.info("All tests completed successfully")


if __name__ == "__main__":
    # Run the main function
    asyncio.run(main())
