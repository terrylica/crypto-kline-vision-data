#!/usr/bin/env python

"""
Test the RestDataClient cleanup behavior to ensure it properly releases resources.
"""

import asyncio
import pytest
from datetime import datetime, timedelta, timezone
import gc

from core.rest_data_client import RestDataClient
from utils.market_constraints import Interval, MarketType
from utils.logger_setup import logger


@pytest.mark.asyncio
async def test_rest_client_cleanup_no_hang():
    """Test that RestDataClient properly cleans up without hanging."""
    # Create a client
    async with RestDataClient(market_type=MarketType.SPOT) as client:
        # Just initialize the client
        assert client is not None
        logger.info("Client initialized successfully")

    # At this point, __aexit__ should have completed without hanging
    logger.info("Client exited context manager successfully")

    # Force garbage collection to help identify any lingering resources
    gc.collect()

    # If we got here without hanging, the test passes
    assert True


@pytest.mark.asyncio
async def test_rest_client_cleanup_after_fetch():
    """Test that RestDataClient properly cleans up after a fetch operation."""
    # Current time
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=10)
    end_time = now

    # Create client and perform a fetch
    async with RestDataClient(market_type=MarketType.SPOT) as client:
        # Fetch some data to ensure resources are allocated
        await client.fetch(
            symbol="BTCUSDT",
            interval=Interval.MINUTE_1,
            start_time=start_time,
            end_time=end_time,
        )
        logger.info("Fetch completed successfully")

    # At this point, __aexit__ should have completed without hanging
    logger.info("Client exited context manager after fetch successfully")

    # Force garbage collection to help identify any lingering resources
    gc.collect()

    # If we got here without hanging, the test passes
    assert True


@pytest.mark.asyncio
async def test_multiple_client_creation_no_hang():
    """Test creating and cleaning up multiple clients in sequence."""
    for i in range(3):
        logger.info(f"Creating client {i+1}/3")
        async with RestDataClient(market_type=MarketType.SPOT) as client:
            # Just initialize the client
            assert client is not None
            logger.info(f"Client {i+1}/3 initialized successfully")

        # At this point, __aexit__ should have completed without hanging
        logger.info(f"Client {i+1}/3 exited context manager successfully")

        # Force garbage collection after each client
        gc.collect()

    # If we got here without hanging, the test passes
    assert True
