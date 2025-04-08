#!/usr/bin/env python

"""
Test the RestDataClient cleanup behavior to ensure it properly releases resources.
"""

import asyncio
import os
import pytest
from datetime import datetime, timedelta, timezone

from core.rest_data_client import RestDataClient
from utils.market_constraints import MarketType, Interval
from utils.logger_setup import logger, set_timeout_log_file


@pytest.mark.asyncio
async def test_rest_client_cleanup_no_hang():
    """Test that RestDataClient cleans up without hanging when initialized and exited."""
    # Setup temporary log file for timeout
    temp_log_file = os.path.join("logs", "timeout_incidents", "test_rest_cleanup.log")
    set_timeout_log_file(temp_log_file)

    # Create and exit client, should clean up properly
    async with RestDataClient(
        market_type=MarketType.SPOT, max_concurrent=3, retry_count=1
    ):
        pass  # Just initialize and exit

    # If we get here without hanging, test passed
    assert True, "Client exited cleanly without hanging"


@pytest.mark.asyncio
async def test_rest_client_cleanup_after_fetch():
    """Test that RestDataClient cleans up properly after fetch operations."""
    # Setup temporary log file for timeout
    temp_log_file = os.path.join("logs", "timeout_incidents", "test_rest_cleanup.log")
    set_timeout_log_file(temp_log_file)

    # Time range for recent data (last hour)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    async with RestDataClient(
        market_type=MarketType.SPOT, max_concurrent=3, retry_count=1
    ) as client:
        # Fetch a small amount of data - BTCUSDT 1-minute candles for the last hour
        df, stats = await client.fetch(
            symbol="BTCUSDT",
            interval=Interval.MINUTE_1,
            start_time=start_time,
            end_time=end_time,
        )
        # Verify that we got data or at least didn't hang
        assert stats is not None, "Stats should be returned even if no data"

    # If we get here without hanging, test passed
    assert True, "Client exited cleanly after fetch"


@pytest.mark.asyncio
async def test_multiple_client_creation_no_hang():
    """Test creating and cleaning up multiple clients in sequence."""
    # Setup temporary log file for timeout
    temp_log_file = os.path.join("logs", "timeout_incidents", "test_rest_cleanup.log")
    set_timeout_log_file(temp_log_file)

    for i in range(3):  # Create 3 clients in sequence
        async with RestDataClient(
            market_type=MarketType.SPOT, max_concurrent=3, retry_count=1
        ):
            # Minimal sleep to ensure startup completes
            await asyncio.sleep(0.1)
            logger.debug(f"Client {i+1} initialized")

        # Sleep between client creation to ensure proper cleanup
        await asyncio.sleep(0.1)
        logger.debug(f"Client {i+1} cleanup completed")

    # If we got here, all clients cleaned up properly
    assert True, "Multiple clients created and cleaned up without hanging"


@pytest.mark.asyncio
async def test_timeout_handling():
    """Test that timeout is properly handled and logged."""
    # Setup temporary log file for timeout
    temp_log_file = os.path.join("logs", "timeout_incidents", "test_timeout.log")
    set_timeout_log_file(temp_log_file)

    # Clean up the log file if it exists
    if os.path.exists(temp_log_file):
        os.remove(temp_log_file)

    # Set up a very short timeout to trigger a timeout error
    async with RestDataClient(
        market_type=MarketType.SPOT,
        max_concurrent=1,  # Limit concurrency to make timeout more likely
        retry_count=0,  # No retries to make the test faster
        fetch_timeout=0.1,  # Very short timeout to ensure it triggers
    ) as client:
        # Use a date range big enough to require multiple chunks
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(
            days=30
        )  # 30 days of data should be too much to fetch in 0.1s

        # Fetch data that should timeout
        df, stats = await client.fetch(
            symbol="BTCUSDT",
            interval=Interval.MINUTE_1,
            start_time=start_time,
            end_time=end_time,
        )

        # Verify we got an empty dataframe due to timeout
        assert df.empty, "DataFrame should be empty when timeout occurs"

    # Wait a moment for log to be written
    await asyncio.sleep(0.5)

    # Verify that the timeout was logged
    assert os.path.exists(temp_log_file), "Timeout log file should exist"

    # Read the log file to verify it contains timeout information
    with open(temp_log_file, "r") as f:
        log_content = f.read()

    assert "TIMEOUT" in log_content, "Log should contain TIMEOUT message"
    assert (
        "REST API fetch" in log_content
    ), "Log should mention the REST API fetch operation"
