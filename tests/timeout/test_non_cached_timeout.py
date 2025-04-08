#!/usr/bin/env python

import asyncio
import time
import os
import random
import pytest
from datetime import datetime, timezone, timedelta
from pathlib import Path
import shutil
import logging
import gc

from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import MarketType, Interval
from utils.logger_setup import logger
from tests.utils.unified_logging import (
    configure_root_logger_for_testing,
)

# Note: Tests using curl_cffi may show a "Task was destroyed but it is pending!" warning
# related to the AsyncCurl._force_timeout coroutine. This is a known issue with how
# curl_cffi handles its internal timeouts and doesn't affect test functionality.
# For more details, see: https://github.com/yifeikong/curl_cffi/issues/68


# Configure root logger for testing to ensure proper log capture
@pytest.fixture(scope="function", autouse=True)
def setup_logging():
    """Configure logging properly for the test."""
    # Configure root logger to allow all messages to be captured
    configure_root_logger_for_testing()
    # Set log level to DEBUG to capture all relevant messages
    logger.setLevel("DEBUG")
    yield
    # No teardown needed as pytest will handle fixture cleanup


@pytest.mark.asyncio
async def test_non_cached_timeout(caplog):
    """Test that REST API data retrieval completes without timeouts.

    This test:
    1. Uses a random date in the past to ensure no cache hits
    2. Clears the cache directory before starting
    3. Makes a direct REST API call with cache disabled
    4. Verifies no timeout logs are created
    """
    logger.info("Starting non-cached timeout test")

    # Create the directory if it doesn't exist
    logs_dir = Path("logs/timeout_incidents")
    logs_dir.mkdir(parents=True, exist_ok=True)

    # Delete any existing timeout log
    log_path = Path("logs/timeout_incidents/timeout_log.txt")
    if log_path.exists():
        os.remove(log_path)
        logger.info("Removed old timeout log")

    # Clear cache directory if it exists to ensure fresh test
    cache_dir = Path("tmp/cache")
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
        logger.info("Cleared cache directory")
        cache_dir.mkdir(parents=True, exist_ok=True)

    # Create a DataSourceManager
    manager = DataSourceManager(market_type=MarketType.SPOT)
    try:
        # Use a random date in the past that won't be in cache
        # Random date between 30 and 60 days ago
        days_ago = random.randint(30, 60)
        random_past_date = datetime.now(timezone.utc) - timedelta(days=days_ago)

        # Create a 7-minute window for the test
        end_time = random_past_date + timedelta(minutes=7)
        start_time = random_past_date

        logger.info(f"Fetching data for BTCUSDT from {start_time} to {end_time}")
        logger.info(
            f"Using a random date {days_ago} days in the past to ensure no cache hits"
        )

        # Time the operation
        start_time_op = time.time()

        # Force REST API to ensure network call
        df = await manager.get_data(
            "BTCUSDT",
            start_time,
            end_time,
            interval=Interval.MINUTE_1,
            enforce_source=DataSource.REST,  # Force REST API
            use_cache=False,  # Disable cache
        )

        elapsed = time.time() - start_time_op
        logger.info(f"Operation completed in {elapsed:.2f} seconds")

        # Assert that we got data back
        assert not df.empty, "Received empty DataFrame (timeout may have occurred)"

        # Log the row count for informational purposes
        logger.info(f"Received {len(df)} rows without timeout")

        # Check if timeout log exists
        assert not log_path.exists(), f"Timeout log was created at {log_path}"

        # Check for timeout-related ERROR messages in logs
        timeout_messages = [
            r
            for r in caplog.records
            if "timeout" in r.message.lower() and r.levelno >= logging.ERROR
        ]
        assert (
            not timeout_messages
        ), f"Found {len(timeout_messages)} timeout error messages in logs"

        # Verify log records have been captured correctly
        assert any(
            "Fetching data for BTCUSDT" in r.message for r in caplog.records
        ), "Expected log message not found in captured logs"

    finally:
        # Clean up the manager
        await manager.__aexit__(None, None, None)
        logger.info("Test cleanup complete")

        # Force garbage collection to help release any lingering resources
        gc.collect()


if __name__ == "__main__":
    asyncio.run(test_non_cached_timeout(None))
