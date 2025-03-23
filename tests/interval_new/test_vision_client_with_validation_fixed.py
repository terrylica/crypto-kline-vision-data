#!/usr/bin/env python
"""Tests for the VisionDataClient with modified validation behavior."""

import pytest
import asyncio
from datetime import datetime, timezone, timedelta
import pandas as pd
import logging
from pathlib import Path
import tempfile

from utils.logger_setup import get_logger
from utils.market_constraints import Interval
from utils.time_alignment import TimeRangeManager
from core.vision_data_client_enhanced import VisionDataClient

logger = get_logger(__name__, "INFO", show_path=False)


@pytest.fixture
def temp_cache_dir():
    """Provide a temporary directory for caching."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


def get_safe_test_time_range():
    """Get a time range that is likely to have data available."""
    # Using a fixed date for consistent testing
    # January 2023 should have stable data across most markets
    start_date = datetime(2023, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    end_date = datetime(2023, 1, 15, 1, 0, 0, tzinfo=timezone.utc)
    return start_date, end_date


@pytest.mark.asyncio
async def test_vision_client_with_validation_fixed(temp_cache_dir):
    """Test that the Vision client now works with the fixed validation."""
    # Setup test parameters
    start_date, end_date = get_safe_test_time_range()

    logger.info(
        f"Testing VisionDataClient with fixed validation: {start_date.isoformat()} to {end_date.isoformat()}"
    )

    # Create client
    client = VisionDataClient(
        symbol="BTCUSDT",
        interval="1h",  # Using 1h interval which has the issue
        cache_dir=temp_cache_dir,
        market_type="spot",  # Using string instead of enum
    )

    # Fetch data with the normal fetch method (now with fixed validation)
    logger.info("Calling standard fetch method with fixed validation...")
    df = await client.fetch(start_date, end_date)

    # Check results
    logger.info(f"Standard fetch result is empty: {df.empty}")
    if not df.empty:
        logger.info(f"Standard fetch result shape: {df.shape}")
        logger.info(
            f"Standard fetch result range: {df.index.min()} to {df.index.max()}"
        )

        # Make assertions to verify the test passes
        assert df.shape[0] > 0, "Should have at least one row of data"
        assert df.shape[1] > 0, "Should have at least one column of data"

        # Check that we got data for at least the start time
        assert (
            df.index.min() <= start_date
        ), f"Data should start no later than {start_date}"
    else:
        pytest.fail("DataFrame should not be empty after validation fix")


@pytest.mark.asyncio
async def test_batch_fetch_with_fixed_validation(temp_cache_dir):
    """Test the batch fetch functionality with the fixed validation."""
    # Setup test parameters
    start_date, end_date = get_safe_test_time_range()
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

    logger.info(
        f"Testing batch fetch with fixed validation: {start_date.isoformat()} to {end_date.isoformat()}"
    )
    logger.info(f"Testing symbols: {symbols}")

    # Create client
    client = VisionDataClient(
        symbol="BTCUSDT",  # Default symbol, will be overridden in batch_fetch
        interval="1h",  # Using 1h interval which has the issue
        cache_dir=temp_cache_dir,
        market_type="spot",  # Using string instead of enum
    )

    # Perform batch fetch
    results = await client.batch_fetch(symbols, start_date, end_date)

    # Verify results
    logger.info(f"Batch fetch returned results for {len(results)} symbols")

    for symbol, df in results.items():
        logger.info(
            f"Results for {symbol}: empty={df.empty}, shape={df.shape if not df.empty else 'N/A'}"
        )
        if not df.empty:
            logger.info(f"Data range: {df.index.min()} to {df.index.max()}")

            # Make assertions
            assert df.shape[0] > 0, f"Should have at least one row of data for {symbol}"
            assert (
                df.shape[1] > 0
            ), f"Should have at least one column of data for {symbol}"

    # Ensure at least one symbol returned data
    non_empty_results = {s: df for s, df in results.items() if not df.empty}
    assert len(non_empty_results) > 0, "At least one symbol should return data"
