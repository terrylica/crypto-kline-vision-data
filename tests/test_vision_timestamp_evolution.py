#!/usr/bin/env python
"""Tests for VisionDataClient's handling of different timestamp formats."""

import pytest
from datetime import datetime, timezone, timedelta

import logging

from ml_feature_set.binance_data_services.core.vision_data_client import VisionDataClient

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_timestamp_format_handling():
    """Test VisionDataClient's ability to handle both timestamp formats.

    Verifies that regardless of input timestamp format (ms or us),
    the client outputs data in a consistent format with:
    - UTC timezone
    - Monotonic timestamps
    - 1-second intervals
    - Proper datetime index
    """
    async with VisionDataClient[str]("BTCUSDT") as client:  # type: ignore[valid-type]
        # Test 2024 data (millisecond format)
        start_2024 = datetime(2024, 12, 1, tzinfo=timezone.utc)
        end_2024 = start_2024 + timedelta(hours=1)

        logger.info(f"Testing 2024 data (milliseconds): {start_2024} to {end_2024}")
        df_2024 = await client.fetch(start_2024, end_2024)

        # Test 2025 data (microsecond format)
        now = datetime.now(timezone.utc)
        # Round to second precision since Binance Vision provides 1-second granularity
        start_2025 = (now - timedelta(days=2)).replace(microsecond=0)
        end_2025 = (start_2025 + timedelta(hours=1)).replace(microsecond=0)

        logger.info(f"Testing 2025 data (microseconds): {start_2025} to {end_2025}")
        df_2025 = await client.fetch(start_2025, end_2025)

        # Verify both datasets have consistent properties
        for period, df in [("2024", df_2024), ("2025", df_2025)]:
            logger.info(f"\nValidating {period} data:")
            logger.info(f"Shape: {df.shape}")
            logger.info(f"Index range: {df.index.min()} to {df.index.max()}")  # type: ignore
            logger.info(f"Sample data:\n{df.head()}")

            # Verify data properties
            assert not df.empty, f"{period} data is empty"
            assert df.index.is_monotonic_increasing, f"{period} data is not monotonic"  # type: ignore
            assert df.index.tz == timezone.utc, f"{period} data timezone is not UTC"  # type: ignore

            # Verify 1-second intervals
            time_diffs = df.index.to_series().diff().dropna()  # type: ignore
            assert all(diff == timedelta(seconds=1) for diff in time_diffs), f"{period} data has irregular intervals"  # type: ignore
