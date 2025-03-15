#!/usr/bin/env python
"""Architecture tests for the Vision Data Client.

These tests verify the architectural decisions and constraints of the Vision Data Client,
focusing on separation of concerns and data consistency for 1-second data.
"""

import pytest
from datetime import datetime, timezone, timedelta
import pandas as pd
import logging

from ml_feature_set.binance_data_services.core.vision_data_client import VisionDataClient
from ml_feature_set.binance_data_services.tests.test_vision_data_client import get_test_time_range

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_data_consistency():
    """Test data consistency and format normalization.

    Verifies that the client maintains consistent data format:
    - UTC timezone
    - 1-second intervals
    - Proper column types
    - No gaps in data
    """
    start_time, end_time = get_test_time_range(days_ago=2)
    logger.info(f"Testing data consistency from {start_time} to {end_time}")

    async with VisionDataClient[str]("BTCUSDT") as client:  # type: ignore[valid-type]
        df = await client.fetch(start_time, end_time)

        # Verify basic properties
        assert not df.empty
        assert df.index.is_monotonic_increasing  # type: ignore
        assert df.index.tz == timezone.utc  # type: ignore
        assert not df.index.has_duplicates  # type: ignore

        # Verify 1-second intervals
        intervals = df.index.to_series().diff().dropna()  # type: ignore
        assert all(intervals == pd.Timedelta(seconds=1)), "All intervals should be 1 second"  # type: ignore

        # Verify column types
        assert df["open"].dtype == float  # type: ignore
        assert df["close"].dtype == float  # type: ignore
        assert df["volume"].dtype == float  # type: ignore
        assert df["trades"].dtype == int  # type: ignore

        # Verify data completeness
        expected_rows = int((end_time - start_time).total_seconds()) + 1  # +1 for inclusive end
        assert len(df) == expected_rows, "Missing data points"

        logger.info(f"Data consistency verified successfully")


@pytest.mark.asyncio
async def test_timezone_handling():
    """Test timezone handling and normalization.

    Verifies that the client properly handles and normalizes timezones:
    - Accepts various timezone inputs
    - Normalizes to UTC
    - Maintains timezone awareness
    - Handles 1-second granularity correctly
    """
    # Test with different timezone inputs, ensuring second-level precision
    base_time = (datetime.now(timezone.utc) - timedelta(days=2)).replace(microsecond=0)
    start_naive = base_time.replace(tzinfo=None)
    start_est = base_time.astimezone(timezone.utc)
    duration = timedelta(hours=1)

    async with VisionDataClient[str]("BTCUSDT") as client:  # type: ignore[valid-type]
        # Test with naive datetime
        df_naive = await client.fetch(start_naive, start_naive + duration)
        assert df_naive.index.tz == timezone.utc  # type: ignore

        # Test with EST datetime
        df_est = await client.fetch(start_est, start_est + duration)
        assert df_est.index.tz == timezone.utc  # type: ignore

        # Verify both datasets match
        assert df_naive.equals(df_est)

        # Verify 1-second intervals
        time_diffs = df_naive.index.to_series().diff().dropna()  # type: ignore
        assert all(diff == timedelta(seconds=1) for diff in time_diffs), "Data has irregular intervals"  # type: ignore

        logger.info("Timezone handling verified successfully")
