#!/usr/bin/env python
"""Tests for Vision Data Client with real data."""

import pytest
from datetime import datetime, timezone, timedelta
import logging

from ml_feature_set.binance_data_services.core.vision_data_client import VisionDataClient

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_test_time_range(days_ago: int = 2, duration: timedelta = timedelta(hours=1)) -> tuple[datetime, datetime]:
    """Generate a time range for testing.

    Args:
        days_ago: Number of days ago to start range
        duration: Duration of the time range (default: 1 hour)

    Returns:
        Tuple of (start_time, end_time) in UTC, rounded to nearest second
    """
    now = datetime.now(timezone.utc)
    # Round to nearest second to avoid sub-second precision issues
    start_time = (now - timedelta(days=days_ago)).replace(microsecond=0)
    end_time = (start_time + duration).replace(microsecond=0)
    logger.info(f"Generated test time range: {start_time} to {end_time}")
    return start_time, end_time


@pytest.mark.asyncio
async def test_real_data_validation():
    """Test real data retrieval and validation."""
    # Test with recent data
    start_time, end_time = get_test_time_range(days_ago=2)

    logger.info(f"Testing data retrieval: {start_time} to {end_time}")

    async with VisionDataClient[str]("BTCUSDT") as client:  # type: ignore[valid-type]
        df = await client.fetch(start_time, end_time)

        # Basic validation
        assert not df.empty
        assert df.index.is_monotonic_increasing  # type: ignore
        assert df.index.tz == timezone.utc  # type: ignore

        # Verify data completeness
        expected_rows = int((end_time - start_time).total_seconds())
        assert len(df) == expected_rows + 1, "Missing data points"  # +1 because end time is inclusive

        # Log data sample
        logger.info(f"\nFirst few rows:\n{df.head()}")
        logger.info(f"Data shape: {df.shape}")
        logger.info(f"Index range: {df.index.min()} to {df.index.max()}")  # type: ignore
