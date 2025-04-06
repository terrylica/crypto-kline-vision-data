#!/usr/bin/env python
"""Tests for VisionDataClient validation behavior especially future date handling."""

import pytest
from datetime import datetime, timezone, timedelta

from core.vision_data_client import VisionDataClient
from tests.utils.unified_logging import assert_log_contains


@pytest.mark.asyncio
async def test_vision_client_basic_fetch(caplog_unified):
    """Test basic fetch with valid date range."""
    caplog_unified.set_level("INFO")

    # Create client
    client = VisionDataClient("BTCUSDT", "1d")
    try:
        # Use recent but not too recent dates to ensure data availability
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=30)  # Go back 30 days
        end = now - timedelta(days=7)  # End 7 days ago

        result = await client.fetch(start, end)

        # Check we got some data
        assert not result.empty, "Expected non-empty DataFrame from fetch"
        assert len(result) > 0, "Expected at least one record"

        # Check logs
        assert_log_contains(caplog_unified, "Successfully downloaded", "INFO")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_vision_client_future_dates(caplog_unified):
    """Test that vision client properly rejects future dates."""
    caplog_unified.set_level("INFO")

    # Create client
    client = VisionDataClient("BTCUSDT", "1d")
    try:
        # Use dates that include the future
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=7)  # 7 days ago
        future_end = now + timedelta(days=1)  # 1 day in the future

        # Attempt to fetch with future date, should raise ValueError
        with pytest.raises(ValueError) as excinfo:
            await client.fetch(start, future_end)

        # Verify error message mentions future date
        assert (
            "future" in str(excinfo.value).lower()
        ), "Error should mention future date"

        # Check logs
        assert_log_contains(caplog_unified, "validation", "INFO")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_vision_client_date_truncation():
    """Test truncation behavior when handle_future_dates='truncate'."""
    # Create client with truncation enabled
    client = VisionDataClient(
        "BTCUSDT", "1d", future_date_handling="truncate"  # Enable truncation
    )
    try:
        # Use dates that include the future
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=7)
        future_end = now + timedelta(days=1)

        # This should not raise an exception but truncate to current time
        result = await client.fetch(start, future_end)

        # Verify successful fetch with truncated date
        assert not result.empty, "Expected non-empty DataFrame after truncation"

        # Check the actual end date in the result is not in the future
        if not result.empty:
            latest_timestamp = result.index.max()
            assert (
                latest_timestamp <= now
            ), "Latest data point should not be in the future"
    finally:
        await client.close()
