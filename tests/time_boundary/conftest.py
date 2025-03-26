#!/usr/bin/env python
"""Test configuration and fixtures."""

import pytest
from datetime import datetime, timedelta, timezone
import aiohttp
import httpx
from typing import AsyncGenerator
import tempfile
from pathlib import Path
import shutil
import pandas as pd
import logging

# Configure logging
logger = logging.getLogger(__name__)


@pytest.fixture
def time_window():
    """Provide a default time window for tests."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=5)
    return start_time, end_time


@pytest.fixture
def default_symbol():
    """Provide a default symbol for tests."""
    return "BTCUSDT"


@pytest.fixture
async def api_session() -> AsyncGenerator[aiohttp.ClientSession, None]:
    """Fixture to provide an aiohttp ClientSession for API tests."""
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        yield session


@pytest.fixture
def test_symbol() -> str:
    """Fixture to provide a test trading pair symbol."""
    return "BTCUSDT"


@pytest.fixture
def test_interval() -> str:
    """Fixture to provide a test time interval."""
    return "1s"


@pytest.fixture
def temp_cache_dir():
    """Create temporary cache directory."""
    temp_dir = Path(tempfile.mkdtemp())
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def get_safe_test_time_range():
    """Generate a time range that's safely beyond the Vision API consolidation delay.

    This function returns a known historical date range that is guaranteed to have
    data available in the Binance Vision API. It uses fixed dates from 2022 that
    are stable and well-established in the historical data archives.

    Following pytest-construction.mdc guidelines:
    - Uses real-world data only (no mocking)
    - Focuses on reliable historical dates to prevent test skipping
    - Ensures consistent tests by using known good data periods

    Args:
        duration: Duration of the time range (default: 1 hour)

    Returns:
        Tuple of (start_time, end_time) in UTC, rounded to nearest second
    """

    def _get_safe_test_time_range(duration: timedelta = timedelta(hours=1)):
        # Use a guaranteed date that will always have data in the Vision API
        # June 15, 2022 is a stable historical date with confirmed data availability
        # This is well beyond any consolidation delays and historical
        # data purge policies
        start_time = datetime(2022, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        end_time = start_time + duration

        # Constrain end time to be within same day if the duration is small
        # This helps avoid day boundary issues with the Vision API
        if duration < timedelta(days=1):
            end_of_day = datetime(2022, 6, 15, 23, 59, 59, tzinfo=timezone.utc)
            end_time = min(end_time, end_of_day)

        logger.info(
            f"Using Vision API safe historical time range: {start_time} to {end_time}"
        )
        return start_time, end_time

    return _get_safe_test_time_range


@pytest.fixture
async def sample_ohlcv_data():
    """Retrieve real OHLCV data from Binance API for 1-second interval tests.

    Following pytest-construction.mdc guidelines:
    - No mocking or sample data
    - Using real-world data only
    - Implementing actual integration with Binance API
    """
    # Use recent real data (2 days ago to ensure availability)
    end_time = datetime.now(timezone.utc) - timedelta(days=2)
    # Round to nearest minute to ensure consistent behavior
    end_time = end_time.replace(second=0, microsecond=0)
    # Get 1 minute of 1-second data
    start_time = end_time - timedelta(minutes=1)

    # Convert to milliseconds for API
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    # Symbol to use
    symbol = "BTCUSDT"

    # Interval - use "1s" directly
    interval = "1s"

    # API endpoint
    api_endpoint = "https://api.binance.com/api/v3/klines"

    # Parameters
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,  # Maximum allowed
    }

    try:
        # Make the API call to get real data
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(api_endpoint, params=params)
            response.raise_for_status()
            api_data = response.json()

        if not api_data:
            # If no data is available for the specified period, try a slightly older period
            end_time = datetime.now(timezone.utc) - timedelta(days=3)
            end_time = end_time.replace(second=0, microsecond=0)
            start_time = end_time - timedelta(minutes=1)

            start_ms = int(start_time.timestamp() * 1000)
            end_ms = int(end_time.timestamp() * 1000)

            params["startTime"] = start_ms
            params["endTime"] = end_ms

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(api_endpoint, params=params)
                response.raise_for_status()
                api_data = response.json()

        # Convert to DataFrame
        columns = [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_volume",
            "taker_buy_quote_volume",
            "ignore",
        ]

        df = pd.DataFrame(api_data, columns=columns)

        # Convert timestamp columns to datetime and set index
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

        # Set index
        df = df.set_index("open_time")

        # Convert numeric columns
        numeric_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trades",
            "taker_buy_volume",
            "taker_buy_quote_volume",
        ]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col])

        # Drop the 'ignore' column
        df = df.drop(columns=["ignore"])

        return df
    except Exception as e:
        logger.warning(f"Failed to fetch real data from Binance API: {e}")
        # Return an empty DataFrame with the correct structure in case of failure
        empty_df = pd.DataFrame(
            columns=[
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "trades",
                "taker_buy_volume",
                "taker_buy_quote_volume",
            ]
        )
        empty_df.index = pd.DatetimeIndex([], name="open_time")
        return empty_df
