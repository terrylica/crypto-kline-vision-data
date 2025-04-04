#!/usr/bin/env python
"""Test configuration and fixtures."""

import pytest
from datetime import datetime, timedelta, timezone

from typing import AsyncGenerator
import tempfile
from pathlib import Path
import shutil
import pandas as pd
import logging
from utils.network_utils import create_client, safely_close_client

# Configure logging
logger = logging.getLogger(__name__)


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "interval(value): specify the interval to use for the test",
    )


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
async def api_session() -> AsyncGenerator:
    """Fixture to provide a HTTP client for API tests."""
    client = create_client(timeout=10.0)  # This will default to curl_cffi
    try:
        yield client
    finally:
        await safely_close_client(client)


@pytest.fixture
def test_symbol() -> str:
    """Fixture to provide a test trading pair symbol."""
    return "BTCUSDT"


@pytest.fixture
def test_interval(request) -> str:
    """Fixture to provide a test time interval.

    Returns "1s" for SPOT market type (default) and "1m" for FUTURES market types.
    The appropriate interval is determined based on:
    1. Explicit interval marker: @pytest.mark.interval("1m")
    2. Market type: FUTURES market types use "1m" instead of "1s"
    """
    # Check for an explicit interval marker
    marker = request.node.get_closest_marker("interval")
    if marker and marker.args:
        return marker.args[0]  # Use explicitly marked interval

    # Get the test's market_type fixture value if available
    try:
        if hasattr(request, "getfixturevalue"):
            market_type = request.getfixturevalue("market_type")
            if market_type and "FUTURES" in str(market_type):
                return "1m"  # Use 1m interval for FUTURES market types
    except Exception:
        # Fixture might not be available, continue checking other possibilities
        pass

    # Check for manager fixture, looking for its market_type
    try:
        if hasattr(request, "getfixturevalue"):
            manager = request.getfixturevalue("manager")
            if hasattr(manager, "market_type") and "FUTURES" in str(
                manager.market_type
            ):
                return "1m"  # Use 1m interval for FUTURES market types
    except Exception:
        # Fixture might not be available, continue with default
        pass

    # Default to 1s (only valid for SPOT markets)
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
        client = create_client(timeout=10.0)  # Use curl_cffi by default

        try:
            response = await client.get(api_endpoint, params=params)

            # Use curl_cffi style response handling
            if response.status_code != 200:
                raise Exception(f"HTTP error {response.status_code}: {response.text}")
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

                response = await client.get(api_endpoint, params=params)

                # Use curl_cffi style response handling
                if response.status_code != 200:
                    raise Exception(
                        f"HTTP error {response.status_code}: {response.text}"
                    )
                api_data = response.json()
        finally:
            # Close the client
            await safely_close_client(client)

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
            "quote_asset_volume",
            "count",
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
                "quote_asset_volume",
                "count",
                "taker_buy_volume",
                "taker_buy_quote_volume",
            ]
        )
        empty_df.index = pd.DatetimeIndex([], name="open_time")
        return empty_df
