#!/usr/bin/env python
"""Centralized market data validation tests.

System Under Test (SUT):
- Binance REST API (external)
- Market data structures and formats
- Data validation utilities

This module consolidates all market data validation logic that was previously
spread across multiple test files. It focuses on three key areas:
1. Data integrity (chronological order, completeness)
2. Data structure (columns, types)
3. Time window validation

The tests in this module serve as a foundation for other test suites by providing
reusable validation functions and baseline tests for market data structures.
They ensure that data from the Binance API meets the expected format and quality
requirements before being processed by higher-level components.
"""

import pytest
import pandas as pd
import aiohttp
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional
import pytest_asyncio

from utils.logger_setup import get_logger

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


@pytest_asyncio.fixture
async def api_session():
    """Create an aiohttp ClientSession for API requests."""
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
def test_symbol():
    """Return test symbol."""
    return "BTCUSDT"


@pytest.fixture
def test_interval():
    """Return test interval."""
    return "1s"


def validate_market_data_structure(df: pd.DataFrame) -> None:
    """Validate market data structure and types."""
    # Column presence and types - support both raw API and market data client formats
    required_columns_raw = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]

    required_columns_client = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote",
    ]

    # Check if DataFrame matches either format
    has_raw_format = all(col in df.columns for col in required_columns_raw)
    has_client_format = all(col in df.columns for col in required_columns_client)

    assert has_raw_format or has_client_format, (
        f"DataFrame columns don't match either format.\n"
        f"Found: {df.columns.tolist()}\n"
        f"Expected raw API format: {required_columns_raw}\n"
        f"Expected client format: {required_columns_client}"
    )

    # Numeric columns validation
    numeric_cols = ["open", "high", "low", "close", "volume", "quote_volume"]
    for col in numeric_cols:
        assert (
            df[col].dtype == "float64"
        ), f"{col} has incorrect data type: {df[col].dtype}"

    # Integer columns validation - accept int32 or int64
    assert df["trades"].dtype in [
        "int32",
        "int64",
        "float64",
    ], f"trades column has incorrect type: {df['trades'].dtype}"


def validate_time_integrity(
    df: pd.DataFrame,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> None:
    """Validate chronological integrity and time windows."""
    # Chronological order
    assert df["open_time"].is_monotonic_increasing, "Data is not in chronological order"

    # Time gaps (for 1s data)
    time_gaps = df["open_time"].diff()[1:] != pd.Timedelta(seconds=1)
    if time_gaps.any():
        gap_indices = df.index[time_gaps]
        gap_times = df["open_time"][gap_indices]
        logger.warning(f"Found time gaps at: {gap_times.tolist()}")
        assert False, "Found time gaps in data"

    # Time boundaries if provided
    if start_time:
        # Strip microseconds for comparison to handle different precision
        df_min_time = df["open_time"].min().replace(microsecond=0)
        start_time_no_micro = start_time.replace(microsecond=0)
        assert df_min_time >= start_time_no_micro, "Data starts before requested window"
    if end_time:
        # Strip microseconds for comparison to handle different precision
        df_max_time = df["open_time"].max().replace(microsecond=0)
        end_time_no_micro = end_time.replace(microsecond=0)
        assert df_max_time <= end_time_no_micro, "Data ends after requested window"


@pytest.mark.real
@pytest.mark.asyncio
async def test_market_data_integrity(
    api_session: aiohttp.ClientSession, test_symbol: str, test_interval: str
):
    """Test market data integrity with real API data."""
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(minutes=5)

    params = {
        "symbol": test_symbol,
        "interval": test_interval,
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(now.timestamp() * 1000),
        "limit": 300,  # 5 minutes of 1s data
    }

    # Fetch data from API
    base_url = "https://api.binance.com/api/v3/klines"
    async with api_session.get(base_url, params=params) as response:
        assert response.status == 200, "API request failed"
        data = await response.json()

    # Convert to DataFrame with correct column names
    columns = pd.Index(
        [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ]
    )
    df = pd.DataFrame(data, columns=columns)

    # Convert timestamps
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    # Convert numeric columns
    numeric_cols = ["open", "high", "low", "close", "volume", "quote_volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="raise")

    df["trades"] = pd.to_numeric(df["trades"], errors="raise").astype("int64")

    # Run validations
    validate_market_data_structure(df)
    validate_time_integrity(df, start_time, now)


@pytest.mark.real
@pytest.mark.asyncio
async def test_market_data_consistency(
    api_session: aiohttp.ClientSession, test_symbol: str, test_interval: str
):
    """Test consistency of market data structure between fetches."""
    now = datetime.now(timezone.utc)
    # Use historical data to avoid live data changes
    start_time = now - timedelta(hours=1)
    end_time = start_time + timedelta(minutes=1)

    params = {
        "symbol": test_symbol,
        "interval": test_interval,
        "startTime": int(start_time.timestamp() * 1000),
        "endTime": int(end_time.timestamp() * 1000),
        "limit": 60,
    }

    # Fetch data twice
    base_url = "https://api.binance.com/api/v3/klines"
    async with api_session.get(base_url, params=params) as response:
        data1 = await response.json()

    await asyncio.sleep(1)  # Small delay between requests

    async with api_session.get(base_url, params=params) as response:
        data2 = await response.json()

    # Convert both to DataFrames with correct column names
    columns = pd.Index(
        [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ]
    )

    df1 = pd.DataFrame(data1, columns=columns)
    df2 = pd.DataFrame(data2, columns=columns)

    # Convert timestamps and numeric columns
    for df in [df1, df2]:
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        for col in ["open", "high", "low", "close", "volume", "quote_volume"]:
            df[col] = pd.to_numeric(df[col], errors="raise")
        df["trades"] = pd.to_numeric(df["trades"], errors="raise").astype("int32")

    # Compare data structure (not values)
    assert df1.shape == df2.shape, "DataFrames have different shapes"
    assert all(df1.columns == df2.columns), "DataFrames have different columns"
    assert df1.dtypes.equals(df2.dtypes), "DataFrames have different column types"
