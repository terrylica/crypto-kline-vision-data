#!/usr/bin/env python
"""Test configuration and fixtures."""

import pytest
from datetime import datetime, timedelta, timezone
import aiohttp
from typing import AsyncGenerator
import tempfile
from pathlib import Path
import shutil
import pandas as pd


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
def sample_ohlcv_data():
    """Create sample OHLCV data."""
    dates = pd.date_range(
        start="2022-01-13 15:15:00",
        end="2022-01-14 15:45:00",
        freq="15min",
        tz=timezone.utc,
    )

    df = pd.DataFrame(
        {
            "open": [43798.71] * len(dates),
            "high": [43802.42] * len(dates),
            "low": [43208.00] * len(dates),
            "close": [43312.84] * len(dates),
            "volume": [670.47] * len(dates),
            "close_time": list(range(len(dates))),
            "quote_volume": [29000000.0] * len(dates),
            "trades": [1000] * len(dates),
            "taker_buy_volume": [300.0] * len(dates),
            "taker_buy_quote_volume": [13000000.0] * len(dates),
        },
        index=dates,
    )
    df.index.name = "open_time"
    return df
