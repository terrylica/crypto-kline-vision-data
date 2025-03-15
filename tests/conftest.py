#!/usr/bin/env python
"""Test configuration and fixtures."""

import pytest
from datetime import datetime, timedelta, timezone
import aiohttp
from typing import AsyncGenerator


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
