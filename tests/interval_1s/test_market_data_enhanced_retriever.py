#!/usr/bin/env python
"""Integration tests for market data client with real Binance API.

System Under Test (SUT):
- core.market_data_client.EnhancedRetriever
- utils.market_constraints (indirectly)

This test suite verifies the functionality of the EnhancedRetriever, which is
responsible for fetching market data from the Binance REST API. It tests:

1. Basic market data retrieval with validation
2. Large dataset retrieval with chunking
3. API limits and error handling
4. Data structure and integrity

Note: This client is specifically optimized for 1-second data retrieval.
For other intervals, use the standard Binance client or vision client.
"""

import pytest
import asyncio
from datetime import datetime, timedelta, timezone
import aiohttp
import pytest_asyncio

from utils.logger_setup import get_logger
from core.market_data_client import EnhancedRetriever
from utils.market_constraints import (
    Interval,
    MarketType,
    get_endpoint_url,
    get_market_capabilities,
)
from tests.interval_1s.test_market_data_structure_validation import (
    validate_market_data_structure,
    validate_time_integrity,
)

logger = get_logger(__name__, "INFO", show_path=False)

# Test configuration
TEST_SYMBOLS = ["BTCUSDT"]  # Focus on BTC for reliable data
TEST_INTERVAL = Interval.SECOND_1  # This client only supports 1-second data
ONE_DAY = timedelta(days=1)
ONE_HOUR = timedelta(hours=1)
FIVE_MINUTES = timedelta(minutes=5)


@pytest.fixture
def reference_time():
    """Fixture providing a consistent reference time for tests."""
    return datetime.now(timezone.utc)


@pytest_asyncio.fixture
async def retriever():
    """Fixture providing an EnhancedRetriever instance."""
    async with EnhancedRetriever(market_type=MarketType.SPOT) as client:
        yield client


@pytest.mark.real
@pytest.mark.asyncio
async def test_market_data_retrieval(
    retriever: EnhancedRetriever, reference_time: datetime
):
    """Test market data retrieval with validation."""
    end_time = reference_time
    start_time = end_time - FIVE_MINUTES

    logger.info(f"Testing market data retrieval for {TEST_SYMBOLS[0]}")
    df, metadata = await retriever.fetch(
        symbol=TEST_SYMBOLS[0],
        interval=TEST_INTERVAL,
        start_time=start_time,
        end_time=end_time,
    )

    # Convert column names if needed
    if "taker_buy_volume" in df.columns:
        df = df.rename(
            columns={
                "taker_buy_volume": "taker_buy_base_volume",
                "timestamp": "open_time",
            }
        )

    # Validate data structure and integrity
    validate_market_data_structure(df)
    validate_time_integrity(df, start_time, end_time)

    # Validate metadata
    assert metadata["total_records"] > 0, "No records retrieved"
    assert metadata["chunks_failed"] == 0, "Some chunks failed to download"


@pytest.mark.real
@pytest.mark.asyncio
async def test_large_data_retrieval(
    retriever: EnhancedRetriever, reference_time: datetime
):
    """Test retrieval of larger datasets."""
    end_time = reference_time
    start_time = end_time - ONE_HOUR

    logger.info(f"Testing large data retrieval for {TEST_SYMBOLS[0]}")
    df, metadata = await retriever.fetch(
        symbol=TEST_SYMBOLS[0],
        interval=TEST_INTERVAL,
        start_time=start_time,
        end_time=end_time,
    )

    # Convert column names if needed
    if "taker_buy_volume" in df.columns:
        df = df.rename(
            columns={
                "taker_buy_volume": "taker_buy_base_volume",
                "timestamp": "open_time",
            }
        )

    # Validate chunking behavior
    assert (
        metadata["chunks_processed"] > 1
    ), "Large dataset should be processed in chunks"
    assert metadata["chunks_failed"] == 0, "No chunks should fail"

    # Validate data
    validate_market_data_structure(df)
    validate_time_integrity(df, start_time, end_time)


@pytest.mark.real
@pytest.mark.asyncio
async def test_api_limits_and_chunking():
    """Test API limits and chunking behavior with direct API calls."""
    # Test different chunk sizes around the 1000-record limit
    chunk_sizes = [500, 999, 1000, 1001, 1500]
    end_time = datetime.now(timezone.utc).replace(microsecond=0)

    # Get market capabilities
    capabilities = get_market_capabilities(MarketType.SPOT)
    endpoint_url = get_endpoint_url(MarketType.SPOT)

    # Configure session with proper timeout settings
    timeout = aiohttp.ClientTimeout(total=30, connect=5, sock_connect=5, sock_read=10)
    connector = aiohttp.TCPConnector(limit=10, force_close=False)

    logger.info("Creating aiohttp client session with increased timeout settings")
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for chunk_size in chunk_sizes:
            logger.info(f"\nTesting chunk size: {chunk_size}")

            # Request enough data to test the chunk size
            start_time = end_time - timedelta(seconds=chunk_size)
            params = {
                "symbol": TEST_SYMBOLS[0],
                "interval": TEST_INTERVAL.value,
                "startTime": int(start_time.timestamp() * 1000),
                "endTime": int(end_time.timestamp() * 1000),
                "limit": chunk_size,
            }

            logger.info(f"Making API request to {endpoint_url} with params: {params}")
            try:
                async with session.get(endpoint_url, params=params) as response:
                    assert (
                        response.status == 200
                    ), f"API request failed for chunk size {chunk_size}"
                    data = await response.json()
                    records = len(data)

                    # Log actual records received for debugging
                    logger.info(
                        f"Requested {chunk_size} records, received {records} records"
                    )

                    # Verify record limit enforcement
                    if chunk_size <= 1000:
                        assert (
                            records == chunk_size
                        ), f"For chunk_size={chunk_size}, expected {chunk_size} records but got {records}"
                    else:
                        assert (
                            records == 1000
                        ), f"For chunk_size={chunk_size}, expected 1000 records (API limit) but got {records}"

                    # Verify data continuity
                    if records > 1:
                        timestamps = [int(x[0]) for x in data]
                        diffs = [
                            timestamps[i] - timestamps[i - 1]
                            for i in range(1, len(timestamps))
                        ]
                        assert all(
                            diff == 1000 for diff in diffs
                        ), f"Found non-standard time gaps in chunk size {chunk_size}"
            except aiohttp.ClientError as e:
                logger.error(f"API request failed: {str(e)}")
                raise

            # Rate limit compliance
            await asyncio.sleep(1)
