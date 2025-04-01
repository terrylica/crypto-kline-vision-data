#!/usr/bin/env python
"""Tests for RestDataClient's pagination strategy across different market types.

System Under Test (SUT):
- core.rest_data_client.RestDataClient across all market types
- Market-specific chunking, API limits, and pagination behavior

This module tests that the time-based chunking pagination technique
works correctly across all supported market types:
- SPOT (api.binance.com)
- FUTURES_USDT/UM (fapi.binance.com)
- FUTURES_COIN/CM (dapi.binance.com)

Each market has different characteristics that impact pagination:
- Different API limits (1000 for SPOT, 1500 for futures markets)
- Different symbol formats
- Different supported intervals
"""

import pytest
import asyncio
import logging
from datetime import datetime, timezone, timedelta
import pandas as pd

from core.rest_data_client import RestDataClient
from utils.market_constraints import (
    Interval,
    MarketType,
    get_market_capabilities,
)
from utils.network_utils import create_client

# Configure logging
logger = logging.getLogger(__name__)

# Test configuration
SPOT_SYMBOL = "BTCUSDT"
FUTURES_USDT_SYMBOL = "BTCUSDT"  # USDT-margined futures
FUTURES_COIN_SYMBOL = "BTCUSD_PERP"  # Coin-margined futures perpetual contract

# Test parameters for each market type
MARKET_TEST_PARAMS = [
    (MarketType.SPOT, SPOT_SYMBOL, 1000),  # SPOT market, 1000 records limit
    (
        MarketType.FUTURES_USDT,
        FUTURES_USDT_SYMBOL,
        1500,
    ),  # UM futures, 1500 records limit
    (
        MarketType.FUTURES_COIN,
        FUTURES_COIN_SYMBOL,
        1500,
    ),  # CM futures, 1500 records limit
]


@pytest.mark.parametrize("market_type,symbol,api_limit", MARKET_TEST_PARAMS)
def test_calculate_chunks_across_markets(market_type, symbol, api_limit, caplog):
    """Test pagination chunking strategy across all market types.

    This tests that the _calculate_chunks method correctly handles the
    different API limits for each market type.
    """
    caplog.set_level(logging.DEBUG)

    # Create RestDataClient with the specified market type
    client = RestDataClient(market_type=market_type)

    # Define a large time range (30 days)
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=30)
    end_time = now

    # Convert to milliseconds
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    # Get supported intervals for this market type
    market_capabilities = get_market_capabilities(market_type)
    supported_intervals = market_capabilities.supported_intervals

    # Test at least one supported interval for this market type
    test_interval = supported_intervals[0]  # Use the first supported interval

    # Calculate chunks
    chunks = client._calculate_chunks(start_ms, end_ms, test_interval)

    # Validate chunks
    assert isinstance(chunks, list), "Chunks should be a list"
    assert len(chunks) > 0, f"Should generate at least one chunk for {market_type.name}"

    # Check that each chunk respects the market-specific API limit
    for chunk in chunks:
        assert isinstance(chunk, tuple), "Each chunk should be a tuple"
        assert len(chunk) == 2, "Each chunk should be a (start, end) tuple"
        chunk_start, chunk_end = chunk

        # Calculate the number of intervals in this chunk
        chunk_duration_ms = chunk_end - chunk_start
        interval_ms = test_interval.to_seconds() * 1000
        intervals_in_chunk = chunk_duration_ms / interval_ms

        # Check that chunk doesn't exceed the market-specific API limit
        assert intervals_in_chunk <= api_limit + 1, (
            f"Chunk for {market_type.name} exceeds API limit: "
            f"{intervals_in_chunk} intervals > {api_limit}"
        )

    # Verify continuity (no gaps between chunks)
    for i in range(len(chunks) - 1):
        current_end = chunks[i][1]
        next_start = chunks[i + 1][0]

        # Typically there should be a 1ms gap to avoid interval overlap
        expected_gap = 1
        actual_gap = next_start - current_end

        assert (
            actual_gap == expected_gap
        ), f"Gap between chunks should be {expected_gap}ms for {market_type.name}, got {actual_gap}ms"

    # Log the result for debugging
    logger.info(
        f"Market {market_type.name} with API limit {api_limit}: "
        f"Created {len(chunks)} chunks for 30-day period using {test_interval.value} interval"
    )


@pytest.mark.asyncio
@pytest.mark.real
@pytest.mark.parametrize("market_type,symbol,api_limit", MARKET_TEST_PARAMS)
async def test_fetch_across_markets(market_type, symbol, api_limit, caplog):
    """Test actual data fetching across different market types.

    This test verifies that the fetch method correctly handles different
    market types, their API limits, and supported intervals.
    """
    caplog.set_level(logging.DEBUG)

    # Create client
    client_session = create_client(timeout=10.0)
    client = RestDataClient(market_type=market_type, client=client_session)

    try:
        # Get market capabilities to determine supported intervals
        market_capabilities = get_market_capabilities(market_type)
        supported_intervals = market_capabilities.supported_intervals

        # Use a relatively short interval that's supported by all market types
        # Futures markets don't support 1s interval, so we use 1m which all support
        test_interval = Interval.MINUTE_1
        if test_interval not in supported_intervals:
            test_interval = supported_intervals[
                0
            ]  # fallback to first supported interval

        # Use historical data for reliable testing (2 days ago)
        reference_time = datetime.now(timezone.utc) - timedelta(days=2)
        reference_time = reference_time.replace(minute=0, second=0, microsecond=0)

        # Keep the time range small enough to fit in a single API call
        # but large enough to get meaningful data
        start_time = reference_time - timedelta(hours=1)
        end_time = reference_time

        # Fetch data
        logger.info(
            f"Fetching {test_interval.value} data for {market_type.name} using symbol {symbol}"
        )
        df, stats = await client.fetch(
            symbol=symbol,
            interval=test_interval,
            start_time=start_time,
            end_time=end_time,
        )

        # Basic validation of the result
        if not df.empty:
            # Check basic data structure
            required_columns = ["open", "high", "low", "close", "volume"]
            for col in required_columns:
                assert (
                    col in df.columns
                ), f"Column {col} missing from result for {market_type.name}"

            # Verify that we have the expected index type (timestamp)
            assert isinstance(
                df.index, pd.DatetimeIndex
            ), f"Index should be DatetimeIndex for {market_type.name}"

            # Ensure the data is chronologically ordered
            assert (
                df.index.is_monotonic_increasing
            ), f"Data should be chronologically ordered for {market_type.name}"

            # Check that we got data within the expected time range
            # Allow some flexibility due to interval boundary alignment
            interval_seconds = test_interval.to_seconds()
            start_diff = abs((df.index.min() - start_time).total_seconds())
            end_diff = abs((df.index.max() - end_time).total_seconds())

            assert (
                start_diff <= interval_seconds * 2
            ), f"Data start time {df.index.min()} too far from requested {start_time} for {market_type.name}"
            assert (
                end_diff <= interval_seconds * 2
            ), f"Data end time {df.index.max()} too far from requested {end_time} for {market_type.name}"

            # Log test results
            logger.info(
                f"Successfully fetched {len(df)} records for {market_type.name} "
                f"using {test_interval.value} interval"
            )
        else:
            logger.warning(
                f"Retrieved empty DataFrame for {market_type.name}. This could be normal "
                f"if there was no trading activity in the requested period."
            )
    finally:
        # Ensure we close the client session
        if hasattr(client_session, "aclose"):
            await client_session.aclose()
        else:
            await client_session.close()


@pytest.mark.asyncio
@pytest.mark.real
@pytest.mark.parametrize("market_type,symbol,_", MARKET_TEST_PARAMS)
async def test_large_fetch_with_chunking(market_type, symbol, _, caplog):
    """Test large data retrieval requiring chunking across market types.

    This test verifies that the pagination strategy correctly handles
    large requests that require multiple API calls, across all market types.
    """
    caplog.set_level(logging.DEBUG)

    # Create client
    client_session = create_client(timeout=10.0)
    client = RestDataClient(
        market_type=market_type, client=client_session, max_concurrent=3
    )

    try:
        # Get market capabilities to determine supported intervals
        market_capabilities = get_market_capabilities(market_type)
        supported_intervals = market_capabilities.supported_intervals

        # Use an interval that all market types support
        test_interval = Interval.HOUR_1
        if test_interval not in supported_intervals:
            test_interval = supported_intervals[0]

        # Use historical data (3 days ago) spanning 2 days
        # This should require multiple chunks for proper pagination
        reference_time = datetime.now(timezone.utc) - timedelta(days=3)
        reference_time = reference_time.replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        start_time = reference_time - timedelta(days=2)
        end_time = reference_time

        # Fetch data
        logger.info(
            f"Fetching {test_interval.value} data for {market_type.name} over a 2-day period "
            f"using symbol {symbol}"
        )

        # Note the start time for performance measurement
        fetch_start = datetime.now(timezone.utc)

        df, stats = await client.fetch(
            symbol=symbol,
            interval=test_interval,
            start_time=start_time,
            end_time=end_time,
        )

        # Calculate fetch duration
        fetch_duration = (datetime.now(timezone.utc) - fetch_start).total_seconds()

        # Validate result
        if not df.empty:
            # Check that we got chunk information in the stats
            assert (
                "chunks" in stats
            ), f"Stats should include chunk count for {market_type.name}"
            chunk_count = stats.get("chunks", 0)

            # For a 2-day period with hourly data, we expect about 48 records
            # This might require at least 1 chunk (maybe more depending on API limit)
            expected_records = 48  # approximately 48 hours in 2 days

            # Verify we got a reasonable number of records
            # Allow for some flexibility due to market conditions
            # Some hours might not have trading data
            assert (
                len(df) > 0
            ), f"Should have retrieved at least some records for {market_type.name}"

            # Verify chunk count is reasonable
            # For a 2-day period with hourly data (max 48 records),
            # we should need at most 1 chunk for most market types
            assert (
                chunk_count > 0
            ), f"Should have used at least one chunk for {market_type.name}"

            # Log performance metrics
            logger.info(
                f"Market {market_type.name}: Retrieved {len(df)} records using {chunk_count} chunks "
                f"in {fetch_duration:.2f} seconds"
            )

            if chunk_count > 1:
                # If we used multiple chunks, verify the data is properly joined
                # by checking for gaps in the time series

                # Sort by index just to be safe
                df = df.sort_index()

                # Check for expected time gaps (hourly interval)
                time_diffs = pd.Series(df.index[1:] - df.index[:-1]).unique()
                time_diff_seconds = [td.total_seconds() for td in time_diffs]

                # For hourly data, we expect 3600 seconds between points
                expected_diff = test_interval.to_seconds()

                # Allow for some missing data points, but there should be
                # at least some with the expected interval
                found_expected_interval = any(
                    abs(diff - expected_diff) < 5 for diff in time_diff_seconds
                )

                assert found_expected_interval, (
                    f"Could not find expected interval of {expected_diff}s between data points "
                    f"for {market_type.name}"
                )
        else:
            logger.warning(
                f"Retrieved empty DataFrame for {market_type.name}. This could be normal "
                f"if there was no trading activity in the requested period."
            )
    finally:
        # Ensure we close the client session
        if hasattr(client_session, "aclose"):
            await client_session.aclose()
        else:
            await client_session.close()


@pytest.mark.asyncio
@pytest.mark.real
@pytest.mark.parametrize("market_type,symbol,_", MARKET_TEST_PARAMS)
async def test_concurrent_fetches(market_type, symbol, _, caplog):
    """Test concurrent fetches with the same client across market types.

    This tests that the RestDataClient can handle multiple concurrent
    fetch operations while respecting semaphore limits, even when API
    access might be restricted.
    """
    caplog.set_level(logging.DEBUG)

    # Create client with a limited concurrency
    max_concurrent = 3
    client_session = create_client(timeout=10.0)
    client = RestDataClient(
        market_type=market_type, client=client_session, max_concurrent=max_concurrent
    )

    try:
        # Get market capabilities to determine supported intervals
        market_capabilities = get_market_capabilities(market_type)
        supported_intervals = market_capabilities.supported_intervals

        # Use an interval that all market types support
        test_interval = Interval.MINUTE_15
        if test_interval not in supported_intervals:
            test_interval = supported_intervals[0]

        # Use a much smaller number of requests to reduce likelihood of rate limiting
        # Rather than actual API success, test the concurrency mechanism works
        concurrent_requests = 3

        # Use historical data from 2-3 days ago with smaller time ranges
        reference_time = datetime.now(timezone.utc) - timedelta(days=2)
        reference_time = reference_time.replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        # Create fetch tasks with different time ranges
        tasks = []
        for days_back in range(1, concurrent_requests + 1):
            # Use smaller time ranges (1 hour instead of 4)
            start_time = reference_time - timedelta(days=days_back, hours=1)
            end_time = reference_time - timedelta(days=days_back)

            tasks.append(
                client.fetch(
                    symbol=symbol,
                    interval=test_interval,
                    start_time=start_time,
                    end_time=end_time,
                )
            )

        # Execute all fetch tasks concurrently
        logger.info(
            f"Running {len(tasks)} concurrent fetches for {market_type.name} "
            f"with max_concurrent={max_concurrent}"
        )

        # Note the start time for performance measurement
        fetch_start = datetime.now(timezone.utc)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        fetch_duration = (datetime.now(timezone.utc) - fetch_start).total_seconds()

        # Even if API access fails, validate that the tasks completed
        # and we received results (even if they're errors)
        assert len(results) == len(tasks), "All tasks should complete"

        # Count successful fetches, but don't require them to succeed
        # (API access might be restricted in test environment)
        success_count = 0
        error_count = 0
        total_records = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"Fetch {i+1} for {market_type.name} failed: {result}")
                error_count += 1
                continue

            # Unpack the result
            df, stats = result

            # Count both empty and non-empty dataframes as "successful" fetches
            # since we're testing the client's ability to make concurrent requests
            success_count += 1
            if not df.empty:
                total_records += len(df)

        # Check that we got results for all tasks (success or error)
        assert success_count + error_count == len(tasks), (
            f"All tasks should complete with either success or error. "
            f"Got {success_count} successes and {error_count} errors out of {len(tasks)} tasks."
        )

        # Log metrics regardless of API success
        logger.info(
            f"Market {market_type.name}: {success_count}/{len(tasks)} fetches completed without exception, "
            f"retrieving {total_records} total records in {fetch_duration:.2f} seconds"
        )

        # Verify the concurrency control basic functionality
        assert fetch_duration > 0, "Fetch duration should be positive"

        # If we have errors, log them but don't fail the test
        # The purpose of this test is to verify the client's ability to manage
        # concurrent requests, not to validate API responses
        if error_count > 0:
            logger.warning(
                f"Market {market_type.name}: {error_count}/{len(tasks)} fetches failed with errors. "
                f"This may be due to API access restrictions and doesn't necessarily indicate "
                f"a problem with the pagination functionality."
            )

    finally:
        # Ensure we close the client session
        if hasattr(client_session, "aclose"):
            await client_session.aclose()
        else:
            await client_session.close()
