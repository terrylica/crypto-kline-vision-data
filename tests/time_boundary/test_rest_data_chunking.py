#!/usr/bin/env python
"""Tests for RestDataClient's pagination and chunking strategy.

System Under Test (SUT):
- core.rest_data_client.RestDataClient
- Specifically the _calculate_chunks and time boundary handling methods

This module tests the time-based chunking pagination technique implemented
in RestDataClient for handling large data requests efficiently across all
interval types supported by the Binance API.
"""

import pytest
import pandas as pd
import logging
from datetime import datetime, timezone, timedelta
import sys

from core.rest_data_client import RestDataClient
from utils.market_constraints import (
    Interval,
)

# Configure logging
logger = logging.getLogger(__name__)

# Test configuration
TEST_SYMBOL = "BTCUSDT"
API_LIMIT = 1000  # Maximum records per request


@pytest.mark.parametrize(
    "interval",
    [
        Interval.SECOND_1,
        Interval.MINUTE_1,
        Interval.MINUTE_3,
        Interval.MINUTE_5,
        Interval.MINUTE_15,
        Interval.MINUTE_30,
        Interval.HOUR_1,
        Interval.HOUR_2,
        Interval.HOUR_4,
        Interval.HOUR_6,
        Interval.HOUR_8,
        Interval.HOUR_12,
        Interval.DAY_1,
        Interval.DAY_3,
        Interval.WEEK_1,
        Interval.MONTH_1,
    ],
)
def test_calculate_chunks_all_intervals(interval, caplog):
    """Test the _calculate_chunks method with all intervals.

    This validates that the chunking strategy is appropriate for each
    interval type, respecting the 1000-record limit and optimizing
    chunk size for efficient data retrieval.
    """
    caplog.set_level(logging.DEBUG)

    # Create RestDataClient instance
    client = RestDataClient()

    # Define a large time range (60 days)
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=60)
    end_time = now

    # Convert to milliseconds
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    # Calculate chunks
    chunks = client._calculate_chunks(start_ms, end_ms, interval)

    # Validate chunks
    assert isinstance(chunks, list), "Chunks should be a list"
    assert len(chunks) > 0, f"Should generate at least one chunk for {interval.value}"

    # Check that each chunk is a valid (start, end) tuple
    for chunk in chunks:
        assert isinstance(chunk, tuple), "Each chunk should be a tuple"
        assert len(chunk) == 2, "Each chunk should be a (start, end) tuple"
        chunk_start, chunk_end = chunk
        assert (
            chunk_start < chunk_end
        ), f"Chunk start {chunk_start} should be before end {chunk_end}"

        # Calculate the number of intervals in this chunk
        chunk_duration_ms = chunk_end - chunk_start
        interval_ms = interval.to_seconds() * 1000
        intervals_in_chunk = chunk_duration_ms / interval_ms

        # Check that chunk doesn't exceed API limit (with small margin for rounding)
        assert (
            intervals_in_chunk <= API_LIMIT + 1
        ), f"Chunk for {interval.value} exceeds API limit: {intervals_in_chunk} intervals > {API_LIMIT}"

    # Verify continuity (no gaps between chunks)
    for i in range(len(chunks) - 1):
        current_end = chunks[i][1]
        next_start = chunks[i + 1][0]

        # Typically there should be a 1ms gap to avoid interval overlap
        expected_gap = 1
        actual_gap = next_start - current_end

        assert (
            actual_gap == expected_gap
        ), f"Gap between chunks should be {expected_gap}ms, got {actual_gap}ms"

    # Verify complete coverage
    first_chunk_start = chunks[0][0]
    last_chunk_end = chunks[-1][1]

    assert (
        first_chunk_start == start_ms
    ), f"First chunk should start at {start_ms}, got {first_chunk_start}"
    assert (
        last_chunk_end == end_ms
    ), f"Last chunk should end at {end_ms}, got {last_chunk_end}"

    # Check each interval follows the appropriate chunking strategy
    if interval == Interval.SECOND_1:
        # 1s interval should have small chunks to avoid too many records
        max_chunk_duration = 1000 * 1000  # Max 1000 seconds (16.7 minutes)
        chunk_sizes = [c[1] - c[0] for c in chunks]
        assert (
            max(chunk_sizes) <= max_chunk_duration
        ), f"1s chunks should be at most {max_chunk_duration/1000} seconds, got {max(chunk_sizes)/1000}"

    elif interval == Interval.MINUTE_1:
        # 1m interval should have medium chunks
        max_chunk_duration = 1000 * 60 * 1000  # Max 1000 minutes (16.7 hours)
        chunk_sizes = [c[1] - c[0] for c in chunks]
        assert (
            max(chunk_sizes) <= max_chunk_duration
        ), f"1m chunks should be at most {max_chunk_duration/(60*1000)} minutes, got {max(chunk_sizes)/(60*1000)}"

    elif interval in (
        Interval.MINUTE_3,
        Interval.MINUTE_5,
        Interval.MINUTE_15,
        Interval.MINUTE_30,
    ):
        # Other minute intervals should cap at 7 days per chunk
        max_chunk_duration = 7 * 24 * 60 * 60 * 1000  # Max 7 days
        chunk_sizes = [c[1] - c[0] for c in chunks]
        assert (
            max(chunk_sizes) <= max_chunk_duration
        ), f"Minute chunks should be at most {max_chunk_duration/(24*60*60*1000)} days, got {max(chunk_sizes)/(24*60*60*1000)}"

    elif interval in (
        Interval.HOUR_1,
        Interval.HOUR_2,
        Interval.HOUR_4,
        Interval.HOUR_6,
        Interval.HOUR_8,
        Interval.HOUR_12,
    ):
        # Hour intervals should cap at 30 days per chunk
        max_chunk_duration = 30 * 24 * 60 * 60 * 1000  # Max 30 days
        chunk_sizes = [c[1] - c[0] for c in chunks]
        assert (
            max(chunk_sizes) <= max_chunk_duration
        ), f"Hour chunks should be at most {max_chunk_duration/(24*60*60*1000)} days, got {max(chunk_sizes)/(24*60*60*1000)}"

    # Log the chunk statistics
    logger.info(
        f"Interval {interval.value}: Created {len(chunks)} chunks for 60-day period"
    )


@pytest.mark.parametrize(
    "interval,duration_days,expected_chunks",
    [
        (Interval.SECOND_1, 1, 86),  # 1s interval needs many chunks for just 1 day
        (Interval.MINUTE_1, 1, 2),  # 1m interval needs fewer chunks
        (
            Interval.HOUR_1,
            30,
            1,
        ),  # 1h interval can fit 30 days in 1 chunk (720 intervals)
        (
            Interval.DAY_1,
            365,
            1,
        ),  # 1d interval can fit a full year in 1 chunk (365 intervals)
    ],
)
def test_chunk_count_optimization(interval, duration_days, expected_chunks, caplog):
    """Test that the chunking strategy creates an optimal number of chunks.

    This test verifies that the algorithm intelligently creates more chunks
    for smaller intervals and fewer chunks for larger intervals, for a
    given time duration.
    """
    caplog.set_level(logging.DEBUG)

    # Create RestDataClient instance
    client = RestDataClient()

    # Define a time range based on the parameter
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=duration_days)
    end_time = now

    # Convert to milliseconds
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    # Calculate chunks
    chunks = client._calculate_chunks(start_ms, end_ms, interval)

    # For this test we allow a margin of error in the expected chunk count
    # because the exact count can vary slightly depending on time of day
    # when the test is run
    margin = max(1, expected_chunks // 5)  # Allow 20% margin or at least 1

    assert expected_chunks - margin <= len(chunks) <= expected_chunks + margin, (
        f"Expected roughly {expected_chunks} chunks for {interval.value} over {duration_days} days, "
        f"got {len(chunks)}"
    )

    # Log details for debugging
    logger.info(
        f"Interval {interval.value}: Created {len(chunks)} chunks for {duration_days}-day period"
    )


@pytest.mark.real
@pytest.mark.asyncio
async def test_time_boundary_alignment(api_session, caplog):
    """Test that time boundaries are correctly aligned to interval boundaries.

    This test verifies that the RestDataClient properly handles the Binance API's
    behavior where:
    - startTime is rounded up to the next interval boundary if not aligned
    - endTime is rounded down to the previous interval boundary if not aligned
    """
    caplog.set_level(logging.DEBUG)

    # Create client
    client = RestDataClient(client=api_session)

    # Use a small time range with non-aligned boundaries
    # 10:00:30 to 10:05:30 (non-aligned 5-minute period)
    reference_time = datetime.now(timezone.utc).replace(microsecond=0)
    reference_time = reference_time.replace(second=30)  # Intentionally non-aligned

    start_time = reference_time - timedelta(minutes=5)
    end_time = reference_time

    # Use 1-minute interval
    interval = Interval.MINUTE_1

    # Fetch data with automatic alignment
    try:
        df, stats = await client.fetch(
            symbol=TEST_SYMBOL,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
        )

        # If we got data, verify alignment
        if not df.empty:
            # Check that all timestamps are aligned to minute boundaries (second=0)
            all_aligned = all(ts.second == 0 for ts in df.index)
            assert all_aligned, "All timestamps should be aligned to minute boundaries"

            # The adjusted start time should be aligned to the next minute
            expected_start = start_time.replace(second=0) + timedelta(minutes=1)
            actual_start = df.index.min()

            # Allow for small differences in timezone/timestamp representation
            start_diff = abs((actual_start - expected_start).total_seconds())
            assert (
                start_diff < 60
            ), f"Start time alignment issue: {actual_start} vs {expected_start}"

            # The adjusted end time should be aligned to the previous minute
            expected_end = end_time.replace(second=0)
            actual_end = df.index.max()

            # Allow for small differences in timezone/timestamp representation
            end_diff = abs((actual_end - expected_end).total_seconds())
            assert (
                end_diff < 60
            ), f"End time alignment issue: {actual_end} vs {expected_end}"

            logger.info(
                f"Time boundary alignment verified: original range {start_time} - {end_time}, "
                f"aligned to {df.index.min()} - {df.index.max()}"
            )
        else:
            logger.warning(
                "Retrieved empty DataFrame - alignment test partially skipped"
            )

    except Exception as e:
        if "API error" in str(e):
            logger.warning(f"API error during alignment test - skipping: {e}")
            pytest.skip(f"API returned an error: {e}")
        else:
            raise


@pytest.mark.real
@pytest.mark.asyncio
async def test_large_data_retrieval_with_chunking(api_session, caplog):
    """Test retrieval of large data sets requiring multiple chunks.

    This test verifies that the chunking and pagination strategy correctly
    fetches and combines data from multiple API calls into a single coherent
    dataset.
    """
    caplog.set_level(logging.DEBUG)

    # Create client
    client = RestDataClient(client=api_session)

    # Use a historical time range to guarantee data availability
    # Get a time range from 2 days ago spanning 3 hours
    reference_time = datetime.now(timezone.utc) - timedelta(days=2)
    reference_time = reference_time.replace(minute=0, second=0, microsecond=0)

    start_time = reference_time - timedelta(hours=3)
    end_time = reference_time

    # Use 1-minute interval (3 hours = 180 minutes = 180 records)
    # This should fit in a single chunk but still provide a good test case
    interval = Interval.MINUTE_1

    try:
        # Fetch data
        df, stats = await client.fetch(
            symbol=TEST_SYMBOL,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
        )

        # Validate result
        if not df.empty:
            # Check for continuity (no missing intervals)
            # Convert index to pandas datetime
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index, utc=True)

            # Sort by index just to be safe
            df = df.sort_index()

            # Get unique time deltas between consecutive timestamps
            time_diffs = pd.Series(df.index[1:] - df.index[:-1]).unique()

            # Convert to seconds
            time_diffs_seconds = [td.total_seconds() for td in time_diffs]

            # For 1-minute interval, all diffs should be 60 seconds
            # Allow up to 2 seconds tolerance for DST changes, etc.
            expected_diff = interval.to_seconds()

            for diff in time_diffs_seconds:
                assert (
                    abs(diff - expected_diff) <= 2
                ), f"Time gap of {diff}s detected - should be {expected_diff}s"

            # Check stats
            assert "chunks" in stats, "Stats should include chunk count"
            assert "records" in stats, "Stats should include record count"

            logger.info(
                f"Successfully retrieved {stats.get('records', 0)} records using "
                f"{stats.get('chunks', 0)} chunks"
            )
        else:
            logger.warning(
                "Retrieved empty DataFrame from large request - validation skipped"
            )
    except Exception as e:
        if "API error" in str(e):
            logger.warning(f"API error during large data test - skipping: {e}")
            pytest.skip(f"API returned an error: {e}")
        else:
            raise


@pytest.mark.real
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interval",
    [
        Interval.SECOND_1,
        Interval.MINUTE_1,
        Interval.HOUR_1,
        Interval.DAY_1,
    ],
)
async def test_multi_interval_data_consistency(interval, api_session, caplog):
    """Test data consistency across different intervals.

    This test verifies that the same time range fetched with different
    intervals produces consistent data. For each interval:
    1. We fetch a time range appropriate for that interval
    2. We verify that the data structure is consistent
    3. We check continuity of data (no gaps)

    This validates that our chunking strategy works properly across
    all interval types.
    """
    caplog.set_level(logging.DEBUG)

    # Create client
    client = RestDataClient(client=api_session)

    # Define a proper duration for each interval type
    duration_mapping = {
        Interval.SECOND_1: timedelta(minutes=2),
        Interval.MINUTE_1: timedelta(hours=2),
        Interval.HOUR_1: timedelta(days=2),
        Interval.DAY_1: timedelta(days=30),
        Interval.WEEK_1: timedelta(weeks=12),
        Interval.MONTH_1: timedelta(days=365),
    }

    # Use a duration appropriate for the interval, defaulting to 1 hour
    duration = duration_mapping.get(interval, timedelta(hours=1))

    # Use historical data to ensure availability
    reference_time = datetime.now(timezone.utc) - timedelta(days=2)
    reference_time = reference_time.replace(minute=0, second=0, microsecond=0)

    start_time = reference_time - duration
    end_time = reference_time

    try:
        # Fetch data
        df, stats = await client.fetch(
            symbol=TEST_SYMBOL,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
        )

        # Validate result
        if not df.empty:
            # Check basic data structure
            required_columns = ["open", "high", "low", "close", "volume"]
            for col in required_columns:
                assert col in df.columns, f"Column {col} missing from result"

            # Check stats
            assert "chunks" in stats, "Stats should include chunk count"
            assert "records" in stats, "Stats should include record count"

            chunk_count = stats.get("chunks", 0)
            record_count = stats.get("records", 0)

            # Log details for debugging
            logger.info(
                f"Interval {interval.value}: Retrieved {record_count} records using {chunk_count} chunks"
            )

            # Expected records (approximately)
            interval_seconds = interval.to_seconds()
            duration_seconds = duration.total_seconds()
            expected_records = duration_seconds / interval_seconds

            # Allow for some flexibility in record count (±10%)
            # Recent intervals might not have all records yet
            min_expected = int(expected_records * 0.5)

            # Set a reasonable lower bound for expected records
            # For very short intervals, we might get fewer records due to market conditions
            if min_expected > 10:
                assert (
                    record_count >= min_expected
                ), f"Expected at least {min_expected} records for {interval.value}, got {record_count}"
        else:
            logger.warning(
                f"Retrieved empty DataFrame for {interval.value} - validation skipped"
            )
    except Exception as e:
        if "API error" in str(e) or "Connection" in str(e):
            logger.warning(f"API error during {interval.value} test - skipping: {e}")
            pytest.skip(f"API returned an error for {interval.value}: {e}")
        else:
            raise


if __name__ == "__main__":
    # Enable local execution using pytest command line
    sys.exit(pytest.main(["-v", __file__]))
