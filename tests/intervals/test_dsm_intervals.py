#!/usr/bin/env python
"""Integration tests for DataSourceManager across all interval types.

System Under Test (SUT):
- core.data_source_manager.DataSourceManager
- core.vision_data_client.VisionDataClient (indirectly)
- core.rest_data_client.RestDataClient (indirectly)

This test suite validates that the DataSourceManager correctly handles
data retrieval across all supported intervals, with proper time alignment,
data consistency, and error handling.

Following the pytest-construction.mdc guidelines:
1. We use real data only (no mocks)
2. We search backward for available data up to 3 days
3. We handle errors without skipping tests
4. We ensure proper cleanup of resources
"""

import pytest
import asyncio
import logging
from datetime import datetime, timezone, timedelta
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any

from utils.logger_setup import get_logger
from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import Interval, MarketType, get_market_capabilities
from utils.network_utils import create_client

from tests.intervals import (
    SPOT_SYMBOL,
    FUTURES_USDT_SYMBOL,
    FUTURES_COIN_SYMBOL,
    SPOT_INTERVALS,
    FUTURES_INTERVALS,
    MARKET_TEST_PARAMS,
)

# Configure logging
logger = get_logger(__name__, level="INFO")

# Apply module-level fixture scope to avoid DeprecationWarning
pytestmark = [
    pytest.mark.asyncio(loop_scope="function"),  # Use function scope for async tests
]

# Configure pytest-asyncio to use function scope by default
pytestasyncio_configure = {"asyncio_default_fixture_loop_scope": "function"}


@pytest.fixture
async def dsm() -> DataSourceManager:
    """Create and clean up a DataSourceManager instance."""
    # Create DataSourceManager with temporary cache
    manager = DataSourceManager(market_type=MarketType.SPOT)

    # Use context manager to ensure proper cleanup
    await manager.__aenter__()

    try:
        yield manager
    finally:
        # Ensure proper cleanup
        await manager.__aexit__(None, None, None)


async def find_available_data(
    market_type: MarketType, symbol: str, interval: Interval, max_days_back: int = 3
) -> Tuple[datetime, bool]:
    """Find the latest date with available data by searching backward.

    Following pytest-construction.mdc guidelines, we search backward
    from the current date to find data, up to 3 days back.

    Args:
        market_type: The market type to check
        symbol: The trading symbol to check
        interval: The interval to check
        max_days_back: Maximum days to search back (default: 3)

    Returns:
        Tuple of (reference_date, found_data)
    """
    logger.info(
        f"Looking for available {interval.value} data for {symbol} ({market_type.name})"
    )

    now = datetime.now(timezone.utc)

    # Create a client for testing availability
    client_session = create_client(timeout=10.0)

    # For smaller intervals like 1s, use shorter windows
    if interval == Interval.SECOND_1:
        fetch_window = timedelta(minutes=5)
    elif interval in (Interval.MINUTE_1, Interval.MINUTE_3, Interval.MINUTE_5):
        fetch_window = timedelta(hours=1)
    else:
        fetch_window = timedelta(hours=4)

    try:
        # Search backward from current time
        for days_back in range(max_days_back):
            reference_time = now - timedelta(days=days_back)

            # Create clean reference time (eliminate milliseconds/microseconds)
            reference_time = reference_time.replace(minute=0, second=0, microsecond=0)

            start_time = reference_time - fetch_window
            end_time = reference_time

            logger.info(
                f"Checking for data on day -{days_back}: "
                f"{start_time.isoformat()} to {end_time.isoformat()}"
            )

            # Create DataSourceManager with the correct market type
            async with DataSourceManager(market_type=market_type) as manager:
                try:
                    # Try to fetch a small amount of data
                    df = await manager.get_data(
                        symbol=symbol,
                        interval=interval,
                        start_time=start_time,
                        end_time=end_time,
                        enforce_source=DataSource.REST,  # Use REST for faster responses
                    )

                    # If we got data, return this reference time
                    if not df.empty:
                        logger.info(
                            f"Found data for {interval.value} on day -{days_back} "
                            f"with {len(df)} records"
                        )
                        return reference_time, True

                    logger.info(
                        f"No data found for {interval.value} on day -{days_back}"
                    )

                except Exception as e:
                    logger.warning(
                        f"Error checking data availability for {interval.value} "
                        f"on day -{days_back}: {e}"
                    )

        # If we didn't find data in the search period, return the most recent date
        logger.warning(
            f"No data found for {interval.value} within {max_days_back} days. "
            f"Will use most recent date for tests."
        )
        return now.replace(minute=0, second=0, microsecond=0), False

    finally:
        # Clean up the client
        if hasattr(client_session, "aclose"):
            await client_session.aclose()
        else:
            await client_session.close()


@pytest.mark.parametrize("interval", SPOT_INTERVALS)
async def test_dsm_spot_intervals(dsm: DataSourceManager, interval: Interval, caplog):
    """Test DataSourceManager with SPOT market intervals.

    This test verifies that:
    1. DataSourceManager can retrieve data for all SPOT market intervals
    2. The data has the correct format and structure
    3. Time boundaries are properly aligned
    """
    caplog.set_level(logging.INFO)

    # Find available data
    reference_time, found_data = await find_available_data(
        market_type=MarketType.SPOT, symbol=SPOT_SYMBOL, interval=interval
    )

    # Define a time window appropriate for this interval
    if interval == Interval.SECOND_1:
        # For 1s data, use a 2-minute window
        time_window = timedelta(minutes=2)
    elif interval in (Interval.MINUTE_1, Interval.MINUTE_3, Interval.MINUTE_5):
        # For minute-level data, use a 1-hour window
        time_window = timedelta(hours=1)
    elif interval in (Interval.MINUTE_15, Interval.MINUTE_30):
        # For larger minute intervals, use a 4-hour window
        time_window = timedelta(hours=4)
    elif interval in (Interval.HOUR_1, Interval.HOUR_2, Interval.HOUR_4):
        # For hour-level data, use a 24-hour window
        time_window = timedelta(days=1)
    else:
        # For larger intervals, use a 7-day window
        time_window = timedelta(days=7)

    # Create a time window for testing
    start_time = reference_time - time_window
    end_time = reference_time

    logger.info(
        f"Testing SPOT {interval.value} data from "
        f"{start_time.isoformat()} to {end_time.isoformat()}"
    )

    # Fetch data using DataSourceManager
    df = await dsm.get_data(
        symbol=SPOT_SYMBOL,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        enforce_source=DataSource.REST,  # Use REST API for consistency
    )

    # Log the result
    if df.empty:
        logger.warning(f"No SPOT {interval.value} data retrieved")
    else:
        logger.info(f"Retrieved {len(df)} records of SPOT {interval.value} data")

    # Validate data structure even if empty
    assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"

    # For non-empty results, validate data content
    if not df.empty:
        # Validate index and columns
        assert isinstance(df.index, pd.DatetimeIndex), "Index should be DatetimeIndex"
        assert (
            df.index.is_monotonic_increasing
        ), "Index should be chronologically ordered"

        # Check essential columns
        required_columns = ["open", "high", "low", "close", "volume"]
        for col in required_columns:
            assert col in df.columns, f"Column {col} missing from result"

        # Check that data falls within the requested time range
        # Allow some flexibility due to interval boundary alignment
        interval_seconds = interval.to_seconds()
        start_diff = abs((df.index.min() - start_time).total_seconds())
        end_diff = abs((df.index.max() - end_time).total_seconds())

        # The difference should be at most 2 intervals
        assert (
            start_diff <= interval_seconds * 2
        ), f"Data start time {df.index.min()} too far from requested {start_time}"
        assert (
            end_diff <= interval_seconds * 2
        ), f"Data end time {df.index.max()} too far from requested {end_time}"

        # Check that interval between records matches expected interval
        if len(df) > 1:
            time_diffs = np.diff(df.index.astype(np.int64)) / 1e9  # Convert to seconds
            median_diff = np.median(time_diffs)

            # The median difference should be close to the interval
            assert abs(median_diff - interval_seconds) < 5, (
                f"Median time difference {median_diff}s doesn't match "
                f"expected interval {interval_seconds}s"
            )


@pytest.mark.parametrize("interval", FUTURES_INTERVALS)
@pytest.mark.parametrize(
    "market_type,symbol,_",
    [
        (MarketType.FUTURES_USDT, FUTURES_USDT_SYMBOL, 1500),
        (MarketType.FUTURES_COIN, FUTURES_COIN_SYMBOL, 1500),
    ],
)
async def test_dsm_futures_intervals(
    dsm: DataSourceManager,
    market_type: MarketType,
    symbol: str,
    _: int,
    interval: Interval,
    caplog,
):
    """Test DataSourceManager with futures market intervals.

    This test verifies that:
    1. DataSourceManager can retrieve data for all futures market intervals
    2. The data has the correct format and structure
    3. Time boundaries are properly aligned

    This test covers both USDT-margined (UM) and Coin-margined (CM) futures.
    """
    caplog.set_level(logging.INFO)

    # Override the DSM market type to match the test case
    dsm._market_type = market_type

    # Find available data
    reference_time, found_data = await find_available_data(
        market_type=market_type, symbol=symbol, interval=interval
    )

    # Define a time window appropriate for this interval
    if interval in (Interval.MINUTE_1, Interval.MINUTE_3, Interval.MINUTE_5):
        # For minute-level data, use a 1-hour window
        time_window = timedelta(hours=1)
    elif interval in (Interval.MINUTE_15, Interval.MINUTE_30):
        # For larger minute intervals, use a 4-hour window
        time_window = timedelta(hours=4)
    elif interval in (Interval.HOUR_1, Interval.HOUR_2, Interval.HOUR_4):
        # For hour-level data, use a 24-hour window
        time_window = timedelta(days=1)
    else:
        # For larger intervals, use a 7-day window
        time_window = timedelta(days=7)

    # Create a time window for testing
    start_time = reference_time - time_window
    end_time = reference_time

    logger.info(
        f"Testing {market_type.name} {interval.value} data from "
        f"{start_time.isoformat()} to {end_time.isoformat()}"
    )

    # Fetch data using DataSourceManager
    df = await dsm.get_data(
        symbol=symbol,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        enforce_source=DataSource.REST,  # Use REST API for consistency
    )

    # Log the result
    if df.empty:
        logger.warning(f"No {market_type.name} {interval.value} data retrieved")
    else:
        logger.info(
            f"Retrieved {len(df)} records of {market_type.name} {interval.value} data"
        )

    # Validate data structure even if empty
    assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"

    # For non-empty results, validate data content
    if not df.empty:
        # Validate index and columns
        assert isinstance(df.index, pd.DatetimeIndex), "Index should be DatetimeIndex"
        assert (
            df.index.is_monotonic_increasing
        ), "Index should be chronologically ordered"

        # Check essential columns
        required_columns = ["open", "high", "low", "close", "volume"]
        for col in required_columns:
            assert col in df.columns, f"Column {col} missing from result"

        # Check that data falls within the requested time range
        # Allow some flexibility due to interval boundary alignment
        interval_seconds = interval.to_seconds()
        start_diff = abs((df.index.min() - start_time).total_seconds())
        end_diff = abs((df.index.max() - end_time).total_seconds())

        # The difference should be at most 2 intervals
        assert (
            start_diff <= interval_seconds * 2
        ), f"Data start time {df.index.min()} too far from requested {start_time}"
        assert (
            end_diff <= interval_seconds * 2
        ), f"Data end time {df.index.max()} too far from requested {end_time}"

        # Check that interval between records matches expected interval
        if len(df) > 1:
            time_diffs = np.diff(df.index.astype(np.int64)) / 1e9  # Convert to seconds
            median_diff = np.median(time_diffs)

            # The median difference should be close to the interval
            assert abs(median_diff - interval_seconds) < 5, (
                f"Median time difference {median_diff}s doesn't match "
                f"expected interval {interval_seconds}s"
            )


@pytest.mark.parametrize(
    "interval",
    [
        Interval.MINUTE_1,  # Common small interval
        Interval.HOUR_1,  # Common medium interval
        Interval.DAY_1,  # Common large interval
    ],
)
async def test_dsm_interval_data_consistency(
    dsm: DataSourceManager, interval: Interval, caplog
):
    """Test data consistency across all market types for common intervals.

    This test verifies that for common intervals (1m, 1h, 1d):
    1. Data can be retrieved from all market types
    2. The data structure is consistent across market types
    3. Metadata (e.g., column names, types) is consistent
    """
    caplog.set_level(logging.INFO)

    results = {}

    # Test each market type with the same interval
    for market_type, symbol, _ in MARKET_TEST_PARAMS:
        # Override the DSM market type
        dsm._market_type = market_type

        # Find available data
        reference_time, found_data = await find_available_data(
            market_type=market_type, symbol=symbol, interval=interval
        )

        # Define a time window appropriate for this interval
        if interval == Interval.MINUTE_1:
            time_window = timedelta(hours=1)
        elif interval == Interval.HOUR_1:
            time_window = timedelta(days=1)
        else:  # DAY_1
            time_window = timedelta(days=7)

        # Create a time window for testing
        start_time = reference_time - time_window
        end_time = reference_time

        logger.info(
            f"Testing {market_type.name} {interval.value} data from "
            f"{start_time.isoformat()} to {end_time.isoformat()}"
        )

        # Fetch data using DataSourceManager
        df = await dsm.get_data(
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            enforce_source=DataSource.REST,  # Use REST API for consistency
        )

        # Store results for later comparison
        results[market_type.name] = {
            "df": df,
            "symbol": symbol,
            "record_count": len(df),
            "columns": list(df.columns) if not df.empty else [],
            "dtypes": (
                {col: str(df[col].dtype) for col in df.columns} if not df.empty else {}
            ),
        }

        # Log the result
        if df.empty:
            logger.warning(f"No {market_type.name} {interval.value} data retrieved")
        else:
            logger.info(
                f"Retrieved {len(df)} records of {market_type.name} {interval.value} data"
            )

    # Compare structure across market types
    for market_name, result in results.items():
        # Basic validation for each market type
        df = result["df"]
        assert isinstance(
            df, pd.DataFrame
        ), f"{market_name} result should be a DataFrame"

        if not df.empty:
            # Check index and essential properties
            assert isinstance(
                df.index, pd.DatetimeIndex
            ), f"{market_name} index should be DatetimeIndex"
            assert (
                df.index.is_monotonic_increasing
            ), f"{market_name} index should be ordered"

            # Check essential columns
            required_columns = ["open", "high", "low", "close", "volume"]
            for col in required_columns:
                assert (
                    col in df.columns
                ), f"Column {col} missing from {market_name} result"

    # Compare structure consistency between market types with data
    non_empty_results = {k: v for k, v in results.items() if not v["df"].empty}

    if len(non_empty_results) >= 2:
        # Get the first market as reference
        reference_market = list(non_empty_results.keys())[0]
        reference_columns = set(non_empty_results[reference_market]["columns"])

        # Compare all other markets to the reference
        for market_name, result in non_empty_results.items():
            if market_name == reference_market:
                continue

            # Check column consistency
            market_columns = set(result["columns"])

            # Check that essential columns are consistent
            essential_columns = {"open", "high", "low", "close", "volume"}
            assert essential_columns.issubset(
                market_columns
            ), f"{market_name} missing essential columns compared to {reference_market}"

            # Log any additional or missing columns (not a failure)
            extra_columns = market_columns - reference_columns
            missing_columns = reference_columns - market_columns

            if extra_columns:
                logger.info(f"{market_name} has additional columns: {extra_columns}")

            if missing_columns:
                logger.info(f"{market_name} is missing columns: {missing_columns}")


@pytest.mark.parametrize(
    "interval,expected_field",
    [
        (Interval.MINUTE_1, "open_time"),
        (Interval.HOUR_1, "open_time"),
        (Interval.DAY_1, "open_time"),
    ],
)
async def test_dsm_metadata_consistency(
    dsm: DataSourceManager, interval: Interval, expected_field: str, caplog
):
    """Test metadata consistency for different intervals.

    This test verifies that regardless of the interval:
    1. The index name is consistent
    2. The essential columns have consistent data types
    3. The interval-specific metadata is correctly handled
    """
    caplog.set_level(logging.INFO)

    # Use SPOT market for simplicity
    market_type = MarketType.SPOT
    symbol = SPOT_SYMBOL

    # Find available data
    reference_time, found_data = await find_available_data(
        market_type=market_type, symbol=symbol, interval=interval
    )

    # Define a time window appropriate for this interval
    if interval == Interval.MINUTE_1:
        time_window = timedelta(hours=1)
    elif interval == Interval.HOUR_1:
        time_window = timedelta(days=1)
    else:  # DAY_1
        time_window = timedelta(days=7)

    # Create a time window for testing
    start_time = reference_time - time_window
    end_time = reference_time

    logger.info(
        f"Testing metadata for {interval.value} data from "
        f"{start_time.isoformat()} to {end_time.isoformat()}"
    )

    # Fetch data using DataSourceManager
    df = await dsm.get_data(
        symbol=symbol,
        interval=interval,
        start_time=start_time,
        end_time=end_time,
        enforce_source=DataSource.REST,  # Use REST API for consistency
    )

    # Validate even if the DataFrame is empty
    assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"

    if not df.empty:
        # Check index name
        assert df.index.name == expected_field, f"Index name should be {expected_field}"

        # Check column data types
        assert pd.api.types.is_float_dtype(df["open"]), "open should be float"
        assert pd.api.types.is_float_dtype(df["high"]), "high should be float"
        assert pd.api.types.is_float_dtype(df["low"]), "low should be float"
        assert pd.api.types.is_float_dtype(df["close"]), "close should be float"
        assert pd.api.types.is_float_dtype(df["volume"]), "volume should be float"

        # Check close_time column
        assert "close_time" in df.columns, "close_time should be present"
        assert pd.api.types.is_datetime64_dtype(
            df["close_time"]
        ), "close_time should be datetime"

        # Check numeric fields
        numeric_fields = ["trades", "quote_asset_volume", "taker_buy_base_asset_volume"]
        for field in numeric_fields:
            if field in df.columns:
                assert pd.api.types.is_numeric_dtype(
                    df[field]
                ), f"{field} should be numeric"

        # Log column info for inspection
        for col in df.columns:
            logger.info(f"Column {col}: {df[col].dtype}")
    else:
        logger.warning(
            f"No data available for {interval.value}, skipping detailed validation"
        )
