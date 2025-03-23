#!/usr/bin/env python
"""Test data source manager behavior specifically across 2024-2025 boundary.

System Under Test (SUT):
- core.data_source_manager.DataSourceManager
- core.vision_data_client.VisionDataClient (indirectly)
- core.market_data_client.EnhancedRetriever (indirectly)

Focus areas:
1. Data consistency across year boundary
2. Handling of timestamp precision changes between years
3. Data source fallback behavior for historical data near boundaries

This test suite verifies that the DataSourceManager correctly handles the transition
between 2024 and 2025, where timestamp precision changes may occur. It ensures data
integrity, proper source selection, and consistent formatting across the boundary.

Note:
- General date validation tests are in test_data_source_manager_consolidated.py
- Recent data fallback tests are in test_data_source_manager_consolidated.py
- Format/precision tests are in test_data_source_manager_format_precision.py
"""

import pytest
import pandas as pd
import arrow
from datetime import timezone, timedelta
import pytest_asyncio
from datetime import datetime
from typing import AsyncGenerator

from utils.logger_setup import get_logger
from core.data_source_manager import (
    DataSourceManager,
    DataSource,
)
from utils.market_constraints import (
    Interval,
    MarketType,
)

logger = get_logger(__name__, "INFO", show_path=False)

# Test Constants
SYMBOL = "BTCUSDT"
REFERENCE_DATE = arrow.get("2025-01-28").datetime.replace(tzinfo=timezone.utc)
YEAR_BOUNDARY = arrow.get("2025-01-01T00:00:00Z").datetime
BEFORE_BOUNDARY = YEAR_BOUNDARY - timedelta(days=5)
AFTER_BOUNDARY = YEAR_BOUNDARY + timedelta(days=5)


@pytest_asyncio.fixture
async def dsm() -> AsyncGenerator[DataSourceManager, None]:
    """Create a DataSourceManager instance."""
    async with DataSourceManager(market_type=MarketType.SPOT) as manager:
        yield manager


@pytest.mark.asyncio
async def test_year_boundary_data_consistency_1s(dsm: DataSourceManager) -> None:
    """Test data consistency when fetching 1-second data across 2024-2025 boundary."""
    logger.info("=" * 80)
    logger.info("TEST: Data Consistency Across 2024-2025 Year Boundary")
    logger.info(
        "Purpose: Verify data integrity and consistency when fetching 1-second data across the 2024-2025 transition"
    )
    logger.info(f"Time Range: From {BEFORE_BOUNDARY} to {AFTER_BOUNDARY}")
    logger.info("=" * 80)

    # Fetch data across the year boundary
    logger.info("Step 1: Fetching data across year boundary...")
    df = await dsm.get_data(
        symbol=SYMBOL,
        interval=Interval.SECOND_1,
        start_time=BEFORE_BOUNDARY,
        end_time=AFTER_BOUNDARY,
        enforce_source=DataSource.AUTO,
    )

    # Basic data presence checks
    logger.info("Step 2: Performing basic data validation...")
    assert not df.empty, "No data returned for 1s interval"
    logger.info(f"✓ Retrieved {len(df)} data points")

    # Check index properties
    logger.info("Step 3: Validating index properties...")
    assert isinstance(df.index, pd.DatetimeIndex), "Index should be DatetimeIndex"
    assert df.index.tz == timezone.utc, "Index timezone should be UTC"
    assert df.index.is_monotonic_increasing, "Index should be monotonically increasing"
    assert df.index.is_unique, "Index should have no duplicates"
    logger.info("✓ Index properties validated successfully")

    # Check column presence and types
    logger.info("Step 4: Checking column datatypes...")
    expected_dtypes = DataSourceManager.get_output_format()
    for col, dtype in expected_dtypes.items():
        assert col in df.columns, f"Missing column: {col}"
        assert (
            df[col].dtype == dtype
        ), f"Incorrect dtype for {col}: {df[col].dtype} != {dtype}"
    logger.info("✓ All columns present with correct datatypes")

    # Check data continuity around boundary
    logger.info("Step 5: Analyzing data continuity around year boundary...")
    # Using between method for selecting date ranges which is type-safe
    start_ts = YEAR_BOUNDARY - timedelta(hours=1)
    end_ts = YEAR_BOUNDARY + timedelta(hours=1)
    boundary_data = df[df.index.to_series().between(start_ts, end_ts)]
    assert not boundary_data.empty, "No data around year boundary"
    logger.info(f"✓ Found {len(boundary_data)} data points around year boundary")

    # Check for suspicious gaps
    logger.info("Step 6: Checking for suspicious gaps in data...")
    max_expected_gap = pd.Timedelta(seconds=5)
    time_diffs = pd.Series(
        [
            boundary_data.index[i + 1] - boundary_data.index[i]
            for i in range(len(boundary_data) - 1)
        ],
        index=boundary_data.index[:-1],
    )
    suspicious_gaps = time_diffs[time_diffs > max_expected_gap]

    if not suspicious_gaps.empty:
        logger.warning(
            f"Found {len(suspicious_gaps)} suspicious gaps around year boundary:"
        )
        for gap_start, gap_duration in zip(boundary_data.index[:-1], suspicious_gaps):
            gap_end = gap_start + gap_duration
            logger.warning(f"Gap from {gap_start} to {gap_end} ({gap_duration})")
    else:
        logger.info("✓ No suspicious gaps found in the data")


@pytest.mark.asyncio
async def test_timestamp_precision_change(dsm: DataSourceManager) -> None:
    """Test handling of timestamp precision change in 2025."""
    logger.info("=" * 80)
    logger.info("TEST: Timestamp Precision Change Handling")
    logger.info(
        "Purpose: Verify correct handling of timestamp precision changes between 2024 and 2025"
    )
    logger.info("=" * 80)

    # Get data from before 2025
    logger.info("Step 1: Fetching 2024 data sample...")
    df_2024 = await dsm.get_data(
        symbol=SYMBOL,
        interval=Interval.SECOND_1,
        start_time=YEAR_BOUNDARY - timedelta(days=2),
        end_time=YEAR_BOUNDARY - timedelta(days=1),
        enforce_source=DataSource.AUTO,
    )
    logger.info(f"✓ Retrieved {len(df_2024)} data points from 2024")

    # Get data from after 2025
    logger.info("Step 2: Fetching 2025 data sample...")
    df_2025 = await dsm.get_data(
        symbol=SYMBOL,
        interval=Interval.SECOND_1,
        start_time=YEAR_BOUNDARY + timedelta(days=1),
        end_time=YEAR_BOUNDARY + timedelta(days=2),
        enforce_source=DataSource.AUTO,
    )
    logger.info(f"✓ Retrieved {len(df_2025)} data points from 2025")

    # Check that both dataframes maintain consistent precision in index
    logger.info("Step 3: Validating timestamp properties...")
    assert df_2024.index.is_monotonic_increasing, "2024 data index should be monotonic"
    assert df_2025.index.is_monotonic_increasing, "2025 data index should be monotonic"
    logger.info("✓ Both datasets have monotonically increasing timestamps")

    # Check microsecond precision
    logger.info("Step 4: Analyzing timestamp precision...")
    microseconds_2024 = [ts.microsecond for ts in df_2024.index]
    microseconds_2025 = [ts.microsecond for ts in df_2025.index]

    if any(microseconds_2024) or any(microseconds_2025):
        logger.warning("Detected microsecond precision in timestamps:")
        logger.warning(f"2024 data microsecond sample: {microseconds_2024[:5]}")
        logger.warning(f"2025 data microsecond sample: {microseconds_2025[:5]}")
    else:
        logger.info("✓ No microsecond precision detected in either dataset")


@pytest.mark.asyncio
async def test_data_source_fallback_behavior(dsm: DataSourceManager) -> None:
    """Test data source fallback behavior for historical data near year boundary.

    This test specifically focuses on historical data across the 2024-2025 boundary
    to verify that data source selection and fallback behave correctly with historical
    data that spans the year transition.

    Note: For recent data fallback behavior, see test_data_source_manager_consolidated.py
    """
    logger.info("=" * 80)
    logger.info("TEST: Historical Data Source Fallback Behavior Near Year Boundary")
    logger.info(
        "Purpose: Verify consistency between Vision API and REST API data sources for historical data"
    )
    logger.info("=" * 80)

    time_range = f"From {REFERENCE_DATE - timedelta(days=2)} to {REFERENCE_DATE - timedelta(hours=1)}"
    logger.info(f"Testing period: {time_range}")

    # First try with Vision API
    logger.info("Step 1: Fetching historical data from Vision API...")
    df_vision = await dsm.get_data(
        symbol=SYMBOL,
        interval=Interval.SECOND_1,
        start_time=REFERENCE_DATE - timedelta(days=2),
        end_time=REFERENCE_DATE - timedelta(hours=1),
        enforce_source=DataSource.VISION,
    )
    logger.info(f"✓ Retrieved {len(df_vision)} data points from Vision API")

    # Then try with REST API
    logger.info("Step 2: Fetching historical data from REST API...")
    df_rest = await dsm.get_data(
        symbol=SYMBOL,
        interval=Interval.SECOND_1,
        start_time=REFERENCE_DATE - timedelta(days=2),
        end_time=REFERENCE_DATE - timedelta(hours=1),
        enforce_source=DataSource.REST,
    )
    logger.info(f"✓ Retrieved {len(df_rest)} data points from REST API")

    # Compare results
    logger.info("Step 3: Comparing historical data from both sources...")
    if not df_vision.empty and not df_rest.empty:
        common_timestamps = df_vision.index.intersection(df_rest.index)
        logger.info(f"Found {len(common_timestamps)} common timestamps between sources")

        if not common_timestamps.empty:
            vision_slice = df_vision.loc[common_timestamps]
            rest_slice = df_rest.loc[common_timestamps]

            # Compare values for key columns
            logger.info(
                "Step 4: Validating historical data consistency across sources..."
            )
            for col in ["open", "high", "low", "close", "volume"]:
                vision_col = vision_slice[col]
                rest_col = rest_slice[col]
                pd.testing.assert_series_equal(
                    vision_col,
                    rest_col,
                    check_dtype=True,
                    check_index=True,
                    obj=f"Column {col} values differ between sources",
                )
            logger.info("✓ Historical data from both sources matches within tolerance")
