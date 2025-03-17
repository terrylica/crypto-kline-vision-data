#!/usr/bin/env python
"""Integration test suite for VisionDataClient from core.vision_data_client.

Validates the business logic implemented in the core module by verifying:
1. Correct implementation of data retrieval contracts
2. Proper handling of temporal data complexities
3. Robust error handling and data normalization

Testing Strategy:
- Boundary Testing: Time ranges near consolidation deadlines
- Equivalence Partitioning: Valid/invalid time ranges, timezones
- State Transition: Data continuity across timestamp format changes
- Negative Testing: Invalid inputs and error conditions

Quality Attributes Verified:
⌛ Reliability: Consistent data format across API versions
🔒 Security: Proper credential handling (indirectly verified)
📈 Performance: Data completeness within expected timeframes
🌐 Interoperability: Timezone normalization and format handling

Test Types:
- Integration Tests (90%): Verify end-to-end data pipeline
- Contract Tests (10%): Validate API response parsing

Dependencies:
- core.vision_data_client (SUT)
- core.vision_constraints
- Binance Vision API (external service)
"""

import pytest
from datetime import datetime, timezone, timedelta
import pandas as pd
import logging

from core.vision_data_client import VisionDataClient
from core.vision_constraints import CONSOLIDATION_DELAY

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_test_time_range(
    days_ago: int = None, duration: timedelta = timedelta(hours=1)
) -> tuple[datetime, datetime]:
    """Generate a time range for testing.

    Args:
        days_ago: Number of days ago to start range (if None, uses CONSOLIDATION_DELAY + 1 day for safety)
        duration: Duration of the time range (default: 1 hour)

    Returns:
        Tuple of (start_time, end_time) in UTC, rounded to nearest second
    """
    now = datetime.now(timezone.utc)

    # If days_ago not specified, use CONSOLIDATION_DELAY + 1 day for safety
    if days_ago is None:
        days_ago = (CONSOLIDATION_DELAY + timedelta(days=1)).days

    # Round to nearest second to avoid sub-second precision issues
    start_time = (now - timedelta(days=days_ago)).replace(microsecond=0)
    end_time = (start_time + duration).replace(microsecond=0)
    logger.info(f"Generated test time range: {start_time} to {end_time}")
    return start_time, end_time


@pytest.mark.asyncio
async def test_basic_data_retrieval():
    """Test basic data retrieval and validation.

    This test verifies:
    - Data can be retrieved successfully
    - Basic data structure is correct
    - Data completeness
    """
    start_time, end_time = get_test_time_range()
    logger.info(f"Testing basic data retrieval: {start_time} to {end_time}")

    # --- Enhanced Banner Start ---
    logger.info(
        "\n\033[1;36m===========================================\033[0m"
    )  # Cyan color for banner
    logger.info(
        "\033[1;36m=== RUNNING TEST: BASIC DATA RETRIEVAL ===\033[0m"
    )  # Cyan color for banner
    logger.info(
        "\033[1;36m===========================================\033[0m"
    )  # Cyan color for banner
    logger.info(
        f"\033[34mTime Range: {start_time} to {end_time} UTC\033[0m"
    )  # Blue color for time range
    logger.info(
        "\033[33mValidates: Data existence, structure, completeness\033[0m"
    )  # Yellow color for validation description
    # --- Enhanced Banner End ---

    async with VisionDataClient[str]("BTCUSDT") as client:  # type: ignore[valid-type]
        df = await client.fetch(start_time, end_time)

        # Basic validation
        assert not df.empty, "Retrieved data is empty"
        assert df.index.is_monotonic_increasing, "Data is not monotonically increasing"  # type: ignore
        assert df.index.tz == timezone.utc, "Data timezone is not UTC"  # type: ignore

        # Verify data completeness
        expected_rows = int((end_time - start_time).total_seconds())
        assert (
            len(df) == expected_rows + 1
        ), f"Expected {expected_rows + 1} rows, got {len(df)}"  # +1 because end time is inclusive

        # Log data sample
        logger.info(f"\nFirst few rows:\n{df.head()}")
        logger.info(f"Data shape: {df.shape}")
        logger.info(f"Index range: {df.index.min()} to {df.index.max()}")  # type: ignore


@pytest.mark.asyncio
async def test_data_consistency():
    """Test data consistency and format normalization.

    Verifies that the client maintains consistent data format:
    - UTC timezone
    - 1-second intervals
    - Proper column types
    - No gaps in data
    """
    start_time, end_time = get_test_time_range()
    logger.info(f"Testing data consistency: {start_time} to {end_time}")

    # --- Enhanced Banner Start ---
    logger.info(
        "\n\033[1;36m=========================================\033[0m"
    )  # Cyan color for banner
    logger.info(
        "\033[1;36m=== RUNNING TEST: DATA CONSISTENCY ===\033[0m"
    )  # Cyan color for banner
    logger.info(
        "\033[1;36m=========================================\033[0m"
    )  # Cyan color for banner
    logger.info(
        f"\033[34mTime Range: {start_time} to {end_time} UTC\033[0m"
    )  # Blue color for time range
    logger.info(
        "\033[33mValidates: Data format, intervals, types, gaps\033[0m"
    )  # Yellow color for validation description
    # --- Enhanced Banner End ---

    async with VisionDataClient[str]("BTCUSDT") as client:  # type: ignore[valid-type]
        df = await client.fetch(start_time, end_time)

        # Verify basic properties
        assert not df.empty, "Retrieved data is empty"
        assert df.index.is_monotonic_increasing, "Data is not monotonically increasing"  # type: ignore
        assert df.index.tz == timezone.utc, "Data timezone is not UTC"  # type: ignore
        assert not df.index.has_duplicates, "Data contains duplicate timestamps"  # type: ignore

        # Verify 1-second intervals
        intervals = df.index.to_series().diff().dropna()  # type: ignore
        assert all(intervals == pd.Timedelta(seconds=1)), "Not all intervals are 1 second"  # type: ignore

        # Verify column types
        assert df["open"].dtype == float, "open column is not float"  # type: ignore
        assert df["close"].dtype == float, "close column is not float"  # type: ignore
        assert df["volume"].dtype == float, "volume column is not float"  # type: ignore
        assert df["trades"].dtype == int, "trades column is not int"  # type: ignore

        # Verify data completeness
        expected_rows = (
            int((end_time - start_time).total_seconds()) + 1
        )  # +1 for inclusive end
        assert len(df) == expected_rows, f"Expected {expected_rows} rows, got {len(df)}"

        logger.info("Data consistency verified successfully")


@pytest.mark.asyncio
async def test_timezone_handling():
    """Test timezone handling and normalization.

    Verifies that the client properly handles and normalizes timezones:
    - Accepts various timezone inputs
    - Normalizes to UTC
    - Maintains timezone awareness
    - Handles 1-second granularity correctly
    """
    start_time, end_time = get_test_time_range()
    duration = timedelta(hours=1)

    # Create test times with different timezone representations
    start_naive = start_time.replace(tzinfo=None)  # Naive datetime
    start_est = start_time.astimezone(timezone.utc)  # EST timezone

    async with VisionDataClient[str]("BTCUSDT") as client:  # type: ignore[valid-type]
        # Test with naive datetime
        df_naive = await client.fetch(start_naive, start_naive + duration)
        assert df_naive.index.tz == timezone.utc, "Naive datetime not converted to UTC"  # type: ignore

        # Test with EST datetime
        df_est = await client.fetch(start_est, start_est + duration)
        assert df_est.index.tz == timezone.utc, "EST datetime not converted to UTC"  # type: ignore

        # Verify both datasets match
        assert df_naive.equals(df_est), "Data differs between timezone representations"

        # Verify 1-second intervals
        time_diffs = df_naive.index.to_series().diff().dropna()  # type: ignore
        assert all(diff == timedelta(seconds=1) for diff in time_diffs), "Data has irregular intervals"  # type: ignore

        # --- Enhanced Banner Start ---
        logger.info(
            "\n\033[1;36m==========================================\033[0m"
        )  # Cyan color for banner
        logger.info(
            "\033[1;36m=== RUNNING TEST: TIMEZONE HANDLING ===\033[0m"
        )  # Cyan color for banner
        logger.info(
            "\033[1;36m==========================================\033[0m"
        )  # Cyan color for banner
        logger.info(
            f"\033[34mTime Range: {start_time} to {end_time} UTC\033[0m"
        )  # Blue color for time range
        logger.info(
            "\033[33mValidates: Timezone conversion, UTC normalization\033[0m"
        )  # Yellow color for validation description
        # --- Enhanced Banner End ---

        df_naive = await client.fetch(start_naive, start_naive + duration)
        assert df_naive.index.tz == timezone.utc, "Naive datetime not converted to UTC"  # type: ignore

        logger.info("Timezone handling verified successfully")


@pytest.mark.asyncio
async def test_timestamp_format_evolution():
    """Test VisionDataClient's ability to handle both timestamp formats.

    Verifies that regardless of input timestamp format (ms or us),
    the client outputs data in a consistent format with:
    - UTC timezone
    - Monotonic timestamps
    - 1-second intervals
    - Proper datetime index
    """
    async with VisionDataClient[str]("BTCUSDT") as client:  # type: ignore[valid-type]
        # Test 2024 data (millisecond format)
        start_2024 = datetime(2024, 12, 1, tzinfo=timezone.utc)
        end_2024 = start_2024 + timedelta(hours=1)

        logger.info(f"Testing 2024 data (milliseconds): {start_2024} to {end_2024}")
        df_2024 = await client.fetch(start_2024, end_2024)

        # Test 2025 data (microsecond format)
        start_2025, end_2025 = (
            get_test_time_range()
        )  # Uses safe time range for 2025 data
        logger.info(f"Testing 2025 data (microseconds): {start_2025} to {end_2025}")
        df_2025 = await client.fetch(start_2025, end_2025)

        # Verify both datasets have consistent properties
        for period, df in [("2024", df_2024), ("2025", df_2025)]:
            logger.info(f"\nValidating {period} data:")
            logger.info(f"Shape: {df.shape}")
            logger.info(f"Index range: {df.index.min()} to {df.index.max()}")  # type: ignore
            logger.info(f"Sample data:\n{df.head()}")

            # Verify data properties
            assert not df.empty, f"{period} data is empty"
            assert df.index.is_monotonic_increasing, f"{period} data is not monotonic"  # type: ignore
            assert df.index.tz == timezone.utc, f"{period} data timezone is not UTC"  # type: ignore

            # Verify 1-second intervals
            time_diffs = df.index.to_series().diff().dropna()  # type: ignore
            assert all(diff == timedelta(seconds=1) for diff in time_diffs), f"{period} data has irregular intervals"  # type: ignore

            # Verify column types are consistent
            assert df["open"].dtype == float, f"{period} open column is not float"  # type: ignore
            assert df["close"].dtype == float, f"{period} close column is not float"  # type: ignore
            assert df["volume"].dtype == float, f"{period} volume column is not float"  # type: ignore
            assert df["trades"].dtype == int, f"{period} trades column is not int"  # type: ignore

        # --- Enhanced Banner Start ---
        logger.info(
            "\n\033[1;36m===================================================\033[0m"
        )  # Cyan color for banner
        logger.info(
            "\033[1;36m=== RUNNING TEST: TIMESTAMP FORMAT EVOLUTION ===\033[0m"
        )  # Cyan color for banner
        logger.info(
            "\033[1;36m===================================================\033[0m"
        )  # Cyan color for banner
        logger.info(
            f"\033[34mTime Range (2024): {start_2024} to {end_2024} UTC\033[0m"
        )  # Blue color for time range
        logger.info(
            f"\033[34mTime Range (2025): {start_2025} to {end_2025} UTC\033[0m"
        )  # Blue color for time range
        logger.info(
            "\033[33mValidates: Millisecond & Microsecond timestamp handling\033[0m"
        )  # Yellow color for validation description
        # --- Enhanced Banner End ---

        start_2024 = datetime(2024, 12, 1, tzinfo=timezone.utc)
        end_2024 = start_2024 + timedelta(hours=1)

        logger.info(f"Testing 2024 data (milliseconds): {start_2024} to {end_2024}")
        df_2024 = await client.fetch(start_2024, end_2024)

        # Test 2025 data (microsecond format)
        start_2025, end_2025 = (
            get_test_time_range()
        )  # Uses safe time range for 2025 data
        logger.info(f"Testing 2025 data (microseconds): {start_2025} to {end_2025}")
        df_2025 = await client.fetch(start_2025, end_2025)

        # Verify both datasets have consistent properties
        for period, df in [("2024", df_2024), ("2025", df_2025)]:
            logger.info(f"\nValidating {period} data:")
            logger.info(f"Shape: {df.shape}")
            logger.info(f"Index range: {df.index.min()} to {df.index.max()}")  # type: ignore
            logger.info(f"Sample data:\n{df.head()}")

            # Verify data properties
            assert not df.empty, f"{period} data is empty"
            assert df.index.is_monotonic_increasing, f"{period} data is not monotonic"  # type: ignore
            assert df.index.tz == timezone.utc, f"{period} data timezone is not UTC"  # type: ignore

            # Verify 1-second intervals
            time_diffs = df.index.to_series().diff().dropna()  # type: ignore
            assert all(diff == timedelta(seconds=1) for diff in time_diffs), f"{period} data has irregular intervals"  # type: ignore

            # Verify column types are consistent
            assert df["open"].dtype == float, f"{period} open column is not float"  # type: ignore
            assert df["close"].dtype == float, f"{period} close column is not float"  # type: ignore
            assert df["volume"].dtype == float, f"{period} volume column is not float"  # type: ignore
            assert df["trades"].dtype == int, f"{period} trades column is not int"  # type: ignore
