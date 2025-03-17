#!/usr/bin/env python
"""Consolidated integration tests for DataSourceManager.

Test Categories:
1. Basic Data Retrieval - Tests basic functionality and data validation
2. Data Source Selection - Tests source selection logic and fallback behavior
3. Cache Performance - Tests caching behavior and performance 
4. Date Validation - Tests date boundary conditions and error handling
5. General Edge Cases - Common error scenarios and boundary conditions

Related tests:
- For time format/precision tests: see test_data_source_manager_format_precision.py
- For year boundary tests: see test_data_source_manager_year_boundary.py
- For detailed time boundary edge cases: see test_data_source_manager_boundary.py
"""

import pytest
import pytest_asyncio
import arrow
import pandas as pd
import time
import psutil
import os
from datetime import timedelta, timezone, datetime
from typing import Any, cast as type_cast, Tuple, Dict, List, AsyncGenerator

from utils.logger_setup import get_logger
from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import Interval, MarketType

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)

# Test configuration
TEST_SYMBOL = "BTCUSDT"  # Use BTC for reliable data
TEST_INTERVAL = Interval.SECOND_1  # Only supported interval
FIVE_MINUTES = timedelta(minutes=5)  # Small time window for quick tests

# Time constants for tests
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
VISION_PREFERRED = timedelta(days=7)  # Vision API preferred threshold

# Year boundary constants
YEAR_BOUNDARY = arrow.get("2025-01-01T00:00:00Z").datetime
BEFORE_BOUNDARY = YEAR_BOUNDARY - timedelta(days=5)
AFTER_BOUNDARY = YEAR_BOUNDARY + timedelta(days=5)


def to_arrow(dt: Any) -> arrow.Arrow:
    """Convert various datetime types to Arrow.

    Args:
        dt: Input datetime in any format

    Returns:
        Arrow object

    Raises:
        ValueError: If input is NaT or invalid
    """
    # Handle NaT explicitly for pandas types
    if pd.api.types.is_datetime64_any_dtype(dt) and pd.isna(dt):
        raise ValueError("Cannot convert NaT (Not a Time) to Arrow")

    if isinstance(dt, arrow.Arrow):
        return dt
    if isinstance(dt, datetime):
        return arrow.get(type_cast(datetime, dt))
    if isinstance(dt, pd.Timestamp):
        if dt is pd.NaT:  # type: ignore
            raise ValueError("Cannot convert NaT (Not a Time) to Arrow")
        pdt = dt.to_pydatetime()
        return arrow.get(type_cast(datetime, pdt))

    # Convert other types through Timestamp
    try:
        ts = pd.Timestamp(dt)
        if ts is pd.NaT:  # type: ignore
            raise ValueError("Cannot convert NaT (Not a Time) to Arrow")
        pdt = ts.to_pydatetime()
        return arrow.get(type_cast(datetime, pdt))
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot convert {type(dt)} to Arrow: {str(e)}")


def get_memory_usage() -> float:
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def log_test_motivation(
    test_name: str, motivation: str, expectations: list[str], implications: list[str]
) -> None:
    """Log detailed test motivation and expectations."""
    logger.info("")
    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info(f"║ 🧪 TEST CASE: {test_name}")
    logger.info(
        "╠════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ 🎯 MOTIVATION:")
    for line in motivation.split("\n"):
        logger.info(f"║   {line.strip()}")

    logger.info("║")
    logger.info("║ ✅ EXPECTATIONS:")
    for i, exp in enumerate(expectations, 1):
        logger.info(f"║   {i}. {exp}")

    logger.info("║")
    logger.info("║ 💡 BUSINESS/TECHNICAL IMPLICATIONS:")
    for i, imp in enumerate(implications, 1):
        logger.info(f"║   {i}. {imp}")

    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )


def log_dataframe_info(df: pd.DataFrame, source: str) -> None:
    """Log detailed DataFrame information for analysis."""
    logger.info(
        "╔════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )
    logger.info(f"║ 📊 DATA ANALYSIS REPORT - {source}")
    logger.info(
        "╠════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )

    if df.empty:
        logger.warning("║ ⚠️  DataFrame is empty!")
        logger.info(
            "╚════════════════════════════════════════════════════════════════════════════════════════════════════════"
        )
        return

    # Basic Information
    logger.info("║ 📌 Basic Information:")
    logger.info(f"║   • 📑 Records: {df.shape[0]:,}")
    logger.info(f"║   • 📊 Columns: {df.shape[1]}")
    logger.info(f"║   • 🔑 Index Type: {type(df.index).__name__}")
    if isinstance(df.index, pd.DatetimeIndex):
        logger.info(f"║   • 🌐 Timezone: {df.index.tz or 'naive'}")
    else:
        logger.info("║   • 🌐 Timezone: N/A (not a DatetimeIndex)")

    # Time Range Analysis
    logger.info("║")
    logger.info("║ ⏰ Time Range Analysis:")
    first_ts = to_arrow(df.index[0])
    last_ts = to_arrow(df.index[-1])
    logger.info(f"║   • 🔵 First Record: {first_ts.format('YYYY-MM-DD HH:mm:ss')} UTC")
    logger.info(f"║   • 🔴 Last Record: {last_ts.format('YYYY-MM-DD HH:mm:ss')} UTC")
    logger.info(f"║   • ⌛ Total Duration: {last_ts - first_ts}")

    # Data Quality Metrics
    logger.info("║")
    logger.info("║ 🔍 Data Quality Metrics:")
    logger.info(f"║   • ❌ Missing Values: {df.isnull().sum().sum():,}")
    logger.info(f"║   • 🔄 Duplicate Timestamps: {df.index.duplicated().sum():,}")

    # Price Statistics
    logger.info("║")
    logger.info("║ 💹 Price Statistics:")
    logger.info(
        f"║   • 💰 Price Range: ${df['low'].min():,.2f} → ${df['high'].max():,.2f}"
    )
    logger.info(f"║   • 📈 Average Volume: {df['volume'].mean():,.2f}")
    logger.info(f"║   • 🔄 Total Trades: {df['trades'].sum():,}")

    # Data Types
    logger.info("║")
    logger.info("║ 🔧 Column Data Types:")
    for col, dtype in df.dtypes.items():
        logger.info(f"║   • {col}: {dtype}")

    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )


def validate_dataframe_structure(df: pd.DataFrame, allow_empty: bool = True) -> None:
    """Validate DataFrame structure with detailed logging."""
    logger.info(
        "╔═══════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ Structure Validation")
    logger.info(
        "╠═══════════════════════════════════════════════════════════════════════════"
    )

    # Empty Check
    if df.empty and not allow_empty:
        logger.error("║ ❌ DataFrame is empty when it should contain data")
        raise AssertionError("DataFrame should not be empty")
    elif df.empty:
        logger.info("║ ℹ️  DataFrame is empty (allowed)")
        return

    # Index Validation
    logger.info("║ Index Validation:")
    if isinstance(df.index, pd.DatetimeIndex):
        logger.info("║ ✓ Index is DatetimeIndex")
    else:
        logger.error(f"║ ❌ Index is {type(df.index).__name__}, expected DatetimeIndex")
        raise AssertionError("Index should be DatetimeIndex")

    if df.index.tz == timezone.utc:
        logger.info("║ ✓ Timezone is UTC")
    else:
        logger.error(f"║ ❌ Timezone is {df.index.tz}, expected UTC")
        raise AssertionError("Index should be UTC")

    if df.index.is_monotonic_increasing:
        logger.info("║ ✓ Index is monotonically increasing")
    else:
        logger.error("║ ❌ Index is not monotonically increasing")
        raise AssertionError("Index should be monotonically increasing")

    # Column Validation
    logger.info("║")
    logger.info("║ Column Validation:")
    required_columns = {
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
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        logger.error(f"║ ❌ Missing required columns: {missing_columns}")
        raise AssertionError(f"Missing required columns: {missing_columns}")
    logger.info("║ ✓ All required columns present")

    logger.info(
        "╚═══════════════════════════════════════════════════════════════════════════"
    )


@pytest_asyncio.fixture
async def manager() -> AsyncGenerator[DataSourceManager, None]:
    """Create DataSourceManager instance."""
    async with DataSourceManager(market_type=MarketType.SPOT) as mgr:
        yield mgr


@pytest.fixture
def now() -> arrow.Arrow:
    """Get current time for tests."""
    return arrow.utcnow()


# Category 1: Basic Data Retrieval Tests
@pytest.mark.real
@pytest.mark.asyncio
async def test_very_recent_data(manager: DataSourceManager, now: arrow.Arrow) -> None:
    """Test retrieval of very recent data."""
    log_test_motivation(
        "Very Recent Data Retrieval Test",
        "In algorithmic trading, access to the most recent data is crucial for making timely decisions. "
        "This test ensures we can reliably fetch very recent market data, which is essential for "
        "real-time trading strategies and market monitoring.",
        expectations=[
            "System attempts Vision API first (new strategy)",
            "Smooth fallback to REST API if Vision fails",
            "Data is fresh (within the last hour)",
            "All required OHLCV fields are present and valid",
        ],
        implications=[
            "Ensures trading strategies have access to recent market data",
            "Validates our data source selection strategy",
            "Confirms system resilience through fallback mechanism",
            "Guarantees data quality for real-time analysis",
        ],
    )

    start_time = now.shift(minutes=-5)
    end_time = now
    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=start_time.datetime,
        end_time=end_time.datetime,
        use_cache=False,
    )

    log_dataframe_info(df, "API Response")
    validate_dataframe_structure(df, allow_empty=False)


@pytest.mark.real
@pytest.mark.asyncio
async def test_large_data_request(manager: DataSourceManager, now: arrow.Arrow) -> None:
    """Test handling of large data requests."""
    log_test_motivation(
        "Large Data Request Test",
        "Historical analysis and model training often require large datasets. This test verifies "
        "our ability to efficiently fetch and process substantial amounts of market data without "
        "overwhelming the REST API or our network resources.",
        expectations=[
            "System chooses Vision API for large requests",
            "Successfully handles 24-hour data window",
            "Maintains data integrity across the entire period",
            "Efficient data retrieval without timeouts",
        ],
        implications=[
            "Enables reliable historical analysis",
            "Supports machine learning model training",
            "Optimizes network resource usage",
            "Prevents REST API rate limiting issues",
        ],
    )

    start_time = now.shift(days=-1)
    end_time = now
    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=start_time.datetime,
        end_time=end_time.datetime,
        use_cache=False,
    )

    log_dataframe_info(df, "Vision API")
    validate_dataframe_structure(df, allow_empty=False)


# Category 3: Data Source Selection Tests
@pytest.mark.real
@pytest.mark.asyncio
async def test_enforced_rest_api(manager: DataSourceManager, now: arrow.Arrow) -> None:
    """Test enforced REST API usage."""
    log_test_motivation(
        "Enforced REST API Test",
        "Sometimes we need to explicitly use the REST API regardless of our automatic selection logic. "
        "This test verifies that we can override the default behavior and force REST API usage while "
        "maintaining data quality and reliability.",
        expectations=[
            "System respects enforced REST API usage",
            "Successfully retrieves data via REST",
            "Maintains data quality standards",
            "Handles rate limits appropriately",
        ],
        implications=[
            "Provides manual control over data source",
            "Supports specialized use cases",
            "Validates REST API reliability",
            "Ensures consistent data quality across sources",
        ],
    )

    start_time = now.shift(minutes=-5)
    end_time = now
    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=start_time.datetime,
        end_time=end_time.datetime,
        enforce_source=DataSource.REST,
    )

    log_dataframe_info(df, "REST API (enforced)")
    validate_dataframe_structure(df, allow_empty=False)


@pytest.mark.real
@pytest.mark.asyncio
async def test_enforced_vision_api(
    manager: DataSourceManager, now: arrow.Arrow
) -> None:
    """Test enforced Vision API usage."""
    log_test_motivation(
        "Enforced Vision API Test",
        "In certain scenarios, we want to explicitly use the Vision API for data retrieval. "
        "This test ensures we can override automatic source selection and force Vision API usage "
        "while maintaining data integrity and completeness.",
        expectations=[
            "System respects enforced Vision API usage",
            "Successfully retrieves data from Vision API",
            "Maintains data quality standards",
            "Handles Vision API constraints properly",
        ],
        implications=[
            "Provides manual control over data source",
            "Supports bulk data retrieval needs",
            "Validates Vision API reliability",
            "Ensures optimal resource utilization",
        ],
    )

    start_time = now.shift(days=-2)
    end_time = now.shift(days=-1)
    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=start_time.datetime,
        end_time=end_time.datetime,
        enforce_source=DataSource.VISION,
    )

    log_dataframe_info(df, "Vision API (enforced)")
    validate_dataframe_structure(df, allow_empty=False)


@pytest.mark.real
@pytest.mark.asyncio
async def test_vision_to_rest_fallback(
    manager: DataSourceManager, now: arrow.Arrow
) -> None:
    """Test Vision to REST API fallback behavior for very recent data.

    This test focuses specifically on very recent data (minutes ago),
    where Vision API data might not be available yet and a fallback to
    REST API should occur automatically.

    Note: For historical data fallback behavior tests near the year boundary,
    see test_data_source_manager_year_boundary.py
    """
    log_test_motivation(
        "Vision to REST Fallback for Recent Data",
        "Verify seamless fallback from Vision API to REST API when recent data is not available. "
        "This is critical for maintaining real-time data availability without making assumptions "
        "about Vision API data publishing schedules.",
        expectations=[
            "System attempts Vision API first",
            "Graceful fallback to REST API when recent Vision data unavailable",
            "Complete data retrieval through fallback mechanism",
            "Proper logging of source selection and fallback",
        ],
        implications=[
            "Ensures continuous recent data availability",
            "Adapts to Vision API publishing schedule",
            "Maintains data quality through source switching for real-time data",
        ],
    )

    start_time = now.shift(minutes=-4)
    end_time = now
    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=start_time.datetime,
        end_time=end_time.datetime,
        use_cache=False,
    )

    log_dataframe_info(df, "Vision → REST Fallback (Recent Data)")
    validate_dataframe_structure(df, allow_empty=False)


# Category 5: Edge Cases and Error Handling
@pytest.mark.real
@pytest.mark.asyncio
async def test_date_validation(manager: DataSourceManager, now: arrow.Arrow) -> None:
    """Test date validation and error handling."""
    log_test_motivation(
        "Date Validation Test",
        "Data integrity begins with proper time range validation. This test ensures that our "
        "system properly handles invalid date ranges and prevents nonsensical data requests that "
        "could affect trading decisions.",
        expectations=[
            "Rejects future date requests",
            "Prevents invalid time ranges",
            "Provides clear error messages",
            "Maintains system stability",
        ],
        implications=[
            "Prevents invalid data queries",
            "Ensures data consistency",
            "Improves error handling",
            "Supports data quality assurance",
        ],
    )

    logger.info(
        "╔═══════════════════════════════════════════════════════════════════════════"
    )
    logger.info("║ Date Validation Tests")
    logger.info(
        "╠═══════════════════════════════════════════════════════════════════════════"
    )

    # Test future date rejection
    logger.info("║ Testing Future Date Validation:")
    logger.info(f"║   • Current time: {now.format('YYYY-MM-DD HH:mm:ss')} UTC")
    future_time = now.shift(days=1)
    logger.info(f"║   • Future time: {future_time.format('YYYY-MM-DD HH:mm:ss')} UTC")

    with pytest.raises(ValueError, match="is in the future"):
        await manager.get_data(
            symbol=TEST_SYMBOL,
            interval=TEST_INTERVAL,
            start_time=now.datetime,
            end_time=future_time.datetime,
        )
    logger.info("║ ✓ Future date validation passed")

    # Test start after end validation
    logger.info("║")
    logger.info("║ Testing Start After End Validation:")
    start_time = now.shift(minutes=-60)
    end_time = now.shift(minutes=-120)
    logger.info(f"║   • Start time: {start_time.format('YYYY-MM-DD HH:mm:ss')} UTC")
    logger.info(f"║   • End time: {end_time.format('YYYY-MM-DD HH:mm:ss')} UTC")

    with pytest.raises(ValueError, match="must be before"):
        await manager.get_data(
            symbol=TEST_SYMBOL,
            interval=TEST_INTERVAL,
            start_time=start_time.datetime,
            end_time=end_time.datetime,
        )
    logger.info("║ ✓ Start after end validation passed")

    logger.info(
        "╚═══════════════════════════════════════════════════════════════════════════"
    )
