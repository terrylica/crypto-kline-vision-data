#!/usr/bin/env python
"""Basic integration tests for DataSourceManager.

Focus:
1. 1-second data only
2. Data format alignment between sources
3. Testing Vision-first strategy with fallback
4. No edge cases or caching tests initially
"""

import pytest
import pytest_asyncio
import arrow
import pandas as pd
from datetime import timedelta, timezone, datetime
from typing import Any, cast as type_cast

from ml_feature_set.utils.logger_setup import get_logger
from ml_feature_set.binance_data_services.core.data_source_manager import DataSourceManager, DataSource
from ml_feature_set.binance_data_services.utils.market_constraints import Interval, MarketType

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)

# Test configuration
TEST_SYMBOL = "BTCUSDT"  # Use BTC for reliable data
TEST_INTERVAL = Interval.SECOND_1  # Only supported interval
FIVE_MINUTES = timedelta(minutes=5)  # Small time window for quick tests

# Time constants for tests
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
VISION_PREFERRED = timedelta(days=7)  # Vision API preferred threshold


@pytest.fixture
def now() -> arrow.Arrow:
    """Get current time for tests."""
    return arrow.utcnow()


@pytest.fixture
def reference_time(now: arrow.Arrow) -> arrow.Arrow:  # type: ignore
    """Get reference time for tests."""
    return now.shift(days=-1)  # Use a time from yesterday to ensure data availability


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

    # For pandas index values or other types, convert through Timestamp
    try:
        ts = pd.Timestamp(dt)
        if ts is pd.NaT:  # type: ignore
            raise ValueError("Cannot convert NaT (Not a Time) to Arrow")
        pdt = ts.to_pydatetime()
        return arrow.get(type_cast(datetime, pdt))
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot convert {type(dt)} to Arrow: {str(e)}")


@pytest_asyncio.fixture
async def manager():
    """Create DataSourceManager instance."""
    async with DataSourceManager(market_type=MarketType.SPOT) as mgr:
        yield mgr


def log_test_configuration(test_name: str, start_time: arrow.Arrow, end_time: arrow.Arrow, expected_source: str) -> None:
    """Log test configuration in a structured format.

    Args:
        test_name: Name of the test being run
        start_time: Start time of data request
        end_time: End time of data request
        expected_source: Expected data source
    """
    logger.info("╔════════════════════════════════════════════════════════════════════════════════════════════════════════")
    logger.info(f"║ ⚙️  TEST CONFIGURATION: {test_name}")
    logger.info("╠════════════════════════════════════════════════════════════════════════════════════════════════════════")
    logger.info("║ 📊 Test Parameters:")
    logger.info(f"║   • 🕒 Time Range: {start_time.format('YYYY-MM-DD HH:mm:ss')} → {end_time.format('YYYY-MM-DD HH:mm:ss')} UTC")
    logger.info(f"║   • ⏱️  Duration: {end_time - start_time}")
    logger.info(f"║   • 🔄 Expected Source: {expected_source}")
    logger.info("║")
    logger.info("║ 📝 Strategy:")
    logger.info("║   1. Attempt data retrieval using specified configuration")
    logger.info("║   2. Validate data structure and content")
    logger.info("║   3. Verify time boundaries and data integrity")
    logger.info("║   4. Analyze performance and data quality metrics")
    logger.info("╚════════════════════════════════════════════════════════════════════════════════════════════════════════")


def log_dataframe_info(df: pd.DataFrame, source: str) -> None:
    """Log detailed DataFrame information for debugging.

    Args:
        df: DataFrame to analyze
        source: Source of the data (REST/Vision API)
    """
    logger.info("╔════════════════════════════════════════════════════════════════════════════════════════════════════════")
    logger.info(f"║ 📊 DATA ANALYSIS REPORT - {source}")
    logger.info("╠════════════════════════════════════════════════════════════════════════════════════════════════════════")

    if df.empty:
        logger.warning("║ ⚠️  DataFrame is empty!")
        logger.info("╚════════════════════════════════════════════════════════════════════════════════════════════════════════")
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

    # Convert index values to Arrow objects for consistent formatting
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
    logger.info(f"║   • 💰 Price Range: ${df['low'].min():,.2f} → ${df['high'].max():,.2f}")
    logger.info(f"║   • 📈 Average Volume: {df['volume'].mean():,.2f}")
    logger.info(f"║   • 🔄 Total Trades: {df['trades'].sum():,}")

    # Data Types
    logger.info("║")
    logger.info("║ 🔧 Column Data Types:")
    for col, dtype in df.dtypes.items():
        logger.info(f"║   • {col}: {dtype}")

    logger.info("╚════════════════════════════════════════════════════════════════════════════════════════════════════════")


def validate_dataframe_structure(df: pd.DataFrame, allow_empty: bool = True) -> None:
    """Validate DataFrame structure with detailed logging.

    Args:
        df: DataFrame to validate
        allow_empty: Whether empty DataFrames are allowed
    """
    logger.info("╔═══════════════════════════════════════════════════════════════════════════")
    logger.info("║ Structure Validation")
    logger.info("╠═══════════════════════════════════════════════════════════════════════════")

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

    logger.info("║")
    logger.info("║ Column Validation:")
    missing_columns = required_columns - set(df.columns)
    extra_columns = set(df.columns) - required_columns

    if not missing_columns:
        logger.info("║ ✓ All required columns present")
    else:
        logger.error(f"║ ❌ Missing columns: {missing_columns}")
        raise AssertionError(f"Missing required columns: {missing_columns}")

    if extra_columns:
        logger.warning(f"║ ⚠️  Extra columns present: {extra_columns}")

    logger.info("╚═══════════════════════════════════════════════════════════════════════════")


def validate_time_boundaries(df: pd.DataFrame, start_time: arrow.Arrow, end_time: arrow.Arrow) -> None:
    """Validate that data falls within the requested time boundaries.

    Args:
        df: DataFrame to validate
        start_time: Expected start time
        end_time: Expected end time
    """
    if df.empty:
        return

    logger.info("╔═══════════════════════════════════════════════════════════════════════════")
    logger.info("║ Time Boundary Validation")
    logger.info("╠═══════════════════════════════════════════════════════════════════════════")

    # Convert DataFrame index timestamps to Arrow objects
    actual_start = to_arrow(df.index.min())
    actual_end = to_arrow(df.index.max())

    logger.info("║ Time Range Analysis:")
    logger.info(f"║   • Requested: {start_time.format('YYYY-MM-DD HH:mm:ss')} → {end_time.format('YYYY-MM-DD HH:mm:ss')} UTC")
    logger.info(f"║   • Actual: {actual_start.format('YYYY-MM-DD HH:mm:ss')} → {actual_end.format('YYYY-MM-DD HH:mm:ss')} UTC")

    if actual_start >= start_time:
        logger.info("║ ✓ Data starts within bounds")
    else:
        logger.error(f"║ ❌ Data starts {start_time - actual_start} too early")
        raise AssertionError("Data starts too early")

    if actual_end <= end_time:
        logger.info("║ ✓ Data ends within bounds")
    else:
        logger.error(f"║ ❌ Data ends {actual_end - end_time} too late")
        raise AssertionError("Data ends too late")

    logger.info("╚═══════════════════════════════════════════════════════════════════════════")


def log_test_motivation(test_name: str, motivation: str, expectations: list[str], implications: list[str]) -> None:
    """Log detailed test motivation and expectations.

    Args:
        test_name: Name of the test
        motivation: Why this test exists
        expectations: List of what we expect to see
        implications: Business/technical implications of this test
    """
    # Add newline before banner to ensure clean separation
    logger.info("")
    logger.info("╔════════════════════════════════════════════════════════════════════════════════════════════════════════")
    logger.info(f"║ 🧪 TEST CASE: {test_name}")
    logger.info("╠════════════════════════════════════════════════════════════════════════════════════════════════════════")
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

    logger.info("╚════════════════════════════════════════════════════════════════════════════════════════════════════════")


@pytest.mark.real
@pytest.mark.asyncio
async def test_very_recent_data(manager: DataSourceManager, now: arrow.Arrow):  # type: ignore
    """Test very recent data retrieval (Vision first, with fallback to REST)."""
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

    end_time = now.shift(hours=-1)
    start_time = end_time.shift(minutes=-5)

    log_test_configuration("Very Recent Data Retrieval", start_time, end_time, "Vision API with REST fallback")

    df = await manager.get_data(symbol=TEST_SYMBOL, start_time=start_time.datetime, end_time=end_time.datetime, use_cache=False)

    log_dataframe_info(df, "API Response")
    validate_dataframe_structure(df)
    validate_time_boundaries(df, start_time, end_time)


@pytest.mark.real
@pytest.mark.asyncio
async def test_large_data_request(manager: DataSourceManager, now: arrow.Arrow):  # type: ignore
    """Test large data request (should use Vision API)."""
    log_test_motivation(
        "Large Data Request Test",
        "Historical analysis and model training often require large datasets. This test verifies our "
        "ability to efficiently fetch and process substantial amounts of market data without "
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

    end_time = now.shift(days=-2)
    start_time = end_time.shift(hours=-24)

    log_test_configuration("Large Data Request", start_time, end_time, "Vision API")

    df = await manager.get_data(symbol=TEST_SYMBOL, start_time=start_time.datetime, end_time=end_time.datetime, use_cache=False)

    log_dataframe_info(df, "Vision API")
    validate_dataframe_structure(df)
    validate_time_boundaries(df, start_time, end_time)


@pytest.mark.real
@pytest.mark.asyncio
async def test_historical_data(manager: DataSourceManager, now: arrow.Arrow):  # type: ignore
    """Test historical data retrieval (should use Vision API)."""
    log_test_motivation(
        "Historical Data Retrieval Test",
        "Backtesting and historical analysis require reliable access to older market data. "
        "This test ensures we can accurately retrieve historical data points while optimizing "
        "for cost and performance using the Vision API.",
        expectations=[
            "System uses Vision API for historical data",
            "Data is complete and accurate",
            "Timestamps are properly aligned",
            "All historical metrics are preserved",
        ],
        implications=[
            "Enables accurate backtesting of trading strategies",
            "Supports historical market analysis",
            "Optimizes data retrieval costs",
            "Maintains data consistency for research",
        ],
    )

    end_time = now.shift(days=-30)
    start_time = end_time.shift(minutes=-5)

    log_test_configuration("Historical Data Retrieval", start_time, end_time, "Vision API")

    df = await manager.get_data(symbol=TEST_SYMBOL, start_time=start_time.datetime, end_time=end_time.datetime, use_cache=False)

    log_dataframe_info(df, "Vision API")
    validate_dataframe_structure(df)
    validate_time_boundaries(df, start_time, end_time)


@pytest.mark.real
@pytest.mark.asyncio
async def test_enforced_rest_api(manager: DataSourceManager, now: arrow.Arrow):  # type: ignore
    """Test enforcing REST API for data retrieval."""
    log_test_motivation(
        "Enforced REST API Test",
        "Sometimes we need to explicitly use the REST API regardless of our automatic selection logic. "
        "This test verifies that we can override the default behavior and force REST API usage "
        "while maintaining data quality and reliability.",
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

    end_time = now.shift(hours=-1)
    start_time = end_time.shift(minutes=-5)

    log_test_configuration("Enforced REST API Usage", start_time, end_time, "REST API (enforced)")

    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        start_time=start_time.datetime,
        end_time=end_time.datetime,
        use_cache=False,
        enforce_source=DataSource.REST,
    )

    log_dataframe_info(df, "REST API (enforced)")
    validate_dataframe_structure(df)
    validate_time_boundaries(df, start_time, end_time)


@pytest.mark.real
@pytest.mark.asyncio
async def test_enforced_vision_api(manager: DataSourceManager, now: arrow.Arrow):  # type: ignore
    """Test enforcing Vision API for data retrieval."""
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

    end_time = now.shift(days=-2)
    start_time = end_time.shift(minutes=-5)

    log_test_configuration("Enforced Vision API Usage", start_time, end_time, "Vision API (enforced)")

    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        start_time=start_time.datetime,
        end_time=end_time.datetime,
        use_cache=False,
        enforce_source=DataSource.VISION,
    )

    log_dataframe_info(df, "Vision API (enforced)")
    validate_dataframe_structure(df)
    validate_time_boundaries(df, start_time, end_time)


@pytest.mark.real
@pytest.mark.asyncio
async def test_vision_to_rest_fallback(manager: DataSourceManager, now: arrow.Arrow):  # type: ignore
    """Test Vision API fallback to REST API when data is not available.

    This test verifies that:
    1. The system attempts to use Vision API first
    2. When Vision API data is not available, it falls back to REST API
    3. The fallback is seamless and returns valid data
    4. The process is properly logged for monitoring

    Note:
        The actual availability of Vision API data is determined dynamically.
        We don't make assumptions about when data will be available, as this
        depends on Binance's data publishing schedule.
    """
    # Test configuration
    start_time = now.shift(minutes=-5)
    end_time = now.shift(minutes=-1)

    log_test_motivation(
        "Vision to REST Fallback",
        """
        Verify seamless fallback from Vision API to REST API when data is not available.
        This is critical for maintaining data availability without making assumptions
        about Vision API data publishing schedules.
        """,
        [
            "System attempts Vision API first",
            "Graceful fallback to REST API when Vision data unavailable",
            "Complete data retrieval through fallback mechanism",
            "Proper logging of source selection and fallback",
        ],
        [
            "Ensures continuous data availability",
            "Adapts to Vision API publishing schedule",
            "Maintains data quality through source switching",
        ],
    )

    log_test_configuration("Vision to REST Fallback", start_time, end_time, "Vision → REST")

    # Request data - should try Vision first, then fall back to REST
    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        start_time=start_time.datetime,
        end_time=end_time.datetime,
    )

    # Validate data structure and boundaries
    validate_dataframe_structure(df)
    validate_time_boundaries(df, start_time, end_time)

    # Log detailed data analysis
    log_dataframe_info(df, "Vision → REST Fallback")

    # Verify we got valid data
    assert not df.empty, "Should receive data through fallback mechanism"
    assert df.index.is_monotonic_increasing, "Data should be properly ordered"
    assert isinstance(df.index, pd.DatetimeIndex), "Index should be DatetimeIndex"
    assert df.index.tz == timezone.utc, "Data should be in UTC"


@pytest.mark.real
@pytest.mark.asyncio
async def test_date_validation(manager: DataSourceManager, now: arrow.Arrow):
    """Test date validation logic."""
    log_test_motivation(
        "Date Validation Test",
        "Data integrity begins with proper time range validation. This test ensures that our "
        "system properly handles invalid date ranges and prevents nonsensical data requests "
        "that could affect trading decisions.",
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

    logger.info("╔═══════════════════════════════════════════════════════════════════════════")
    logger.info("║ Date Validation Tests")
    logger.info("╠═══════════════════════════════════════════════════════════════════════════")

    # Test future end time
    future_time = now.shift(days=1)
    logger.info("║ Testing Future Date Validation:")
    logger.info(f"║   • Current time: {now.format('YYYY-MM-DD HH:mm:ss')} UTC")
    logger.info(f"║   • Future time: {future_time.format('YYYY-MM-DD HH:mm:ss')} UTC")

    with pytest.raises(ValueError, match="is in the future"):
        await manager.get_data(symbol=TEST_SYMBOL, start_time=now.datetime, end_time=future_time.datetime, use_cache=False)
    logger.info("║ ✓ Future date validation passed")

    # Test start time after end time
    invalid_start = now.shift(hours=-1)
    invalid_end = now.shift(hours=-2)
    logger.info("║")
    logger.info("║ Testing Start After End Validation:")
    logger.info(f"║   • Start time: {invalid_start.format('YYYY-MM-DD HH:mm:ss')} UTC")
    logger.info(f"║   • End time: {invalid_end.format('YYYY-MM-DD HH:mm:ss')} UTC")

    with pytest.raises(ValueError, match="is after end time"):
        await manager.get_data(symbol=TEST_SYMBOL, start_time=invalid_start.datetime, end_time=invalid_end.datetime, use_cache=False)
    logger.info("║ ✓ Start after end validation passed")

    logger.info("╚═══════════════════════════════════════════════════════════════════════════")
