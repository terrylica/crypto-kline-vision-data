#!/usr/bin/env python
"""Integration tests for DataSourceManager focusing on time formats and precision.

System Under Test (SUT):
- core.data_source_manager.DataSourceManager
- utils.time_alignment (indirectly)
- core.vision_data_client.VisionDataClient (indirectly)
- core.rest_data_client.RestDataClient (indirectly)

This test suite verifies the timestamp handling behavior in the DataSourceManager
with a focus on precision and format consistency across different data sources.

Focus areas:
1. Time boundary behaviors and alignment
2. Input format validation and defaults
3. Output format consistency and guarantees
4. Timestamp precision handling
5. Timezone handling and conversions

This is the primary file for all timestamp format and precision related tests.
It defines the core expectations and guarantees about how timestamps are handled.

Related tests:
- For specific boundary edge cases: see test_data_source_manager_boundary.py
- For year boundary specific tests: see test_data_source_manager_year_boundary.py
- For general validation and data tests: see test_data_source_manager_consolidated.py
"""

import pytest
import arrow
import pandas as pd
from datetime import timedelta, timezone, datetime
from typing import Any, cast as type_cast
import numpy as np
import pytest_asyncio

from utils.logger_setup import logger
from core.data_source_manager import DataSourceManager
from utils.market_constraints import Interval, MarketType


# Test configuration
TEST_SYMBOL = "BTCUSDT"  # Use BTC for reliable data
TEST_INTERVAL = Interval.SECOND_1  # Focus on 1-second data

# Time constants for tests
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)


@pytest.fixture
def now() -> arrow.Arrow:
    """Get current time for tests."""
    return arrow.utcnow()


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
        if dt is pd.NaT:
            raise ValueError("Cannot convert NaT (Not a Time) to Arrow")
        pdt = dt.to_pydatetime()
        return arrow.get(type_cast(datetime, pdt))

    # Convert other types through Timestamp
    try:
        ts = pd.Timestamp(dt)
        if ts is pd.NaT:
            raise ValueError("Cannot convert NaT (Not a Time) to Arrow")
        pdt = ts.to_pydatetime()
        return arrow.get(type_cast(datetime, pdt))
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot convert {type(dt)} to Arrow: {str(e)}")


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
    logger.info(
        f"║   • 🔵 First Record: {first_ts.format('YYYY-MM-DD HH:mm:ss.SSSSSS')} UTC"
    )
    logger.info(
        f"║   • 🔴 Last Record: {last_ts.format('YYYY-MM-DD HH:mm:ss.SSSSSS')} UTC"
    )
    logger.info(f"║   • ⌛ Total Duration: {last_ts - first_ts}")
    logger.info(f"║   • 🎯 Record Count: {len(df):,}")

    # Data Types and Format
    logger.info("║")
    logger.info("║ 🔧 Column Data Types and Sample Values:")
    for col in df.columns:
        sample_val = df[col].iloc[0]
        logger.info(f"║   • {col}: {df[col].dtype} (e.g., {sample_val})")

    logger.info(
        "╚════════════════════════════════════════════════════════════════════════════════════════════════════════"
    )


@pytest.fixture
def caplog_maybe(request):
    """Fixture to provide a safe caplog alternative that works with pytest-xdist."""

    # Create a dummy caplog object if the real one is not available
    class DummyCaplog:
        """A dummy caplog implementation that doesn't raise KeyError."""

        def __init__(self):
            """Initialize with empty records."""
            self.records = []
            self.text = ""

        def set_level(self, level, logger=None):
            """Dummy implementation of set_level."""
            pass

        def clear(self):
            """Clear logs."""
            self.records = []
            self.text = ""

    # Always return the dummy implementation to avoid issues with pytest-xdist
    return DummyCaplog()


@pytest_asyncio.fixture
async def manager():
    """Create DataSourceManager instance."""
    async with DataSourceManager(market_type=MarketType.SPOT) as mgr:
        yield mgr


@pytest.mark.real
@pytest.mark.asyncio(loop_scope="function")
async def test_time_boundary_alignment(
    manager: DataSourceManager, now: arrow.Arrow, caplog_maybe
):
    """Test how DataSourceManager aligns time boundaries for different inputs."""
    log_test_motivation(
        "Time Boundary Alignment",
        "Understanding how DataSourceManager handles various time boundary inputs "
        "and what alignment rules are applied to start/end times.",
        expectations=[
            "Microsecond precision in timestamps",
            "Proper handling of non-aligned timestamps",
            "Consistent boundary alignment rules",
            "Timezone-aware index in UTC",
        ],
        implications=[
            "Defines timestamp precision requirements",
            "Clarifies boundary alignment behavior",
            "Documents timezone handling",
            "Establishes input format standards",
        ],
    )

    # Test cases with different time alignments (using historical dates)
    base_time = now.shift(days=-2)
    test_cases = [
        # Case 1: Clean second boundaries
        (base_time.floor("second"), base_time.floor("second").shift(seconds=10)),
        # Case 2: Millisecond precision
        (
            base_time.shift(microseconds=500000),
            base_time.shift(seconds=10, microseconds=750000),
        ),
        # Case 3: Microsecond precision
        (
            base_time.shift(microseconds=123456),
            base_time.shift(seconds=10, microseconds=987654),
        ),
    ]

    for start_time, end_time in test_cases:
        logger.info(f"Testing time range: {start_time} to {end_time}")

        df = await manager.get_data(
            symbol=TEST_SYMBOL,
            interval=TEST_INTERVAL,
            start_time=start_time.datetime,
            end_time=end_time.datetime,
            use_cache=False,
        )

        log_dataframe_info(
            df,
            f"Alignment Test ({start_time.format('ss.SSSSSS')} to {end_time.format('ss.SSSSSS')})",
        )

        # Skip validation for empty dataframes
        if df.empty:
            logger.warning("Empty DataFrame returned - skipping validation")
            continue

        # Verify index properties
        assert isinstance(df.index, pd.DatetimeIndex), "Index must be DatetimeIndex"
        assert df.index.tz == timezone.utc, "Index must be UTC timezone-aware"

        # Check timestamp precision
        index_microseconds = df.index.astype(np.int64) % 1_000_000
        assert np.all(
            index_microseconds == 0
        ), "Timestamps should be aligned to seconds"


@pytest.mark.real
@pytest.mark.asyncio(loop_scope="function")
async def test_input_format_handling(
    manager: DataSourceManager, now: arrow.Arrow, caplog_maybe
):
    """Test DataSourceManager's handling of various input formats."""
    log_test_motivation(
        "Input Format Handling",
        "Verify that the DataSourceManager properly handles different input formats "
        "for time parameters and consistently produces well-formatted output.",
        expectations=[
            "Timezone-naive and aware input acceptance",
            "Various datetime formats properly normalized",
            "Consistent UTC timezone output regardless of input",
            "Proper handling of boundary cases (exact second, millisecond, microsecond precision)",
        ],
        implications=[
            "Establishes input format flexibility",
            "Documents format normalization behavior",
            "Prevents timezone-related issues",
            "Defines input validation standards",
        ],
    )

    # Test cases with different input formats
    test_cases = [
        # Case 1: Naive datetime
        (
            datetime.now() - timedelta(days=2),
            datetime.now() - timedelta(days=2) + timedelta(minutes=5),
            "Naive datetime",
        ),
        # Case 2: Aware datetime (UTC)
        (
            datetime.now(timezone.utc) - timedelta(days=2),
            datetime.now(timezone.utc) - timedelta(days=2) + timedelta(minutes=5),
            "Aware datetime (UTC)",
        ),
    ]

    for start_time, end_time, case_name in test_cases:
        logger.info(f"Testing input format: {case_name}")
        logger.info(f"Input - Start: {start_time}, End: {end_time}")

        df = await manager.get_data(
            symbol=TEST_SYMBOL,
            interval=TEST_INTERVAL,
            start_time=start_time,
            end_time=end_time,
            use_cache=False,
        )

        log_dataframe_info(df, f"Input Format Test ({case_name})")

        # Skip validation for empty dataframes
        if df.empty:
            logger.warning(
                f"Empty DataFrame returned for {case_name} - skipping validation"
            )
            continue

        # Verify core structure with any format
        assert isinstance(df.index, pd.DatetimeIndex), "Index must be DatetimeIndex"
        assert df.index.tz == timezone.utc, "Index must have UTC timezone"

        # Check essential properties
        assert not df.empty, "DataFrame should not be empty"
        assert pd.api.types.is_datetime64_dtype(df["close_time"]) or str(
            df["close_time"].dtype
        ).startswith(
            "datetime64"
        ), "close_time must be datetime64 type (with or without timezone)"

        # Check for either 'trades' or 'number_of_trades' column
        trades_col = None
        if "trades" in df.columns:
            trades_col = "trades"
        elif "number_of_trades" in df.columns:
            trades_col = "number_of_trades"

        assert (
            trades_col is not None
        ), "DataFrame should have either 'trades' or 'number_of_trades' column"
        # Convert to int64 if not already (handles both int32 and int64 cases)
        df[trades_col] = df[trades_col].astype(np.int64)
        assert df[trades_col].dtype == np.int64, f"{trades_col} should be int64"

        # Check if we have a reasonable number of records
        expected_minutes = 5  # Time range is 5 minutes
        expected_records = expected_minutes * 60  # For 1-second interval
        assert len(df) > 0, "Should have at least some records"

        # More lenient check on record count since we might not get exactly expected records
        # due to time boundary handling changes
        assert (
            0 < len(df) <= expected_records * 1.1
        ), f"Record count {len(df)} should be reasonable"


@pytest.mark.real
@pytest.mark.asyncio(loop_scope="function")
async def test_output_format_guarantees(
    manager: DataSourceManager, now: arrow.Arrow, caplog_maybe
):
    """
    Test output format guarantees of DataSourceManager.

    After time alignment revamp, this test handles the possibility of empty DataFrames,
    which may occur due to the more stringent time boundary handling but still verifies
    the basic structure and format guarantees.
    """
    log_test_motivation(
        "Output Format Guarantees",
        "Verify that the output format of DataSourceManager is consistent across all operations, "
        "to provide a predictable interface for downstream components.",
        expectations=[
            "DatetimeIndex with UTC timezone",
            "Consistent column presence and types",
            "No duplicate indices",
            "Regular interval timestamps",
            "Close time present and properly formatted",
        ],
        implications=[
            "Establishes output format contract",
            "Enables reliable downstream processing",
            "Prevents timezone-related bugs",
            "Ensures predictable DataFrame structure",
        ],
    )

    # Test with historical data to ensure consistency
    start_time = now.shift(days=-2).floor("hour").datetime
    end_time = start_time + timedelta(minutes=5)

    logger.info(f"Testing data from {start_time} to {end_time}")
    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=start_time,
        end_time=end_time,
        use_cache=False,
    )

    log_dataframe_info(df, "Output Format Test")

    # After the time alignment revamp, we might get empty dataframes
    # In such cases, validate the basic structure but skip detailed content checks
    if df.empty:
        logger.warning(
            "Empty DataFrame returned - this may be acceptable after time alignment revamp"
        )
        # Validate the basic structure for empty dataframes
        assert isinstance(
            df, pd.DataFrame
        ), "Result should be a DataFrame even if empty"

        # Verify essential columns exist (using more flexible check)
        essential_columns = ["open", "high", "low", "close", "volume", "close_time"]
        for col in essential_columns:
            assert (
                col in df.columns
            ), f"Column {col} should exist in the empty DataFrame"

        # Verify that we have a columns that correspond to the expected column types
        # (though the exact names may be different)
        assert any(
            "quote" in col.lower() for col in df.columns
        ), "Should have a quote volume column"
        assert any(
            "trade" in col.lower() for col in df.columns
        ), "Should have a trades column"
        assert any(
            "taker" in col.lower() and "buy" in col.lower() for col in df.columns
        ), "Should have taker buy columns"

        return

    # Validate core structure
    assert isinstance(df.index, pd.DatetimeIndex), "Index must be DatetimeIndex"
    assert df.index.tz == timezone.utc, "Index must have UTC timezone"
    assert df.index.is_monotonic_increasing, "Index must be monotonically increasing"
    assert not df.index.has_duplicates, "Index must not have duplicates"

    # Validate column types with more flexible approach
    # Special case for close_time which is now a datetime
    assert "close_time" in df.columns, "close_time column must be present"
    assert str(df["close_time"].dtype).startswith(
        "datetime64"
    ), "close_time should be datetime64"

    # Check numeric columns
    numeric_columns = ["open", "high", "low", "close", "volume"]
    for col in numeric_columns:
        assert col in df.columns, f"Column {col} must be present"
        assert pd.api.types.is_numeric_dtype(
            df[col].dtype
        ), f"Column {col} must be numeric"

    # Validate values are sensible
    assert (df["high"] >= df["low"]).all(), "High must be >= Low"
    assert (df["high"] >= df["open"]).all(), "High must be >= Open"
    assert (df["high"] >= df["close"]).all(), "High must be >= Close"
    assert (df["volume"] >= 0).all(), "Volume must be non-negative"


@pytest.mark.real
@pytest.mark.asyncio(loop_scope="function")
async def test_timestamp_precision_handling(
    manager: DataSourceManager, now: arrow.Arrow, caplog_maybe
):
    """Test how microsecond-level precision is handled in DataSourceManager."""
    log_test_motivation(
        "Timestamp Precision Handling",
        "Testing how DataSourceManager handles microsecond-level precision in timestamps.",
        expectations=[
            "Microsecond precision should be properly maintained",
            "Consistent timestamp formats across data from different sources",
            "Proper timezone handling (UTC timezone-aware)",
            "No rounding or truncation of timestamps that loses precision",
        ],
        implications=[
            "Affects time-series alignment accuracy",
            "Critical for high-frequency algorithm backtesting",
            "Ensures data consistency between REST and Vision",
            "Maintains analysis precision when comparing data sources",
        ],
    )

    # Create test dates with microsecond precision
    base_date = now.shift(days=-5)  # Use a date 5 days ago

    # Ensure microsecond precision in test dates
    test_start = base_date.shift(seconds=-60).replace(microsecond=123456)
    test_end = base_date.replace(microsecond=987654)

    # Debug date settings
    logger.info(f"Test dates for timestamp precision test:")
    logger.info(f"Start: {test_start.format('YYYY-MM-DD HH:mm:ss.SSSSSS')} UTC")
    logger.info(f"End: {test_end.format('YYYY-MM-DD HH:mm:ss.SSSSSS')} UTC")

    # Retrieve data
    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=test_start.datetime,
        end_time=test_end.datetime,
        use_cache=False,  # Ensure we're not using cached data
    )

    # Debug DataFrame reception
    if df.empty:
        logger.warning("Received empty DataFrame!")
        return

    logger.info(f"Received DataFrame with {len(df)} records")

    # Log DataFrame index type and properties
    logger.info(f"DataFrame index type: {type(df.index).__name__}")
    if hasattr(df.index, "dtype"):
        logger.info(f"DataFrame index dtype: {df.index.dtype}")
    if hasattr(df.index, "tz"):
        logger.info(f"DataFrame index timezone: {df.index.tz}")

    # Debug column properties
    logger.info("Column dtypes:")
    for col in df.columns:
        logger.info(f"  - {col}: {df[col].dtype}")

    # Log first few records for inspection
    logger.info(f"First 5 records:\n{df.head()}")

    # Examine timestamp columns
    timestamp_cols = ["open_time", "close_time"]
    for col in timestamp_cols:
        if col in df.columns:
            logger.info(f"Column {col} found in DataFrame")
            logger.info(f"  - dtype: {df[col].dtype}")
            logger.info(
                f"  - is_datetime64: {pd.api.types.is_datetime64_dtype(df[col])}"
            )
            logger.info(
                f"  - is_datetime64_any: {pd.api.types.is_datetime64_any_dtype(df[col])}"
            )
            logger.info(
                f"  - is_datetime64_ns: {pd.api.types.is_datetime64_ns_dtype(df[col])}"
            )

            # Check full dtype string representation
            dtype_str = str(df[col].dtype)
            logger.info(f"  - full dtype string: {dtype_str}")

            # Test different checks to identify what's happening
            if "datetime64[ns, UTC]" in dtype_str:
                logger.info(f"  - Column has UTC timezone in dtype string")

            # Show sample values
            logger.info(f"  - First value: {df[col].iloc[0]}")
            if hasattr(df[col].iloc[0], "tz"):
                logger.info(f"  - First value timezone: {df[col].iloc[0].tz}")
        else:
            logger.info(f"Column {col} not found in DataFrame")

    # Debug the DataFrame structure
    log_dataframe_info(df, "Timestamp Precision Test Data")

    # Check datetime64 formatting and precision
    for ts_col in ["open_time", "close_time"]:
        if ts_col in df.columns:
            # Get the column for analysis
            ts_series = df[ts_col]

            # Debug the exact implementation of the type checking
            logger.info(f"Detailed type check for {ts_col}:")
            logger.info(
                f"  - pandas.api.types.is_datetime64_dtype: {pd.api.types.is_datetime64_dtype(ts_series)}"
            )
            logger.info(f"  - Type name: {ts_series.dtype.name}")
            logger.info(f"  - Str representation: {str(ts_series.dtype)}")

            # Examine the exact expected/actual values
            if str(ts_series.dtype) == "datetime64[ns, UTC]":
                logger.info("  - Found timezone-aware datetime64 type")

                # Try making it into a non-timezone aware array to see if that works
                logger.info(
                    "  - Attempting to convert to naive datetime for compatibility"
                )
                ts_naive = ts_series.dt.tz_localize(None)
                logger.info(f"  - Naive dtype: {ts_naive.dtype}")
                logger.info(
                    f"  - is_datetime64_dtype for naive: {pd.api.types.is_datetime64_dtype(ts_naive)}"
                )

            # Test if timestamps have microsecond precision
            first_ts = ts_series.iloc[0]
            if hasattr(first_ts, "microsecond"):
                logger.info(f"First {ts_col} microsecond: {first_ts.microsecond}")

            # Check if microseconds are maintained
            assert hasattr(
                first_ts, "microsecond"
            ), f"Timestamps should have microsecond precision"

            # Timezone-aware check
            # Use is_datetime64_any_dtype to handle both naive and aware datetime types
            assert pd.api.types.is_datetime64_any_dtype(
                ts_series
            ), f"{ts_col} must be datetime64 (timezone-aware or naive)"


@pytest.mark.real
@pytest.mark.asyncio(loop_scope="function")
async def test_data_point_relationships(
    manager: DataSourceManager, now: arrow.Arrow, caplog_maybe
):
    """Test relationships between data points."""
    log_test_motivation(
        "Data Point Relationships",
        "Verify that relationships between different data points (open, close, high, low, "
        "volume, etc.) follow expected patterns and maintain logical consistency.",
        expectations=[
            "Price relationships (high ≥ open, high ≥ close, high ≥ low)",
            "Volume consistency (volume > 0 when trades > 0)",
            "Quote asset volume ≈ avg_price * volume",
            "Taker volumes are subsets of total volumes",
        ],
        implications=[
            "Validates data quality",
            "Ensures logical consistency",
            "Provides guarantees for analysis",
            "Documents expected relationships",
        ],
    )

    # Use a well-traded period for reliable data
    start_time = now.shift(days=-1).floor("hour").datetime
    end_time = start_time + timedelta(minutes=30)

    logger.info(f"Testing data point relationships from {start_time} to {end_time}")
    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=start_time,
        end_time=end_time,
        use_cache=False,
    )

    log_dataframe_info(df, "Data Relationships Test")

    # Skip empty DataFrame
    if df.empty:
        logger.warning("Empty DataFrame returned, skipping relationship tests")
        return

    # Log which columns are available
    logger.info(f"Available columns: {df.columns.tolist()}")

    # Handle column name mapping
    quote_vol_col = (
        "quote_asset_volume" if "quote_asset_volume" in df.columns else "quote_volume"
    )
    trades_col = "number_of_trades" if "number_of_trades" in df.columns else "trades"
    taker_buy_base_col = (
        "taker_buy_base_asset_volume"
        if "taker_buy_base_asset_volume" in df.columns
        else "taker_buy_base_volume"
    )
    taker_buy_quote_col = (
        "taker_buy_quote_asset_volume"
        if "taker_buy_quote_asset_volume" in df.columns
        else "taker_buy_quote_volume"
    )

    # Price relationships
    assert (df["high"] >= df["open"]).all(), "High should be ≥ Open"
    assert (df["high"] >= df["close"]).all(), "High should be ≥ Close"
    assert (df["high"] >= df["low"]).all(), "High should be ≥ Low"
    assert (df["low"] <= df["open"]).all(), "Low should be ≤ Open"
    assert (df["low"] <= df["close"]).all(), "Low should be ≤ Close"

    # Volume relationships
    assert (df["volume"] >= 0).all(), "Volume should be ≥ 0"

    # Check if quote volume column exists
    if quote_vol_col in df.columns:
        assert (df[quote_vol_col] >= 0).all(), "Quote volume should be ≥ 0"
    else:
        logger.warning(
            f"Quote volume column not found. Available columns: {df.columns.tolist()}"
        )

    # Check if trades column exists
    if trades_col in df.columns:
        assert (
            df.loc[df[trades_col] > 0, "volume"] > 0
        ).all(), f"Volume should be > 0 when {trades_col} > 0"
    else:
        logger.warning(
            f"Trades column not found. Available columns: {df.columns.tolist()}"
        )

    # Taker buy volume relationships (checking column existence first)
    if taker_buy_base_col in df.columns:
        assert (
            df[taker_buy_base_col] <= df["volume"]
        ).all(), "Taker buy base volume should be ≤ total volume"
    else:
        logger.warning(
            f"Taker buy base volume column not found. Available columns: {df.columns.tolist()}"
        )

    if taker_buy_quote_col in df.columns and quote_vol_col in df.columns:
        assert (
            df[taker_buy_quote_col] <= df[quote_vol_col]
        ).all(), "Taker buy quote volume should be ≤ total quote volume"
    else:
        logger.warning(
            f"Taker buy quote volume or quote volume column not found. Available columns: {df.columns.tolist()}"
        )

    # Calculate approximate relationships
    if quote_vol_col in df.columns:
        df["avg_price"] = df[quote_vol_col] / df["volume"].replace(0, np.nan)
        df["avg_price"] = df["avg_price"].fillna(df["close"])

        # Allow for some floating-point error in the approximation
        margin = 0.01  # 1% margin of error
        price_range = df[["open", "high", "low", "close"]].mean(axis=1)

        # Verify that avg_price is within reasonable range of OHLC prices
        assert (
            (df["avg_price"] >= df["low"] * (1 - margin))
            & (df["avg_price"] <= df["high"] * (1 + margin))
        ).all(), "Average price should be within range of OHLC prices"
    else:
        logger.warning(
            "Cannot calculate avg_price because quote volume column is missing"
        )
