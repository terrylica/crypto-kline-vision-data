#!/usr/bin/env python
"""Integration tests for DataSourceManager focusing on time formats and precision.

System Under Test (SUT):
- core.data_source_manager.DataSourceManager
- utils.time_alignment (indirectly)
- core.vision_data_client.VisionDataClient (indirectly)
- core.market_data_client.EnhancedRetriever (indirectly)

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

from utils.logger_setup import get_logger
from core.data_source_manager import DataSourceManager
from utils.market_constraints import Interval, MarketType

logger = get_logger(__name__, "INFO", show_path=False)

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


@pytest_asyncio.fixture
async def manager():
    """Create DataSourceManager instance."""
    async with DataSourceManager(market_type=MarketType.SPOT) as mgr:
        yield mgr


@pytest.mark.real
@pytest.mark.asyncio
async def test_time_boundary_alignment(manager: DataSourceManager, now: arrow.Arrow):
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

        # Verify index properties
        assert isinstance(df.index, pd.DatetimeIndex), "Index must be DatetimeIndex"
        assert df.index.tz == timezone.utc, "Index must be UTC timezone-aware"

        # Check timestamp precision
        index_microseconds = df.index.astype(np.int64) % 1_000_000
        assert np.all(
            index_microseconds == 0
        ), "Timestamps should be aligned to seconds"


@pytest.mark.real
@pytest.mark.asyncio
async def test_input_format_handling(manager: DataSourceManager, now: arrow.Arrow):
    """Test how DataSourceManager handles different input time formats."""
    log_test_motivation(
        "Input Format Handling",
        "Testing DataSourceManager's ability to handle various input time formats "
        "and understanding what formats are accepted/rejected.",
        expectations=[
            "Accepts datetime objects",
            "Accepts pandas Timestamps",
            "Accepts Arrow objects (converted to datetime)",
            "Consistent timezone handling",
        ],
        implications=[
            "Documents supported input formats",
            "Validates timezone conversion logic",
            "Ensures format flexibility",
            "Defines input validation rules",
        ],
    )

    base_time = now.shift(days=-1)
    time_window = timedelta(minutes=5)

    # Test different input formats
    test_cases = [
        # Python datetime
        (base_time.datetime, (base_time + time_window).datetime),
        # Pandas Timestamp
        (
            pd.Timestamp(base_time.datetime),
            pd.Timestamp((base_time + time_window).datetime),
        ),
        # Arrow object (converted to datetime)
        (base_time.datetime, base_time.shift(minutes=5).datetime),
    ]

    for start_time, end_time in test_cases:
        logger.info(f"Testing input format: {type(start_time).__name__}")

        df = await manager.get_data(
            symbol=TEST_SYMBOL,
            interval=TEST_INTERVAL,
            start_time=start_time,
            end_time=end_time,
            use_cache=False,
        )

        log_dataframe_info(df, f"Format Test ({type(start_time).__name__})")

        # Verify consistent output format regardless of input
        assert isinstance(
            df.index, pd.DatetimeIndex
        ), "Output index must be DatetimeIndex"
        assert df.index.tz == timezone.utc, "Output must be UTC timezone-aware"

        # Verify data types of key columns
        assert df["open"].dtype == "float64", "Open price must be float64"
        assert df["volume"].dtype == "float64", "Volume must be float64"
        assert df["trades"].dtype == "int64", "Trades must be int64"


@pytest.mark.real
@pytest.mark.asyncio
async def test_output_format_guarantees(manager: DataSourceManager, now: arrow.Arrow):
    """Test output format consistency and guarantees."""
    log_test_motivation(
        "Output Format Guarantees",
        "Documenting the guaranteed properties of DataSourceManager output "
        "including data types, column presence, and format consistency.",
        expectations=[
            "Consistent column set",
            "Guaranteed data types",
            "Index properties",
            "Value ranges and constraints",
        ],
        implications=[
            "Establishes output contract",
            "Defines data quality guarantees",
            "Documents type system",
            "Specifies format requirements",
        ],
    )

    # Test with 1-second interval (only supported interval)
    base_time = now.shift(days=-2)

    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=base_time.datetime,
        end_time=base_time.shift(minutes=30).datetime,
        use_cache=False,
    )

    log_dataframe_info(df, "Output Test (1-second)")

    # Verify required columns and types
    required_columns = {
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "close_time": "int64",
        "quote_volume": "float64",
        "trades": "int64",
        "taker_buy_volume": "float64",
        "taker_buy_quote_volume": "float64",
    }

    for col, dtype in required_columns.items():
        assert col in df.columns, f"Required column {col} missing"
        assert str(df[col].dtype) == dtype, f"Column {col} must be {dtype}"

    # Verify value constraints
    assert (df["high"] >= df["low"]).all(), "High price must be >= low price"
    assert (df["volume"] >= 0).all(), "Volume cannot be negative"
    assert (df["trades"] >= 0).all(), "Trade count cannot be negative"

    # Verify timestamp alignment
    time_diffs = df.index.to_series().diff().dropna()
    assert (
        time_diffs == pd.Timedelta(seconds=1)
    ).all(), "Timestamps must be 1-second aligned"


@pytest.mark.real
@pytest.mark.asyncio
async def test_timestamp_precision_handling(
    manager: DataSourceManager, now: arrow.Arrow
):
    """Test timestamp precision handling and microsecond behavior."""
    log_test_motivation(
        "Timestamp Precision Handling",
        "Understanding how DataSourceManager handles timestamp precision at microsecond level "
        "and verifying the behavior of close_time vs index alignment.",
        expectations=[
            "Microsecond precision in close_time column",
            "Second-aligned index timestamps",
            "Consistent time delta between bars",
            "Proper handling of partial seconds",
        ],
        implications=[
            "Validates microsecond precision requirements",
            "Documents timestamp alignment behavior",
            "Ensures data point continuity",
            "Defines time precision standards",
        ],
    )

    base_time = now.shift(days=-2)
    test_cases = [
        # Case 1: Exact second boundary
        (base_time.floor("second"), base_time.floor("second").shift(seconds=5)),
        # Case 2: Sub-second precision
        (
            base_time.shift(microseconds=123456),
            base_time.shift(seconds=5, microseconds=654321),
        ),
    ]

    for start_time, end_time in test_cases:
        logger.info(
            f"Testing precision case: {start_time.format('YYYY-MM-DD HH:mm:ss.SSSSSS')} to "
            f"{end_time.format('YYYY-MM-DD HH:mm:ss.SSSSSS')}"
        )

        df = await manager.get_data(
            symbol=TEST_SYMBOL,
            interval=TEST_INTERVAL,
            start_time=start_time.datetime,
            end_time=end_time.datetime,
            use_cache=False,
        )

        log_dataframe_info(
            df,
            f"Precision Test ({start_time.format('ss.SSSSSS')} to {end_time.format('ss.SSSSSS')})",
        )

        # Verify close_time precision
        close_time_micros = pd.Series(df["close_time"].values).astype(str).str[-6:]
        assert not (
            close_time_micros == "000000"
        ).all(), "close_time should maintain microsecond precision"

        # Check index alignment
        index_microseconds = df.index.astype(np.int64) % 1_000_000
        assert (
            np.sum(index_microseconds) == 0
        ), "Index timestamps should be second-aligned"


@pytest.mark.real
@pytest.mark.asyncio
async def test_data_point_relationships(manager: DataSourceManager, now: arrow.Arrow):
    """Test relationships between data points and their properties."""
    log_test_motivation(
        "Data Point Relationships",
        "Examining the relationships between OHLCV data points and their properties, "
        "focusing on logical consistency and time-based patterns.",
        expectations=[
            "OHLC price relationships",
            "Quote volume derivation accuracy",
            "Taker volume constraints",
            "Time-based continuity",
        ],
        implications=[
            "Validates data integrity rules",
            "Documents derived field calculations",
            "Ensures logical consistency",
            "Defines relationship invariants",
        ],
    )

    base_time = now.shift(days=-2)
    df = await manager.get_data(
        symbol=TEST_SYMBOL,
        interval=TEST_INTERVAL,
        start_time=base_time.datetime,
        end_time=base_time.shift(minutes=5).datetime,
        use_cache=False,
    )

    log_dataframe_info(df, "Data Relationships Test")

    # Verify OHLC relationships
    assert (df["high"] >= df["open"]).all(), "High should be >= open"
    assert (df["high"] >= df["close"]).all(), "High should be >= close"
    assert (df["low"] <= df["open"]).all(), "Low should be <= open"
    assert (df["low"] <= df["close"]).all(), "Low should be <= close"

    # Verify volume relationships
    assert (
        df["taker_buy_volume"] <= df["volume"]
    ).all(), "Taker buy volume should be <= total volume"
    assert (
        df["taker_buy_quote_volume"] <= df["quote_volume"]
    ).all(), "Taker buy quote volume should be <= total quote volume"

    # Verify time-based patterns
    time_diffs = df.index.to_series().diff().dropna()
    assert (
        time_diffs == pd.Timedelta(seconds=1)
    ).all(), "Time difference between points should be exactly 1 second"
