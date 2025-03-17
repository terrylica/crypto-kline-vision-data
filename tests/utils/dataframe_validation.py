#!/usr/bin/env python
"""Validation utilities for DataSourceManager tests.

This module contains common validation functions used across multiple test files
to ensure consistent validation behavior and reduce code duplication.
"""

import pandas as pd
from datetime import timezone
from typing import Any, Optional

from utils.logger_setup import get_logger

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


def validate_dataframe_structure(
    df: pd.DataFrame, allow_empty: bool = True, name: str = "DataFrame"
) -> None:
    """Validate DataFrame structure with detailed logging.

    Args:
        df: DataFrame to validate
        allow_empty: Whether empty DataFrames are acceptable
        name: Name of the DataFrame for logging purposes

    Raises:
        AssertionError: If validation fails
    """
    logger.info(
        "╔═══════════════════════════════════════════════════════════════════════════"
    )
    logger.info(f"║ Structure Validation: {name}")
    logger.info(
        "╠═══════════════════════════════════════════════════════════════════════════"
    )

    # Empty Check
    if df.empty and not allow_empty:
        logger.error("║ ❌ DataFrame is empty when it should contain data")
        raise AssertionError(f"{name} should not be empty")
    elif df.empty:
        logger.info("║ ℹ️  DataFrame is empty (allowed)")
        logger.info(
            "╚═══════════════════════════════════════════════════════════════════════════"
        )
        return

    # Index Validation
    logger.info("║ Index Validation:")
    if isinstance(df.index, pd.DatetimeIndex):
        logger.info("║ ✓ Index is DatetimeIndex")
    else:
        logger.error(f"║ ❌ Index is {type(df.index).__name__}, expected DatetimeIndex")
        raise AssertionError(f"{name} index should be DatetimeIndex")

    if df.index.tz == timezone.utc:
        logger.info("║ ✓ Timezone is UTC")
    else:
        logger.error(f"║ ❌ Timezone is {df.index.tz}, expected UTC")
        raise AssertionError(f"{name} index should be UTC")

    if df.index.is_monotonic_increasing:
        logger.info("║ ✓ Index is monotonically increasing")
    else:
        logger.error("║ ❌ Index is not monotonically increasing")
        raise AssertionError(f"{name} index should be monotonically increasing")

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


def log_dataframe_info(
    df: pd.DataFrame, source: str, to_arrow_fn: Optional[Any] = None
) -> None:
    """Log detailed DataFrame information for analysis.

    Args:
        df: DataFrame to analyze
        source: Source description for the DataFrame
        to_arrow_fn: Optional function to convert timestamps to Arrow objects
    """
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

    # Handle timestamp conversion based on whether to_arrow_fn is provided
    if to_arrow_fn and not df.empty:
        first_ts = to_arrow_fn(df.index[0])
        last_ts = to_arrow_fn(df.index[-1])
        logger.info(
            f"║   • 🔵 First Record: {first_ts.format('YYYY-MM-DD HH:mm:ss')} UTC"
        )
        logger.info(
            f"║   • 🔴 Last Record: {last_ts.format('YYYY-MM-DD HH:mm:ss')} UTC"
        )
        logger.info(f"║   • ⌛ Total Duration: {last_ts - first_ts}")
    elif not df.empty:
        logger.info(f"║   • 🔵 First Record: {df.index[0]}")
        logger.info(f"║   • 🔴 Last Record: {df.index[-1]}")
        logger.info(f"║   • ⌛ Total Duration: {df.index[-1] - df.index[0]}")

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
