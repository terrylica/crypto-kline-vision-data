#!/usr/bin/env python3
"""
Utility module for timestamp handling in Vision API data.

This module provides functions for processing timestamp columns from Binance Vision API data,
ensuring proper conversion and preservation of semantic meaning.
"""

import re

import pandas as pd

from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import Interval
from data_source_manager.utils.time_utils import detect_timestamp_unit


def process_timestamp_columns(df: pd.DataFrame, interval_str: str) -> pd.DataFrame:
    """Process timestamp columns in the dataframe, handling various formats.

    This method preserves the exact timestamps from the raw data without any shifting:
    - open_time represents the BEGINNING of a candle period
    - close_time represents the END of the candle period

    Args:
        df: DataFrame with timestamp columns to process
        interval_str: Interval string (e.g., "1s", "1m", "1h")

    Returns:
        DataFrame with processed timestamp columns
    """
    if df.empty:
        return df

    try:
        # Check timestamp format if dataframe has rows
        if len(df) > 0:
            # Debug: Log first few raw rows to track data through the pipeline
            logger.debug(f"[TIMESTAMP TRACE] Input data to process_timestamp_columns has {len(df)} rows")
            for i in range(min(3, len(df))):
                logger.debug(f"[TIMESTAMP TRACE] Raw row {i}: open_time={df.iloc[i, 0]}, close={df.iloc[i, 4]}")

            first_ts = df.iloc[0, 0]  # First timestamp in first column
            last_ts = df.iloc[-1, 0] if len(df) > 1 else first_ts

            logger.debug(f"First raw timestamp detected: {first_ts}")
            logger.debug(f"Last raw timestamp detected: {last_ts}")

            try:
                # Detect timestamp unit using the standardized function from utils.time_utils
                timestamp_unit = detect_timestamp_unit(first_ts)

                # Log timestamp details for debugging
                logger.debug(f"First timestamp: {first_ts} ({timestamp_unit})")
                if len(df) > 1:
                    last_ts = df.iloc[-1, 0]
                    logger.debug(f"Last timestamp: {last_ts} ({timestamp_unit})")

                # Convert timestamps to datetime, preserving their semantic meaning:
                # - open_time (1st column) is the BEGINNING of the candle period
                # - close_time (7th column) is the END of the candle period
                if "open_time" in df.columns:
                    df["open_time"] = pd.to_datetime(df["open_time"], unit=timestamp_unit, utc=True)
                    logger.debug(f"Converted open_time: first value = {df['open_time'].iloc[0]} (BEGINNING of candle)")
                    # Debug: Log first few converted timestamps to track processing
                    for i in range(min(3, len(df))):
                        logger.debug(f"[TIMESTAMP TRACE] Converted row {i}: open_time={df['open_time'].iloc[i]}, close={df.iloc[i, 4]}")

                if "close_time" in df.columns:
                    df["close_time"] = pd.to_datetime(df["close_time"], unit=timestamp_unit, utc=True)
                    logger.debug(f"Converted close_time: first value = {df['close_time'].iloc[0]} (END of candle)")

                # Verify timestamp semantics are preserved (for debugging)
                if "open_time" in df.columns and "close_time" in df.columns and len(df) > 0:
                    first_open = df["open_time"].iloc[0]
                    first_close = df["close_time"].iloc[0]
                    time_diff = (first_close - first_open).total_seconds()

                    # Calculate expected difference based on interval
                    # For 1s interval, close should be 0.999 seconds after open
                    # For 1m interval, close should be 59.999 seconds after open, etc.
                    expected_diff = get_interval_seconds(interval_str) - 0.001

                    logger.debug(
                        f"Time difference between first open_time and close_time: {time_diff:.3f}s "
                        f"(expected ~{expected_diff:.3f}s for {interval_str} interval)"
                    )

                    # Verify the time difference is within expected range
                    # Allow for a small tolerance to account for precision differences
                    tolerance = 0.1  # 100ms tolerance
                    if abs(time_diff - expected_diff) > tolerance:
                        logger.warning(
                            f"Unexpected time difference between open_time and close_time: "
                            f"{time_diff:.3f}s vs expected {expected_diff:.3f}s for {interval_str} interval. "
                            f"This could indicate a timestamp interpretation issue."
                        )
                    else:
                        logger.debug(
                            f"Time difference between open_time and close_time is as expected "
                            f"({time_diff:.3f}s) for {interval_str} interval."
                        )

                    logger.debug(
                        "Timestamps converted preserving their semantic meaning: open_time=BEGINNING of candle, close_time=END of candle"
                    )

            except ValueError as e:
                logger.warning(f"Error detecting timestamp unit: {e}")
                # Fall back to default handling with microseconds as unit
                logger.warning("Falling back to microseconds as the timestamp unit")
                if "open_time" in df.columns:
                    df["open_time"] = pd.to_datetime(df["open_time"], unit="us", utc=True)
                    logger.debug(
                        f"Converted open_time using fallback method: first value = {df['open_time'].iloc[0]} (BEGINNING of candle)"
                    )
                if "close_time" in df.columns:
                    df["close_time"] = pd.to_datetime(df["close_time"], unit="us", utc=True)
                    logger.debug(f"Converted close_time using fallback method: first value = {df['close_time'].iloc[0]} (END of candle)")

        # Debug: Log output from timestamp processing
        logger.debug(f"[TIMESTAMP TRACE] After process_timestamp_columns: {len(df)} rows")
        if len(df) > 0 and "open_time" in df.columns:
            for i in range(min(3, len(df))):
                logger.debug(f"[TIMESTAMP TRACE] Processed row {i}: open_time={df['open_time'].iloc[i]}, close={df.iloc[i, 4]}")

    except Exception as e:
        logger.error(f"Error processing timestamp columns: {e}")

    return df


def get_interval_seconds(interval: str) -> int:
    """Get interval duration in seconds from interval string.

    This method handles converting string intervals directly to seconds
    without requiring the MarketInterval enum object.

    Args:
        interval: Interval string (e.g., "1s", "1m", "1h")

    Returns:
        Number of seconds in the interval
    """
    # Parse interval value and unit
    match = re.match(r"(\d+)([smhdwM])", interval)
    if not match:
        raise ValueError(f"Invalid interval format: {interval}")

    num, unit = match.groups()
    num = int(num)

    # Define multipliers for each unit
    multipliers = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 604800,
        "M": 2592000,  # Approximate - using 30 days
    }

    if unit not in multipliers:
        raise ValueError(f"Unknown interval unit: {unit}")

    return num * multipliers[unit]


def parse_interval(interval_str: str) -> Interval:
    """Parse and validate interval string against market_constraints.Interval.

    Args:
        interval_str: Interval string (e.g., "1m", "1h")

    Returns:
        Parsed Interval enum

    Raises:
        ValueError: If interval is invalid or not supported
    """
    try:
        # Try to find the interval enum by value
        interval_obj = next((i for i in Interval if i.value == interval_str), None)
        if interval_obj is None:
            # Try by enum name (upper case with _ instead of number)
            try:
                interval_obj = Interval[interval_str.upper()]
            except KeyError as e:
                raise ValueError(f"Invalid interval: {interval_str}") from e

        logger.debug(f"Using interval {interval_obj.name} ({interval_obj.value})")

        return interval_obj
    except Exception as e:
        logger.error(f"Error parsing interval {interval_str}: {e}")
        # Default to 1s as a failsafe
        return Interval.SECOND_1
