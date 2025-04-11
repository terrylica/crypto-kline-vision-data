#!/usr/bin/env python
"""
Test script to verify timestamp format detection for 2025 microsecond precision.

This script creates sample data in both millisecond (pre-2025) and microsecond (2025+)
formats and verifies that the detection logic works properly.

Usage:
    python scripts/arrow_cache/test_timestamp_detection.py
"""

from utils.logger_setup import logger
from rich import print
import pandas as pd
import numpy as np
import pyarrow as pa
from pathlib import Path
import tempfile
import os
import sys

# Constants for timestamp detection
MILLISECOND_DIGITS = 13
MICROSECOND_DIGITS = 16


def detect_timestamp_unit(sample_ts):
    """Detect timestamp unit based on number of digits.

    Args:
        sample_ts: Sample timestamp value

    Returns:
        "us" for microseconds (16 digits)
        "ms" for milliseconds (13 digits)
    """
    digits = len(str(int(sample_ts)))

    if digits == MICROSECOND_DIGITS:
        return "us"
    elif digits == MILLISECOND_DIGITS:
        return "ms"
    else:
        return f"unknown ({digits} digits)"


def create_test_dataframes():
    """Create test dataframes with different timestamp formats.

    Returns:
        Tuple of (millisecond_df, microsecond_df)
    """
    # Create millisecond precision dataframe (pre-2025)
    ms_data = {
        "open_time": [
            1609459200000,
            1609459260000,
            1609459320000,
        ],  # 2021-01-01 00:00:00, 00:01:00, 00:02:00
        "close_time": [1609459259999, 1609459319999, 1609459379999],
        "open": [10.0, 11.0, 10.5],
        "high": [12.0, 11.5, 11.0],
        "low": [9.5, 10.8, 10.2],
        "close": [11.0, 10.5, 10.8],
        "volume": [100.0, 150.0, 120.0],
    }
    ms_df = pd.DataFrame(ms_data)

    # Create microsecond precision dataframe (2025+)
    us_data = {
        "open_time": [
            1609459200000000,
            1609459260000000,
            1609459320000000,
        ],  # Same times but microsecond precision
        "close_time": [1609459259999999, 1609459319999999, 1609459379999999],
        "open": [10.0, 11.0, 10.5],
        "high": [12.0, 11.5, 11.0],
        "low": [9.5, 10.8, 10.2],
        "close": [11.0, 10.5, 10.8],
        "volume": [100.0, 150.0, 120.0],
    }
    us_df = pd.DataFrame(us_data)

    return ms_df, us_df


def convert_to_datetime(df, unit):
    """Convert timestamp columns to datetime.

    Args:
        df: DataFrame with timestamp columns
        unit: Unit for timestamp conversion ('ms' or 'us')

    Returns:
        DataFrame with converted timestamps
    """
    result = df.copy()
    result["open_time"] = pd.to_datetime(result["open_time"], unit=unit, utc=True)
    result["close_time"] = pd.to_datetime(result["close_time"], unit=unit, utc=True)
    return result


def save_and_load_arrow(df, filename):
    """Save DataFrame to Arrow file and load it back.

    Args:
        df: DataFrame to save
        filename: File path to save to

    Returns:
        Loaded DataFrame
    """
    # Save to Arrow
    table = pa.Table.from_pandas(df)
    with pa.OSFile(filename, "wb") as f:
        with pa.RecordBatchFileWriter(f, table.schema) as writer:
            writer.write_table(table)

    # Load from Arrow
    with pa.OSFile(filename, "rb") as f:
        reader = pa.RecordBatchFileReader(f)
        loaded_table = reader.read_all()

    return loaded_table.to_pandas()


def run_test():
    """Run the timestamp detection and conversion test."""
    print(
        "[bold blue]===== Testing Timestamp Format Detection and Conversion =====[/bold blue]"
    )

    # Create test data
    ms_df, us_df = create_test_dataframes()

    # Test detection
    ms_format = detect_timestamp_unit(ms_df["open_time"].iloc[0])
    us_format = detect_timestamp_unit(us_df["open_time"].iloc[0])

    print(f"[green]Millisecond timestamp detected as: {ms_format}[/green]")
    print(f"[green]Microsecond timestamp detected as: {us_format}[/green]")

    # Convert to datetime
    ms_df_datetime = convert_to_datetime(ms_df, "ms")
    us_df_datetime = convert_to_datetime(us_df, "us")

    print("\n[bold]Sample datetime values:[/bold]")
    print(
        f"Millisecond precision first timestamp: {ms_df_datetime['open_time'].iloc[0]}"
    )
    print(
        f"Microsecond precision first timestamp: {us_df_datetime['open_time'].iloc[0]}"
    )

    # Create temp directory for Arrow files
    with tempfile.TemporaryDirectory() as temp_dir:
        ms_file = os.path.join(temp_dir, "millisecond.arrow")
        us_file = os.path.join(temp_dir, "microsecond.arrow")

        # Save and load both types
        ms_loaded = save_and_load_arrow(ms_df_datetime, ms_file)
        us_loaded = save_and_load_arrow(us_df_datetime, us_file)

        print("\n[bold]Arrow file roundtrip test:[/bold]")
        print(
            f"Millisecond data saved and loaded successfully: {len(ms_loaded) == len(ms_df)}"
        )
        print(
            f"Microsecond data saved and loaded successfully: {len(us_loaded) == len(us_df)}"
        )

        # Check timestamps
        print("\n[bold]Timestamp preservation test:[/bold]")
        print(f"Millisecond open_time type: {ms_loaded['open_time'].dtype}")
        print(f"Microsecond open_time type: {us_loaded['open_time'].dtype}")

        # Compare first values
        ms_first = ms_loaded["open_time"].iloc[0]
        us_first = us_loaded["open_time"].iloc[0]
        print(f"Millisecond first timestamp after roundtrip: {ms_first}")
        print(f"Microsecond first timestamp after roundtrip: {us_first}")

        # Check if timezone info is preserved
        print(f"\nTimezone preserved (millisecond): {ms_first.tzinfo is not None}")
        print(f"Timezone preserved (microsecond): {us_first.tzinfo is not None}")

    print("\n[bold blue]===== Test Complete =====[/bold blue]")


if __name__ == "__main__":
    run_test()
