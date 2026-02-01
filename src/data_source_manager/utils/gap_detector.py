#!/usr/bin/env python3
# polars-exception: Gap detection relies on pd.Timedelta arithmetic and
# pd.Timestamp operations for time series gap analysis
"""Gap Detector - Robust time series gap detection.

This module provides a clean, streamlined implementation for detecting gaps in time-series data
based on expected intervals defined in market_constraints.py.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from rich import print

from data_source_manager.utils.config import MIN_ROWS_FOR_GAP_DETECTION
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import Interval


@dataclass
class Gap:
    """Represents a detected gap in time series data."""

    start_time: pd.Timestamp  # Start timestamp of the gap
    end_time: pd.Timestamp  # End timestamp of the gap
    duration: pd.Timedelta  # Duration of the gap
    missing_points: int  # Number of missing data points
    crosses_day_boundary: bool = False  # Whether the gap crosses a day boundary


def detect_gaps(
    df: pd.DataFrame,
    interval: Interval,
    time_column: str = "open_time",
    gap_threshold: float = 0.3,  # 30% threshold
    day_boundary_threshold: float = 1.5,  # Use higher threshold for day boundaries
    enforce_min_span: bool = True,  # Enforce minimum timespan requirement
) -> tuple[list[Gap], dict[str, Any]]:
    """Detect gaps in time series data based on a fixed interval.

    This function uses a streamlined approach to find gaps in time-series data:
    1. It calculates the expected interval based on the provided Interval enum
    2. It identifies gaps where the actual time difference exceeds the expected
       interval by more than the specified threshold
    3. It calculates statistics about the gaps found

    Args:
        df: DataFrame containing time series data
        interval: Interval enum from market_constraints.py defining the expected time interval
        time_column: Name of the timestamp column in the DataFrame
        gap_threshold: Threshold as a fraction (0.3 = 30%) above the expected
            interval to consider as a gap
        day_boundary_threshold: Separate threshold for day boundary transitions
            (default 1.5 = 150% for greater tolerance)
        enforce_min_span: If True, require dataset to span at least 23 hours
            to prevent analyzing individual daily files

    Returns:
        Tuple containing:
        - List of Gap objects representing each detected gap
        - Dictionary with statistics about the gaps

    Raises:
        SystemExit: If the data doesn't meet the minimum span requirement or if interval is not an Interval enum
    """
    # Strictly validate that interval is an Interval enum from market_constraints.py
    if not isinstance(interval, Interval):
        error_msg = (
            f"CRITICAL ERROR: Invalid interval type: {type(interval)}. "
            f"Gap detection requires a valid Interval enum from market_constraints.py. "
            f"Available intervals: {', '.join([i.value for i in Interval])}"
        )
        logger.critical(error_msg)
        print(f"[bold red]{error_msg}[/bold red]")
        sys.exit(1)

    if df.empty or len(df) < MIN_ROWS_FOR_GAP_DETECTION:
        logger.warning("DataFrame has fewer than 2 rows, cannot detect gaps")
        return [], {"total_gaps": 0, "total_records": len(df)}

    # Enforce minimum timespan requirement (23 hours) to ensure proper data merging
    # This prevents analysis of single daily files which would produce misleading gaps
    if enforce_min_span:
        min_hours_span = 23  # Minimum required timespan in hours
        time_span = df[time_column].max() - df[time_column].min()
        span_hours = time_span.total_seconds() / 3600

        if span_hours < min_hours_span:
            warning_msg = (
                f"WARNING: Input data spans only {span_hours:.2f} hours. "
                f"Gap detection normally requires at least {min_hours_span} hours of continuous data. "
                f"This may produce misleading gap analysis results."
            )
            logger.warning(warning_msg)
            print(f"[bold yellow]{warning_msg}[/bold yellow]")
            # Continue with analysis instead of exiting

    # Ensure DataFrame is sorted by time
    df_sorted = df.sort_values(time_column).reset_index(drop=True)

    # Get expected interval in seconds
    expected_seconds = interval.to_seconds()
    expected_interval = pd.Timedelta(seconds=expected_seconds)

    # Calculate time differences
    df_sorted["next_time"] = df_sorted[time_column].shift(-1)
    df_sorted["time_diff"] = df_sorted["next_time"] - df_sorted[time_column]

    # Calculate threshold for gap detection
    gap_interval_threshold = expected_interval * (1 + gap_threshold)

    # Identify day boundary transitions (where date changes between consecutive records)
    df_sorted["curr_date"] = df_sorted[time_column].dt.date
    df_sorted["next_date"] = df_sorted["next_time"].dt.date
    df_sorted["crosses_day_boundary"] = df_sorted["curr_date"] != df_sorted["next_date"]

    # Apply different thresholds based on whether the transition crosses a day boundary
    boundary_mask = df_sorted["crosses_day_boundary"]
    regular_mask = ~boundary_mask

    # For day boundaries, use a more tolerant threshold
    day_boundary_threshold_value = expected_interval * (1 + day_boundary_threshold)

    # Create a mask for gaps that combines both conditions
    gaps_mask = (regular_mask & (df_sorted["time_diff"] > gap_interval_threshold)) | (
        boundary_mask & (df_sorted["time_diff"] > day_boundary_threshold_value)
    )

    # Extract gaps
    gaps_df = df_sorted[gaps_mask].copy()

    # Prepare gap list using itertuples for performance (avoids Series creation)
    gaps = [
        Gap(
            start_time=getattr(row, time_column),
            end_time=row.next_time,
            duration=row.time_diff - expected_interval,
            missing_points=int((row.time_diff / expected_interval) - 1),
            crosses_day_boundary=row.crosses_day_boundary,
        )
        for row in gaps_df.itertuples(index=False)
    ]

    # Compile statistics
    stats = {
        "total_gaps": len(gaps),
        "day_boundary_gaps": sum(1 for gap in gaps if gap.crosses_day_boundary),
        "non_boundary_gaps": sum(1 for gap in gaps if not gap.crosses_day_boundary),
        "max_gap_duration": max((gap.duration for gap in gaps), default=pd.Timedelta(0)),
        "total_records": len(df),
        "first_timestamp": (df_sorted[time_column].min() if not df_sorted.empty else None),
        "last_timestamp": df_sorted[time_column].max() if not df_sorted.empty else None,
        "timespan_hours": (
            (df_sorted[time_column].max() - df_sorted[time_column].min()).total_seconds() / 3600 if not df_sorted.empty else 0
        ),
    }

    return gaps, stats


def format_gaps_for_display(gaps: list[Gap]) -> pd.DataFrame:
    """Format gaps into a DataFrame for display or analysis.

    Args:
        gaps: List of Gap objects

    Returns:
        DataFrame with formatted gap information
    """
    if not gaps:
        return pd.DataFrame()

    data = [
        {
            "start_time": gap.start_time,
            "end_time": gap.end_time,
            "duration": gap.duration,
            "duration_seconds": gap.duration.total_seconds(),
            "missing_points": gap.missing_points,
            "crosses_day_boundary": gap.crosses_day_boundary,
        }
        for gap in gaps
    ]

    return pd.DataFrame(data)


def analyze_file_for_gaps(
    file_path: Path,
    interval: Interval,
    time_column: str = "open_time",
    time_unit: str = "ms",
    gap_threshold: float = 0.3,
    enforce_min_span: bool = True,
) -> tuple[list[Gap], dict[str, Any]]:
    """Analyze a CSV file for gaps in time series data.

    Args:
        file_path: Path to CSV file containing time series data
        interval: Interval enum from market_constraints.py defining the expected interval
        time_column: Name of timestamp column in CSV
        time_unit: Unit of timestamp ('ms' for milliseconds, 's' for seconds)
        gap_threshold: Threshold as a fraction above expected interval to consider as gap
        enforce_min_span: If True, require dataset to span at least 23 hours

    Returns:
        Tuple of (gaps, stats) where:
        - gaps is a list of Gap objects
        - stats is a dictionary with gap statistics

    Raises:
        SystemExit: If interval is not an Interval enum from market_constraints.py
    """
    # Strictly validate that interval is an Interval enum
    if not isinstance(interval, Interval):
        error_msg = (
            f"CRITICAL ERROR: Invalid interval type: {type(interval)}. "
            f"Gap detection requires a valid Interval enum from market_constraints.py. "
            f"Available intervals: {', '.join([i.value for i in Interval])}"
        )
        logger.critical(error_msg)
        print(f"[bold red]{error_msg}[/bold red]")
        sys.exit(1)

    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        return [], {"error": "File not found"}

    # Read CSV file
    try:
        df = pd.read_csv(file_path)

        # Convert timestamp column to datetime if it's not already
        if pd.api.types.is_numeric_dtype(df[time_column]):
            df[time_column] = pd.to_datetime(df[time_column], unit=time_unit, utc=True)

        # Detect gaps
        return detect_gaps(df, interval, time_column, gap_threshold, enforce_min_span=enforce_min_span)

    except (OSError, pd.errors.ParserError, ValueError, KeyError) as e:
        logger.error(f"Error analyzing file {file_path}: {e!s}")
        return [], {"error": str(e)}


def combine_daily_files(
    file_paths: list[Path],
    interval: Interval,
    time_column: str = "open_time",
    time_unit: str = "ms",
) -> pd.DataFrame:
    """Combine multiple daily files into a single DataFrame for gap analysis.

    Args:
        file_paths: List of paths to CSV files to combine
        interval: Interval enum from market_constraints.py for the data
        time_column: Name of timestamp column in CSV
        time_unit: Unit of timestamp ('ms' for milliseconds, 's' for seconds)

    Returns:
        Combined DataFrame with sorted timestamps

    Raises:
        SystemExit: If interval is not an Interval enum from market_constraints.py
    """
    # Strictly validate that interval is an Interval enum
    if not isinstance(interval, Interval):
        error_msg = (
            f"CRITICAL ERROR: Invalid interval type: {type(interval)}. "
            f"Gap detection requires a valid Interval enum from market_constraints.py. "
            f"Available intervals: {', '.join([i.value for i in Interval])}"
        )
        logger.critical(error_msg)
        print(f"[bold red]{error_msg}[/bold red]")
        sys.exit(1)

    if not file_paths:
        logger.error("No files provided to combine")
        return pd.DataFrame()

    dfs = []
    for path in file_paths:
        if not path.exists():
            logger.warning(f"File not found and will be skipped: {path}")
            continue

        try:
            df = pd.read_csv(path)

            # Convert timestamp column to datetime if it's not already
            if pd.api.types.is_numeric_dtype(df[time_column]):
                df[time_column] = pd.to_datetime(df[time_column], unit=time_unit, utc=True)

            dfs.append(df)
        except (OSError, pd.errors.ParserError, ValueError, KeyError) as e:
            logger.error(f"Error reading file {path}: {e!s}")

    if not dfs:
        return pd.DataFrame()

    # Combine all DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)

    # Sort by timestamp and remove duplicates
    return combined_df.sort_values(time_column).drop_duplicates(subset=[time_column])
