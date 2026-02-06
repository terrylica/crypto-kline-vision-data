#!/usr/bin/env python3
# Memory optimization: Gap dataclass uses int64 milliseconds internally
# for 85% memory reduction vs pd.Timestamp objects
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""Gap Detector - Robust time series gap detection.

This module provides a clean, streamlined implementation for detecting gaps in time-series data
based on expected intervals defined in market_constraints.py.
"""

import sys
from dataclasses import dataclass
from typing import Any

import pandas as pd
from rich import print

from data_source_manager.utils.config import MIN_ROWS_FOR_GAP_DETECTION
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import Interval


@dataclass(slots=True)
class Gap:
    """Represents a detected gap in time series data.

    Uses int64 milliseconds internally for memory efficiency (85% reduction).
    Property accessors provide backward-compatible pd.Timestamp/pd.Timedelta access.
    """

    start_time_ms: int  # Start timestamp as Unix milliseconds
    end_time_ms: int  # End timestamp as Unix milliseconds
    duration_ms: int  # Duration in milliseconds
    missing_points: int  # Number of missing data points
    crosses_day_boundary: bool = False  # Whether the gap crosses a day boundary

    @property
    def start_time(self) -> pd.Timestamp:
        """Get start time as pd.Timestamp for backward compatibility."""
        return pd.Timestamp(self.start_time_ms, unit="ms", tz="UTC")

    @property
    def end_time(self) -> pd.Timestamp:
        """Get end time as pd.Timestamp for backward compatibility."""
        return pd.Timestamp(self.end_time_ms, unit="ms", tz="UTC")

    @property
    def duration(self) -> pd.Timedelta:
        """Get duration as pd.Timedelta for backward compatibility."""
        return pd.Timedelta(milliseconds=self.duration_ms)


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

    # Extract gaps (boolean indexing already returns a copy, no need for .copy())
    gaps_df = df_sorted[gaps_mask]

    # Pre-calculate expected interval in milliseconds
    expected_interval_ms = int(expected_interval.total_seconds() * 1000)

    # Prepare gap list using itertuples for performance (avoids Series creation)
    # Convert pd.Timestamp to int64 milliseconds for memory efficiency
    gaps = [
        Gap(
            start_time_ms=int(getattr(row, time_column).value // 1_000_000),  # nanoseconds to milliseconds
            end_time_ms=int(row.next_time.value // 1_000_000),
            duration_ms=int(row.time_diff.value // 1_000_000) - expected_interval_ms,
            missing_points=int((row.time_diff / expected_interval) - 1),
            crosses_day_boundary=row.crosses_day_boundary,
        )
        for row in gaps_df.itertuples(index=False)
    ]

    # Compile statistics using single-pass for gap metrics (avoids 4 separate iterations)
    day_boundary_gaps = 0
    max_gap_duration_ms = 0
    for gap in gaps:
        if gap.crosses_day_boundary:
            day_boundary_gaps += 1
        max_gap_duration_ms = max(max_gap_duration_ms, gap.duration_ms)

    stats = {
        "total_gaps": len(gaps),
        "day_boundary_gaps": day_boundary_gaps,
        "non_boundary_gaps": len(gaps) - day_boundary_gaps,
        "max_gap_duration": pd.Timedelta(milliseconds=max_gap_duration_ms) if gaps else pd.Timedelta(0),
        "total_records": len(df),
        "first_timestamp": (df_sorted[time_column].min() if not df_sorted.empty else None),
        "last_timestamp": df_sorted[time_column].max() if not df_sorted.empty else None,
        "timespan_hours": (
            (df_sorted[time_column].max() - df_sorted[time_column].min()).total_seconds() / 3600 if not df_sorted.empty else 0
        ),
    }

    return gaps, stats
