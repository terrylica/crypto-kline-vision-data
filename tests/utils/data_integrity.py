#!/usr/bin/env python3
# Salvaged from utils_for_debug/data_integrity.py for FCP edge case testing.
"""Test utilities for analyzing data integrity in market data DataFrames."""

import pandas as pd

from data_source_manager.utils.loguru_setup import logger


def analyze_data_integrity(df, start_time, end_time, interval):
    """Analyze data integrity for the requested time period.

    This function checks for completeness, gaps, and other integrity issues
    in market data. It's useful for debugging data retrieval issues.

    Args:
        df: DataFrame with market data
        start_time: Requested start time
        end_time: Requested end time
        interval: Interval enum

    Returns:
        dict: Analysis results with expected records, actual records, missing records,
              and gap information if found
    """
    logger.debug(f"Analyzing data integrity for time range: {start_time} to {end_time}")

    # Calculate expected number of records
    interval_seconds = interval.to_seconds()
    expected_seconds = int((end_time - start_time).total_seconds())
    expected_records = (expected_seconds // interval_seconds) + 1

    # Get actual number of records
    actual_records = len(df)

    missing_records = expected_records - actual_records
    missing_percentage = (missing_records / expected_records) * 100 if expected_records > 0 else 0

    logger.debug(f"Expected records: {expected_records}")
    logger.debug(f"Actual records: {actual_records}")
    logger.debug(f"Missing records: {missing_records} ({missing_percentage:.2f}%)")

    # Find gaps in the time series
    if not df.empty:
        if "open_time" in df.columns:
            time_column = "open_time"
            # Ensure it's datetime type
            if not pd.api.types.is_datetime64_any_dtype(df[time_column]):
                logger.warning(f"Converting open_time to datetime - current type: {df[time_column].dtype}")
                try:
                    df[time_column] = pd.to_datetime(df[time_column], unit="ms", utc=True)
                except (ValueError, TypeError, OverflowError) as e:
                    logger.error(f"Error converting open_time to datetime: {e}")

            # Sort by time
            df_sorted = df.sort_values(time_column).copy()
        elif isinstance(df.index, pd.DatetimeIndex):
            time_column = df.index.name or "index"
            df_sorted = df.sort_index().reset_index().copy()
        else:
            logger.error("Cannot analyze time series: no datetime column or index found")
            return {
                "expected_records": expected_records,
                "actual_records": actual_records,
                "missing_records": missing_records,
                "missing_percentage": missing_percentage,
                "gaps_found": False,
                "error": "No datetime column or index found",
            }

        # Find gaps
        df_sorted["next_time"] = df_sorted[time_column].shift(-1)
        df_sorted["time_gap"] = (df_sorted["next_time"] - df_sorted[time_column]).dt.total_seconds()

        # Normal gap is the interval
        normal_gap = interval_seconds
        gap_threshold = normal_gap * 1.5  # 50% more than expected is considered a gap

        large_gaps = df_sorted[df_sorted["time_gap"] > gap_threshold].copy()

        # Calculate number of missing points in each gap
        if not large_gaps.empty:
            large_gaps["missing_points"] = ((large_gaps["time_gap"] / normal_gap) - 1).astype(int)
            total_missing_in_gaps = large_gaps["missing_points"].sum()

            logger.debug(f"Found {len(large_gaps)} gaps with {total_missing_in_gaps} missing points")

            # Show the largest gaps (up to 5)
            largest_gaps = large_gaps.nlargest(5, "time_gap")
            if not largest_gaps.empty:
                logger.debug("Largest gaps:")
                for _, row in largest_gaps.iterrows():
                    gap_minutes = row["time_gap"] / 60
                    logger.debug(f"  {row[time_column]} to {row['next_time']}: {gap_minutes:.1f} minutes ({row['missing_points']} points)")

            return {
                "expected_records": expected_records,
                "actual_records": actual_records,
                "missing_records": missing_records,
                "missing_percentage": missing_percentage,
                "gaps_found": True,
                "num_gaps": len(large_gaps),
                "total_missing_in_gaps": total_missing_in_gaps,
                "largest_gap_seconds": (large_gaps["time_gap"].max() if not large_gaps.empty else 0),
                "first_timestamp": df_sorted[time_column].min(),
                "last_timestamp": df_sorted[time_column].max(),
            }

    return {
        "expected_records": expected_records,
        "actual_records": actual_records,
        "missing_records": missing_records,
        "missing_percentage": missing_percentage,
        "gaps_found": False,
    }
