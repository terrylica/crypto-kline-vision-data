#!/usr/bin/env python3
"""
Timestamp debugging utilities for dataframe operations.

This module contains functions for debugging timestamp-related operations
in dataframes, including tracing timestamps during filtering and validation.
"""

from datetime import datetime

import pandas as pd

from utils.logger_setup import logger


def trace_dataframe_timestamps(
    df: pd.DataFrame,
    time_column: str,
    start_time: datetime,
    end_time: datetime,
    operation_name: str = "filter_operation",
    sample_rows: int = 3,
) -> None:
    """
    Trace timestamp details before and after time-based dataframe operations.

    This function logs detailed timestamp information for debugging timestamp-related
    operations on dataframes. It helps identify issues with timestamp boundary handling,
    missing data at exact boundaries, and unexpected timestamp formats.

    Args:
        df: The dataframe containing timestamps to trace
        time_column: Name of the column containing timestamps
        start_time: The start time used in the filtering operation
        end_time: The end time used in the filtering operation
        operation_name: Name of the operation for logging context
        sample_rows: Number of sample rows to log (default: 3)
    """
    if df.empty:
        logger.debug(f"[TIMESTAMP TRACE] {operation_name}: Empty DataFrame")
        return

    # Log the basic information about the operation
    logger.debug(f"[TIMESTAMP TRACE] {operation_name} with range: {start_time} to {end_time}")

    # Check if time column exists in columns or as index
    is_time_in_columns = time_column in df.columns
    is_time_as_index = df.index.name == time_column and isinstance(df.index, pd.DatetimeIndex)

    # Log information about DataFrame timestamp range
    if is_time_in_columns:
        min_ts = df[time_column].min()
        max_ts = df[time_column].max()
        logger.debug(f"[TIMESTAMP TRACE] DataFrame time range: {min_ts} to {max_ts}")

        # Check for exact boundary matches
        exact_start_match = (df[time_column] == start_time).any()
        exact_end_match = (df[time_column] == end_time).any()
        logger.debug(f"[TIMESTAMP TRACE] Exact match at start_time: {exact_start_match}")
        logger.debug(f"[TIMESTAMP TRACE] Exact match at end_time: {exact_end_match}")

        # Log sample rows
        for i in range(min(sample_rows, len(df))):
            logger.debug(f"[TIMESTAMP TRACE] Row {i}: {time_column}={df[time_column].iloc[i]}")
    elif is_time_as_index:
        min_ts = df.index.min()
        max_ts = df.index.max()
        logger.debug(f"[TIMESTAMP TRACE] DataFrame index range: {min_ts} to {max_ts}")

        # Check for exact boundary matches in index
        exact_start_match = start_time in df.index
        exact_end_match = end_time in df.index
        logger.debug(f"[TIMESTAMP TRACE] Exact match at start_time in index: {exact_start_match}")
        logger.debug(f"[TIMESTAMP TRACE] Exact match at end_time in index: {exact_end_match}")

        # Log sample rows
        for i in range(min(sample_rows, len(df))):
            logger.debug(f"[TIMESTAMP TRACE] Row {i}: {time_column}={df.index[i]}")


def analyze_filter_conditions(
    df: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
    time_column: str,
) -> None:
    """
    Analyze filter conditions to debug timestamp filtering issues.

    This function examines how many rows in a dataframe match specific timestamp
    filtering conditions, helping to identify issues with filter boundaries.

    Args:
        df: The dataframe to analyze
        start_time: Start time boundary for filtering
        end_time: End time boundary for filtering
        time_column: Name of the timestamp column
    """
    if df.empty:
        logger.debug("[TIMESTAMP TRACE] Empty DataFrame for filter condition analysis")
        return

    if time_column not in df.columns:
        if df.index.name == time_column and isinstance(df.index, pd.DatetimeIndex):
            # Convert index to column for analysis
            df_with_column = df.reset_index()

            # Check how many rows would match each condition separately
            start_condition = df_with_column[time_column] >= start_time
            end_condition = df_with_column[time_column] <= end_time
            both_conditions = start_condition & end_condition

            logger.debug(f"[TIMESTAMP TRACE] Rows meeting start condition ({time_column} >= {start_time}): {start_condition.sum()}")
            logger.debug(f"[TIMESTAMP TRACE] Rows meeting end condition ({time_column} <= {end_time}): {end_condition.sum()}")
            logger.debug(f"[TIMESTAMP TRACE] Rows meeting both conditions: {both_conditions.sum()}")

            # Check specifically for exact boundary matches
            exact_start = df_with_column[df_with_column[time_column] == start_time]
            exact_end = df_with_column[df_with_column[time_column] == end_time]
            logger.debug(f"[TIMESTAMP TRACE] Rows exactly matching start_time: {len(exact_start)}")
            logger.debug(f"[TIMESTAMP TRACE] Rows exactly matching end_time: {len(exact_end)}")

            return
    elif time_column in df.columns:
        # Check how many rows would match each condition separately
        start_condition = df[time_column] >= start_time
        end_condition = df[time_column] <= end_time
        both_conditions = start_condition & end_condition

        logger.debug(f"[TIMESTAMP TRACE] Rows meeting start condition ({time_column} >= {start_time}): {start_condition.sum()}")
        logger.debug(f"[TIMESTAMP TRACE] Rows meeting end condition ({time_column} <= {end_time}): {end_condition.sum()}")
        logger.debug(f"[TIMESTAMP TRACE] Rows meeting both conditions: {both_conditions.sum()}")

        # Check specifically for exact boundary matches
        exact_start = df[df[time_column] == start_time]
        exact_end = df[df[time_column] == end_time]
        logger.debug(f"[TIMESTAMP TRACE] Rows exactly matching start_time: {len(exact_start)}")
        logger.debug(f"[TIMESTAMP TRACE] Rows exactly matching end_time: {len(exact_end)}")

        # If no rows match exact start time, find nearest
        if len(exact_start) == 0:
            earliest_after_start = df[df[time_column] > start_time]
            if not earliest_after_start.empty:
                earliest_time = earliest_after_start[time_column].min()
                time_diff = (earliest_time - start_time).total_seconds()
                logger.debug(
                    f"[TIMESTAMP TRACE] No exact start_time match. Earliest timestamp after start_time is {earliest_time}, which is {time_diff} seconds later"
                )

    logger.debug(f"[TIMESTAMP TRACE] Unable to analyze filter conditions for column {time_column}")


def compare_filtered_results(
    input_df: pd.DataFrame,
    output_df: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
    time_column: str,
) -> None:
    """
    Compare input and filtered output dataframes to identify filtering issues.

    Args:
        input_df: Original dataframe before filtering
        output_df: Filtered dataframe
        start_time: Start time used for filtering
        end_time: End time used for filtering
        time_column: Name of the timestamp column
    """
    logger.debug(f"[TIMESTAMP TRACE] filter operation completed. Input rows: {len(input_df)}, Output rows: {len(output_df)}")

    if len(output_df) > 0 and len(input_df) > 0:
        # Check if rows at exact boundaries were handled correctly
        if time_column in input_df.columns and time_column in output_df.columns:
            start_match_in_input = (input_df[time_column] == start_time).any()
            start_match_in_output = (output_df[time_column] == start_time).any()

            logger.debug(f"[TIMESTAMP TRACE] Start time exact match in input: {start_match_in_input}, in output: {start_match_in_output}")

            if start_match_in_input and not start_match_in_output:
                logger.warning("[TIMESTAMP TRACE] Critical issue: Row with exact start_time existed in input but not in output!")

        # Check if all qualifying rows were included in the output
        if time_column in input_df.columns:
            qualifying_rows = input_df[(input_df[time_column] >= start_time) & (input_df[time_column] <= end_time)]
            if len(qualifying_rows) != len(output_df):
                logger.warning(
                    f"[TIMESTAMP TRACE] Potential data loss: {len(qualifying_rows)} rows qualify in input, but output has {len(output_df)} rows"
                )
