#!/usr/bin/env python3
"""
Debug utilities for displaying and formatting DataFrames.

This module contains functions for formatting, displaying, and logging
information about DataFrames in a readable format.
"""

import pandas as pd
from datetime import datetime
from pathlib import Path

from utils.logger_setup import logger

# No need to import rich.print separately as it's handled by logger


def log_dataframe_info(df, label="DataFrame"):
    """Log basic information about a DataFrame for debugging.

    Args:
        df: DataFrame to analyze
        label: Label to use in log messages for context

    Returns:
        None
    """
    logger.debug(f"{label} shape: {df.shape}")
    logger.debug(f"{label} columns: {list(df.columns)}")
    logger.debug(f"{label} dtypes: {df.dtypes}")

    if "_data_source" in df.columns and not df.empty:
        source_counts = df["_data_source"].value_counts()
        logger.debug(f"Data sources: {source_counts.to_dict()}")


def print_integrity_results(integrity_result):
    """Print data integrity analysis results in a user-friendly format.

    Args:
        integrity_result: Dictionary with integrity analysis results

    Returns:
        None
    """
    # Use print directly when smart_print is enabled
    print(f"\n[bold cyan]Data Integrity Analysis:[/bold cyan]")
    print(f"Expected records: {integrity_result['expected_records']}")
    print(f"Actual records: {integrity_result['actual_records']}")
    print(
        f"Missing records: {integrity_result['missing_records']} ({integrity_result.get('missing_percentage', 0):.2f}%)"
    )

    if integrity_result.get("gaps_found", False):
        print(
            f"Found {integrity_result.get('num_gaps', 0)} gaps with {integrity_result.get('total_missing_in_gaps', 0)} missing points"
        )
        print(
            f"Largest gap: {integrity_result.get('largest_gap_seconds', 0) / 60:.1f} minutes"
        )
        print(
            f"Data spans from {integrity_result.get('first_timestamp')} to {integrity_result.get('last_timestamp')}"
        )


def format_dataframe_for_display(df):
    """Format a DataFrame for display, making datetime columns human-readable.

    Args:
        df: DataFrame to format

    Returns:
        DataFrame: A copy of the input with formatted datetime columns
    """
    if df is None or df.empty:
        return df

    # Create a copy to avoid modifying the original
    display_df = df.copy()

    # Reset index if it's datetime
    if isinstance(display_df.index, pd.DatetimeIndex):
        display_df = display_df.reset_index()

    # Convert datetime columns to readable strings
    for col in display_df.columns:
        if pd.api.types.is_datetime64_any_dtype(display_df[col]):
            display_df[col] = display_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    return display_df


def save_dataframe_to_csv(
    df, market_type, symbol, interval, output_dir="./logs/fcp_demo"
):
    """Save DataFrame to a CSV file in the specified directory.

    Args:
        df: DataFrame to save
        market_type: Market type string (e.g., "spot")
        symbol: Symbol string (e.g., "BTCUSDT")
        interval: Interval string (e.g., "1m")
        output_dir: Directory to save the CSV file

    Returns:
        Path: Path to the saved CSV file
    """
    if df is None or df.empty:
        logger.warning("Cannot save empty DataFrame to CSV")
        return None

    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"{market_type.lower()}_{symbol}_{interval}_{timestamp}.csv"
    csv_path = output_path / csv_filename

    # Save to CSV without the index
    df.to_csv(csv_path, index=False)
    return csv_path


def print_no_data_message(
    symbol, market_type, interval, start_time, end_time, enforce_source, use_cache
):
    """Print a user-friendly message when no data is retrieved.

    Args:
        symbol: Symbol string
        market_type: Market type object
        interval: Interval string
        start_time: Start time datetime
        end_time: End time datetime
        enforce_source: Data source being enforced
        use_cache: Whether cache is enabled

    Returns:
        None
    """
    # Use regular print for normal output that should be visible at INFO level
    print("[bold red]No data retrieved for the specified time range[/bold red]")
    print("[yellow]Possible reasons:[/yellow]")
    print("1. The requested time range is outside the available data")
    print("2. There's no data available for this symbol in this time range")
    print("3. Both REST and VISION APIs failed to provide data")
    print("\n[bold cyan]Debugging information:[/bold cyan]")
    print(f"Symbol: {symbol}")
    print(f"Market type: {market_type.name}")
    print(f"Interval: {interval.value if hasattr(interval, 'value') else interval}")
    print(f"Start time: {start_time.isoformat()}")
    print(f"End time: {end_time.isoformat()}")
    print(
        f"Enforced source: {enforce_source.name if hasattr(enforce_source, 'name') else enforce_source}"
    )
    print(f"Cache enabled: {use_cache}")


def print_always_visible(message):
    """Print a message that should always be visible regardless of log level.

    Args:
        message: The message to print

    Returns:
        None
    """
    # Use logger.console.print for messages that should always be visible
    logger.console.print(message)
