#!/usr/bin/env python3
"""
Table display utilities for the Failover Composition Priority (FCP) mechanism.
"""

import pandas as pd
from rich.table import Table
from utils.logger_setup import logger
from rich import print
from utils_for_debug.dataframe_output import format_dataframe_for_display


def display_source_breakdown(df):
    """
    Display a breakdown of data sources used in the retrieved data.

    Args:
        df: DataFrame containing the retrieved data with a '_data_source' column

    Returns:
        bool: True if source information was available and displayed, False otherwise
    """
    if "_data_source" not in df.columns:
        print(
            "[bold yellow]Warning: Source information not available in the data[/bold yellow]"
        )
        return False

    source_counts = df["_data_source"].value_counts()

    source_table = Table(title="Data Source Breakdown")
    source_table.add_column("Source", style="cyan")
    source_table.add_column("Records", style="green", justify="right")
    source_table.add_column("Percentage", style="yellow", justify="right")

    for source, count in source_counts.items():
        percentage = count / len(df) * 100
        source_table.add_row(source, f"{count:,}", f"{percentage:.1f}%")

    print(source_table)
    return True


def display_source_timeline(df):
    """
    Display a timeline visualization of how data sources are distributed by date.

    Args:
        df: DataFrame containing the retrieved data with '_data_source' and 'open_time' columns

    Returns:
        bool: True if the timeline was successfully displayed, False otherwise
    """
    if "_data_source" not in df.columns or "open_time" not in df.columns:
        logger.warning("Required columns missing for source timeline display")
        return False

    try:
        # Create a new column with the date part only
        df["date"] = df["open_time"].dt.date
        source_counts = df["_data_source"].value_counts()

        date_groups = (
            df.groupby("date")["_data_source"].value_counts().unstack(fill_value=0)
        )

        # Display timeline visualization
        timeline_table = Table(title="Sources by Date")
        timeline_table.add_column("Date", style="cyan")

        # Add columns for each source found
        for source in source_counts.index:
            timeline_table.add_column(source, style="green", justify="right")

        # Add rows for each date
        for date, row in date_groups.iterrows():
            values = [str(date)]
            for source in source_counts.index:
                if source in row:
                    values.append(f"{row[source]:,}")
                else:
                    values.append("0")
            timeline_table.add_row(*values)

        print("\n[bold cyan]Source Distribution Timeline:[/bold cyan]")
        print(timeline_table)
        return True
    except Exception as e:
        logger.error(f"Error displaying source timeline: {e}")
        return False


def display_source_samples(df, max_samples=2):
    """
    Display sample data from each source in the dataset.

    Args:
        df: DataFrame containing the retrieved data with a '_data_source' column
        max_samples: Maximum number of sample rows to display per source

    Returns:
        bool: True if samples were displayed, False otherwise
    """
    if "_data_source" not in df.columns:
        return False

    print(f"\n[bold cyan]Sample Data by Source:[/bold cyan]")
    source_counts = df["_data_source"].value_counts()

    for source in source_counts.index:
        source_df = df[df["_data_source"] == source].head(max_samples)
        if not source_df.empty:
            print(f"\n[bold green]Records from {source} source:[/bold green]")
            display_df = format_dataframe_for_display(source_df)
            print(display_df)

    return True
