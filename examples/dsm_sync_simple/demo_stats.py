#!/usr/bin/env python3
"""
Statistics and visualization helper module for DataSourceManager demo.
Provides detailed analysis of data sources and time ranges using terminal-based tools.
"""

from datetime import datetime, timedelta, timezone
import pandas as pd
from pathlib import Path
import json
from typing import Dict, List, Tuple, Optional, Any
import numpy as np

from utils.logger_setup import logger
from rich import print
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.progress import Progress, BarColumn, TextColumn

console = Console()


def analyze_data_sources(df: pd.DataFrame) -> Dict[str, int]:
    """
    Analyze data sources using the _data_source column.

    Args:
        df: DataFrame with merged data

    Returns:
        Dictionary with count of records from each source
    """
    if df.empty:
        return {}

    if "_data_source" in df.columns:
        return df["_data_source"].value_counts().to_dict()

    logger.warning("No _data_source column found in DataFrame")
    return {}


def analyze_time_ranges(
    df: pd.DataFrame,
    cache_range: Tuple[datetime, datetime],
    vision_range: Tuple[datetime, datetime],
    rest_range: Tuple[datetime, datetime],
) -> Dict[str, int]:
    """
    Analyze data by time ranges to classify each record.

    Args:
        df: DataFrame with merged data
        cache_range: Tuple of (start_time, end_time) for cache data
        vision_range: Tuple of (start_time, end_time) for Vision API data
        rest_range: Tuple of (start_time, end_time) for REST API data

    Returns:
        Dictionary with count of records in each time range
    """
    if df.empty:
        return {"cache": 0, "vision": 0, "rest": 0, "other": 0, "total": 0}

    # If index is not open_time, try to set it
    if df.index.name != "open_time":
        if "open_time" in df.columns:
            df = df.set_index("open_time")
        else:
            logger.error("DataFrame doesn't have an open_time index or column")
            return {"cache": 0, "vision": 0, "rest": 0, "other": 0, "total": len(df)}

    # Create masks for each range
    cache_start, cache_end = cache_range
    vision_start, vision_end = vision_range
    rest_start, rest_end = rest_range

    cache_mask = (df.index >= cache_start) & (df.index <= cache_end)
    vision_mask = (df.index >= vision_start) & (df.index <= vision_end)
    rest_mask = (df.index >= rest_start) & (df.index <= rest_end)

    # Count records in each range
    cache_count = cache_mask.sum()
    vision_count = vision_mask.sum()
    rest_count = rest_mask.sum()
    other_count = len(df) - cache_count - vision_count - rest_count

    return {
        "cache": cache_count,
        "vision": vision_count,
        "rest": rest_count,
        "other": other_count,
        "total": len(df),
    }


def get_date_range_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get statistics about the date range covered by the data.

    Args:
        df: DataFrame with merged data

    Returns:
        Dictionary with date range statistics
    """
    if df.empty:
        return {
            "start_date": None,
            "end_date": None,
            "duration_days": 0,
            "count_by_day": {},
        }

    # Ensure we have an open_time column or index
    if df.index.name == "open_time":
        dates = df.index
    elif "open_time" in df.columns:
        dates = df["open_time"]
    else:
        logger.error("DataFrame doesn't have an open_time index or column")
        return {
            "start_date": None,
            "end_date": None,
            "duration_days": 0,
            "count_by_day": {},
        }

    # Get start and end dates
    start_date = dates.min()
    end_date = dates.max()

    # Duration in days
    duration = (end_date - start_date).total_seconds() / (60 * 60 * 24)

    # Count records by day
    # Convert to pandas datetime to use dt accessor
    if not isinstance(dates, pd.DatetimeIndex):
        dates = pd.to_datetime(dates)

    # Group by date and count
    if df.index.name == "open_time":
        # If index is open_time, reset it first
        temp_df = df.reset_index()
        counts_by_day = temp_df.groupby(temp_df["open_time"].dt.date).size()
    else:
        counts_by_day = df.groupby(df["open_time"].dt.date).size()

    # Convert to dictionary with string dates for serialization
    count_by_day = {str(date): int(count) for date, count in counts_by_day.items()}

    return {
        "start_date": start_date,
        "end_date": end_date,
        "duration_days": duration,
        "count_by_day": count_by_day,
    }


def analyze_coverage(df: pd.DataFrame, interval_minutes: int = 1) -> Dict[str, Any]:
    """
    Analyze data coverage and identify gaps.

    Args:
        df: DataFrame with merged data
        interval_minutes: Expected interval in minutes between data points

    Returns:
        Dictionary with coverage statistics
    """
    if df.empty:
        return {
            "total_records": 0,
            "expected_records": 0,
            "coverage_percent": 0,
            "gap_count": 0,
            "largest_gap_minutes": 0,
        }

    # Ensure we have an open_time column or index
    if df.index.name == "open_time":
        # If index is open_time, reset it to get it as a column
        df = df.reset_index()

    if "open_time" not in df.columns:
        logger.error("DataFrame doesn't have an open_time column")
        return {
            "total_records": len(df),
            "expected_records": 0,
            "coverage_percent": 0,
            "gap_count": 0,
            "largest_gap_minutes": 0,
        }

    # Sort by open_time
    df = df.sort_values("open_time")

    # Get start and end times
    start_time = df["open_time"].min()
    end_time = df["open_time"].max()

    # Calculate expected number of records
    expected_duration_minutes = (end_time - start_time).total_seconds() / 60
    expected_records = (
        expected_duration_minutes / interval_minutes + 1
    )  # +1 to include endpoints

    # Calculate actual coverage
    actual_records = len(df)
    coverage_percent = (actual_records / expected_records) * 100

    # Calculate time differences between consecutive points
    time_diffs = df["open_time"].diff().dropna()

    # Convert time differences to minutes
    time_diffs_minutes = time_diffs.dt.total_seconds() / 60

    # Find gaps (time differences greater than expected interval)
    gaps = time_diffs_minutes[time_diffs_minutes > (interval_minutes * 1.5)]

    # Get largest gap in minutes
    largest_gap_minutes = gaps.max() if not gaps.empty else 0

    return {
        "total_records": actual_records,
        "expected_records": int(expected_records),
        "coverage_percent": coverage_percent,
        "gap_count": len(gaps),
        "largest_gap_minutes": largest_gap_minutes,
    }


def save_stats_to_file(stats: Dict[str, Any], file_path: str) -> None:
    """
    Save statistics to a JSON file.

    Args:
        stats: Dictionary with statistics
        file_path: Path to save JSON file
    """
    # Create directory if it doesn't exist
    output_dir = Path(file_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert non-serializable objects to strings
    def convert_to_serializable(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, np.int64):
            return int(obj)
        if isinstance(obj, np.float64):
            return float(obj)
        if pd.isna(obj):
            return None
        return obj

    # Process the stats dictionary recursively
    def process_dict(d):
        result = {}
        for k, v in d.items():
            if isinstance(v, dict):
                result[k] = process_dict(v)
            elif isinstance(v, list):
                result[k] = [convert_to_serializable(item) for item in v]
            else:
                result[k] = convert_to_serializable(v)
        return result

    serializable_stats = process_dict(stats)

    # Save to file
    with open(file_path, "w") as f:
        json.dump(serializable_stats, f, indent=2)

    logger.info(f"Statistics saved to {file_path}")


def display_data_distribution_terminal(
    counts_by_day: Dict[str, int], max_width: int = 50
) -> None:
    """
    Display data distribution by day using terminal-based visualization.

    Args:
        counts_by_day: Dictionary mapping date strings to record counts
        max_width: Maximum width of the bar in characters
    """
    # Find the maximum count for scaling
    max_count = max(counts_by_day.values()) if counts_by_day else 0

    print("\n[bold cyan]Data Distribution by Day:[/bold cyan]")

    # Display bar for each day
    for date, count in sorted(counts_by_day.items()):
        # Calculate bar length with a minimum of 1 character
        if max_count > 0:
            bar_length = max(1, int((count / max_count) * max_width))
        else:
            bar_length = 1

        # Create the bar with block characters
        bar = "█" * bar_length

        # Print the bar with date and count
        print(f"{date}: {count:5d} records | {bar}")


def display_source_breakdown_table(
    source_stats: Dict[str, int], total_records: int
) -> None:
    """
    Display source breakdown in a rich table.

    Args:
        source_stats: Dictionary mapping source names to record counts
        total_records: Total number of records
    """
    # Create a table for the source breakdown
    source_table = Table(title="Data Source Breakdown")
    source_table.add_column("Source", style="cyan")
    source_table.add_column("Records", style="green", justify="right")
    source_table.add_column("Percentage", style="yellow", justify="right")

    # Add rows for each source
    for source, count in source_stats.items():
        percent = (count / total_records) * 100 if total_records > 0 else 0
        source_table.add_row(source, f"{count:,}", f"{percent:.1f}%")

    # Show the table
    console.print(source_table)


def display_gaps_info(gap_data: Dict[str, Any]) -> None:
    """
    Display information about gaps in the data.

    Args:
        gap_data: Dictionary with gap information
    """
    # Create a simple table for gap information
    gap_table = Table(title="Data Gaps Information")
    gap_table.add_column("Metric", style="cyan")
    gap_table.add_column("Value", style="green")

    # Add gap information rows
    gap_table.add_row("Total Gaps", f"{gap_data['gap_count']:,}")
    gap_table.add_row("Largest Gap", f"{gap_data['largest_gap_minutes']:.1f} minutes")
    gap_table.add_row("Coverage", f"{gap_data['coverage_percent']:.2f}%")
    gap_table.add_row("Expected Records", f"{gap_data['expected_records']:,}")
    gap_table.add_row("Actual Records", f"{gap_data['total_records']:,}")

    # Show the table
    console.print(gap_table)


def display_detailed_stats(
    df: pd.DataFrame,
    cache_range: Tuple[datetime, datetime],
    vision_range: Tuple[datetime, datetime],
    rest_range: Tuple[datetime, datetime],
    symbol: str,
    market_type: str,
    interval: str,
    chart_type: str,
    save_to_file: bool = True,
) -> Dict[str, Any]:
    """
    Display detailed statistics and visualizations for the merged data.

    Args:
        df: DataFrame with merged data
        cache_range: Tuple of (start_time, end_time) for cache data
        vision_range: Tuple of (start_time, end_time) for Vision API data
        rest_range: Tuple of (start_time, end_time) for REST API data
        symbol: Symbol being analyzed
        market_type: Market type (SPOT, UM, CM)
        interval: Time interval between data points
        chart_type: Type of chart data (klines, fundingRate)
        save_to_file: Whether to save statistics to a file

    Returns:
        Dictionary with all statistics
    """
    # Create rich table for better display
    table = Table(title=f"Data Source Analysis for {symbol} ({market_type})")

    # Add columns
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="green")
    table.add_column("Details", style="yellow")

    # Get data source statistics
    source_stats = analyze_data_sources(df)

    # Get time range statistics
    time_range_stats = analyze_time_ranges(df, cache_range, vision_range, rest_range)

    # Get date range statistics
    date_range_stats = get_date_range_stats(df)

    # Get coverage statistics
    coverage_stats = analyze_coverage(df)

    # Add rows for source statistics
    table.add_row(
        "Total Records",
        str(len(df)),
        f"From {date_range_stats['start_date'].strftime('%Y-%m-%d %H:%M')} to {date_range_stats['end_date'].strftime('%Y-%m-%d %H:%M')}",
    )

    # Add rows for source breakdown
    total_records = len(df)
    source_rows = []
    for source, count in source_stats.items():
        percent = (count / total_records) * 100 if total_records > 0 else 0
        source_rows.append((source, count, percent))

    table.add_row(
        "Source Breakdown (Actual)",
        ", ".join([f"{source}: {count}" for source, count, _ in source_rows]),
        ", ".join([f"{source}: {percent:.1f}%" for source, _, percent in source_rows]),
    )

    # Add rows for time range statistics
    table.add_row(
        "Time Range Analysis",
        f"Cache: {time_range_stats['cache']}, Vision: {time_range_stats['vision']}, REST: {time_range_stats['rest']}",
        f"Other: {time_range_stats['other']} ({time_range_stats['other']/total_records*100:.1f}% outside defined ranges)",
    )

    # Add rows for coverage statistics
    table.add_row(
        "Data Coverage",
        f"{coverage_stats['coverage_percent']:.2f}%",
        f"Expected: {coverage_stats['expected_records']}, Actual: {coverage_stats['total_records']}",
    )

    table.add_row(
        "Data Gaps",
        f"{coverage_stats['gap_count']} gaps",
        f"Largest gap: {coverage_stats['largest_gap_minutes']:.1f} minutes",
    )

    # Add time range details
    table.add_row(
        "Cache Time Range",
        f"{cache_range[0].strftime('%Y-%m-%d %H:%M')} to {cache_range[1].strftime('%Y-%m-%d %H:%M')}",
        f"Duration: {(cache_range[1] - cache_range[0]).total_seconds() / 3600:.1f} hours",
    )

    table.add_row(
        "Vision Time Range",
        f"{vision_range[0].strftime('%Y-%m-%d %H:%M')} to {vision_range[1].strftime('%Y-%m-%d %H:%M')}",
        f"Duration: {(vision_range[1] - vision_range[0]).total_seconds() / 3600:.1f} hours",
    )

    table.add_row(
        "REST Time Range",
        f"{rest_range[0].strftime('%Y-%m-%d %H:%M')} to {rest_range[1].strftime('%Y-%m-%d %H:%M')}",
        f"Duration: {(rest_range[1] - rest_range[0]).total_seconds() / 3600:.1f} hours",
    )

    # Display the table
    console.print("\n")
    console.print(
        Panel.fit(
            Text(
                f"DETAILED ANALYSIS: {symbol} {interval} {chart_type} in {market_type} Market",
                style="bold white",
            ),
            border_style="blue",
        )
    )
    console.print(table)

    # Display data distribution by day using terminal-based visualization
    display_data_distribution_terminal(date_range_stats["count_by_day"])

    # Compile all statistics
    all_stats = {
        "symbol": symbol,
        "market_type": market_type,
        "interval": interval,
        "chart_type": chart_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_statistics": source_stats,
        "time_range_statistics": time_range_stats,
        "date_range_statistics": date_range_stats,
        "coverage_statistics": coverage_stats,
        "time_ranges": {
            "cache": {
                "start": cache_range[0].isoformat(),
                "end": cache_range[1].isoformat(),
                "duration_hours": (cache_range[1] - cache_range[0]).total_seconds()
                / 3600,
            },
            "vision": {
                "start": vision_range[0].isoformat(),
                "end": vision_range[1].isoformat(),
                "duration_hours": (vision_range[1] - vision_range[0]).total_seconds()
                / 3600,
            },
            "rest": {
                "start": rest_range[0].isoformat(),
                "end": rest_range[1].isoformat(),
                "duration_hours": (rest_range[1] - rest_range[0]).total_seconds()
                / 3600,
            },
        },
    }

    # Save to file if requested
    if save_to_file:
        stats_dir = Path("./logs/statistics")
        stats_dir.mkdir(parents=True, exist_ok=True)

        filename = (
            f"{market_type.lower()}_{symbol}_{interval}_{chart_type.lower()}_stats.json"
        )
        stats_path = stats_dir / filename

        save_stats_to_file(all_stats, str(stats_path))
        print(f"[bold green]Detailed statistics saved to {stats_path}[/bold green]")

    return all_stats
