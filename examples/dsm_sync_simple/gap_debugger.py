#!/usr/bin/env python3
"""
Gap Debugger - A tool for analyzing and visualizing gaps in time series data.
This script can be used to:
1. Load time series data from CSV or Parquet files
2. Detect gaps based on specified interval and threshold
3. Visualize gaps in the data
4. Generate detailed gap analysis reports

This script uses utils/gap_detector.py as the single source of truth for gap detection.
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from utils.logger_setup import logger
from utils.market_constraints import Interval
from utils.gap_detector import (
    detect_gaps,
    format_gaps_for_display,
    analyze_file_for_gaps,
)

console = Console()


def load_data(file_path):
    """
    Load data from CSV or Parquet file based on file extension.

    Args:
        file_path (str or Path): Path to the data file

    Returns:
        pd.DataFrame: Loaded DataFrame with time series data
    """
    file_path = Path(file_path)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"Loading data from {file_path.name}...", total=1)

        try:
            if file_path.suffix.lower() == ".csv":
                df = pd.read_csv(file_path, parse_dates=["timestamp"])
            elif file_path.suffix.lower() in (".parquet", ".pq"):
                df = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")

            # Ensure timestamp column exists
            if "timestamp" not in df.columns:
                timestamp_cols = [
                    col
                    for col in df.columns
                    if "time" in col.lower() or "date" in col.lower()
                ]
                if timestamp_cols:
                    df = df.rename(columns={timestamp_cols[0]: "timestamp"})
                else:
                    raise ValueError("No timestamp column found in the data")

            # Convert timestamp to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Sort by timestamp
            df = df.sort_values("timestamp")

            progress.update(task, completed=1)
            return df

        except Exception as e:
            logger.exception(f"Error loading data from {file_path}")
            raise ValueError(f"Failed to load data: {str(e)}")


def convert_interval_string(interval_str):
    """
    Convert an interval string (e.g., "5") to the appropriate Interval enum.

    Args:
        interval_str (str or int): Interval value (minutes if numeric)

    Returns:
        Interval: The corresponding Interval enum
    """
    # Handle case where interval is passed as an integer (minutes)
    if isinstance(interval_str, int) or interval_str.isdigit():
        interval_minutes = int(interval_str)
        if interval_minutes == 1:
            return Interval.MINUTE_1
        elif interval_minutes == 3:
            return Interval.MINUTE_3
        elif interval_minutes == 5:
            return Interval.MINUTE_5
        elif interval_minutes == 15:
            return Interval.MINUTE_15
        elif interval_minutes == 30:
            return Interval.MINUTE_30
        elif interval_minutes == 60:
            return Interval.HOUR_1
        else:
            logger.warning(f"Non-standard interval: {interval_minutes}m. Using 1m.")
            return Interval.MINUTE_1

    # Handle case where full interval string is passed (e.g., "1m", "1h")
    try:
        return Interval(interval_str)
    except ValueError:
        logger.warning(f"Unknown interval format: {interval_str}. Using 1m.")
        return Interval.MINUTE_1


def display_dataset_info(df):
    """Display basic information about the dataset."""
    table = Table(title="Dataset Information")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    time_range = df["timestamp"].max() - df["timestamp"].min()

    table.add_row("Total records", str(len(df)))
    table.add_row(
        "Time range start", df["timestamp"].min().strftime("%Y-%m-%d %H:%M:%S")
    )
    table.add_row("Time range end", df["timestamp"].max().strftime("%Y-%m-%d %H:%M:%S"))
    table.add_row("Duration", f"{time_range.total_seconds() / 3600:.2f} hours")
    table.add_row("Columns", ", ".join(df.columns))

    console.print(table)

    # Display first and last few rows
    console.print("[cyan]First 3 rows:[/cyan]")
    console.print(df.head(3))
    console.print("[cyan]Last 3 rows:[/cyan]")
    console.print(df.tail(3))


def display_gap_summary(gaps, stats):
    """Display summary of gap analysis results."""
    # Create a gap_df for easier visualization
    gap_df = format_gaps_for_display(gaps)

    # Format durations to minutes for display
    max_gap_minutes = (
        stats.get("max_gap_duration", pd.Timedelta(0)).total_seconds() / 60
    )

    # Calculate gap distribution
    gap_sizes = []
    for gap in gaps:
        # Get interval size in multiples of the expected interval
        minutes = gap.duration.total_seconds() / 60
        gap_sizes.append(minutes)

    # Create gap size distribution buckets
    gap_size_buckets = {
        "1.5-2x": 0,
        "2-5x": 0,
        "5-10x": 0,
        "10-20x": 0,
        "20-50x": 0,
        "50-100x": 0,
        ">100x": 0,
    }

    # Simple classification for demonstration
    for gap in gaps:
        rel_size = (
            gap.missing_points + 1
        )  # Add 1 because missing_points doesn't count the expected interval
        if rel_size < 2:
            gap_size_buckets["1.5-2x"] += 1
        elif rel_size < 5:
            gap_size_buckets["2-5x"] += 1
        elif rel_size < 10:
            gap_size_buckets["5-10x"] += 1
        elif rel_size < 20:
            gap_size_buckets["10-20x"] += 1
        elif rel_size < 50:
            gap_size_buckets["20-50x"] += 1
        elif rel_size < 100:
            gap_size_buckets["50-100x"] += 1
        else:
            gap_size_buckets[">100x"] += 1

    # Calculate average gap size
    avg_gap_minutes = sum(gap_sizes) / len(gap_sizes) if gap_sizes else 0

    timespan_hours = stats.get("timespan_hours", 0)
    total_records = stats.get("total_records", 0)
    expected_records = (
        int(timespan_hours * 60) + 1 if timespan_hours > 0 else 0
    )  # For 1-minute data
    missing_records = expected_records - total_records if expected_records > 0 else 0
    coverage_percent = (
        (total_records / expected_records * 100) if expected_records > 0 else 0
    )

    panel = Panel(
        f"Total records: [green]{total_records}[/green]\n"
        f"Expected records: [green]{expected_records}[/green]\n"
        f"Missing records: [green]{missing_records}[/green]\n"
        f"Data coverage: [green]{coverage_percent:.2f}%[/green]\n"
        f"Total gaps: [green]{len(gaps)}[/green]\n"
        f"Largest gap: [green]{max_gap_minutes:.1f} minutes[/green]\n"
        f"Average gap: [green]{avg_gap_minutes:.2f} minutes[/green]",
        title="Gap Analysis Summary",
        border_style="blue",
    )
    console.print(panel)

    # Gap size distribution
    table = Table(title="Gap Size Distribution")
    table.add_column("Gap Size", style="cyan")
    table.add_column("Count", style="green")

    for size, count in gap_size_buckets.items():
        table.add_row(str(size), str(count))

    console.print(table)

    # Display largest gaps
    if gaps:
        table = Table(title="Largest Gaps (Top 5)")
        table.add_column("Start", style="cyan")
        table.add_column("End", style="cyan")
        table.add_column("Duration (min)", style="green")
        table.add_column("Relative Size", style="green")

        # Sort gaps by duration
        sorted_gaps = sorted(gaps, key=lambda x: x.duration, reverse=True)
        for gap in sorted_gaps[:5]:
            start = gap.start_time.strftime("%Y-%m-%d %H:%M:%S")
            end = gap.end_time.strftime("%Y-%m-%d %H:%M:%S")
            duration_min = gap.duration.total_seconds() / 60
            relative_size = gap.missing_points + 1  # Add 1 for the expected interval
            table.add_row(
                start,
                end,
                f"{duration_min:.2f}",
                f"{relative_size:.2f}x",
            )

        console.print(table)


def visualize_gaps(df, gaps, interval):
    """
    Create a visualization of gaps using ASCII charts.

    Args:
        df (pd.DataFrame): The original data
        gaps (List[Gap]): List of Gap objects
        interval (Interval): The interval enum
    """
    if not gaps:
        console.print("[green]No significant gaps found in the data![/green]")
        return

    # Create a simple ASCII chart of gap distribution over time
    time_range = df["timestamp"].max() - df["timestamp"].min()
    hours = time_range.total_seconds() / 3600

    # Create a timeline with 80 characters
    console.print("\n[cyan]Timeline of Gaps:[/cyan]")
    start_time = df["timestamp"].min()
    end_time = df["timestamp"].max()

    # Create a timeline with 80 characters
    timeline = ["─"] * 80

    for gap in gaps:
        # Calculate position on the timeline
        position = int(((gap.start_time - start_time) / time_range) * 79)
        if 0 <= position < 80:
            timeline[position] = "▼"

    # Add start and end markers
    timeline[0] = "┌"
    timeline[-1] = "┐"

    console.print("".join(timeline))
    console.print(
        f"{start_time.strftime('%Y-%m-%d %H:%M')} {' ' * 74} {end_time.strftime('%Y-%m-%d %H:%M')}"
    )
    console.print("[cyan]▼ = gap location[/cyan]")


def format_results_for_json(gaps, stats, df):
    """Format gap results for JSON output."""
    results = {
        "summary": {
            "total_points": stats.get("total_records", 0),
            "expected_points": int(stats.get("timespan_hours", 0) * 60)
            + 1,  # For 1-minute data
            "missing_points": int(stats.get("timespan_hours", 0) * 60)
            + 1
            - stats.get("total_records", 0),
            "coverage_percent": round(
                (
                    (
                        stats.get("total_records", 0)
                        / (int(stats.get("timespan_hours", 0) * 60) + 1)
                    )
                    * 100
                    if int(stats.get("timespan_hours", 0) * 60) + 1 > 0
                    else 0
                ),
                2,
            ),
            "total_gaps": len(gaps),
            "time_range": {
                "start": (
                    stats.get("first_timestamp", "").isoformat()
                    if stats.get("first_timestamp")
                    else ""
                ),
                "end": (
                    stats.get("last_timestamp", "").isoformat()
                    if stats.get("last_timestamp")
                    else ""
                ),
                "duration_hours": round(stats.get("timespan_hours", 0), 2),
            },
        },
        "statistics": {
            "gap_sizes": {},  # Will be populated
            "largest_gap_minutes": (
                round(
                    stats.get("max_gap_duration", pd.Timedelta(0)).total_seconds() / 60,
                    2,
                )
            ),
            "average_gap_minutes": (
                round(
                    sum(gap.duration.total_seconds() for gap in gaps) / 60 / len(gaps),
                    2,
                )
                if gaps
                else 0
            ),
            "median_gap_minutes": 0,  # Placeholder
        },
        "gaps": [],
    }

    # Gap size distribution
    gap_size_buckets = {
        "1.5-2x": 0,
        "2-5x": 0,
        "5-10x": 0,
        "10-20x": 0,
        "20-50x": 0,
        "50-100x": 0,
        ">100x": 0,
    }

    for gap in gaps:
        rel_size = gap.missing_points + 1
        if rel_size < 2:
            gap_size_buckets["1.5-2x"] += 1
        elif rel_size < 5:
            gap_size_buckets["2-5x"] += 1
        elif rel_size < 10:
            gap_size_buckets["5-10x"] += 1
        elif rel_size < 20:
            gap_size_buckets["10-20x"] += 1
        elif rel_size < 50:
            gap_size_buckets["20-50x"] += 1
        elif rel_size < 100:
            gap_size_buckets["50-100x"] += 1
        else:
            gap_size_buckets[">100x"] += 1

    results["statistics"]["gap_sizes"] = gap_size_buckets

    # Add individual gaps to results
    for gap in gaps:
        results["gaps"].append(
            {
                "start": gap.start_time.isoformat(),
                "end": gap.end_time.isoformat(),
                "duration_minutes": round(gap.duration.total_seconds() / 60, 2),
                "relative_size": gap.missing_points + 1,
                "crosses_day_boundary": gap.crosses_day_boundary,
            }
        )

    return results


def main():
    """Main function to run the gap analysis."""
    parser = argparse.ArgumentParser(
        description="Gap Debugger - Analyze and visualize gaps in time series data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--input", type=str, required=True, help="Path to input CSV or Parquet file"
    )

    parser.add_argument(
        "--interval",
        type=str,
        default="1",
        help="Expected interval between data points in minutes (e.g., 1, 5, 15) or as interval string (e.g., 1m, 1h)",
    )

    parser.add_argument(
        "--threshold",
        type=float,
        default=0.3,
        help="Threshold fraction to identify gaps (e.g., 0.3 means 30% above expected interval)",
    )

    parser.add_argument(
        "--time-column",
        type=str,
        default="timestamp",
        help="Name of the timestamp column in the data file",
    )

    parser.add_argument(
        "--output",
        type=str,
        help="Path to save the JSON output file (default: gap_analysis_<filename>.json)",
    )

    args = parser.parse_args()

    try:
        # Load data
        input_path = Path(args.input)
        df = load_data(input_path)

        # Display basic info
        console.rule(f"[bold blue]Gap Analysis for {input_path.name}")
        display_dataset_info(df)

        # Convert interval string to Interval enum
        interval = convert_interval_string(args.interval)

        # Analyze gaps using the gap_detector.py from utils
        console.print("  Analyzing gaps...")
        gaps, stats = detect_gaps(
            df,
            interval=interval,
            time_column=args.time_column,
            gap_threshold=args.threshold,
        )

        # Display results
        console.rule("[bold blue]Gap Analysis Results")
        display_gap_summary(gaps, stats)

        # Visualize gaps
        console.rule("[bold blue]Gap Visualization")
        visualize_gaps(df, gaps, interval)

        # Format results for JSON
        results = format_results_for_json(gaps, stats, df)

        # Save results
        if args.output:
            output_path = Path(args.output)
        else:
            output_path = input_path.parent / f"gap_analysis_{input_path.stem}.json"

        with open(output_path, "w") as f:
            json.dump(results, f, indent=2)

        console.print(f"\nGap analysis saved to: {output_path}")

        # Recommendations
        console.rule("[bold blue]Recommendations")

        missing_pct = (
            results["summary"]["missing_points"]
            / results["summary"]["expected_points"]
            * 100
            if results["summary"]["expected_points"] > 0
            else 0
        )

        if missing_pct > 20:
            console.print(
                "[bold red]⚠️ Critical: Data has significant gaps (>20% missing)[/bold red]"
            )
            console.print("- Investigate data collection process for major failures")
            console.print(
                "- Consider regenerating this dataset or using a more complete source"
            )
        elif missing_pct > 5:
            console.print(
                "[bold yellow]⚠️ Warning: Data has moderate gaps (5-20% missing)[/bold yellow]"
            )
            console.print("- Review data collection process for intermittent failures")
            console.print("- Consider implementing gap filling methods for analysis")
        else:
            console.print(
                "[bold green]✓ Minor: Data has minimal gaps (<5% missing)[/bold green]"
            )
            console.print(
                "- Consider simple interpolation techniques for missing points"
            )

        # Specific recommendations based on gap patterns
        largest_gap = results["statistics"]["largest_gap_minutes"]
        if largest_gap > 60:
            console.print(
                f"[yellow]⚠️ Largest gap is {largest_gap:.1f} minutes (~{largest_gap/60:.1f} hours)[/yellow]"
            )
            console.print("- Check for system outages during gap periods")

        return 0

    except Exception as e:
        logger.exception("Error in gap_debugger")
        console.print(f"[bold red]Error: {str(e)}[/bold red]")
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
