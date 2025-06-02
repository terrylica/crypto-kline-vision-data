#!/usr/bin/env python3
"""Example demonstrating proper datetime handling with Data Source Manager.

This example shows best practices for:
1. Working with timezone-aware datetimes
2. Checking data completeness
3. Handling potential gaps in data
4. Safe reindexing for analysis
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Add project root to path if needed
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Import DSM components
from core.sync.data_source_manager import DataSourceManager
from utils.dataframe_utils import verify_data_completeness
from utils.for_core.dsm_utilities import (
    check_window_data_completeness,
    safely_reindex_dataframe,
)
from utils.logger_setup import configure_session_logging, logger
from utils.market_constraints import DataProvider, Interval, MarketType

# Console for rich output
console = Console()


def setup():
    """Set up logging and environment."""
    # Configure logging
    main_log, error_log, _ = configure_session_logging("dsm_datetime_example", "INFO")
    logger.info(f"Logs: {main_log} and {error_log}")

    # Create DSM instance
    return DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)


def example_timezone_aware_retrieval(dsm):
    """Demonstrate retrieval with proper timezone handling."""
    console.print(Panel("Example 1: Timezone-Aware DateTime Retrieval", style="green"))

    # Always use timezone-aware datetimes
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=3)

    logger.info(f"Retrieving data from {start_time} to {end_time}")

    # Retrieve data
    df = dsm.get_data(
        symbol="BTCUSDT",
        start_time=start_time,
        end_time=end_time,
        interval=Interval.MINUTE_15,
    )

    # Print data information
    console.print(f"Retrieved {len(df)} rows")
    console.print(f"First timestamp: {df.index[0]}")
    console.print(f"Last timestamp: {df.index[-1]}")
    console.print(f"Timezone info: {df.index.tz}")

    # Show data source info
    if "_data_source" in df.columns:
        sources = df["_data_source"].value_counts().to_dict()
        table = Table(title="Data Sources Used")
        table.add_column("Source")
        table.add_column("Count")
        table.add_column("Percentage")

        for source, count in sources.items():
            percentage = (count / len(df)) * 100
            table.add_row(source, str(count), f"{percentage:.1f}%")

        console.print(table)

    return df


def example_check_data_completeness(df, start_time, end_time):
    """Demonstrate checking for data completeness."""
    console.print(Panel("Example 2: Checking Data Completeness", style="green"))

    # Check for gaps in the data
    is_complete, gaps = verify_data_completeness(df, start_time, end_time, interval="15m")

    if is_complete:
        console.print("[green]Data is complete - no gaps detected[/green]")
    else:
        console.print(f"[yellow]Found {len(gaps)} gaps in the data[/yellow]")

        # Show details of the gaps
        table = Table(title="Data Gaps")
        table.add_column("Start")
        table.add_column("End")
        table.add_column("Duration")

        for start, end in gaps:
            duration = end - start
            hours = duration.total_seconds() / 3600
            table.add_row(start.strftime("%Y-%m-%d %H:%M"), end.strftime("%Y-%m-%d %H:%M"), f"{hours:.1f} hours")

        console.print(table)


def example_window_calculations(df):
    """Demonstrate safe window-based calculations."""
    console.print(Panel("Example 3: Window-Based Calculations", style="green"))

    # Check if we have enough data for calculations
    for window in [24, 48, 96]:
        has_enough, pct = check_window_data_completeness(df, window)

        if has_enough:
            console.print(f"[green]✓[/green] Enough data for {window}-period window ({pct:.1f}%)")

            # Calculate moving average
            ma = df["close"].rolling(window).mean()

            # Show last few values
            console.print(f"Last {window}-period MA: {ma.iloc[-1]:.2f}")
        else:
            console.print(f"[red]✗[/red] Not enough data for {window}-period window (only {pct:.1f}%)")


def example_reindexing(df, start_time, end_time):
    """Demonstrate safe reindexing for analysis."""
    console.print(Panel("Example 4: Safe Reindexing", style="green"))

    # Create a subset with potential gaps
    subset_end = end_time
    subset_start = subset_end - timedelta(hours=24)

    subset_df = df[(df.index >= subset_start) & (df.index < subset_end)].copy()

    # Deliberately create some gaps for demonstration
    if len(subset_df) > 10:
        indices_to_drop = subset_df.index[5:10]
        subset_df = subset_df.drop(indices_to_drop)

        console.print(f"Created a subset with {len(subset_df)} rows (removed 5 rows)")

    # Safely reindex to create a complete time series
    complete_df = safely_reindex_dataframe(
        subset_df,
        subset_start,
        subset_end,
        interval="15m",
        fill_method="ffill",  # Forward fill missing values
    )

    console.print(f"After reindexing: {len(complete_df)} rows")
    console.print(f"Missing values before fill: {subset_df.isna().sum().sum()}")
    console.print(f"Missing values after fill: {complete_df.isna().sum().sum()}")

    # Show a simple chart of closing prices
    if len(complete_df) > 0:
        try:
            from rich.chart import Chart

            chart = Chart()
            chart.add_item("Original", [x for x in subset_df["close"].values if not pd.isna(x)])
            chart.add_item("Reindexed", [x for x in complete_df["close"].values if not pd.isna(x)])

            console.print(chart)
        except ImportError:
            console.print("Chart rendering requires rich>=10.0.0")


def main():
    """Run the examples."""
    try:
        # Setup
        dsm = setup()

        # Always use timezone-aware datetimes
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=3)

        # Example 1: Basic retrieval
        df = example_timezone_aware_retrieval(dsm)

        # Example 2: Check data completeness
        example_check_data_completeness(df, start_time, end_time)

        # Example 3: Window calculations
        example_window_calculations(df)

        # Example 4: Reindexing
        example_reindexing(df, start_time, end_time)

    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
