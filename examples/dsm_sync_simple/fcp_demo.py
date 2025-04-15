#!/usr/bin/env python3
"""
FCP Demo: Demonstrates the Failover Composition Priority (FCP) mechanism.

This script allows users to specify a time span and observe how the
DataSourceManager automatically retrieves data from different sources
following the Failover Composition Priority strategy:

1. Cache (Local Arrow files)
2. VISION API
3. REST API

It shows real-time source information about where each data point comes from,
and provides a summary of the data source breakdown.
"""

import argparse
from datetime import datetime, timezone, timedelta
import pandas as pd
from pathlib import Path
import time
import sys
import os
import shutil

# Import the logger for logging and rich formatting
from utils.logger_setup import logger

# Set initial log level (will be overridden by command line args)


logger.setLevel("DEBUG")

# Rich components - import after enabling smart print
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.console import Console
from rich.text import Text
from rich.markdown import Markdown
from rich import box

from utils.market_constraints import MarketType, Interval, DataProvider, ChartType
from core.sync.data_source_manager import DataSourceManager, DataSource
from utils_for_debug.data_integrity import analyze_data_integrity
from utils_for_debug.dataframe_output import (
    log_dataframe_info,
    print_integrity_results,
    format_dataframe_for_display,
    save_dataframe_to_csv,
    print_no_data_message,
)


# We'll use this cache dir for all demos
CACHE_DIR = Path("./cache")


# Custom rich-formatted ArgumentParser
class RichArgumentParser(argparse.ArgumentParser):
    """ArgumentParser with rich-formatted help output."""

    def _print_message(self, message, file=None):
        """Override _print_message to use rich formatting."""
        if message:
            if file is None:
                file = sys.stdout

            # Skip formatting if it's an error message (usually goes to stderr)
            if file == sys.stderr:
                file.write(message)
                return

            # Convert standard argparse output to rich-formatted output
            if message.startswith("usage:"):
                self._print_rich_help()
            else:
                # For other messages, just print normally
                file.write(message)

    def _print_rich_help(self):
        """Print help using rich formatting."""
        console = Console()

        # Program name and description panel
        console.print(
            Panel(
                f"[bold green]{self.description}[/bold green]",
                expand=False,
                border_style="green",
            )
        )

        # Usage section
        usage_text = Text()
        usage_text.append("Usage: ", style="bold")
        usage_text.append(f"{os.path.basename(sys.argv[0])} ")
        usage_text.append("[OPTIONS]", style="yellow")
        console.print(usage_text)
        console.print()

        # Create options table
        options_table = Table(
            title="[bold]Command Options[/bold]",
            box=box.ROUNDED,
            highlight=True,
            title_justify="left",
            expand=True,
        )
        options_table.add_column("Option", style="cyan", no_wrap=True)
        options_table.add_column("Description", style="white")
        options_table.add_column("Default", style="green")

        # Add main options
        options_table.add_row(
            "--symbol SYMBOL", "Trading symbol (e.g., BTCUSDT)", "BTCUSDT"
        )
        options_table.add_row(
            "--market {spot,um,cm,futures_usdt,futures_coin}",
            "Market type: spot, um (USDT-M futures), cm (Coin-M futures)",
            "spot",
        )
        options_table.add_row(
            "--interval INTERVAL", "Time interval (e.g., 1m, 5m, 1h)", "1m"
        )
        options_table.add_row(
            "--retries RETRIES", "Maximum number of retry attempts", "3"
        )
        options_table.add_row(
            "--chart-type {klines,fundingRate}", "Type of chart data", "klines"
        )
        options_table.add_row(
            "--log-level, -l {DEBUG,INFO,...}",
            "Set the log level (see Log Level Options below)",
            "INFO",
        )
        options_table.add_row("-h, --help", "Show this help message and exit", "")

        console.print(options_table)
        console.print()

        # Time Range options
        time_table = Table(
            title="[bold]Time Range Options[/bold]",
            box=box.ROUNDED,
            highlight=True,
            title_justify="left",
            expand=True,
        )
        time_table.add_column("Option", style="cyan", no_wrap=True)
        time_table.add_column("Description", style="white")
        time_table.add_column("Default", style="green")

        time_table.add_row(
            "--start-time START_TIME",
            "Start time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD",
            "",
        )
        time_table.add_row(
            "--end-time END_TIME",
            "End time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD",
            "",
        )
        time_table.add_row(
            "--days DAYS",
            "Number of days to fetch (used if start-time and end-time not provided)",
            "3",
        )

        console.print(time_table)
        console.print()

        # Cache options
        cache_table = Table(
            title="[bold]Cache Options[/bold]",
            box=box.ROUNDED,
            highlight=True,
            title_justify="left",
            expand=True,
        )
        cache_table.add_column("Option", style="cyan", no_wrap=True)
        cache_table.add_column("Description", style="white")
        cache_table.add_column("Default", style="green")

        cache_table.add_row(
            "--no-cache", "Disable caching (cache is enabled by default)", "False"
        )
        cache_table.add_row(
            "--clear-cache", "-cc", "Clear the cache directory before running", "False"
        )

        console.print(cache_table)
        console.print()

        # Source options
        source_table = Table(
            title="[bold]Source Options[/bold]",
            box=box.ROUNDED,
            highlight=True,
            title_justify="left",
            expand=True,
        )
        source_table.add_column("Option", style="cyan", no_wrap=True)
        source_table.add_column("Description", style="white")
        source_table.add_column("Default", style="green")

        source_table.add_row(
            "--enforce-source {AUTO,REST,VISION}", "Force specific data source", "AUTO"
        )

        console.print(source_table)
        console.print()

        # Log Level options
        level_table = Table(
            title="[bold]Log Level Options[/bold]",
            box=box.ROUNDED,
            highlight=True,
            title_justify="left",
            expand=True,
        )
        level_table.add_column("Option", style="cyan", no_wrap=True)
        level_table.add_column("Log Level", style="white")
        level_table.add_column("Shorthand", style="green")

        level_table.add_row("--log-level DEBUG, -l D", "Debug (most verbose)", "D")
        level_table.add_row("--log-level INFO, -l I", "Information (default)", "I")
        level_table.add_row("--log-level WARNING, -l W", "Warning", "W")
        level_table.add_row("--log-level ERROR, -l E", "Error", "E")
        level_table.add_row(
            "--log-level CRITICAL, -l C", "Critical (least verbose)", "C"
        )

        console.print(level_table)
        console.print()

        # Examples section
        if self.epilog:
            # Create a better examples display with custom tables
            console.print("\n[bold]Examples:[/bold]")

            # Parse examples from epilog - this assumes specific format with ## headers and examples in code blocks
            # We'll create a more structured display
            import re

            sections = re.split(r"##\s+([^\n]+)", self.epilog)[
                1:
            ]  # Split on ## headers, skip first empty element

            # Process sections in pairs (title, content)
            for i in range(0, len(sections), 2):
                if i + 1 < len(sections):
                    title = sections[i].strip()
                    content = sections[i + 1].strip()

                    # Extract example commands (between ``` ```)
                    examples = re.findall(r"```\s*(.*?)\s*```", content, re.DOTALL)

                    # Create section table
                    console.print(f"\n[bold cyan]{title}[/bold cyan]")

                    for example in examples:
                        lines = example.strip().split("\n")
                        description = ""
                        command = ""

                        # First line with # is description, rest is command
                        for line in lines:
                            if line.startswith("#"):
                                description = line[1:].strip()
                            elif line.strip() and not description:
                                # If we found a non-empty line before description, it's not formatted correctly
                                command = line.strip()
                            elif line.strip():
                                # Add to command
                                if command:
                                    command += f"\n{line.strip()}"
                                else:
                                    command = line.strip()

                        # Display without a panel - just use colors and spacing
                        console.print(f"[yellow]• {description}[/yellow]")
                        console.print(
                            f"[green]  {command.replace(chr(10), chr(10) + '  ')}[/green]"
                        )
                        console.print()


def clear_cache_directory():
    """Remove the cache directory and its contents."""
    if CACHE_DIR.exists():
        logger.info(f"Clearing cache directory: {CACHE_DIR}")
        print(f"[bold yellow]Removing cache directory: {CACHE_DIR}[/bold yellow]")
        shutil.rmtree(CACHE_DIR, ignore_errors=True)
        print(f"[bold green]Cache directory removed successfully[/bold green]")
    else:
        logger.info(f"Cache directory does not exist: {CACHE_DIR}")
        print(f"[bold yellow]Cache directory does not exist: {CACHE_DIR}[/bold yellow]")


def verify_project_root():
    """Verify that we're running from the project root directory."""
    if os.path.isdir("core") and os.path.isdir("utils") and os.path.isdir("examples"):
        # Already in project root
        print("Running from project root directory")
        return True

    # Try to navigate to project root if we're in the example directory
    if os.path.isdir("../../core") and os.path.isdir("../../utils"):
        os.chdir("../..")
        print(f"Changed to project root directory: {os.getcwd()}")
        return True

    print("[bold red]Error: Unable to locate project root directory[/bold red]")
    print(
        "Please run this script from either the project root or the examples/dsm_sync_simple directory"
    )
    return False


def parse_datetime(dt_str):
    """Parse datetime string in ISO format or human readable format."""
    try:
        # Try ISO format first (YYYY-MM-DDTHH:MM:SS)
        return datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            # Try more flexible format (YYYY-MM-DD HH:MM:SS)
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            try:
                # Try date only format (YYYY-MM-DD)
                return datetime.strptime(dt_str, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                raise ValueError(
                    f"Unable to parse datetime: {dt_str}. Please use ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD HH:MM:SS or YYYY-MM-DD"
                )


def fetch_data_with_fcp(
    market_type: MarketType,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: Interval = Interval.MINUTE_1,
    provider: DataProvider = DataProvider.BINANCE,
    chart_type: ChartType = ChartType.KLINES,
    use_cache: bool = True,
    enforce_source: DataSource = DataSource.AUTO,
    max_retries: int = 3,
):
    """
    Fetch data using Failover Composition Priority (FCP) mechanism.

    Args:
        market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
        symbol: Symbol to retrieve data for (e.g., "BTCUSDT")
        start_time: Start time for data retrieval
        end_time: End time for data retrieval
        interval: Time interval between data points
        provider: Data provider (currently only BINANCE is supported)
        chart_type: Type of chart data to retrieve (KLINES, FUNDING_RATE)
        use_cache: Whether to use caching
        enforce_source: Force specific data source (AUTO, REST, VISION)
        max_retries: Maximum number of retry attempts

    Returns:
        Pandas DataFrame containing the retrieved data
    """
    logger.info(
        f"Retrieving {interval.value} {chart_type.name} data for {symbol} in {market_type.name} market"
    )
    logger.info(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")
    logger.info(f"Cache enabled: {use_cache}")

    if enforce_source != DataSource.AUTO:
        logger.info(f"Enforcing data source: {enforce_source.name}")

    logger.info(
        f"[bold red]Attempting[/bold red] to fetch data from {start_time.isoformat()} to {end_time.isoformat()}..."
    )

    # Calculate expected record count for validation
    interval_seconds = interval.to_seconds()
    expected_seconds = int((end_time - start_time).total_seconds())
    expected_records = (expected_seconds // interval_seconds) + 1
    logger.debug(
        f"Expected record count: {expected_records} for {expected_seconds} seconds range"
    )

    # Enhanced logging for the enforce_source parameter
    if enforce_source == DataSource.REST:
        logger.info(
            f"Explicitly enforcing REST API as the data source (bypassing Vision API)"
        )
    elif enforce_source == DataSource.VISION:
        logger.info(
            f"Explicitly enforcing VISION API as the data source (no REST fallback)"
        )
    else:
        logger.info(f"Using AUTO source selection (FCP: Cache → Vision → REST)")

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]Fetching data..."),
            transient=True,
        ) as progress:
            progress_task = progress.add_task("Fetching...", total=None)

            start_time_retrieval = time.time()

            # Create a DataSourceManager instance with the specified parameters
            with DataSourceManager(
                market_type=market_type,
                provider=provider,
                chart_type=chart_type,
                use_cache=use_cache,
                retry_count=max_retries,
            ) as manager:
                # Retrieve data using the manager
                # The manager will handle the FCP strategy: cache → Vision API → REST API
                df = manager.get_data(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval,
                    chart_type=chart_type,
                    enforce_source=enforce_source,
                    include_source_info=True,  # Always include source information
                )

            elapsed_time = time.time() - start_time_retrieval
            progress.update(progress_task, completed=100)

        if df is None or df.empty:
            logger.warning(f"No data retrieved for {symbol}")
            print_no_data_message(
                symbol,
                market_type,
                interval,
                start_time,
                end_time,
                enforce_source,
                use_cache,
            )
            return pd.DataFrame()

        logger.info(
            f"Retrieved {len(df)} records for {symbol} in {elapsed_time:.2f} seconds"
        )

        # Analyze data integrity
        logger.debug("Analyzing data integrity...")
        integrity_result = analyze_data_integrity(df, start_time, end_time, interval)

        # Print the integrity results in a user-friendly format
        print_integrity_results(integrity_result)

        # Log DataFrame structure information for debugging
        log_dataframe_info(df)

        return df
    except Exception as e:
        print(f"[bold red]Error fetching data: {e}[/bold red]")
        import traceback

        traceback.print_exc()
        return pd.DataFrame()


def display_results(df, symbol, market_type, interval, chart_type):
    """Display the results of the FCP data retrieval."""
    if df is None or df.empty:
        print("[bold red]No data to display[/bold red]")
        return

    print(f"\n[bold green]Successfully retrieved {len(df)} records[/bold green]")

    # Create a table for source breakdown
    if "_data_source" in df.columns:
        source_counts = df["_data_source"].value_counts()

        source_table = Table(title="Data Source Breakdown")
        source_table.add_column("Source", style="cyan")
        source_table.add_column("Records", style="green", justify="right")
        source_table.add_column("Percentage", style="yellow", justify="right")

        for source, count in source_counts.items():
            percentage = count / len(df) * 100
            source_table.add_row(source, f"{count:,}", f"{percentage:.1f}%")

        print(source_table)

        # Show sample data from each source
        print(f"\n[bold cyan]Sample Data by Source:[/bold cyan]")
        for source in source_counts.index:
            source_df = df[df["_data_source"] == source].head(2)
            if not source_df.empty:
                print(f"\n[bold green]Records from {source} source:[/bold green]")
                # Format the display for better readability
                display_df = format_dataframe_for_display(source_df)
                # Display in a clean format
                print(display_df)
    else:
        print(
            "[bold yellow]Warning: Source information not available in the data[/bold yellow]"
        )

    # Save data to CSV
    # Convert market_type to string if it's an enum
    market_str = (
        market_type.name.lower()
        if hasattr(market_type, "name")
        else market_type.lower()
    )
    csv_path = save_dataframe_to_csv(df, market_str, symbol, interval)
    if csv_path:
        print(f"\n[bold green]Data saved to: {csv_path}[/bold green]")


def test_fcp_pm_mechanism(
    symbol: str = "BTCUSDT",
    market_type: MarketType = MarketType.SPOT,
    interval: Interval = Interval.MINUTE_1,
    chart_type: ChartType = ChartType.KLINES,
    start_date: str = None,
    end_date: str = None,
    days: int = 5,
    prepare_cache: bool = False,
):
    """Test the Failover Composition and Parcel Merge (FCP-PM) mechanism.

    This function demonstrates how DataSourceManager combines data from multiple sources:
    1. First retrieves data from local cache
    2. Then fetches missing segments from Vision API
    3. Finally fetches any remaining gaps from REST API

    For proper demonstration, this will:
    1. First set up the cache with partial data
    2. Then request a time span that requires all three sources
    3. Show detailed logs of each merge operation

    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")
        market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
        interval: Time interval between data points
        chart_type: Type of chart data
        start_date: Start date in YYYY-MM-DD format (optional)
        end_date: End date in YYYY-MM-DD format (optional)
        days: Number of days to fetch if start_date/end_date not provided
        prepare_cache: Whether to prepare cache with partial data first
    """
    current_time = datetime.now(timezone.utc)

    # Determine the date range
    if start_date and end_date:
        start_time = datetime.fromisoformat(f"{start_date}T00:00:00").replace(
            tzinfo=timezone.utc
        )
        end_time = datetime.fromisoformat(f"{end_date}T23:59:59").replace(
            tzinfo=timezone.utc
        )
    else:
        # Default to 5 days with end_time as now
        end_time = current_time
        start_time = end_time - timedelta(days=days)

    # Make sure cache directory exists
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    print(
        Panel(
            f"[bold green]Testing Failover Composition and Parcel Merge (FCP-PM) Mechanism[/bold green]\n"
            f"Symbol: {symbol}\n"
            f"Market: {market_type.name}\n"
            f"Interval: {interval.value}\n"
            f"Date Range: {start_time.isoformat()} to {end_time.isoformat()}",
            title="FCP-PM Test",
            border_style="green",
        )
    )

    # Divide the full date range into three segments for different sources
    time_range = (end_time - start_time).total_seconds()
    one_third = timedelta(seconds=time_range / 3)

    segment1_start = start_time
    segment1_end = start_time + one_third

    segment2_start = segment1_end
    segment2_end = segment2_start + one_third

    segment3_start = segment2_end
    segment3_end = end_time

    # Print segments
    print(f"[bold cyan]Testing with 3 segments:[/bold cyan]")
    print(
        f"Segment 1: {segment1_start.isoformat()} to {segment1_end.isoformat()} (Target: CACHE)"
    )
    print(
        f"Segment 2: {segment2_start.isoformat()} to {segment2_end.isoformat()} (Target: VISION API)"
    )
    print(
        f"Segment 3: {segment3_start.isoformat()} to {segment3_end.isoformat()} (Target: REST API)"
    )

    # Skip pre-population step if not requested
    if prepare_cache:
        print(
            "\n[bold cyan]Step 1: Pre-populating cache with first segment data...[/bold cyan]"
        )

        # First, get data for segment 1 from REST API and save to cache
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]Fetching segment 1 data to cache..."),
            transient=True,
        ) as progress:
            task = progress.add_task("Fetching...", total=None)

            # Create DataSourceManager with caching enabled
            with DataSourceManager(
                market_type=market_type,
                provider=DataProvider.BINANCE,
                chart_type=chart_type,
                use_cache=True,
                retry_count=3,
            ) as manager:
                # Fetch data using REST API for the first segment and save to cache
                segment1_df = manager.get_data(
                    symbol=symbol,
                    start_time=segment1_start,
                    end_time=segment1_end,
                    interval=interval,
                    chart_type=chart_type,
                    enforce_source=DataSource.REST,  # Force REST API for segment 1
                    include_source_info=True,
                )

            progress.update(task, completed=100)

        if segment1_df is not None and not segment1_df.empty:
            print(
                f"[bold green]Successfully cached {len(segment1_df)} records for segment 1[/bold green]"
            )
        else:
            print("[bold red]Failed to cache data for segment 1[/bold red]")
            return

    # Now test the FCP-PM mechanism on the full date range
    step_label = "Step 2: " if prepare_cache else ""
    print(
        f"\n[bold cyan]{step_label}Testing FCP-PM mechanism with all segments...[/bold cyan]"
    )
    print(
        "[bold yellow]This should demonstrate the automatic merging of data from different sources[/bold yellow]"
    )

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]Fetching data with FCP-PM..."),
            transient=True,
        ) as progress:
            task = progress.add_task("Fetching...", total=None)

            start_time_retrieval = time.time()

            # Set log level to DEBUG temporarily to see detailed merging logs
            original_log_level = logger.level
            logger.setLevel("DEBUG")

            # Create a fresh DataSourceManager (this will use the cache we prepared)
            with DataSourceManager(
                market_type=market_type,
                provider=DataProvider.BINANCE,
                chart_type=chart_type,
                use_cache=True,
                retry_count=3,
            ) as manager:
                # Retrieve data for the entire range - this should use the FCP-PM mechanism
                full_df = manager.get_data(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval,
                    chart_type=chart_type,
                    enforce_source=DataSource.AUTO,  # AUTO will enable the full FCP-PM mechanism
                    include_source_info=True,
                )

            # Restore original log level
            logger.setLevel(original_log_level)

            elapsed_time = time.time() - start_time_retrieval
            progress.update(task, completed=100)

        if full_df is None or full_df.empty:
            print("[bold red]No data retrieved for the full date range[/bold red]")
            return

        print(
            f"[bold green]Retrieved {len(full_df)} records in {elapsed_time:.2f} seconds[/bold green]"
        )

        # Analyze and display the source breakdown
        if "_data_source" in full_df.columns:
            source_counts = full_df["_data_source"].value_counts()

            source_table = Table(title="Data Source Breakdown")
            source_table.add_column("Source", style="cyan")
            source_table.add_column("Records", style="green", justify="right")
            source_table.add_column("Percentage", style="yellow", justify="right")

            for source, count in source_counts.items():
                percentage = count / len(full_df) * 100
                source_table.add_row(source, f"{count:,}", f"{percentage:.1f}%")

            print(source_table)

            # Show timeline visualization of source distribution
            print("\n[bold cyan]Source Distribution Timeline:[/bold cyan]")

            # First, create a new column with the date part only
            full_df["date"] = full_df["open_time"].dt.date
            date_groups = (
                full_df.groupby("date")["_data_source"]
                .value_counts()
                .unstack(fill_value=0)
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

            print(timeline_table)

            # Show sample data from each source
            print(f"\n[bold cyan]Sample Data by Source:[/bold cyan]")
            for source in source_counts.index:
                source_df = full_df[full_df["_data_source"] == source].head(2)
                if not source_df.empty:
                    print(f"\n[bold green]Records from {source} source:[/bold green]")
                    display_df = format_dataframe_for_display(source_df)
                    print(display_df)
        else:
            print(
                "[bold yellow]Warning: Source information not available in the data[/bold yellow]"
            )

        # Save data to CSV
        csv_path = save_dataframe_to_csv(
            full_df, market_type.name.lower(), symbol, interval
        )
        if csv_path:
            print(f"\n[bold green]Data saved to: {csv_path}[/bold green]")

    except Exception as e:
        print(f"[bold red]Error testing FCP-PM mechanism: {e}[/bold red]")
        import traceback

        traceback.print_exc()

    print(
        Panel(
            "[bold green]FCP-PM Test Complete[/bold green]\n"
            "This test demonstrated how the DataSourceManager automatically:\n"
            "1. Retrieved data from cache for the first segment\n"
            "2. Retrieved missing data from Vision API for the second segment\n"
            "3. Retrieved remaining data from REST API for the third segment\n"
            "4. Merged all data sources into a single coherent DataFrame",
            title="Summary",
            border_style="green",
        )
    )


def parse_arguments():
    """Parse command line arguments."""
    # Create examples based on successfully tested commands with a simpler format
    # that our custom parser can handle
    examples = """
## Basic Usage
```
# Basic usage with default parameters (BTCUSDT on spot market for the last 3 days)
python examples/dsm_sync_simple/fcp_demo.py
```

## Market Types and Intervals
```
# Fetch Ethereum data with 15-minute intervals and detailed logging
python examples/dsm_sync_simple/fcp_demo.py --symbol ETHUSDT --market spot --interval 15m --days 1 --log-level DEBUG
```

```
# Use REST API as source for Bitcoin data from USDT-M futures with cache cleared
python examples/dsm_sync_simple/fcp_demo.py --symbol BTCUSDT --market um --days 1 --enforce-source REST --clear-cache
```

## Date Range Specification
```
# Fetch hourly Bitcoin spot data for a specific date range
python examples/dsm_sync_simple/fcp_demo.py --symbol BTCUSDT --market spot \\
  --start-time "2025-04-13" --end-time "2025-04-14" --interval 1h
```

## Log Level Shorthand Options
```
# Using shorthand log level options for debugging
python examples/dsm_sync_simple/fcp_demo.py -l D  # Same as --log-level DEBUG

# Using warning log level with shorthand
python examples/dsm_sync_simple/fcp_demo.py -l W  # Same as --log-level WARNING
```

## FCP-PM Mechanism Test
```
# Test the Failover Composition and Parcel Merge mechanism
python examples/dsm_sync_simple/fcp_demo.py --test-fcp-pm --days 5 --interval 1h
```
"""

    parser = RichArgumentParser(
        description="FCP Demo: Demonstrate the Failover Composition Priority mechanism",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Required arguments
    parser.add_argument(
        "--symbol", type=str, default="BTCUSDT", help="Trading symbol (e.g., BTCUSDT)"
    )

    parser.add_argument(
        "--market",
        type=str,
        default="spot",
        choices=["spot", "um", "cm", "futures_usdt", "futures_coin"],
        help="Market type: spot, um (USDT-M futures), cm (Coin-M futures)",
    )

    parser.add_argument(
        "--interval", type=str, default="1m", help="Time interval (e.g., 1m, 5m, 1h)"
    )

    # Time range arguments
    time_group = parser.add_argument_group("Time Range")
    time_group.add_argument(
        "--start-time",
        type=str,
        help="Start time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD",
    )

    time_group.add_argument(
        "--end-time",
        type=str,
        help="End time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD",
    )

    time_group.add_argument(
        "--days",
        type=int,
        default=3,
        help="Number of days to fetch (used if start-time and end-time not provided)",
    )

    # Cache options
    cache_group = parser.add_argument_group("Cache Options")
    cache_group.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable caching (cache is enabled by default)",
    )

    cache_group.add_argument(
        "--clear-cache",
        "-cc",
        action="store_true",
        help="Clear the cache directory before running",
    )

    # Source options
    source_group = parser.add_argument_group("Source Options")
    source_group.add_argument(
        "--enforce-source",
        type=str,
        choices=["AUTO", "REST", "VISION"],
        default="AUTO",
        help="Force specific data source (default: AUTO)",
    )

    # FCP-PM Test option
    parser.add_argument(
        "--test-fcp-pm",
        action="store_true",
        help="Run the special test for Failover Composition and Parcel Merge mechanism",
    )

    # Other options
    parser.add_argument(
        "--retries", type=int, default=3, help="Maximum number of retry attempts"
    )

    parser.add_argument(
        "--chart-type",
        type=str,
        choices=["klines", "fundingRate"],
        default="klines",
        help="Type of chart data",
    )

    # Add a log level option to demonstrate rich output control
    parser.add_argument(
        "--log-level",
        "-l",
        type=str,
        choices=[
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
            "D",
            "I",
            "W",
            "E",
            "C",
        ],
        default="INFO",
        help="Set the log level (default: INFO). Shorthand options: D=DEBUG, I=INFO, W=WARNING, E=ERROR, C=CRITICAL",
    )

    # Add a new argument for prepare_cache
    parser.add_argument(
        "--prepare-cache",
        action="store_true",
        help="Pre-populate cache with the first segment of data (for FCP-PM test)",
    )

    # For --help flag, use our custom formatter
    if "-h" in sys.argv or "--help" in sys.argv:
        parser.print_help()
        sys.exit(0)

    return parser.parse_args()


def main():
    logger.info(f"Current time: {datetime.now().isoformat()}")
    """Run the FCP demo."""
    try:
        print(
            Panel(
                "[bold green]FCP Demo: Failover Composition Priority[/bold green]\n"
                "This script demonstrates how DataSourceManager automatically retrieves data\n"
                "from different sources using the Failover Composition Priority strategy:\n"
                "1. Cache (Local Arrow files)\n"
                "2. VISION API\n"
                "3. REST API",
                expand=False,
                border_style="green",
            )
        )

        # Verify project root
        if not verify_project_root():
            sys.exit(1)

        # Parse arguments
        args = parse_arguments()
        print(f"[bold cyan]Command line arguments:[/bold cyan] {args}")

        # Set the log level
        # Convert shorthand to full names
        log_level = args.log_level
        if log_level == "D":
            log_level = "DEBUG"
        elif log_level == "I":
            log_level = "INFO"
        elif log_level == "W":
            log_level = "WARNING"
        elif log_level == "E":
            log_level = "ERROR"
        elif log_level == "C":
            log_level = "CRITICAL"

        logger.setLevel(log_level)
        print(f"[bold cyan]Log level set to:[/bold cyan] {log_level}")

        # Clear cache if requested
        if args.clear_cache:
            clear_cache_directory()

        # Check if we should run the FCP-PM test
        if hasattr(args, "test_fcp_pm") and args.test_fcp_pm:
            # Run the FCP-PM mechanism test
            test_fcp_pm_mechanism(
                symbol=args.symbol,
                market_type=MarketType.from_string(args.market),
                interval=Interval(args.interval),
                chart_type=ChartType.from_string(args.chart_type),
                start_date=args.start_time,
                end_date=args.end_time,
                days=args.days,
                prepare_cache=args.prepare_cache,
            )
            return

        # Validate and process arguments
        try:
            # Convert market type string to enum
            market_type = MarketType.from_string(args.market)

            # Convert interval string to enum
            interval = Interval(args.interval)

            # Convert chart type string to enum
            chart_type = ChartType.from_string(args.chart_type)

            # Determine time range
            if args.start_time and args.end_time:
                # Use specified time range
                start_time = parse_datetime(args.start_time)
                end_time = parse_datetime(args.end_time)
            else:
                # Use days parameter to calculate time range
                end_time = datetime.now(timezone.utc)
                start_time = end_time - timedelta(days=args.days)

            # Process caching option
            use_cache = not args.no_cache

            # Process enforce source option
            if args.enforce_source == "AUTO":
                enforce_source = DataSource.AUTO
            elif args.enforce_source == "REST":
                enforce_source = DataSource.REST
                logger.debug(f"Enforcing REST API source: {enforce_source}")
            elif args.enforce_source == "VISION":
                enforce_source = DataSource.VISION
            else:
                enforce_source = DataSource.AUTO

            # Adjust symbol for CM market if needed
            symbol = args.symbol
            if market_type == MarketType.FUTURES_COIN and symbol == "BTCUSDT":
                symbol = "BTCUSD_PERP"
                print(f"[yellow]Adjusted symbol for CM market: {symbol}[/yellow]")

            # Display configuration
            print(f"[bold cyan]Configuration:[/bold cyan]")
            print(f"Market type: {market_type.name}")
            print(f"Symbol: {symbol}")
            print(f"Interval: {interval.value}")
            print(f"Chart type: {chart_type.name}")
            print(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")
            print(f"Cache enabled: {use_cache}")
            print(f"Enforce source: {args.enforce_source}")
            print(f"Max retries: {args.retries}")
            print()

            # Fetch data using FCP
            df = fetch_data_with_fcp(
                market_type=market_type,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                provider=DataProvider.BINANCE,
                chart_type=chart_type,
                use_cache=use_cache,
                enforce_source=enforce_source,
                max_retries=args.retries,
            )

            # Display results
            display_results(
                df,
                symbol,
                market_type.name.lower(),
                interval.value,
                chart_type.name.lower(),
            )

            # Add note about log level and rich output
            print(
                Panel(
                    "[bold cyan]Note about Log Level and Rich Output:[/bold cyan]\n"
                    "- When log level is DEBUG, INFO, or WARNING: Rich output is visible\n"
                    "- When log level is ERROR or CRITICAL: Rich output is suppressed\n\n"
                    "Try running with different log levels to see the difference:\n"
                    "  python examples/dsm_sync_simple/fcp_demo.py --log-level ERROR\n"
                    "  python examples/dsm_sync_simple/fcp_demo.py -l E (shorthand for ERROR)\n",
                    title="Rich Output Control",
                    border_style="blue",
                )
            )

        except ValueError as e:
            print(f"[bold red]Error: {e}[/bold red]")
            import traceback

            traceback.print_exc()
            sys.exit(1)
        except Exception as e:
            print(f"[bold red]Unexpected error: {e}[/bold red]")
            import traceback

            traceback.print_exc()
            sys.exit(1)
    except Exception as e:
        print(f"[bold red]CRITICAL ERROR: {e}[/bold red]")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
