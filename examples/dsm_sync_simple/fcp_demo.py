#!/usr/bin/env python3
"""
FCP-PM Demo: Demonstrates the Failover Control Protocol and Priority Merge (PCP-PM) mechanism.

This script allows users to specify a time span and observe how the
DataSourceManager automatically retrieves data from different sources
following the Failover Control Protocol and Priority Merge (PCP-PM) strategy:

1. Cache (Local Arrow files)
2. VISION API
3. REST API

It shows real-time source information about where each data point comes from,
and provides a summary of the data source breakdown.
"""

import pandas as pd
from pathlib import Path
import time
from time import perf_counter
import sys
import os
import shutil
from typing import Optional
from enum import Enum
import typer
from typing_extensions import Annotated
import pendulum

# Import the logger or logging and rich formatting
from utils.logger_setup import logger, configure_session_logging
from rich import print

# Rich components - import after enabling smart print
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

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

# Start the performance timer at module initialization
start_time_perf = perf_counter()

# We'll use this cache dir for all demos
CACHE_DIR = Path("./cache")

# Create Typer app with custom rich formatting
app = typer.Typer(
    help="FCP-PM Demo: Demonstrate the Failover Control Protocol and Priority Merge (PCP-PM) mechanism",
    rich_markup_mode="rich",
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
    epilog="""
[bold cyan]Examples:[/bold cyan]

[green]Basic Usage:[/green]
  ./examples/dsm_sync_simple/fcp_demo.py
  ./examples/dsm_sync_simple/fcp_demo.py --symbol ETHUSDT --market spot

[green]Time Range Options:[/green]
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -st 2025-04-05T00:00:00 -et 2025-04-06T00:00:00
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -d 7

[green]Market Types:[/green]
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -m um
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSD_PERP -m cm

[green]Different Intervals:[/green]
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -i 5m
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -i 1h

[green]Data Source Options:[/green]
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -es REST
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -nc
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -cc

[green]Testing FCP-PM Mechanism:[/green]
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -tfp
  ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -tfp -pc

[green]Combined Examples:[/green]
  ./examples/dsm_sync_simple/fcp_demo.py -s ETHUSDT -m um -i 15m -st 2025-04-01 -et 2025-04-10 -r 5 -l DEBUG
  ./examples/dsm_sync_simple/fcp_demo.py -s ETHUSD_PERP -m cm -i 5m -d 10 -tfp -pc -l D -cc
""",
)


# Enum for market types to use with Typer
class MarketTypeChoice(str, Enum):
    SPOT = "spot"
    UM = "um"
    CM = "cm"
    FUTURES_USDT = "futures_usdt"
    FUTURES_COIN = "futures_coin"


# Enum for data sources to use with Typer
class DataSourceChoice(str, Enum):
    AUTO = "AUTO"
    REST = "REST"
    VISION = "VISION"


# Enum for chart types to use with Typer
class ChartTypeChoice(str, Enum):
    KLINES = "klines"
    FUNDING_RATE = "fundingRate"


# Enum for log levels to use with Typer
class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    D = "D"
    I = "I"
    W = "W"
    E = "E"
    C = "C"


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
    """Parse datetime string in ISO format or human readable format using pendulum."""
    logger.debug(f"Attempting to parse datetime string: {dt_str!r}")

    # If input is None, return None
    if dt_str is None:
        logger.debug("Input datetime string is None")
        return None

    try:
        # Use pendulum's powerful parse function which handles most formats
        dt = pendulum.parse(dt_str)
        # Ensure UTC timezone
        if dt.timezone_name != "UTC":
            dt = dt.in_timezone("UTC")
        logger.debug(
            f"Successfully parsed datetime: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')}"
        )
        return dt
    except Exception as e:
        try:
            # Try more explicitly with from_format for certain patterns
            if "T" not in dt_str and ":" in dt_str:
                # Try YYYY-MM-DD HH:MM:SS format
                dt = pendulum.from_format(dt_str, "YYYY-MM-DD HH:mm:ss", tz="UTC")
                logger.debug(
                    f"Successfully parsed with from_format: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')}"
                )
                return dt
            elif len(dt_str) == 10 and "-" in dt_str:
                # Try YYYY-MM-DD format
                dt = pendulum.from_format(dt_str, "YYYY-MM-DD", tz="UTC")
                logger.debug(
                    f"Successfully parsed date-only string: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')}"
                )
                return dt
        except Exception as e2:
            logger.debug(f"Failed specific format parsing: {e2}")

        error_msg = f"Unable to parse datetime: {dt_str!r}. Error: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)


def fetch_data_with_fcp(
    market_type: MarketType,
    symbol: str,
    start_time: pendulum.DateTime,
    end_time: pendulum.DateTime,
    interval: Interval = Interval.MINUTE_1,
    provider: DataProvider = DataProvider.BINANCE,
    chart_type: ChartType = ChartType.KLINES,
    use_cache: bool = True,
    enforce_source: DataSource = DataSource.AUTO,
    max_retries: int = 3,
):
    """
    Fetch data using Failover Control Protocol and Priority Merge (PCP-PM) mechanism.

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
        logger.info(f"Using AUTO source selection (FCP-PM: Cache → Vision → REST)")

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
                # The manager will handle the FCP-PM strategy: cache → Vision API → REST API
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


def display_results(df, symbol, market_type, interval, chart_type, log_timestamp=None):
    """Display the results of the FCP-PM data retrieval."""
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

    # Generate timestamp with pendulum
    timestamp = pendulum.now("UTC").format("YYYYMMDD_HHmmss")

    # Define the CSV path using pendulum timestamp
    csv_dir = Path("logs/fcp_demo")
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / f"{market_str}_{symbol}_{interval}_{timestamp}.csv"

    try:
        df.to_csv(csv_path)
        print(f"\n[bold green]Data saved to: {csv_path}[/bold green]")

        # Display log file paths
        print("\n[bold cyan]Log Files:[/bold cyan]")

        # If log_timestamp is provided, use it for log paths
        if log_timestamp:
            main_log_path = Path(f"logs/fcp_demo_logs/fcp_demo_{log_timestamp}.log")
            error_log_path = Path(
                f"logs/error_logs/fcp_demo_errors_{log_timestamp}.log"
            )
        else:
            # Fall back to timestamp from CSV file if log_timestamp not provided
            main_log_path = Path(f"logs/fcp_demo_logs/fcp_demo_{timestamp}.log")
            error_log_path = Path(f"logs/error_logs/fcp_demo_errors_{timestamp}.log")

        # Check detailed logs
        if main_log_path.exists():
            log_size = main_log_path.stat().st_size
            print(f"[green]Detailed logs: {main_log_path} ({log_size:,} bytes)[/green]")
        else:
            # Try looking for a log file with a similar timestamp (with seconds off by ±10)
            found_log = False
            log_dir = Path("logs/fcp_demo_logs")
            if log_dir.exists():
                for log_file in log_dir.glob("fcp_demo_*.log"):
                    # Only check files from today
                    if timestamp[:8] in log_file.name:
                        found_log = True
                        log_size = log_file.stat().st_size
                        print(
                            f"[green]Detailed logs: {log_file} ({log_size:,} bytes)[/green]"
                        )
                        break

            if not found_log:
                print(
                    f"[yellow]Detailed logs: {main_log_path} (file not found)[/yellow]"
                )

        # Check error logs
        if error_log_path.exists():
            error_size = error_log_path.stat().st_size
            if error_size > 0:
                print(
                    f"[yellow]Error logs: {error_log_path} ({error_size:,} bytes - contains errors)[/yellow]"
                )
            else:
                print(
                    f"[green]Error logs: {error_log_path} (empty - no errors)[/green]"
                )
        else:
            # Try looking for an error log file with a similar timestamp
            found_error_log = False
            error_log_dir = Path("logs/error_logs")
            if error_log_dir.exists():
                for error_file in error_log_dir.glob("fcp_demo_errors_*.log"):
                    # Only check files from today
                    if timestamp[:8] in error_file.name:
                        found_error_log = True
                        error_size = error_file.stat().st_size
                        if error_size > 0:
                            print(
                                f"[yellow]Error logs: {error_file} ({error_size:,} bytes - contains errors)[/yellow]"
                            )
                        else:
                            print(
                                f"[green]Error logs: {error_file} (empty - no errors)[/green]"
                            )
                        break

            if not found_error_log:
                print(f"[yellow]Error logs: {error_log_path} (file not found)[/yellow]")

        print("\n[dim]To view logs: cat logs/fcp_demo_logs/fcp_demo_*.log[/dim]")

        return csv_path
    except Exception as e:
        print(f"[bold red]Error saving data to CSV: {e}[/bold red]")
        return None


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
    """Test the Failover Control Protocol and Priority Merge (FCP-PM) mechanism.

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
        start_date: Start date in YYYY-MM-DD format or full ISO format (optional)
        end_date: End date in YYYY-MM-DD format or full ISO format (optional)
        days: Number of days to fetch if start_date/end_date not provided
        prepare_cache: Whether to prepare cache with partial data first
    """
    current_time = pendulum.now("UTC")

    # Enhanced logging for date parsing
    logger.debug(f"test_fcp_pm_mechanism received start_date: {start_date!r}")
    logger.debug(f"test_fcp_pm_mechanism received end_date: {end_date!r}")

    # Determine the date range
    if start_date and end_date:
        try:
            # Use the parse_datetime function to handle different formats
            start_time = parse_datetime(start_date)
            end_time = parse_datetime(end_date)
            logger.debug(
                f"Successfully parsed dates: {start_time.format('YYYY-MM-DD HH:mm:ss.SSS')} to {end_time.format('YYYY-MM-DD HH:mm:ss.SSS')}"
            )
        except ValueError as e:
            logger.error(f"Error parsing dates: {e}")
            print(f"[bold red]Error parsing dates: {e}[/bold red]")
            # Fallback to default date range
            end_time = current_time
            start_time = end_time.subtract(days=days)
            logger.warning(
                f"Using fallback date range: {start_time.format('YYYY-MM-DD HH:mm:ss.SSS')} to {end_time.format('YYYY-MM-DD HH:mm:ss.SSS')}"
            )
    else:
        # Default to 5 days with end_time as now
        end_time = current_time
        start_time = end_time.subtract(days=days)
        logger.debug(f"Using default date range based on days={days}")

    # Make sure cache directory exists
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    print(
        Panel(
            "[bold green]Testing Failover Control Protocol and Priority Merge (FCP-PM) Mechanism[/bold green]\n"
            f"Symbol: {symbol}\n"
            f"Market: {market_type.name}\n"
            f"Interval: {interval.value}\n"
            f"Date Range: {start_time.format('YYYY-MM-DD HH:mm:ss.SSS')} to {end_time.format('YYYY-MM-DD HH:mm:ss.SSS')}",
            title="FCP-PM Test",
            border_style="green",
        )
    )

    # Divide the full date range into three segments for different sources
    time_range = (end_time - start_time).total_seconds()
    one_third = pendulum.duration(seconds=time_range / 3)

    segment1_start = start_time
    segment1_end = start_time.add(seconds=one_third.total_seconds())

    segment2_start = segment1_end
    segment2_end = segment2_start.add(seconds=one_third.total_seconds())

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

            # Use current log level, don't force DEBUG level
            # original_log_level = logger.level
            # logger.setLevel("DEBUG")

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

            # No need to restore log level since we didn't change it
            # logger.setLevel(original_log_level)

            elapsed_time = time.time() - start_time_retrieval
            progress.update(task, completed=100)

        if full_df is None or full_df.empty:
            print("[bold red]No data retrieved for the full date range[/bold red]")
            return

        print(
            f"[bold green]Retrieved {len(full_df)} records in {elapsed_time:.2f} seconds[/bold green]"
        )

        # Calculate records per second and per minute for this operation
        records_per_second = len(full_df) / elapsed_time if elapsed_time > 0 else 0
        records_per_minute = records_per_second * 60

        # Add performance metrics for the data retrieval
        print(
            f"[cyan]Performance: {records_per_second:.2f} records/second, {records_per_minute:.2f} records/minute[/cyan]"
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

    # Calculate and display script execution time
    end_time_perf = perf_counter()
    elapsed_time = end_time_perf - start_time_perf

    # Calculate records per second and per minute for the entire script
    records_count = 0
    if "full_df" in locals() and full_df is not None and not full_df.empty:
        records_count = len(full_df)

    records_per_second = records_count / elapsed_time if elapsed_time > 0 else 0
    records_per_minute = records_per_second * 60

    # First show performance results
    print(
        Panel(
            f"[cyan]Total script execution time: {elapsed_time:.4f} seconds[/cyan]\n"
            f"[green]Records processed: {records_count:,}[/green]\n"
            f"[yellow]Processing rate: {records_per_second:.2f} records/second, {records_per_minute:.2f} records/minute[/yellow]",
            title="Performance Timing",
            border_style="cyan",
        )
    )

    # Then show summary panel
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


@app.command()
def main(
    # Data Selection
    symbol: Annotated[
        str, typer.Option("--symbol", "-s", help="Trading symbol (e.g., BTCUSDT)")
    ] = "BTCUSDT",
    market: Annotated[
        MarketTypeChoice,
        typer.Option(
            "--market",
            "-m",
            help="Market type: spot, um (USDT-M futures), cm (Coin-M futures)",
        ),
    ] = MarketTypeChoice.SPOT,
    interval: Annotated[
        str, typer.Option("--interval", "-i", help="Time interval (e.g., 1m, 5m, 1h)")
    ] = "1m",
    chart_type: Annotated[
        ChartTypeChoice, typer.Option("--chart-type", "-ct", help="Type of chart data")
    ] = ChartTypeChoice.KLINES,
    # Time Range (mutually exclusive options)
    start_time: Annotated[
        Optional[str],
        typer.Option(
            "--start-time",
            "-st",
            help="Start time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD",
        ),
    ] = "2025-04-01T00:17:23.321",
    end_time: Annotated[
        Optional[str],
        typer.Option(
            "--end-time",
            "-et",
            help="End time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD",
        ),
    ] = "2025-04-03T23:51:09.789",
    days: Annotated[
        int,
        typer.Option(
            "--days",
            "-d",
            help="Number of days to fetch (alternative to start-time/end-time)",
        ),
    ] = 3,
    # Data Source
    enforce_source: Annotated[
        DataSourceChoice,
        typer.Option(
            "--enforce-source", "-es", help="Force specific data source (default: AUTO)"
        ),
    ] = DataSourceChoice.AUTO,
    retries: Annotated[
        int, typer.Option("--retries", "-r", help="Maximum number of retry attempts")
    ] = 3,
    # Cache Control
    no_cache: Annotated[
        bool,
        typer.Option(
            "--no-cache", "-nc", help="Disable caching (cache is enabled by default)"
        ),
    ] = False,
    clear_cache: Annotated[
        bool,
        typer.Option(
            "--clear-cache", "-cc", help="Clear the cache directory before running"
        ),
    ] = False,
    # Test Mode
    test_fcp_pm: Annotated[
        bool,
        typer.Option(
            "--test-fcp-pm",
            "-tfp",
            help="Run the special test for Failover Control Protocol and Priority Merge mechanism",
        ),
    ] = False,
    prepare_cache: Annotated[
        bool,
        typer.Option(
            "--prepare-cache",
            "-pc",
            help="Pre-populate cache with the first segment of data (only used with --test-fcp-pm)",
        ),
    ] = False,
    # Other
    log_level: Annotated[
        LogLevel,
        typer.Option(
            "--log-level",
            "-l",
            help="Set the log level (default: INFO). Shorthand options: D=DEBUG, I=INFO, W=WARNING, E=ERROR, C=CRITICAL",
        ),
    ] = LogLevel.INFO,
):
    """
    FCP-PM Demo: Demonstrates the Failover Control Protocol and Priority Merge (PCP-PM) mechanism.

    This script shows how DataSourceManager automatically retrieves data from different sources:

    1. Cache (Local Arrow files)
    2. VISION API
    3. REST API

    It displays real-time source information about where each data point comes from.

    [bold cyan]Sample Commands:[/bold cyan]

    [green]Basic Usage:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py
      ./examples/dsm_sync_simple/fcp_demo.py --symbol ETHUSDT --market spot

    [green]Time Range Options:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -st 2025-04-05T00:00:00 -et 2025-04-06T00:00:00
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -d 7

    [green]Market Types:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -m um
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSD_PERP -m cm

    [green]Different Intervals:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -i 5m
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -i 1h

    [green]Data Source Options:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -es REST
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -nc
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -cc

    [green]Testing FCP-PM Mechanism:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -tfp
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -tfp -pc

    [green]Combined Examples:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s ETHUSDT -m um -i 15m -st 2025-04-01 -et 2025-04-10 -r 5 -l DEBUG
      ./examples/dsm_sync_simple/fcp_demo.py -s ETHUSD_PERP -m cm -i 5m -d 10 -tfp -pc -l D -cc
    """
    # Convert shorthand log levels to full names
    level = log_level.value
    if level == "D":
        level = "DEBUG"
    elif level == "I":
        level = "INFO"
    elif level == "W":
        level = "WARNING"
    elif level == "E":
        level = "ERROR"
    elif level == "C":
        level = "CRITICAL"

    # Set up session logging (delegated to logger_setup.py)
    main_log, error_log, log_timestamp = configure_session_logging("fcp_demo", level)

    logger.info(f"Current time: {pendulum.now().isoformat()}")

    try:
        print(
            Panel(
                "[bold green]FCP-PM Demo: Failover Control Protocol and Priority Merge (PCP-PM)[/bold green]\n"
                "This script demonstrates how DataSourceManager automatically retrieves data\n"
                "from different sources using the Failover Control Protocol and Priority Merge (PCP-PM) strategy:\n"
                "1. Cache (Local Arrow files)\n"
                "2. VISION API\n"
                "3. REST API",
                expand=False,
                border_style="green",
            )
        )

        # Print logging information
        print(
            Panel(
                f"[bold cyan]Logging Configuration:[/bold cyan]\n"
                f"Detailed logs: [green]{main_log}[/green]\n"
                f"Error logs: [yellow]{error_log}[/yellow]",
                title="Logging Info",
                border_style="blue",
            )
        )

        # Verify project root
        if not verify_project_root():
            sys.exit(1)

        # Show command line arguments grouped by function with clear hierarchy
        print(f"[bold cyan]Command line arguments:[/bold cyan]")
        print(f"[cyan]Data Selection:[/cyan]")
        print(f"  Symbol: {symbol}")
        print(f"  Market: {market.value}")
        print(f"  Interval: {interval}")
        print(f"  Chart type: {chart_type.value}")

        print(f"[cyan]Time Range:[/cyan]")
        print(f"  Start time: {start_time}")
        print(f"  End time: {end_time}")
        print(f"  Days: {days}")

        print(f"[cyan]Data Source:[/cyan]")
        print(f"  Enforce source: {enforce_source.value}")
        print(f"  Retries: {retries}")

        print(f"[cyan]Cache Control:[/cyan]")
        print(f"  No cache: {no_cache}")
        print(f"  Clear cache: {clear_cache}")

        print(f"[cyan]Test Mode:[/cyan]")
        print(f"  Test FCP-PM: {test_fcp_pm}")
        print(f"  Prepare cache: {prepare_cache}")

        print(f"[cyan]Other:[/cyan]")
        print(f"  Log level: {log_level.value}")

        # Clear cache if requested
        if clear_cache:
            clear_cache_directory()

        # Check if we should run the FCP-PM test
        if test_fcp_pm:
            # Add debug logging
            logger.debug(f"Running FCP-PM test with:")
            logger.debug(f"  Symbol: {symbol}")
            logger.debug(f"  Market: {market.value} (converting to enum)")
            logger.debug(f"  Interval: {interval}")
            logger.debug(f"  Start time: {start_time!r}")
            logger.debug(f"  End time: {end_time!r}")
            logger.debug(f"  Days: {days}")
            logger.debug(f"  Prepare cache: {prepare_cache}")

            # Calculate dates based on days parameter if provided
            days_provided = "--days" in sys.argv or "-d" in sys.argv
            if days_provided:
                calculated_end_time = pendulum.now("UTC")
                calculated_start_time = calculated_end_time.subtract(days=days)
                logger.debug(f"Using calculated date range based on days={days}")
                logger.debug(
                    f"Calculated start time: {calculated_start_time.isoformat()}"
                )
                logger.debug(f"Calculated end time: {calculated_end_time.isoformat()}")
                pass_start_date = calculated_start_time.isoformat()
                pass_end_date = calculated_end_time.isoformat()
            else:
                # Use the provided start_time and end_time
                pass_start_date = start_time
                pass_end_date = end_time

            # Run the FCP-PM mechanism test
            test_fcp_pm_mechanism(
                symbol=symbol,
                market_type=MarketType.from_string(market.value),
                interval=Interval(interval),
                chart_type=ChartType.from_string(chart_type.value),
                start_date=pass_start_date,
                end_date=pass_end_date,
                days=days,
                prepare_cache=prepare_cache,
            )
            # Return from function after running test_fcp_pm_mechanism
            # to avoid duplicating performance output
            return

        # Validate and process arguments
        try:
            # Convert market type string to enum
            market_type = MarketType.from_string(market.value)

            # Convert interval string to enum
            interval_enum = Interval(interval)

            # Convert chart type string to enum
            chart_type_enum = ChartType.from_string(chart_type.value)

            # Determine time range
            days_provided = sys.argv and "--days" in sys.argv or "-d" in sys.argv
            if start_time and end_time and not days_provided:
                # Use specified time range
                start_datetime = parse_datetime(start_time)
                end_datetime = parse_datetime(end_time)
            else:
                # Use days parameter to calculate time range
                end_datetime = pendulum.now("UTC")
                start_datetime = end_datetime.subtract(days=days)
                print(
                    f"[yellow]Using dynamic date range based on --days={days}[/yellow]"
                )
                print(f"[yellow]Overriding default start_time and end_time[/yellow]")

            # Process caching option
            use_cache = not no_cache

            # Process enforce source option
            if enforce_source == DataSourceChoice.AUTO:
                enforce_source_enum = DataSource.AUTO
            elif enforce_source == DataSourceChoice.REST:
                enforce_source_enum = DataSource.REST
                logger.debug(f"Enforcing REST API source: {enforce_source_enum}")
            elif enforce_source == DataSourceChoice.VISION:
                enforce_source_enum = DataSource.VISION
            else:
                enforce_source_enum = DataSource.AUTO

            # Adjust symbol for CM market if needed
            symbol_adjusted = symbol
            if market_type == MarketType.FUTURES_COIN and symbol == "BTCUSDT":
                symbol_adjusted = "BTCUSD_PERP"
                print(
                    f"[yellow]Adjusted symbol for CM market: {symbol_adjusted}[/yellow]"
                )

            # Display configuration
            print(f"[bold cyan]Configuration:[/bold cyan]")
            print(f"Market type: {market_type.name}")
            print(f"Symbol: {symbol_adjusted}")
            print(f"Interval: {interval_enum.value}")
            print(f"Chart type: {chart_type_enum.name}")
            print(
                f"Time range: {start_datetime.format('YYYY-MM-DD HH:mm:ss.SSS')} to {end_datetime.format('YYYY-MM-DD HH:mm:ss.SSS')}"
            )
            print(f"Cache enabled: {use_cache}")
            print(f"Enforce source: {enforce_source.value}")
            print(f"Max retries: {retries}")
            print()

            # Fetch data using FCP-PM
            df = fetch_data_with_fcp(
                market_type=market_type,
                symbol=symbol_adjusted,
                start_time=start_datetime,
                end_time=end_datetime,
                interval=interval_enum,
                provider=DataProvider.BINANCE,
                chart_type=chart_type_enum,
                use_cache=use_cache,
                enforce_source=enforce_source_enum,
                max_retries=retries,
            )

            # Display results
            display_results(
                df,
                symbol_adjusted,
                market_type.name.lower(),
                interval_enum.value,
                chart_type_enum.name.lower(),
                log_timestamp,
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

            # Calculate and display script execution time
            end_time_perf = perf_counter()
            elapsed_time = end_time_perf - start_time_perf

            # Calculate records per second and per minute
            records_count = 0 if df is None or df.empty else len(df)
            records_per_second = records_count / elapsed_time if elapsed_time > 0 else 0
            records_per_minute = records_per_second * 60

            print(
                Panel(
                    f"[cyan]Total script execution time: {elapsed_time:.4f} seconds[/cyan]\n"
                    f"[green]Records processed: {records_count:,}[/green]\n"
                    f"[yellow]Processing rate: {records_per_second:.2f} records/second, {records_per_minute:.2f} records/minute[/yellow]",
                    title="Performance Timing",
                    border_style="cyan",
                )
            )

        except ValueError as e:
            print(f"[bold red]Error: {e}[/bold red]")
            import traceback

            traceback.print_exc()
            sys.exit(1)
        except Exception as e:
            try:
                # Safely handle the error to prevent rich text formatting issues
                error_msg = str(e)
                # Sanitize error message to replace non-printable characters
                safe_error_msg = "".join(
                    c if c.isprintable() else f"\\x{ord(c):02x}" for c in error_msg
                )
                print(f"[bold red]CRITICAL ERROR: {safe_error_msg}[/bold red]")
                import traceback

                # Also sanitize the traceback
                tb_str = traceback.format_exc()
                safe_tb = "".join(
                    c if c.isprintable() else f"\\x{ord(c):02x}" for c in tb_str
                )
                print(safe_tb)

                # Even in case of error, display the execution time
                end_time_perf = perf_counter()
                elapsed_time = end_time_perf - start_time_perf
                print(
                    Panel(
                        f"[cyan]Total script execution time: {elapsed_time:.4f} seconds[/cyan]\n"
                        "[red]Unable to calculate processing rate due to error[/red]",
                        title="Performance Timing",
                        border_style="cyan",
                    )
                )
                sys.exit(1)
            except Exception as nested_error:
                # If even our error handling fails, print a simple message without rich formatting
                print("CRITICAL ERROR occurred")
                print(f"Error type: {type(e).__name__}")
                print(f"Error handling also failed: {type(nested_error).__name__}")
                sys.exit(1)

    except Exception as e:
        try:
            # Safely handle the error to prevent rich text formatting issues
            error_msg = str(e)
            # Sanitize error message to replace non-printable characters
            safe_error_msg = "".join(
                c if c.isprintable() else f"\\x{ord(c):02x}" for c in error_msg
            )
            print(f"[bold red]CRITICAL ERROR: {safe_error_msg}[/bold red]")
            import traceback

            # Also sanitize the traceback
            tb_str = traceback.format_exc()
            safe_tb = "".join(
                c if c.isprintable() else f"\\x{ord(c):02x}" for c in tb_str
            )
            print(safe_tb)

            # Even in case of error, show execution time
            end_time_perf = perf_counter()
            elapsed_time = end_time_perf - start_time_perf

            # In case of error, we might not have record count
            print(
                Panel(
                    f"[cyan]Total script execution time: {elapsed_time:.4f} seconds[/cyan]\n"
                    "[red]Unable to calculate processing rate due to error[/red]",
                    title="Performance Timing",
                    border_style="cyan",
                )
            )

            sys.exit(1)
        except Exception as nested_error:
            # If even our error handling fails, print a simple message without rich formatting
            print("CRITICAL ERROR occurred")
            print(f"Error type: {type(e).__name__}")
            print(f"Error handling also failed: {type(nested_error).__name__}")
            sys.exit(1)


if __name__ == "__main__":
    app()
