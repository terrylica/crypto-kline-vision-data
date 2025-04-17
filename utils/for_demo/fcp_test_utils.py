#!/usr/bin/env python3
"""
Test utilities for the Failover Control Protocol (FCP) mechanism.

This module provides test functionality for demonstrating the FCP mechanism
with multiple data sources and segment testing.
"""

import time
from pathlib import Path
from time import perf_counter
from rich import print
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

import pendulum
from utils.market_constraints import MarketType, Interval, DataProvider, ChartType
from core.sync.data_source_manager import DataSourceManager, DataSource

from utils.for_demo.fcp_datetime_parser import calculate_date_range
from utils.for_demo.fcp_cache_utils import ensure_cache_directory
from utils_for_debug.dataframe_output import (
    format_dataframe_for_display,
    save_dataframe_to_csv,
)


def test_fcp_pm_mechanism(
    symbol: str = "BTCUSDT",
    market_type: MarketType = MarketType.SPOT,
    interval: Interval = Interval.MINUTE_1,
    chart_type: ChartType = ChartType.KLINES,
    start_date: str = None,
    end_date: str = None,
    days: int = 5,
    prepare_cache: bool = False,
    cache_dir: Path = Path("./cache"),
    performance_timer_start=None,
):
    """Test the Failover Control Protocol (FCP) mechanism.

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
        cache_dir: Path to the cache directory
        performance_timer_start: Optional start time for performance measurement
    """
    # Use calculated_date_range from our utility module
    start_time, end_time = calculate_date_range(start_date, end_date, days)

    # Make sure cache directory exists
    ensure_cache_directory(cache_dir)

    print(
        Panel(
            "[bold green]Testing Failover Control Protocol (FCP) Mechanism[/bold green]\n"
            f"Symbol: {symbol}\n"
            f"Market: {market_type.name}\n"
            f"Interval: {interval.value}\n"
            f"Date Range: {start_time.format('YYYY-MM-DD HH:mm:ss.SSS')} to {end_time.format('YYYY-MM-DD HH:mm:ss.SSS')}",
            title="FCP Test",
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

    # Now test the FCP mechanism on the full date range
    step_label = "Step 2: " if prepare_cache else ""
    print(
        f"\n[bold cyan]{step_label}Testing FCP mechanism with all segments...[/bold cyan]"
    )
    print(
        "[bold yellow]This should demonstrate the automatic merging of data from different sources[/bold yellow]"
    )

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]Fetching data with FCP..."),
            transient=True,
        ) as progress:
            task = progress.add_task("Fetching...", total=None)

            start_time_retrieval = time.time()

            # Create a fresh DataSourceManager (this will use the cache we prepared)
            with DataSourceManager(
                market_type=market_type,
                provider=DataProvider.BINANCE,
                chart_type=chart_type,
                use_cache=True,
                retry_count=3,
            ) as manager:
                # Retrieve data for the entire range - this should use the FCP mechanism
                full_df = manager.get_data(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval,
                    chart_type=chart_type,
                    enforce_source=DataSource.AUTO,  # AUTO will enable the full FCP mechanism
                    include_source_info=True,
                )

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
        print(f"[bold red]Error testing FCP mechanism: {e}[/bold red]")
        import traceback

        traceback.print_exc()

    # Show performance results if a start timer was provided
    if performance_timer_start:
        end_time_perf = perf_counter()
        elapsed_time = end_time_perf - performance_timer_start

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
            "[bold green]FCP Test Complete[/bold green]\n"
            "This test demonstrated how the DataSourceManager automatically:\n"
            "1. Retrieved data from cache for the first segment\n"
            "2. Retrieved missing data from Vision API for the second segment\n"
            "3. Retrieved remaining data from REST API for the third segment\n"
            "4. Merged all data sources into a single coherent DataFrame",
            title="Summary",
            border_style="green",
        )
    )

    return full_df if "full_df" in locals() else None
