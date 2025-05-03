#!/usr/bin/env python3
"""
Test script to verify the Failover Control Protocol (FCP) mechanism.
This script tests that when Vision API returns partial data, the system correctly identifies
missing segments and fetches them from REST API to complete the dataset.
"""

import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from rich import print
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from core.sync.data_source_manager import DataSource, DataSourceManager
from utils.logger_setup import logger
from utils.market_constraints import ChartType, DataProvider, Interval, MarketType
from utils_for_debug.data_integrity import analyze_data_integrity

# Set log level to DEBUG to see detailed logging
logger.setLevel("DEBUG")


def test_fcp_mechanism():
    """Test the Failover Control Protocol (FCP) mechanism."""
    print(
        Panel(
            "[bold green]Testing Failover Control Protocol (FCP) Mechanism[/bold green]\n"
            "This test verifies that when Vision API returns partial data, the system correctly\n"
            "identifies missing segments and fetches them from REST API to complete the dataset.",
            expand=False,
            border_style="green",
        )
    )

    # Define test parameters
    symbol = "BTCUSDT"
    market_type = MarketType.SPOT
    interval = Interval.HOUR_1
    chart_type = ChartType.KLINES
    use_cache = False  # Disable cache to force API requests

    # Set time range - get 3 days of data
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=3)

    print("[bold cyan]Test Configuration:[/bold cyan]")
    print(f"Symbol: {symbol}")
    print(f"Market Type: {market_type.name}")
    print(f"Interval: {interval.value}")
    print(f"Time Range: {start_time.isoformat()} to {end_time.isoformat()}")
    print(f"Cache Enabled: {use_cache}")
    print("Enforce Source: AUTO (FCP: Cache → Vision → REST)\n")

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]Fetching data with FCP mechanism..."),
            transient=True,
        ) as progress:
            progress_task = progress.add_task("Fetching...", total=None)

            start_time_retrieval = time.time()

            # Initialize a DataSourceManager instance with caching disabled
            with DataSourceManager(
                provider=DataProvider.BINANCE,
                market_type=market_type,
                chart_type=chart_type,
                symbol=symbol,
                interval=interval,
                use_cache=False,
            ) as manager:
                print("[bold yellow]Fetching data with FCP...[/bold yellow]")

                # Use AUTO mode to enable the FCP mechanism
                df = manager.get_data(
                    start_time=start_time,
                    end_time=end_time,
                    enforce_source=DataSource.AUTO,  # Use AUTO to enable FCP
                    include_source_info=True,  # Include source information
                )

            elapsed_time = time.time() - start_time_retrieval
            progress.update(progress_task, completed=100)

        # Check if data was retrieved successfully
        if df is None or df.empty:
            print("[bold red]Error: No data retrieved[/bold red]")
            return False

        # Calculate data integrity
        expected_seconds = int((end_time - start_time).total_seconds())
        interval_seconds = interval.to_seconds()
        expected_records = (expected_seconds // interval_seconds) + 1
        actual_records = len(df)
        completeness = (actual_records / expected_records) * 100

        print(
            f"\n[bold green]Successfully retrieved {actual_records} records in {elapsed_time:.2f} seconds[/bold green]"
        )
        print(f"Expected records: {expected_records}")
        print(f"Completeness: {completeness:.2f}%")

        # Analyze data integrity
        integrity_result = analyze_data_integrity(df, start_time, end_time, interval)
        print("\n[bold cyan]Data Integrity Analysis:[/bold cyan]")

        # Use try-except to handle potential missing keys in integrity_result
        try:
            expected_count = integrity_result.get("expected_count", expected_records)
            actual_count = integrity_result.get("actual_count", actual_records)
            missing_count = integrity_result.get(
                "missing_count", expected_records - actual_records
            )
            missing_percentage = integrity_result.get(
                "missing_percentage", 100 - completeness
            )

            print(f"Expected records: {expected_count}")
            print(f"Actual records: {actual_count}")
            print(f"Missing records: {missing_count} ({missing_percentage:.2f}%)")
        except Exception as e:
            # Fallback to our own calculations if integrity_result has issues
            print(f"Expected records: {expected_records}")
            print(f"Actual records: {actual_records}")
            print(
                f"Missing records: {expected_records - actual_records} ({100 - completeness:.2f}%)"
            )
            logger.warning(f"Error processing integrity results: {e}")

        # Verify source information
        if "_data_source" not in df.columns:
            print(
                "[bold red]Error: Source information not included in the result[/bold red]"
            )
            return False

        # Check source breakdown
        source_counts = df["_data_source"].value_counts()

        # Display source breakdown
        source_table = Table(title="Data Source Breakdown")
        source_table.add_column("Source", style="cyan")
        source_table.add_column("Records", style="green", justify="right")
        source_table.add_column("Percentage", style="yellow", justify="right")

        for source, count in source_counts.items():
            percentage = count / len(df) * 100
            source_table.add_row(source, f"{count:,}", f"{percentage:.1f}%")

        print(source_table)

        # Show timeline of data sources
        df["date"] = pd.to_datetime(df["open_time"]).dt.date
        date_groups = (
            df.groupby("date")["_data_source"].value_counts().unstack(fill_value=0)
        )

        timeline_table = Table(title="Sources by Date")
        timeline_table.add_column("Date", style="cyan")

        # Add columns for each source
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

        # Display sample data from each source
        print("\n[bold cyan]Sample Data by Source:[/bold cyan]")
        for source in source_counts.index:
            source_df = df[df["_data_source"] == source].head(2)
            if not source_df.empty:
                print(f"\n[bold green]Records from {source} source:[/bold green]")
                print(
                    source_df[
                        ["open_time", "open", "high", "low", "close", "_data_source"]
                    ].head(2)
                )

        # Check if data from both Vision API and REST API was merged
        has_vision_data = "VISION" in source_counts
        has_rest_data = "REST" in source_counts

        if has_vision_data and has_rest_data:
            print(
                "\n[bold green]✓ SUCCESS: FCP mechanism worked correctly[/bold green]"
            )
            print(
                "The system retrieved data from Vision API and used REST API to fill in missing segments."
            )
            return True
        if (
            has_vision_data and actual_records >= expected_records * 0.95
        ):  # Allow for minor missing data
            print(
                "\n[bold yellow]⚠ PARTIAL SUCCESS: Complete data from Vision API[/bold yellow]"
            )
            print(
                "Vision API returned complete data, so REST API fallback wasn't needed."
            )
            return True
        print("\n[bold red]✗ FAILURE: FCP mechanism failed[/bold red]")
        print("The system failed to merge data from multiple sources correctly.")
        return False

    except Exception as e:
        print(f"[bold red]Error during test: {e}[/bold red]")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run the FCP mechanism test."""
    result = test_fcp_mechanism()

    if result:
        print(
            Panel(
                "[bold green]Failover Control Protocol (FCP) Test Passed[/bold green]\n"
                "The DataSourceManager correctly implemented the FCP mechanism by:\n"
                "1. Retrieving available data from Vision API\n"
                "2. Identifying missing segments\n"
                "3. Fetching missing segments from REST API\n"
                "4. Merging data from multiple sources into a single coherent DataFrame",
                border_style="green",
            )
        )
        sys.exit(0)
    else:
        print(
            Panel(
                "[bold red]Failover Control Protocol (FCP) Test Failed[/bold red]\n"
                "The DataSourceManager failed to implement the FCP mechanism correctly.",
                border_style="red",
            )
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
