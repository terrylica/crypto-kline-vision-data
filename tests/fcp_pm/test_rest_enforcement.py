#!/usr/bin/env python3
"""
Test script to verify that REST API enforcement works correctly in the DataSourceManager.
This script enforces the use of REST API and verifies that the resulting data has the correct source tag.
"""

import sys
from datetime import datetime, timedelta, timezone

from rich import print
from rich.panel import Panel
from rich.table import Table

from core.sync.data_source_manager import DataSource, DataSourceManager
from utils.loguru_setup import logger
from utils.market_constraints import ChartType, DataProvider, Interval, MarketType

# Set log level to DEBUG to see detailed logging
logger.setLevel("DEBUG")


def test_rest_enforcement():
    """Test REST API enforcement in the DataSourceManager."""
    print(
        Panel(
            "[bold green]Testing REST API Enforcement[/bold green]\n"
            "This script verifies that the DataSourceManager correctly enforces the REST API source\n"
            "by checking the _data_source column in the returned DataFrame.",
            expand=False,
            border_style="green",
        )
    )

    # Define test parameters
    symbol = "BTCUSDT"
    market_type = MarketType.SPOT
    interval = Interval.MINUTE_15
    chart_type = ChartType.KLINES
    use_cache = False  # Disable cache to force API requests

    # Set time range - just get 1 hour of data
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    print("[bold cyan]Test Configuration:[/bold cyan]")
    print(f"Symbol: {symbol}")
    print(f"Market Type: {market_type.name}")
    print(f"Interval: {interval.value}")
    print(f"Time Range: {start_time.isoformat()} to {end_time.isoformat()}")
    print(f"Cache Enabled: {use_cache}")
    print("Enforce Source: REST\n")

    try:
        # Create DataSourceManager with cache disabled
        with DataSourceManager(
            provider=DataProvider.BINANCE,
            market_type=market_type,
            chart_type=chart_type,
            use_cache=use_cache,
            retry_count=3,
        ) as manager:
            print(
                "[bold yellow]Fetching data with REST API enforcement...[/bold yellow]"
            )

            # Explicitly enforce REST API as the source
            df = manager.get_data(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                chart_type=chart_type,
                enforce_source=DataSource.REST,  # Enforce REST API
                include_source_info=True,  # Include source information
            )

        # Check if data was retrieved successfully
        if df is None or df.empty:
            print("[bold red]Error: No data retrieved[/bold red]")
            assert False, "No data retrieved"

        # Verify source information
        if "_data_source" not in df.columns:
            print(
                "[bold red]Error: Source information not included in the result[/bold red]"
            )
            assert False, "Source information not included in the result"

        # Check that all data came from REST API
        source_counts = df["_data_source"].value_counts()

        # Display source breakdown
        source_table = Table(title="Data Source Breakdown")
        source_table.add_column("Source", style="cyan")
        source_table.add_column("Records", style="green", justify="right")
        source_table.add_column("Percentage", style="yellow", justify="right")

        all_from_rest = True
        for source, count in source_counts.items():
            percentage = count / len(df) * 100
            source_table.add_row(source, f"{count:,}", f"{percentage:.1f}%")

            if source != "REST":
                all_from_rest = False

        print(source_table)

        # Display sample data
        print("\n[bold cyan]Sample Data:[/bold cyan]")
        print(df.head(2))

        # Final result
        if all_from_rest:
            print(
                "\n[bold green]✓ SUCCESS: All data came from REST API as expected[/bold green]"
            )
            assert True
        else:
            print(
                "\n[bold red]✗ FAILURE: Some data did not come from REST API[/bold red]"
            )
            assert False, "Some data did not come from REST API"

    except Exception as e:
        print(f"[bold red]Error during test: {e}[/bold red]")
        import traceback

        traceback.print_exc()
        assert False, f"Error during test: {e}"


def main():
    """Run the REST API enforcement test."""
    result = test_rest_enforcement()

    if result:
        print(
            Panel(
                "[bold green]REST API Enforcement Test Passed[/bold green]\n"
                "The DataSourceManager correctly enforced the REST API source for all data.",
                border_style="green",
            )
        )
        sys.exit(0)
    else:
        print(
            Panel(
                "[bold red]REST API Enforcement Test Failed[/bold red]\n"
                "The DataSourceManager did not correctly enforce the REST API source.",
                border_style="red",
            )
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
