#!/usr/bin/env python3
"""
Default Logging Level Comparison

This script demonstrates the difference between the old INFO default
and the new ERROR default logging levels in DSM.

Usage:
    python examples/default_logging_comparison.py
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

console = Console()


def test_logging_level(level: str, description: str):
    """Test DSM with a specific logging level."""
    console.print(Panel.fit(f"[bold cyan]Testing: {description}[/bold cyan]", border_style="cyan"))

    # Set the logging level
    os.environ["DSM_LOG_LEVEL"] = level

    # Import DSM after setting the environment variable
    from core.sync.data_source_manager import DataSourceManager
    from utils.market_constraints import DataProvider, Interval, MarketType

    console.print(f"[green]Log level set to: {level}[/green]")
    console.print("[yellow]Creating DataSourceManager and fetching sample data...[/yellow]")
    console.print()

    try:
        # Create DSM instance
        dsm = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

        # Try to get a small amount of recent data
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=30)  # Just 30 minutes

        # This will demonstrate the logging behavior
        df = dsm.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.MINUTE_1,
        )

        if not df.empty:
            console.print(f"[green]✅ Retrieved {len(df)} records[/green]")
        else:
            console.print("[yellow]⚠ No data retrieved (normal for demo)[/yellow]")

        dsm.close()

    except Exception as e:
        console.print(f"[red]Error: {str(e)[:100]}...[/red]")
        console.print("[yellow]This is expected for demo purposes[/yellow]")

    console.print()
    console.print("=" * 80)
    console.print()


def main():
    """Compare different logging levels."""

    console.print(
        Panel.fit(
            "[bold blue]DSM Default Logging Level Comparison[/bold blue]\nDemonstrating the improvement from INFO to ERROR default",
            border_style="blue",
        )
    )

    console.print("[bold magenta]🎯 The Change:[/bold magenta]")
    console.print("• [red]Old default: INFO[/red] - Shows cache checks, FCP steps, DataFrame processing")
    console.print("• [green]New default: ERROR[/green] - Only shows errors and critical issues")
    console.print()

    # Test with INFO level (old default)
    test_logging_level("INFO", "Old Default (INFO) - Verbose Output")

    # Test with ERROR level (new default)
    test_logging_level("ERROR", "New Default (ERROR) - Quiet Output")

    # Test with CRITICAL level (feature engineering)
    test_logging_level("CRITICAL", "Feature Engineering (CRITICAL) - Minimal Output")

    console.print(
        Panel.fit(
            "[bold green]Summary of Improvements[/bold green]\n\n"
            "🔇 [bold]Quieter by Default:[/bold] ERROR level reduces noise significantly\n"
            "🎯 [bold]Better for Production:[/bold] Only important issues are logged\n"
            "🚀 [bold]Cleaner Feature Engineering:[/bold] Use CRITICAL for minimal output\n"
            "🔧 [bold]Flexible Development:[/bold] Use INFO when you need detailed logs\n\n"
            "[cyan]The new ERROR default provides the perfect balance of useful information\n"
            "without overwhelming users with internal DSM operations.[/cyan]",
            border_style="green",
        )
    )


if __name__ == "__main__":
    main()
