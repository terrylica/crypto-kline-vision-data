#!/usr/bin/env python3
"""
DSM Logging Control Demo

This demo shows how to control DSM logging levels for clean feature engineering workflows.
It demonstrates the solution to the user's request for configurable logging levels.

Usage:
    # Clean output for feature engineering (suppress DSM logs)
    DSM_LOG_LEVEL=CRITICAL python examples/dsm_logging_demo.py

    # Normal output with DSM info logs
    DSM_LOG_LEVEL=INFO python examples/dsm_logging_demo.py

    # Detailed debugging output
    DSM_LOG_LEVEL=DEBUG python examples/dsm_logging_demo.py

    # Using command line options
    python examples/dsm_logging_demo.py --log-level CRITICAL
    python examples/dsm_logging_demo.py --log-level DEBUG --show-all
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Import DSM components
from core.sync.data_source_manager import DataSourceManager
from utils.loguru_setup import logger
from utils.market_constraints import DataProvider, Interval, MarketType

console = Console()


def demonstrate_logging_levels():
    """Show how different log levels affect DSM output."""
    console.print(Panel.fit("[bold blue]DSM Logging Levels Demo[/bold blue]", border_style="blue"))

    # Create a table showing log level effects
    table = Table(title="DSM Log Level Effects")
    table.add_column("Log Level", style="cyan", no_wrap=True)
    table.add_column("What You See", style="magenta")
    table.add_column("Use Case", style="green")

    table.add_row("CRITICAL", "Only critical errors (connection failures, data corruption)", "Feature engineering workflows - clean output")
    table.add_row("ERROR", "Errors that don't stop execution + critical (DEFAULT)", "Production monitoring and normal usage")
    table.add_row("WARNING", "Data quality warnings, cache misses + errors", "Development with some visibility")
    table.add_row("INFO", "Basic operation info + warnings", "Detailed development and debugging")
    table.add_row("DEBUG", "Detailed debugging info + all above", "Deep debugging and troubleshooting")

    console.print(table)
    console.print()


def demonstrate_environment_control():
    """Show environment variable control."""
    console.print(Panel.fit("[bold green]Environment Variable Control[/bold green]", border_style="green"))

    console.print("[bold]Current environment:[/bold]")
    current_level = os.getenv("DSM_LOG_LEVEL", "ERROR")
    console.print(f"  DSM_LOG_LEVEL = {current_level}")
    console.print(f"  Effective level: {logger.getEffectiveLevel()}")
    console.print()

    console.print("[bold]To control DSM logging:[/bold]")
    console.print("  [cyan]# Clean output for feature engineering[/cyan]")
    console.print("  export DSM_LOG_LEVEL=CRITICAL")
    console.print()
    console.print("  [cyan]# Normal development[/cyan]")
    console.print("  export DSM_LOG_LEVEL=INFO")
    console.print()
    console.print("  [cyan]# Default behavior (errors and critical only)[/cyan]")
    console.print("  # No need to set anything - ERROR is the default")
    console.print()
    console.print("  [cyan]# Detailed debugging[/cyan]")
    console.print("  export DSM_LOG_LEVEL=DEBUG")
    console.print()


def demonstrate_programmatic_control():
    """Show programmatic logging control."""
    console.print(Panel.fit("[bold yellow]Programmatic Control[/bold yellow]", border_style="yellow"))

    console.print("[bold]Option 1: Configure logger directly[/bold]")
    console.print("```python")
    console.print("from utils.loguru_setup import logger")
    console.print("logger.configure_level('CRITICAL')  # Suppress DSM logs")
    console.print("```")
    console.print()

    console.print("[bold]Option 2: Set environment in code[/bold]")
    console.print("```python")
    console.print("import os")
    console.print("os.environ['DSM_LOG_LEVEL'] = 'CRITICAL'")
    console.print("# Import DSM after setting environment")
    console.print("from core.sync.data_source_manager import DataSourceManager")
    console.print("```")
    console.print()


def demonstrate_feature_engineering_workflow():
    """Show clean feature engineering workflow."""
    console.print(Panel.fit("[bold magenta]Feature Engineering Workflow Example[/bold magenta]", border_style="magenta"))

    console.print("[bold]Before (cluttered output):[/bold]")
    console.print("[dim]2024-06-04 10:15:23 | INFO     | dsm_cache_utils:get_from_cache:45 - Checking cache for SOLUSDT")
    console.print("[dim]2024-06-04 10:15:23 | DEBUG    | dsm_fcp_utils:process_cache_step:67 - Cache miss, fetching from source")
    console.print("[dim]2024-06-04 10:15:23 | INFO     | dsm_cache_utils:save_to_cache:123 - Storing data in cache")
    console.print("[dim]... (hundreds of similar lines)")
    console.print()

    console.print("[bold]After (clean output with DSM_LOG_LEVEL=CRITICAL):[/bold]")
    console.print("[green]✓ Feature extraction started")
    console.print("[green]✓ Processing SOLUSDT data...")
    console.print("[green]✓ Feature engineering complete: 1440 records processed")
    console.print()

    console.print("[bold]Code example:[/bold]")
    console.print("```python")
    console.print("# Clean feature engineering code")
    console.print("import os")
    console.print("os.environ['DSM_LOG_LEVEL'] = 'CRITICAL'")
    console.print("")
    console.print("from core.sync.data_source_manager import DataSourceManager")
    console.print("from utils.market_constraints import DataProvider, MarketType, Interval")
    console.print("")
    console.print("# No more logging boilerplate needed!")
    console.print("dsm = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)")
    console.print("data = dsm.get_data(")
    console.print("    symbol='SOLUSDT',")
    console.print("    start_time=start_time,")
    console.print("    end_time=end_time,")
    console.print("    interval=Interval.MINUTE_1,")
    console.print(")")
    console.print("# Clean output - only your feature engineering logs visible")
    console.print("```")
    console.print()


def test_actual_dsm_logging(log_level: str):
    """Test actual DSM logging with the specified level."""
    console.print(Panel.fit(f"[bold cyan]Testing DSM with {log_level} Level[/bold cyan]", border_style="cyan"))

    # Configure the logger to the specified level
    logger.configure_level(log_level)
    console.print(f"[green]Set DSM log level to: {log_level}[/green]")
    console.print(f"[green]Effective level: {logger.getEffectiveLevel()}[/green]")
    console.print()

    try:
        # Create DSM instance (this will generate some logs)
        console.print("[yellow]Creating DataSourceManager...[/yellow]")
        dsm = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

        # Try to get a small amount of recent data (this will generate logs)
        console.print("[yellow]Fetching small data sample...[/yellow]")
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=1)

        # This will demonstrate the logging behavior
        df = dsm.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.MINUTE_1,
        )

        if not df.empty:
            console.print(f"[green]✓ Successfully retrieved {len(df)} records[/green]")
        else:
            console.print("[yellow]⚠ No data retrieved (this is normal for demo)[/yellow]")

        dsm.close()

    except Exception as e:
        console.print(f"[red]Error during DSM test: {e}[/red]")
        console.print("[yellow]This is expected for demo purposes[/yellow]")

    console.print()


def main(
    log_level: str = typer.Option("INFO", "--log-level", "-l", help="Log level to demonstrate"),
    show_all: bool = typer.Option(False, "--show-all", "-a", help="Show all demonstrations"),
    test_dsm: bool = typer.Option(False, "--test-dsm", "-t", help="Test actual DSM logging"),
):
    """Demonstrate DSM logging control capabilities."""

    console.print(
        Panel.fit("[bold blue]DSM Logging Control Demo[/bold blue]\nSolution for clean feature engineering workflows", border_style="blue")
    )

    # Show the benefits
    console.print(
        Panel.fit(
            "[bold green]✅ Problem Solved![/bold green]\n\n"
            "🎯 [bold]Easy Control:[/bold] DSM_LOG_LEVEL=CRITICAL vs 15+ lines of boilerplate\n"
            "🚀 [bold]Clean Output:[/bold] No more cluttered console logs in feature engineering\n"
            "🔧 [bold]Configurable:[/bold] Different log levels for different use cases\n"
            "📝 [bold]No Code Changes:[/bold] Existing DSM code works unchanged",
            border_style="green",
        )
    )

    # Always show the main demonstrations
    demonstrate_logging_levels()
    demonstrate_environment_control()

    if show_all:
        demonstrate_programmatic_control()
        demonstrate_feature_engineering_workflow()

    if test_dsm:
        test_actual_dsm_logging(log_level)

    # Show the solution summary
    console.print(
        Panel.fit(
            "[bold magenta]Implementation Status[/bold magenta]\n\n"
            "✅ [bold]Environment Variable Control:[/bold] DSM_LOG_LEVEL already implemented\n"
            "✅ [bold]Programmatic Control:[/bold] logger.configure_level() available\n"
            "✅ [bold]All DSM Components:[/bold] Use centralized loguru logger\n"
            "✅ [bold]Cleaner Default:[/bold] Default ERROR level for quieter operation\n"
            "✅ [bold]Feature Engineering Ready:[/bold] Set DSM_LOG_LEVEL=CRITICAL for minimal output\n\n"
            "[cyan]The requested logging control is already implemented and ready to use![/cyan]",
            border_style="magenta",
        )
    )


if __name__ == "__main__":
    typer.run(main)
