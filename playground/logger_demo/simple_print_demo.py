#!/usr/bin/env python3
"""
Simple Print Demo

This script demonstrates how a single import of logger allows you to:
1. Control print output visibility based on log level
2. Get powerful rich formatting for all print statements
3. Properly render rich objects like tables and panels without extra code

Usage:
  python examples/logger_demo/simple_print_demo.py --level INFO  # Shows all prints
  python examples/logger_demo/simple_print_demo.py --level ERROR  # Hides non-essential prints
"""

import argparse

from rich.panel import Panel
from rich.table import Table

from utils.logger_setup import logger


def main():
    """Demonstrate the power of logger.enable_smart_print()."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Simple Print Demo")
    parser.add_argument(
        "--level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO)",
    )
    args = parser.parse_args()

    # Set logging level
    logger.setLevel(args.level)

    # Enable smart print - now all print statements will:
    # 1. Use rich console for best rendering
    # 2. Respect log level (show for DEBUG, INFO, WARNING; hide for ERROR, CRITICAL)
    logger.enable_smart_print(True)

    # Log some messages
    logger.info(f"Log level set to: {args.level}")
    logger.error("This error message is always visible")

    # Simple print with rich formatting - shown at INFO level, hidden at ERROR level
    print("\n[bold green]This text uses rich formatting[/bold green]")
    print("[blue]Blue text[/blue] with [yellow]yellow highlights[/yellow]")

    # Create and print a rich table with a single print statement
    # This is automatically rendered correctly when using smart print
    table = Table(title="Data Table Example")
    table.add_column("Name", style="cyan")
    table.add_column("Value", style="green", justify="right")
    table.add_column("Description", style="yellow")

    table.add_row("Alpha", "100", "First item")
    table.add_row("Beta", "200", "Second item")
    table.add_row("Gamma", "300", "Third item")

    print("\n[bold]Rich Table Example:[/bold]")
    print(table)  # Prints a nicely formatted table (shown at INFO, hidden at ERROR)

    # Create and print a rich panel
    panel = Panel(
        "[bold yellow]Smart Print Demo[/bold yellow]\n"
        "[cyan]Simply import logger and enable_smart_print()[/cyan]\n"
        "[green]All print statements become beautiful and log-level aware[/green]",
        title="Smart Print in Action",
        border_style="blue",
    )

    print("\n[bold]Rich Panel Example:[/bold]")
    print(panel)  # Prints a nicely formatted panel (shown at INFO, hidden at ERROR)

    # Messages that should always be visible regardless of log level
    # can use logger.console.print directly
    logger.console.print(
        "\n[bold red]Important message that's always visible[/bold red]"
    )
    logger.console.print("[yellow]Even when log level is ERROR or CRITICAL[/yellow]")

    # Show conclusion
    logger.console.print("\n[bold cyan]Demo completed![/bold cyan]")
    logger.console.print("Try running with different log levels to see the difference:")
    logger.console.print(
        "  python examples/logger_demo/simple_print_demo.py --level INFO"
    )
    logger.console.print(
        "  python examples/logger_demo/simple_print_demo.py --level ERROR"
    )


if __name__ == "__main__":
    main()
