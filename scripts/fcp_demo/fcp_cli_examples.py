#!/usr/bin/env python3
"""
CLI example utilities for the Failover Control Protocol (FCP) mechanism.
"""

from typing import List, Dict, Any, Callable
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax


def define_example_commands() -> List[Dict[str, Any]]:
    """
    Define example commands for demonstration purposes.

    Returns:
        List of dictionaries containing example command definitions
    """
    # Example 1: Basic SPOT market with BTC
    example1 = {
        "title": "Fetch recent BTCUSDT 1-minute data from SPOT market",
        "description": "Retrieves 3 days of 1-minute candles for BTCUSDT in SPOT market with default settings",
        "command": "./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -m spot -i 1m",
        "explanation": "This is the simplest form of data retrieval, using default settings for most parameters.",
        "args": ["-s", "BTCUSDT", "-m", "spot", "-i", "1m"],
    }

    # Example 2: Coin-M futures market example
    example2 = {
        "title": "Fetch BTCUSD_PERP from Coin-M futures market with specific date range",
        "description": "Gets data for a specific date range with debug logging enabled",
        "command": "./examples/dsm_sync_simple/fcp_demo.py -s BTCUSD_PERP -m cm -i 1m -l D -st 2025-04-01 -et 2025-04-05",
        "explanation": "Uses Coin-M futures market (cm) with a specific date range and debug logging enabled.",
        "args": [
            "-s",
            "BTCUSD_PERP",
            "-m",
            "cm",
            "-i",
            "1m",
            "-l",
            "D",
            "-st",
            "2025-04-01",
            "-et",
            "2025-04-05",
        ],
    }

    # Example 3: USDT-M futures example
    example3 = {
        "title": "Fetch data from USDT-M futures with cache clearing",
        "description": "Demonstrates how to clear cache before fetching data",
        "command": "./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -m um -i 1m -cc -d 5",
        "explanation": "Clears the cache directory before fetching 5 days of data from USDT-M futures market.",
        "args": ["-s", "BTCUSDT", "-m", "um", "-i", "1m", "-cc", "-d", "5"],
    }

    # Example 4: Enforcing REST API source
    example4 = {
        "title": "Force data retrieval from REST API only",
        "description": "Bypasses the standard failover mechanism to use only REST API",
        "command": "./examples/dsm_sync_simple/fcp_demo.py -s ETHUSDT -m spot -i 1m -es REST -d 2",
        "explanation": "Forces data retrieval directly from REST API, bypassing cache and Vision API.",
        "args": ["-s", "ETHUSDT", "-m", "spot", "-i", "1m", "-es", "REST", "-d", "2"],
    }

    # Example 5: Run FCP mechanism test
    example5 = {
        "title": "Test the Failover Control Protocol (FCP) mechanism",
        "description": "Runs a comprehensive test demonstrating how data is merged from multiple sources",
        "command": "./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -m spot -i 1m -fcp -pc",
        "explanation": "Demonstrates the full FCP mechanism by pre-populating cache and then retrieving data across multiple sources.",
        "args": ["-s", "BTCUSDT", "-m", "spot", "-i", "1m", "-fcp", "-pc"],
    }

    return [example1, example2, example3, example4, example5]


def display_examples(examples: List[Dict[str, Any]], run_example_func: Callable = None):
    """
    Display example commands with rich formatting.

    Args:
        examples: List of example command dictionaries
        run_example_func: Optional function to run an example
    """
    console = Console()

    console.print(
        Panel(
            "[bold green]FCP Demo: Tested Usage Examples[/bold green]\n"
            "Below are some real-world examples of using the fcp_demo.py script",
            expand=False,
            border_style="green",
        )
    )

    console.print("\n[bold cyan]Basic Examples:[/bold cyan]")

    for i, example in enumerate(examples, 1):
        console.print(f"\n[bold magenta]Example {i}: {example['title']}[/bold magenta]")
        console.print(f"{example['description']}")

        syntax = Syntax(example["command"], "bash", theme="monokai", word_wrap=True)
        console.print(syntax)

        console.print(f"[dim]{example['explanation']}[/dim]")
        if run_example_func:
            console.print(
                f"[bold green]Run this example:[/bold green] [yellow]python examples/dsm_sync_simple/fcp_demo.py examples --run {i}[/yellow]"
            )

    # Replace markdown with rich formatted text to avoid rendering issues
    console.print("\n[bold cyan]Advanced Usage Patterns:[/bold cyan]")

    # Replace markdown with rich formatted text to avoid rendering issues
    console.print("\n[bold]Date Formats[/bold]")
    console.print("• ISO format with timezone: [cyan]2025-04-01T00:00:00+00:00[/cyan]")
    console.print(
        "• ISO format without timezone (assumes UTC): [cyan]2025-04-01T00:00:00[/cyan]"
    )
    console.print("• Date only (assumes 00:00:00 UTC): [cyan]2025-04-01[/cyan]")
    console.print("• Human readable with time: [cyan]2025-04-01 12:30:45[/cyan]")

    console.print("\n[bold]Intervals[/bold]")
    console.print("The [green]-i/--interval[/green] parameter accepts values like:")
    console.print("• [cyan]1m[/cyan] - 1 minute")
    console.print("• [cyan]5m[/cyan] - 5 minutes")
    console.print("• [cyan]1h[/cyan] - 1 hour")
    console.print("• [cyan]1d[/cyan] - 1 day")

    console.print("\n[bold]Log Levels[/bold]")
    console.print("You can use either full names or single-letter shortcuts:")
    console.print("• [green]-l DEBUG[/green] or [green]-l D[/green]")
    console.print("• [green]-l INFO[/green] or [green]-l I[/green]")
    console.print("• [green]-l WARNING[/green] or [green]-l W[/green]")
    console.print("• [green]-l ERROR[/green] or [green]-l E[/green]")
    console.print("• [green]-l CRITICAL[/green] or [green]-l C[/green]")

    # Print explanatory notes
    console.print(
        Panel(
            "[bold yellow]Tips for Successful Usage:[/bold yellow]\n\n"
            "1. [green]Cache Management:[/green] Use -cc to clear cache if you suspect stale data\n"
            "2. [green]Debug Mode:[/green] Enable debug logging with -l D to see detailed information\n"
            "3. [green]Symbol Format:[/green] Coin-M futures require _PERP suffix (e.g., BTCUSD_PERP)\n"
            "4. [green]Test Mode:[/green] Use -fcp to test the full Failover Control Protocol (FCP) process",
            title="Best Practices",
            border_style="yellow",
        )
    )


def display_humanized_help():
    """Display a human-friendly simplified help screen."""
    from rich.console import Console
    from rich.panel import Panel

    console = Console()

    # Add usage examples
    console.print(
        Panel(
            "[bold cyan]Usage Examples:[/bold cyan]\n"
            "1. Run with default settings:\n"
            "   [yellow]./fcp_demo.py -s BTCUSDT -m spot -i 1m[/yellow]\n\n"
            "2. Run with specific date range and log level:\n"
            "   [yellow]./fcp_demo.py -s BTCUSD_PERP -m cm -i 1m -l D -st 2025-04-01 -et 2025-04-05[/yellow]\n\n"
            "3. View example commands:\n"
            "   [yellow]./fcp_demo.py examples[/yellow]\n\n"
            "4. Run a specific example:\n"
            "   [yellow]./fcp_demo.py examples --run 1[/yellow]",
            title="Quick Reference",
            border_style="green",
        )
    )

    # Show available flags
    console.print(
        Panel(
            "[bold cyan]Available Command-Line Options:[/bold cyan]\n"
            "[yellow]--help, -h[/yellow]: Show detailed help with all parameters\n"
            "[yellow]--humanize-help, -hh[/yellow]: Show this simplified help screen\n"
            "[yellow]--full-help, -fh[/yellow]: Same as --help\n"
            "[yellow]--detailed-help[/yellow]: Same as --help\n",
            title="Help Options",
            border_style="cyan",
        )
    )

    # Add commands information
    console.print(
        Panel(
            "[bold cyan]Available Commands:[/bold cyan]\n"
            "[yellow]main[/yellow]: (default) FCP Demo with all parameters\n"
            "   Example: [yellow]./fcp_demo.py main -s BTCUSDT -m spot[/yellow]\n\n"
            "[yellow]examples[/yellow]: Show and run tested example commands\n"
            "   Example: [yellow]./fcp_demo.py examples --run 1[/yellow]",
            title="Commands",
            border_style="blue",
        )
    )
