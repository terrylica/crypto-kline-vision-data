#!/usr/bin/env python3
"""
Loguru Logger Demo for DSM Package

This demo shows how easy it is to control logging with the new loguru-based logger.
Compare this to the complexity of the old logging system!

Usage:
    python examples/loguru_demo.py [--level LEVEL] [--file FILE]

Examples:
    # Basic usage
    python examples/loguru_demo.py

    # Set log level
    python examples/loguru_demo.py --level DEBUG

    # Log to file
    python examples/loguru_demo.py --file ./logs/demo.log

    # Environment variable control (easiest!)
    DSM_LOG_LEVEL=DEBUG python examples/loguru_demo.py
"""

import typer
from rich.console import Console
from rich.panel import Panel

# Import the new loguru-based logger
from data_source_manager.utils.loguru_setup import logger

console = Console()


def demonstrate_basic_logging():
    """Show basic logging functionality."""
    console.print(Panel.fit("[bold blue]Basic Logging Demo[/bold blue]", border_style="blue"))

    logger.debug("This is a debug message - shows detailed information")
    logger.info("This is an info message - general information")
    logger.warning("This is a warning message - something to be aware of")
    logger.error("This is an error message - something went wrong")
    logger.critical("This is a critical message - serious problem!")


def demonstrate_rich_formatting():
    """Show rich formatting capabilities."""
    console.print(Panel.fit("[bold green]Rich Formatting Demo[/bold green]", border_style="green"))

    logger.info("Status: <green>SUCCESS</green> - Operation completed")
    logger.warning("Status: <yellow>WARNING</yellow> - Check configuration")
    logger.error("Status: <red>ERROR</red> - Operation failed")

    # Show structured logging
    logger.info("Processing user <cyan>john_doe</cyan> with ID <yellow>12345</yellow>")
    logger.info("Database connection: <green>CONNECTED</green> to <blue>postgresql://localhost:5432</blue>")


def demonstrate_contextual_logging():
    """Show contextual logging with bound data."""
    console.print(Panel.fit("[bold yellow]Contextual Logging Demo[/bold yellow]", border_style="yellow"))

    # Bind context to logger
    user_logger = logger.bind(user_id=123, session="abc-def-ghi")
    user_logger.info("User logged in")
    user_logger.info("User accessed dashboard")
    user_logger.warning("User attempted unauthorized action")


def demonstrate_level_control():
    """Show how easy log level control is."""
    console.print(Panel.fit("[bold red]Log Level Control Demo[/bold red]", border_style="red"))

    console.print("[blue]Setting log level to ERROR - only errors and critical messages will show[/blue]")
    logger.configure_level("ERROR")

    logger.debug("Debug message - should NOT appear")
    logger.info("Info message - should NOT appear")
    logger.warning("Warning message - should NOT appear")
    logger.error("Error message - should appear")
    logger.critical("Critical message - should appear")

    console.print("\n[blue]Setting log level back to INFO[/blue]")
    logger.configure_level("INFO")
    logger.info("Info message - should appear again")


def demonstrate_exception_logging():
    """Show exception logging with traceback."""
    console.print(Panel.fit("[bold magenta]Exception Logging Demo[/bold magenta]", border_style="magenta"))

    try:
        # Simulate an error
        pass
    except ZeroDivisionError:
        logger.exception("Division by zero error occurred")


def main(
    level: str = typer.Option(None, "--level", "-l", help="Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"),
    log_file: str = typer.Option(None, "--file", "-f", help="Optional log file path"),
    show_all: bool = typer.Option(False, "--all", "-a", help="Show all demos"),
):
    """Demonstrate the new loguru-based logger for DSM."""

    console.print(
        Panel.fit(
            "[bold blue]DSM Loguru Logger Demo[/bold blue]\nSee how easy logging control is with the new system!", border_style="blue"
        )
    )

    # Configure logger based on arguments (only if explicitly provided)
    if level:
        logger.configure_level(level)
        console.print(f"[green]Log level set to: {level}[/green]")
    else:
        console.print(f"[green]Using log level: {logger.getEffectiveLevel()} (from environment or default)[/green]")

    if log_file:
        logger.configure_file(log_file)
        console.print(f"[green]Logging to file: {log_file}[/green]")

    console.print()

    # Run demonstrations
    demonstrate_basic_logging()
    console.print()

    demonstrate_rich_formatting()
    console.print()

    if show_all:
        demonstrate_contextual_logging()
        console.print()

        demonstrate_level_control()
        console.print()

        demonstrate_exception_logging()
        console.print()

    # Show comparison with old system
    console.print(
        Panel.fit(
            "[bold green]Benefits vs Old Logger[/bold green]\n\n"
            "🎯 [bold]Easy log control:[/bold] DSM_LOG_LEVEL=DEBUG vs complex logging config\n"
            "🚀 [bold]Better performance:[/bold] Loguru is faster than standard logging\n"
            "🔄 [bold]Auto rotation:[/bold] Built-in log file rotation and compression\n"
            "🎨 [bold]Rich formatting:[/bold] Beautiful colored output with context\n"
            "🔧 [bold]Same API:[/bold] All existing logging calls work unchanged\n\n"
            "[blue]Try setting different log levels:[/blue]\n"
            "DSM_LOG_LEVEL=DEBUG python examples/loguru_demo.py\n"
            "DSM_LOG_LEVEL=ERROR python examples/loguru_demo.py",
            border_style="green",
        )
    )


if __name__ == "__main__":
    typer.run(main)
