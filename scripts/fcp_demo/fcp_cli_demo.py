#!/usr/bin/env python3
"""
Demo CLI tool for FCP progress utilities
"""

import time
import typer
import pendulum
from rich.panel import Panel
from utils.logger_setup import logger
from utils.for_demo.fcp_progress_utils import with_progress, configure_log_level

app = typer.Typer()


def simulate_data_fetch(delay: float = 2.0, points: int = 100):
    """Simulate fetching data with a delay"""
    time.sleep(delay)
    logger.debug(f"Generated {points} data points")
    return {
        "points": points,
        "timestamp": pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss.SSS"),
    }


def simulate_data_fetch_with_error(delay: float = 1.0):
    """Simulate fetching data with an error"""
    time.sleep(delay)
    logger.error("Encountered a network error during data fetch!")
    # Continue processing but log as error
    return {
        "error": True,
        "timestamp": pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss.SSS"),
    }


@app.command()
def fetch(
    symbol: str = typer.Option("BTCUSDT", "--symbol", "-s", help="Trading symbol"),
    delay: float = typer.Option(
        2.0, "--delay", "-d", help="Simulated delay in seconds"
    ),
    verbose: int = typer.Option(
        1, "--verbose", "-v", count=True, help="Verbosity level (repeat for more)"
    ),
    quiet: bool = typer.Option(
        False, "--quiet", "-q", help="Suppress all output except critical errors"
    ),
    error: bool = typer.Option(
        False, "--error", "-e", help="Simulate error during fetch to test error logging"
    ),
):
    """
    Simulate data fetching with progress bar based on logger settings
    """
    # Configure logger based on CLI flags
    log_level = configure_log_level(verbose, quiet)

    # Print banner
    print(
        Panel(
            f"[bold]FCP Progress Demo[/bold]\n"
            f"Symbol: {symbol}\n"
            f"Log level: {log_level}\n"
            f"Simulate error: {error}\n"
            f"Current time: {pendulum.now('UTC').format('YYYY-MM-DD HH:mm:ss.SSS')}",
            border_style="green",
        )
    )

    # Log some messages at different levels
    logger.debug("This is a DEBUG message")
    logger.info("This is an INFO message")
    logger.warning("This is a WARNING message")
    logger.error("This is an ERROR message")

    # Define the operation to run with progress tracking
    if error:

        def fetch_operation():
            logger.warning(f"Fetching data for {symbol} with simulated error...")
            return simulate_data_fetch_with_error(delay=delay)

    else:

        def fetch_operation():
            logger.info(f"Fetching data for {symbol}...")
            return simulate_data_fetch(delay=delay)

    # Use the with_progress utility
    logger.info("Starting data fetch...")
    result = with_progress(
        operation=fetch_operation,
        message=f"Fetching data for {symbol}...",
    )

    # Display results
    if error:
        logger.error(f"Fetch completed with error at {result['timestamp']}")

        print(
            Panel(
                f"[bold red]Fetch Error[/bold red]\n"
                f"Symbol: {symbol}\n"
                f"Status: Error during data fetch\n"
                f"Timestamp: {result['timestamp']}",
                border_style="red",
            )
        )
    else:
        logger.info(f"Fetch completed at {result['timestamp']}")
        logger.debug(f"Retrieved {result['points']} data points")

        print(
            Panel(
                f"[bold green]Fetch Complete[/bold green]\n"
                f"Symbol: {symbol}\n"
                f"Data points: {result['points']}\n"
                f"Timestamp: {result['timestamp']}",
                border_style="green",
            )
        )


if __name__ == "__main__":
    app()
