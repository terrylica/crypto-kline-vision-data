#!/usr/bin/env python3
"""
Command-line interface utilities for the DSM Demo applications.

This module provides common CLI setup and display functions for DSM Demo scripts.
"""

from enum import Enum
from rich import print
from rich.panel import Panel
from rich.table import Table
import rich.box as box
from pathlib import Path

from utils.logger_setup import logger
from utils.market_constraints import (
    get_market_symbol_format,
    validate_symbol_for_market_type,
    MarketType,
)
from core.sync.data_source_manager import DataSource
from utils.for_demo.dsm_help_content import INTRO_PANEL_TEXT, RICH_OUTPUT_HELP_TEXT


class MarketTypeChoice(str, Enum):
    """Market type choices for CLI arguments."""

    SPOT = "spot"
    UM = "um"
    CM = "cm"


class DataProviderChoice(str, Enum):
    """Data provider choices for CLI arguments."""

    BINANCE = "binance"
    # Add other providers if needed


class DataSourceChoice(str, Enum):
    """Data source choices for CLI arguments."""

    AUTO = "AUTO"
    REST = "REST"
    VISION = "VISION"


class ChartTypeChoice(str, Enum):
    """Chart type choices for CLI arguments."""

    KLINES = "klines"
    FUNDING_RATE = "fundingRate"


class LogLevel(str, Enum):
    """Log level choices."""

    # Shorthand values for log levels
    D = "D"  # DEBUG
    I = "I"  # INFO
    W = "W"  # WARNING
    E = "E"  # ERROR
    C = "C"  # CRITICAL


def resolve_log_level(level):
    """Resolve log level from shorthand to full name.

    Args:
        level: Log level value which could be shorthand

    Returns:
        str: Full log level name
    """
    # Convert shorthand log levels to full names
    if level == "D":
        return "DEBUG"
    elif level == "I":
        return "INFO"
    elif level == "W":
        return "WARNING"
    elif level == "E":
        return "ERROR"
    elif level == "C":
        return "CRITICAL"
    return level


def print_intro_panel():
    """Print the introductory panel for DSM Demo applications."""
    print(
        Panel(
            INTRO_PANEL_TEXT,
            expand=False,
            border_style="green",
        )
    )


def print_logging_panel(main_log, error_log):
    """Print logging configuration information.

    Args:
        main_log: Path to the main log file
        error_log: Path to the error log file
    """
    # Convert to Path objects if they aren't already
    main_log_path = Path(main_log)
    error_log_path = Path(error_log)

    print(
        Panel(
            f"[bold cyan]Logging Configuration:[/bold cyan]\n"
            f"Detailed logs: [green]{main_log_path}[/green]\n"
            f"Error logs: [yellow]{error_log_path}[/yellow]",
            title="Logging Info",
            border_style="blue",
        )
    )


def print_config_table(
    provider,
    market,
    chart_type,
    symbol,
    interval,
    start_time,
    end_time,
    days,
    enforce_source,
    retries,
    no_cache,
    clear_cache,
    log_level,
):
    """Print a formatted table of configuration settings.

    Args:
        Various configuration parameters to display
    """
    # Convert market string to MarketType enum
    market_type = MarketType.from_string(market)

    # Get the properly formatted symbol for this market
    display_symbol = get_market_symbol_format(symbol, market_type)

    args_table = Table(
        title="[bold cyan]Configuration Settings[/bold cyan]",
        show_header=False,
        box=box.SIMPLE,
    )
    args_table.add_column("Category", style="cyan")
    args_table.add_column("Values", style="")

    # Data Selection row with teal color
    args_table.add_row(
        "Data Selection",
        f"[spring_green3]Provider: {provider} | Market: {market} | Chart type: {chart_type} | Symbol: {display_symbol} | Interval: {interval}[/spring_green3]",
    )

    # Time Range row with slate blue color
    args_table.add_row(
        "Time Range",
        f"[sky_blue1]Start: {start_time} | End: {end_time} | Days: {days}[/sky_blue1]",
    )

    # Data Source row with gold color
    args_table.add_row(
        "Data Source",
        f"[gold1]Enforce source: {enforce_source} | Retries: {retries}[/gold1]",
    )

    # Cache Control row with coral color
    args_table.add_row(
        "Cache Control",
        f"[dark_orange]No cache: {no_cache} | Clear cache: {clear_cache}[/dark_orange]",
    )

    # Other row with dark slate gray
    args_table.add_row("Other", f"[grey70]Log level: {log_level}[/grey70]")

    print(args_table)
    print()


def print_performance_panel(elapsed_time, records_count=0):
    """Print performance metrics panel.

    Args:
        elapsed_time: Execution time in seconds
        records_count: Number of records processed
    """
    # Calculate records per second and per minute
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


def print_rich_output_help():
    """Print help information about rich output and log levels."""
    print(
        Panel(
            RICH_OUTPUT_HELP_TEXT,
            title="Rich Output Control",
            border_style="blue",
        )
    )


def handle_error(error, start_time_perf=None):
    """Handle and display errors in a user-friendly way.

    Args:
        error: Exception object
        start_time_perf: Optional start time for performance measurement
    """
    try:
        # Safely handle the error to prevent rich text formatting issues
        error_msg = str(error)
        # Sanitize error message to replace non-printable characters
        safe_error_msg = "".join(
            c if c.isprintable() else f"\\x{ord(c):02x}" for c in error_msg
        )
        print(f"[bold red]CRITICAL ERROR: {safe_error_msg}[/bold red]")
        import traceback

        # Also sanitize the traceback
        tb_str = traceback.format_exc()
        safe_tb = "".join(c if c.isprintable() else f"\\x{ord(c):02x}" for c in tb_str)
        print(safe_tb)

        # Display execution time if start_time_perf is provided
        if start_time_perf:
            from time import perf_counter

            end_time_perf = perf_counter()
            elapsed_time = end_time_perf - start_time_perf
            print(
                Panel(
                    f"[cyan]Total script execution time: {elapsed_time:.4f} seconds[/cyan]\n"
                    "[red]Unable to calculate processing rate due to error[/red]",
                    title="Performance Timing",
                    border_style="red",
                )
            )
    except:
        # Last resort if even error handling fails
        print("An error occurred, but the error handler encountered an exception.")
        import traceback

        traceback.print_exc()


def adjust_symbol_for_market(symbol, market_type):
    """Adjust symbol format based on market type.

    Args:
        symbol: Trading symbol (e.g. BTCUSDT)
        market_type: Market type enum

    Returns:
        str: Adjusted symbol

    Raises:
        ValueError: If the symbol is invalid for the market type
    """
    # First transform the symbol using the centralized function
    adjusted_symbol = get_market_symbol_format(symbol, market_type)

    if adjusted_symbol != symbol:
        logger.debug(
            f"Adjusted symbol for {market_type.name} market: {adjusted_symbol}"
        )

    # Then validate the adjusted symbol
    try:
        validate_symbol_for_market_type(adjusted_symbol, market_type)
    except ValueError as e:
        # Log the error and re-raise
        logger.error(f"Symbol validation error: {str(e)}")
        raise

    return adjusted_symbol


def convert_source_choice(enforce_source):
    """Convert DataSourceChoice enum to DataSource enum.

    Args:
        enforce_source: DataSourceChoice enum value

    Returns:
        DataSource: Corresponding DataSource enum value
    """
    if enforce_source == DataSourceChoice.AUTO:
        return DataSource.AUTO
    elif enforce_source == DataSourceChoice.REST:
        return DataSource.REST
    elif enforce_source == DataSourceChoice.VISION:
        return DataSource.VISION
    return DataSource.AUTO
