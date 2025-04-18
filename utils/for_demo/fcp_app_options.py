#!/usr/bin/env python3
"""
Application options for FCP demo CLI applications.

This module contains Typer app options and argument definitions for FCP demo CLI tools.
"""

from typing import Optional, Dict, Any
import typer
from typing_extensions import Annotated

from utils.for_demo.fcp_cli_utils import (
    MarketTypeChoice,
    DataSourceChoice,
    ChartTypeChoice,
    LogLevel,
)


def create_typer_app(app_name="FCP Demo"):
    """Create a preconfigured Typer app with consistent styling and settings.

    Args:
        app_name: Name of the application

    Returns:
        typer.Typer: Configured Typer app
    """
    return typer.Typer(
        help=f"{app_name}: Demonstrate the Failover Control Protocol (FCP) mechanism",
        rich_markup_mode="rich",
        add_completion=False,
        context_settings={
            "help_option_names": ["-h", "--help"],
            "allow_extra_args": False,  # Don't allow unknown args
            "ignore_unknown_options": False,  # Error on unknown options
        },
        epilog="Use the -h or --help flag to see sample commands and examples.",
    )


def get_standard_options() -> Dict[str, Any]:
    """Get standard CLI options for FCP demo applications.

    Returns:
        dict: Dictionary of standard Typer CLI options
    """
    return {
        # Data Selection
        "symbol": typer.Option(
            "BTCUSDT", "--symbol", "-s", help="Trading symbol (e.g., BTCUSDT)"
        ),
        "market": typer.Option(
            MarketTypeChoice.SPOT,
            "--market",
            "-m",
            help="Market type: spot, um (USDT-M futures), cm (Coin-M futures)",
        ),
        "interval": typer.Option(
            "1m", "--interval", "-i", help="Time interval (e.g., 1m, 5m, 1h)"
        ),
        "chart_type": typer.Option(
            ChartTypeChoice.KLINES,
            "--chart-type",
            "-ct",
            help="Type of chart data",
        ),
        # Time Range options - ordered by priority
        "days": typer.Option(
            3,
            "--days",
            "-d",
            help="[HIGHEST PRIORITY] Number of days to fetch from current time. Overrides --start-time/--end-time if provided",
        ),
        "start_time": typer.Option(
            None,
            "--start-time",
            "-st",
            help="[SECOND PRIORITY] Start time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. Used only if both --start-time AND --end-time are provided AND --days is NOT provided",
        ),
        "end_time": typer.Option(
            None,
            "--end-time",
            "-et",
            help="[SECOND PRIORITY] End time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. Used only if both --start-time AND --end-time are provided AND --days is NOT provided",
        ),
        # Data Source options
        "enforce_source": typer.Option(
            DataSourceChoice.AUTO,
            "--enforce-source",
            "-es",
            help="Force specific data source (default: AUTO)",
        ),
        "retries": typer.Option(
            3, "--retries", "-r", help="Maximum number of retry attempts"
        ),
        # Cache Control options
        "no_cache": typer.Option(
            False,
            "--no-cache",
            "-nc",
            help="Disable caching (cache is enabled by default)",
        ),
        "clear_cache": typer.Option(
            False,
            "--clear-cache",
            "-cc",
            help="Clear the cache directory before running",
        ),
        # Test Mode options
        "test_fcp_pm": typer.Option(
            False,
            "--test-fcp",
            "-fcp",
            help="Run the special test for Failover Control Protocol (FCP) mechanism",
        ),
        "prepare_cache": typer.Option(
            False,
            "--prepare-cache",
            "-pc",
            help="Pre-populate cache with the first segment of data (only used with --test-fcp)",
        ),
        # Documentation options
        "gen_doc": typer.Option(
            False,
            "--gen-doc",
            "-gd",
            help="Generate Markdown documentation from Typer help into docs/fcp_demo/ directory",
        ),
        "gen_lint_config": typer.Option(
            False,
            "--gen-lint-config",
            "-glc",
            help="Generate markdown linting configuration files along with documentation (only used with --gen-doc)",
        ),
        # Other options
        "log_level": typer.Option(
            LogLevel.INFO,
            "--log-level",
            "-l",
            help="Set the log level (default: INFO). Shorthand options: D=DEBUG, I=INFO, W=WARNING, E=ERROR, C=CRITICAL",
        ),
    }


def get_cmd_help_text():
    """Get a standardized help text for FCP demo command.

    Returns:
        str: Help text for the command with examples
    """
    return """
    FCP Demo: Demonstrates the Failover Control Protocol (FCP) mechanism.

    This script shows how DataSourceManager automatically retrieves data from different sources:

    1. Cache (Local Arrow files)
    2. VISION API
    3. REST API

    It displays real-time source information about where each data point comes from.

    [bold cyan]Time Range Priority Hierarchy:[/bold cyan]

    [green]1. --days or -d flag (HIGHEST PRIORITY):[/green]
      - If provided, overrides any --start-time and --end-time values
      - Calculates range as [current_time - days, current_time]
      - Example: --days 5 will fetch data from 5 days ago until now

    [green]2. --start-time and --end-time (SECOND PRIORITY):[/green]
      - Used only when BOTH are provided AND --days is NOT provided
      - Defines exact time range to fetch data from
      - Example: --start-time 2025-04-10 --end-time 2025-04-15

    [green]3. Default Behavior (FALLBACK):[/green]
      - If neither of the above conditions are met
      - Uses default days=3 to calculate range as [current_time - 3 days, current_time]

    [bold cyan]Sample Commands:[/bold cyan]

    [green]Basic Usage:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py
      ./examples/dsm_sync_simple/fcp_demo.py --symbol ETHUSDT --market spot

    [green]Time Range Options (By Priority):[/green]
      # PRIORITY 1: Using --days flag (overrides any start/end times)
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -d 7
      
      # PRIORITY 2: Using start and end times (only if --days is NOT provided)
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -st 2025-04-05T00:00:00 -et 2025-04-06T00:00:00
      
      # FALLBACK: No time flags (uses default days=3)
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT

    [green]Market Types:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -m um
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSD_PERP -m cm

    [green]Different Intervals:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -i 5m
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -i 1h
      ./examples/dsm_sync_simple/fcp_demo.py -s SOLUSDT -m spot -i 1s  -cc -l D -st 2025-04-14T15:31:01 -et 2025-04-14T15:32:01

    [green]Data Source Options:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -es REST
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -nc
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -cc

    [green]Testing FCP Mechanism:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -fcp
      ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -fcp -pc

    [green]Combined Examples:[/green]
      ./examples/dsm_sync_simple/fcp_demo.py -s ETHUSDT -m um -i 15m -st 2025-04-01 -et 2025-04-10 -r 5 -l DEBUG
      ./examples/dsm_sync_simple/fcp_demo.py -s ETHUSD_PERP -m cm -i 5m -d 10 -fcp -pc -l D -cc
    """
