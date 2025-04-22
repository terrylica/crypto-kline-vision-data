#!/usr/bin/env python3
"""
Centralized help content for DSM Demo applications.

This module provides all help text content for DSM Demo scripts to maintain DRY principle.
"""

# App description used in multiple places
APP_DESCRIPTION = "Demonstrates the Failover Control Protocol (FCP) mechanism"

# Brief app help for typer app creation
APP_HELP = "DSM Demo: Demonstrate the Failover Control Protocol (FCP) mechanism"

# Intro panel text
INTRO_PANEL_TEXT = """[bold green]DSM Demo: Failover Control Protocol (FCP)[/bold green]
This script demonstrates how DataSourceManager automatically retrieves data
from different sources using the Failover Control Protocol (FCP) strategy:
1. Cache (Local Arrow files)
2. VISION API
3. REST API"""

# Rich output help panel text
RICH_OUTPUT_HELP_TEXT = """[bold cyan]Note about Log Level and Rich Output:[/bold cyan]
- When log level is DEBUG, INFO, or WARNING: Rich output is visible
- When log level is ERROR or CRITICAL: Rich output is suppressed

Try running with different log levels to see the difference:
  python examples/sync/dsm_demo.py --log-level ERROR
  python examples/sync/dsm_demo.py -l E (shorthand for ERROR)"""

# Main script docstring
MAIN_DOCSTRING = """
DSM Demo: Demonstrates the Failover Control Protocol (FCP) mechanism specified in
`.cursor/rules/always_focus_demo.mdc`

This script allows users to specify a time span and observe how the
DataSourceManager automatically retrieves data from different sources
following the Failover Control Protocol (FCP) strategy:

1. Cache (Local Arrow files)
2. VISION API
3. REST API

It shows real-time source information about where each data point comes from,
and provides a summary of the data source breakdown with timeline visualization.
"""

# Help text for the main command
COMMAND_HELP_TEXT = """
DSM Demo: Demonstrates the Failover Control Protocol (FCP) mechanism.

This script shows how DataSourceManager automatically retrieves data from different sources:

1. Cache (Local Arrow files)
2. VISION API
3. REST API

It displays real-time source information about where each data point comes from.

[bold cyan]Time Range Options[/bold cyan]

[green]1. End Time with Days[/green]
  - Use --end-time with --days to fetch data backward from a specific end time
  - Calculates range as [end_time - days, end_time]
  - Example: --end-time 2025-04-15 --days 5 will fetch data from April 10-15, 2025

[green]2. Start Time with Days[/green]
  - Use --start-time with --days to fetch data forward from a specific start time
  - Calculates range as [start_time, start_time + days]
  - Example: --start-time 2025-04-10 --days 5 will fetch data from April 10-15, 2025

[green]3. Exact Time Range[/green]
  - Provide both --start-time and --end-time for an exact time range
  - Example: --start-time 2025-04-10 --end-time 2025-04-15

[green]4. Days Only[/green]
  - Use --days alone to fetch data relative to current time
  - Calculates range as [current_time - days, current_time]
  - Example: --days 5 will fetch data from 5 days ago until now

[green]5. Default Behavior (No Options)[/green]
  - If no time options provided, uses default of 3 days from current time
  - Equivalent to --days 3

[bold cyan]Sample Commands[/bold cyan]

[green]End Time Backward Retrieval with Log Control[/green]
  > End time with days and ERROR log level (complex case)
  ./examples/sync/dsm_demo.py -s BTCUSDT -et 2025-04-14T15:59:59 -i 3m -d 5 -l E

[green]Time Range CLI Examples[/green]
  > End time with days (fetch backward from end time)
  ./examples/sync/dsm_demo.py -s BTCUSDT -et 2025-04-15 -d 7
  
  > Start time with days (fetch forward from start time)
  ./examples/sync/dsm_demo.py -s BTCUSDT -st 2025-04-05 -d 10
  
  > Exact time range (start time to end time)
  ./examples/sync/dsm_demo.py -s BTCUSDT -st 2025-04-05 -et 2025-04-15
  
  > Days only (fetch backward from current time)
  ./examples/sync/dsm_demo.py -s BTCUSDT -d 7
  
  > Default (3 days backward from current time)
  ./examples/sync/dsm_demo.py -s BTCUSDT

[green]Market Types[/green]
  ./examples/sync/dsm_demo.py -s BTCUSDT -m um
  ./examples/sync/dsm_demo.py -s BTCUSD_PERP -m cm

[green]Different Intervals[/green]
  ./examples/sync/dsm_demo.py -s BTCUSDT -i 5m
  ./examples/sync/dsm_demo.py -s BTCUSDT -i 1h
  ./examples/sync/dsm_demo.py -s SOLUSDT -m spot -i 1s  -cc -l D -st 2025-04-14T15:31:01 -et 2025-04-14T15:32:01

[green]Data Source Options[/green]
  ./examples/sync/dsm_demo.py -s BTCUSDT -es REST
  ./examples/sync/dsm_demo.py -s BTCUSDT -nc
  ./examples/sync/dsm_demo.py -s BTCUSDT -cc
  
[green]Documentation Generation[/green]
  > Generate documentation
  ./examples/sync/dsm_demo.py -gd
  
  > Generate documentation with linting configuration files
  ./examples/sync/dsm_demo.py -gd -glc

[green]Combined Examples[/green]
  ./examples/sync/dsm_demo.py -s ETHUSDT -m um -i 15m -st 2025-04-01 -et 2025-04-10 -r 5 -l DEBUG
  ./examples/sync/dsm_demo.py -s ETHUSD_PERP -m cm -i 5m -d 10 -l D -cc
"""

# CLI Option Definitions
# Each option has a structure with key information about the option
CLI_OPTIONS = {
    # Data Selection options
    "symbol": {
        "long_flag": "--symbol",
        "short_flag": "-s",
        "help": "Symbol to fetch data for",
        "default": "BTCUSDT",
    },
    "market": {
        "long_flag": "--market",
        "short_flag": "-m",
        "help": "Market type (spot, um, cm)",
        "default": "spot",  # MarketTypeChoice.SPOT
    },
    "interval": {
        "long_flag": "--interval",
        "short_flag": "-i",
        "help": "Time interval for klines/premiums",
        "default": "1m",
    },
    "chart_type": {
        "long_flag": "--chart-type",
        "short_flag": "-ct",
        "help": "Chart type (klines, premiums)",
        "default": "klines",  # ChartTypeChoice.KLINES
    },
    # Time Range options
    "start_time": {
        "long_flag": "--start-time",
        "short_flag": "-st",
        "help": "Start time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. Can be used alone with --days to fetch forward, or with --end-time for exact range",
        "default": None,
    },
    "end_time": {
        "long_flag": "--end-time",
        "short_flag": "-et",
        "help": "End time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. Can be used alone with --days to fetch backward, or with --start-time for exact range",
        "default": None,
    },
    "days": {
        "long_flag": "--days",
        "short_flag": "-d",
        "help": "Number of days of data to fetch. If used with --end-time, fetches data backward from end time. If used with --start-time, fetches data forward from start time. If used alone, fetches data backward from current time",
        "default": 3,
    },
    # Data Source options
    "enforce_source": {
        "long_flag": "--enforce-source",
        "short_flag": "-es",
        "help": "Force specific data source (default: AUTO)",
        "default": "AUTO",  # DataSourceChoice.AUTO
    },
    "retries": {
        "long_flag": "--retries",
        "short_flag": "-r",
        "help": "Maximum number of retry attempts",
        "default": 3,
    },
    # Cache Control options
    "no_cache": {
        "long_flag": "--no-cache",
        "short_flag": "-nc",
        "help": "Disable caching (cache is enabled by default)",
        "default": False,
    },
    "clear_cache": {
        "long_flag": "--clear-cache",
        "short_flag": "-cc",
        "help": "Clear the cache directory before running",
        "default": False,
    },
    # Documentation options
    "gen_doc": {
        "long_flag": "--gen-doc",
        "short_flag": "-gd",
        "help": "Generate Markdown documentation from Typer help into docs/dsm_demo/ directory",
        "default": False,
    },
    "gen_lint_config": {
        "long_flag": "--gen-lint-config",
        "short_flag": "-glc",
        "help": "Generate markdown linting configuration files along with documentation (only used with --gen-doc)",
        "default": False,
    },
    # Other options
    "log_level": {
        "long_flag": "--log-level",
        "short_flag": "-l",
        "help": "Set the log level (default: INFO). Shorthand options: D=DEBUG, I=INFO, W=WARNING, E=ERROR, C=CRITICAL",
        "default": "INFO",  # LogLevel.INFO
    },
    "help": {
        "long_flag": "--help",
        "short_flag": "-h",
        "help": "Show this message and exit.",
    },
}

# Legacy help texts dictionary (kept for backward compatibility)
OPTION_HELP_TEXTS = {
    key: value["help"] for key, value in CLI_OPTIONS.items() if "help" in value
}
