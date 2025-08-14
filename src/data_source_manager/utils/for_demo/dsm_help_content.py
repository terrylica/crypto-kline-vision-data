#!/usr/bin/env python3
"""
Centralized help content for DSM Demo applications.

This module provides all help text content for DSM Demo scripts to maintain DRY principle.
"""

# Core reusable components
FCP_NAME = "Failover Control Protocol (FCP)"
APP_TITLE = f"DSM Demo: {FCP_NAME}"

# Data sources (listed in FCP priority order)
DATA_SOURCES = ["Cache (Local Arrow files)", "VISION API", "REST API"]

# Additional atomic fragments needed by doc utils
RETRIEVES_DATA = "retrieves data from multiple sources"
APP_BEHAVIOR = "It displays real-time source information about where each data point comes from"


# String builder for data sources list
def build_source_list(numbered=True, indent=0):
    """Build a string of data sources, optionally numbered with custom indentation."""
    indent_str = " " * indent
    source_lines = []
    for i, source in enumerate(DATA_SOURCES, 1):
        if numbered:
            source_lines.append(f"{indent_str}{i}. {source}")
        else:
            source_lines.append(f"{indent_str}{source}")
    return "\n".join(source_lines)


# Core descriptions
MECHANISM_DESC_SHORT = f"Demonstrates the {FCP_NAME} mechanism"
MECHANISM_DESC = f"""This script shows how DataSourceManager automatically retrieves data from different sources:

{build_source_list()}"""

SOURCE_INFO_DESC = "It displays real-time source information about where each data point comes from."

# App description used in multiple places
APP_DESCRIPTION = MECHANISM_DESC_SHORT

# Brief app help for typer app creation
APP_HELP = f"{APP_TITLE}"

# Intro panel text
INTRO_PANEL_TEXT = f"""[bold green]{APP_TITLE}[/bold green]
This script demonstrates how DataSourceManager automatically retrieves data
from different sources using the {FCP_NAME} strategy:
{build_source_list()}"""

# Rich output help panel text
RICH_OUTPUT_HELP_TEXT = """[bold cyan]Note about Log Level and Rich Output:[/bold cyan]
- When log level is D, I, or W: Rich output is visible
- When log level is E or C: Rich output is suppressed

Try running with different log levels to see the difference:
  dsm-demo-cli --log-level E
  dsm-demo-cli -l E (shorthand for E)"""

# Main script docstring
MAIN_DOCSTRING = f"""
{APP_TITLE} specified in
`.cursor/rules/always_focus_demo.mdc`

This script allows users to specify a time span and observe how the
DataSourceManager automatically retrieves data from different sources
following the {FCP_NAME} strategy:

{build_source_list()}

{SOURCE_INFO_DESC}
and provides a summary of the data source breakdown with timeline visualization.
"""

# Sample Commands section
SAMPLE_COMMANDS = """[bold cyan]Sample Commands[/bold cyan]

[green]End Time Backward Retrieval with Log Control[/green]
  > End time with days and ERROR log level (complex case)
  dsm-demo-cli -s BTCUSDT -et 2025-04-14T15:59:59 -i 3m -d 5 -l E

[green]Time Range CLI Examples[/green]
  > End time with days (fetch backward from end time)
  dsm-demo-cli -s BTCUSDT -et 2025-04-15 -d 7

  > Start time with days (fetch forward from start time)
  dsm-demo-cli -s BTCUSDT -st 2025-04-05 -d 10

  > Exact time range (start time to end time)
  dsm-demo-cli -s BTCUSDT -st 2025-04-05 -et 2025-04-15

  > Days only (fetch backward from current time)
  dsm-demo-cli -s BTCUSDT -d 7

  > Default (3 days backward from current time)
  dsm-demo-cli -s BTCUSDT

[green]Market Types[/green]
  dsm-demo-cli -s BTCUSDT -m um
  dsm-demo-cli -s BTCUSD_PERP -m cm

  > Note: Coin-margined futures (-m cm) require symbols with USD_PERP format (e.g., BTCUSD_PERP, not BTCUSDT)
  dsm-demo-cli -s BTCUSD_PERP -m cm -d 1 -et 2025-03-01

[green]Data Provider Options[/green]
  dsm-demo-cli -s BTCUSDT -p binance
  dsm-demo-cli -s BTCUSDT -p tradestation

[green]Different Intervals[/green]
  dsm-demo-cli -s BTCUSDT -i 5m
  dsm-demo-cli -s BTCUSDT -i 1h
  dsm-demo-cli -s SOLUSDT -m spot -i 1s  -cc -l D -st 2025-04-14T15:31:01 -et 2025-04-14T15:32:01

[green]Data Source Options[/green]
  dsm-demo-cli -s BTCUSDT -es REST
  dsm-demo-cli -s BTCUSDT -nc
  dsm-demo-cli -s BTCUSDT -cc

[green]Documentation Generation[/green]
  > Generate documentation
  dsm-demo-cli -gd

  > Generate documentation with linting configuration files
  dsm-demo-cli -gd -glc

[green]Combined Examples[/green]
  dsm-demo-cli -s ETHUSDT -m um -i 15m -st 2025-04-01 -et 2025-04-10 -r 5 -l D
  dsm-demo-cli -s ETHUSD_PERP -m cm -i 5m -d 10 -l D -cc
  dsm-demo-cli -s BTCUSDT -p binance -es VISION -m spot -i 1m -st 2025-04-01 -et 2025-04-03

  > Bitcoin historical data for coin-margined futures (using required USD_PERP format)
  dsm-demo-cli -s BTCUSD_PERP -m cm -i 15m -d 7 -et 2025-03-01 -l D -cc
"""

# Help text for the main command
COMMAND_HELP_TEXT = f"""
{APP_TITLE}.

{MECHANISM_DESC}

{SOURCE_INFO_DESC}

{SAMPLE_COMMANDS}
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
    "provider": {
        "long_flag": "--provider",
        "short_flag": "-p",
        "help": "Data provider (binance, tradestation)",
        "default": "binance",  # DataProviderChoice.BINANCE
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
        "help": "Start time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. "
        "Can be used alone with --days to fetch forward, or with --end-time for exact range",
        "default": None,
    },
    "end_time": {
        "long_flag": "--end-time",
        "short_flag": "-et",
        "help": "End time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. "
        "Can be used alone with --days to fetch backward, or with --start-time for exact range",
        "default": None,
    },
    "days": {
        "long_flag": "--days",
        "short_flag": "-d",
        "help": "Number of days to fetch (default: 3)",
        "default": 3,
    },
    # Data Source options
    "enforce_source": {
        "long_flag": "--enforce-source",
        "short_flag": "-es",
        "help": "Force specific data source (auto, REST, VISION)",
        "default": "auto",  # DataSourceChoice.AUTO
    },
    "retries": {
        "long_flag": "--retries",
        "short_flag": "-r",
        "help": "Max retry attempts for network operations",
        "default": 3,
    },
    # Cache Control options
    "no_cache": {
        "long_flag": "--no-cache",
        "short_flag": "-nc",
        "help": "Disable cache usage",
        "default": False,
    },
    "clear_cache": {
        "long_flag": "--clear-cache",
        "short_flag": "-cc",
        "help": "Clear cache before fetching data",
        "default": False,
    },
    # Documentation options
    "gen_doc": {
        "long_flag": "--gen-doc",
        "short_flag": "-gd",
        "help": "Generate Markdown documentation",
        "default": False,
    },
    "gen_lint_config": {
        "long_flag": "--gen-lint-config",
        "short_flag": "-glc",
        "help": "Generate markdownlint configuration",
        "default": False,
    },
    # Other options
    "log_level": {
        "long_flag": "--log-level",
        "short_flag": "-l",
        "help": "Log level (D=debug, I=info, W=warning, E=error, C=critical)",
        "default": "info",  # LogLevel.INFO
    },
    # New options
    "show_cache_info": {
        "long_flag": "--show-cache-info",
        "short_flag": "-sci",
        "help": "Show cache directory information and exit",
        "default": False,
    },
}

# Legacy help texts dictionary (kept for backward compatibility)
OPTION_HELP_TEXTS = {key: value["help"] for key, value in CLI_OPTIONS.items() if "help" in value}
