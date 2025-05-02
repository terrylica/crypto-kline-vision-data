#!/usr/bin/env python3
"""
Application options for DSM Demo CLI applications.

This module contains Typer app options and argument definitions for DSM Demo CLI tools.
"""

from typing import Any, Dict

import typer

from utils.for_demo.dsm_cli_utils import (
    ChartTypeChoice,
    DataProviderChoice,
    DataSourceChoice,
    LogLevel,
    MarketTypeChoice,
)
from utils.for_demo.dsm_help_content import (
    APP_HELP,
    CLI_OPTIONS,
    COMMAND_HELP_TEXT,
)


def create_typer_app():
    """Create a preconfigured Typer app with consistent styling and settings.

    Returns:
        typer.Typer: Configured Typer app
    """
    return typer.Typer(
        help=APP_HELP,
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
    """Get standard CLI options for DSM Demo applications.

    Returns:
        Dict: Dictionary of standard options with their default values
    """
    return {
        # Data Selection options
        "provider": typer.Option(
            DataProviderChoice.BINANCE,
            CLI_OPTIONS["provider"]["long_flag"],
            CLI_OPTIONS["provider"]["short_flag"],
            help=CLI_OPTIONS["provider"]["help"],
        ),
        "market": typer.Option(
            MarketTypeChoice.SPOT,
            CLI_OPTIONS["market"]["long_flag"],
            CLI_OPTIONS["market"]["short_flag"],
            help=CLI_OPTIONS["market"]["help"],
        ),
        "chart_type": typer.Option(
            ChartTypeChoice.KLINES,
            CLI_OPTIONS["chart_type"]["long_flag"],
            CLI_OPTIONS["chart_type"]["short_flag"],
            help=CLI_OPTIONS["chart_type"]["help"],
        ),
        "symbol": typer.Option(
            CLI_OPTIONS["symbol"]["default"],
            CLI_OPTIONS["symbol"]["long_flag"],
            CLI_OPTIONS["symbol"]["short_flag"],
            help=CLI_OPTIONS["symbol"]["help"],
        ),
        "interval": typer.Option(
            CLI_OPTIONS["interval"]["default"],
            CLI_OPTIONS["interval"]["long_flag"],
            CLI_OPTIONS["interval"]["short_flag"],
            help=CLI_OPTIONS["interval"]["help"],
        ),
        # Time Range options
        "start_time": typer.Option(
            CLI_OPTIONS["start_time"]["default"],
            CLI_OPTIONS["start_time"]["long_flag"],
            CLI_OPTIONS["start_time"]["short_flag"],
            help=CLI_OPTIONS["start_time"]["help"],
        ),
        "end_time": typer.Option(
            CLI_OPTIONS["end_time"]["default"],
            CLI_OPTIONS["end_time"]["long_flag"],
            CLI_OPTIONS["end_time"]["short_flag"],
            help=CLI_OPTIONS["end_time"]["help"],
        ),
        "days": typer.Option(
            CLI_OPTIONS["days"]["default"],
            CLI_OPTIONS["days"]["long_flag"],
            CLI_OPTIONS["days"]["short_flag"],
            help=CLI_OPTIONS["days"]["help"],
        ),
        # Data Source options
        "enforce_source": typer.Option(
            DataSourceChoice.AUTO,
            CLI_OPTIONS["enforce_source"]["long_flag"],
            CLI_OPTIONS["enforce_source"]["short_flag"],
            help=CLI_OPTIONS["enforce_source"]["help"],
        ),
        "retries": typer.Option(
            CLI_OPTIONS["retries"]["default"],
            CLI_OPTIONS["retries"]["long_flag"],
            CLI_OPTIONS["retries"]["short_flag"],
            help=CLI_OPTIONS["retries"]["help"],
        ),
        # Cache Control options
        "no_cache": typer.Option(
            CLI_OPTIONS["no_cache"]["default"],
            CLI_OPTIONS["no_cache"]["long_flag"],
            CLI_OPTIONS["no_cache"]["short_flag"],
            help=CLI_OPTIONS["no_cache"]["help"],
        ),
        "clear_cache": typer.Option(
            CLI_OPTIONS["clear_cache"]["default"],
            CLI_OPTIONS["clear_cache"]["long_flag"],
            CLI_OPTIONS["clear_cache"]["short_flag"],
            help=CLI_OPTIONS["clear_cache"]["help"],
        ),
        # Documentation options
        "gen_doc": typer.Option(
            CLI_OPTIONS["gen_doc"]["default"],
            CLI_OPTIONS["gen_doc"]["long_flag"],
            CLI_OPTIONS["gen_doc"]["short_flag"],
            help=CLI_OPTIONS["gen_doc"]["help"],
        ),
        "gen_lint_config": typer.Option(
            CLI_OPTIONS["gen_lint_config"]["default"],
            CLI_OPTIONS["gen_lint_config"]["long_flag"],
            CLI_OPTIONS["gen_lint_config"]["short_flag"],
            help=CLI_OPTIONS["gen_lint_config"]["help"],
        ),
        # Other options
        "log_level": typer.Option(
            LogLevel.INFO,
            CLI_OPTIONS["log_level"]["long_flag"],
            CLI_OPTIONS["log_level"]["short_flag"],
            help=CLI_OPTIONS["log_level"]["help"],
            case_sensitive=False,
        ),
    }


def get_cmd_help_text():
    """Get a standardized help text for DSM Demo command.

    Returns:
        str: Help text for the command with examples
    """
    return COMMAND_HELP_TEXT
