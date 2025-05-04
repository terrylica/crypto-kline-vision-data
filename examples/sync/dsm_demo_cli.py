#!/usr/bin/env python3
"""
DSM Demo CLI: Command-line interface for data source management demo.
This module provides a CLI wrapper around the core DSM functionality.
"""

from utils.for_demo.dsm_help_content import MAIN_DOCSTRING

__doc__ = MAIN_DOCSTRING

import sys
from pathlib import Path
from time import perf_counter
from typing import Optional

import pendulum

# Import library functions
from core.sync.dsm_lib import (
    fetch_market_data,
    process_market_parameters,
    setup_environment,
)
from utils.for_demo.dsm_app_options import (
    create_typer_app,
    get_cmd_help_text,
    get_standard_options,
)
from utils.for_demo.dsm_cli_utils import (
    ChartTypeChoice,
    DataProviderChoice,
    DataSourceChoice,
    LogLevel,
    MarketTypeChoice,
    handle_error,
    print_config_table,
    print_intro_panel,
    print_logging_panel,
    print_performance_panel,
    print_rich_output_help,
    resolve_log_level,
)

# Import utility modules for DSM Demo
from utils.for_demo.dsm_display_utils import display_results
from utils.for_demo.dsm_doc_utils import (
    generate_markdown_docs,
    verify_and_install_typer_cli,
)

# Import the logger or logging and rich formatting
from utils.logger_setup import configure_session_logging, logger

# Start the performance timer at module initialization
start_time_perf = perf_counter()

# We'll use this cache dir for all demos
CACHE_DIR = Path("./cache")

# Create Typer app with custom rich formatting
app = create_typer_app()

# Get standard options and their defaults
options = get_standard_options()


@app.command(help=get_cmd_help_text())
def main(
    # Data Selection
    provider: DataProviderChoice = options["provider"],
    market: MarketTypeChoice = options["market"],
    chart_type: ChartTypeChoice = options["chart_type"],
    symbol: str = options["symbol"],
    interval: str = options["interval"],
    # Time Range options
    start_time: Optional[str] = options["start_time"],
    end_time: Optional[str] = options["end_time"],
    days: int = options["days"],
    # Data Source options
    enforce_source: DataSourceChoice = options["enforce_source"],
    retries: int = options["retries"],
    # Cache Control options
    no_cache: bool = options["no_cache"],
    clear_cache: bool = options["clear_cache"],
    # Documentation options
    gen_doc: bool = options["gen_doc"],
    gen_lint_config: bool = options["gen_lint_config"],
    # Other options
    log_level: LogLevel = options["log_level"],
):
    """DSM Demo: Demonstrates the Failover Control Protocol (FCP) mechanism."""
    # Convert shorthand log levels to full names
    level = resolve_log_level(log_level.value)

    # Set up session logging (delegated to logger_setup.py)
    main_log, error_log, log_timestamp = configure_session_logging(
        "dsm_demo_cli", level
    )

    logger.info(f"Current time: {pendulum.now().isoformat()}")

    try:
        # Check if we should generate documentation
        if gen_doc:
            logger.info("Generating Markdown documentation from Typer help...")

            # Check if typer-cli is installed
            typer_cli_available = verify_and_install_typer_cli()

            # Generate documentation
            doc_path = generate_markdown_docs(
                app,
                output_dir="examples/sync",
                gen_lint_config=gen_lint_config,
                cli_name="dsm-demo-cli",
            )
            logger.info(f"Documentation generated and saved to {doc_path}")

            # If typer-cli was installed and used, provide additional information
            if typer_cli_available:
                logger.info(
                    "Documentation was generated using typer-cli for optimal GitHub rendering"
                )

            return

        # Print introductory information
        print_intro_panel()
        print_logging_panel(main_log, error_log)

        # Set up environment
        if not setup_environment(clear_cache):
            sys.exit(1)

        # Display configuration
        print_config_table(
            provider.value,
            market.value,
            chart_type.value,
            symbol,
            interval,
            start_time,
            end_time,
            days,
            enforce_source.value,
            retries,
            no_cache,
            clear_cache,
            level,
        )

        try:
            # Process market parameters
            (
                provider_enum,
                market_type,
                chart_type_enum,
                symbol_adjusted,
                interval_enum,
            ) = process_market_parameters(
                provider.value,
                market.value,
                chart_type.value,
                symbol,
                interval,
            )

            # Fetch market data
            df, elapsed_time, records_count = fetch_market_data(
                provider=provider_enum,
                market_type=market_type,
                chart_type=chart_type_enum,
                symbol=symbol_adjusted,
                interval=interval_enum,
                start_time=start_time,
                end_time=end_time,
                days=days,
                use_cache=not no_cache,
                enforce_source=enforce_source.value,
                max_retries=retries,
            )

            # Display results with enhanced visualizations
            display_results(
                df,
                symbol_adjusted,
                market_type.name.lower(),
                interval_enum.value,
                chart_type_enum.name.lower(),
                log_timestamp,
            )

            # Add info about rich output and log levels
            print_rich_output_help()

            # Display performance metrics
            print_performance_panel(elapsed_time, records_count)

        except ValueError as e:
            print(f"[bold red]Error: {e}[/bold red]")
            import traceback

            traceback.print_exc()
            sys.exit(1)
        except Exception as e:
            handle_error(e, start_time_perf)

    except Exception as e:
        handle_error(e, start_time_perf)


if __name__ == "__main__":
    app()
