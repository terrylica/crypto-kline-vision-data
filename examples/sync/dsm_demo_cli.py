#!/usr/bin/env python3
"""Data Source Manager CLI demo tool.

This module provides a command-line interface for demonstrating the Failover Control
Protocol (FCP) mechanism implemented in the Data Source Manager package. It allows
users to fetch market data from multiple sources with a single command.

The FCP mechanism consists of three integrated phases:
1. Local Cache Retrieval: Quickly obtain data from local Apache Arrow files
2. Vision API Retrieval: Supplement missing data segments from Vision API
3. REST API Fallback: Ensure complete data coverage for any remaining segments

Key features:
- Comprehensive command-line options for data selection
- Flexible time range specification
- Cache control for improved performance
- Rich terminal output with data visualization
- Detailed performance metrics

Example usage:
    # Basic usage with default parameters
    dsm-demo-cli

    # Fetch ETHUSDT data for the last 5 days with 1-hour intervals
    dsm-demo-cli -s ETHUSDT -i 1h -d 5

    # Fetch data for a specific date range
    dsm-demo-cli -s BTCUSDT --start-time 2023-01-01 --end-time 2023-01-05

    # Force data retrieval from REST API (bypass cache and Vision API)
    dsm-demo-cli -s BTCUSDT -e REST
"""

from data_source_manager.utils.for_demo.dsm_help_content import MAIN_DOCSTRING

__doc__ = MAIN_DOCSTRING

import sys
from time import perf_counter

import pendulum
from rich.console import Console

# Import library functions
from data_source_manager.core.sync.dsm_lib import (
    fetch_market_data,
    process_market_parameters,
    setup_environment,
)
from data_source_manager.utils.app_paths import get_cache_dir, get_log_dir
from data_source_manager.utils.for_demo.dsm_app_options import (
    create_typer_app,
    get_cmd_help_text,
    get_standard_options,
)
from data_source_manager.utils.for_demo.dsm_cache_utils import print_cache_info
from data_source_manager.utils.for_demo.dsm_cli_utils import (
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
from data_source_manager.utils.for_demo.dsm_display_utils import display_results
from data_source_manager.utils.for_demo.dsm_doc_utils import (
    generate_markdown_docs,
    verify_and_install_typer_cli,
)

# Import the logger or logging and rich formatting
from data_source_manager.utils.loguru_setup import configure_session_logging, logger

# Start the performance timer at module initialization
start_time_perf = perf_counter()

# Create Typer app with custom rich formatting
app = create_typer_app()

# Get standard options and their defaults
options = get_standard_options()

# Console for rich output
console = Console()


@app.command(help=get_cmd_help_text())
def main(
    # Data Selection
    provider: DataProviderChoice = options["provider"],
    market: MarketTypeChoice = options["market"],
    chart_type: ChartTypeChoice = options["chart_type"],
    symbol: str = options["symbol"],
    interval: str = options["interval"],
    # Time Range options
    start_time: str | None = options["start_time"],
    end_time: str | None = options["end_time"],
    days: int = options["days"],
    # Data Source options
    enforce_source: DataSourceChoice = options["enforce_source"],
    retries: int = options["retries"],
    # Cache Control options
    no_cache: bool = options["no_cache"],
    clear_cache: bool = options["clear_cache"],
    show_cache_info: bool = options["show_cache_info"],
    # Documentation options
    gen_doc: bool = options["gen_doc"],
    gen_lint_config: bool = options["gen_lint_config"],
    # Other options
    log_level: LogLevel = options["log_level"],
):
    """Fetch and display market data using the Failover Control Protocol.

    This demo tool demonstrates the Data Source Manager's ability to retrieve
    market data from multiple sources using a progressive approach that
    prioritizes speed and reliability:

    1. First attempts to retrieve data from local cache (if use_cache=True)
    2. Then retrieves missing data from Vision API
    3. Finally falls back to REST API for any remaining data

    The tool will automatically handle time range validation, data normalization,
    and merging data from multiple sources into a consistent output.

    Example usage:

        dsm-demo-cli -s BTCUSDT -i 1m -d 10

        dsm-demo-cli --symbol ETHUSDT --interval 1h --days 5 --no-cache

        dsm-demo-cli -s BTCUSDT --start-time 2023-01-01 --end-time 2023-01-05
    """
    # Convert shorthand log levels to full names
    level = resolve_log_level(log_level.value)

    # Set up session logging (delegated to logger_setup.py)
    main_log, error_log, log_timestamp = configure_session_logging("dsm_demo_cli", level)

    logger.info(f"Current time: {pendulum.now().isoformat()}")

    # Log directories for reference
    log_dir = get_log_dir()
    cache_dir = get_cache_dir()
    logger.info(f"Using log directory: {log_dir}")
    logger.info(f"Using cache directory: {cache_dir}")

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
                logger.info("Documentation was generated using typer-cli for optimal GitHub rendering")

            return

        # Check if we should show cache information and exit
        if show_cache_info:
            print_cache_info()
            return

        # Print introductory information
        print_intro_panel()
        print_logging_panel(main_log, error_log)

        # Set up environment
        if not setup_environment(clear_cache):
            console.print("[bold red]Failed to set up environment. Exiting.[/bold red]")
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
            console.print(f"[bold red]Error: {e}[/bold red]")
            import traceback

            traceback.print_exc()
            sys.exit(1)
        except Exception as e:
            handle_error(e, start_time_perf)

    except Exception as e:
        handle_error(e, start_time_perf)


if __name__ == "__main__":
    app()
