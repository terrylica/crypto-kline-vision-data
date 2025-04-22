#!/usr/bin/env python3
from utils.for_demo.dsm_help_content import MAIN_DOCSTRING

__doc__ = MAIN_DOCSTRING

from pathlib import Path
from time import perf_counter
import sys
from typing import Optional
import pendulum

# Import the logger or logging and rich formatting
from utils.logger_setup import logger, configure_session_logging

from utils.market_constraints import MarketType, Interval, DataProvider, ChartType

# Import utility modules for DSM Demo
from utils.for_demo.dsm_datetime_parser import parse_datetime
from utils.for_demo.dsm_cache_utils import clear_cache_directory, verify_project_root
from utils.for_demo.dsm_data_fetcher import fetch_data_with_fcp
from utils.for_demo.dsm_display_utils import display_results
from utils.for_demo.dsm_doc_utils import generate_markdown_docs
from utils.for_demo.dsm_cli_utils import (
    resolve_log_level,
    print_intro_panel,
    print_logging_panel,
    print_config_table,
    print_performance_panel,
    print_rich_output_help,
    handle_error,
    adjust_symbol_for_market,
    convert_source_choice,
    MarketTypeChoice,
    DataSourceChoice,
    ChartTypeChoice,
    LogLevel,
    DataProviderChoice,
)
from utils.for_demo.dsm_app_options import (
    create_typer_app,
    get_standard_options,
    get_cmd_help_text,
)

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
    main_log, error_log, log_timestamp = configure_session_logging("dsm_demo", level)

    logger.info(f"Current time: {pendulum.now().isoformat()}")

    try:
        # Check if we should generate documentation
        if gen_doc:
            logger.info("Generating Markdown documentation from Typer help...")

            # Check if typer-cli is installed
            try:
                import shutil

                typer_cli_available = shutil.which("typer") is not None

                if not typer_cli_available:
                    logger.info(
                        "typer-cli not found. Installing typer-cli for optimal documentation..."
                    )
                    import subprocess

                    subprocess.run(
                        [sys.executable, "-m", "pip", "install", "typer-cli"],
                        check=True,
                        capture_output=True,
                    )
                    logger.info("typer-cli installed successfully")
                    typer_cli_available = True
            except Exception as e:
                logger.warning(f"Could not install typer-cli: {e}")
                typer_cli_available = False

            # Generate documentation
            doc_path = generate_markdown_docs(
                app,
                gen_lint_config=gen_lint_config,
                cli_name="dsm_demo",
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

        # Verify project root
        if not verify_project_root():
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

        # Clear cache if requested
        if clear_cache:
            clear_cache_directory(CACHE_DIR)

        # Validate and process arguments
        try:
            # Convert provider string to enum
            provider_enum = DataProvider.from_string(provider.value)

            # Convert market type string to enum
            market_type = MarketType.from_string(market.value)

            # Convert chart type string to enum
            chart_type_enum = ChartType.from_string(chart_type.value)

            # Convert interval string to enum
            interval_enum = Interval(interval)

            # Use the core DataSourceManager utility to calculate time range
            try:
                # Parse datetime strings if provided
                st = parse_datetime(start_time) if start_time else None
                et = parse_datetime(end_time) if end_time else None

                # Use the core data source manager utility
                from core.sync.data_source_manager import DataSourceManager

                start_datetime, end_datetime = DataSourceManager.calculate_time_range(
                    start_time=st, end_time=et, days=days, interval=interval_enum
                )
            except Exception as e:
                print(f"[bold red]Error calculating date range: {e}[/bold red]")
                sys.exit(1)

            # Process caching option
            use_cache = not no_cache

            # Process enforce source option
            enforce_source_enum = convert_source_choice(enforce_source)

            # Adjust symbol for market type if needed
            symbol_adjusted = adjust_symbol_for_market(symbol, market_type)

            # Fetch data using FCP
            df = fetch_data_with_fcp(
                provider=provider_enum,
                market_type=market_type,
                chart_type=chart_type_enum,
                symbol=symbol_adjusted,
                interval=interval_enum,
                start_time=start_datetime,
                end_time=end_datetime,
                use_cache=use_cache,
                enforce_source=enforce_source_enum,
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

            # Calculate and display script execution time
            end_time_perf = perf_counter()
            elapsed_time = end_time_perf - start_time_perf

            # Calculate and display performance metrics
            records_count = 0 if df is None or df.empty else len(df)
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
