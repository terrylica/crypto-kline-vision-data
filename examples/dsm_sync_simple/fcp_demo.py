#!/usr/bin/env python3
"""
FCP Demo: Demonstrates the Failover Control Protocol (FCP) mechanism.

This script allows users to specify a time span and observe how the
DataSourceManager automatically retrieves data from different sources
following the Failover Control Protocol (FCP) strategy:

1. Cache (Local Arrow files)
2. VISION API
3. REST API

It shows real-time source information about where each data point comes from,
and provides a summary of the data source breakdown.
"""

from pathlib import Path
from time import perf_counter
import sys
from typing import Optional
import pendulum

# Import the logger or logging and rich formatting
from utils.logger_setup import logger, configure_session_logging

from utils.market_constraints import MarketType, Interval, DataProvider, ChartType

# Import utility modules for FCP demo
from utils.for_demo.fcp_datetime_parser import parse_datetime, calculate_date_range
from utils.for_demo.fcp_cache_utils import clear_cache_directory, verify_project_root
from utils.for_demo.fcp_data_fetcher import fetch_data_with_fcp
from utils.for_demo.fcp_display_utils import display_results
from utils.for_demo.fcp_test_utils import test_fcp_pm_mechanism
from utils.for_demo.fcp_doc_utils import generate_markdown_docs
from utils.for_demo.fcp_cli_utils import (
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
)
from utils.for_demo.fcp_app_options import (
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
    symbol: str = options["symbol"],
    market: MarketTypeChoice = options["market"],
    interval: str = options["interval"],
    chart_type: ChartTypeChoice = options["chart_type"],
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
    # Test Mode options
    test_fcp_pm: bool = options["test_fcp_pm"],
    prepare_cache: bool = options["prepare_cache"],
    # Documentation options
    gen_doc: bool = options["gen_doc"],
    gen_lint_config: bool = options["gen_lint_config"],
    # Other options
    log_level: LogLevel = options["log_level"],
):
    """FCP Demo: Demonstrates the Failover Control Protocol (FCP) mechanism."""
    # Convert shorthand log levels to full names
    level = resolve_log_level(log_level.value)

    # Set up session logging (delegated to logger_setup.py)
    main_log, error_log, log_timestamp = configure_session_logging("fcp_demo", level)

    logger.info(f"Current time: {pendulum.now().isoformat()}")

    try:
        # Check if we should generate documentation
        if gen_doc:
            logger.info("Generating Markdown documentation from Typer help...")
            doc_path = generate_markdown_docs(app, gen_lint_config=gen_lint_config)
            logger.info(f"Documentation generated and saved to {doc_path}")
            return

        # Print introductory information
        print_intro_panel()
        print_logging_panel(main_log, error_log)

        # Verify project root
        if not verify_project_root():
            sys.exit(1)

        # Display configuration
        print_config_table(
            symbol,
            market.value,
            interval,
            chart_type.value,
            start_time,
            end_time,
            days,
            enforce_source.value,
            retries,
            no_cache,
            clear_cache,
            test_fcp_pm,
            prepare_cache,
            level,
        )

        # Clear cache if requested
        if clear_cache:
            clear_cache_directory(CACHE_DIR)

        # Check if we should run the FCP test
        if test_fcp_pm:
            # Add debug logging
            logger.debug(f"Running FCP test with:")
            logger.debug(f"  Symbol: {symbol}")
            logger.debug(f"  Market: {market.value} (converting to enum)")
            logger.debug(f"  Interval: {interval}")
            logger.debug(f"  Start time: {start_time!r}")
            logger.debug(f"  End time: {end_time!r}")
            logger.debug(f"  Days: {days}")
            logger.debug(f"  Prepare cache: {prepare_cache}")

            # Calculate dates based on days parameter if provided
            days_provided = "--days" in sys.argv or "-d" in sys.argv
            if days_provided:
                start_datetime, end_datetime = calculate_date_range(None, None, days)
                logger.debug(f"Using calculated date range based on days={days}")
                logger.debug(f"Calculated start time: {start_datetime.isoformat()}")
                logger.debug(f"Calculated end time: {end_datetime.isoformat()}")
                pass_start_date = start_datetime.isoformat()
                pass_end_date = end_datetime.isoformat()
            else:
                # Use the provided start_time and end_time
                pass_start_date = start_time
                pass_end_date = end_time

            # Run the FCP mechanism test
            test_fcp_pm_mechanism(
                symbol=symbol,
                market_type=MarketType.from_string(market.value),
                interval=Interval(interval),
                chart_type=ChartType.from_string(chart_type.value),
                start_date=pass_start_date,
                end_date=pass_end_date,
                days=days,
                prepare_cache=prepare_cache,
                cache_dir=CACHE_DIR,
                performance_timer_start=start_time_perf,
            )
            # Return from function after running test_fcp_pm_mechanism
            # to avoid duplicating performance output
            return

        # Validate and process arguments
        try:
            # Convert market type string to enum
            market_type = MarketType.from_string(market.value)

            # Convert interval string to enum
            interval_enum = Interval(interval)

            # Convert chart type string to enum
            chart_type_enum = ChartType.from_string(chart_type.value)

            # Determine time range using utility function
            days_provided = sys.argv and "--days" in sys.argv or "-d" in sys.argv
            if start_time and end_time and not days_provided:
                # Use specified time range
                start_datetime = parse_datetime(start_time)
                end_datetime = parse_datetime(end_time)
            else:
                # Use days parameter to calculate time range
                start_datetime, end_datetime = calculate_date_range(None, None, days)
                print(f"[yellow]Using dynamic date range based on days={days}[/yellow]")
                print(f"[yellow]Overriding default start_time and end_time[/yellow]")

            # Process caching option
            use_cache = not no_cache

            # Process enforce source option
            enforce_source_enum = convert_source_choice(enforce_source)

            # Adjust symbol for market type if needed
            symbol_adjusted = adjust_symbol_for_market(symbol, market_type)

            # Fetch data using FCP
            df = fetch_data_with_fcp(
                market_type=market_type,
                symbol=symbol_adjusted,
                start_time=start_datetime,
                end_time=end_datetime,
                interval=interval_enum,
                provider=DataProvider.BINANCE,
                chart_type=chart_type_enum,
                use_cache=use_cache,
                enforce_source=enforce_source_enum,
                max_retries=retries,
            )

            # Display results
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
