#!/usr/bin/env python3
"""Data Source Manager (DSM) Library Module Usage Demonstration.

This module demonstrates how to use the DSM library API programmatically in your
own Python applications. It showcases:

1. Proper configuration and initialization of the DSM library
2. Fetching historical market data with flexible time parameters
3. Processing and displaying the retrieved data
4. Error handling and logging best practices
5. Cache management and performance optimization

The module is structured as a reusable example that you can adapt for your own
applications. It includes both a showcased function for backward data retrieval
and a main entry point that demonstrates different usage patterns.

Example usage as a script:
    $ python examples/lib_module/dsm_demo_module.py

Example usage as a module:
    >>> from examples.lib_module.dsm_demo_module import showcase_backward_retrieval
    >>>
    >>> # Basic usage with default parameters
    >>> showcase_backward_retrieval()
    >>>
    >>> # Custom parameters for specific data retrieval
    >>> showcase_backward_retrieval(
    ...     symbol="ETHUSDT",
    ...     end_time="2025-01-01T00:00:00",
    ...     interval="5m",
    ...     days=3,
    ...     log_level="DEBUG"
    ... )

This is analogous to the CLI command:
    dsm-demo-cli -s BTCUSDT -et 2025-04-14T15:59:59 -i 1m -d 10 -l E
"""

import os

import pandas as pd
import pendulum
from rich import print

from data_source_manager.core.sync.dsm_lib import (
    fetch_market_data,
    process_market_parameters,
    setup_environment,
)
from data_source_manager.utils.app_paths import get_cache_dir, get_log_dir
from data_source_manager.utils.deprecation_rules import Interval as DeprecationInterval
from data_source_manager.utils.for_demo.dsm_cache_utils import print_cache_info
from data_source_manager.utils.for_demo.dsm_display_utils import display_results
from data_source_manager.utils.loguru_setup import configure_session_logging, logger


def showcase_backward_retrieval(
    symbol: str = "BTCUSDT",
    end_time: str = "2025-04-14T15:59:59",
    interval: str = "1m",
    days: int = 10,
    log_level: str = "INFO",
    log_timestamp: str | None = None,
) -> None:
    """Demonstrate backward data retrieval from a specified end time with rich display.

    This function provides a complete example of using the DSM library API for retrieving
    historical market data, starting from a specified end time and going backward for a
    given number of days. It demonstrates proper parameter processing, data retrieval,
    and result visualization.

    Key aspects demonstrated:
    1. Configuring logging with appropriate levels
    2. Parameter validation and transformation
    3. Market data retrieval with progress tracking
    4. Rich terminal display of results
    5. Statistics generation and data range validation

    Args:
        symbol: Trading symbol to retrieve data for (e.g., "BTCUSDT", "ETHUSDT")
        end_time: End time in ISO format (YYYY-MM-DDTHH:mm:ss)
            This is the point from which to retrieve data backward
        interval: Time interval for data points (e.g., "1s", "1m", "5m", "1h")
            Supported values match those in utils.market_constraints.Interval
        days: Number of days to retrieve backward from end_time
            Larger values will result in more data and longer retrieval times
        log_level: Logging level to use (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            DEBUG provides the most detailed output, INFO is recommended for general use
        log_timestamp: Optional timestamp string for log file naming
            If None, current timestamp will be used

    Returns:
        None: Results are displayed to the console and written to log files

    Examples:
        Basic usage with default parameters:

        >>> showcase_backward_retrieval()

        Custom parameters for specific data needs:

        >>> showcase_backward_retrieval(
        ...     symbol="ETHUSDT",  # Use Ethereum instead of Bitcoin
        ...     end_time="2025-01-01T00:00:00",  # Specific date
        ...     interval="5m",  # 5-minute intervals for less granular data
        ...     days=3,  # Just 3 days of data
        ...     log_level="DEBUG"  # More detailed logging
        ... )

    Note:
        This function relies on the DSM library's fetch_market_data function, which
        handles data source selection, caching, and error handling automatically.
        The display portion uses Rich for terminal formatting, but could be modified
        to output to files, databases, or other visualization tools.
    """
    # Set the log level from the parameter
    logger.setLevel(log_level)

    logger.info(f"Starting showcase_backward_retrieval with symbol={symbol}, interval={interval}, days={days}")

    print("\n[bold blue]Backward Data Retrieval Example[/bold blue]")
    print("[cyan]Configuration:[/cyan]")
    print(f"• Symbol: {symbol}")
    print(f"• End Time: {end_time}")
    print(f"• Interval: {interval}")
    print(f"• Days Back: {days}")
    print(f"• Log Level: {log_level}\n")

    # Process market parameters
    logger.debug(f"Processing market parameters for {symbol}")
    provider_enum, market_type, chart_type_enum, symbol, interval_enum = process_market_parameters(
        provider="binance",
        market="spot",
        chart_type="klines",
        symbol=symbol,
        interval=interval,
    )
    logger.debug(f"Market parameters processed: provider={provider_enum}, market_type={market_type}, chart_type={chart_type_enum}")

    # Calculate start time for display
    end_dt = pendulum.parse(end_time)
    start_dt = end_dt.subtract(days=days)
    logger.debug(f"Time range: {start_dt.isoformat()} to {end_dt.isoformat()}")

    print("[yellow]Time Range:[/yellow]")
    print(f"From: {start_dt.format('YYYY-MM-DD HH:mm:ss')}")
    print(f"To:   {end_dt.format('YYYY-MM-DD HH:mm:ss')}\n")

    # Fetch data with backward retrieval
    logger.info(f"Fetching market data for {symbol} from {start_dt.isoformat()} to {end_dt.isoformat()}")
    df, elapsed_time, records = fetch_market_data(
        provider=provider_enum,
        market_type=market_type,
        chart_type=chart_type_enum,
        symbol=symbol,
        interval=interval_enum,
        end_time=end_time,
        days=days,
    )
    logger.info(f"Fetched {records} records in {elapsed_time:.2f} seconds")

    # Display results
    if records > 0:
        print(f"[green]✓ Successfully fetched {records:,} records in {elapsed_time:.2f} seconds[/green]")

        # Use the display_results function for consistent display with dsm_demo_cli.py
        timestamp = log_timestamp or pendulum.now().format("YYYYMMDD_HHmmss")
        logger.debug(f"Using timestamp {timestamp} for result display")

        display_results(
            df,
            symbol,
            market_type,
            interval_enum.value,
            chart_type_enum.name.lower(),
            timestamp,
            "dsm_demo_module",
        )

        # Show data range summary
        print("\n[cyan]Data Range Summary:[/cyan]")

        # Convert index to datetime if it's not already
        if not isinstance(df.index, pd.DatetimeIndex):
            logger.debug("Converting DataFrame index to DatetimeIndex")
            # Create a DeprecationInterval instance from MarketInterval
            interval_obj = DeprecationInterval.from_market_interval(interval_enum)
            # Create the frequency string using the non-deprecated format
            freq = f"{interval_obj.value}{interval_obj.unit.value}"
            # Use the proper frequency string for date_range
            df.index = pd.date_range(start=start_dt.isoformat(), periods=len(df), freq=freq, tz="UTC")

        first_ts = df.index[0]
        last_ts = df.index[-1]
        logger.debug(f"Data range: {first_ts} to {last_ts}")
        print(f"First timestamp: {first_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"Last timestamp:  {last_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")

        # Calculate actual time coverage
        actual_days = (last_ts - first_ts).days
        logger.debug(f"Actual days covered: {actual_days}")
        print(f"\nActual days covered: {actual_days} days")

        # Show data distribution
        dates = df.index.date
        date_counts = pd.Series(dates).value_counts().sort_index()
        logger.debug(f"Date distribution: {date_counts.to_dict()}")
        print("\n[cyan]Records per date:[/cyan]")
        for date, count in date_counts.items():
            print(f"• {date}: {count:,} records")
    else:
        logger.warning("No data retrieved")
        print("[red]✗ No data retrieved[/red]")


def main():
    """Run the DSM demo showcase with multiple examples.

    This function serves as the main entry point when this module is executed directly.
    It demonstrates:

    1. Environment setup and initialization
    2. Logging configuration for both console and file output
    3. Cache information display and management
    4. Multiple example runs with different parameters
    5. Error handling and graceful failure modes

    The function runs two demonstration scenarios:
    1. Default example: BTCUSDT with 1-minute intervals for 10 days
    2. Custom example: ETHUSDT with 5-minute intervals for 5 days

    This allows you to compare behavior across different symbols and intervals.

    Returns:
        None: Results are displayed to the console and written to log files

    Examples:
        To run this function directly:

        $ python examples/lib_module/dsm_demo_module.py

        Or from Python:

        >>> from examples.lib_module.dsm_demo_module import main
        >>> main()

    Note:
        You can modify this function to create your own custom examples
        by adding more calls to showcase_backward_retrieval with different
        parameters, or by implementing additional showcase functions for
        other retrieval patterns.
    """
    # Show execution environment info
    cwd = os.getcwd()
    logger.debug(f"Current working directory: {cwd}")

    # Log directories for reference
    log_dir = get_log_dir()
    cache_dir = get_cache_dir()
    logger.info(f"Using log directory: {log_dir}")
    logger.info(f"Using cache directory: {cache_dir}")

    # Configure logging with DEBUG level by default
    current_time = pendulum.now()
    logger.info(f"Starting showcase at {current_time.isoformat()}")

    # Configure logging and capture log file paths and timestamp
    main_log, error_log, log_timestamp = configure_session_logging("dsm_demo_module", "INFO")

    # Log the paths to help with debugging
    logger.debug(f"Main log file: {main_log}")
    logger.debug(f"Error log file: {error_log}")
    logger.debug(f"Log timestamp: {log_timestamp}")

    # Display cache info once at startup
    print_cache_info()

    # Set up environment
    logger.info("Setting up environment")
    if not setup_environment():
        logger.error("Failed to set up environment")
        print("[red]Failed to set up environment[/red]")
        return

    try:
        # Run the showcase with default parameters
        showcase_backward_retrieval(log_timestamp=log_timestamp)

        # Example of running with custom parameters
        print("\n[bold blue]Additional Example with Custom Parameters:[/bold blue]")
        logger.info("Running additional example with ETHUSDT")
        showcase_backward_retrieval(
            symbol="ETHUSDT",
            end_time="2025-04-15T00:00:00",
            interval="5m",
            days=5,
            log_level="INFO",
            log_timestamp=log_timestamp,
        )

    except Exception as e:
        logger.exception(f"Showcase failed: {e}")
        print(f"[red]Showcase failed: {e}[/red]")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
