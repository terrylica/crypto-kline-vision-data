#!/usr/bin/env python3
"""
DSM Demo Showcase: Example of end time backward retrieval with log control.
This module demonstrates how to use the DSM library functions directly for
fetching historical data from a specified end time, similar to the CLI usage:

    ./examples/sync/dsm_demo_cli.py -s BTCUSDT -et 2025-04-14T15:59:59 -i 1m -d 10 -l E
"""

import os
from pathlib import Path

import pandas as pd
import pendulum
from rich import print

from core.sync.dsm_lib import (
    fetch_market_data,
    process_market_parameters,
    setup_environment,
)
from utils.deprecation_rules import Interval as DeprecationInterval
from utils.for_demo.dsm_display_utils import display_results
from utils.logger_setup import configure_session_logging, logger


def showcase_backward_retrieval(
    symbol: str = "BTCUSDT",
    end_time: str = "2025-04-14T15:59:59",
    interval: str = "1m",
    days: int = 10,
    log_level: str = "INFO",
    log_timestamp: str = None,
) -> None:
    """
    Demonstrate backward data retrieval from a specified end time.

    This function shows how to:
    1. Configure logging with specific level
    2. Process market parameters
    3. Fetch historical data going backward from end time
    4. Handle and display results

    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")
        end_time: End time in ISO format (YYYY-MM-DDTHH:mm:ss)
        interval: Time interval (e.g., "1m", "5m", "1h")
        days: Number of days to fetch backward
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_timestamp: Timestamp string for log file naming
    """
    # Set the log level from the parameter
    logger.setLevel(log_level)

    logger.info(
        f"Starting showcase_backward_retrieval with symbol={symbol}, interval={interval}, days={days}"
    )

    print("\n[bold blue]Backward Data Retrieval Example[/bold blue]")
    print("[cyan]Configuration:[/cyan]")
    print(f"• Symbol: {symbol}")
    print(f"• End Time: {end_time}")
    print(f"• Interval: {interval}")
    print(f"• Days Back: {days}")
    print(f"• Log Level: {log_level}\n")

    # Process market parameters
    logger.debug(f"Processing market parameters for {symbol}")
    provider_enum, market_type, chart_type_enum, symbol, interval_enum = (
        process_market_parameters(
            provider="binance",
            market="spot",
            chart_type="klines",
            symbol=symbol,
            interval=interval,
        )
    )
    logger.debug(
        f"Market parameters processed: provider={provider_enum}, market_type={market_type}, chart_type={chart_type_enum}"
    )

    # Calculate start time for display
    end_dt = pendulum.parse(end_time)
    start_dt = end_dt.subtract(days=days)
    logger.debug(f"Time range: {start_dt.isoformat()} to {end_dt.isoformat()}")

    print("[yellow]Time Range:[/yellow]")
    print(f"From: {start_dt.format('YYYY-MM-DD HH:mm:ss')}")
    print(f"To:   {end_dt.format('YYYY-MM-DD HH:mm:ss')}\n")

    # Fetch data with backward retrieval
    logger.info(
        f"Fetching market data for {symbol} from {start_dt.isoformat()} to {end_dt.isoformat()}"
    )
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
        print(
            f"[green]✓ Successfully fetched {records:,} records in {elapsed_time:.2f} seconds[/green]"
        )

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
            df.index = pd.date_range(
                start=start_dt.isoformat(), periods=len(df), freq=freq, tz="UTC"
            )

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
    """Run the backward retrieval showcase example."""
    # Show execution environment info
    cwd = os.getcwd()
    logger.debug(f"Current working directory: {cwd}")
    if Path(cwd).name != "raw-data-services":
        print(
            f"[yellow]Warning: Not running from project root. Current directory: {cwd}[/yellow]"
        )
    else:
        print("Running from project root directory")

    # Configure logging with DEBUG level by default
    current_time = pendulum.now()
    logger.info(f"Starting showcase at {current_time.isoformat()}")

    # Configure logging and capture log file paths and timestamp
    main_log, error_log, log_timestamp = configure_session_logging(
        "dsm_demo_module", "INFO"
    )

    # Log the paths to help with debugging
    logger.debug(f"Main log file: {main_log}")
    logger.debug(f"Error log file: {error_log}")
    logger.debug(f"Log timestamp: {log_timestamp}")

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
