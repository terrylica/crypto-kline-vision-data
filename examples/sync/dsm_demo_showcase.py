#!/usr/bin/env python3
"""
DSM Demo Showcase: Example of end time backward retrieval with log control.
This module demonstrates how to use the DSM library functions directly for
fetching historical data from a specified end time, similar to the CLI usage:

    ./examples/sync/dsm_demo.py -s BTCUSDT -et 2025-04-14T15:59:59 -i 1m -d 10 -l E
"""

import pendulum
from rich import print
from utils.logger_setup import logger, configure_session_logging
import pandas as pd

from dsm_demo_lib import (
    setup_environment,
    process_market_parameters,
    fetch_market_data,
)
from utils.deprecation_rules import Interval as DeprecationInterval


def showcase_backward_retrieval(
    symbol: str = "BTCUSDT",
    end_time: str = "2025-04-14T15:59:59",
    interval: str = "1m",
    days: int = 10,
    log_level: str = "ERROR",
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
    """
    print(f"\n[bold blue]Backward Data Retrieval Example[/bold blue]")
    print(f"[cyan]Configuration:[/cyan]")
    print(f"• Symbol: {symbol}")
    print(f"• End Time: {end_time}")
    print(f"• Interval: {interval}")
    print(f"• Days Back: {days}")
    print(f"• Log Level: {log_level}\n")

    # Process market parameters
    provider_enum, market_type, chart_type_enum, symbol, interval_enum = (
        process_market_parameters(
            provider="binance",
            market="spot",
            chart_type="klines",
            symbol=symbol,
            interval=interval,
        )
    )

    # Calculate start time for display
    end_dt = pendulum.parse(end_time)
    start_dt = end_dt.subtract(days=days)
    print(f"[yellow]Time Range:[/yellow]")
    print(f"From: {start_dt.format('YYYY-MM-DD HH:mm:ss')}")
    print(f"To:   {end_dt.format('YYYY-MM-DD HH:mm:ss')}\n")

    # Fetch data with backward retrieval
    df, elapsed_time, records = fetch_market_data(
        provider=provider_enum,
        market_type=market_type,
        chart_type=chart_type_enum,
        symbol=symbol,
        interval=interval_enum,
        end_time=end_time,
        days=days,
    )

    # Display results
    if records > 0:
        print(
            f"[green]✓ Successfully fetched {records:,} records in {elapsed_time:.2f} seconds[/green]"
        )
        print("\n[cyan]Data Range Summary:[/cyan]")

        # Convert index to datetime if it's not already
        if not isinstance(df.index, pd.DatetimeIndex):
            # Create a DeprecationInterval instance from MarketInterval
            interval_obj = DeprecationInterval.from_market_interval(interval_enum)
            # Convert to pandas Timedelta for frequency
            pandas_timedelta = interval_obj.to_pandas_timedelta()
            # Create the frequency string using the non-deprecated format
            freq = f"{interval_obj.value}{interval_obj.unit.value}"
            # Use the proper frequency string for date_range
            df.index = pd.date_range(
                start=start_dt.isoformat(), periods=len(df), freq=freq, tz="UTC"
            )

        first_ts = df.index[0]
        last_ts = df.index[-1]
        print(f"First timestamp: {first_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"Last timestamp:  {last_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")

        # Calculate actual time coverage
        actual_days = (last_ts - first_ts).days
        print(f"\nActual days covered: {actual_days} days")

        # Show data distribution
        dates = df.index.date
        date_counts = pd.Series(dates).value_counts().sort_index()
        print("\n[cyan]Records per date:[/cyan]")
        for date, count in date_counts.items():
            print(f"• {date}: {count:,} records")
    else:
        print("[red]✗ No data retrieved[/red]")


def main():
    """Run the backward retrieval showcase example."""
    # Configure logging with ERROR level
    configure_session_logging("dsm_showcase", "ERROR")
    logger.info(f"Starting showcase at {pendulum.now().isoformat()}")

    # Set up environment
    if not setup_environment():
        print("[red]Failed to set up environment[/red]")
        return

    try:
        # Run the showcase with default parameters matching CLI example
        showcase_backward_retrieval()

        # Example of running with custom parameters
        print("\n[bold blue]Additional Example with Custom Parameters:[/bold blue]")
        showcase_backward_retrieval(
            symbol="ETHUSDT",
            end_time="2025-04-15T00:00:00",
            interval="5m",
            days=5,
            log_level="ERROR",
        )

    except Exception as e:
        print(f"[red]Showcase failed: {e}[/red]")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
