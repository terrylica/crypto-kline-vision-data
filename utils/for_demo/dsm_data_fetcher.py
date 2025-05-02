#!/usr/bin/env python3
"""
Data fetching utilities for the Failover Control Protocol (FCP) demos.

This module provides functions to fetch data using the Failover Control Protocol
mechanism, which automatically selects the appropriate data source based on availability.
"""

import time

import pandas as pd
import pendulum
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from core.sync.data_source_manager import DataSource, DataSourceManager
from utils.logger_setup import logger
from utils.market_constraints import (
    ChartType,
    DataProvider,
    Interval,
    MarketType,
    get_market_capabilities,
    is_interval_supported,
)
from utils_for_debug.data_integrity import analyze_data_integrity
from utils_for_debug.dataframe_output import (
    log_dataframe_info,
    print_integrity_results,
    print_no_data_message,
)


def fetch_data_with_fcp(
    provider: DataProvider = DataProvider.BINANCE,
    market_type: MarketType = MarketType.SPOT,
    chart_type: ChartType = ChartType.KLINES,
    symbol: str = "BTCUSDT",
    interval: Interval = Interval.MINUTE_1,
    start_time: pendulum.DateTime = None,
    end_time: pendulum.DateTime = None,
    use_cache: bool = True,
    enforce_source: DataSource = DataSource.AUTO,
    max_retries: int = 3,
):
    """
    Fetch data using Failover Control Protocol (FCP) mechanism.

    Args:
        provider: Data provider (currently only BINANCE is supported)
        market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
        chart_type: Type of chart data to retrieve (KLINES, FUNDING_RATE)
        symbol: Symbol to retrieve data for (e.g., "BTCUSDT")
        interval: Time interval between data points
        start_time: Start time for data retrieval
        end_time: End time for data retrieval
        use_cache: Whether to use caching
        enforce_source: Force specific data source (AUTO, REST, VISION)
        max_retries: Maximum number of retry attempts

    Returns:
        Pandas DataFrame containing the retrieved data
    """
    # Validate if interval is supported by the market type
    if not is_interval_supported(market_type, interval):
        capabilities = get_market_capabilities(market_type)
        supported = [i.value for i in capabilities.supported_intervals]

        console = Console()
        console.print(
            f"[bold red]ERROR: Interval {interval.value} is not supported by {market_type.name} market.[/bold red]"
        )
        console.print(f"[yellow]Supported intervals: {', '.join(supported)}[/yellow]")
        console.print("[cyan]Please choose a supported interval and try again.[/cyan]")

        logger.error(
            f"Interval {interval.value} not supported by {market_type.name} market. Supported intervals: {supported}"
        )
        return pd.DataFrame()

    logger.info(
        f"Retrieving {interval.value} {chart_type.name} data for {symbol} in {market_type.name} market"
    )
    logger.info(f"Time range: {start_time.isoformat()} to {end_time.isoformat()}")
    logger.info(f"Cache enabled: {use_cache}")

    if enforce_source is not None:
        logger.info(f"Enforcing data source: {enforce_source.name}")
    else:
        logger.info("Using AUTO source selection (FCP: Cache → Vision → REST)")

    logger.info(
        f"[bold red]Attempting[/bold red] to fetch data from {start_time.isoformat()} to {end_time.isoformat()}..."
    )

    # Calculate expected record count for validation
    interval_seconds = interval.to_seconds()
    expected_seconds = int((end_time - start_time).total_seconds())
    expected_records = (expected_seconds // interval_seconds) + 1
    logger.debug(
        f"Expected record count: {expected_records} for {expected_seconds} seconds range"
    )

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold green]Fetching data..."),
            transient=True,
        ) as progress:
            progress_task = progress.add_task("Fetching...", total=None)

            start_time_retrieval = time.time()

            # Create a DataSourceManager instance with the specified parameters
            with DataSourceManager(
                provider=provider,
                market_type=market_type,
                chart_type=chart_type,
                use_cache=use_cache,
                retry_count=max_retries,
            ) as manager:
                # Retrieve data using the manager
                # The manager will handle the FCP strategy: cache → Vision API → REST API
                df = manager.get_data(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval,
                    chart_type=chart_type,
                    enforce_source=enforce_source,
                    include_source_info=True,  # Always include source information
                )

            elapsed_time = time.time() - start_time_retrieval
            progress.update(progress_task, completed=100)

        if df is None or df.empty:
            logger.warning(f"No data retrieved for {symbol}")
            print_no_data_message(
                provider=provider,
                market_type=market_type,
                chart_type=chart_type,
                symbol=symbol,
                interval=interval,
                start_time=start_time,
                end_time=end_time,
                enforce_source=enforce_source,
                use_cache=use_cache,
            )
            return pd.DataFrame()

        logger.info(
            f"Retrieved {len(df)} records for {symbol} in {elapsed_time:.2f} seconds"
        )

        # Analyze data integrity
        logger.debug("Analyzing data integrity...")
        integrity_result = analyze_data_integrity(df, start_time, end_time, interval)

        # Print the integrity results in a user-friendly format
        print_integrity_results(integrity_result)

        # Log DataFrame structure information for debugging
        log_dataframe_info(df)

        # Show a summary panel
        print(
            Panel(
                "[bold green]FCP Mechanism Complete[/bold green]\n"
                "The DataSourceManager automatically:\n"
                "1. Retrieved data from local cache when available\n"
                "2. Retrieved missing data from Vision API\n"
                "3. Retrieved remaining data from REST API\n"
                "4. Merged all data sources into a single coherent DataFrame",
                title="Summary",
                border_style="green",
            )
        )

        return df
    except Exception as e:
        print(f"[bold red]Error fetching data: {e}[/bold red]")
        import traceback

        traceback.print_exc()
        return pd.DataFrame()
