#!/usr/bin/env python3
"""
DSM Demo Library: Core functionality for data source management demo.
This module provides reusable functions for data fetching and processing,
independent of any CLI or presentation logic.
"""

from pathlib import Path
from typing import Optional, Any, Tuple
from time import perf_counter

# Import the logger
from utils.logger_setup import logger

# Import core types and constraints
from utils.market_constraints import (
    MarketType,
    Interval,
    DataProvider,
    ChartType,
)

# Import utility modules
from utils.for_demo.dsm_cache_utils import clear_cache_directory, verify_project_root
from utils.for_demo.dsm_data_fetcher import fetch_data_with_fcp
from utils.for_demo.dsm_validation_utils import validate_interval, calculate_date_range
from core.sync.data_source_manager import DataSource

# Default cache directory
CACHE_DIR = Path("./cache")


def setup_environment(clear_cache: bool = False) -> bool:
    """
    Set up the environment for data fetching.

    Args:
        clear_cache: Whether to clear the cache directory

    Returns:
        bool: True if setup successful, False otherwise
    """
    try:
        # Verify project root
        if not verify_project_root():
            return False

        # Clear cache if requested
        if clear_cache:
            clear_cache_directory(CACHE_DIR)

        return True
    except Exception as e:
        logger.error(f"Environment setup failed: {e}")
        return False


def process_market_parameters(
    provider: str, market: str, chart_type: str, symbol: str, interval: str
) -> Tuple[DataProvider, MarketType, ChartType, str, Interval]:
    """
    Process and validate market-related parameters.

    Args:
        provider: Data provider name
        market: Market type name
        chart_type: Chart type name
        symbol: Trading symbol
        interval: Time interval

    Returns:
        Tuple containing processed enums and validated symbol
    """
    # Convert strings to enums
    provider_enum = DataProvider.from_string(provider)
    market_type = MarketType.from_string(market)
    chart_type_enum = ChartType.from_string(chart_type)
    interval_enum = Interval(interval)

    # Validate interval support
    validate_interval(market_type, interval_enum)

    # Adjust symbol for market type if needed
    if market_type == MarketType.FUTURES_COIN and not symbol.endswith("_PERP"):
        symbol = f"{symbol}_PERP"

    return provider_enum, market_type, chart_type_enum, symbol, interval_enum


def fetch_market_data(
    provider: DataProvider,
    market_type: MarketType,
    chart_type: ChartType,
    symbol: str,
    interval: Interval,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    days: int = 3,
    use_cache: bool = True,
    enforce_source: str = "AUTO",
    max_retries: int = 3,
) -> Tuple[Any, float, int]:
    """
    Fetch market data using the Failover Control Protocol.

    Args:
        provider: Data provider enum
        market_type: Market type enum
        chart_type: Chart type enum
        symbol: Trading symbol
        interval: Time interval enum
        start_time: Start time in ISO format
        end_time: End time in ISO format
        days: Number of days to fetch
        use_cache: Whether to use cache
        enforce_source: Enforce specific data source
        max_retries: Maximum retry attempts

    Returns:
        Tuple containing:
        - DataFrame with market data
        - Elapsed time in seconds
        - Number of records fetched
    """
    start_time_perf = perf_counter()

    try:
        # Calculate time range
        start_datetime, end_datetime = calculate_date_range(
            start_time, end_time, days, interval
        )

        # Convert enforce_source string to enum
        enforce_source_enum = DataSource[enforce_source.upper()]

        # Fetch data using FCP
        df = fetch_data_with_fcp(
            provider=provider,
            market_type=market_type,
            chart_type=chart_type,
            symbol=symbol,
            interval=interval,
            start_time=start_datetime,
            end_time=end_datetime,
            use_cache=use_cache,
            enforce_source=enforce_source_enum,
            max_retries=max_retries,
        )

        # Calculate performance metrics
        end_time_perf = perf_counter()
        elapsed_time = end_time_perf - start_time_perf
        records_count = 0 if df is None or df.empty else len(df)

        return df, elapsed_time, records_count

    except Exception as e:
        logger.error(f"Data fetching failed: {e}")
        raise
