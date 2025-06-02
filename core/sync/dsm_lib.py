#!/usr/bin/env python3
"""Data Source Manager library interface module.

This module provides the primary high-level interface for the Data Source Manager,
implementing the Failover Control Protocol (FCP) for robust data retrieval.

The FCP mechanism consists of three integrated phases:
1. Local Cache Retrieval: Quickly obtain data from local Apache Arrow files
2. Vision API Retrieval: Supplement missing data segments from Vision API
3. REST API Fallback: Ensure complete data coverage for any remaining segments

The main entry point is the fetch_market_data function, which orchestrates
data retrieval from all available sources based on the provided parameters.

Key components:
- setup_environment: Prepare the environment for data fetching
- process_market_parameters: Validate and process market parameters
- fetch_market_data: Primary interface for retrieving market data using FCP

Example:
    >>> from data_source_manager import fetch_market_data, MarketType, DataProvider, Interval, ChartType
    >>> from datetime import datetime
    >>>
    >>> df, elapsed_time, records_count = fetch_market_data(
    ...     provider=DataProvider.BINANCE,
    ...     market_type=MarketType.SPOT,
    ...     chart_type=ChartType.KLINES,
    ...     symbol="BTCUSDT",
    ...     interval=Interval.MINUTE_1,
    ...     start_time=datetime(2023, 1, 1),
    ...     end_time=datetime(2023, 1, 10),
    ...     use_cache=True,
    ... )
"""

from datetime import datetime
from time import perf_counter

from core.sync.data_source_manager import DataSource
from utils.dataframe_types import TimestampedDataFrame

# Import utility modules
from utils.for_demo.dsm_cache_utils import (
    clear_all_cache_directories,
    ensure_cache_directory,
    verify_project_root,
)
from utils.for_demo.dsm_data_fetcher import fetch_data_with_fcp
from utils.for_demo.dsm_validation_utils import calculate_date_range, validate_interval

# Import the logger
from utils.loguru_setup import logger

# Import core types and constraints
from utils.market_constraints import (
    ChartType,
    DataProvider,
    Interval,
    MarketType,
    get_default_symbol,
)


def setup_environment(clear_cache: bool = False) -> bool:
    """Set up the environment for data fetching.

    This function prepares the environment for data fetching operations by:
    1. Verifying the project root location
    2. Ensuring the cache directory exists
    3. Optionally clearing existing cache data

    Args:
        clear_cache: Whether to clear the cache directory before fetching data.
                    If True, all cached data will be removed.

    Returns:
        bool: True if setup was successful, False otherwise

    Raises:
        OSError: If cache directory creation fails

    Example:
        >>> success = setup_environment(clear_cache=False)
        >>> print(success)
        True
    """
    try:
        # Verify project root (always returns True now, kept for backward compatibility)
        verify_project_root()

        # Create cache directory if it doesn't exist
        cache_dir = ensure_cache_directory()
        logger.debug(f"Using cache directory: {cache_dir}")

        # Clear cache if requested
        if clear_cache:
            # Clear all cache directories for a complete cleanup
            clear_all_cache_directories()

        return True
    except Exception as e:
        logger.error(f"Environment setup failed: {e}")
        return False


def process_market_parameters(
    provider: str, market: str, chart_type: str, symbol: str, interval: str
) -> tuple[DataProvider, MarketType, ChartType, str, Interval]:
    """Process and validate market-related parameters.

    This function converts string parameters to their appropriate enum types,
    validates that the parameters are compatible with each other, and performs
    market-specific validations.

    Args:
        provider: Data provider name (e.g., "binance")
        market: Market type name (e.g., "spot", "futures_usdt", "futures_coin")
        chart_type: Chart type name (e.g., "klines")
        symbol: Trading symbol (e.g., "BTCUSDT" for spot, "BTC_PERP" for CM)
        interval: Time interval (e.g., "1m", "1h")

    Returns:
        Tuple containing:
        - provider_enum: Validated DataProvider enum
        - market_type: Validated MarketType enum
        - chart_type_enum: Validated ChartType enum
        - symbol: Validated symbol string (default provided if empty)
        - interval_enum: Validated Interval enum

    Raises:
        ValueError: If any parameter is invalid or incompatible with others

    Example:
        >>> provider, market, chart, sym, interval = process_market_parameters(
        ...     "binance", "spot", "klines", "BTCUSDT", "1m"
        ... )
        >>> print(market)
        spot
    """
    # Convert strings to enums
    provider_enum = DataProvider.from_string(provider)
    market_type = MarketType.from_string(market)
    chart_type_enum = ChartType.from_string(chart_type)
    interval_enum = Interval(interval)

    # Validate interval support
    validate_interval(market_type, interval_enum)

    # Validate symbol format for market type
    # For CM market, ensure symbol ends with _PERP and has USD (not USDT)
    if market_type == MarketType.FUTURES_COIN:
        if not symbol.endswith("_PERP"):
            raise ValueError(f"Symbol for coin-margined futures must end with '_PERP'. Invalid: '{symbol}'")
        if "USDT" in symbol:
            raise ValueError(f"Symbol for coin-margined futures must use USD, not USDT. Invalid: '{symbol}'")

    # If no symbol is provided, use default symbol for the market type
    if not symbol:
        symbol = get_default_symbol(market_type)

    return provider_enum, market_type, chart_type_enum, symbol, interval_enum


def fetch_market_data(
    provider: DataProvider,
    market_type: MarketType,
    chart_type: ChartType,
    symbol: str,
    interval: Interval,
    start_time: datetime | str | None = None,
    end_time: datetime | str | None = None,
    days: int = 3,
    use_cache: bool = True,
    enforce_source: str = "AUTO",
    max_retries: int = 3,
) -> tuple[TimestampedDataFrame | None, float, int]:
    """Fetch market data using the Failover Control Protocol.

    This function retrieves market data from multiple sources using a progressive
    approach that prioritizes speed and reliability:
    1. First attempts to retrieve data from local cache (if use_cache=True)
    2. Then retrieves missing data from Vision API
    3. Finally falls back to REST API for any remaining data

    The function handles time range validation, data normalization, and merging
    data from multiple sources into a consistent DataFrame.

    Args:
        provider: The data provider (e.g., BINANCE)
        market_type: Type of market (SPOT, UM, CM)
        chart_type: Type of chart data (KLINES, etc.)
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: Time interval (e.g., MINUTE_1, HOUR_1)
        start_time: Start datetime (UTC) or ISO format string
        end_time: End datetime (UTC) or ISO format string
        days: Number of days to fetch (backward from end_time)
        use_cache: Whether to use the local cache
        enforce_source: Enforce specific data source ("AUTO", "CACHE", "VISION", "REST")
        max_retries: Maximum retry attempts for API calls

    Returns:
        Tuple containing:
        - DataFrame with market data (or None if error)
        - Elapsed time in seconds
        - Number of records retrieved

    Raises:
        ValueError: If time parameters are invalid or incompatible
        RuntimeError: If data cannot be retrieved from any source

    Example:
        >>> df, elapsed_time, count = fetch_market_data(
        ...     provider=DataProvider.BINANCE,
        ...     market_type=MarketType.SPOT,
        ...     chart_type=ChartType.KLINES,
        ...     symbol="BTCUSDT",
        ...     interval=Interval.MINUTE_1,
        ...     end_time=datetime(2023, 1, 10),
        ...     days=5,
        ...     use_cache=True
        ... )
        >>> print(f"Retrieved {count} records in {elapsed_time:.2f} seconds")
    """
    start_time_perf = perf_counter()

    try:
        # Calculate time range
        start_datetime, end_datetime = calculate_date_range(start_time, end_time, days, interval)

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
