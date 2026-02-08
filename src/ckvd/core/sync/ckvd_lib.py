#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Crypto Kline Vision Data library interface module.

This module provides the primary high-level interface for the Crypto Kline Vision Data,
implementing the Failover Control Protocol (FCP) for robust data retrieval.

The main entry point is the fetch_market_data function, which delegates to
CryptoKlineVisionData.get_data() for all FCP logic including cache, Vision API,
and REST API fallback.

Example:
    >>> from ckvd import fetch_market_data, MarketType, DataProvider, Interval, ChartType
    >>> from datetime import datetime, timezone
    >>>
    >>> df, elapsed_time, records_count = fetch_market_data(
    ...     provider=DataProvider.BINANCE,
    ...     market_type=MarketType.SPOT,
    ...     chart_type=ChartType.KLINES,
    ...     symbol="BTCUSDT",
    ...     interval=Interval.MINUTE_1,
    ...     start_time=datetime(2023, 1, 1, tzinfo=timezone.utc),
    ...     end_time=datetime(2023, 1, 10, tzinfo=timezone.utc),
    ...     use_cache=True,
    ... )
"""

from datetime import datetime
from time import perf_counter
from typing import Literal, overload

import pandas as pd
import polars as pl

from ckvd.core.sync.crypto_kline_vision_data import DataSource, CryptoKlineVisionData
from ckvd.utils.for_core.ckvd_date_range_utils import calculate_date_range
from ckvd.utils.for_core.ckvd_fcp_utils import validate_interval
from ckvd.utils.loguru_setup import logger
from ckvd.utils.market_constraints import (
    ChartType,
    DataProvider,
    Interval,
    MarketType,
)


@overload
def fetch_market_data(
    provider: DataProvider,
    market_type: MarketType,
    chart_type: ChartType,
    symbol: str,
    interval: Interval,
    start_time: datetime | str | None = ...,
    end_time: datetime | str | None = ...,
    days: int = ...,
    use_cache: bool = ...,
    enforce_source: str = ...,
    max_retries: int = ...,
    return_polars: Literal[False] = ...,
) -> tuple[pd.DataFrame | None, float, int]: ...


@overload
def fetch_market_data(
    provider: DataProvider,
    market_type: MarketType,
    chart_type: ChartType,
    symbol: str,
    interval: Interval,
    start_time: datetime | str | None = ...,
    end_time: datetime | str | None = ...,
    days: int = ...,
    use_cache: bool = ...,
    enforce_source: str = ...,
    max_retries: int = ...,
    return_polars: Literal[True] = ...,
) -> tuple[pl.DataFrame | None, float, int]: ...


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
    return_polars: bool = False,
) -> tuple[pd.DataFrame | pl.DataFrame | None, float, int]:
    """Fetch market data using the Failover Control Protocol.

    This function retrieves market data from multiple sources using a progressive
    approach that prioritizes speed and reliability:
    1. First attempts to retrieve data from local cache (if use_cache=True)
    2. Then retrieves missing data from Vision API
    3. Finally falls back to REST API for any remaining data

    Args:
        provider: The data provider (e.g., BINANCE)
        market_type: Type of market (SPOT, FUTURES_USDT, FUTURES_COIN)
        chart_type: Type of chart data (KLINES, etc.)
        symbol: Trading pair symbol (e.g., 'BTCUSDT')
        interval: Time interval (e.g., MINUTE_1, HOUR_1)
        start_time: Start datetime (UTC) or ISO format string
        end_time: End datetime (UTC) or ISO format string
        days: Number of days to fetch (backward from end_time)
        use_cache: Whether to use the local cache
        enforce_source: Enforce specific data source ("AUTO", "CACHE", "VISION", "REST")
        max_retries: Maximum retry attempts for API calls
        return_polars: Whether to return a Polars DataFrame instead of Pandas.
                      When True, returns pl.DataFrame via zero-copy path.
                      When False (default), returns pd.DataFrame for backward compatibility.

    Returns:
        Tuple containing:
        - DataFrame with market data (pd.DataFrame or pl.DataFrame based on return_polars)
        - Elapsed time in seconds
        - Number of records retrieved

    Raises:
        ValueError: If time parameters are invalid or incompatible
        UnsupportedIntervalError: If interval is not supported for the market type
        RuntimeError: If data cannot be retrieved from any source
    """
    start_time_perf = perf_counter()

    # Validate interval (raises UnsupportedIntervalError, not sys.exit)
    validate_interval(market_type, interval)

    # Calculate time range (handles datetime | str | None via pendulum)
    start_datetime, end_datetime = calculate_date_range(start_time, end_time, days, interval)

    # Convert enforce_source string to enum
    enforce_source_enum = DataSource[enforce_source.upper()]

    # Create manager and fetch data — Polars pipeline flows through get_data()
    with CryptoKlineVisionData(
        provider=provider,
        market_type=market_type,
        chart_type=chart_type,
        use_cache=use_cache,
        retry_count=max_retries,
    ) as manager:
        df = manager.get_data(
            symbol=symbol,
            start_time=start_datetime,
            end_time=end_datetime,
            interval=interval,
            chart_type=chart_type,
            enforce_source=enforce_source_enum,
            return_polars=return_polars,
        )

    elapsed_time = perf_counter() - start_time_perf

    if df is None:
        records_count = 0
    elif return_polars:
        records_count = len(df)
    else:
        records_count = 0 if df.empty else len(df)

    logger.debug(f"fetch_market_data completed: {records_count} records in {elapsed_time:.2f}s")

    return df, elapsed_time, records_count
