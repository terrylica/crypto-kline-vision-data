#!/usr/bin/env python

"""Data Source Manager package for efficient market data retrieval.

This package provides tools for downloading and caching market data from Binance Vision.
The primary interface is the fetch_market_data function, which implements the
Failover Control Protocol for robust data retrieval from multiple sources.

The Failover Control Protocol (FCP) mechanism consists of three integrated phases:
1. Local Cache Retrieval: Quickly obtain data from local Apache Arrow files
2. Vision API Retrieval: Supplement missing data segments from Vision API
3. REST API Fallback: Ensure complete data coverage for any remaining segments

Key Features:
- Efficient data retrieval using Apache Arrow MMAP
- Automatic caching with zero-copy reads
- Progressive data retrieval from multiple sources
- Timezone-aware timestamp handling
- Column-based data access

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
    >>> print(f"Retrieved {records_count} records in {elapsed_time:.2f} seconds")
"""

from core.providers.binance.vision_data_client import VisionDataClient
from core.sync.data_source_manager import DataSource, DataSourceConfig
from core.sync.dsm_lib import fetch_market_data
from utils.dataframe_types import TimestampedDataFrame
from utils.market_constraints import ChartType, DataProvider, Interval, MarketType

__all__ = [
    "ChartType",
    "DataProvider",
    "DataSource",
    "DataSourceConfig",
    "Interval",
    "MarketType",
    "TimestampedDataFrame",
    "VisionDataClient",
    "fetch_market_data",
]
