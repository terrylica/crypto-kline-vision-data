#!/usr/bin/env python

"""Data Source Manager package for efficient market data retrieval.

This package provides tools for downloading and caching market data from Binance Vision.
The primary interface is the VisionDataClient, which uses Apache Arrow MMAP for
optimal performance in data storage and retrieval.

Key Features:
- Efficient data retrieval using Apache Arrow MMAP
- Automatic caching with zero-copy reads
- Timezone-aware timestamp handling
- Column-based data access
- Concurrent download management
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
