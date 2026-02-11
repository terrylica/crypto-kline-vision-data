"""Crypto Kline Vision Data - Professional market data integration.

This package provides a unified interface for retrieving and managing market data
from multiple sources including Binance Vision API, REST APIs, and local cache.

The main entry point is the CryptoKlineVisionData class which implements the
Failover Control Protocol (FCP) for reliable data retrieval with automatic
failover, retry logic, and data validation.

Key Features:
- **Failover Control Protocol**: Automatic cache → Vision API → REST API fallback
- **Professional Package Structure**: Clean src-layout with proper namespace
- **Type Safety**: Full type hints and validation
- **Rich Logging**: Beautiful, configurable logging with loguru
- **High Performance**: Apache Arrow caching and memory-mapped files

Quick Start:
    >>> from ckvd import CryptoKlineVisionData, DataProvider, MarketType, Interval
    >>> from datetime import datetime, timedelta
    >>>
    >>> # Create a manager for USDT-margined futures
    >>> manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
    >>>
    >>> # Fetch recent data with automatic failover (always use UTC)
    >>> from datetime import timezone
    >>> end_time = datetime.now(timezone.utc)
    >>> start_time = end_time - timedelta(days=7)
    >>> df = manager.get_data("BTCUSDT", start_time, end_time, Interval.HOUR_1)
    >>> print(f"Loaded {len(df)} bars of BTCUSDT data")

The FCP automatically handles:
1. Local cache lookup (fastest)
2. Vision API for historical data (efficient)
3. REST API fallback (real-time)

All with automatic retry, data validation, and gap detection.
"""

__version__ = "4.0.0"
__author__ = "EonLabs"
__email__ = "terry@eonlabs.com"

from typing import Any


# Lazy imports to avoid dependency issues during package discovery
def __getattr__(name: str) -> Any:
    """Lazy import for main package exports."""
    if name == "CryptoKlineVisionData":
        from .core.sync.crypto_kline_vision_data import CryptoKlineVisionData

        return CryptoKlineVisionData
    if name == "DataSource":
        from .core.sync.crypto_kline_vision_data import DataSource

        return DataSource
    if name == "CKVDConfig":
        from .core.sync.crypto_kline_vision_data import CKVDConfig

        return CKVDConfig
    if name == "DataProvider":
        from .utils.market_constraints import DataProvider

        return DataProvider
    if name == "MarketType":
        from .utils.market_constraints import MarketType

        return MarketType
    if name == "Interval":
        from .utils.market_constraints import Interval

        return Interval
    if name == "ChartType":
        from .utils.market_constraints import ChartType

        return ChartType
    if name == "fetch_market_data":
        from .core.sync.ckvd_lib import fetch_market_data

        return fetch_market_data
    if name == "__probe__":  # GitHub Issue #22
        import importlib

        return importlib.import_module("ckvd.__probe__")
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = [
    "CKVDConfig",
    "ChartType",
    "CryptoKlineVisionData",
    "DataProvider",
    "DataSource",
    "Interval",
    "MarketType",
    "fetch_market_data",
]
