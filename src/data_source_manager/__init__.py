"""Data Source Manager - Professional market data integration.

This package provides a unified interface for retrieving and managing market data
from multiple sources including Binance Vision API, REST APIs, and local cache.

The main entry point is the DataSourceManager class which implements the
Failover Control Protocol (FCP) for reliable data retrieval with automatic
failover, retry logic, and data validation.

Key Features:
- **Failover Control Protocol**: Automatic cache → Vision API → REST API fallback
- **Professional Package Structure**: Clean src-layout with proper namespace
- **Type Safety**: Full type hints and validation
- **Rich Logging**: Beautiful, configurable logging with loguru
- **High Performance**: Apache Arrow caching and memory-mapped files

Quick Start:
    >>> from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval
    >>> from datetime import datetime, timedelta
    >>> 
    >>> # Create a manager for USDT-margined futures
    >>> manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.FUTURES_USDT)
    >>> 
    >>> # Fetch recent data with automatic failover
    >>> end_time = datetime.now()
    >>> start_time = end_time - timedelta(days=7)
    >>> df = manager.get_data("BTCUSDT", start_time, end_time, Interval.HOUR_1)
    >>> print(f"Loaded {len(df)} bars of BTCUSDT data")

The FCP automatically handles:
1. Local cache lookup (fastest)
2. Vision API for historical data (efficient)  
3. REST API fallback (real-time)

All with automatic retry, data validation, and gap detection.
"""

__version__ = "0.2.0"
__author__ = "EonLabs"
__email__ = "chen@eonlabs.com"

# Lazy imports to avoid dependency issues during package discovery
def __getattr__(name):
    """Lazy import for main package exports."""
    if name == "DataSourceManager":
        from .core.sync.data_source_manager import DataSourceManager
        return DataSourceManager
    elif name == "DataProvider":
        from .utils.market_constraints import DataProvider
        return DataProvider
    elif name == "MarketType":
        from .utils.market_constraints import MarketType
        return MarketType
    elif name == "Interval":
        from .utils.market_constraints import Interval
        return Interval
    else:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

__all__ = [
    "DataSourceManager",
    "DataProvider", 
    "Interval",
    "MarketType",
]
