"""Data Source Manager - Professional market data integration.

This package provides a unified interface for retrieving and managing market data
from multiple sources including Binance Vision API, REST APIs, and local cache.

The main entry point is the DataSourceManager class which implements the
Failover Control Protocol (FCP) for reliable data retrieval.

Example:
    >>> from data_source_manager import DataSourceManager, DataProvider, MarketType, Interval
    >>> # Create a manager for spot market
    >>> manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
"""

__version__ = "0.1.73"
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
