"""Data provider implementations with factory pattern.

This module provides a provider factory pattern for creating provider-specific
clients. It addresses the "Silent Provider Failure" bug where passing unsupported
providers (OKX, TradeStation) would silently use Binance clients.

ADR: docs/adr/2025-01-30-failover-control-protocol.md

Usage:
    >>> from data_source_manager.core.providers import get_provider_clients
    >>> from data_source_manager import DataProvider, MarketType
    >>>
    >>> clients = get_provider_clients(DataProvider.BINANCE, MarketType.FUTURES_USDT)
    >>> print(clients.vision)  # Vision API client
    >>> print(clients.rest)    # REST API client
    >>> print(clients.cache)   # Cache manager
"""

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from data_source_manager.utils.market_constraints import DataProvider, MarketType

if TYPE_CHECKING:
    from datetime import datetime

    import pandas as pd
    import polars as pl

    from data_source_manager.utils.market_constraints import Interval


# =============================================================================
# Protocol Definitions
# =============================================================================


@runtime_checkable
class VisionClient(Protocol):
    """Protocol for Vision API clients.

    Vision clients provide bulk historical data access, typically from
    pre-generated files on cloud storage (e.g., AWS S3 for Binance Vision).
    """

    def fetch_data(
        self,
        symbol: str,
        start: "datetime",
        end: "datetime",
        interval: "Interval",
    ) -> "pl.DataFrame":
        """Fetch bulk historical data.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            start: Start time (UTC)
            end: End time (UTC)
            interval: Time interval enum

        Returns:
            Polars DataFrame with OHLCV data
        """
        ...

    def close(self) -> None:
        """Release resources."""
        ...


@runtime_checkable
class RestClient(Protocol):
    """Protocol for REST API clients.

    REST clients provide real-time data access via exchange REST APIs.
    They are typically rate-limited and used for recent data.
    """

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
    ) -> list[list]:
        """Fetch klines/candlestick data.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Interval string (e.g., "1h", "1m")
            start_ms: Start time in milliseconds since epoch
            end_ms: End time in milliseconds since epoch

        Returns:
            List of kline data arrays
        """
        ...

    def close(self) -> None:
        """Release resources."""
        ...


@runtime_checkable
class CacheManager(Protocol):
    """Protocol for cache managers.

    Cache managers handle local storage of market data in
    Apache Arrow format for fast retrieval.
    """

    def read(self, key: str) -> "pd.DataFrame | None":
        """Read cached data.

        Args:
            key: Cache key

        Returns:
            Cached DataFrame or None if not found
        """
        ...

    def write(self, key: str, data: "pd.DataFrame") -> None:
        """Write data to cache.

        Args:
            key: Cache key
            data: DataFrame to cache
        """
        ...


# =============================================================================
# Provider Clients Container
# =============================================================================


@dataclass
class ProviderClients:
    """Container for provider-specific clients.

    Attributes:
        vision: Vision API client (None for providers without bulk historical API)
        rest: REST API client
        cache: Cache manager
        provider: Data provider enum
        market_type: Market type enum
    """

    vision: VisionClient | None
    rest: RestClient
    cache: CacheManager
    provider: DataProvider
    market_type: MarketType


# =============================================================================
# Provider Registry
# =============================================================================

# Registry mapping providers to their factory functions
_PROVIDER_REGISTRY: dict[DataProvider, type] = {}


def register_provider(provider: DataProvider):
    """Decorator to register provider implementations.

    Usage:
        @register_provider(DataProvider.BINANCE)
        class BinanceProviderFactory:
            @classmethod
            def create_clients(cls, market_type: MarketType, **kwargs) -> ProviderClients:
                ...
    """

    def decorator(cls):
        _PROVIDER_REGISTRY[provider] = cls
        return cls

    return decorator


def get_provider_clients(
    provider: DataProvider,
    market_type: MarketType,
    cache_dir: Path | None = None,
    retry_count: int = 3,
    **kwargs,
) -> ProviderClients:
    """Factory function to get provider-specific clients.

    This function creates and returns the appropriate clients for the
    specified data provider and market type. It ensures that unsupported
    providers raise a clear error instead of silently falling back.

    Args:
        provider: Data provider enum (e.g., BINANCE)
        market_type: Market type enum (e.g., FUTURES_USDT)
        cache_dir: Optional cache directory path
        retry_count: Number of retries for network operations
        **kwargs: Additional provider-specific configuration

    Returns:
        ProviderClients container with vision, rest, and cache clients

    Raises:
        ValueError: If provider is not supported

    Example:
        >>> clients = get_provider_clients(DataProvider.BINANCE, MarketType.SPOT)
        >>> data = clients.rest.fetch_klines("BTCUSDT", "1h", start_ms, end_ms)
    """
    if provider not in _PROVIDER_REGISTRY:
        supported = sorted(p.name for p in _PROVIDER_REGISTRY)
        raise ValueError(
            f"Provider '{provider.name}' is not supported. "
            f"Supported providers: {supported}. "
            f"TradeStation support is planned but not yet implemented."
        )

    factory = _PROVIDER_REGISTRY[provider]
    return factory.create_clients(
        market_type=market_type,
        cache_dir=cache_dir,
        retry_count=retry_count,
        **kwargs,
    )


def get_supported_providers() -> frozenset[DataProvider]:
    """Get the set of supported providers.

    Returns:
        Frozenset of DataProvider enums that are currently supported
    """
    return frozenset(_PROVIDER_REGISTRY.keys())


# =============================================================================
# Binance Provider Factory
# =============================================================================


@register_provider(DataProvider.BINANCE)
class BinanceProviderFactory:
    """Factory for creating Binance-specific clients.

    This factory creates Vision API, REST API, and cache clients
    configured for Binance market data.
    """

    @classmethod
    def create_clients(
        cls,
        market_type: MarketType,
        cache_dir: Path | None = None,
        retry_count: int = 3,
        **kwargs,
    ) -> ProviderClients:
        """Create Binance clients for the specified market type.

        Args:
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
            cache_dir: Optional cache directory
            retry_count: Number of retries for REST calls
            **kwargs: Additional configuration

        Returns:
            ProviderClients with Binance implementations
        """
        # Import here to avoid circular imports and allow lazy loading
        from data_source_manager.core.providers.binance.cache_manager import (
            UnifiedCacheManager,
        )
        from data_source_manager.core.providers.binance.rest_data_client import (
            RestDataClient,
        )
        from data_source_manager.core.providers.binance.vision_data_client import (
            VisionDataClient,
        )
        from data_source_manager.utils.app_paths import get_cache_dir

        # Use default cache directory if not specified
        if cache_dir is None:
            cache_dir = get_cache_dir() / "data"

        # Create Vision client (Binance Vision API on AWS S3)
        # VisionDataClient is initialized with defaults; symbol/interval are passed per fetch() call
        vision_client = VisionDataClient(
            symbol="BTCUSDT",  # Default, overridden per fetch() call
            interval="1h",  # Default, overridden per fetch() call
            market_type=market_type,
            cache_dir=cache_dir,
        )

        # Create REST client
        rest_client = RestDataClient(
            market_type=market_type,
            retry_count=retry_count,
        )

        # Create cache manager
        cache_manager = UnifiedCacheManager(cache_dir=cache_dir)

        return ProviderClients(
            vision=vision_client,
            rest=rest_client,
            cache=cache_manager,
            provider=DataProvider.BINANCE,
            market_type=market_type,
        )


# =============================================================================
# OKX Provider Factory
# =============================================================================


@register_provider(DataProvider.OKX)
class OKXProviderFactory:
    """Factory for creating OKX-specific clients.

    OKX does not have a Vision API (bulk historical S3 storage).
    FCP for OKX uses: Cache → REST (history-candles) → REST (candles)

    Symbol formats:
    - SPOT: BTC-USDT (hyphenated)
    - SWAP/Futures: BTC-USD-SWAP
    """

    @classmethod
    def create_clients(
        cls,
        market_type: MarketType,
        cache_dir: Path | None = None,
        retry_count: int = 3,
        **kwargs,
    ) -> ProviderClients:
        """Create OKX clients for the specified market type.

        Args:
            market_type: Market type (SPOT, FUTURES_USDT)
            cache_dir: Optional cache directory
            retry_count: Number of retries for REST calls
            **kwargs: Additional configuration

        Returns:
            ProviderClients with OKX implementations (vision=None)
        """
        # Import here to avoid circular imports and allow lazy loading
        from data_source_manager.core.providers.binance.cache_manager import (
            UnifiedCacheManager,
        )
        from data_source_manager.core.providers.okx.okx_rest_client import (
            OKXRestClient,
        )
        from data_source_manager.utils.app_paths import get_cache_dir

        # Use default cache directory if not specified
        if cache_dir is None:
            cache_dir = get_cache_dir() / "data"

        # OKX has no Vision API - all historical data via REST
        vision_client = None

        # Create OKX REST client
        rest_client = OKXRestClient(
            market_type=market_type,
            retry_count=retry_count,
        )

        # Use shared cache manager (provider-agnostic cache paths)
        cache_manager = UnifiedCacheManager(cache_dir=cache_dir)

        return ProviderClients(
            vision=vision_client,
            rest=rest_client,
            cache=cache_manager,
            provider=DataProvider.OKX,
            market_type=market_type,
        )


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    "CacheManager",
    "OKXProviderFactory",
    "ProviderClients",
    "RestClient",
    "VisionClient",
    "get_provider_clients",
    "get_supported_providers",
    "register_provider",
]
