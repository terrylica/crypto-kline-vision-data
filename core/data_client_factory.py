#!/usr/bin/env python
"""Factory for creating data clients based on provider, market type, and chart type."""

from typing import Optional, Dict, Type, Union, Any
from pathlib import Path

from utils.logger_setup import logger
from utils.market_constraints import (
    DataProvider,
    MarketType,
    ChartType,
    Interval,
)
from core.data_client_interface import DataClientInterface
from core.rest_data_client import RestDataClient
from core.binance_funding_rate_client import BinanceFundingRateClient


class DataClientFactory:
    """Factory for creating data clients based on provider, market type, and chart type."""

    # Dictionary mapping (provider, market_type, chart_type) to DataClientInterface implementations
    # This will be populated as new client implementations are registered
    _client_registry: Dict[tuple, Type[DataClientInterface]] = {}

    @classmethod
    def register_client(
        cls,
        provider: DataProvider,
        market_type: MarketType,
        chart_type: ChartType,
        client_class: Type[DataClientInterface],
    ) -> None:
        """Register a data client implementation for a specific provider, market type, and chart type.

        Args:
            provider: Data provider
            market_type: Market type
            chart_type: Chart type
            client_class: DataClientInterface implementation class
        """
        key = (provider.name, market_type.name, chart_type.name)
        cls._client_registry[key] = client_class
        logger.debug(f"Registered client {client_class.__name__} for {key}")

    @classmethod
    def create_data_client(
        cls,
        provider: DataProvider = DataProvider.BINANCE,
        market_type: MarketType = MarketType.SPOT,
        chart_type: ChartType = ChartType.KLINES,
        symbol: str = "BTCUSDT",
        interval: Union[str, Interval] = Interval.MINUTE_1,
        rest_client: Optional[Any] = None,
        vision_client: Optional[Any] = None,
        use_cache: bool = True,
        cache_dir: Optional[Path] = None,
        max_concurrent: int = 50,
        retry_count: int = 5,
        max_concurrent_downloads: Optional[int] = None,
        **kwargs,
    ) -> DataClientInterface:
        """Create a data client for the specified provider, market type, and chart type.

        Args:
            provider: Data provider
            market_type: Market type
            chart_type: Chart type
            symbol: Trading pair symbol
            interval: Time interval
            rest_client: Optional pre-configured REST client
            vision_client: Optional pre-configured Vision client
            use_cache: Whether to use cache
            cache_dir: Path to cache directory
            max_concurrent: Maximum concurrent requests
            retry_count: Number of retries
            max_concurrent_downloads: Maximum concurrent downloads
            **kwargs: Additional client-specific parameters

        Returns:
            Data client instance

        Raises:
            ValueError: If no client is registered for the specified provider, market type, and chart type
        """
        # Ensure interval is an Interval enum
        if isinstance(interval, str):
            interval = Interval(interval)

        # Special case for Binance provider with KLINES chart type
        # This is temporary until we refactor RestDataClient and VisionDataClient to implement DataClientInterface
        if provider == DataProvider.BINANCE and chart_type == ChartType.KLINES:
            # For Binance KLINES, we create a RestDataClient directly
            return RestDataClient(
                market_type=market_type,
                max_concurrent=max_concurrent,
                retry_count=retry_count,
                client=rest_client,
                **kwargs,
            )

        # Special case for Binance provider with FUNDING_RATE chart type
        if provider == DataProvider.BINANCE and chart_type == ChartType.FUNDING_RATE:
            # For Binance FUNDING_RATE, create BinanceFundingRateClient directly
            return BinanceFundingRateClient(
                symbol=symbol,
                interval=interval,
                market_type=market_type,
                use_cache=use_cache,
                cache_dir=cache_dir,
                max_concurrent=max_concurrent,
                retry_count=retry_count,
                max_concurrent_downloads=max_concurrent_downloads,
                **kwargs,
            )

        # For other combinations, look up the registered client class
        key = (provider.name, market_type.name, chart_type.name)
        if key not in cls._client_registry:
            raise ValueError(
                f"No client registered for provider={provider.name}, "
                f"market_type={market_type.name}, chart_type={chart_type.name}"
            )

        # Create and return the client instance
        client_class = cls._client_registry[key]
        return client_class(
            symbol=symbol,
            interval=interval,
            market_type=market_type,
            use_cache=use_cache,
            cache_dir=cache_dir,
            max_concurrent=max_concurrent,
            retry_count=retry_count,
            max_concurrent_downloads=max_concurrent_downloads,
            **kwargs,
        )
