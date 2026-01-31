#!/usr/bin/env python
"""Data Source Manager types and configuration.

This module contains the type definitions and configuration classes for the
DataSourceManager, extracted for better modularity.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from data_source_manager.py (1182 lines) for modularity
"""

from enum import Enum, auto
from pathlib import Path
from typing import TypeVar

import attr

from data_source_manager.utils.market_constraints import ChartType, DataProvider, MarketType

__all__ = [
    "DataSource",
    "DataSourceConfig",
]


class DataSource(Enum):
    """Enum for data source selection.

    This enum defines the available data sources for the Failover Control Protocol.
    It is used to control the source selection behavior.

    Attributes:
        AUTO: Automatically select the best source based on the FCP strategy
        REST: Force use of the REST API only
        VISION: Force use of the Vision API only
        CACHE: Force use of the local cache only
    """

    AUTO = auto()  # Automatically select best source
    REST = auto()  # Force REST API
    VISION = auto()  # Force Vision API
    CACHE = auto()  # Force local cache


T = TypeVar("T")


@attr.define(slots=True, frozen=True)
class DataSourceConfig:
    """Configuration for DataSourceManager.

    This immutable configuration class uses attrs to provide a strongly typed,
    validated configuration for the DataSourceManager with proper defaults.

    Attributes:
        market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN).
            Mandatory parameter that determines which market data to retrieve.
        provider: Data provider (BINANCE).
            Mandatory parameter that determines which data provider to use.
        chart_type: Chart type (KLINES, FUNDING_RATE).
            Default is KLINES (candlestick data).
        cache_dir: Directory to store cache files.
            Default is None, which uses the platform-specific cache directory.
        use_cache: Whether to use caching.
            Default is True. Set to False to always fetch fresh data.
        retry_count: Number of retries for failed requests.
            Default is 5. Increase for less stable networks.
        log_level: Logging level for DSM operations.
            Default is 'WARNING'. Can be 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.
        suppress_http_debug: Whether to suppress HTTP debug logging.
            Default is True. Set to False to see detailed HTTP request/response logs.
        quiet_mode: Whether to suppress all non-error logging.
            Default is False. Set to True for completely silent operation except for errors.

    Example:
        >>> from data_source_manager import DataProvider, MarketType, ChartType
        >>> from pathlib import Path
        >>>
        >>> # Basic configuration for SPOT market
        >>> config = DataSourceConfig(
        ...     market_type=MarketType.SPOT,
        ...     provider=DataProvider.BINANCE
        ... )
        >>>
        >>> # Configuration with custom logging settings
        >>> config = DataSourceConfig(
        ...     market_type=MarketType.FUTURES_USDT,
        ...     provider=DataProvider.BINANCE,
        ...     chart_type=ChartType.FUNDING_RATE,
        ...     cache_dir=Path("./custom_cache"),
        ...     retry_count=10,
        ...     log_level='DEBUG',
        ...     suppress_http_debug=False  # Show detailed HTTP debugging
        ... )
        >>>
        >>> # Configuration for quiet operation
        >>> config = DataSourceConfig(
        ...     market_type=MarketType.SPOT,
        ...     provider=DataProvider.BINANCE,
        ...     quiet_mode=True  # Only show errors
        ... )
    """

    # Mandatory parameters with validators
    market_type: MarketType = attr.field(validator=attr.validators.instance_of(MarketType))
    provider: DataProvider = attr.field(validator=attr.validators.instance_of(DataProvider))

    # Optional parameters with defaults and validators
    chart_type: ChartType = attr.field(default=ChartType.KLINES, validator=attr.validators.instance_of(ChartType))
    cache_dir: Path | None = attr.field(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of((str, Path))),
        converter=lambda p: Path(p) if p is not None and not isinstance(p, Path) else p,
    )
    use_cache: bool = attr.field(default=True, validator=attr.validators.instance_of(bool))
    retry_count: int = attr.field(default=5, validator=[attr.validators.instance_of(int), lambda _, __, value: value >= 0])

    # New logging control parameters
    log_level: str = attr.field(
        default="WARNING",
        validator=[attr.validators.instance_of(str), attr.validators.in_(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])],
        converter=str.upper,
    )
    suppress_http_debug: bool = attr.field(default=True, validator=attr.validators.instance_of(bool))
    quiet_mode: bool = attr.field(default=False, validator=attr.validators.instance_of(bool))

    @classmethod
    def create(cls: type[T], provider: DataProvider, market_type: MarketType, **kwargs) -> T:
        """Create a DataSourceConfig with the given provider, market_type and optional overrides.

        This is a convenience builder method that allows for a more fluent interface.

        Args:
            provider: Data provider (BINANCE)
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
            **kwargs: Optional parameter overrides

        Returns:
            Configured DataSourceConfig instance

        Raises:
            TypeError: If market_type is not a MarketType enum or provider is not a DataProvider enum
            ValueError: If any parameter values are invalid

        Example:
            >>> from data_source_manager import DataProvider, MarketType
            >>> from pathlib import Path
            >>>
            >>> # Basic configuration for SPOT market
            >>> config = DataSourceConfig(
            ...     market_type=MarketType.SPOT,
            ...     provider=DataProvider.BINANCE
            ... )
            >>>
            >>> # Configuration with custom settings
            >>> config = DataSourceConfig(
            ...     market_type=MarketType.FUTURES_USDT,
            ...     provider=DataProvider.BINANCE,
            ...     chart_type=ChartType.FUNDING_RATE,
            ...     cache_dir=Path("./custom_cache"),
            ...     retry_count=10
            ... )
        """
        return cls(market_type=market_type, provider=provider, **kwargs)
