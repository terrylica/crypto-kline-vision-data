#!/usr/bin/env python
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Configuration management for DSM following industry best practices.

This module provides configuration-driven initialization patterns similar to
industry standards like SQLAlchemy, AWS SDK, and other production libraries.

Key Features:
- Explicit configuration over implicit defaults
- Environment variable support
- Validation and type safety with attrs
- Thread-safe initialization options
- Connection pooling configuration
"""

import os
import threading
from pathlib import Path
from typing import Any

import attr

from data_source_manager.utils.market_constraints import ChartType, DataProvider, MarketType


@attr.define(slots=True, frozen=True)
class DSMConfig:
    """Configuration for DataSourceManager following industry best practices.

    This configuration class follows the same pattern as:
    - SQLAlchemy's create_engine() config
    - AWS SDK's Config class
    - requests.Session() configuration

    All parameters are explicit and validated, preventing the import-time
    initialization issues that cause hanging.

    Examples:
        >>> # Basic configuration
        >>> config = DSMConfig(
        ...     provider=DataProvider.BINANCE,
        ...     market_type=MarketType.SPOT
        ... )
        >>>
        >>> # Configuration with connection pooling
        >>> config = DSMConfig(
        ...     provider=DataProvider.BINANCE,
        ...     market_type=MarketType.SPOT,
        ...     connection_pool_size=20,
        ...     connection_timeout=60,
        ...     max_retries=5
        ... )
        >>>
        >>> # Configuration from environment
        >>> config = DSMConfig.from_env()
        >>>
        >>> # Production configuration
        >>> config = DSMConfig.for_production(
        ...     provider=DataProvider.BINANCE,
        ...     market_type=MarketType.SPOT
        ... )
    """

    # ✅ MANDATORY: Core configuration (must be provided)
    provider: DataProvider = attr.field(validator=attr.validators.instance_of(DataProvider))
    market_type: MarketType = attr.field(validator=attr.validators.instance_of(MarketType))

    # ✅ OPTIONAL: Chart and caching configuration
    chart_type: ChartType = attr.field(
        default=ChartType.KLINES,
        validator=attr.validators.instance_of(ChartType)
    )
    cache_dir: Path | None = attr.field(
        default=None,
        validator=attr.validators.optional(attr.validators.instance_of((str, Path))),
        converter=lambda p: Path(p) if p is not None and not isinstance(p, Path) else p,
    )
    use_cache: bool = attr.field(
        default=True,
        validator=attr.validators.instance_of(bool)
    )

    # ✅ NETWORK: Connection and timeout configuration
    connection_timeout: int = attr.field(
        default=30,
        validator=[attr.validators.instance_of(int), lambda _, __, v: v > 0]
    )
    max_retries: int = attr.field(
        default=3,
        validator=[attr.validators.instance_of(int), lambda _, __, v: v >= 0]
    )

    # ✅ PERFORMANCE: Connection pooling and threading
    connection_pool_size: int = attr.field(
        default=10,
        validator=[attr.validators.instance_of(int), lambda _, __, v: v > 0]
    )
    thread_safe: bool = attr.field(
        default=True,
        validator=attr.validators.instance_of(bool)
    )

    # ✅ INITIALIZATION: Lazy loading control
    lazy_init: bool = attr.field(
        default=True,
        validator=attr.validators.instance_of(bool)
    )

    # ✅ LOGGING: Granular logging control
    log_level: str = attr.field(
        default="WARNING",
        validator=[
            attr.validators.instance_of(str),
            attr.validators.in_(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
        ],
        converter=str.upper
    )
    suppress_http_debug: bool = attr.field(
        default=True,
        validator=attr.validators.instance_of(bool)
    )
    quiet_mode: bool = attr.field(
        default=False,
        validator=attr.validators.instance_of(bool)
    )

    # ✅ RETRY: Configurable retry policy
    retry_backoff_factor: float = attr.field(
        default=1.0,
        validator=[attr.validators.instance_of((int, float)), lambda _, __, v: v > 0]
    )
    retry_backoff_max: float = attr.field(
        default=60.0,
        validator=[attr.validators.instance_of((int, float)), lambda _, __, v: v > 0]
    )

    @classmethod
    def create(
        cls,
        provider: DataProvider,
        market_type: MarketType,
        **kwargs: Any
    ) -> "DSMConfig":
        """Create a DSMConfig with the given provider, market_type and optional overrides.

        This factory method follows the same pattern as:
        - sqlalchemy.create_engine()
        - boto3.client() with config
        - requests.Session()

        Args:
            provider: Data provider (BINANCE)
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
            **kwargs: Optional parameter overrides

        Returns:
            Configured DSMConfig instance

        Example:
            >>> config = DSMConfig.create(
            ...     DataProvider.BINANCE,
            ...     MarketType.SPOT,
            ...     connection_timeout=60,
            ...     max_retries=5
            ... )
        """
        return cls(provider=provider, market_type=market_type, **kwargs)

    @classmethod
    def from_env(cls, **overrides: Any) -> "DSMConfig":
        """Create configuration from environment variables.

        Environment variables:
        - DSM_PROVIDER: Data provider (default: BINANCE)
        - DSM_MARKET_TYPE: Market type (default: SPOT)
        - DSM_CHART_TYPE: Chart type (default: KLINES)
        - DSM_CONNECTION_TIMEOUT: Connection timeout in seconds (default: 30)
        - DSM_MAX_RETRIES: Max retry attempts (default: 3)
        - DSM_CONNECTION_POOL_SIZE: Connection pool size (default: 10)
        - DSM_LOG_LEVEL: Log level (default: WARNING)
        - DSM_USE_CACHE: Whether to use cache (default: True)
        - DSM_CACHE_DIR: Cache directory path

        Args:
            **overrides: Override any environment variable values

        Returns:
            DSMConfig instance configured from environment

        Example:
            >>> # With environment variables set
            >>> os.environ['DSM_MAX_RETRIES'] = '5'
            >>> os.environ['DSM_LOG_LEVEL'] = 'DEBUG'
            >>> config = DSMConfig.from_env()
            >>>
            >>> # With overrides
            >>> config = DSMConfig.from_env(connection_timeout=60)
        """
        # Default values from environment
        provider_str = os.getenv("DSM_PROVIDER", "BINANCE")
        market_type_str = os.getenv("DSM_MARKET_TYPE", "SPOT")
        chart_type_str = os.getenv("DSM_CHART_TYPE", "KLINES")

        # Convert string values to enums
        try:
            provider = DataProvider[provider_str.upper()]
        except KeyError:
            provider = DataProvider.BINANCE

        try:
            market_type = MarketType[market_type_str.upper()]
        except KeyError:
            market_type = MarketType.SPOT

        try:
            chart_type = ChartType[chart_type_str.upper()]
        except KeyError:
            chart_type = ChartType.KLINES

        # Build configuration from environment
        config_dict = {
            "provider": provider,
            "market_type": market_type,
            "chart_type": chart_type,
            "connection_timeout": int(os.getenv("DSM_CONNECTION_TIMEOUT", "30")),
            "max_retries": int(os.getenv("DSM_MAX_RETRIES", "3")),
            "connection_pool_size": int(os.getenv("DSM_CONNECTION_POOL_SIZE", "10")),
            "log_level": os.getenv("DSM_LOG_LEVEL", "WARNING").upper(),
            "use_cache": os.getenv("DSM_USE_CACHE", "true").lower() == "true",
            "cache_dir": os.getenv("DSM_CACHE_DIR"),
        }

        # Apply overrides
        config_dict.update(overrides)

        # Remove None values
        config_dict = {k: v for k, v in config_dict.items() if v is not None}

        return cls(**config_dict)

    @classmethod
    def for_production(
        cls,
        provider: DataProvider,
        market_type: MarketType,
        **kwargs: Any
    ) -> "DSMConfig":
        """Create production-optimized configuration.

        This provides sensible defaults for production environments:
        - Higher connection timeout and retries
        - Larger connection pool
        - Error-level logging only
        - Thread safety enabled
        - Connection pooling optimized

        Args:
            provider: Data provider
            market_type: Market type
            **kwargs: Additional overrides

        Returns:
            Production-optimized DSMConfig

        Example:
            >>> config = DSMConfig.for_production(
            ...     DataProvider.BINANCE,
            ...     MarketType.SPOT
            ... )
        """
        production_defaults = {
            "connection_timeout": 60,
            "max_retries": 5,
            "connection_pool_size": 20,
            "log_level": "ERROR",
            "thread_safe": True,
            "lazy_init": True,
            "suppress_http_debug": True,
            "quiet_mode": False,  # Still want to see errors in production
        }

        # Merge with user overrides
        production_defaults.update(kwargs)

        return cls.create(provider, market_type, **production_defaults)

    @classmethod
    def for_development(
        cls,
        provider: DataProvider,
        market_type: MarketType,
        **kwargs: Any
    ) -> "DSMConfig":
        """Create development-optimized configuration.

        This provides sensible defaults for development environments:
        - Detailed logging
        - HTTP debug information
        - Shorter timeouts for faster feedback
        - Smaller connection pool

        Args:
            provider: Data provider
            market_type: Market type
            **kwargs: Additional overrides

        Returns:
            Development-optimized DSMConfig

        Example:
            >>> config = DSMConfig.for_development(
            ...     DataProvider.BINANCE,
            ...     MarketType.SPOT
            ... )
        """
        dev_defaults = {
            "connection_timeout": 15,
            "max_retries": 2,
            "connection_pool_size": 5,
            "log_level": "DEBUG",
            "thread_safe": True,
            "lazy_init": True,
            "suppress_http_debug": False,  # Show HTTP details in dev
            "quiet_mode": False,
        }

        # Merge with user overrides
        dev_defaults.update(kwargs)

        return cls.create(provider, market_type, **dev_defaults)

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary.

        Useful for logging, debugging, or serialization.

        Returns:
            Dictionary representation of the configuration
        """
        return attr.asdict(self)

    def with_overrides(self, **kwargs: Any) -> "DSMConfig":
        """Create a new configuration with specified overrides.

        This follows the immutable configuration pattern used by
        many production libraries.

        Args:
            **kwargs: Configuration parameters to override

        Returns:
            New DSMConfig instance with overrides applied

        Example:
            >>> base_config = DSMConfig.create(DataProvider.BINANCE, MarketType.SPOT)
            >>> debug_config = base_config.with_overrides(log_level='DEBUG')
        """
        return attr.evolve(self, **kwargs)


class DSMConnectionPool:
    """Thread-safe connection pool for DSM following industry patterns.

    This implements connection pooling similar to SQLAlchemy's QueuePool
    or requests Session management, preventing the resource exhaustion
    issues that can cause hanging.
    """

    def __init__(self, config: DSMConfig) -> None:
        """Initialize DSMConnectionPool with configuration.

        Args:
            config: DSM configuration object.
        """
        self.config = config
        self._pool = {}
        self._pool_lock = threading.Lock()
        self._initialized = False

    def get_connection(self, connection_key: str):
        """Get a connection from the pool (thread-safe)."""
        if not self.config.thread_safe:
            # Non-thread-safe mode - return new connection each time
            return self._create_connection()

        with self._pool_lock:
            if connection_key not in self._pool:
                self._pool[connection_key] = self._create_connection()
            return self._pool[connection_key]

    def _create_connection(self):
        """Create a new connection (implemented by subclasses)."""
        # This would be implemented by the actual connection managers
        pass

    def close_all(self):
        """Close all pooled connections."""
        with self._pool_lock:
            for connection in self._pool.values():
                if hasattr(connection, "close"):
                    connection.close()
            self._pool.clear()
