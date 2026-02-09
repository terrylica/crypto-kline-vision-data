#!/usr/bin/env python
# polars-exception: Core FCP implementation - CryptoKlineVisionData returns pandas DataFrames
# for compatibility with all downstream consumers. Coordinated migration needed.
"""Crypto Kline Vision Data (CKVD) that mediates between different data sources.

This module implements the core Failover Control Protocol (FCP) strategy for robust
data retrieval from multiple sources. It orchestrates the data retrieval process
through a sequence of increasingly reliable sources:

1. Local Cache: Quick retrieval from local Apache Arrow files
2. Vision API: Fetching from Binance Vision API for historical data
3. REST API: Direct API calls for recent or missing data

The main class is CryptoKlineVisionData, which is the core implementation of the FCP strategy.
DataSource and CKVDConfig are imported from ckvd_types for backward compatibility.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract DataSource and CKVDConfig to ckvd_types.py

Example:
    >>> from core.sync.crypto_kline_vision_data import CryptoKlineVisionData, DataSource
    >>> from ckvd import DataProvider, MarketType, Interval
    >>> from datetime import datetime
    >>>
    >>> # Create a manager for spot market
    >>> manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
    >>>
    >>> # Fetch BTCUSDT data for the last 3 days
    >>> df = manager.get_data(
    ...     symbol="BTCUSDT",
    ...     start_time=datetime(2023, 1, 1),
    ...     end_time=datetime(2023, 1, 5),
    ...     interval=Interval.MINUTE_1,
    ... )
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from ckvd.core.providers import ProviderClients, get_provider_clients, get_supported_providers
from ckvd.core.providers.binance.binance_funding_rate_client import BinanceFundingRateClient
from ckvd.core.sync.ckvd_types import DataSource, CKVDConfig
from ckvd.utils.app_paths import get_cache_dir
from ckvd.utils.config import (
    FUNDING_RATE_DTYPES,
    OUTPUT_DTYPES,
    REST_CHUNK_SIZE,
    REST_MAX_CHUNKS,
    VISION_DATA_DELAY_HOURS,
    create_empty_dataframe,
)
from ckvd.utils.for_core.ckvd_api_utils import (
    fetch_from_rest,
    fetch_from_vision,
)
from ckvd.utils.for_core.ckvd_date_range_utils import (
    calculate_date_range,
    get_date_range_description,
)
from ckvd.utils.config import FeatureFlags
from ckvd.utils.for_core.ckvd_fcp_utils import (
    handle_error,
    process_rest_step,
    process_vision_step,
    validate_interval,
    verify_final_data,
)
from ckvd.utils.internal.polars_pipeline import PolarsDataPipeline
from ckvd.utils.for_core.ckvd_time_range_utils import (
    standardize_columns,
)
from ckvd.utils.for_core.rest_exceptions import RateLimitError, RestAPIError
from ckvd.utils.for_core.vision_exceptions import VisionAPIError
from ckvd.utils.loguru_setup import logger
from ckvd.utils.market_constraints import ChartType, DataProvider, Interval, MarketType
from ckvd.utils.time_utils import align_time_boundaries

# Re-export for backward compatibility
__all__ = [
    "CKVDConfig",
    "CryptoKlineVisionData",
    "DataSource",
]

# Supported providers - dynamically determined from the provider registry
# CRITICAL: This prevents silent failures where unsupported providers would silently fail
# The factory pattern in core.providers registers supported providers
SUPPORTED_PROVIDERS: frozenset[DataProvider] = get_supported_providers()


class CryptoKlineVisionData:
    """Mediator between data sources with smart selection and caching.

    This class orchestrates data retrieval from different sources following
    the Failover Control Protocol (FCP) strategy:

    1. Cache (Local Arrow files): Check cached data first
    2. VISION API: For missing data, try Binance Vision API
    3. REST API: If Vision fails, use REST API with chunking

    It ensures consistent data format regardless of the source, handles retries,
    and properly merges data segments from different sources when needed.

    Attributes:
        provider (DataProvider): The data provider (e.g., BINANCE)
        market_type (MarketType): Type of market (SPOT, UM, CM)
        chart_type (ChartType): Type of chart data (KLINES, etc.)
        use_cache (bool): Whether to use the local cache
        retry_count (int): Number of retry attempts for API calls
        cache_dir (Path): Directory to store cache files
        VISION_DATA_DELAY_HOURS (int): Hours of delay for Vision data availability
        REST_CHUNK_SIZE (int): Size of chunks for REST API requests
        REST_MAX_CHUNKS (int): Maximum number of chunks to request from REST API

    Examples:
        >>> from core.sync.crypto_kline_vision_data import CryptoKlineVisionData
        >>> from ckvd import DataProvider, MarketType, Interval, ChartType
        >>> from datetime import datetime
        >>>
        >>> # Basic usage
        >>> manager = CryptoKlineVisionData(
        ...     provider=DataProvider.BINANCE,
        ...     market_type=MarketType.SPOT,
        ...     chart_type=ChartType.KLINES
        ... )
        >>>
        >>> # Fetch data for a specific time range
        >>> df = manager.get_data(
        ...     symbol="BTCUSDT",
        ...     start_time=datetime(2023, 1, 1),
        ...     end_time=datetime(2023, 1, 10),
        ...     interval=Interval.MINUTE_1
        ... )
        >>>
        >>> # Using the context manager pattern for automatic resource cleanup
        >>> with CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT) as manager:
        ...     df = manager.get_data(
        ...         symbol="ETHUSDT",
        ...         start_time=datetime(2023, 1, 1),
        ...         end_time=datetime(2023, 1, 5),
        ...         interval=Interval.HOUR_1
        ...     )
    """

    # Vision API constraints - using imported constant
    VISION_DATA_DELAY_HOURS = VISION_DATA_DELAY_HOURS

    # REST API constraints - using imported constants
    REST_CHUNK_SIZE = REST_CHUNK_SIZE
    REST_MAX_CHUNKS = REST_MAX_CHUNKS

    # Output format specification from centralized config
    OUTPUT_DTYPES = OUTPUT_DTYPES.copy()
    FUNDING_RATE_DTYPES = FUNDING_RATE_DTYPES.copy()

    # Default market type that can be configured globally
    DEFAULT_MARKET_TYPE = MarketType.SPOT

    @classmethod
    def calculate_time_range(cls, start_time=None, end_time=None, days=3, interval=Interval.MINUTE_1) -> tuple[datetime, datetime]:
        """Calculate time range with flexible parameters.

        This utility method provides a flexible way to define time ranges for data
        retrieval. It supports various combinations of inputs:

        1. End time with days (backward calculation): Specify end_time and days to
           calculate start_time as end_time - days
        2. Start time with days (forward calculation): Specify start_time and days to
           calculate end_time as start_time + days
        3. Explicit start and end times: Specify both start_time and end_time directly
        4. Days-only calculation (backward from current time): Specify only days to
           get end_time = now and start_time = now - days

        Args:
            start_time: Start time as string (ISO format) or datetime object, or None
            end_time: End time as string (ISO format) or datetime object, or None
            days: Number of days for the range when only one time bound is provided
            interval: Time interval for data, used to align boundaries to interval points

        Returns:
            tuple: (start_datetime, end_datetime) as properly aligned datetime objects

        Raises:
            ValueError: If both start_time and end_time are provided and start_time is after end_time

        Examples:
            >>> # Days only - backwards from now
            >>> start, end = CryptoKlineVisionData.calculate_time_range(days=5)
            >>> print(f"Duration: {(end - start).days} days")
            Duration: 5 days

            >>> # End time with days - backwards from specified date
            >>> from datetime import datetime
            >>> end = datetime(2023, 1, 10)
            >>> start, _ = CryptoKlineVisionData.calculate_time_range(end_time=end, days=7)
            >>> print(start.date())
            2023-01-03

            >>> # Start time with days - forwards from specified date
            >>> start = datetime(2023, 1, 1)
            >>> _, end = CryptoKlineVisionData.calculate_time_range(start_time=start, days=3)
            >>> print(end.date())
            2023-01-04
        """
        # Use the core utility to calculate date range
        start_datetime, end_datetime = calculate_date_range(start_time=start_time, end_time=end_time, days=days, interval=interval)

        # Get description for logging
        description = get_date_range_description(
            start_datetime,
            end_datetime,
            {"start_time": start_time, "end_time": end_time, "days": days},
        )
        logger.info(description)

        return start_datetime, end_datetime

    @classmethod
    def create(
        cls,
        provider: DataProvider | None = None,
        market_type: MarketType | None = None,
        **kwargs: Any,
    ) -> "CryptoKlineVisionData":
        """Create a CryptoKlineVisionData with a more Pythonic interface.

        This factory method provides a cleaner way to instantiate the CryptoKlineVisionData
        with proper default values and parameter validation.

        Args:
            provider: Data provider (e.g., BINANCE)
                If None, raises ValueError as provider is now mandatory
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
                If None, uses the class's DEFAULT_MARKET_TYPE
            **kwargs: Additional parameters passed to the constructor:
                - chart_type: Type of chart data (default: KLINES)
                - cache_dir: Directory to store cache files (default: platform-specific cache dir)
                - use_cache: Whether to use caching (default: True)
                - retry_count: Number of retries for failed requests (default: 3)
                - log_level: Logging level for CKVD operations (default: 'WARNING')
                - suppress_http_debug: Whether to suppress HTTP debug logging (default: True)
                - quiet_mode: Whether to suppress all non-error logging (default: False)

        Returns:
            CryptoKlineVisionData: Initialized CryptoKlineVisionData instance

        Raises:
            ValueError: If provider is None

        Examples:
            >>> # Basic creation with required parameters (clean output by default)
            >>> from core.sync.crypto_kline_vision_data import CryptoKlineVisionData
            >>> from ckvd import DataProvider, MarketType
            >>>
            >>> manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
            >>>
            >>> # Creation with debug logging for troubleshooting
            >>> manager = CryptoKlineVisionData.create(
            ...     DataProvider.BINANCE,
            ...     MarketType.SPOT,
            ...     log_level='DEBUG',
            ...     suppress_http_debug=False  # Show detailed HTTP logs
            ... )
            >>>
            >>> # Creation for feature engineering (completely quiet)
            >>> manager = CryptoKlineVisionData.create(
            ...     DataProvider.BINANCE,
            ...     MarketType.SPOT,
            ...     quiet_mode=True  # Only show errors
            ... )
            >>>
            >>> # Creation with additional parameters
            >>> from ckvd.utils.market_constraints import ChartType
            >>> from pathlib import Path
            >>>
            >>> manager = CryptoKlineVisionData.create(
            ...     DataProvider.BINANCE,
            ...     MarketType.FUTURES_USDT,
            ...     chart_type=ChartType.FUNDING_RATE,
            ...     cache_dir=Path("./custom_cache"),
            ...     retry_count=5,
            ...     log_level='INFO'
            ... )
        """
        # Provider is now mandatory
        if provider is None:
            raise ValueError("Data provider must be specified")

        # CRITICAL: Validate provider is supported to prevent silent failures
        # Previously, passing OKX or TradeStation would silently use Binance clients
        if provider not in SUPPORTED_PROVIDERS:
            supported_names = sorted(p.name for p in SUPPORTED_PROVIDERS)
            raise ValueError(
                f"Provider '{provider.name}' is not supported. "
                f"Supported providers: {supported_names}. "
                f"OKX and TradeStation support is planned but not yet implemented."
            )

        # Use the configured default market type if none provided
        if market_type is None:
            market_type = cls.DEFAULT_MARKET_TYPE
            logger.debug(f"Using default market type: {market_type.name}")

        config = CKVDConfig.create(provider, market_type, **kwargs)
        return cls(
            provider=config.provider,
            market_type=config.market_type,
            chart_type=config.chart_type,
            cache_dir=config.cache_dir,
            use_cache=config.use_cache,
            retry_count=config.retry_count,
            log_level=config.log_level,
            suppress_http_debug=config.suppress_http_debug,
            quiet_mode=config.quiet_mode,
        )

    def __init__(
        self,
        provider: DataProvider = DataProvider.BINANCE,
        market_type: MarketType = MarketType.SPOT,
        chart_type: ChartType = ChartType.KLINES,
        use_cache: bool = True,
        cache_dir: Path | None = None,
        retry_count: int = 3,
        log_level: str = "WARNING",
        suppress_http_debug: bool = True,
        quiet_mode: bool = False,
    ) -> None:
        """Initialize CryptoKlineVisionData.

        Args:
            provider: Data provider (BINANCE)
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
            chart_type: Chart type (KLINES, FUNDING_RATE)
            use_cache: Whether to use local cache
            cache_dir: Directory to store cache files (default: platform-specific cache dir)
            retry_count: Number of retries for network operations
            log_level: Logging level for CKVD operations ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
            suppress_http_debug: Whether to suppress HTTP debug logging (default: True)
            quiet_mode: Whether to suppress all non-error logging (default: False)
        """
        self.provider = provider
        self.market_type = market_type
        self.chart_type = chart_type
        self.use_cache = use_cache
        # Environment variable override: CKVD_ENABLE_CACHE=false disables cache
        if self.use_cache and os.environ.get("CKVD_ENABLE_CACHE", "").lower() in ("false", "0", "no"):
            self.use_cache = False
            logger.info("[CKVD] Cache disabled via CKVD_ENABLE_CACHE environment variable")
        self.retry_count = retry_count

        # Store logging configuration
        self.log_level = log_level.upper()
        self.suppress_http_debug = suppress_http_debug
        self.quiet_mode = quiet_mode

        # Configure logging based on user preferences
        self._configure_logging()

        # Set up cache directory
        if cache_dir is not None:
            self.cache_dir = Path(cache_dir)
        else:
            # Use platform-specific cache directory from app_paths
            self.cache_dir = get_cache_dir() / "data"

        # Initialize provider clients using the factory pattern
        # This addresses the "Silent Provider Failure" bug where OKX/TradeStation
        # would silently use Binance clients. Now the factory ensures correct
        # provider-specific clients are created.
        try:
            self._provider_clients: ProviderClients = get_provider_clients(
                provider=self.provider,
                market_type=self.market_type,
                cache_dir=self.cache_dir,
                retry_count=self.retry_count,
            )
            logger.info(f"Initialized provider clients for {self.provider.name}")
        except ValueError as e:
            # Re-raise with context - this should not happen if create() validated
            raise ValueError(f"Failed to initialize provider clients: {e}") from e

        # Extract clients for backward compatibility with existing code paths
        self.cache_manager = self._provider_clients.cache  # Cache manager
        self.rest_client = self._provider_clients.rest  # REST API client
        self.vision_client = self._provider_clients.vision  # Vision API client (None for OKX)

        # Log cache status
        if self.use_cache and self.cache_manager is not None:
            logger.debug("Cache manager initialized via factory pattern")

    def _configure_logging(self) -> None:
        """Configure logging levels based on user preferences.

        This method implements the logging behavior recommendations:
        1. Suppress HTTP debug logging by default
        2. Allow users to control CKVD log levels
        3. Provide quiet mode for feature engineering workflows
        """
        # Configure CKVD's own logging level
        # In quiet mode, only show errors and critical messages
        effective_level = "ERROR" if self.quiet_mode else self.log_level

        # Configure the main CKVD logger
        logger.configure_level(effective_level)

        # Configure HTTP library logging via shared SSoT
        from ckvd.utils.loguru_setup import suppress_http_logging

        suppress_http_logging(suppress=self.suppress_http_debug)

        # Log the configuration for debugging
        if not self.quiet_mode:
            logger.debug(f"CKVD logging configured: level={effective_level}, suppress_http_debug={self.suppress_http_debug}")

    def reconfigure_logging(
        self, log_level: str | None = None, suppress_http_debug: bool | None = None, quiet_mode: bool | None = None
    ) -> None:
        """Reconfigure logging settings after initialization.

        This method allows users to change logging behavior dynamically,
        which is useful for debugging or changing verbosity during runtime.

        Args:
            log_level: New logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
            suppress_http_debug: Whether to suppress HTTP debug logging
            quiet_mode: Whether to enable quiet mode

        Example:
            >>> # Start with default settings (quiet for feature engineering)
            >>> ckvd = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
            >>>
            >>> # Enable debug mode for troubleshooting
            >>> ckvd.reconfigure_logging(log_level='DEBUG', suppress_http_debug=False)
            >>>
            >>> # Return to quiet mode
            >>> ckvd.reconfigure_logging(quiet_mode=True)
        """
        # Update configuration if new values provided
        if log_level is not None:
            self.log_level = log_level.upper()
        if suppress_http_debug is not None:
            self.suppress_http_debug = suppress_http_debug
        if quiet_mode is not None:
            self.quiet_mode = quiet_mode

        # Re-apply logging configuration
        self._configure_logging()

    def _get_market_type_str(self) -> MarketType:
        """Get the market type enum.

        Returns:
            MarketType enum
        """
        return self.market_type

    def _get_from_cache(
        self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval
    ) -> tuple[pd.DataFrame, list[tuple[datetime, datetime]]]:
        """Get data from cache and identify missing time ranges.

        This method is part of the FCP's first phase - checking local cache.
        It searches for cached data files that match the requested parameters
        and identifies any missing time segments that need to be fetched from
        other sources.

        Args:
            symbol: Symbol to retrieve data for (e.g., "BTCUSDT")
            start_time: Start time for data retrieval (UTC)
            end_time: End time for data retrieval (UTC)
            interval: Time interval between data points (e.g., MINUTE_1)

        Returns:
            Tuple containing:
            - pd.DataFrame: DataFrame with cached data (may be empty)
            - list: List of time ranges (start, end) tuples that are missing from cache

        Note:
            If caching is disabled or the cache directory doesn't exist,
            this returns an empty DataFrame and the entire requested time range
            as missing.
        """
        from ckvd.utils.for_core.ckvd_cache_utils import get_from_cache

        if not self.use_cache or self.cache_dir is None:
            # Return empty DataFrame and the entire date range as missing
            return create_empty_dataframe(), [(start_time, end_time)]

        logger.info(f"Checking cache for {symbol} from {start_time} to {end_time}")

        # Use cache utils for Arrow file operations
        return get_from_cache(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            cache_dir=self.cache_dir,
            market_type=self.market_type,
            chart_type=self.chart_type,
            provider=self.provider,
        )

    def _save_to_cache(
        self,
        df: pd.DataFrame,
        symbol: str,
        interval: Interval,
        source: str | None = None,
    ) -> None:
        """Save market data to the local cache.

        This method stores retrieved data in the local cache for future use,
        improving performance for subsequent requests covering the same time period.
        Data is saved as Apache Arrow files organized by provider, market type,
        symbol, and interval.

        Args:
            df: DataFrame to cache
            symbol: Symbol the data is for (e.g., "BTCUSDT")
            interval: Time interval of the data (e.g., MINUTE_1)
            source: Data source identifier (e.g., "VISION", "REST")

        Note:
            - If caching is disabled or the cache directory doesn't exist, this is a no-op
            - Empty DataFrames are not cached
            - The source parameter is tracked for telemetry but doesn't affect caching behavior
        """
        from ckvd.utils.for_core.ckvd_cache_utils import save_to_cache

        if not self.use_cache or self.cache_dir is None:
            return

        if df.empty:
            logger.warning(f"Empty DataFrame for {symbol} - skipping cache save")
            return

        logger.info(f"Saving {len(df)} records for {symbol} to cache")

        # Track the source of the data for future source-specific optimizations
        # Currently not used in the underlying implementation but preserved for telemetry
        if source:
            logger.debug(f"Data source for cache: {source}")

        # Use cache utils for Arrow file operations
        save_to_cache(
            df=df,
            symbol=symbol,
            interval=interval,
            market_type=self.market_type,
            cache_dir=self.cache_dir,
            chart_type=self.chart_type,
            provider=self.provider,
        )

    def _fetch_from_vision(self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval) -> pd.DataFrame:
        """Fetch data from the Binance Vision API.

        This method is part of the FCP's second phase - retrieving data from
        Binance Vision API. It handles client creation/reuse and delegates to
        the specialized vision fetching utility.

        The Vision API provides highly efficient access to historical data through
        pre-generated files hosted on AWS S3, avoiding REST API rate limits.

        Args:
            symbol: Symbol to retrieve data for (e.g., "BTCUSDT")
            start_time: Start time for data retrieval (UTC)
            end_time: End time for data retrieval (UTC)
            interval: Time interval between data points (e.g., MINUTE_1)

        Returns:
            pd.DataFrame: DataFrame with data from Vision API (may be empty if no data available)

        Note:
            Vision API typically doesn't have data for the most recent time periods
            (defined by VISION_DATA_DELAY_HOURS, usually 48 hours). For recent data,
            the FCP will fall back to the REST API.
        """
        # Vision client is initialized via factory pattern in __init__
        # For providers without Vision API (e.g., OKX), vision_client will be None
        if self.vision_client is None:
            logger.debug(f"Provider {self.provider.name} does not have Vision API, returning empty DataFrame")
            return create_empty_dataframe()

        # Call the extracted utility function
        return fetch_from_vision(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            vision_client=self.vision_client,
            chart_type=self.chart_type,
            use_cache=self.use_cache,
            save_to_cache_func=self._save_to_cache if self.use_cache else None,
        )

    def _fetch_from_rest(self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval) -> pd.DataFrame:
        """Fetch data from the Binance REST API.

        This method is part of the FCP's third phase - retrieving data directly
        from the Binance REST API. It's used as a fallback when data is not available
        in the cache or Vision API, especially for recent data.

        The implementation handles client creation/reuse and delegates to the
        specialized REST fetching utility, which manages chunking and rate limits.

        Args:
            symbol: Symbol to retrieve data for (e.g., "BTCUSDT")
            start_time: Start time for data retrieval (UTC)
            end_time: End time for data retrieval (UTC)
            interval: Time interval between data points (e.g., MINUTE_1)

        Returns:
            pd.DataFrame: DataFrame with data from REST API

        Note:
            REST API requests are subject to rate limits and are chunked into
            smaller requests (defined by REST_CHUNK_SIZE and REST_MAX_CHUNKS)
            to avoid timeouts and improve reliability.
        """
        # REST client is initialized via factory pattern in __init__
        # Call the extracted utility function
        return fetch_from_rest(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            rest_client=self.rest_client,
            chart_type=self.chart_type,
        )

    def _fetch_funding_rate(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval = Interval.HOUR_8,
        return_polars: bool = False,
    ) -> pd.DataFrame | pl.DataFrame:
        """Fetch funding rate data using the BinanceFundingRateClient.

        This method handles funding rate data retrieval separately from the main
        FCP flow since funding rate data has different structure and sources.

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            start_time: Start time for data retrieval (UTC)
            end_time: End time for data retrieval (UTC)
            interval: Funding rate interval (default: HOUR_8, standard for Binance)
            return_polars: Whether to return a Polars DataFrame

        Returns:
            DataFrame with funding rate data containing columns:
            - symbol: Trading pair symbol
            - funding_time: Timestamp of the funding rate
            - funding_rate: The funding rate value
            - interval: The interval string

        Raises:
            ValueError: If market type doesn't support funding rates
        """
        # Validate market type - funding rates only available for futures
        if self.market_type not in (MarketType.FUTURES_USDT, MarketType.FUTURES_COIN):
            raise ValueError(
                f"Funding rate data is only available for futures markets. "
                f"Current market type: {self.market_type.name}. "
                f"Use FUTURES_USDT or FUTURES_COIN instead."
            )

        logger.info(f"[FCP] Fetching funding rate for {symbol} from {start_time} to {end_time}")

        # Create funding rate client
        funding_client = BinanceFundingRateClient(
            symbol=symbol,
            interval=interval,
            market_type=self.market_type,
            use_cache=self.use_cache,
            cache_dir=self.cache_dir,
            retry_count=self.retry_count,
        )

        try:
            # Fetch funding rate data
            result_df = funding_client.fetch(
                symbol=symbol,
                interval=interval.value,
                start_time=start_time,
                end_time=end_time,
            )

            logger.info(f"[FCP] Retrieved {len(result_df)} funding rate records for {symbol}")

            # Convert to Polars if requested
            if return_polars and not result_df.empty:
                result_pl = pl.from_pandas(result_df)
                logger.debug(f"[FCP] Converted funding rate to Polars DataFrame with {len(result_pl)} rows")
                return result_pl

            return result_df

        finally:
            # Ensure client is closed
            funding_client.close()

    @overload
    def get_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval = ...,
        chart_type: ChartType | None = ...,
        include_source_info: bool = ...,
        enforce_source: DataSource = ...,
        auto_reindex: bool = ...,
        return_polars: Literal[False] = ...,
    ) -> pd.DataFrame: ...

    @overload
    def get_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval = ...,
        chart_type: ChartType | None = ...,
        include_source_info: bool = ...,
        enforce_source: DataSource = ...,
        auto_reindex: bool = ...,
        return_polars: Literal[True] = ...,
    ) -> pl.DataFrame: ...

    def get_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval = Interval.MINUTE_1,
        chart_type: ChartType | None = None,
        include_source_info: bool = True,
        enforce_source: DataSource = DataSource.AUTO,
        auto_reindex: bool = True,
        return_polars: bool = False,
    ) -> pd.DataFrame | pl.DataFrame:
        """Retrieve market data for a symbol within a specified time range.

        This method implements the Failover Control Protocol (FCP) to ensure robust
        data retrieval from multiple sources:

        1. **Cache (Local Arrow files)**: Check cached data first for fast access
        2. **Vision API**: For missing data, try Binance Vision API (historical data)
        3. **REST API**: If Vision fails or is unavailable, use REST API as fallback

        The method automatically handles:
        - Time boundary alignment based on the specified interval
        - Progressive merging of data from different sources
        - Retry logic with exponential backoff for network failures
        - Consistent data format standardization across all sources
        - Optional reindexing to create complete time series

        Args:
            symbol: Trading symbol (e.g., "BTCUSDT", "ETHUSDT")
            start_time: Start time for data retrieval (timezone-aware datetime)
            end_time: End time for data retrieval (timezone-aware datetime)
            interval: Time interval for data points (default: 1 minute)
            chart_type: Type of chart data to retrieve (default: uses instance setting)
            include_source_info: Whether to include data source information in results
            enforce_source: Force use of specific data source (default: AUTO for FCP)
            auto_reindex: Whether to automatically reindex to create complete time series.
                         When True (default), missing timestamps are filled with NaN.
                         When False, only returns available data without artificial padding.
            return_polars: Whether to return a Polars DataFrame instead of Pandas.
                         When True, returns pl.DataFrame for better performance.
                         When False (default), returns pd.DataFrame for backward compatibility.

        Returns:
            pd.DataFrame or pl.DataFrame (based on return_polars parameter) containing
            the requested market data with columns:
            - open_time: Opening time of the interval
            - open, high, low, close: OHLC price data
            - volume: Trading volume
            - quote_asset_volume: Quote asset volume
            - count: Number of trades
            - taker_buy_volume: Taker buy volume
            - taker_buy_quote_volume: Taker buy quote volume
            - _data_source: Source of each record (if include_source_info=True)

        Raises:
            ValueError: If start_time >= end_time or invalid parameters
            RuntimeError: If all data sources fail and no data can be retrieved

        Examples:
            >>> # Basic usage with automatic source selection
            >>> df = manager.get_data("BTCUSDT", start_time, end_time, Interval.MINUTE_1)
            >>>
            >>> # Force use of REST API only
            >>> df = manager.get_data(
            ...     "BTCUSDT", start_time, end_time, Interval.MINUTE_1,
            ...     enforce_source=DataSource.REST
            ... )
            >>>
            >>> # Get only available data without NaN padding
            >>> df = manager.get_data(
            ...     "BTCUSDT", start_time, end_time, Interval.MINUTE_1,
            ...     auto_reindex=False
            ... )
            >>>
            >>> # Access data sources used
            >>> if "_data_source" in df.columns:
            ...     sources = df["_data_source"].unique()
            ...     print(f"Data sources used: {sources}")
            >>>
            >>> # Return Polars DataFrame for better performance
            >>> df_polars = manager.get_data(
            ...     "BTCUSDT", start_time, end_time, Interval.MINUTE_1,
            ...     return_polars=True
            ... )
            >>> print(type(df_polars))  # <class 'polars.dataframe.frame.DataFrame'>

        Note:
            When the current time is close to end_time, Vision API data may not be
            available due to the VISION_DATA_DELAY_HOURS constraint (typically 48 hours).
            In such cases, the method will automatically use the REST API for recent data.

            When auto_reindex=False and only partial cache data is available, the method
            will return only the cached data without attempting to fetch missing data
            from APIs, preventing artificial NaN value creation.
        """
        # Use chart_type from instance if None is provided
        if chart_type is None:
            chart_type = self.chart_type

        # Route to funding rate handler if chart_type is FUNDING_RATE
        # This uses a separate data path since funding rates have different structure
        if chart_type == ChartType.FUNDING_RATE:
            return self._fetch_funding_rate(
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                interval=interval,
                return_polars=return_polars,
            )

        try:
            # Validate interval against market type
            validate_interval(self.market_type, interval)

            # Generate trace_id for request correlation (GitHub Issue #10 - structured logging)
            trace_id = logger.generate_trace_id()
            bound_logger = logger.bind(
                trace_id=trace_id,
                symbol=symbol.upper(),
                market_type=self.market_type.name,
                interval=interval.value,
                event_type="fcp_request",
            )
            bound_logger.info(f"[FCP] Starting data retrieval trace_id={trace_id}")

            logger.debug(
                f"[FCP] get_data called with use_cache={self.use_cache}, auto_reindex={auto_reindex} for "
                f"symbol={symbol}, interval={interval.value}, chart_type={chart_type.name}"
            )
            logger.debug(f"[FCP] Time range: {start_time.isoformat()} to {end_time.isoformat()}")

            # Validate time range
            if start_time >= end_time:
                raise ValueError(f"start_time ({start_time}) must be before end_time ({end_time})")

            # Normalize symbol to upper case
            symbol = symbol.upper()

            # FAIL-LOUD: Validate symbol availability before any API calls (GitHub Issue #10)
            from ckvd.utils.for_core.vision_exceptions import DataNotAvailableError
            from ckvd.utils.validation.availability_data import (
                check_futures_counterpart_availability,
                is_symbol_available_at,
            )

            is_available, earliest_date = is_symbol_available_at(self.market_type, symbol, start_time)
            if not is_available and earliest_date is not None:
                raise DataNotAvailableError(
                    symbol=symbol,
                    market_type=self.market_type.name,
                    requested_start=start_time,
                    earliest_available=earliest_date,
                )

            # CROSS-MARKET WARNING: Check futures counterpart availability (for SPOT requests)
            futures_warning = check_futures_counterpart_availability(self.market_type, symbol, start_time)
            if futures_warning:
                # Console warning (loud) - stderr for visibility
                import sys

                print(
                    f"\n\u26a0\ufe0f  FUTURES COUNTERPART WARNING: {futures_warning.message}\n",
                    file=sys.stderr,
                )
                # Log for telemetry
                logger.warning(
                    f"[FCP] Futures counterpart unavailable: {futures_warning.message}",
                    extra={
                        "event_type": "futures_counterpart_unavailable",
                        "futures_market": futures_warning.futures_market,
                        "futures_earliest": futures_warning.earliest_date.isoformat(),
                        "requested_start": start_time.isoformat(),
                    },
                )

            # Log key parameters
            logger.info(f"Retrieving {interval.value} data for {symbol} from {start_time} to {end_time}")

            # CRITICAL FIX: Use different alignment strategies based on auto_reindex
            if auto_reindex:
                # When auto_reindex=True, align boundaries to ensure complete time series
                aligned_start, aligned_end = align_time_boundaries(start_time, end_time, interval)
                logger.debug(f"[FCP] Aligned boundaries for complete time series: {aligned_start} to {aligned_end}")
            else:
                # When auto_reindex=False, use exact user boundaries to prevent artificial gaps
                aligned_start, aligned_end = start_time, end_time
                logger.debug(f"[FCP] Using exact user boundaries (auto_reindex=False): {aligned_start} to {aligned_end}")

            # Initialize result DataFrame to hold progressively merged data
            result_df = pd.DataFrame()
            missing_ranges = []

            # Polars pipeline is always active for internal processing

            # ----------------------------------------------------------------
            # STEP 1: Local Cache Retrieval
            # ----------------------------------------------------------------
            if enforce_source == DataSource.CACHE and not self.use_cache:
                raise ValueError(
                    "Cannot use enforce_source=DataSource.CACHE when use_cache=False. "
                    "Either enable caching or use a different data source."
                )

            skip_cache = not self.use_cache or enforce_source in (
                DataSource.REST,
                DataSource.VISION,
            )

            # Initialize Polars pipeline for internal processing
            polars_pipeline = PolarsDataPipeline()

            if not skip_cache:
                # Use Polars LazyFrame-based cache retrieval
                from ckvd.utils.for_core.ckvd_cache_utils import get_cache_lazyframes

                logger.info(f"[FCP] STEP 1: Checking local cache for {symbol}")
                cache_lazyframes = get_cache_lazyframes(
                    symbol=symbol,
                    start_time=aligned_start,
                    end_time=aligned_end,
                    interval=interval,
                    cache_dir=self.cache_dir,
                    market_type=self.market_type,
                    chart_type=chart_type,
                )

                if cache_lazyframes:
                    for lf in cache_lazyframes:
                        polars_pipeline.add_source(lf, "CACHE")
                    logger.info(f"[FCP] Cache contributed {len(cache_lazyframes)} LazyFrame(s) to pipeline")

                    # Still need to identify missing ranges for Vision/REST steps
                    # Collect cache data to check coverage
                    cache_df = polars_pipeline.collect_pandas(use_streaming=True)
                    if not cache_df.empty:
                        from ckvd.utils.for_core.ckvd_time_range_utils import identify_missing_segments

                        missing_ranges = identify_missing_segments(cache_df, aligned_start, aligned_end, interval)
                        result_df = cache_df
                    else:
                        missing_ranges = [(aligned_start, aligned_end)]
                else:
                    missing_ranges = [(aligned_start, aligned_end)]
                    logger.debug(f"[FCP] No cache data available, entire range marked as missing: {aligned_start} to {aligned_end}")
            else:
                if enforce_source == DataSource.REST:
                    logger.info("[FCP] Cache check skipped due to enforce_source=REST")
                elif enforce_source == DataSource.VISION:
                    logger.info("[FCP] Cache check skipped due to enforce_source=VISION")
                else:
                    logger.info("[FCP] Cache disabled, skipping cache check")

                # If cache is disabled, treat entire range as missing
                missing_ranges = [(aligned_start, aligned_end)]

            # CRITICAL FIX: When auto_reindex=False and we have some data, don't fetch missing ranges
            if not auto_reindex and not result_df.empty:
                logger.info(f"[FCP] auto_reindex=False: Found {len(result_df)} cached records, skipping API calls to prevent NaN creation")
                missing_ranges = []  # Clear missing ranges to prevent API calls

            # ----------------------------------------------------------------
            # STEP 2: Vision API Retrieval with Iterative Merge
            # ----------------------------------------------------------------
            if enforce_source != DataSource.REST and missing_ranges:
                result_df, missing_ranges = process_vision_step(
                    fetch_from_vision_func=self._fetch_from_vision,
                    symbol=symbol,
                    missing_ranges=missing_ranges,
                    interval=interval,
                    include_source_info=include_source_info,
                    result_df=result_df,
                )

                # Add Vision data to Polars pipeline for final merge
                if not result_df.empty and "_data_source" in result_df.columns:
                    vision_df = result_df[result_df["_data_source"] == "VISION"]
                    if not vision_df.empty:
                        polars_pipeline.add_pandas(vision_df, "VISION")

            # ----------------------------------------------------------------
            # STEP 3: REST API Fallback with Final Merge
            # ----------------------------------------------------------------
            if missing_ranges and enforce_source != DataSource.VISION:
                result_df = process_rest_step(
                    fetch_from_rest_func=self._fetch_from_rest,
                    symbol=symbol,
                    missing_ranges=missing_ranges,
                    interval=interval,
                    include_source_info=include_source_info,
                    result_df=result_df,
                    save_to_cache_func=self._save_to_cache if self.use_cache else None,
                )

                # Add REST data to Polars pipeline for final merge
                if not result_df.empty and "_data_source" in result_df.columns:
                    rest_only_df = result_df[result_df["_data_source"] == "REST"]
                    if not rest_only_df.empty:
                        polars_pipeline.add_pandas(rest_only_df, "REST")

            # ----------------------------------------------------------------
            # Final check and standardization
            # ----------------------------------------------------------------
            verify_final_data(result_df, aligned_start, aligned_end)

            # First standardize columns to ensure consistent data types and format
            result_df = standardize_columns(result_df)

            # CRITICAL FIX: Filter to user's exact time range when auto_reindex=False
            if not auto_reindex and not result_df.empty:
                # Filter the result to the user's exact requested time range
                from ckvd.utils.time_utils import filter_dataframe_by_time

                original_length = len(result_df)
                result_df = filter_dataframe_by_time(result_df, start_time, end_time, "open_time")
                logger.info(f"[FCP] auto_reindex=False: Filtered to user's exact range: {original_length} -> {len(result_df)} records")

            # ----------------------------------------------------------------
            # Intelligent Reindexing Logic
            # ----------------------------------------------------------------
            # Only reindex if explicitly requested AND if we have some data to work with
            if auto_reindex and not result_df.empty:
                # Import additional utilities for enhanced functionality
                from ckvd.utils.for_core.ckvd_utilities import safely_reindex_dataframe

                # Check if we have significant missing ranges that couldn't be filled
                if missing_ranges:
                    # Calculate the percentage of missing data
                    total_expected_seconds = (aligned_end - aligned_start).total_seconds()
                    missing_seconds = sum((end - start).total_seconds() for start, end in missing_ranges)
                    missing_percentage = (missing_seconds / total_expected_seconds) * 100 if total_expected_seconds > 0 else 0

                    # If more than 50% of data is missing and we couldn't fetch it from APIs,
                    # warn the user about potential NaN padding
                    if missing_percentage > 50:
                        logger.warning(
                            f"[FCP] Reindexing will create {missing_percentage:.1f}% NaN values. "
                            f"Consider setting auto_reindex=False to get only available data, "
                            f"or ensure API access to fetch missing data."
                        )

                # Safely reindex to ensure a complete time series with no gaps
                # This gives users a complete DataFrame with the expected number of rows
                # even if some data could not be retrieved
                result_df = safely_reindex_dataframe(df=result_df, start_time=aligned_start, end_time=aligned_end, interval=interval)

            elif not auto_reindex:
                logger.info(
                    f"[FCP] auto_reindex=False: Returning {len(result_df)} available records without NaN padding for missing timestamps"
                )

            # Skip source info column if not requested
            if not include_source_info and "_data_source" in result_df.columns:
                result_df = result_df.drop(columns=["_data_source"])

            # CRITICAL FIX: Different completeness checks based on auto_reindex
            if auto_reindex:
                # Original completeness check for reindexed data
                from ckvd.utils.dataframe_utils import verify_data_completeness

                is_complete, gaps = verify_data_completeness(result_df, aligned_start, aligned_end, interval.value)

                if not is_complete:
                    logger.warning(
                        f"Data retrieval for {symbol} has {len(gaps)} gaps in the time series. "
                        f"The DataFrame contains NaN values for missing timestamps."
                    )
            # For auto_reindex=False, just report actual data coverage
            elif not result_df.empty and "open_time" in result_df.columns:
                actual_start = result_df["open_time"].min()
                actual_end = result_df["open_time"].max()
                logger.info(f"[FCP] auto_reindex=False: Data covers {actual_start} to {actual_end} ({len(result_df)} records)")

                # Check if we have NaN values (which shouldn't happen with auto_reindex=False)
                nan_count = result_df.isna().sum().sum()
                if nan_count > 0:
                    logger.error(f"[FCP] BUG: auto_reindex=False should not create {nan_count} NaN values!")

            logger.info(f"[FCP] Successfully retrieved {len(result_df)} records for {symbol}")

            # Convert to Polars if requested
            if return_polars:
                # Check if zero-copy Polars output is enabled
                use_polars_output = FeatureFlags().USE_POLARS_OUTPUT

                if use_polars_output:
                    # Zero-copy path: Use PolarsDataPipeline directly
                    # This avoids the wasteful pandas → polars conversion
                    logger.debug("[FCP] Using zero-copy Polars output (USE_POLARS_OUTPUT=True)")
                    result_pl = polars_pipeline.collect_polars(use_streaming=True)
                    logger.debug(f"[FCP] Zero-copy Polars DataFrame with {len(result_pl)} rows")
                    return result_pl
                # Fallback: Convert pandas DataFrame to Polars DataFrame
                # Reset index to include open_time as a column before conversion
                if result_df.index.name == "open_time":
                    result_df = result_df.reset_index()
                result_pl = pl.from_pandas(result_df)
                logger.debug(f"[FCP] Converted to Polars DataFrame with {len(result_pl)} rows")
                return result_pl

            return result_df

        except (
            VisionAPIError,
            RestAPIError,
            ValueError,
            TypeError,
            KeyError,
            OSError,
            pd.errors.ParserError,
        ) as e:
            # Preserve partial data on rate limit instead of destroying it
            if isinstance(e, RateLimitError) and not result_df.empty:
                logger.warning(f"[FCP] Rate limited but returning {len(result_df)} partial records")
                result_df.attrs["_rate_limited"] = True
                return standardize_columns(result_df)

            handle_error(e)
            return None  # unreachable, handle_error always raises

    def __enter__(self) -> "CryptoKlineVisionData":
        """Context manager entry point.

        Allows using the CryptoKlineVisionData in a with statement for automatic
        resource cleanup.

        Returns:
            CryptoKlineVisionData: Self reference for use in with statements

        Example:
            >>> with CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT) as manager:
            ...     df = manager.get_data("BTCUSDT", start_time, end_time, Interval.MINUTE_1)
            ...     # Resources are automatically cleaned up after the block
        """
        return self

    def __exit__(self, _exc_type: type | None, _exc_val: BaseException | None, _exc_tb: Any) -> None:
        """Context manager exit with resource cleanup.

        Automatically closes all clients and releases resources when exiting
        a with statement block.

        Args:
            _exc_type: Exception type if an exception occurred
            _exc_val: Exception value if an exception occurred
            _exc_tb: Exception traceback if an exception occurred
        """
        self.close()

    def close(self) -> None:
        """Close all clients and release resources.

        This method should be called when the CryptoKlineVisionData is no longer needed
        to properly clean up resources, particularly network clients.

        Note:
            If using the context manager pattern (with statement), this method
            is called automatically when exiting the block.

        Example:
            >>> # Manual resource cleanup
            >>> manager = CryptoKlineVisionData.create(DataProvider.BINANCE, MarketType.SPOT)
            >>> df = manager.get_data("BTCUSDT", start_time, end_time, Interval.MINUTE_1)
            >>> manager.close()  # Clean up resources
        """
        # Close Vision client if it exists
        if self.vision_client is not None:
            try:
                self.vision_client.close()
            except (OSError, AttributeError) as e:
                logger.warning(f"Error closing Vision client: {e}")
            self.vision_client = None

        # Close REST client if it exists
        if self.rest_client is not None:
            try:
                if hasattr(self.rest_client, "close"):
                    self.rest_client.close()
            except (OSError, AttributeError) as e:
                logger.warning(f"Error closing REST client: {e}")
            self.rest_client = None

        logger.debug("Closed all data clients")
