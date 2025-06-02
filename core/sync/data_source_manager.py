#!/usr/bin/env python
"""Data Source Manager (DSM) that mediates between different data sources.

This module implements the core Failover Control Protocol (FCP) strategy for robust
data retrieval from multiple sources. It orchestrates the data retrieval process
through a sequence of increasingly reliable sources:

1. Local Cache: Quick retrieval from local Apache Arrow files
2. Vision API: Fetching from Binance Vision API for historical data
3. REST API: Direct API calls for recent or missing data

The main classes are:
- DataSource: Enum for selecting data sources
- DataSourceConfig: Configuration for the DataSourceManager
- DataSourceManager: Core implementation of the FCP strategy

Example:
    >>> from core.sync.data_source_manager import DataSourceManager, DataSource
    >>> from utils.market_constraints import DataProvider, MarketType, Interval
    >>> from datetime import datetime
    >>>
    >>> # Create a manager for spot market
    >>> manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
    >>>
    >>> # Fetch BTCUSDT data for the last 3 days
    >>> df = manager.get_data(
    ...     symbol="BTCUSDT",
    ...     start_time=datetime(2023, 1, 1),
    ...     end_time=datetime(2023, 1, 5),
    ...     interval=Interval.MINUTE_1,
    ... )
"""

from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import TypeVar

import attr
import pandas as pd

from core.providers.binance.cache_manager import UnifiedCacheManager
from core.providers.binance.rest_data_client import RestDataClient
from core.providers.binance.vision_data_client import VisionDataClient
from core.providers.binance.vision_path_mapper import FSSpecVisionHandler
from utils.app_paths import get_cache_dir
from utils.config import (
    FUNDING_RATE_DTYPES,
    OUTPUT_DTYPES,
    REST_CHUNK_SIZE,
    REST_MAX_CHUNKS,
    VISION_DATA_DELAY_HOURS,
    create_empty_dataframe,
)
from utils.for_core.dsm_api_utils import (
    create_client_if_needed,
    fetch_from_rest,
    fetch_from_vision,
)
from utils.for_core.dsm_date_range_utils import (
    calculate_date_range,
    get_date_range_description,
)
from utils.for_core.dsm_fcp_utils import (
    handle_error,
    process_cache_step,
    process_rest_step,
    process_vision_step,
    validate_interval,
    verify_final_data,
)
from utils.for_core.dsm_time_range_utils import (
    standardize_columns,
)
from utils.loguru_setup import logger
from utils.market_constraints import ChartType, DataProvider, Interval, MarketType
from utils.time_utils import align_time_boundaries


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

    Example:
        >>> from utils.market_constraints import DataProvider, MarketType, ChartType
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
            >>> from utils.market_constraints import DataProvider, MarketType
            >>> from pathlib import Path
            >>>
            >>> # Basic config for SPOT market with Binance provider
            >>> config = DataSourceConfig.create(
            ...     DataProvider.BINANCE,
            ...     MarketType.SPOT
            ... )
            >>>
            >>> # Config for FUTURES with custom cache directory
            >>> config = DataSourceConfig.create(
            ...     DataProvider.BINANCE,
            ...     MarketType.FUTURES_USDT,
            ...     cache_dir=Path("./my_cache")
            ... )
        """
        return cls(market_type=market_type, provider=provider, **kwargs)


class DataSourceManager:
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
        >>> from core.sync.data_source_manager import DataSourceManager
        >>> from utils.market_constraints import DataProvider, MarketType, Interval, ChartType
        >>> from datetime import datetime
        >>>
        >>> # Basic usage
        >>> manager = DataSourceManager(
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
        >>> with DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT) as manager:
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
            >>> start, end = DataSourceManager.calculate_time_range(days=5)
            >>> print(f"Duration: {(end - start).days} days")
            Duration: 5 days

            >>> # End time with days - backwards from specified date
            >>> from datetime import datetime
            >>> end = datetime(2023, 1, 10)
            >>> start, _ = DataSourceManager.calculate_time_range(end_time=end, days=7)
            >>> print(start.date())
            2023-01-03

            >>> # Start time with days - forwards from specified date
            >>> start = datetime(2023, 1, 1)
            >>> _, end = DataSourceManager.calculate_time_range(start_time=start, days=3)
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
    def get_output_format(cls, chart_type: ChartType = ChartType.KLINES) -> dict[str, str]:
        """Get the standardized output format specification.

        Returns the column definitions and data types for the specified chart type.
        This ensures consistent DataFrame structure regardless of data source.

        Args:
            chart_type: Type of chart data (KLINES or FUNDING_RATE)

        Returns:
            dict: Dictionary mapping column names to their pandas dtypes

        Note:
            The returned format ensures:
            - Index is always pd.DatetimeIndex in UTC timezone
            - All timestamps are aligned to interval boundaries
            - Empty DataFrames maintain this structure

        Example:
            >>> # Get format for klines data
            >>> format_spec = DataSourceManager.get_output_format(ChartType.KLINES)
            >>> print(list(format_spec.keys())[:5])  # First 5 column names
            ['open', 'high', 'low', 'close', 'volume']

            >>> # Get format for funding rate data
            >>> fr_format = DataSourceManager.get_output_format(ChartType.FUNDING_RATE)
            >>> print('funding_rate' in fr_format)
            True
        """
        if chart_type == ChartType.FUNDING_RATE:
            return cls.FUNDING_RATE_DTYPES.copy()
        return cls.OUTPUT_DTYPES.copy()

    @classmethod
    def create(
        cls,
        provider: DataProvider | None = None,
        market_type: MarketType | None = None,
        **kwargs,
    ):
        """Create a DataSourceManager with a more Pythonic interface.

        This factory method provides a cleaner way to instantiate the DataSourceManager
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

        Returns:
            DataSourceManager: Initialized DataSourceManager instance

        Raises:
            ValueError: If provider is None

        Examples:
            >>> # Basic creation with required parameters
            >>> from core.sync.data_source_manager import DataSourceManager
            >>> from utils.market_constraints import DataProvider, MarketType
            >>>
            >>> manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
            >>>
            >>> # Creation with additional parameters
            >>> from utils.market_constraints import ChartType
            >>> from pathlib import Path
            >>>
            >>> manager = DataSourceManager.create(
            ...     DataProvider.BINANCE,
            ...     MarketType.FUTURES_USDT,
            ...     chart_type=ChartType.FUNDING_RATE,
            ...     cache_dir=Path("./custom_cache"),
            ...     retry_count=5
            ... )
        """
        # Provider is now mandatory
        if provider is None:
            raise ValueError("Data provider must be specified")

        # Use the configured default market type if none provided
        if market_type is None:
            market_type = cls.DEFAULT_MARKET_TYPE
            logger.debug(f"Using default market type: {market_type.name}")

        config = DataSourceConfig.create(provider, market_type, **kwargs)
        return cls(
            provider=config.provider,
            market_type=config.market_type,
            chart_type=config.chart_type,
            cache_dir=config.cache_dir,
            use_cache=config.use_cache,
            retry_count=config.retry_count,
        )

    @classmethod
    def configure_defaults(cls, market_type: MarketType) -> None:
        """Configure default market type for all DataSourceManager instances.

        This class method allows you to set a global default market type that will
        be used when no market_type is provided to the create() method.

        Args:
            market_type: Default market type to use for all future instances

        Example:
            >>> # Configure FUTURES_USDT as the default market type
            >>> from core.sync.data_source_manager import DataSourceManager
            >>> from utils.market_constraints import MarketType
            >>>
            >>> DataSourceManager.configure_defaults(MarketType.FUTURES_USDT)
            >>>
            >>> # Create a manager using the configured default
            >>> manager = DataSourceManager.create(DataProvider.BINANCE)  # Uses FUTURES_USDT
        """
        cls.DEFAULT_MARKET_TYPE = market_type
        logger.info(f"Configured default market type: {market_type.name}")

    def __init__(
        self,
        provider: DataProvider = DataProvider.BINANCE,
        market_type: MarketType = MarketType.SPOT,
        chart_type: ChartType = ChartType.KLINES,
        use_cache: bool = True,
        cache_dir: Path | None = None,
        retry_count: int = 3,
    ):
        """Initialize the data source manager.

        Args:
            provider: Data provider (BINANCE)
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
            chart_type: Chart type (KLINES, FUNDING_RATE)
            use_cache: Whether to use local cache
            cache_dir: Directory to store cache files (default: platform-specific cache dir)
            retry_count: Number of retries for network operations
        """
        self.provider = provider
        self.market_type = market_type
        self.chart_type = chart_type
        self.use_cache = use_cache
        self.retry_count = retry_count

        # Set up cache directory
        if cache_dir is not None:
            self.cache_dir = Path(cache_dir)
        else:
            # Use platform-specific cache directory from app_paths
            self.cache_dir = get_cache_dir() / "data"

        # Initialize FSSpecVisionHandler for cache operations
        self.fs_handler = None
        if self.use_cache:
            try:
                self.fs_handler = FSSpecVisionHandler(base_cache_dir=self.cache_dir)
                logger.info(f"Initialized FSSpecVisionHandler with cache_dir={self.cache_dir}")
            except Exception as e:
                logger.error(f"Failed to initialize FSSpecVisionHandler: {e}")
                logger.warning("Continuing without cache")
                self.use_cache = False

        # Legacy cache manager (kept for backward compatibility but not used for new code paths)
        self.cache_manager = None
        if self.use_cache:
            try:
                self.cache_manager = UnifiedCacheManager(cache_dir=self.cache_dir)
                logger.debug("Legacy cache manager initialized (for backward compatibility)")
            except Exception as e:
                logger.warning(f"Failed to initialize legacy cache manager: {e}")

        # Initialize API clients
        self.rest_client = None
        self.vision_client = None

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
        from utils.for_core.dsm_cache_utils import get_from_cache

        if not self.use_cache or self.cache_dir is None:
            # Return empty DataFrame and the entire date range as missing
            return create_empty_dataframe(), [(start_time, end_time)]

        logger.info(f"Checking cache for {symbol} from {start_time} to {end_time}")

        # Use the new cache utils implementation with FSSpecVisionHandler
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
        from utils.for_core.dsm_cache_utils import save_to_cache

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

        # Use the new cache utils implementation with FSSpecVisionHandler
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
        # Create or reconfigure Vision client if needed
        self.vision_client = create_client_if_needed(
            client=self.vision_client,
            client_class=VisionDataClient,
            symbol=symbol,
            interval=interval.value,
            market_type=self.market_type,
            chart_type=self.chart_type,
            cache_dir=self.cache_dir,
        )

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
            fs_handler=self.fs_handler,
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
        # Create REST client if not already created
        self.rest_client = create_client_if_needed(
            client=self.rest_client,
            client_class=RestDataClient,
            market_type=self.market_type,
            retry_count=self.retry_count,
        )

        # Call the extracted utility function
        return fetch_from_rest(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            rest_client=self.rest_client,
            chart_type=self.chart_type,
        )

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
    ) -> pd.DataFrame:
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

        Returns:
            DataFrame containing the requested market data with columns:
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

        try:
            # Validate interval against market type
            validate_interval(self.market_type, interval)

            logger.debug(
                f"[FCP] get_data called with use_cache={self.use_cache} for "
                f"symbol={symbol}, interval={interval.value}, chart_type={chart_type.name}"
            )
            logger.debug(f"[FCP] Time range: {start_time.isoformat()} to {end_time.isoformat()}")

            # Validate time range
            if start_time >= end_time:
                raise ValueError(f"start_time ({start_time}) must be before end_time ({end_time})")

            # Normalize symbol to upper case
            symbol = symbol.upper()

            # Log key parameters
            logger.info(f"Retrieving {interval.value} data for {symbol} from {start_time} to {end_time}")

            # Record the aligned boundaries for consistent reference
            aligned_start, aligned_end = align_time_boundaries(start_time, end_time, interval)
            logger.debug(f"[FCP] Aligned boundaries for retrieval: {aligned_start} to {aligned_end}")

            # Initialize result DataFrame to hold progressively merged data
            result_df = pd.DataFrame()
            missing_ranges = []

            # ----------------------------------------------------------------
            # STEP 1: Local Cache Retrieval
            # ----------------------------------------------------------------
            skip_cache = not self.use_cache or enforce_source in (
                DataSource.REST,
                DataSource.VISION,
            )

            if not skip_cache:
                result_df, missing_ranges = process_cache_step(
                    use_cache=self.use_cache,
                    get_from_cache_func=self._get_from_cache,
                    symbol=symbol,
                    aligned_start=aligned_start,
                    aligned_end=aligned_end,
                    interval=interval,
                    include_source_info=include_source_info,
                )
            else:
                if enforce_source == DataSource.REST:
                    logger.info("[FCP] Cache check skipped due to enforce_source=REST")
                elif enforce_source == DataSource.VISION:
                    logger.info("[FCP] Cache check skipped due to enforce_source=VISION")
                else:
                    logger.info("[FCP] Cache disabled, skipping cache check")

                # If cache is disabled, treat entire range as missing
                missing_ranges = [(aligned_start, aligned_end)]

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

            # ----------------------------------------------------------------
            # Final check and standardization
            # ----------------------------------------------------------------
            verify_final_data(result_df, aligned_start, aligned_end)

            # First standardize columns to ensure consistent data types and format
            result_df = standardize_columns(result_df)

            # ----------------------------------------------------------------
            # Intelligent Reindexing Logic
            # ----------------------------------------------------------------
            # Only reindex if explicitly requested AND if we have some data to work with
            if auto_reindex and not result_df.empty:
                # Import additional utilities for enhanced functionality
                from utils.for_core.dsm_utilities import safely_reindex_dataframe

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

            # Final consistency check and logging
            from utils.dataframe_utils import verify_data_completeness

            is_complete, gaps = verify_data_completeness(result_df, aligned_start, aligned_end, interval.value)

            if not is_complete:
                logger.warning(
                    f"Data retrieval for {symbol} has {len(gaps)} gaps in the time series. "
                    f"The DataFrame contains NaN values for missing timestamps."
                )

            logger.info(f"[FCP] Successfully retrieved {len(result_df)} records for {symbol}")
            return result_df

        except Exception as e:
            # Improved error handling using the dedicated error handler
            handle_error(e)

    def __enter__(self):
        """Context manager entry point.

        Allows using the DataSourceManager in a with statement for automatic
        resource cleanup.

        Returns:
            DataSourceManager: Self reference for use in with statements

        Example:
            >>> with DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT) as manager:
            ...     df = manager.get_data("BTCUSDT", start_time, end_time, Interval.MINUTE_1)
            ...     # Resources are automatically cleaned up after the block
        """
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit with resource cleanup.

        Automatically closes all clients and releases resources when exiting
        a with statement block.

        Args:
            _exc_type: Exception type if an exception occurred
            _exc_val: Exception value if an exception occurred
            _exc_tb: Exception traceback if an exception occurred
        """
        self.close()

    def close(self):
        """Close all clients and release resources.

        This method should be called when the DataSourceManager is no longer needed
        to properly clean up resources, particularly network clients.

        Note:
            If using the context manager pattern (with statement), this method
            is called automatically when exiting the block.

        Example:
            >>> # Manual resource cleanup
            >>> manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)
            >>> df = manager.get_data("BTCUSDT", start_time, end_time, Interval.MINUTE_1)
            >>> manager.close()  # Clean up resources
        """
        # Close Vision client if it exists
        if self.vision_client is not None:
            try:
                self.vision_client.close()
            except Exception as e:
                logger.warning(f"Error closing Vision client: {e}")
            self.vision_client = None

        # Close REST client if it exists
        if self.rest_client is not None:
            try:
                if hasattr(self.rest_client, "close"):
                    self.rest_client.close()
            except Exception as e:
                logger.warning(f"Error closing REST client: {e}")
            self.rest_client = None

        logger.debug("Closed all data clients")
