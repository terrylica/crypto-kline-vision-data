#!/usr/bin/env python
"""Data Source Manager (DSM) that mediates between different data sources."""

from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Type, TypeVar

import attrs
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
from utils.logger_setup import logger
from utils.market_constraints import ChartType, DataProvider, Interval, MarketType
from utils.time_utils import align_time_boundaries


class DataSource(Enum):
    """Enum for data source selection."""

    AUTO = auto()  # Automatically select best source
    REST = auto()  # Force REST API
    VISION = auto()  # Force Vision API


T = TypeVar("T")


@attrs.define
class DataSourceConfig:
    """Configuration for DataSourceManager.

    This class provides a convenient way to configure the DataSourceManager
    with clear parameter documentation and defaults.

    Attributes:
        market_type (MarketType): Market type (SPOT, FUTURES_USDT, FUTURES_COIN).
            Mandatory parameter that determines which market data to retrieve.
        provider (DataProvider): Data provider (BINANCE).
            Mandatory parameter that determines which data provider to use.
        chart_type (ChartType): Chart type (KLINES, FUNDING_RATE).
            Default is KLINES (candlestick data).
        cache_dir (Optional[Path]): Directory to store cache files.
            Default is './cache'. Set to None to disable caching.
        use_cache (bool): Whether to use caching.
            Default is True. Set to False to always fetch fresh data.
        retry_count (int): Number of retries for failed requests.
            Default is 5. Increase for less stable networks.
    """

    # Mandatory parameters
    market_type: MarketType = attrs.field()
    provider: DataProvider = attrs.field()

    # Optional parameters with defaults
    chart_type: ChartType = attrs.field(default=ChartType.KLINES)
    cache_dir: Optional[Path] = attrs.field(default=None)
    use_cache: bool = attrs.field(default=True)
    retry_count: int = attrs.field(default=5)

    def __attrs_post_init__(self):
        """Validate parameters after initialization."""
        if not isinstance(self.market_type, MarketType):
            raise TypeError(f"market_type must be a MarketType enum, got {type(self.market_type)}")

        if not isinstance(self.provider, DataProvider):
            raise TypeError(f"provider must be a DataProvider enum, got {type(self.provider)}")

        if not isinstance(self.chart_type, ChartType):
            raise TypeError(f"chart_type must be a ChartType enum, got {type(self.chart_type)}")

        if self.cache_dir is not None and not isinstance(self.cache_dir, Path):
            self.cache_dir = Path(str(self.cache_dir))

        if self.retry_count < 0:
            raise ValueError(f"retry_count must be >= 0, got {self.retry_count}")

    @classmethod
    def create(cls: Type[T], provider: DataProvider, market_type: MarketType, **kwargs) -> T:
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

        Examples:
            # Basic config for SPOT market with Binance provider
            config = DataSourceConfig.create(DataProvider.BINANCE, MarketType.SPOT)

            # Config for FUTURES with custom cache directory
            config = DataSourceConfig.create(
                DataProvider.BINANCE,
                MarketType.FUTURES_USDT,
                cache_dir=Path("./my_cache")
            )
        """
        if not isinstance(provider, DataProvider):
            raise TypeError(f"provider must be a DataProvider enum, got {type(provider)}")
        if not isinstance(market_type, MarketType):
            raise TypeError(f"market_type must be a MarketType enum, got {type(market_type)}")
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
    def calculate_time_range(cls, start_time=None, end_time=None, days=3, interval=Interval.MINUTE_1) -> Tuple[datetime, datetime]:
        """Calculate time range with flexible parameters.

        This method delegates to dsm_date_range_utils to handle various time range scenarios:
        1. End time with days (backward calculation)
        2. Start time with days (forward calculation)
        3. Explicit start and end times
        4. Days-only calculation (backward from current time)

        Args:
            start_time: Start time string or datetime object, or None
            end_time: End time string or datetime object, or None
            days: Number of days for the range if only start_time or end_time is provided
            interval: Time interval for data, used to align boundaries

        Returns:
            tuple: (start_datetime, end_datetime) as datetime objects

        Raises:
            ValueError: If both start_time and end_time are provided and start_time is after end_time
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
    def get_output_format(cls, chart_type: ChartType = ChartType.KLINES) -> Dict[str, str]:
        """Get the standardized output format specification.

        Args:
            chart_type: Type of chart data

        Returns:
            Dictionary mapping column names to their dtypes

        Note:
            - Index is always pd.DatetimeIndex in UTC timezone
            - All timestamps are aligned to interval boundaries
            - Empty DataFrames maintain this structure
        """
        if chart_type == ChartType.FUNDING_RATE:
            return cls.FUNDING_RATE_DTYPES.copy()
        return cls.OUTPUT_DTYPES.copy()

    @classmethod
    def create(
        cls,
        provider: Optional[DataProvider] = None,
        market_type: Optional[MarketType] = None,
        **kwargs,
    ):
        """Create a DataSourceManager with a more Pythonic interface.

        This factory method provides a cleaner way to instantiate the DataSourceManager
        with proper default values and documentation.

        Args:
            provider: Data provider (BINANCE)
                If None, raises ValueError as provider is now mandatory
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
                If None, uses the class's DEFAULT_MARKET_TYPE
            **kwargs: Additional parameters as needed

        Returns:
            Initialized DataSourceManager

        Raises:
            ValueError: If provider is None

        Examples:
            # Create a manager for spot market with Binance provider
            manager = DataSourceManager.create(DataProvider.BINANCE, MarketType.SPOT)

            # Create a manager for futures with custom settings
            manager = DataSourceManager.create(
                DataProvider.BINANCE,
                MarketType.FUTURES_USDT,
                chart_type=ChartType.FUNDING_RATE,
                cache_dir=Path("./my_cache")
            )
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
            market_type: Default market type to use

        Examples:
            # Configure FUTURES_USDT as the default market type
            DataSourceManager.configure_defaults(MarketType.FUTURES_USDT)

            # Create a manager using the configured default
            manager = DataSourceManager.create()  # Uses FUTURES_USDT
        """
        cls.DEFAULT_MARKET_TYPE = market_type
        logger.info(f"Configured default market type: {market_type.name}")

    def __init__(
        self,
        provider: DataProvider = DataProvider.BINANCE,
        market_type: MarketType = MarketType.SPOT,
        chart_type: ChartType = ChartType.KLINES,
        use_cache: bool = True,
        cache_dir: Optional[Path] = None,
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
    ) -> Tuple[pd.DataFrame, List[Tuple[datetime, datetime]]]:
        """Get data from cache and identify missing time ranges.

        Args:
            symbol: Symbol to retrieve data for
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Time interval between data points

        Returns:
            Tuple of (cached DataFrame, list of missing date ranges)
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
        """Save data to cache.

        Args:
            df: DataFrame to cache
            symbol: Symbol the data is for
            interval: Time interval of the data
            source: Data source (VISION, REST, etc.) - can be used for source-specific cache strategies
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
        """Fetch data from the Vision API using the utility function."""
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
        """Fetch data from REST API with chunking using the utility function."""
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
        chart_type: Optional[ChartType] = None,
        include_source_info: bool = True,
        enforce_source: DataSource = DataSource.AUTO,
    ) -> pd.DataFrame:
        """Retrieve data with the optimal retrieval strategy.

        This method applies the Failover Control Protocol (FCP) to retrieve data
        in the most efficient way:

        1. First, check local cache (if enabled)
        2. For missing data, try Binance VISION API (highly performant)
        3. If Vision API fails or is unavailable, use REST API as fallback

        Args:
            symbol: Trading symbol (e.g., BTCUSDT)
            start_time: Start time for data window
            end_time: End time for data window
            interval: Data interval (1m, 5m, 1h, etc.)
            chart_type: Chart type to retrieve (klines, etc.)
            include_source_info: Whether to include source information
            enforce_source: Optional override to enforce specific data source

        Returns:
            DataFrame with aligned data from the selected sources
        """
        # Use chart_type from instance if None is provided
        if chart_type is None:
            chart_type = self.chart_type

        try:
            # Validate interval against market type
            validate_interval(self.market_type, interval)

            logger.debug(
                f"[FCP] get_data called with use_cache={self.use_cache} for symbol={symbol}, interval={interval.value}, chart_type={chart_type.name}"
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

            # Standardize columns
            result_df = standardize_columns(result_df)

            # Skip source info column if not requested
            if not include_source_info and "_data_source" in result_df.columns:
                result_df = result_df.drop(columns=["_data_source"])

            logger.info(f"[FCP] Successfully retrieved {len(result_df)} records for {symbol}")
            return result_df

        except Exception as e:
            # Improved error handling using the dedicated error handler
            handle_error(e)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit with resource cleanup."""
        self.close()

    def close(self):
        """Close clients and release resources."""
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
