#!/usr/bin/env python
"""Data source manager that mediates between different data sources."""

from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple, TypeVar, Type, List
from enum import Enum, auto
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
import time

from utils.logger_setup import logger
from utils.market_constraints import Interval, MarketType, ChartType, DataProvider
from utils.market_utils import get_market_type_str
from utils.time_utils import (
    filter_dataframe_by_time,
    align_time_boundaries,
    standardize_timestamp_precision,
)
from utils.config import (
    OUTPUT_DTYPES,
    FUNDING_RATE_DTYPES,
    VISION_DATA_DELAY_HOURS,
    REST_CHUNK_SIZE,
    REST_MAX_CHUNKS,
    create_empty_dataframe,
    CANONICAL_INDEX_NAME,
    REST_IS_STANDARD,
)
from utils.dataframe_utils import (
    ensure_open_time_as_column,
    ensure_open_time_as_index,
    standardize_dataframe,
    merge_dataframes,
)
from core.sync.rest_data_client import RestDataClient
from core.sync.vision_data_client import VisionDataClient
from core.sync.cache_manager import UnifiedCacheManager


class DataSource(Enum):
    """Enum for data source selection."""

    AUTO = auto()  # Automatically select best source
    REST = auto()  # Force REST API
    VISION = auto()  # Force Vision API


T = TypeVar("T")


@dataclass
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
        cache_expires_minutes (int): Cache expiration time in minutes.
            Default is 60 minutes. Increase for less frequently updated data.
        use_httpx (bool): Whether to use httpx instead of curl_cffi for HTTP clients.
            Default is False. Set to True if experiencing issues with curl_cffi.
        rest_client (Optional[RestDataClient]): Optional external REST API client.
            Default is None (auto-created). Only provide if you need custom REST client behavior.
        vision_client (Optional[VisionDataClient]): Optional external Vision API client.
            Default is None (auto-created). Only provide if you need custom Vision API behavior.
    """

    # Mandatory parameters
    market_type: MarketType
    provider: DataProvider

    # Optional parameters with defaults
    chart_type: ChartType = ChartType.KLINES
    cache_dir: Optional[Path] = None
    use_cache: bool = True
    retry_count: int = 5
    cache_expires_minutes: int = 60
    use_httpx: bool = False

    # Advanced parameters (rarely need to be changed)
    rest_client: Optional[RestDataClient] = None
    vision_client: Optional[VisionDataClient] = None

    def __post_init__(self):
        """Validate parameters after initialization."""
        if not isinstance(self.market_type, MarketType):
            raise TypeError(
                f"market_type must be a MarketType enum, got {type(self.market_type)}"
            )

        if not isinstance(self.provider, DataProvider):
            raise TypeError(
                f"provider must be a DataProvider enum, got {type(self.provider)}"
            )

        if not isinstance(self.chart_type, ChartType):
            raise TypeError(
                f"chart_type must be a ChartType enum, got {type(self.chart_type)}"
            )

        if self.cache_dir is not None and not isinstance(self.cache_dir, Path):
            self.cache_dir = Path(str(self.cache_dir))

        if self.retry_count < 0:
            raise ValueError(f"retry_count must be >= 0, got {self.retry_count}")

        if self.cache_expires_minutes <= 0:
            raise ValueError(
                f"cache_expires_minutes must be > 0, got {self.cache_expires_minutes}"
            )

    @classmethod
    def create(
        cls: Type[T], market_type: MarketType, provider: DataProvider, **kwargs
    ) -> T:
        """Create a DataSourceConfig with the given market_type, provider and optional overrides.

        This is a convenience builder method that allows for a more fluent interface.

        Args:
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
            provider: Data provider (currently only BINANCE is supported)
            **kwargs: Optional parameter overrides

        Returns:
            Configured DataSourceConfig instance

        Raises:
            TypeError: If market_type is not a MarketType enum or provider is not a DataProvider enum
            ValueError: If any parameter values are invalid

        Examples:
            # Basic config for SPOT market with Binance provider
            config = DataSourceConfig.create(MarketType.SPOT, DataProvider.BINANCE)

            # Config for FUTURES with custom cache directory and HTTP client
            config = DataSourceConfig.create(
                MarketType.FUTURES_USDT,
                DataProvider.BINANCE,
                cache_dir=Path("./my_cache"),
                use_httpx=True
            )
        """
        if not isinstance(market_type, MarketType):
            raise TypeError(
                f"market_type must be a MarketType enum, got {type(market_type)}"
            )
        if not isinstance(provider, DataProvider):
            raise TypeError(
                f"provider must be a DataProvider enum, got {type(provider)}"
            )
        return cls(market_type=market_type, provider=provider, **kwargs)


@dataclass
class DataQueryConfig:
    """Configuration for data queries in the DataSourceManager.

    This class provides a clear way to specify parameters for the get_data method.

    Attributes:
        symbol (str): Trading pair symbol (e.g., "BTCUSDT").
            Mandatory parameter specifying which trading pair to retrieve data for.
        start_time (datetime): Start time for data retrieval.
            Mandatory parameter. Must be timezone-aware (preferably UTC).
        end_time (datetime): End time for data retrieval.
            Mandatory parameter. Must be timezone-aware (preferably UTC).
        interval (Interval): Time interval between data points.
            Default is SECOND_1. Options vary by market type.
        use_cache (bool): Whether to use cache for this specific query.
            Default is True. Override to fetch fresh data for this query only.
        enforce_source (DataSource): Force specific data source.
            Default is AUTO (smart selection). Override to force REST or VISION API.
        provider (Optional[DataProvider]): Override provider for this query.
            Default is None (use the DataSourceManager's provider). When specified,
            this will override the manager's provider for this specific query.
        chart_type (Optional[ChartType]): Override chart type for this query.
            Default is None (use the DataSourceManager's chart type).
    """

    # Mandatory parameters
    symbol: str
    start_time: datetime
    end_time: datetime

    # Optional parameters with defaults
    interval: Interval = Interval.SECOND_1
    use_cache: bool = True
    enforce_source: DataSource = DataSource.AUTO

    # Override parameters
    provider: Optional[DataProvider] = None
    chart_type: Optional[ChartType] = None

    def __post_init__(self):
        """Validate parameters after initialization."""
        if not isinstance(self.symbol, str) or not self.symbol:
            raise ValueError(f"symbol must be a non-empty string, got {self.symbol}")

        if not isinstance(self.start_time, datetime):
            raise TypeError(
                f"start_time must be a datetime object, got {type(self.start_time)}"
            )

        if not isinstance(self.end_time, datetime):
            raise TypeError(
                f"end_time must be a datetime object, got {type(self.end_time)}"
            )

        # Check if datetimes are timezone-aware
        if self.start_time.tzinfo is None:
            raise ValueError("start_time must be timezone-aware")

        if self.end_time.tzinfo is None:
            raise ValueError("end_time must be timezone-aware")

        if self.start_time >= self.end_time:
            raise ValueError(
                f"start_time ({self.start_time}) must be before end_time ({self.end_time})"
            )

        if not isinstance(self.interval, Interval):
            raise TypeError(
                f"interval must be an Interval enum, got {type(self.interval)}"
            )

        if not isinstance(self.enforce_source, DataSource):
            raise TypeError(
                f"enforce_source must be a DataSource enum, got {type(self.enforce_source)}"
            )

        if self.provider is not None and not isinstance(self.provider, DataProvider):
            raise TypeError(
                f"provider must be a DataProvider enum or None, got {type(self.provider)}"
            )

        if self.chart_type is not None and not isinstance(self.chart_type, ChartType):
            raise TypeError(
                f"chart_type must be a ChartType enum or None, got {type(self.chart_type)}"
            )

        # Convert symbol to uppercase for consistency
        self.symbol = self.symbol.upper()

    @classmethod
    def create(
        cls: Type[T], symbol: str, start_time: datetime, end_time: datetime, **kwargs
    ) -> T:
        """Create a DataQueryConfig with required parameters and optional overrides.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            start_time: Start time for data retrieval (timezone-aware)
            end_time: End time for data retrieval (timezone-aware)
            **kwargs: Optional parameter overrides, which may include:
                - interval: Time interval between data points
                - use_cache: Whether to use cache for this query
                - enforce_source: Force specific data source
                - provider: Override the manager's provider for this query
                - chart_type: Override the manager's chart type for this query

        Returns:
            Configured DataQueryConfig instance

        Raises:
            ValueError: If symbol is empty or start_time >= end_time
            TypeError: If parameters have incorrect types

        Examples:
            # Basic query for Bitcoin hourly data
            query = DataQueryConfig.create(
                "BTCUSDT",
                datetime(2023, 1, 1, tzinfo=timezone.utc),
                datetime(2023, 1, 2, tzinfo=timezone.utc),
                interval=Interval.HOUR_1
            )

            # Query with forced Vision API usage
            query = DataQueryConfig.create(
                "ETHUSDT",
                start_time,
                end_time,
                enforce_source=DataSource.VISION
            )

            # Override provider for a specific query (if using a manager with a different provider)
            query = DataQueryConfig.create(
                "BTCUSDT",
                start_time,
                end_time,
                provider=DataProvider.BINANCE
            )
        """
        if not symbol:
            raise ValueError("symbol cannot be empty")

        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise TypeError("start_time and end_time must be datetime objects")

        if start_time >= end_time:
            raise ValueError(
                f"start_time ({start_time}) must be before end_time ({end_time})"
            )

        return cls(symbol=symbol, start_time=start_time, end_time=end_time, **kwargs)


class DataSourceManager:
    """Mediator between data sources with smart selection and caching.

    This class orchestrates data retrieval from different sources following
    the Failover Composition Priority (FCP) strategy:

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
    def get_output_format(
        cls, chart_type: ChartType = ChartType.KLINES
    ) -> Dict[str, str]:
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
        market_type: Optional[MarketType] = None,
        provider: Optional[DataProvider] = None,
        **kwargs,
    ):
        """Create a DataSourceManager with a more Pythonic interface.

        This factory method provides a cleaner way to instantiate the DataSourceManager
        with proper default values and documentation.

        Args:
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
                If None, uses the class's DEFAULT_MARKET_TYPE
            provider: Data provider (BINANCE)
                If None, raises ValueError as provider is now mandatory
            **kwargs: Additional parameters as needed

        Returns:
            Initialized DataSourceManager

        Raises:
            ValueError: If provider is None

        Examples:
            # Create a manager for spot market with Binance provider
            manager = DataSourceManager.create(MarketType.SPOT, DataProvider.BINANCE)

            # Create a manager for futures with custom settings
            manager = DataSourceManager.create(
                MarketType.FUTURES_USDT,
                DataProvider.BINANCE,
                chart_type=ChartType.FUNDING_RATE,
                cache_dir=Path("./my_cache"),
                use_httpx=True
            )
        """
        # Use the configured default market type if none provided
        if market_type is None:
            market_type = cls.DEFAULT_MARKET_TYPE
            logger.debug(f"Using default market type: {market_type.name}")

        # Provider is now mandatory
        if provider is None:
            raise ValueError("Data provider must be specified")

        config = DataSourceConfig.create(market_type, provider, **kwargs)
        return cls(
            market_type=config.market_type,
            provider=config.provider,
            chart_type=config.chart_type,
            cache_dir=config.cache_dir,
            use_cache=config.use_cache,
            retry_count=config.retry_count,
            cache_expires_minutes=config.cache_expires_minutes,
            use_httpx=config.use_httpx,
            rest_client=config.rest_client,
            vision_client=config.vision_client,
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
        market_type: MarketType = MarketType.SPOT,
        provider: DataProvider = DataProvider.BINANCE,
        use_cache: bool = True,
        cache_dir: Optional[Path] = None,
        retry_count: int = 3,
        chart_type: ChartType = ChartType.KLINES,
    ):
        """Initialize the data source manager.

        Args:
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
            provider: Data provider (currently only BINANCE is supported)
            use_cache: Whether to use caching
            cache_dir: Directory to store cache files (defaults to ./cache)
            retry_count: Number of retries for failed requests
            chart_type: Chart type (KLINES, FUNDING_RATE)
        """
        self.market_type = market_type
        self.provider = provider
        self.use_cache = use_cache
        self.retry_count = retry_count
        self.chart_type = chart_type

        # Set up cache directory
        if cache_dir is None:
            self.cache_dir = Path("./cache")
        else:
            self.cache_dir = Path(cache_dir)

        # Initialize cache manager if caching is enabled
        if self.use_cache:
            self.cache_manager = UnifiedCacheManager(cache_dir=self.cache_dir)
        else:
            self.cache_manager = None

        # Initialize clients to None - they will be created on demand
        self.rest_client = None
        self.vision_client = None

        logger.info(
            f"Initialized DataSourceManager with market_type={market_type.name}, "
            f"provider={provider.name}, use_cache={use_cache}, retry_count={retry_count}"
        )

    def _get_market_type_str(self) -> str:
        """Get string representation of market type for cache keys.

        Returns:
            String representation of market type
        """
        return get_market_type_str(self.market_type)

    def _should_use_vision_api(self, start_time: datetime, end_time: datetime) -> bool:
        """Determine if Vision API should be used based on time range.

        According to the Failover Composition Priority (FCP) strategy,
        we should always attempt to use Vision API first before falling back to REST.

        This function now returns True to enforce using Vision API as the preferred
        source for all missing data, regardless of how recent it is.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval

        Returns:
            True to always try Vision API first
        """
        # According to FCP, always attempt Vision API first
        return True

    def _get_from_cache(
        self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval
    ) -> Tuple[pd.DataFrame, List[Tuple[datetime, datetime]]]:
        """Retrieve data from cache and identify missing ranges.

        Args:
            symbol: Symbol to retrieve data for
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Time interval between data points

        Returns:
            Tuple of (cached DataFrame, list of missing date ranges)
        """
        if not self.use_cache or self.cache_manager is None:
            # Return empty DataFrame and the entire date range as missing
            return create_empty_dataframe(), [(start_time, end_time)]

        # Align time boundaries
        aligned_start, aligned_end = align_time_boundaries(
            start_time, end_time, interval
        )

        # Get market type string for cache key
        market_type_str = self._get_market_type_str()

        # Generate list of dates in the range
        dates = []
        current_date = aligned_start.replace(hour=0, minute=0, second=0, microsecond=0)
        while current_date <= aligned_end:
            dates.append(current_date)
            current_date += timedelta(days=1)

        # Try to load each date from cache
        cached_dfs = []
        missing_ranges = []
        last_missing_start = None

        for date in dates:
            df = self.cache_manager.load_from_cache(
                symbol=symbol,
                interval=interval.value,
                date=date,
                provider=self.provider.name,
                chart_type=self.chart_type.name,
                market_type=market_type_str,
            )

            if df is not None and not df.empty:
                # Add source information
                df["_data_source"] = "CACHE"

                cached_dfs.append(df)
                # If we were tracking a missing range, close it
                if last_missing_start is not None:
                    missing_end = date - timedelta(microseconds=1)
                    missing_ranges.append((last_missing_start, missing_end))
                    last_missing_start = None
            else:
                # Start tracking a missing range if we haven't already
                if last_missing_start is None:
                    last_missing_start = date

        # Close any open missing range
        if last_missing_start is not None:
            missing_end = aligned_end
            missing_ranges.append((last_missing_start, missing_end))

        # If we have no cached data, return empty DataFrame and the entire range as missing
        if not cached_dfs:
            return create_empty_dataframe(), [(aligned_start, aligned_end)]

        # Combine cached DataFrames
        combined_df = pd.concat(cached_dfs, ignore_index=True)

        # Remove duplicates and sort by open_time
        if not combined_df.empty:
            combined_df = combined_df.drop_duplicates(subset=["open_time"])
            combined_df = combined_df.sort_values("open_time").reset_index(drop=True)

            # Filter to requested time range
            combined_df = filter_dataframe_by_time(
                combined_df, aligned_start, aligned_end, "open_time"
            )

        return combined_df, missing_ranges

    def _save_to_cache(
        self, df: pd.DataFrame, symbol: str, interval: Interval, source: str = None
    ) -> None:
        """Save data to cache.

        Args:
            df: DataFrame to cache
            symbol: Symbol the data is for
            interval: Time interval of the data
            source: Data source (VISION, REST, etc.) - used to prioritize Vision API data for caching
        """
        print(
            f"**** SAVING TO CACHE: {symbol} {interval.value} with {len(df)} records, source={source}, use_cache={self.use_cache}"
        )

        if not self.use_cache or self.cache_manager is None:
            logger.debug(
                "Caching disabled or cache manager is None - skipping cache save"
            )
            return

        if df.empty:
            logger.warning(f"Empty DataFrame for {symbol} - skipping cache save")
            return

        # Enhanced debug info about incoming data
        logger.debug(f"_save_to_cache called for {symbol} with {len(df)} records")
        logger.debug(f"DataFrame columns: {list(df.columns)}")
        logger.debug(f"DataFrame dtypes: {df.dtypes}")

        try:
            # Ensure data is sorted by open_time before caching to prevent unsorted cache entries
            if (
                "open_time" in df.columns
                and not df["open_time"].is_monotonic_increasing
            ):
                logger.debug(f"Sorting data by open_time before caching for {symbol}")
                df = df.sort_values("open_time").reset_index(drop=True)

            # Get market type string for cache key
            market_type_str = self._get_market_type_str()

            # Group data by date
            if "open_time" not in df.columns:
                logger.error(f"DataFrame missing open_time column: {list(df.columns)}")
                return

            logger.debug(f"Creating date column from open_time for grouping")
            df["date"] = df["open_time"].dt.date

            logger.debug(f"Grouping {len(df)} records by date")
            date_groups = df.groupby("date")
            logger.debug(f"Found {len(date_groups)} date groups")

            for date, group in date_groups:
                logger.debug(
                    f"Processing group for date {date} with {len(group)} records"
                )
                # Remove the date column
                group = group.drop(columns=["date"])

                # Convert date to datetime at midnight
                cache_date = datetime.combine(date, datetime.min.time()).replace(
                    tzinfo=timezone.utc
                )

                logger.debug(
                    f"Saving {len(group)} records for {symbol} on {cache_date.date()} to cache"
                )

                # Always save data directly to Arrow cache
                success = self.cache_manager.save_to_cache(
                    df=group,
                    symbol=symbol,
                    interval=interval.value,
                    date=cache_date,
                    provider=self.provider.name,
                    chart_type=self.chart_type.name,
                    market_type=market_type_str,
                )

                if success:
                    logger.debug(
                        f"Successfully saved cache data for {symbol} on {cache_date.date()}"
                    )
                else:
                    logger.warning(
                        f"Failed to save cache data for {symbol} on {cache_date.date()}"
                    )
        except Exception as e:
            logger.error(f"Error in _save_to_cache: {e}")
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")

    def _fetch_from_vision(
        self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval
    ) -> pd.DataFrame:
        """Fetch data from Binance Vision API.

        Args:
            symbol: Symbol to retrieve data for
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Time interval between data points

        Returns:
            DataFrame with data from Vision API
        """
        logger.info(
            f"Fetching data from Vision API for {symbol} from {start_time} to {end_time}"
        )

        try:
            # Create Vision client if not already created
            if self.vision_client is None:
                logger.debug("Creating new Vision API client")
                self.vision_client = VisionDataClient(
                    symbol=symbol,
                    interval=interval.value,
                    market_type=self.market_type,
                )
            elif self.vision_client.symbol != symbol:
                # If client exists but for a different symbol, reconfigure it
                logger.debug("Reconfiguring Vision API client for new symbol")
                # Close existing client if needed
                if hasattr(self.vision_client, "close"):
                    self.vision_client.close()
                # Create new client
                self.vision_client = VisionDataClient(
                    symbol=symbol,
                    interval=interval.value,
                    market_type=self.market_type,
                )
            # Otherwise, reuse the existing client

            # Fetch data from Vision API
            df = self.vision_client.fetch(
                symbol=symbol,
                interval=interval.value,
                start_time=start_time,
                end_time=end_time,
            )

            # Add detailed debugging to understand DataFrame structure
            logger.debug(f"Vision API returned DataFrame with shape: {df.shape}")
            logger.debug(f"Vision API DataFrame columns: {list(df.columns)}")
            logger.debug(f"Vision API DataFrame index name: {df.index.name}")
            logger.debug(f"Vision API DataFrame index type: {type(df.index)}")

            if df.empty:
                # At this point, the Vision client should have already logged a specific warning
                # about why it couldn't download data, so we'll just log this as an info
                # to avoid duplicating warnings
                logger.info(
                    f"Vision API returned no data for {symbol} - falling back to REST API"
                )
                return create_empty_dataframe()

            # Ensure open_time exists in DataFrame (either as index or column)
            df = ensure_open_time_as_column(df)

            # Add source information
            df["_data_source"] = "VISION"

            # Save Vision API data to cache immediately
            if self.use_cache and not df.empty:
                logger.info(f"Saving Vision API data to Arrow cache for {symbol}")
                self._save_to_cache(df, symbol, interval, source="VISION")

            logger.info(f"Retrieved {len(df)} records from Vision API")

            # Debug the finalized DataFrame before returning
            logger.debug(f"Final Vision API DataFrame columns: {list(df.columns)}")
            if "open_time" in df.columns:
                logger.debug(f"open_time column type: {df['open_time'].dtype}")

            # Apply standardization which handles timestamp normalization
            df = self._standardize_columns(df)

            return df

        except Exception as e:
            logger.error(f"Error fetching data from Vision API: {e}")
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")

            # Check if this is a checksum verification error
            if "VISION API DATA INTEGRITY ERROR" in str(
                e
            ) or "Checksum verification failed" in str(e):
                logger.warning(
                    "Vision API checksum verification failed, falling back to REST API"
                )
                # Let the manager continue to the next data source in FCP

            return create_empty_dataframe()

    def _fetch_from_rest(
        self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval
    ) -> pd.DataFrame:
        """Fetch data from REST API with chunking for large requests.

        Args:
            symbol: Symbol to retrieve data for
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Time interval between data points

        Returns:
            DataFrame with data from REST API
        """
        logger.info(
            f"Fetching data from REST API for {symbol} from {start_time} to {end_time}"
        )

        # Create REST client if not already created
        if self.rest_client is None:
            self.rest_client = RestDataClient(
                market_type=self.market_type,
                retry_count=self.retry_count,
            )

        try:
            # Fetch data using the REST client
            df = self.rest_client.fetch(
                symbol=symbol,
                interval=interval.value,
                start_time=start_time,
                end_time=end_time,
            )

            if df.empty:
                logger.warning(f"REST API returned no data for {symbol}")
                return create_empty_dataframe()

            # Add source information
            df["_data_source"] = "REST"

            logger.info(f"Retrieved {len(df)} records from REST API")
            return df

        except Exception as e:
            logger.error(f"Error fetching data from REST API: {e}")
            return create_empty_dataframe()

    def _merge_dataframes(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Merge multiple DataFrames into one, handling overlaps.

        This method ensures:
        1. Each DataFrame has the same timestamp precision before merging
        2. Timestamps are standardized according to the REST API format
        3. Columns are consistently named and typed
        4. Duplicates are properly handled

        Args:
            dfs: List of DataFrames to merge

        Returns:
            Merged DataFrame
        """
        if not dfs:
            logger.warning("Empty list of DataFrames to merge")
            return create_empty_dataframe()

        if len(dfs) == 1:
            logger.debug("Only one DataFrame to merge, returning as is")
            return dfs[0]

        # Log information about DataFrames to be merged
        logger.debug(f"Merging {len(dfs)} DataFrames")
        for i, df in enumerate(dfs):
            logger.debug(
                f"DataFrame {i}: {len(df)} rows, source: {df.get('_data_source', 'unknown')[0] if not df.empty and '_data_source' in df.columns else 'unknown'}"
            )

            # Debug timestamp precision
            if not df.empty and isinstance(df.index, pd.DatetimeIndex):
                sample_ts = df.index[0].value
                precision = (
                    "microsecond" if len(str(abs(sample_ts))) > 13 else "millisecond"
                )
                logger.debug(f"DataFrame {i} has {precision} precision")

            # Ensure each DataFrame is properly formatted
            dfs[i] = self._standardize_columns(df)

            # Ensure open_time is set as index for proper merging
            if "open_time" in df.columns and df.index.name != CANONICAL_INDEX_NAME:
                dfs[i] = ensure_open_time_as_index(df)

            # Debug timestamp information
            if not dfs[i].empty:
                logger.debug(f"DataFrame {i} index type: {type(dfs[i].index)}")
                if isinstance(dfs[i].index, pd.DatetimeIndex):
                    logger.debug(f"DataFrame {i} index name: {dfs[i].index.name}")
                    logger.debug(f"DataFrame {i} index timezone: {dfs[i].index.tz}")

                    # Debug first few timestamps
                    if len(dfs[i]) > 0:
                        logger.debug(
                            f"DataFrame {i} first index timestamp: {dfs[i].index[0]}"
                        )
                        if "open_time" in dfs[i].columns:
                            logger.debug(
                                f"DataFrame {i} open_time type: {dfs[i]['open_time'].dtype}"
                            )

        # Now merge the DataFrames
        # First, concatenate all DataFrames
        logger.debug(f"Concatenating {len(dfs)} DataFrames")
        merged = pd.concat(dfs)

        # Ensure the index is sorted
        if not merged.index.is_monotonic_increasing:
            logger.debug("Sorting merged DataFrame by index")
            merged = merged.sort_index()

        # Remove duplicates, keeping the last occurrence (which is usually from the more reliable source)
        if merged.index.has_duplicates:
            duplicates_count = merged.index.duplicated().sum()
            logger.debug(
                f"Removing {duplicates_count} duplicate rows (keeping last occurrence)"
            )
            merged = merged[~merged.index.duplicated(keep="last")]

        # Final standardization to ensure consistency
        merged = self._standardize_columns(merged)

        logger.debug(
            f"Successfully merged {len(dfs)} DataFrames into one with {len(merged)} rows"
        )
        return merged

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
        """Get data from the most appropriate source with caching.

        This is the main entry point for getting data. It will:
        1. Check if data is in cache and use it if available
        2. Otherwise, get data from Vision API for older data if applicable
        3. If Vision API doesn't have the data, fall back to REST API
        4. Merge data from multiple sources if needed

        Args:
            symbol: Symbol to retrieve data for
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Time interval between data points
            chart_type: Type of chart data to retrieve (defaults to self.chart_type)
            include_source_info: Include _data_source column in output
            enforce_source: Force specific data source (AUTO, REST, VISION)

        Returns:
            DataFrame with the requested data
        """
        logger.debug(
            f"**** DEBUG INFO: get_data called with use_cache={self.use_cache} for symbol={symbol}, interval={interval.value}"
        )

        # Use the class chart_type if none is specified
        if chart_type is None:
            chart_type = self.chart_type

        # Validate input
        if not isinstance(symbol, str) or not symbol:
            raise ValueError(f"Invalid symbol: {symbol}")

        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise ValueError("start_time and end_time must be datetime objects")

        if not start_time.tzinfo or not end_time.tzinfo:
            raise ValueError("start_time and end_time must be timezone-aware")

        if start_time >= end_time:
            raise ValueError(
                f"start_time ({start_time}) must be earlier than end_time ({end_time})"
            )

        if not isinstance(interval, Interval):
            raise ValueError(f"interval must be an Interval enum, got {interval}")

        if not isinstance(chart_type, ChartType):
            raise ValueError(f"chart_type must be a ChartType enum, got {chart_type}")

        if not isinstance(enforce_source, DataSource):
            raise ValueError(
                f"enforce_source must be a DataSource enum, got {enforce_source}"
            )

        # Normalize symbol to upper case
        symbol = symbol.upper()

        # Log key parameters
        logger.info(
            f"Retrieving {interval.value} data for {symbol} from {start_time} to {end_time}"
        )

        try:
            # First, check cache if enabled
            dfs = []
            missing_ranges = []
            rest_fallback_ranges = []

            # Use cache if enabled
            if self.use_cache:
                # Get data from cache and identify missing ranges
                cache_df, missing_ranges = self._get_from_cache(
                    symbol, start_time, end_time, interval
                )
                # Add cache data to list if not empty
                if not cache_df.empty:
                    # Add source info if requested
                    if include_source_info and "_data_source" not in cache_df.columns:
                        cache_df["_data_source"] = "CACHE"
                    dfs.append(cache_df)
            else:
                # If cache is disabled, treat entire range as missing
                missing_ranges = [(start_time, end_time)]

            # If there are missing ranges, fetch from other sources
            if missing_ranges:
                logger.info(
                    f"Fetching missing data from {missing_ranges[0][0]} to {missing_ranges[-1][1]}"
                )

                # Determine which source to use based on enforce_source
                if enforce_source == DataSource.REST:
                    # Skip Vision API, use REST directly
                    for miss_start, miss_end in missing_ranges:
                        rest_fallback_ranges.append((miss_start, miss_end))
                elif enforce_source == DataSource.VISION:
                    # Use only Vision API (no REST fallback)
                    vision_df = pd.DataFrame()
                    for miss_start, miss_end in missing_ranges:
                        range_df = self._fetch_from_vision(
                            symbol, miss_start, miss_end, interval
                        )
                        # Add source info if requested
                        if (
                            include_source_info
                            and "_data_source" not in range_df.columns
                        ):
                            range_df["_data_source"] = "VISION"
                        vision_df = pd.concat([vision_df, range_df], ignore_index=True)

                    if not vision_df.empty:
                        dfs.append(vision_df)
                else:
                    # AUTO mode - try Vision first, fall back to REST
                    for miss_start, miss_end in missing_ranges:
                        # Check if we should try Vision API
                        if self._should_use_vision_api(miss_start, miss_end):
                            logger.info("Attempting to use Vision API")
                            range_df = self._fetch_from_vision(
                                symbol, miss_start, miss_end, interval
                            )
                            # If Vision API returned data, add it
                            if not range_df.empty:
                                # Add source info if requested
                                if (
                                    include_source_info
                                    and "_data_source" not in range_df.columns
                                ):
                                    range_df["_data_source"] = "VISION"
                                dfs.append(range_df)

                                # If Vision data was retrieved successfully and caching is enabled, save it
                                if self.use_cache:
                                    logger.debug(
                                        f"Auto-saving Vision data to cache for {symbol}"
                                    )
                                    self._save_to_cache(
                                        range_df, symbol, interval, source="VISION"
                                    )
                            else:
                                # If Vision failed, add to REST fallback ranges
                                rest_fallback_ranges.append((miss_start, miss_end))
                        else:
                            # If we shouldn't use Vision (e.g., too recent), use REST
                            rest_fallback_ranges.append((miss_start, miss_end))

                # Use REST API for any remaining ranges
                if rest_fallback_ranges:
                    for rest_start, rest_end in rest_fallback_ranges:
                        logger.info(
                            f"Falling back to REST API for {rest_start} to {rest_end}"
                        )
                        rest_df = self._fetch_from_rest(
                            symbol, rest_start, rest_end, interval
                        )
                        # Add source info if requested
                        if (
                            include_source_info
                            and "_data_source" not in rest_df.columns
                        ):
                            rest_df["_data_source"] = "REST"
                        dfs.append(rest_df)

                        # If REST data was retrieved successfully and caching is enabled, save it
                        if not rest_df.empty and self.use_cache:
                            logger.debug(f"Auto-saving REST data to cache for {symbol}")
                            self._save_to_cache(
                                rest_df, symbol, interval, source="REST"
                            )

            # Merge all DataFrames
            if not dfs:
                logger.warning("No data available from any source")
                return create_empty_dataframe(chart_type)

            # Check if we need to merge or just return the single DataFrame
            if len(dfs) == 1:
                result_df = dfs[0]
            else:
                try:
                    # Log info about each DataFrame before merging
                    for i, df in enumerate(dfs):
                        logger.debug(f"DataFrame {i} shape: {df.shape}")
                        logger.debug(f"DataFrame {i} columns: {list(df.columns)}")
                        if "open_time" in df.columns:
                            logger.debug(
                                f"DataFrame {i} open_time type: {df['open_time'].dtype}"
                            )
                        elif hasattr(df.index, "name") and df.index.name == "open_time":
                            logger.debug(f"DataFrame {i} has open_time as index")

                    result_df = self._merge_dataframes(dfs)
                except Exception as merge_error:
                    logger.error(f"Error merging DataFrames: {merge_error}")
                    # If merging fails, try to return the largest DataFrame
                    largest_df = max(dfs, key=len)
                    logger.warning(
                        f"Returning largest DataFrame with {len(largest_df)} rows instead"
                    )
                    result_df = largest_df

            # Skip source info column if not requested
            if not include_source_info and "_data_source" in result_df.columns:
                result_df = result_df.drop(columns=["_data_source"])

            logger.info(f"Successfully retrieved {len(result_df)} records for {symbol}")
            return result_df

        except Exception as e:
            # Improved error handling with better diagnostics
            logger.error(f"Error retrieving data: {e}")
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")

            # Special handling for open_time related errors
            if "'open_time'" in str(e):
                logger.error(
                    "This appears to be an issue with the open_time column in the DataFrame"
                )
                logger.error("Please check the data sources and conversion process")

                # Additional diagnostic information for debugging
                if "dfs" in locals() and dfs:
                    logger.error(f"Number of DataFrames: {len(dfs)}")
                    for i, df in enumerate(dfs):
                        logger.error(f"DataFrame {i} columns: {list(df.columns)}")
                        logger.error(f"DataFrame {i} index name: {df.index.name}")
                        if not df.empty:
                            logger.error(
                                f"DataFrame {i} first row: {df.iloc[0].to_dict()}"
                            )

            # Return empty DataFrame on error
            return create_empty_dataframe(chart_type)

    def analyze_merged_data(
        self,
        df: pd.DataFrame,
        cache_start: datetime,
        vision_start: datetime,
        rest_start: datetime,
        rest_end: datetime,
    ) -> Dict[str, int]:
        """Analyze the merged data to determine the source of each record.

        Args:
            df: The merged DataFrame
            cache_start: Start time for cache data
            vision_start: Start time for Vision API data
            rest_start: Start time for REST API data
            rest_end: End time for REST API data

        Returns:
            Dict with count of records from each source
        """
        if df.empty:
            return {"cache": 0, "vision": 0, "rest": 0, "gap": 0}

        # If we have the _data_source column, use it directly
        if "_data_source" in df.columns:
            source_counts = df["_data_source"].value_counts().to_dict()
            return {
                "cache": source_counts.get("CACHE", 0),
                "vision": source_counts.get("VISION", 0),
                "rest": source_counts.get("REST", 0),
                "gap": 0,  # No gap with direct source tracking
            }

        # Legacy fallback to timestamp-based classification
        # (This should only be used if _data_source is not available)
        # Calculate cache_end as the time before vision_start
        cache_end = vision_start
        vision_end = rest_start

        # Convert to UTC timestamps for comparison
        cache_ts = int(cache_start.timestamp() * 1000)
        cache_end_ts = int(cache_end.timestamp() * 1000)
        vision_ts = int(vision_start.timestamp() * 1000)
        vision_end_ts = int(vision_end.timestamp() * 1000)
        rest_ts = int(rest_start.timestamp() * 1000)
        rest_end_ts = int(rest_end.timestamp() * 1000)

        # Count records from each source
        cache_count = len(
            df[(df["open_time"] >= cache_ts) & (df["open_time"] < cache_end_ts)]
        )
        vision_count = len(
            df[(df["open_time"] >= vision_ts) & (df["open_time"] < vision_end_ts)]
        )
        rest_count = len(
            df[(df["open_time"] >= rest_ts) & (df["open_time"] < rest_end_ts)]
        )

        # Calculate gap filling (records outside the specified ranges)
        total = len(df)
        gap_count = total - (cache_count + vision_count + rest_count)

        logger.warning(
            "Using legacy timestamp-based source classification. "
            "This is less accurate than using the _data_source column."
        )

        return {
            "cache": cache_count,
            "vision": vision_count,
            "rest": rest_count,
            "gap": gap_count,
        }

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

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and data types to ensure consistency.

        This method ensures:
        1. Standardized column names (mapping variant names to canonical names)
        2. Consistent data types for all columns
        3. Timestamp precision standardization (to milliseconds, matching REST API)
        4. Proper handling of all timestamp-related columns

        Args:
            df: DataFrame to standardize

        Returns:
            Standardized DataFrame following REST API format
        """
        if df.empty:
            return df

        # Standardize column names
        column_map = {
            # Common name variations
            "open_time_ms": "open_time",
            "openTime": "open_time",
            "close_time_ms": "close_time",
            "closeTime": "close_time",
            # Volume variants
            "volume_base": "volume",
            "baseVolume": "volume",
            "volume_quote": "quote_asset_volume",
            "quoteVolume": "quote_asset_volume",
            # Other variants
            "trades": "count",
            "numberOfTrades": "count",
        }

        # Apply column mapping
        for old_name, new_name in column_map.items():
            if old_name in df.columns and new_name not in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)

        # First apply the centralized standardize_dataframe function
        # This function ensures proper column structure and data types
        df = standardize_dataframe(df)

        # Then standardize timestamp precision to align with REST API format
        # This ensures Vision API data (which may use microsecond precision in 2025+)
        # is converted to millisecond precision to match REST API format
        if REST_IS_STANDARD:
            logger.debug("Standardizing timestamp precision to match REST API format")
            df = standardize_timestamp_precision(df)

        return df

    def _load_from_cache(
        self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval
    ) -> pd.DataFrame:
        """Load data from cache.

        Args:
            symbol: Symbol to retrieve data for
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Time interval between data points

        Returns:
            DataFrame with data from cache
        """
        logger.info(
            f"Loading data from cache for {symbol} from {start_time} to {end_time}"
        )

        # Create a cache manager if not already created
        if self.cache_manager is None:
            self.cache_manager = UnifiedCacheManager()

        # Convert interval to string value
        interval_str = interval.value

        # Get market type string
        market_type_str = self._get_market_type_str()

        # Determine date range to load
        start_date = start_time.date()
        end_date = end_time.date()
        date_range = []
        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date)
            current_date = (
                datetime.combine(current_date, datetime.min.time(), timezone.utc)
                + timedelta(days=1)
            ).date()

        # Load data for each date in range
        all_dfs = []
        for date in date_range:
            # Convert date to datetime at midnight UTC
            date_dt = datetime.combine(date, datetime.min.time(), timezone.utc)

            # Try to load from cache
            df = self.cache_manager.load_from_cache(
                symbol=symbol,
                interval=interval_str,
                date=date_dt,
                provider=self.provider.name,
                chart_type=self.chart_type.name,
                market_type=market_type_str,
            )
            if df is not None and not df.empty:
                # Apply standardization to ensure consistent format including timestamp normalization
                df = self._standardize_columns(df)

                # Add source information
                df["_data_source"] = "CACHE"
                all_dfs.append(df)

        # If no data was loaded from cache, return empty DataFrame
        if not all_dfs:
            logger.info(f"No cache data found for {symbol}")
            return create_empty_dataframe()

        # Concatenate all loaded DataFrames
        combined_df = pd.concat(all_dfs)

        # Filter data to requested time range
        filtered_df = filter_dataframe_by_time(combined_df, start_time, end_time)

        logger.info(f"Successfully loaded {len(filtered_df)} records from cache")
        return filtered_df
