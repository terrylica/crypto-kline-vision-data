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
from utils.time_utils import (
    filter_dataframe_by_time,
    align_time_boundaries,
)
from utils.config import (
    OUTPUT_DTYPES,
    FUNDING_RATE_DTYPES,
    VISION_DATA_DELAY_HOURS,
    REST_CHUNK_SIZE,
    REST_MAX_CHUNKS,
    create_empty_dataframe,
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
        if self.market_type == MarketType.SPOT:
            return "spot"
        elif self.market_type == MarketType.FUTURES_USDT:
            return "futures_usdt"
        elif self.market_type == MarketType.FUTURES_COIN:
            return "futures_coin"
        elif self.market_type == MarketType.FUTURES:
            return "futures_usdt"  # Default to USDT for legacy type
        else:
            raise ValueError(f"Unsupported market type: {self.market_type}")

    def _should_use_vision_api(self, start_time: datetime, end_time: datetime) -> bool:
        """Determine if Vision API should be used based on time range.

        Vision API has a delay before data is available, typically 48 hours.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval

        Returns:
            True if Vision API should be used, False otherwise
        """
        # Get current time
        now = datetime.now(timezone.utc)

        # Check if end time is before the Vision data delay
        cutoff_time = now - timedelta(hours=VISION_DATA_DELAY_HOURS)

        # Use Vision API if entire time range is before the cutoff
        return end_time <= cutoff_time

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

    def _save_to_cache(self, df: pd.DataFrame, symbol: str, interval: Interval) -> None:
        """Save data to cache.

        Args:
            df: DataFrame to cache
            symbol: Symbol the data is for
            interval: Time interval of the data
        """
        if not self.use_cache or self.cache_manager is None or df.empty:
            return

        # Get market type string for cache key
        market_type_str = self._get_market_type_str()

        # Group data by date
        df["date"] = df["open_time"].dt.date
        for date, group in df.groupby("date"):
            # Remove the date column
            group = group.drop(columns=["date"])

            # Convert date to datetime at midnight
            cache_date = datetime.combine(date, datetime.min.time()).replace(
                tzinfo=timezone.utc
            )

            # Save to cache
            self.cache_manager.save_to_cache(
                df=group,
                symbol=symbol,
                interval=interval.value,
                date=cache_date,
                provider=self.provider.name,
                chart_type=self.chart_type.name,
                market_type=market_type_str,
            )

    def _fetch_from_vision(
        self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval
    ) -> pd.DataFrame:
        """Fetch data from Vision API.

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

        # Create Vision client if not already created
        if self.vision_client is None:
            self.vision_client = VisionDataClient(
                symbol=symbol,
                interval=interval.value,
                market_type=self.market_type,
            )

        try:
            # Fetch data from Vision API
            df = self.vision_client.fetch(
                start_time=start_time,
                end_time=end_time,
            )

            if df.empty:
                logger.warning(f"Vision API returned no data for {symbol}")
                return create_empty_dataframe()

            # Add source information
            df["_data_source"] = "VISION"

            logger.info(f"Retrieved {len(df)} records from Vision API")
            return df

        except Exception as e:
            logger.error(f"Error fetching data from Vision API: {e}")
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
                interval=interval,
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

        Args:
            dfs: List of DataFrames to merge

        Returns:
            Merged DataFrame
        """
        # Filter out empty DataFrames
        dfs = [df for df in dfs if not df.empty]

        if not dfs:
            return create_empty_dataframe()

        if len(dfs) == 1:
            return dfs[0]

        # Concatenate all DataFrames
        merged = pd.concat(dfs, ignore_index=True)

        # For overlapping records (same open_time), prioritize data sources in order:
        # CACHE > VISION > REST
        # First sort by open_time and source priority
        if "_data_source" in merged.columns:
            # Create a priority order for data sources
            def source_priority(source):
                priorities = {"CACHE": 1, "VISION": 2, "REST": 3}
                return priorities.get(source, 4)  # Default lowest priority for unknown

            merged["_source_priority"] = merged["_data_source"].apply(source_priority)
            # Sort by open_time first, then by source priority
            merged = merged.sort_values(["open_time", "_source_priority"])
            # Drop the temporary priority column
            merged = merged.drop(columns=["_source_priority"])

        # Remove duplicates based on open_time, keeping the first one (highest priority)
        merged = merged.drop_duplicates(subset=["open_time"], keep="first")

        # Sort by open_time
        merged = merged.sort_values("open_time").reset_index(drop=True)

        # Log data source statistics
        if "_data_source" in merged.columns:
            source_counts = merged["_data_source"].value_counts()
            logger.info(f"Merged data source statistics: {source_counts.to_dict()}")

        return merged

    def get_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval = Interval.MINUTE_1,
        chart_type: Optional[ChartType] = None,
        include_source_info: bool = True,
    ) -> pd.DataFrame:
        """Get data for the specified time range, using the FCP strategy.

        This method follows the Failover Composition Priority (FCP) strategy:
        1. Check cache first
        2. For missing data, try Vision API
        3. If Vision API fails, fall back to REST API

        Args:
            symbol: Symbol to retrieve data for
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Time interval between data points
            chart_type: Override chart type for this query
            include_source_info: Whether to include source information in the result

        Returns:
            DataFrame with requested data
        """
        # Use override chart type if provided
        original_chart_type = self.chart_type
        if chart_type is not None:
            self.chart_type = chart_type

        try:
            # Validate inputs
            if not isinstance(symbol, str) or not symbol:
                raise ValueError(f"Invalid symbol: {symbol}")

            if not isinstance(start_time, datetime) or not isinstance(
                end_time, datetime
            ):
                raise ValueError("Start time and end time must be datetime objects")

            if start_time >= end_time:
                raise ValueError(
                    f"Start time {start_time} must be before end time {end_time}"
                )

            if start_time.tzinfo is None or end_time.tzinfo is None:
                raise ValueError("Start time and end time must be timezone-aware")

            # Standardize symbol
            symbol = symbol.upper()

            logger.info(
                f"Retrieving {interval.value} data for {symbol} from {start_time} to {end_time}"
            )

            # Step 1: Try to get data from cache
            start_time_retrieval = time.time()
            cached_df, missing_ranges = self._get_from_cache(
                symbol, start_time, end_time, interval
            )

            # If we have all data from cache, return it
            if not missing_ranges:
                logger.info(f"Retrieved all data from cache: {len(cached_df)} records")
                if not include_source_info and "_data_source" in cached_df.columns:
                    cached_df = cached_df.drop(columns=["_data_source"])
                return cached_df

            # Step 2: Get missing data from appropriate sources
            all_dfs = [cached_df] if not cached_df.empty else []

            for missing_start, missing_end in missing_ranges:
                logger.info(
                    f"Fetching missing data from {missing_start} to {missing_end}"
                )

                # Check if we should use Vision API for this range
                use_vision = self._should_use_vision_api(missing_start, missing_end)

                # Try Vision API first if appropriate
                df = create_empty_dataframe()
                if use_vision:
                    logger.info("Attempting to use Vision API")
                    df = self._fetch_from_vision(
                        symbol, missing_start, missing_end, interval
                    )

                # If Vision API failed or returned no data, try REST API
                if df.empty:
                    logger.info("Falling back to REST API")
                    df = self._fetch_from_rest(
                        symbol, missing_start, missing_end, interval
                    )

                # Add to list of DataFrames if we got data
                if not df.empty:
                    all_dfs.append(df)

                    # Cache the data we just fetched
                    if self.use_cache:
                        self._save_to_cache(df, symbol, interval)

            # Merge all DataFrames
            result_df = self._merge_dataframes(all_dfs)

            # Log retrieval stats
            elapsed_time = time.time() - start_time_retrieval
            logger.info(
                f"Retrieved {len(result_df)} records for {symbol} in {elapsed_time:.2f} seconds"
            )

            # Log source breakdown
            if "_data_source" in result_df.columns:
                source_counts = result_df["_data_source"].value_counts()
                logger.info(f"Data source breakdown: {source_counts.to_dict()}")

            # Remove source information if not requested
            if not include_source_info and "_data_source" in result_df.columns:
                result_df = result_df.drop(columns=["_data_source"])

            return result_df

        except Exception as e:
            logger.error(f"Error retrieving data: {e}")
            return create_empty_dataframe()
        finally:
            # Restore original chart type
            if chart_type is not None:
                self.chart_type = original_chart_type

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

    def __exit__(self, exc_type, exc_val, exc_tb):
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
