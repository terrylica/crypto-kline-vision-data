#!/usr/bin/env python
"""Data source manager that mediates between different data sources."""

from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple, TypeVar, Type, List
from enum import Enum, auto
import pandas as pd
from pathlib import Path
from dataclasses import dataclass

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
    REST_IS_STANDARD,
)
from utils.dataframe_utils import (
    ensure_open_time_as_column,
    standardize_dataframe,
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

    @classmethod
    def create(
        cls: Type[T], market_type: MarketType, provider: DataProvider, **kwargs
    ) -> T:
        """Create a DataSourceConfig with the given market_type, provider and optional overrides.

        This is a convenience builder method that allows for a more fluent interface.

        Args:
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
            provider: Data provider (BINANCE)
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
        use_httpx: bool = False,
    ):
        """Initialize the data source manager.

        Args:
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN)
            provider: Data provider (BINANCE)
            use_cache: Whether to use local cache
            cache_dir: Directory to store cache files (default: "./cache")
            retry_count: Number of retries for network operations
            chart_type: Chart type (KLINES, FUNDING_RATE)
            use_httpx: Whether to use httpx instead of curl_cffi
        """
        self.market_type = market_type
        self.provider = provider
        self.use_cache = use_cache
        self.retry_count = retry_count
        self.chart_type = chart_type
        self.use_httpx = use_httpx

        # Set up cache directory
        if cache_dir is not None:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path("./cache")

        # Initialize cache manager if caching is enabled
        self.cache_manager = None
        if self.use_cache:
            try:
                self.cache_manager = UnifiedCacheManager(cache_dir=self.cache_dir)
            except Exception as e:
                logger.error(f"Failed to initialize cache manager: {e}")
                logger.warning("Continuing without cache")
                self.use_cache = False

        # Initialize API clients
        self.rest_client = None
        self.vision_client = None

    def _get_market_type_str(self) -> str:
        """Get string representation of market type for cache keys.

        Returns:
            String representation of market type
        """
        return get_market_type_str(self.market_type)

    def _should_use_vision_api(self, start_time: datetime, end_time: datetime) -> bool:
        """Determine if Vision API should be used based on time range.

        According to the Failover Control Protocol (FCP) strategy,
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
            logger.debug(
                f"Cache disabled or not initialized. Returning entire range as missing: {start_time} to {end_time}"
            )
            return create_empty_dataframe(), [(start_time, end_time)]

        # Align time boundaries
        aligned_start, aligned_end = align_time_boundaries(
            start_time, end_time, interval
        )

        logger.debug(
            f"[FCP] Cache retrieval with aligned boundaries: {aligned_start} to {aligned_end}"
        )

        # Get market type string for cache key
        market_type_str = self._get_market_type_str()

        # Generate list of dates in the range
        dates = []
        current_date = aligned_start.replace(hour=0, minute=0, second=0, microsecond=0)
        while current_date <= aligned_end:
            dates.append(current_date)
            current_date += timedelta(days=1)

        logger.debug(
            f"[FCP] Checking cache for {len(dates)} dates from {dates[0].date()} to {dates[-1].date()}"
        )

        # Try to load each date from cache
        cached_dfs = []
        missing_ranges = []
        last_missing_start = None
        incomplete_days = []
        all_empty = True

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
                all_empty = False
                # Add source information
                df["_data_source"] = "CACHE"

                # Check if this day has complete data (1440 minutes for a full day)
                expected_records = 1440  # Full day of 1-minute data
                if len(df) < expected_records:
                    incomplete_days.append((date, len(df)))
                    logger.debug(
                        f"[FCP] Day {date.date()} has incomplete data: {len(df)}/{expected_records} records"
                    )
                else:
                    logger.debug(
                        f"[FCP] Loaded {len(df)} records from cache for {date.date()}"
                    )

                cached_dfs.append(df)
                # If we were tracking a missing range, close it
                if last_missing_start is not None:
                    missing_end = date - timedelta(microseconds=1)
                    missing_ranges.append((last_missing_start, missing_end))
                    logger.debug(
                        f"[FCP] Identified missing range: {last_missing_start} to {missing_end}"
                    )
                    last_missing_start = None
            else:
                # Start tracking a missing range if we haven't already
                if last_missing_start is None:
                    last_missing_start = date
                    logger.debug(
                        f"[FCP] Started tracking missing range from {date.date()}"
                    )

        # Close any open missing range
        if last_missing_start is not None:
            missing_end = aligned_end
            missing_ranges.append((last_missing_start, missing_end))
            logger.debug(
                f"[FCP] Closing final missing range: {last_missing_start} to {missing_end}"
            )

        # If we have no cached data, return empty DataFrame and the entire range as missing
        if all_empty or not cached_dfs:
            logger.debug(
                f"[FCP] No cached data found for entire range. Missing: {aligned_start} to {aligned_end}"
            )
            return create_empty_dataframe(), [(aligned_start, aligned_end)]

        # Combine cached DataFrames
        combined_df = pd.concat(cached_dfs, ignore_index=True)
        logger.debug(
            f"[FCP] Combined {len(cached_dfs)} cache dataframes with total {len(combined_df)} records"
        )

        # Remove duplicates and sort by open_time
        if not combined_df.empty:
            combined_df = combined_df.drop_duplicates(subset=["open_time"])
            combined_df = combined_df.sort_values("open_time").reset_index(drop=True)
            logger.debug(
                f"[FCP] After deduplication: {len(combined_df)} records from cache"
            )

            # Filter to requested time range
            before_filter_len = len(combined_df)
            combined_df = filter_dataframe_by_time(
                combined_df, aligned_start, aligned_end, "open_time"
            )
            logger.debug(
                f"[FCP] After time filtering: {len(combined_df)} records (removed {before_filter_len - len(combined_df)})"
            )

            # Check time bounds of the filtered data
            if not combined_df.empty:
                min_time = combined_df["open_time"].min()
                max_time = combined_df["open_time"].max()
                logger.debug(f"[FCP] Cache data spans from {min_time} to {max_time}")

                # Check for gaps at the beginning or end of the range
                if min_time > aligned_start:
                    logger.debug(
                        f"[FCP] Missing data at beginning: {aligned_start} to {min_time}"
                    )
                    missing_ranges.append(
                        (aligned_start, min_time - timedelta(seconds=1))
                    )

                if max_time < aligned_end:
                    # This is the critical fix - detect missing data at the end!
                    logger.debug(
                        f"[FCP] Missing data at end: {max_time} to {aligned_end}"
                    )
                    missing_ranges.append(
                        (max_time + timedelta(minutes=1), aligned_end)
                    )

                # Now check for incomplete days and add them to missing ranges
                # This ensures that days with just a few records get fully refreshed
                for date, record_count in incomplete_days:
                    # If this day is within our aligned range and significantly incomplete
                    if (
                        date.date() >= aligned_start.date()
                        and date.date() <= aligned_end.date()
                        and record_count < 1440 * 0.9
                    ):  # If less than 90% complete

                        # Create a range for this day
                        day_start = date.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        )
                        day_end = date.replace(
                            hour=23, minute=59, second=59, microsecond=999999
                        )

                        # If this is the first day, adjust start time
                        if day_start.date() == aligned_start.date():
                            day_start = aligned_start

                        # If this is the last day, adjust end time
                        if day_end.date() == aligned_end.date():
                            day_end = aligned_end

                        logger.debug(
                            f"[FCP] Adding incomplete day to missing ranges: {day_start} to {day_end} ({record_count}/1440 records)"
                        )
                        missing_ranges.append((day_start, day_end))

        # Merge overlapping or adjacent ranges
        if missing_ranges:
            merged_ranges = self._merge_adjacent_ranges(missing_ranges, interval)
            logger.debug(
                f"[FCP] Merged {len(missing_ranges)} missing ranges into {len(merged_ranges)} ranges"
            )
            missing_ranges = merged_ranges

        # Log the missing ranges in detail
        if missing_ranges:
            for i, (miss_start, miss_end) in enumerate(missing_ranges):
                logger.debug(
                    f"[FCP] Missing range {i+1}/{len(missing_ranges)}: {miss_start} to {miss_end}"
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

        Note:
            Following the FCP mechanism requirements, Vision data is delivered in daily packs.
            When Vision data is requested for any part of a day, the entire day's data is
            downloaded and cached. This ensures complete daily data availability in the cache
            regardless of the specific time range requested.
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
            logger.error(f"Empty DataFrame for {symbol} - skipping cache save")
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
        """Fetch data from the Vision API.

        Args:
            symbol: Symbol to retrieve data for
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Time interval between data points

        Returns:
            DataFrame with data from Vision API filtered to the requested time range

        Note:
            As a core part of the FCP mechanism, this method implements the Daily Pack Caching requirement:
            1. Regardless of the requested time range (start_time to end_time), the method expands
               the request to fetch full days of data from Vision API
            2. The complete daily data is cached to ensure consistent and complete availability
            3. Only the originally requested time range is returned to the caller

            This ensures that even if a request specifies a start time at the beginning, middle, or end
            of the day, the entire day's data is cached for future use.
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

            # Get aligned boundaries to ensure complete data
            aligned_start, aligned_end = align_time_boundaries(
                start_time, end_time, interval
            )

            # For the FCP mechanism, we need to ensure that the full days' data is downloaded
            # and cached from Vision API, even if only a partial day is requested

            # Calculate full-day boundaries for Vision API data retrieval
            # Start from the beginning of the day for start_time
            vision_start = aligned_start.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            # End at the end of the day for end_time
            vision_end = aligned_end.replace(
                hour=23, minute=59, second=59, microsecond=999999
            )

            logger.debug(
                f"[FCP] Expanding Vision API request to full days: {vision_start} to {vision_end}"
            )

            # Vision API has date-based files, fetch with chunking
            df = self.vision_client.fetch(
                symbol=symbol,
                interval=interval.value,
                start_time=vision_start,
                end_time=vision_end,
                chart_type=self.chart_type,
            )

            if df is not None and not df.empty:
                # Add debugging information about dataframe
                logger.debug(f"Vision API returned DataFrame with shape: {df.shape}")
                if hasattr(df, "index") and df.index is not None:
                    logger.debug(
                        f"DataFrame index name: {df.index.name}, type: {type(df.index).__name__}"
                    )

                # Add source information
                df["_data_source"] = "VISION"

                # Save the entire day's data to cache before filtering to the requested range
                if self.use_cache:
                    logger.debug(f"[FCP] Caching full day's data from Vision API")
                    self._save_to_cache(df, symbol, interval, source="VISION")

                # Filter the dataframe to the originally requested time range
                logger.debug(
                    f"[FCP] Filtering Vision API data to originally requested range: {aligned_start} to {aligned_end}"
                )
                filtered_df = filter_dataframe_by_time(
                    df, aligned_start, aligned_end, "open_time"
                )

                # Help with debugging
                logger.info(
                    f"Retrieved {len(filtered_df)} records from Vision API (after filtering to requested range)"
                )

                return filtered_df
            else:
                logger.warning(f"Vision API returned no data for {symbol}")
                return create_empty_dataframe()

        except Exception as e:
            # Sanitize error message to prevent binary data from causing rich formatting issues
            try:
                error_message = str(e)
                # Replace any non-printable characters to prevent rich markup errors
                safe_error_message = "".join(
                    c if c.isprintable() else f"\\x{ord(c):02x}" for c in error_message
                )

                # Check if this is a critical error that should be propagated
                if (
                    "CRITICAL ERROR" in safe_error_message
                    or "DATA INTEGRITY ERROR" in safe_error_message
                ):
                    logger.critical(f"Vision API critical error: {safe_error_message}")
                    raise  # Re-raise to trigger failover

                # Check if the request is within the allowed delay window for Vision API
                # Only tolerate failures for recent data that may not be available yet
                current_time = datetime.now(timezone.utc)
                vision_delay = timedelta(hours=self.VISION_DATA_DELAY_HOURS)

                if end_time > (current_time - vision_delay):
                    # This falls within the allowable delay window for Vision API
                    logger.warning(
                        f"Error fetching recent data from Vision API (within {self.VISION_DATA_DELAY_HOURS}h delay window): {safe_error_message}"
                    )
                    return create_empty_dataframe()

                # For historical data outside the delay window, log critical error
                logger.critical(
                    f"Vision API failed to retrieve historical data: {safe_error_message}"
                )
                logger.critical(f"Error type: {type(e).__name__}")

                # More controlled traceback handling
                import traceback

                tb_string = traceback.format_exc()
                # Sanitize the traceback too
                safe_tb = "".join(
                    c if c.isprintable() else f"\\x{ord(c):02x}" for c in tb_string
                )
                tb_lines = safe_tb.splitlines()

                logger.critical("Traceback summary:")
                for line in tb_lines[:3]:  # Just log first few lines
                    logger.critical(line)
                logger.critical("...")
                for line in tb_lines[-3:]:  # And last few lines
                    logger.critical(line)

                # Propagate the error to trigger failover
                raise RuntimeError(
                    f"CRITICAL: Vision API failed to retrieve historical data: {safe_error_message}"
                )

            except Exception as nested_error:
                # If even our error handling fails, log a simpler message
                logger.critical(
                    f"Vision API error occurred (details unavailable): {type(e).__name__}"
                )
                logger.critical(
                    f"Error handling also failed: {type(nested_error).__name__}"
                )

                # Propagate the error to trigger failover
                raise RuntimeError(
                    "CRITICAL: Vision API error could not be handled properly"
                )

    def _fetch_from_rest(
        self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval
    ) -> pd.DataFrame:
        """Fetch data from REST API with chunking.

        Args:
            symbol: Symbol to retrieve data for
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Time interval between data points

        Returns:
            DataFrame with data from REST API

        Raises:
            RuntimeError: When REST API fails to retrieve data. As this is the final
                          data source in the FCP chain, failures here represent
                          complete failure of all data sources.
        """
        logger.info(
            f"Fetching data from REST API for {symbol} from {start_time} to {end_time}"
        )

        try:
            # Create REST client if not already created
            if self.rest_client is None:
                logger.debug("Initialized RestDataClient")
                self.rest_client = RestDataClient(
                    market_type=self.market_type,
                    retry_count=self.retry_count,
                )

            # Get aligned boundaries to ensure complete data
            aligned_start, aligned_end = align_time_boundaries(
                start_time, end_time, interval
            )
            logger.debug(
                f"Complete data range after alignment: {aligned_start} to {aligned_end}"
            )

            # REST API has limits, so get data with chunking
            df = self.rest_client.fetch(
                symbol=symbol,
                interval=interval.value,
                start_time=aligned_start,
                end_time=aligned_end,
                chart_type=self.chart_type,
            )

            if df.empty:
                logger.critical(f"REST API returned no data for {symbol}")
                raise RuntimeError(f"CRITICAL: REST API returned no data for {symbol}")

            # Add source information
            df["_data_source"] = "REST"

            # Help with debugging
            logger.info(f"Retrieved {len(df)} records from REST API")

            return df
        except Exception as e:
            # Sanitize error message to prevent binary data from causing rich formatting issues
            try:
                error_message = str(e)
                # Replace any non-printable characters to prevent rich markup errors
                safe_error_message = "".join(
                    c if c.isprintable() else f"\\x{ord(c):02x}" for c in error_message
                )

                logger.critical(f"Error in _fetch_from_rest: {safe_error_message}")
                logger.critical(f"Error type: {type(e).__name__}")

                # More controlled traceback handling
                import traceback

                tb_string = traceback.format_exc()
                # Sanitize the traceback
                safe_tb = "".join(
                    c if c.isprintable() else f"\\x{ord(c):02x}" for c in tb_string
                )
                tb_lines = safe_tb.splitlines()

                logger.critical("Traceback summary:")
                for line in tb_lines[
                    :3
                ]:  # Just log first few lines to avoid binary data
                    logger.critical(line)
                logger.critical("...")
                for line in tb_lines[-3:]:  # And last few lines
                    logger.critical(line)

                # This is the final fallback in the FCP chain, so raise an error
                # to indicate complete failure of all sources
                raise RuntimeError(
                    f"CRITICAL: REST API fallback failed: {safe_error_message}"
                )

            except Exception as nested_error:
                # If even our error handling fails, log a simpler message
                logger.critical(f"REST API critical error: {type(e).__name__}")
                logger.critical(
                    f"Error handling also failed: {type(nested_error).__name__}"
                )

                # Propagate the error
                raise RuntimeError(
                    "CRITICAL: REST API error could not be handled properly"
                )

            # This line should never be reached due to the raises above

    def _merge_dataframes(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Merge multiple DataFrames into one, handling overlaps.

        This method is a critical part of the FCP mechanism that ensures:
        1. Each DataFrame has consistent open_time formatting
        2. Source information is preserved during merging
        3. When duplicate timestamps exist, higher priority sources are preferred
           (REST > VISION > CACHE, unless the data came from recent updates)
        4. Columns are consistently named, typed, and aligned
        5. The resulting DataFrame maintains 1-minute granularity

        Args:
            dfs: List of DataFrames to merge

        Returns:
            Merged DataFrame with consistent schema
        """
        if not dfs:
            logger.warning("[FCP] Empty list of DataFrames to merge")
            return create_empty_dataframe()

        if len(dfs) == 1:
            logger.debug(
                "[FCP] Only one DataFrame to merge, standardizing and returning"
            )
            # Ensure consistent formatting even for single DataFrame
            result = dfs[0].copy()
            return self._standardize_columns(result)

        # Log information about DataFrames to be merged
        logger.debug(f"[FCP] Merging {len(dfs)} DataFrames")

        # Ensure all DataFrames have open_time as a column, not just an index
        for i, df in enumerate(dfs):
            if df.empty:
                logger.warning(f"[FCP] DataFrame {i} is empty, skipping")
                continue

            if "open_time" not in df.columns:
                logger.debug(
                    f"[FCP] Converting index to open_time column in DataFrame {i}"
                )
                if df.index.name == "open_time":
                    df = df.reset_index()
                else:
                    logger.warning(
                        f"[FCP] DataFrame {i} has no open_time column or index"
                    )

            # Ensure open_time is a datetime column
            if not pd.api.types.is_datetime64_any_dtype(df["open_time"]):
                logger.debug(f"[FCP] Converting open_time to datetime in DataFrame {i}")
                df["open_time"] = pd.to_datetime(df["open_time"], utc=True)

            # Add data source information if missing
            if "_data_source" not in df.columns:
                logger.debug(f"[FCP] Adding unknown source tag to DataFrame {i}")
                df["_data_source"] = "UNKNOWN"

            # Replace the DataFrame in the list with the processed version
            dfs[i] = df

        # Log source counts before merging
        for i, df in enumerate(dfs):
            if not df.empty and "_data_source" in df.columns:
                source_counts = df["_data_source"].value_counts()
                for source, count in source_counts.items():
                    logger.debug(
                        f"[FCP] DataFrame {i} contains {count} records from source={source}"
                    )

        # Concatenate all DataFrames
        logger.debug(f"[FCP] Concatenating {len(dfs)} DataFrames")
        merged = pd.concat(dfs, ignore_index=True)

        # Set source priority for resolving duplicates (higher number = higher priority)
        source_priority = {
            "UNKNOWN": 0,
            "CACHE": 1,
            "VISION": 2,
            "REST": 3,
        }

        # Add a numeric priority column based on data source
        if "_data_source" in merged.columns:
            merged["_source_priority"] = merged["_data_source"].map(source_priority)
        else:
            merged["_source_priority"] = 0

        # Sort by open_time and source priority (high priority last to keep in drop_duplicates)
        logger.debug("[FCP] Sorting merged DataFrame by open_time and source priority")
        merged = merged.sort_values(["open_time", "_source_priority"])

        # Remove duplicates, keeping the highest priority source for each timestamp
        if "open_time" in merged.columns:
            before_count = len(merged)
            merged = merged.drop_duplicates(subset=["open_time"], keep="last")
            after_count = len(merged)

            if before_count > after_count:
                logger.debug(
                    f"[FCP] Removed {before_count - after_count} duplicate timestamps, keeping highest priority source"
                )

        # Remove the temporary source priority column
        if "_source_priority" in merged.columns:
            merged = merged.drop(columns=["_source_priority"])

        # Sort by open_time to ensure chronological order
        merged = merged.sort_values("open_time").reset_index(drop=True)

        # Final standardization to ensure consistency across all columns
        merged = self._standardize_columns(merged)

        # Log statistics about the merged result
        if "_data_source" in merged.columns and not merged.empty:
            source_counts = merged["_data_source"].value_counts()
            for source, count in source_counts.items():
                percentage = (count / len(merged)) * 100
                logger.debug(
                    f"[FCP] Final merged DataFrame contains {count} records ({percentage:.1f}%) from {source}"
                )

        logger.debug(
            f"[FCP] Successfully merged {len(dfs)} DataFrames into one with {len(merged)} rows"
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
        """Get data for the specified symbol and time range from the best available source.

        This method is the main entry point for retrieving data. It implements the
        Failover Control Protocol (FCP) mechanism with three integrated phases:

        1. LOCAL CACHE RETRIEVAL: First check local Apache Arrow files for data
           - Data successfully retrieved from cache is immediately merged into the output
           - Missing segments are identified for retrieval from other sources

        2. VISION API RETRIEVAL WITH ITERATIVE MERGE: For missing segments, try Binance Vision API
           - Vision data is downloaded in full daily packs (core business logic requirement)
           - Each day's complete data is cached regardless of the specific time range requested
           - Retrieved Vision data is iteratively merged with available cache data

        3. REST API FALLBACK WITH FINAL MERGE: For any remaining missing segments, use REST API
           - REST API is queried for the precise missing ranges only
           - Retrieved REST data is merged with the cumulative dataset
           - All data is standardized to ensure consistent column formats

        Args:
            symbol: Symbol to retrieve data for (e.g., "BTCUSDT")
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Time interval between data points
            chart_type: Type of chart data to retrieve (overrides the instance setting)
            include_source_info: Whether to include _data_source column in output
            enforce_source: Force specific data source (AUTO, REST, VISION)

        Returns:
            DataFrame with data from the best available source(s), merged according to FCP
        """
        # Use the chart type from parameters or fall back to instance setting
        chart_type = chart_type or self.chart_type

        try:
            logger.debug(
                f"[FCP] get_data called with use_cache={self.use_cache} for symbol={symbol}, interval={interval.value}, chart_type={chart_type.name}"
            )
            logger.debug(
                f"[FCP] Time range: {start_time.isoformat()} to {end_time.isoformat()}"
            )

            # Validate time range
            if start_time >= end_time:
                raise ValueError(
                    f"start_time ({start_time}) must be before end_time ({end_time})"
                )

            # Normalize symbol to upper case
            symbol = symbol.upper()

            # Log key parameters
            logger.info(
                f"Retrieving {interval.value} data for {symbol} from {start_time} to {end_time}"
            )

            # Record the aligned boundaries for consistent reference
            aligned_start, aligned_end = align_time_boundaries(
                start_time, end_time, interval
            )
            logger.debug(
                f"[FCP] Aligned boundaries for retrieval: {aligned_start} to {aligned_end}"
            )

            # Initialize result DataFrame to hold progressively merged data
            result_df = pd.DataFrame()

            # ----------------------------------------------------------------
            # STEP 1: Local Cache Retrieval
            # ----------------------------------------------------------------
            if (
                self.use_cache
                and enforce_source != DataSource.REST
                and enforce_source != DataSource.VISION
            ):
                logger.info(f"[FCP] STEP 1: Checking local cache for {symbol}")
                # Get data from cache
                cache_df, missing_ranges = self._get_from_cache(
                    symbol, aligned_start, aligned_end, interval
                )

                if not cache_df.empty:
                    # Add source info if requested
                    if include_source_info and "_data_source" not in cache_df.columns:
                        cache_df["_data_source"] = "CACHE"

                    # Log the time range of the cache data
                    min_time = cache_df["open_time"].min()
                    max_time = cache_df["open_time"].max()
                    logger.debug(
                        f"[FCP] Cache data provides records from {min_time} to {max_time}"
                    )

                    # Set result_df to the cache data
                    result_df = cache_df
                    logger.info(f"[FCP] Cache contributed {len(cache_df)} records")
                else:
                    # If cache is empty, treat entire range as missing
                    missing_ranges = [(aligned_start, aligned_end)]
                    logger.debug(
                        f"[FCP] No cache data available, entire range marked as missing: {aligned_start} to {aligned_end}"
                    )
            else:
                if enforce_source == DataSource.REST:
                    logger.info("[FCP] Cache check skipped due to enforce_source=REST")
                elif enforce_source == DataSource.VISION:
                    logger.info(
                        "[FCP] Cache check skipped due to enforce_source=VISION"
                    )
                else:
                    logger.info("[FCP] Cache disabled, skipping cache check")

                # If cache is disabled, treat entire range as missing
                missing_ranges = [(aligned_start, aligned_end)]

            # ----------------------------------------------------------------
            # STEP 2: Vision API Retrieval with Iterative Merge
            # ----------------------------------------------------------------
            if enforce_source != DataSource.REST and missing_ranges:
                logger.info(f"[FCP] STEP 2: Checking Vision API for missing data")

                # Process each missing range
                vision_ranges_to_fetch = []

                for miss_start, miss_end in missing_ranges:
                    # Check if we should try Vision API for this range
                    if self._should_use_vision_api(miss_start, miss_end):
                        vision_ranges_to_fetch.append((miss_start, miss_end))
                    else:
                        logger.debug(
                            f"[FCP] Range too recent for Vision API: {miss_start} to {miss_end}"
                        )

                # Process Vision API ranges
                if vision_ranges_to_fetch and enforce_source != DataSource.REST:
                    remaining_ranges = []

                    for range_idx, (miss_start, miss_end) in enumerate(
                        vision_ranges_to_fetch
                    ):
                        logger.debug(
                            f"[FCP] Fetching from Vision API range {range_idx+1}/{len(vision_ranges_to_fetch)}: {miss_start} to {miss_end}"
                        )

                        range_df = self._fetch_from_vision(
                            symbol, miss_start, miss_end, interval
                        )

                        if not range_df.empty:
                            # Add source info
                            if (
                                include_source_info
                                and "_data_source" not in range_df.columns
                            ):
                                range_df["_data_source"] = "VISION"

                            # If we already have data, merge with the new data
                            if not result_df.empty:
                                logger.debug(
                                    f"[FCP] Merging {len(range_df)} Vision records with existing {len(result_df)} records"
                                )
                                result_df = self._merge_dataframes(
                                    [result_df, range_df]
                                )
                            else:
                                # Otherwise just use the Vision data
                                result_df = range_df

                            # Save to cache if enabled (removed as it's now handled in _fetch_from_vision)
                            # Note: Full day's data is now cached directly in _fetch_from_vision

                            # Check if Vision API returned all expected records or if there are gaps
                            if not result_df.empty:
                                # Identify any remaining missing segments from Vision API
                                missing_segments = self._identify_missing_segments(
                                    result_df, miss_start, miss_end, interval
                                )

                                if missing_segments:
                                    logger.debug(
                                        f"[FCP] Vision API left {len(missing_segments)} missing segments"
                                    )
                                    remaining_ranges.extend(missing_segments)
                                else:
                                    logger.debug(
                                        f"[FCP] Vision API provided complete coverage for this range"
                                    )
                        else:
                            # Vision API returned no data for this range
                            logger.debug(f"[FCP] Vision API returned no data for range")
                            remaining_ranges.append((miss_start, miss_end))

                    # Update missing_ranges to only include what's still missing after Vision API
                    if remaining_ranges:
                        # Merge adjacent or overlapping ranges
                        missing_ranges = self._merge_adjacent_ranges(
                            remaining_ranges, interval
                        )
                        logger.debug(
                            f"[FCP] After Vision API, still have {len(missing_ranges)} missing ranges"
                        )
                    else:
                        missing_ranges = []
                        logger.debug(f"[FCP] No missing ranges after Vision API")

            # ----------------------------------------------------------------
            # STEP 3: REST API Fallback with Final Merge
            # ----------------------------------------------------------------
            if missing_ranges and enforce_source != DataSource.VISION:
                logger.info(
                    f"[FCP] STEP 3: Using REST API for {len(missing_ranges)} remaining missing ranges"
                )

                # Merge adjacent ranges to minimize API calls
                merged_rest_ranges = self._merge_adjacent_ranges(
                    missing_ranges, interval
                )

                for range_idx, (miss_start, miss_end) in enumerate(merged_rest_ranges):
                    logger.debug(
                        f"[FCP] Fetching from REST API range {range_idx+1}/{len(merged_rest_ranges)}: {miss_start} to {miss_end}"
                    )

                    rest_df = self._fetch_from_rest(
                        symbol, miss_start, miss_end, interval
                    )

                    if not rest_df.empty:
                        # Add source info
                        if (
                            include_source_info
                            and "_data_source" not in rest_df.columns
                        ):
                            rest_df["_data_source"] = "REST"

                        # If we already have data, merge with the new data
                        if not result_df.empty:
                            logger.debug(
                                f"[FCP] Merging {len(rest_df)} REST records with existing {len(result_df)} records"
                            )
                            result_df = self._merge_dataframes([result_df, rest_df])
                        else:
                            # Otherwise just use the REST data
                            result_df = rest_df

                        # Save to cache if enabled
                        if self.use_cache:
                            logger.debug(f"[FCP] Auto-saving REST data to cache")
                            self._save_to_cache(
                                rest_df, symbol, interval, source="REST"
                            )

            # ----------------------------------------------------------------
            # Final check and standardization
            # ----------------------------------------------------------------
            if result_df.empty:
                logger.critical(
                    "[FCP] CRITICAL ERROR: No data available from any source"
                )
                raise RuntimeError(
                    "All data sources failed. Unable to retrieve data for the requested time range."
                )

            # Standardize columns
            result_df = self._standardize_columns(result_df)

            # Final verification of the result
            min_time = result_df["open_time"].min()
            max_time = result_df["open_time"].max()
            logger.debug(
                f"[FCP] Final result spans from {min_time} to {max_time} with {len(result_df)} records"
            )

            # Check if result covers the entire requested range
            if min_time > aligned_start or max_time < aligned_end:
                logger.warning(
                    f"[FCP] Result does not cover full requested range. Missing start: {min_time > aligned_start}, Missing end: {max_time < aligned_end}"
                )

                if min_time > aligned_start:
                    logger.warning(
                        f"[FCP] Missing data at start: {aligned_start} to {min_time}"
                    )
                if max_time < aligned_end:
                    logger.warning(
                        f"[FCP] Missing data at end: {max_time} to {aligned_end}"
                    )

            # Skip source info column if not requested
            if not include_source_info and "_data_source" in result_df.columns:
                result_df = result_df.drop(columns=["_data_source"])

            logger.info(
                f"[FCP] Successfully retrieved {len(result_df)} records for {symbol}"
            )
            return result_df

        except Exception as e:
            # Improved error handling for any exception in the main get_data method
            try:
                # Sanitize error message to prevent binary data from causing rich formatting issues
                error_message = str(e)
                # Replace any non-printable characters
                safe_error_message = "".join(
                    c if c.isprintable() else f"\\x{ord(c):02x}" for c in error_message
                )

                logger.critical(f"Error in get_data: {safe_error_message}")
                logger.critical(f"Error type: {type(e).__name__}")

                # More controlled traceback handling
                import traceback

                tb_string = traceback.format_exc()
                # Sanitize the traceback
                safe_tb = "".join(
                    c if c.isprintable() else f"\\x{ord(c):02x}" for c in tb_string
                )
                tb_lines = safe_tb.splitlines()

                logger.critical("Traceback summary:")
                for line in tb_lines[:3]:
                    logger.critical(line)
                logger.critical("...")
                for line in tb_lines[-3:]:
                    logger.critical(line)
            except Exception as nested_error:
                # If even our error handling fails, log a simpler message
                logger.critical(f"Critical error in get_data: {type(e).__name__}")
                logger.critical(
                    f"Error handling also failed: {type(nested_error).__name__}"
                )

            # Re-raise the exception to properly exit with error rather than returning an empty DataFrame
            if "All data sources failed" in str(e):
                raise
            else:
                raise RuntimeError(
                    f"Failed to retrieve data from all sources: {safe_error_message}"
                )

    def _merge_adjacent_ranges(
        self, ranges: List[Tuple[datetime, datetime]], interval: Interval
    ) -> List[Tuple[datetime, datetime]]:
        """Merge adjacent or overlapping time ranges to minimize API calls.

        Args:
            ranges: List of (start, end) tuples representing time ranges
            interval: Time interval to determine adjacency threshold

        Returns:
            List of merged (start, end) tuples
        """
        if not ranges:
            return []

        # Sort ranges by start time
        sorted_ranges = sorted(ranges, key=lambda x: x[0])

        # Determine the threshold for what's considered "adjacent"
        # (allow for a small gap, typically 1-2x the interval)
        adjacency_threshold = timedelta(seconds=interval.to_seconds() * 2)

        merged = []
        current_start, current_end = sorted_ranges[0]

        for next_start, next_end in sorted_ranges[1:]:
            # If ranges overlap or are adjacent, extend the current range
            if next_start <= current_end + adjacency_threshold:
                current_end = max(current_end, next_end)
            else:
                # Otherwise, add the current range and start a new one
                merged.append((current_start, current_end))
                current_start, current_end = next_start, next_end

        # Add the final range
        merged.append((current_start, current_end))

        return merged

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

    def _identify_missing_segments(
        self,
        df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
    ) -> List[Tuple[datetime, datetime]]:
        """Identify missing segments in the data using gap_detector."""
        logger.debug(
            f"[FCP] Identifying missing segments between {start_time} and {end_time}"
        )
        if df.empty:
            logger.debug("[FCP] DataFrame is empty, entire range is missing")
            return [(start_time, end_time)]

        df = ensure_open_time_as_column(df)
        if not pd.api.types.is_datetime64_any_dtype(df["open_time"]):
            logger.warning("[FCP] open_time not datetime, converting...")
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df = df.sort_values("open_time")

        min_time, max_time = df["open_time"].min(), df["open_time"].max()
        logger.debug(f"[FCP] Data spans from {min_time} to {max_time}")

        from utils.gap_detector import detect_gaps

        gaps, stats = detect_gaps(
            df=df,
            interval=interval,
            time_column="open_time",
            gap_threshold=0.1,
            day_boundary_threshold=1.0,
            enforce_min_span=False,
        )
        logger.debug(f"[FCP] Gap detector found {stats['total_gaps']} gaps")

        missing_segments: List[Tuple[datetime, datetime]] = []
        for gap in gaps:
            start = gap.start_time + timedelta(seconds=interval.to_seconds())
            end = gap.end_time
            missing_segments.append((start, end))

        if min_time > start_time:
            missing_segments.insert(0, (start_time, min_time))
        if max_time < end_time:
            boundary_end = max_time + timedelta(seconds=interval.to_seconds())
            if boundary_end < end_time:
                missing_segments.append((boundary_end, end_time))

        if missing_segments:
            missing_segments = self._merge_adjacent_ranges(missing_segments, interval)
        logger.debug(f"[FCP] Final missing segments count: {len(missing_segments)}")
        return missing_segments
