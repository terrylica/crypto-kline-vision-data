#!/usr/bin/env python
"""Data Source Manager (DSM) that mediates between different data sources."""

from datetime import datetime
from typing import Dict, Optional, Tuple, TypeVar, Type, List
from enum import Enum, auto
import pandas as pd
from pathlib import Path
from dataclasses import dataclass

from utils.logger_setup import logger
from utils.market_constraints import Interval, MarketType, ChartType, DataProvider
from utils.market_utils import get_market_type_str
from utils.time_utils import align_time_boundaries
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
from utils.for_core.dsm_time_range_utils import (
    merge_adjacent_ranges,
    standardize_columns,
    identify_missing_segments,
    merge_dataframes,
)
from utils.for_core.dsm_cache_utils import (
    save_to_cache,
    get_from_cache,
)
from utils.for_core.dsm_api_utils import (
    fetch_from_vision,
    fetch_from_rest,
    create_client_if_needed,
)
from utils.for_core.dsm_date_range_utils import (
    calculate_date_range,
    get_date_range_description,
)


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
    """

    # Mandatory parameters
    market_type: MarketType
    provider: DataProvider

    # Optional parameters with defaults
    chart_type: ChartType = ChartType.KLINES
    cache_dir: Optional[Path] = None
    use_cache: bool = True
    retry_count: int = 5

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
        cls: Type[T], provider: DataProvider, market_type: MarketType, **kwargs
    ) -> T:
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
            raise TypeError(
                f"provider must be a DataProvider enum, got {type(provider)}"
            )
        if not isinstance(market_type, MarketType):
            raise TypeError(
                f"market_type must be a MarketType enum, got {type(market_type)}"
            )
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
    def calculate_time_range(
        cls, start_time=None, end_time=None, days=3, interval=Interval.MINUTE_1
    ) -> Tuple[datetime, datetime]:
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
        start_datetime, end_datetime = calculate_date_range(
            start_time=start_time, end_time=end_time, days=days, interval=interval
        )

        # Get description for logging
        description = get_date_range_description(
            start_datetime,
            end_datetime,
            {"start_time": start_time, "end_time": end_time, "days": days},
        )
        logger.info(description)

        return start_datetime, end_datetime

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
            cache_dir: Directory to store cache files (default: "./cache")
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

    def _get_from_cache(
        self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval
    ) -> Tuple[pd.DataFrame, List[Tuple[datetime, datetime]]]:
        """Retrieve data from cache and identify missing ranges using the utility function."""
        if not self.use_cache:
            logger.debug(
                f"Cache disabled. Returning entire range as missing: {start_time} to {end_time}"
            )
            return create_empty_dataframe(), [(start_time, end_time)]

        return get_from_cache(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            cache_manager=self.cache_manager,
            provider=self.provider.name,
            chart_type=self.chart_type.name,
            market_type=self._get_market_type_str(),
        )

    def _save_to_cache(
        self, df: pd.DataFrame, symbol: str, interval: Interval, source: str = None
    ) -> None:
        """Save data to cache using the utility function."""
        if not self.use_cache:
            return

        save_to_cache(
            df=df,
            symbol=symbol,
            interval=interval,
            cache_manager=self.cache_manager,
            provider=self.provider.name,
            chart_type=self.chart_type.name,
            market_type=self._get_market_type_str(),
            source=source,
        )

    def _fetch_from_vision(
        self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval
    ) -> pd.DataFrame:
        """Fetch data from the Vision API using the utility function."""
        # Create or reconfigure Vision client if needed
        self.vision_client = create_client_if_needed(
            client=self.vision_client,
            client_class=VisionDataClient,
            symbol=symbol,
            interval=interval.value,
            market_type=self.market_type,
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
        )

    def _fetch_from_rest(
        self, symbol: str, start_time: datetime, end_time: datetime, interval: Interval
    ) -> pd.DataFrame:
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
                vision_ranges_to_fetch = (
                    missing_ranges.copy()
                )  # All ranges will be processed by Vision API

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
                                result_df = merge_dataframes([result_df, range_df])
                            else:
                                # Otherwise just use the Vision data
                                result_df = range_df

                            # Save to cache if enabled (removed as it's now handled in _fetch_from_vision)
                            # Note: Full day's data is now cached directly in _fetch_from_vision

                            # Check if Vision API returned all expected records or if there are gaps
                            if not result_df.empty:
                                # Identify any remaining missing segments from Vision API
                                missing_segments = identify_missing_segments(
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
                        missing_ranges = merge_adjacent_ranges(
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
                merged_rest_ranges = merge_adjacent_ranges(missing_ranges, interval)

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
                            result_df = merge_dataframes([result_df, rest_df])
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
            result_df = standardize_columns(result_df)

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
