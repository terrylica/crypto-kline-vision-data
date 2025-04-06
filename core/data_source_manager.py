#!/usr/bin/env python
"""Data source manager that mediates between different data sources."""

from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple
from enum import Enum, auto
import pandas as pd
from pathlib import Path
import asyncio
import gc
import os

from utils.logger_setup import logger
from utils.market_constraints import Interval, MarketType, ChartType, DataProvider
from utils.time_utils import (
    filter_dataframe_by_time,
    align_time_boundaries,
)
from utils.validation import DataFrameValidator, DataValidation
from utils.async_cleanup import direct_resource_cleanup
from utils.config import (
    OUTPUT_DTYPES,
    FUNDING_RATE_DTYPES,
    VISION_DATA_DELAY_HOURS,
    REST_CHUNK_SIZE,
    REST_MAX_CHUNKS,
    API_TIMEOUT,
    standardize_column_names,
    create_empty_dataframe,
    create_empty_funding_rate_dataframe,
)
from core.rest_data_client import RestDataClient
from core.vision_data_client import VisionDataClient
from core.binance_funding_rate_client import BinanceFundingRateClient
from core.cache_manager import UnifiedCacheManager
from core.data_client_factory import DataClientFactory
from core.data_client_interface import DataClientInterface


class DataSource(Enum):
    """Enum for data source selection."""

    AUTO = auto()  # Automatically select best source
    REST = auto()  # Force REST API
    VISION = auto()  # Force Vision API


class DataSourceManager:
    """Mediator between data sources with smart selection and caching.

    This class serves as the central point for:
    1. Data source selection between different providers and APIs
    2. Unified caching strategy across all data sources
    3. Cache integrity validation and management
    4. Data format standardization
    """

    # Vision API constraints - using imported constant
    VISION_DATA_DELAY_HOURS = VISION_DATA_DELAY_HOURS

    # REST API constraints - using imported constants
    REST_CHUNK_SIZE = REST_CHUNK_SIZE
    REST_MAX_CHUNKS = REST_MAX_CHUNKS

    # Output format specification from centralized config
    OUTPUT_DTYPES = OUTPUT_DTYPES.copy()
    FUNDING_RATE_DTYPES = FUNDING_RATE_DTYPES.copy()

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

    def __init__(
        self,
        market_type: MarketType = MarketType.SPOT,
        provider: DataProvider = DataProvider.BINANCE,
        chart_type: ChartType = ChartType.KLINES,
        rest_client: Optional[RestDataClient] = None,
        cache_dir: Optional[Path] = None,
        use_cache: bool = True,
        max_concurrent: int = 50,
        retry_count: int = 5,
        max_concurrent_downloads: Optional[int] = None,
        vision_client: Optional[VisionDataClient] = None,
        cache_expires_minutes: int = 60,
    ):
        """Initialize the data source manager.

        Args:
            market_type: Type of market (SPOT, FUTURES_USDT, FUTURES_COIN, etc.)
            provider: Data provider (BINANCE, BYBIT, etc.)
            chart_type: Type of chart data (KLINES, FUNDING_RATE, etc.)
            rest_client: Optional pre-configured REST client (for backward compatibility)
            cache_dir: Directory for caching data
            use_cache: Whether to use caching
            max_concurrent: Maximum concurrent API requests for REST client (default: 50)
            retry_count: Number of retries for failed REST API requests (default: 5)
            max_concurrent_downloads: Maximum concurrent downloads for Vision API (default: None, uses client default)
            vision_client: Optional existing VisionDataClient
            cache_expires_minutes: Cache expiration time in minutes
        """
        if cache_dir is None:
            from utils.config import DEFAULT_CACHE_DIR

            cache_dir = os.path.join(os.getcwd(), DEFAULT_CACHE_DIR)

        # Set client attributes and track if they're external
        self.vision_client = vision_client
        self.rest_client = rest_client
        self._vision_client_is_external = vision_client is not None
        self._rest_client_is_external = rest_client is not None
        self._vision_client_initialized = vision_client is not None

        # Initialize internal state
        self._data_client = None
        self._data_client_initialized = False

        # Store performance tuning parameters
        self.max_concurrent = max_concurrent
        self.retry_count = retry_count
        self.max_concurrent_downloads = max_concurrent_downloads

        # Store market configuration
        self.market_type = market_type
        self.provider = provider
        self.chart_type = chart_type

        # Convert market_type to string for Vision API if needed
        self.market_type_str = self._get_market_type_str(market_type)

        # Initialize cache manager if caching is enabled
        self.use_cache = use_cache and cache_dir is not None
        self.cache_manager = (
            UnifiedCacheManager(cache_dir)
            if (use_cache and cache_dir is not None)
            else None
        )
        self._cache_stats = {"hits": 0, "misses": 0, "errors": 0}

        # Register client implementations with factory
        self._register_client_implementations()

    def _get_market_type_str(self, market_type: MarketType) -> str:
        """Convert MarketType enum to string representation for Vision API.

        Args:
            market_type: MarketType enum value

        Returns:
            String representation for Vision API
        """
        if market_type.name == MarketType.SPOT.name:
            return "spot"
        elif market_type.name == MarketType.FUTURES_USDT.name:
            return "futures_usdt"
        elif market_type.name == MarketType.FUTURES_COIN.name:
            return "futures_coin"
        elif market_type.name == MarketType.FUTURES.name:
            return "futures_usdt"  # Default to USDT-margined for legacy type
        else:
            raise ValueError(f"Unsupported market type: {market_type}")

    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache performance statistics.

        Returns:
            Dictionary containing cache hits, misses, and errors
        """
        return self._cache_stats.copy()

    async def validate_cache_integrity(
        self, symbol: str, interval: str, date: datetime
    ) -> Tuple[bool, Optional[str]]:
        """Validate cache integrity for a specific data point.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self.cache_manager:
            return False, "Cache manager not initialized"

        try:
            # Check if cache exists first
            cache_key = self.cache_manager.get_cache_key(symbol, interval, date)
            if cache_key not in self.cache_manager.metadata:
                return False, "Cache miss"

            # Load data and verify format
            df = await self.cache_manager.load_from_cache(symbol, interval, date)
            if df is None:
                return False, "Failed to load cache data"

            # Validate data structure using our centralized validator
            try:
                DataFrameValidator.validate_dataframe(df)
                return True, None
            except ValueError as e:
                return False, str(e)

        except Exception as e:
            return False, f"Validation error: {str(e)}"

    async def repair_cache(self, symbol: str, interval: str, date: datetime) -> bool:
        """Attempt to repair corrupted cache entry.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            True if repair successful, False otherwise
        """
        if not self.cache_manager:
            return False

        try:
            # Invalidate corrupted entry
            self.cache_manager.invalidate_cache(symbol, interval, date)

            # Refetch and cache data
            df = await self._fetch_from_source(
                symbol, date, date + timedelta(days=1), Interval(interval)
            )
            if df.empty:
                return False

            # Validate data before caching
            try:
                DataFrameValidator.validate_dataframe(df)
            except ValueError as e:
                logger.error(f"Cannot repair cache with invalid data: {e}")
                return False

            await self.cache_manager.save_to_cache(df, symbol, interval, date)

            # Verify the repair was successful
            is_valid, error = await self.validate_cache_integrity(
                symbol, interval, date
            )
            if not is_valid:
                logger.error(f"Cache repair verification failed: {error}")
                return False

            return True

        except Exception as e:
            logger.error(f"Cache repair failed: {e}")
            return False

    def _estimate_data_points(
        self, start_time: datetime, end_time: datetime, interval: Interval
    ) -> int:
        """Estimate number of data points for a time range.

        Args:
            start_time: Start time
            end_time: End time
            interval: Time interval

        Returns:
            Estimated number of data points
        """
        time_diff = end_time - start_time
        interval_seconds = interval.to_seconds()
        return int(time_diff.total_seconds()) // interval_seconds

    def _should_use_vision_api(
        self, start_time: datetime, end_time: datetime, interval: Interval
    ) -> bool:
        """Determine if Vision API should be used based on time range and interval.

        Args:
            start_time: Start time
            end_time: End time
            interval: Time interval

        Returns:
            True if Vision API should be used, False for REST API
        """
        # Compare enum names rather than objects to avoid issues in parallel testing
        # where enum objects might be different instances due to module reloading

        # Use REST API for small intervals like 1s that Vision doesn't support
        if interval.name == Interval.SECOND_1.name:
            logger.debug("Using REST API for 1s data (Vision API doesn't support it)")
            return False

        # Always use Vision for large time ranges to avoid multiple chunked API calls
        time_range = end_time - start_time
        data_points = self._estimate_data_points(start_time, end_time, interval)

        if data_points > self.REST_CHUNK_SIZE * self.REST_MAX_CHUNKS:
            logger.debug(
                f"Using Vision API due to large data request ({data_points} points, "
                f"exceeding REST max of {self.REST_CHUNK_SIZE * self.REST_MAX_CHUNKS})"
            )
            return True

        # Use Vision API for historical data beyond the delay threshold
        # Ensure consistent timezone for comparison
        now = datetime.now(timezone.utc)
        vision_threshold = now - timedelta(hours=self.VISION_DATA_DELAY_HOURS)

        if end_time < vision_threshold:
            logger.debug(
                f"Using Vision API for historical data older than {self.VISION_DATA_DELAY_HOURS} hours"
            )
            return True

        # Default to REST API for recent data
        logger.debug(
            f"Using REST API for recent data within {self.VISION_DATA_DELAY_HOURS} hours"
        )
        return False

    def _format_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format DataFrame to ensure consistent structure and data types.

        Args:
            df: DataFrame to format

        Returns:
            Formatted DataFrame
        """
        logger.debug(
            f"Formatting DataFrame with shape: {df.shape if not df.empty else 'empty'}"
        )
        logger.debug(
            f"Columns before formatting: {list(df.columns) if not df.empty else 'none'}"
        )
        logger.debug(
            f"Index type before formatting: {type(df.index) if not df.empty else 'none'}"
        )

        # Note: Vision API data no longer needs column name standardization since
        # it now uses KLINE_COLUMNS directly during parsing.
        # However, we still run standardize_column_names for any other potential data sources
        # and to maintain backward compatibility with third-party APIs.
        df = standardize_column_names(df)

        # Then use the centralized formatter
        return DataFrameValidator.format_dataframe(df, self.OUTPUT_DTYPES)

    async def _fetch_from_source(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        use_vision: bool = False,
    ) -> pd.DataFrame:
        """Fetch data from appropriate source based on parameters.

        Args:
            symbol: Trading pair symbol
            start_time: Start time
            end_time: End time
            interval: Time interval
            use_vision: Whether to try Vision API first (with REST API fallback)

        Returns:
            DataFrame with market data
        """
        # Initialize with empty DataFrame in case of errors
        result_df = self.create_empty_dataframe()

        try:
            # For non-klines chart types, use the appropriate data client
            if self.chart_type != ChartType.KLINES:
                client = await self._get_data_client(symbol, interval)

                # Fetch data using the client with proper timeout handling
                try:
                    # Use the standard API timeout from config, not arbitrary values
                    result_df = await asyncio.wait_for(
                        client.fetch(start_time, end_time), timeout=API_TIMEOUT
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        f"Timeout after {API_TIMEOUT}s while fetching data for {symbol}"
                    )
                    return self.create_empty_dataframe()

                # Validate the data
                if not result_df.empty:
                    try:
                        if self.chart_type == ChartType.FUNDING_RATE:
                            is_valid, error = await client.validate_data(result_df)
                            if not is_valid:
                                logger.error(f"Invalid funding rate data: {error}")
                                return self.create_empty_dataframe()
                        else:
                            DataFrameValidator.validate_dataframe(result_df)
                    except ValueError as e:
                        logger.error(f"Data validation error: {e}")
                        return self.create_empty_dataframe()

                return result_df

            # For KLINES, we still use the legacy code path with REST/Vision
            if use_vision:
                try:
                    # Get aligned boundaries once and reuse them
                    vision_start, vision_end = align_time_boundaries(
                        start_time, end_time, interval
                    )

                    logger.info(
                        f"Using Vision API with aligned boundaries: {vision_start} -> {vision_end}"
                    )

                    # Create Vision client if not exists
                    self._ensure_vision_client(symbol, interval.value)

                    # Fetch from Vision API with aligned boundaries and proper timeout
                    try:
                        # Use the standard API timeout from config, not arbitrary values
                        vision_df = await asyncio.wait_for(
                            self.vision_client.fetch(vision_start, vision_end),
                            timeout=API_TIMEOUT
                            * 2,  # Vision API might take longer for downloads
                        )
                    except asyncio.TimeoutError:
                        logger.error(
                            f"Vision API timeout after {API_TIMEOUT * 2}s, falling back to REST API"
                        )
                        use_vision = False  # Fall back to REST API
                        # Continue to REST API fallback below
                    else:
                        # Check if we got valid data
                        if not vision_df.empty:
                            # Filter result to exact requested time range if needed
                            result_df = filter_dataframe_by_time(
                                vision_df, start_time, end_time
                            )

                            # If we have data, return it
                            if not result_df.empty:
                                logger.info(
                                    f"Successfully retrieved {len(result_df)} records from Vision API"
                                )
                                return result_df

                        # If we get here, Vision API failed or returned empty results
                        logger.info(
                            "Vision API returned no data, falling back to REST API"
                        )

                except Exception as e:
                    logger.warning(f"Vision API error, falling back to REST API: {e}")

            # Fall back to REST API (or use it directly if use_vision=False)
            try:
                logger.info(
                    f"Using REST API with original boundaries: {start_time} -> {end_time}"
                )

                # Ensure REST client is initialized
                if not self.rest_client:
                    logger.debug("Initializing REST client for fetch operation")
                    self.rest_client = RestDataClient(
                        market_type=self.market_type,
                        max_concurrent=self.max_concurrent,
                        retry_count=self.retry_count,
                    )

                # Fetch from REST API with proper timeout handling
                try:
                    # Use the standard API timeout from config, not arbitrary values
                    rest_result = await asyncio.wait_for(
                        self.rest_client.fetch(symbol, interval, start_time, end_time),
                        timeout=API_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        f"REST API timeout after {API_TIMEOUT}s while fetching data for {symbol}"
                    )
                    return self.create_empty_dataframe()

                # Unpack the tuple - RestDataClient.fetch returns (df, stats)
                rest_df, stats = rest_result

                if not rest_df.empty:
                    logger.info(
                        f"Successfully retrieved {len(rest_df)} records from REST API"
                    )
                    # Validate the DataFrame
                    DataFrameValidator.validate_dataframe(rest_df)
                    return rest_df

                logger.warning(
                    f"REST API returned no data for {symbol} from {start_time} to {end_time}"
                )

            except Exception as e:
                logger.error(f"REST API fetch error: {e}")

        except Exception as e:
            logger.error(f"Error fetching data: {e}")

        # If we reach here, all sources failed or returned empty results
        logger.warning(
            f"No data returned for {symbol} from {start_time} to {end_time} from any source"
        )
        return self.create_empty_dataframe()

    def _get_aligned_cache_date(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        use_vision: bool,
    ) -> datetime:
        """Get aligned date for cache operations that's consistent across REST and Vision APIs.

        Args:
            start_time: Original start time
            end_time: Original end time
            interval: Time interval
            use_vision: Whether Vision API is being used

        Returns:
            Aligned date for cache operations
        """
        if use_vision:
            # For Vision API, get aligned start time
            aligned_start, _ = align_time_boundaries(start_time, end_time, interval)
            return aligned_start
        else:
            # For REST API, use original start time - the REST client will handle alignment
            return start_time

    async def get_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval = Interval.SECOND_1,
        use_cache: bool = True,
        enforce_source: DataSource = DataSource.AUTO,
        provider: Optional[DataProvider] = None,
        chart_type: Optional[ChartType] = None,
    ) -> pd.DataFrame:
        """Get data for symbol within time range, with smart source selection.

        Args:
            symbol: Trading pair symbol
            start_time: Start time
            end_time: End time
            interval: Time interval
            use_cache: Whether to use cache
            enforce_source: Force specific data source
            provider: Optional override for data provider
            chart_type: Optional override for chart type

        Returns:
            DataFrame with market data
        """
        # Override provider and chart_type if specified
        original_provider = self.provider
        original_chart_type = self.chart_type

        if provider is not None:
            self.provider = provider

        if chart_type is not None:
            self.chart_type = chart_type

        try:
            # Standardize input parameters
            symbol = symbol.upper()

            # Apply comprehensive time boundary validation
            start_time, end_time, metadata = (
                DataValidation.validate_query_time_boundaries(
                    start_time, end_time, handle_future_dates="error"
                )
            )

            # Log any warnings from validation
            for warning in metadata.get("warnings", []):
                logger.warning(warning)

            # Log input parameters
            logger.info(
                f"Getting {self.chart_type.value} data for {symbol} from {start_time} to {end_time} "
                f"with interval {interval.value}, provider={self.provider.name}"
            )

            # Determine data source to use (only applies to KLINES)
            use_vision = self._determine_data_source(
                start_time, end_time, interval, enforce_source
            )

            # Check if we can use cache
            is_valid = use_cache and self.cache_manager
            is_cache_hit = False

            # Cache key components
            cache_components = {
                "symbol": symbol,
                "interval": interval.value,
                "provider": self.provider.name,
                "chart_type": self.chart_type.name,
            }

            try:
                # Attempt to load from cache if enabled
                if is_valid:
                    # Get the aligned cache date
                    cache_date = self._get_aligned_cache_date(
                        start_time, end_time, interval, use_vision
                    )

                    cached_data = await self.cache_manager.load_from_cache(
                        date=cache_date, **cache_components
                    )

                    if cached_data is not None:
                        # Filter DataFrame based on original requested time range
                        # Use inclusive start, inclusive end consistent with API behavior
                        filtered_data = filter_dataframe_by_time(
                            cached_data, start_time, end_time
                        )

                        if not filtered_data.empty:
                            self._cache_stats["hits"] += 1
                            logger.info(
                                f"Cache hit for {symbol} {self.chart_type.name} from {start_time}"
                            )
                            return filtered_data

                        logger.info(
                            "Cache hit, but filtered data is empty. Fetching from source."
                        )
                    else:
                        logger.info(
                            f"Cache miss for {symbol} {self.chart_type.name} from {start_time}"
                        )

                    self._cache_stats["misses"] += 1

            except Exception as e:
                logger.error(f"Cache error: {e}")
                self._cache_stats["errors"] += 1
                # Continue with fetching from source

            # Fetch data from appropriate source
            df = await self._fetch_from_source(
                symbol, start_time, end_time, interval, use_vision
            )

            # Cache if enabled and data is not empty
            if is_valid and not df.empty and self.cache_manager:
                try:
                    # Get the aligned cache date
                    cache_date = self._get_aligned_cache_date(
                        start_time, end_time, interval, use_vision
                    )

                    await self.cache_manager.save_to_cache(
                        df=df, date=cache_date, **cache_components
                    )
                    logger.info(
                        f"Cached {len(df)} records for {symbol} {self.chart_type.name}"
                    )
                except Exception as e:
                    logger.error(f"Error caching data: {e}")

            return df

        finally:
            # Restore original values
            self.provider = original_provider
            self.chart_type = original_chart_type

    def _determine_data_source(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        enforce_source: DataSource,
    ) -> bool:
        """Determine which data source to use based on parameters and preferences.

        Args:
            start_time: Start time
            end_time: End time
            interval: Time interval
            enforce_source: User-enforced data source preference

        Returns:
            True if Vision API should be used, False for REST API
        """
        # Handle user-enforced source selection
        if enforce_source == DataSource.VISION:
            logger.info("Using Vision API (enforced)")
            return True
        elif enforce_source == DataSource.REST:
            logger.info("Using REST API (enforced)")
            return False

        # AUTO: Apply smart selection logic
        use_vision = self._should_use_vision_api(start_time, end_time, interval)
        logger.info(
            f"Auto-selected source: {'Vision API' if use_vision else 'REST API'}"
        )
        return use_vision

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Python 3.13 compatible direct cleanup without background tasks.

        This implementation guarantees immediate resource release without relying
        on background tasks or event loop scheduling, preventing hanging issues.
        """
        # Use the centralized direct cleanup utility
        await direct_resource_cleanup(
            self,
            ("_data_client", "data client", False),
            ("vision_client", "vision client", self._vision_client_is_external),
            ("rest_client", "REST client", self._rest_client_is_external),
        )

    def _ensure_vision_client(self, symbol: str, interval: str) -> None:
        """Ensure a VisionDataClient is available for the current operation.

        Lazily initializes the VisionDataClient when needed.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
        """
        # Handle symbol formatting for FUTURES_COIN market type
        if self.market_type == MarketType.FUTURES_COIN and "_PERP" not in symbol:
            # Append _PERP suffix for coin-margined futures
            symbol = f"{symbol}_PERP"
            logger.debug(f"Adjusted symbol for FUTURES_COIN market: {symbol}")

        if not self._vision_client_initialized:
            logger.debug(
                f"Initializing VisionDataClient for {symbol} with interval {interval}"
            )
            self.vision_client = VisionDataClient(
                symbol=symbol,
                interval=interval,
                market_type=self.market_type,
                max_concurrent_downloads=self.max_concurrent_downloads,
            )
            self._vision_client_initialized = True
        elif (
            self.vision_client.symbol != symbol
            or self.vision_client.interval != interval
        ):
            # If symbol or interval doesn't match, reinitialize
            logger.debug(
                f"Reinitializing VisionDataClient for {symbol} with interval {interval}"
            )
            # Clean up the old client first
            try:
                old_client = self.vision_client
                self.vision_client = VisionDataClient(
                    symbol=symbol,
                    interval=interval,
                    market_type=self.market_type,
                    max_concurrent_downloads=self.max_concurrent_downloads,
                )
                # Properly close the old client
                asyncio.create_task(old_client.__aexit__(None, None, None))
            except Exception as e:
                logger.warning(f"Error while reinitializing VisionDataClient: {e}")
                # Create a new client anyway
                self.vision_client = VisionDataClient(
                    symbol=symbol,
                    interval=interval,
                    market_type=self.market_type,
                    max_concurrent_downloads=self.max_concurrent_downloads,
                )

            self._vision_client_initialized = True

    def _register_client_implementations(self):
        """Register all client implementations with the factory."""
        try:
            # Register BinanceFundingRateClient for funding rate data
            DataClientFactory.register_client(
                provider=DataProvider.BINANCE,
                market_type=MarketType.FUTURES_USDT,
                chart_type=ChartType.FUNDING_RATE,
                client_class=BinanceFundingRateClient,
            )

            DataClientFactory.register_client(
                provider=DataProvider.BINANCE,
                market_type=MarketType.FUTURES_COIN,
                chart_type=ChartType.FUNDING_RATE,
                client_class=BinanceFundingRateClient,
            )

            logger.debug("Registered client implementations with factory")
        except Exception as e:
            logger.error(f"Failed to register client implementations: {e}")

    async def _get_data_client(
        self, symbol: str, interval: Interval
    ) -> DataClientInterface:
        """Get the appropriate data client for the configured parameters.

        This method is part of the transition to the new architecture. It will create
        a client from the factory if the chart type is supported, or fall back to the
        legacy clients for backward compatibility.

        Args:
            symbol: Trading pair symbol
            interval: Time interval

        Returns:
            DataClientInterface implementation
        """
        # Try to get a client from the factory for non-klines data
        if self.chart_type != ChartType.KLINES:
            try:
                if (
                    not self._data_client_initialized
                    or (
                        hasattr(self._data_client, "symbol")
                        and self._data_client.symbol != symbol
                    )
                    or (
                        hasattr(self._data_client, "interval")
                        and self._data_client.interval != interval
                    )
                ):
                    # Create a new client
                    self._data_client = DataClientFactory.create_data_client(
                        provider=self.provider,
                        market_type=self.market_type,
                        chart_type=self.chart_type,
                        symbol=symbol,
                        interval=interval,
                        max_concurrent=self.max_concurrent,
                        retry_count=self.retry_count,
                        max_concurrent_downloads=self.max_concurrent_downloads,
                        use_cache=False,  # We use our own caching
                    )
                    self._data_client_initialized = True

                return self._data_client
            except Exception as e:
                logger.error(f"Failed to create data client from factory: {e}")
                # Fall back to legacy clients

        # For KLINES, we still use the legacy clients
        # Initialize REST client if needed
        if not self.rest_client:
            self.rest_client = RestDataClient(
                market_type=self.market_type,
                max_concurrent=self.max_concurrent,
                retry_count=self.retry_count,
            )

        return self.rest_client

    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct structure for the configured chart type.

        Returns:
            Empty DataFrame with correct columns and types
        """
        if self.chart_type == ChartType.FUNDING_RATE:
            return create_empty_funding_rate_dataframe()
        return create_empty_dataframe()
