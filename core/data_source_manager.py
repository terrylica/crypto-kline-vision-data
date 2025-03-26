#!/usr/bin/env python
"""Data source manager that mediates between REST and Vision API data sources."""

from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple
from enum import Enum, auto
import pandas as pd
from pathlib import Path

from utils.logger_setup import get_logger
from utils.market_constraints import Interval, MarketType
from utils.time_alignment import TimeRangeManager
from utils.validation import DataFrameValidator
from utils.config import (
    OUTPUT_DTYPES,
    VISION_DATA_DELAY_HOURS,
    REST_CHUNK_SIZE,
    REST_MAX_CHUNKS,
    standardize_column_names,
)
from core.market_data_client import EnhancedRetriever
from core.vision_data_client import VisionDataClient
from core.cache_manager import UnifiedCacheManager

logger = get_logger(__name__, "INFO", show_path=False)


class DataSource(Enum):
    """Enum for data source selection."""

    AUTO = auto()  # Automatically select best source
    REST = auto()  # Force REST API
    VISION = auto()  # Force Vision API


class DataSourceManager:
    """Mediator between REST and Vision API data sources with smart selection and caching.

    This class serves as the central point for:
    1. Data source selection between REST and Vision APIs
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

    @classmethod
    def get_output_format(cls) -> Dict[str, str]:
        """Get the standardized output format specification.

        Returns:
            Dictionary mapping column names to their dtypes

        Note:
            - Index is always pd.DatetimeIndex in UTC timezone
            - All timestamps are aligned to interval boundaries
            - Empty DataFrames maintain this structure
            - Both REST and Vision API data are normalized to this format
        """
        return cls.OUTPUT_DTYPES.copy()

    def __init__(
        self,
        market_type: MarketType = MarketType.SPOT,
        rest_client: Optional[EnhancedRetriever] = None,
        vision_client: Optional[VisionDataClient] = None,
        cache_dir: Optional[Path] = None,
        use_cache: bool = True,
    ):
        """Initialize the data source manager.

        Args:
            market_type: Type of market (SPOT, FUTURES, etc.)
            rest_client: Optional pre-configured REST client
            vision_client: Optional pre-configured Vision client
            cache_dir: Directory for caching data
            use_cache: Whether to use caching
        """
        self.market_type = market_type
        self.rest_client = rest_client or EnhancedRetriever(market_type=market_type)

        # Store original vision client cache settings and disable its caching
        self._vision_original_cache = None
        if vision_client:
            self._vision_original_cache = {
                "dir": vision_client.cache_dir,
                "use_cache": vision_client.use_cache,
            }
            vision_client.use_cache = False
            vision_client.cache_dir = None
        self.vision_client = vision_client

        # Initialize cache manager if caching is enabled
        self.use_cache = use_cache and cache_dir is not None
        self.cache_manager = (
            UnifiedCacheManager(cache_dir)
            if (use_cache and cache_dir is not None)
            else None
        )
        self._cache_stats = {"hits": 0, "misses": 0, "errors": 0}

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
        if interval == Interval.SECOND_1:
            return int(time_diff.total_seconds())
        elif interval == Interval.MINUTE_1:
            return int(time_diff.total_seconds()) // 60
        else:
            raise ValueError(f"Unsupported interval: {interval}")

    def _should_use_vision_api(
        self, start_time: datetime, end_time: datetime, interval: Interval
    ) -> bool:
        """Determine if Vision API should be used based on request parameters.

        Args:
            start_time: Start time of data request
            end_time: End time of data request
            interval: Time interval

        Returns:
            True if Vision API should be used, False otherwise
        """
        now = datetime.now(timezone.utc)
        estimated_points = self._estimate_data_points(start_time, end_time, interval)

        # Log decision factors
        logger.info(f"Data source selection factors:")
        logger.info(f"- Current time: {now}")
        logger.info(f"- Request time range: {start_time} to {end_time}")
        logger.info(f"- Estimated data points: {estimated_points}")

        # Rule 1: Always use Vision API for large requests
        if estimated_points > self.REST_CHUNK_SIZE * self.REST_MAX_CHUNKS:
            logger.info("Using Vision API: Large data request")
            return True

        # Rule 2: Try Vision API first for all other cases
        logger.info(
            "Using Vision API: Default choice (will fall back to REST if unavailable)"
        )
        return True

    def _format_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format DataFrame to ensure consistent structure.

        Args:
            df: Input DataFrame

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

        # First standardize column names
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
            use_vision: Whether to force using Vision API

        Returns:
            DataFrame with market data
        """
        # Initialize with empty DataFrame in case of errors
        result_df = pd.DataFrame()

        try:
            if use_vision:
                # For Vision API, we need to manually align timestamps to match REST API behavior
                aligned_boundaries = TimeRangeManager.align_vision_api_to_rest(
                    start_time, end_time, interval
                )
                vision_start = aligned_boundaries["adjusted_start"]
                vision_end = aligned_boundaries["adjusted_end"]

                logger.info(
                    f"Using Vision API with aligned boundaries: {vision_start} -> {vision_end} "
                    f"(to match REST API behavior)"
                )

                # Create Vision client if not exists
                if not self.vision_client:
                    raise ValueError("Vision client is not configured")

                # Fetch from Vision API with aligned boundaries
                result_df = await self.vision_client.fetch(vision_start, vision_end)

                # Filter result to exact requested time range if needed
                if not result_df.empty:
                    result_df = TimeRangeManager.filter_dataframe(
                        result_df, start_time, end_time
                    )
            else:
                # For REST API, use original timestamps - the API handles its own boundary alignment
                logger.info(
                    f"Using REST API with original boundaries: {start_time} -> {end_time}"
                )

                # Fetch from REST API
                result_df = await self.rest_client.fetch(
                    symbol, interval, start_time, end_time
                )

            # Validate and standardize the DataFrame
            if not result_df.empty:
                DataFrameValidator.validate_dataframe(result_df)
            else:
                # Return empty DataFrame with proper structure
                logger.warning(
                    f"No data returned for {symbol} from {start_time} to {end_time}"
                )
                result_df = self.rest_client.create_empty_dataframe()

            return result_df

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            # Return empty DataFrame with proper structure
            return self.rest_client.create_empty_dataframe()

    async def get_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval = Interval.SECOND_1,
        use_cache: bool = True,
        enforce_source: DataSource = DataSource.AUTO,
    ) -> pd.DataFrame:
        """Get data for symbol within time range, with smart source selection.

        Args:
            symbol: Trading pair symbol
            start_time: Start time
            end_time: End time
            interval: Time interval
            use_cache: Whether to use cache
            enforce_source: Force specific data source

        Returns:
            DataFrame with market data
        """
        # Validate inputs
        if not symbol:
            raise ValueError("Symbol must be provided")
        if not isinstance(interval, Interval):
            raise ValueError(f"Invalid interval: {interval}")

        # Standardize using utils
        symbol = symbol.upper()

        # Ensure timestamps are UTC timezone-aware
        start_time = TimeRangeManager.enforce_utc_timezone(start_time)
        end_time = TimeRangeManager.enforce_utc_timezone(end_time)

        # Log input parameters
        logger.info(
            f"Getting data for {symbol} from {start_time} to {end_time} "
            f"with interval {interval.value}"
        )

        # Determine data source to use
        use_vision = False

        if enforce_source == DataSource.VISION:
            use_vision = True
            logger.info("Using Vision API (enforced)")
        elif enforce_source == DataSource.REST:
            use_vision = False
            logger.info("Using REST API (enforced)")
        else:  # AUTO: smart selection
            use_vision = self._should_use_vision_api(start_time, end_time, interval)
            logger.info(
                f"Auto-selected source: {'Vision API' if use_vision else 'REST API'}"
            )

        # Check if we can use cache
        is_valid = use_cache and self.cache_manager
        is_cache_hit = False

        try:
            # Attempt to load from cache if enabled
            if is_valid:
                # For cache operations, we need to align dates to match REST API behavior
                # This ensures caching works consistently with both REST and Vision APIs
                if use_vision:
                    # For Vision API, use aligned timestamps for cache operations
                    aligned_boundaries = TimeRangeManager.align_vision_api_to_rest(
                        start_time, end_time, interval
                    )
                    cache_date = aligned_boundaries["adjusted_start"]
                else:
                    # For REST API, use original start date for cache lookup
                    # The REST API will handle its own boundary alignment
                    cache_date = start_time

                cached_data = await self.cache_manager.load_from_cache(
                    symbol=symbol, interval=interval.value, date=cache_date
                )

                if cached_data is not None:
                    # Filter DataFrame based on original requested time range
                    # Use inclusive start, exclusive end consistent with API behavior
                    filtered_data = TimeRangeManager.filter_dataframe(
                        cached_data, start_time, end_time
                    )

                    if not filtered_data.empty:
                        self._cache_stats["hits"] += 1
                        logger.info(f"Cache hit for {symbol} from {start_time}")
                        return filtered_data

                    logger.info(
                        "Cache hit, but filtered data is empty. Fetching from source."
                    )
                else:
                    logger.info(f"Cache miss for {symbol} from {start_time}")

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
                # For caching purposes, use properly aligned date that matches REST API behavior
                if use_vision:
                    # For Vision API, we need to manually align the date for caching
                    aligned_boundaries = TimeRangeManager.align_vision_api_to_rest(
                        start_time, end_time, interval
                    )
                    cache_date = aligned_boundaries["adjusted_start"]
                else:
                    # For REST API, use original start date - the API already handled alignment
                    cache_date = start_time

                await self.cache_manager.save_to_cache(
                    df, symbol, interval.value, cache_date
                )
                logger.info(f"Cached {len(df)} records for {symbol}")
            except Exception as e:
                logger.error(f"Error caching data: {e}")

        return df

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup resources and restore original cache settings."""
        try:
            if self.vision_client and self._vision_original_cache:
                # Restore original cache settings
                self.vision_client.cache_dir = self._vision_original_cache["dir"]
                self.vision_client.use_cache = self._vision_original_cache["use_cache"]
                logger.debug("Restored original vision client cache settings")
        except Exception as e:
            logger.warning(f"Failed to restore vision client cache settings: {e}")

        if self.rest_client:
            await self.rest_client.__aexit__(exc_type, exc_val, exc_tb)
