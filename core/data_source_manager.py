#!/usr/bin/env python
"""Data source manager that mediates between REST and Vision API data sources."""

from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple
from enum import Enum, auto
import pandas as pd
from pathlib import Path

from utils.logger_setup import get_logger
from utils.market_constraints import Interval, MarketType
from utils.time_alignment import adjust_time_window
from core.market_data_client import EnhancedRetriever
from core.vision_data_client import VisionDataClient
from core.cache_manager import UnifiedCacheManager

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


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

    # Vision API constraints
    VISION_DATA_DELAY_HOURS = 36  # Data newer than this isn't available in Vision API

    # REST API constraints
    REST_CHUNK_SIZE = 1000  # Maximum records per REST API request
    REST_MAX_CHUNKS = 10  # Maximum number of chunks to request via REST

    # Output format specification
    OUTPUT_DTYPES = {
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "close_time": "int64",
        "quote_volume": "float64",
        "trades": "int64",
        "taker_buy_volume": "float64",
        "taker_buy_quote_volume": "float64",
    }

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

            # Verify data structure
            if not all(col in df.columns for col in self.OUTPUT_DTYPES.keys()):
                return False, "Missing required columns"

            # Verify data types
            for col, dtype in self.OUTPUT_DTYPES.items():
                if str(df[col].dtype) != dtype:
                    return (
                        False,
                        f"Invalid dtype for {col}: expected {dtype}, got {df[col].dtype}",
                    )

            # Verify index
            if not isinstance(df.index, pd.DatetimeIndex):
                return False, "Index is not DatetimeIndex"
            if df.index.tz != timezone.utc:
                return False, "Index timezone is not UTC"

            return True, None

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

            await self.cache_manager.save_to_cache(df, symbol, interval, date)
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

    def _validate_dates(self, start_time: datetime, end_time: datetime) -> None:
        """Validate date ranges for data retrieval.

        Args:
            start_time: Start time
            end_time: End time

        Raises:
            ValueError: If dates are invalid
        """
        now = datetime.now(timezone.utc)
        if end_time > now:
            raise ValueError(f"End time {end_time} is in the future")
        if start_time > end_time:
            raise ValueError(f"Start time {start_time} is after end time {end_time}")

    def _format_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Format DataFrame to ensure consistent structure.

        Args:
            df: Input DataFrame

        Returns:
            Formatted DataFrame
        """
        if df.empty:
            # Create empty DataFrame with correct structure
            df = pd.DataFrame(
                columns=pd.Index(
                    ["open_time"] + list(self.OUTPUT_DTYPES.keys())
                ),  # Convert list to Index
                dtype=object,
            )
            for col, dtype in self.OUTPUT_DTYPES.items():
                df[col] = df[col].astype(dtype)
            df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
            df.set_index("open_time", inplace=True)
            return df

        # Ensure open_time is the index and in UTC
        if "open_time" in df.columns:
            df.set_index("open_time", inplace=True)

        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        elif df.index.tz != timezone.utc:
            df.index = df.index.tz_convert("UTC")

        # Normalize column names
        column_mapping = {
            "taker_buy_base": "taker_buy_volume",
            "taker_buy_quote": "taker_buy_quote_volume",
        }
        df = df.rename(columns=column_mapping)

        # Ensure correct columns and types
        df = df[list(self.OUTPUT_DTYPES.keys())]
        for col, dtype in self.OUTPUT_DTYPES.items():
            df[col] = df[col].astype(dtype)

        return df

    async def _fetch_from_source(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        use_vision: bool = True,
    ) -> pd.DataFrame:
        """Fetch data from the appropriate source.

        Args:
            symbol: Trading pair symbol
            start_time: Start time
            end_time: End time
            interval: Time interval
            use_vision: Whether to try Vision API first

        Returns:
            DataFrame containing market data
        """
        df = pd.DataFrame()

        try:
            if use_vision:
                try:
                    if not self.vision_client:
                        self.vision_client = VisionDataClient(
                            symbol=symbol,
                            interval=interval.value,
                            use_cache=False,  # We use our own cache
                        )
                    df = await self.vision_client.fetch(start_time, end_time)
                    if df.empty:
                        logger.warning(
                            "Vision API returned no records, falling back to REST"
                        )
                        use_vision = False
                except Exception as e:
                    logger.warning(f"Vision API fetch failed: {e}")
                    use_vision = False

            if not use_vision:
                if not self.rest_client:
                    self.rest_client = EnhancedRetriever(market_type=self.market_type)
                async with self.rest_client as client:
                    df, _ = await client.fetch(symbol, interval, start_time, end_time)

            # Format DataFrame and slice to exact time range
            df = self._format_dataframe(df)
            if not df.empty:
                mask = (df.index >= start_time) & (df.index <= end_time)
                df = df.loc[mask].copy()

            return df

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise

    async def get_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: Interval = Interval.SECOND_1,
        use_cache: bool = True,
        enforce_source: DataSource = DataSource.AUTO,
    ) -> pd.DataFrame:
        """Get market data from the most appropriate source with enhanced caching.

        Args:
            symbol: Trading pair symbol
            start_time: Start time
            end_time: End time
            interval: Time interval
            use_cache: Whether to use cached data
            enforce_source: Force specific data source

        Returns:
            DataFrame containing market data
        """
        # Validate dates
        self._validate_dates(start_time, end_time)

        # Adjust time window
        start_time, end_time = adjust_time_window(start_time, end_time, interval)
        logger.info(f"Adjusted time window: {start_time} -> {end_time}")

        # Check cache if enabled
        if use_cache and self.cache_manager:
            try:
                # Check if cache exists and is valid
                is_valid, error = await self.validate_cache_integrity(
                    symbol, interval.value, start_time
                )

                if is_valid:
                    cached_data = await self.cache_manager.load_from_cache(
                        symbol=symbol, interval=interval.value, date=start_time
                    )
                    if cached_data is not None:
                        # Slice cached data to exact time range
                        mask = (cached_data.index >= start_time) & (
                            cached_data.index <= end_time
                        )
                        cached_data = cached_data.loc[mask].copy()
                        if not cached_data.empty:
                            self._cache_stats["hits"] += 1
                            logger.info(f"Cache hit for {symbol} from {start_time}")
                            return cached_data
                    self._cache_stats["misses"] += 1
                else:
                    logger.warning(f"Cache validation failed: {error}")
                    if error != "Cache miss":
                        self._cache_stats["errors"] += 1
                        # Attempt to repair if validation failed (but not for cache misses)
                        if await self.repair_cache(symbol, interval.value, start_time):
                            logger.info("Cache repair successful")
                            cached_data = await self.cache_manager.load_from_cache(
                                symbol=symbol, interval=interval.value, date=start_time
                            )
                            if cached_data is not None:
                                # Slice repaired data to exact time range
                                mask = (cached_data.index >= start_time) & (
                                    cached_data.index <= end_time
                                )
                                cached_data = cached_data.loc[mask].copy()
                                if not cached_data.empty:
                                    return cached_data
                    else:
                        self._cache_stats["misses"] += 1

            except Exception as e:
                self._cache_stats["errors"] += 1
                logger.warning(f"Cache operation failed: {e}")

        # Determine data source
        use_vision = False
        if enforce_source == DataSource.VISION:
            use_vision = True
        elif enforce_source == DataSource.REST:
            use_vision = False
        else:
            use_vision = self._should_use_vision_api(start_time, end_time, interval)

        # Fetch from source
        df = await self._fetch_from_source(
            symbol, start_time, end_time, interval, use_vision
        )

        # Cache result if enabled
        if not df.empty and use_cache and self.cache_manager:
            try:
                await self.cache_manager.save_to_cache(
                    df=df, symbol=symbol, interval=interval.value, date=start_time
                )
                logger.info(f"Cached {len(df)} records for {symbol}")
            except Exception as e:
                logger.warning(f"Failed to cache data: {e}")

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
