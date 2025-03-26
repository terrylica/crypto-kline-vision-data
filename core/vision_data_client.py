#!/usr/bin/env python
"""VisionDataClient provides direct access to Binance Vision API for historical data.

This module implements a client for retrieving historical market data from the
Binance Vision API. It provides functions for fetching, validating, and processing data.

Functionality:
- Fetch historical market data by symbol, interval, and time range
- Validate data integrity and structure
- Process data into pandas DataFrames for analysis

The VisionDataClient is primarily used through the DataSourceManager, which provides
a unified interface for data retrieval with automatic source selection and caching.

For most use cases, users should interact with the DataSourceManager rather than
directly with this client.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Sequence, TypeVar, Generic, Dict, Any, Union

import pandas as pd
import warnings

from utils.logger_setup import get_logger
from utils.cache_validator import (
    CacheKeyManager,
    CacheValidator,
    VisionCacheManager,
    SafeMemoryMap,
    CacheValidationError,
)
from utils.validation import DataFrameValidator
from utils.market_constraints import Interval
from utils.time_alignment import TimeRangeManager
from utils.download_handler import VisionDownloadManager
from core.vision_constraints import FileExtensions
from utils.config import create_empty_dataframe
from utils.http_client_factory import create_client
from core.vision_constraints import (
    TimestampedDataFrame,
    MAX_CONCURRENT_DOWNLOADS,
)
from core.cache_manager import UnifiedCacheManager

# Define the type variable for VisionDataClient
T = TypeVar("T")

logger = get_logger(__name__, "INFO", show_path=False)


class VisionDataClient(Generic[T]):
    """Enhanced Vision Data Client with optimized caching."""

    def __init__(
        self,
        symbol: str,
        interval: str = "1s",
        cache_dir: Optional[Path] = None,
        use_cache: bool = False,
        max_concurrent_downloads: Optional[int] = None,
    ):
        """Initialize Vision Data Client.

        Args:
            symbol: Trading symbol e.g. 'BTCUSDT'
            interval: Kline interval e.g. '1s', '1m'
            cache_dir: Optional directory for caching data
            use_cache: Whether to use cache
            max_concurrent_downloads: Maximum concurrent downloads
        """
        self.symbol = symbol.upper()
        self.interval = interval

        # Parse interval string to Interval object
        try:
            # Try to find the interval enum by value
            self.interval_obj = next((i for i in Interval if i.value == interval), None)
            if self.interval_obj is None:
                # Try by enum name (upper case with _ instead of number)
                try:
                    self.interval_obj = Interval[interval.upper()]
                except KeyError:
                    raise ValueError(f"Invalid interval: {interval}")
        except Exception as e:
            logger.warning(
                f"Could not parse interval {interval}, using SECOND_1 as default: {e}"
            )
            self.interval_obj = Interval.SECOND_1

        # Cache setup
        self.use_cache = use_cache
        self.cache_dir = cache_dir
        self._current_mmap = None
        self._current_mmap_path = None

        # Initialize cache manager if caching is enabled
        if use_cache and cache_dir:
            # Emit deprecation warning
            warnings.warn(
                "Direct caching through VisionDataClient is deprecated. "
                "Use DataSourceManager with caching enabled instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.cache_manager = UnifiedCacheManager(cache_dir)

            # Setup cache directory for backward compatibility
            sample_date = datetime.now(timezone.utc)
            self.symbol_cache_dir = CacheKeyManager.get_cache_path(
                cache_dir, self.symbol, self.interval, sample_date
            ).parent
            self.symbol_cache_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.cache_manager = None
            self.symbol_cache_dir = None

        # Configure download concurrency
        self._max_concurrent_downloads = (
            max_concurrent_downloads or MAX_CONCURRENT_DOWNLOADS
        )
        # Prepare HTTP client for API access
        self._client = create_client(client_type="httpx", timeout=30)
        # Initialize download manager
        self._download_manager = VisionDownloadManager(
            client=self._client, symbol=self.symbol, interval=self.interval
        )

        # Validator for checking results
        self._validator = DataFrameValidator()

    async def __aenter__(self) -> "VisionDataClient":
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[object],
    ) -> None:
        """Async context manager exit."""
        # Clean up memory map resources
        if hasattr(self, "_current_mmap") and self._current_mmap is not None:
            try:
                self._current_mmap.close()
            except Exception as e:
                logger.warning(f"Error closing memory map: {e}")
            finally:
                self._current_mmap = None
                self._current_mmap_path = None

        # Close HTTP client
        try:
            await self._client.aclose()
        except Exception as e:
            logger.warning(f"Error closing HTTP client: {e}")

    def _get_cache_path(self, date: datetime) -> Path:
        """Get cache file path for a specific date.

        This method ensures the date is aligned to match REST API behavior for consistent caching.

        Args:
            date: Date for cache lookup (will be aligned to REST API boundaries)

        Returns:
            Path to cache file
        """
        # Ensure date is aligned to REST API boundaries
        aligned_date = self._align_date_to_rest_api_boundary(date)
        date_str = aligned_date.strftime("%Y-%m-%d")
        filename = (
            f"{self.symbol}-{self.interval}-{date_str}{FileExtensions.CACHE.value}"
        )
        return self.cache_dir / filename if self.cache_dir else Path()

    def _align_date_to_rest_api_boundary(self, date: datetime) -> datetime:
        """Align date to match REST API boundary behavior.

        This method applies the same boundary alignment that the REST API
        would naturally apply, ensuring consistent behavior between
        the REST API and Vision API/cache.

        Args:
            date: Date to align

        Returns:
            Date aligned to match REST API boundary behavior
        """
        # Ensure datetime uses consistent timezone
        date = TimeRangeManager.enforce_utc_timezone(date)

        # Use interval object for alignment
        try:
            interval = self.interval_obj
        except AttributeError:
            # If interval_obj isn't available, try to get it from the interval string
            from utils.market_constraints import Interval

            interval = next(
                (i for i in Interval if i.value == self.interval), Interval.SECOND_1
            )

        # Apply REST API-like alignment by flooring to interval boundary
        from utils.time_alignment import get_interval_floor

        aligned_date = get_interval_floor(date, interval)

        return aligned_date

    def _validate_cache(self, start_time: datetime, end_time: datetime) -> bool:
        """Validate cache existence, integrity, and data completeness.

        This method checks if the cache is valid for the given time range
        with alignment to match REST API behavior.

        Args:
            start_time: Start time
            end_time: End time

        Returns:
            True if cache is valid, False otherwise
        """
        if not self.cache_dir or not self.use_cache or not self.cache_manager:
            return False

        try:
            # Align dates to match REST API behavior for cache validation
            start_time = TimeRangeManager.enforce_utc_timezone(start_time)
            end_time = TimeRangeManager.enforce_utc_timezone(end_time)

            # Use interval object for alignment
            try:
                interval = self.interval_obj
            except AttributeError:
                from utils.market_constraints import Interval

                interval = next(
                    (i for i in Interval if i.value == self.interval), Interval.SECOND_1
                )

            # Get aligned boundaries to match REST API behavior
            aligned_boundaries = TimeRangeManager.align_vision_api_to_rest(
                start_time, end_time, interval
            )
            aligned_start = aligned_boundaries["adjusted_start"]
            aligned_end = aligned_boundaries["adjusted_end"]

            # Validate cache for each day in range
            current_day = aligned_start.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            last_day = aligned_end.replace(hour=0, minute=0, second=0, microsecond=0)

            while current_day <= last_day:
                # Get cache information
                cache_info = self.cache_manager.get_cache_info(
                    symbol=self.symbol, interval=self.interval, date=current_day
                )
                cache_path = self._get_cache_path(current_day)

                # Perform validation checks
                if not CacheValidator.validate_cache_metadata(cache_info):
                    return False
                if not CacheValidator.validate_cache_records(
                    cache_info.get("record_count", 0)
                ):
                    return False
                error = CacheValidator.validate_cache_integrity(cache_path)
                if error:
                    return False
                if not CacheValidator.validate_cache_checksum(
                    cache_path, cache_info.get("checksum", "")
                ):
                    return False

                current_day += timedelta(days=1)

            return True

        except Exception as e:
            logger.error(f"Error validating cache: {e}")
            return False

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with correct structure.

        Returns:
            Empty DataFrame with standardized structure
        """
        return create_empty_dataframe()

    async def _download_and_cache(
        self,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> TimestampedDataFrame:
        """Download and cache data for the specified time range.

        This method applies manual alignment to Vision API requests to match
        REST API's natural boundary behavior.

        Args:
            start_time: Start time
            end_time: End time
            columns: Optional list of columns to return

        Returns:
            DataFrame with market data
        """
        # Use TimeRangeManager to enforce timezone
        start_time = TimeRangeManager.enforce_utc_timezone(start_time)
        end_time = TimeRangeManager.enforce_utc_timezone(end_time)

        # Apply manual alignment to match REST API behavior
        aligned_boundaries = TimeRangeManager.align_vision_api_to_rest(
            start_time, end_time, self.interval_obj
        )
        aligned_start = aligned_boundaries["adjusted_start"]
        aligned_end = aligned_boundaries["adjusted_end"]

        logger.info(
            f"Vision API request with aligned boundaries: {aligned_start} -> {aligned_end} "
            f"(to match REST API behavior)"
        )

        # Get list of dates to download
        current_date = aligned_start.replace(hour=0, minute=0, second=0, microsecond=0)
        dates = []
        while current_date <= aligned_end.replace(
            hour=0, minute=0, second=0, microsecond=0
        ):
            dates.append(current_date)
            current_date += timedelta(days=1)

        # Download data for each date
        all_dfs = []
        semaphore = asyncio.Semaphore(self._max_concurrent_downloads)
        download_tasks = []

        for date in dates:
            download_tasks.append(self._download_date(date, semaphore, columns))

        try:
            results = await asyncio.gather(*download_tasks)
            for df in results:
                if df is not None and not df.empty:
                    all_dfs.append(df)
        except Exception as e:
            logger.error(f"Error downloading data: {e}")

        if not all_dfs:
            return TimestampedDataFrame(self._create_empty_dataframe())

        # Combine all data
        combined_df = pd.concat(all_dfs).sort_index()

        # Apply final filtering to match original requested time range
        result_df = TimeRangeManager.filter_dataframe(combined_df, start_time, end_time)

        return TimestampedDataFrame(result_df)

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> TimestampedDataFrame:
        """Fetch data from Binance Vision API with caching.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            columns: Optional list of specific columns to retrieve

        Returns:
            DataFrame containing requested data
        """
        # Validate and normalize time range
        from utils.time_alignment import TimeRangeManager

        TimeRangeManager.validate_time_window(start_time, end_time)

        # Get time boundaries using the helper function
        time_boundaries = ApiBoundaryValidator.get_time_boundaries(
            start_time, end_time, self.interval_obj
        )
        start_time = time_boundaries["adjusted_start"]
        end_time = time_boundaries["adjusted_end"]

        logger.info(
            f"Fetching {self.symbol} {self.interval} data: "
            f"{start_time.isoformat()} -> {end_time.isoformat()}"
        )

        if self.use_cache:
            # Initialize cache manager if needed
            if self.cache_manager is None:
                self._setup_cache()

            # Try to get data from cache
            try:
                df = await self._get_from_cache(start_time, end_time, columns)
                if not df.empty:
                    return df
                logger.info("Cache miss or invalid cache, downloading data")
            except Exception as e:
                logger.warning(f"Error reading from cache: {e}")

        # Download data directly
        try:
            df = await self._download_and_cache(start_time, end_time, columns=columns)
            if not df.empty:
                # Validate data integrity
                self._validator.validate_dataframe(df)
                TimeRangeManager.validate_boundaries(df, start_time, end_time)
                logger.info(f"Successfully fetched {len(df)} records")
                return df

            logger.warning(f"No data available for {start_time} to {end_time}")
            return TimestampedDataFrame(self._create_empty_dataframe())

        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return TimestampedDataFrame(self._create_empty_dataframe())

    async def prefetch(
        self, start_time: datetime, end_time: datetime, max_days: int = 5
    ) -> None:
        """Prefetch data in background for future use.

        Args:
            start_time: Start time for prefetch
            end_time: End time for prefetch
            max_days: Maximum number of days to prefetch
        """
        # Validate time range and enforce UTC timezone using TimeRangeManager
        start_time = TimeRangeManager.enforce_utc_timezone(start_time)
        end_time = TimeRangeManager.enforce_utc_timezone(end_time)
        TimeRangeManager.validate_time_window(start_time, end_time)

        # Limit prefetch to max_days
        limited_end = min(end_time, start_time + timedelta(days=max_days))
        logger.info(f"Prefetching data from {start_time} to {limited_end}")

        # Just call fetch which will handle downloading and caching
        # We don't need to wait for the result, so we can ignore it
        try:
            await self._download_and_cache(start_time, limited_end)
            logger.info(f"Prefetch completed for {start_time} to {limited_end}")
        except Exception as e:
            logger.error(f"Error during prefetch: {e}")

    async def _check_cache(
        self,
        date_or_start_time: datetime,
        end_time_or_columns: Optional[Union[datetime, Sequence[str]]] = None,
        columns: Optional[Sequence[str]] = None,
    ) -> Optional[pd.DataFrame]:
        """Check if data for a specific date or time range is in cache.

        This method supports two calling patterns:
        1. _check_cache(date, columns=None) - Check cache for a specific date
        2. _check_cache(start_time, end_time, columns=None) - Check cache for a time range

        Args:
            date_or_start_time: Date to check in cache or start time of range
            end_time_or_columns: End time of range or columns to return for date-only call
            columns: Optional list of columns to return when using start_time/end_time

        Returns:
            DataFrame if data is in cache, None otherwise
        """
        # Determine which calling pattern is being used
        if isinstance(end_time_or_columns, datetime):
            # This is the start_time, end_time pattern
            start_time = date_or_start_time
            end_time = end_time_or_columns

            # Apply manual alignment for cache operations to match REST API behavior
            aligned_boundaries = TimeRangeManager.align_vision_api_to_rest(
                start_time, end_time, self.interval_obj
            )

            # Use the first day in the range for cache lookup
            start_date = aligned_boundaries["adjusted_start"].replace(
                hour=0, minute=0, second=0, microsecond=0
            )

            # Call the date-based version
            return await self._check_cache_by_date(start_date, columns)
        else:
            # This is the date pattern - end_time_or_columns contains columns or None
            date = date_or_start_time
            cols = end_time_or_columns

            # Call the date-based version
            return await self._check_cache_by_date(date, cols)

    async def _check_cache_by_date(
        self, date: datetime, columns: Optional[Sequence[str]] = None
    ) -> Optional[pd.DataFrame]:
        """Check if data for a specific date is in cache.

        Args:
            date: Date to check in cache
            columns: Optional list of columns to return

        Returns:
            DataFrame if data is in cache, None otherwise
        """
        if not self.cache_dir or not self.use_cache or not self.cache_manager:
            return None

        # Ensure date has proper timezone using TimeRangeManager
        date = TimeRangeManager.enforce_utc_timezone(date)

        try:
            # Get cache information from the cache manager
            cache_path = self.cache_manager.get_cache_path(
                self.symbol, self.interval, date
            )

            # Check if cache file exists
            if not cache_path.exists():
                logger.debug(f"Cache file not found: {cache_path}")
                return None

            # Validate cache file integrity
            error = CacheValidator.validate_cache_integrity(cache_path)
            if error:
                logger.warning(f"Cache file validation failed: {error.message}")
                return None

            # Load data from cache
            logger.debug(f"Loading data from cache: {cache_path}")
            df = await SafeMemoryMap.safely_read_arrow_file(cache_path, columns)

            if df is None or df.empty:
                logger.debug(f"Empty data loaded from cache: {cache_path}")
                return None

            # Validate the loaded DataFrame
            try:
                DataFrameValidator.validate_dataframe(df)
                logger.info(f"Successfully loaded data from cache for {date.date()}")
                return df
            except ValueError as e:
                logger.warning(f"Invalid data in cache: {e}")
                return None
        except Exception as e:
            logger.warning(f"Error reading from cache: {e}")
            return None

    async def _save_to_cache(self, df: pd.DataFrame, date: datetime) -> None:
        """Save DataFrame to cache.

        Args:
            df: DataFrame to save
            date: Date for which data is being saved
        """
        if not self.cache_dir or not self.use_cache or not self.cache_manager:
            return

        # Ensure date has proper timezone using TimeRangeManager
        date = TimeRangeManager.enforce_utc_timezone(date)

        try:
            logger.debug(f"Saving data to cache for {date.date()}")
            cache_path = self.cache_manager.get_cache_path(
                self.symbol, self.interval, date
            )

            checksum, record_count = await VisionCacheManager.save_to_cache(
                df, cache_path, date
            )

            # Update cache metadata through the unified cache manager
            cache_key = self.cache_manager.get_cache_key(
                self.symbol, self.interval, date
            )
            self.cache_manager.metadata[cache_key] = {
                "symbol": self.symbol,
                "interval": self.interval,
                "year_month": date.strftime("%Y%m"),
                "file_path": str(cache_path.relative_to(self.cache_dir)),
                "checksum": checksum,
                "record_count": record_count,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            self.cache_manager._save_metadata()

            logger.info(f"Successfully cached {record_count} records for {date.date()}")
        except Exception as e:
            logger.error(f"Failed to save to cache: {e}")
            # Don't raise the exception, just log it and continue
            # This is a non-critical operation

    async def _download_date(
        self,
        date: datetime,
        semaphore: asyncio.Semaphore,
        columns: Optional[Sequence[str]] = None,
    ) -> Optional[pd.DataFrame]:
        """Download data for a specific date.

        Args:
            date: Date to download data for
            semaphore: Semaphore for concurrency control
            columns: Optional list of columns to return

        Returns:
            DataFrame with data for the date, or None if download failed
        """
        async with semaphore:
            try:
                logger.debug(f"Downloading data for date: {date.strftime('%Y-%m-%d')}")

                # Check cache first if enabled
                if self.use_cache and self.cache_dir:
                    cached_df = await self._check_cache(date, columns=columns)
                    if cached_df is not None and not cached_df.empty:
                        logger.debug(
                            f"Using cached data for {date.strftime('%Y-%m-%d')}"
                        )
                        return cached_df

                # Download using download manager
                df = await self._download_manager.download_date(date)

                if df is None or df.empty:
                    logger.debug(f"No data for date: {date.strftime('%Y-%m-%d')}")
                    return None

                # Save to cache if enabled
                if self.use_cache and self.cache_dir and not df.empty:
                    try:
                        await self._save_to_cache(df, date)
                        logger.debug(
                            f"Saved data to cache for {date.strftime('%Y-%m-%d')}"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to save to cache: {e}")

                return df

            except Exception as e:
                logger.error(
                    f"Error downloading data for {date.strftime('%Y-%m-%d')}: {e}"
                )
                return None
