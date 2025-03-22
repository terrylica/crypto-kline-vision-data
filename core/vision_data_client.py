#!/usr/bin/env python
"""Enhanced Vision Data Client with optimized Arrow MMAP caching.

This client implements a high-performance caching system using Arrow Memory-Mapped format for:
- Zero-copy reads through Arrow memory mapping
- Columnar data storage and retrieval
- Granular data loading with column selection
- Comprehensive error classification and recovery
- Multi-layered validation framework
- Cache metadata tracking and integrity checks

Migration Guide for Caching:

1. Current (Legacy) Usage:
   ```python
   client = VisionDataClient("BTCUSDT", cache_dir=Path("./cache"), use_cache=True)
   df = await client.fetch(start_time, end_time)
   ```

2. Recommended Usage:
   ```python
   manager = DataSourceManager(
       cache_dir=Path("./cache"),
       use_cache=True
   )
   df = await manager.get_data(
       symbol="BTCUSDT",
       start_time=start_time,
       end_time=end_time,
       enforce_source=DataSource.VISION  # If Vision API is specifically needed
   )
   ```

Benefits of using DataSourceManager:
- Centralized caching through UnifiedCacheManager
- Smart source selection between REST and Vision APIs
- Consistent data format and validation
- Better error handling and retry logic
- Optimized memory usage with MMAP
"""

import httpx
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, Sequence, TypeVar, Generic
import pandas as pd
import warnings

from utils.logger_setup import get_logger
from utils.cache_validator import CacheKeyManager, CacheValidator, VisionCacheManager
from utils.validation import DataFrameValidator
from utils.market_constraints import Interval
from utils.time_alignment import TimeRangeManager
from utils.download_handler import VisionDownloadManager
from utils.config import create_empty_dataframe
from utils.http_client_factory import create_httpx_client
from core.vision_constraints import (
    TimestampedDataFrame,
    MAX_CONCURRENT_DOWNLOADS,
    FILES_PER_DAY,
)
from core.cache_manager import UnifiedCacheManager

# Define the type variable for VisionDataClient
T = TypeVar("T")

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


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
        self._client = create_httpx_client(timeout=30)
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
        """Get cache file path for a specific date."""
        return (
            CacheKeyManager.get_cache_path(
                self.cache_dir, self.symbol, self.interval, date
            )
            if self.cache_dir
            else Path()
        )

    def _validate_cache(self, start_time: datetime, end_time: datetime) -> bool:
        """Validate cache existence, integrity, and data completeness."""
        if not self.cache_dir or not self.use_cache or not self.cache_manager:
            return False

        try:
            # Validate cache for each day in range
            current_day = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
            last_day = end_time.replace(hour=0, minute=0, second=0, microsecond=0)

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
        """Download and cache data for the specified time range."""
        # Get list of dates to download
        current_date = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        dates = []
        while current_date < end_time:
            dates.append(current_date)
            current_date += timedelta(days=1)

        logger.info(
            f"Downloading data for dates: {[d.strftime('%Y-%m-%d') for d in dates]}"
        )

        # Download data for each date
        dfs = []
        for date in dates:
            # Use download manager to get data
            df = await self._download_manager.download_date(date)

            if df is not None:
                # Filter to requested time range
                filtered_df = TimeRangeManager.filter_dataframe(
                    df, start_time, end_time
                )

                if not filtered_df.empty:
                    dfs.append(filtered_df)

                    # Save to cache if enabled
                    if self.cache_dir and self.use_cache:
                        try:
                            # Get cache path and ensure directory exists
                            cache_path = self._get_cache_path(date)
                            cache_path.parent.mkdir(parents=True, exist_ok=True)

                            # Save using cache manager
                            logger.info(f"Saving data to cache: {cache_path}")
                            checksum, record_count = (
                                await VisionCacheManager.save_to_cache(
                                    df, cache_path, date
                                )
                            )

                            # Update metadata if available
                            if self.cache_manager:
                                self.cache_manager.register_cache(
                                    self.symbol,
                                    self.interval,
                                    date,
                                    cache_path,
                                    checksum,
                                    record_count,
                                )

                        except Exception as e:
                            logger.error(f"Failed to save to cache: {e}")
                            continue

        if not dfs:
            logger.error(f"No data available for {start_time} to {end_time}")
            return TimestampedDataFrame(self._create_empty_dataframe())

        # Combine all data
        combined_df = pd.concat(dfs, axis=0)

        # Sort by index and filter columns
        combined_df = combined_df.sort_index()
        if columns is not None:
            combined_df = combined_df[columns]

        return TimestampedDataFrame(combined_df)

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> TimestampedDataFrame:
        """Fetch data from Binance Vision API."""
        # Validate and normalize time range using centralized utility
        TimeRangeManager.validate_time_window(start_time, end_time)

        # Get time boundaries using the centralized manager
        time_boundaries = TimeRangeManager.get_time_boundaries(
            start_time, end_time, self.interval_obj
        )
        start_time = time_boundaries["adjusted_start"]
        end_time = time_boundaries["adjusted_end"]

        logger.info(
            f"Fetching {self.symbol} {self.interval} data: "
            f"{start_time.isoformat()} -> {end_time.isoformat()} (exclusive end)"
        )

        # Attempt to use cache if enabled
        if self.use_cache and self._validate_cache(start_time, end_time):
            try:
                df = await self._download_and_cache(
                    start_time, end_time, columns=columns
                )
                if not df.empty:
                    # Validate data integrity
                    self._validator.validate_dataframe(df)
                    TimeRangeManager.validate_boundaries(df, start_time, end_time)

                return df
            except Exception as e:
                logger.warning(
                    f"Error loading from cache: {e}, falling back to direct fetch"
                )

        # Direct fetch without caching
        try:
            df = await self._download_and_cache(start_time, end_time, columns=columns)
            if not df.empty:
                # Validate data integrity
                self._validator.validate_dataframe(df)
                TimeRangeManager.validate_boundaries(df, start_time, end_time)

                return df

            return self._create_empty_dataframe()

        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return self._create_empty_dataframe()

    async def prefetch(
        self, start_time: datetime, end_time: datetime, max_days: int = 5
    ) -> None:
        """Prefetch data in background for future use.

        Args:
            start_time: Start time for prefetch
            end_time: End time for prefetch
            max_days: Maximum number of days to prefetch
        """
        # Validate time range
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

    async def _check_cache(self, date_to_check: datetime) -> Optional[pd.DataFrame]:
        """Check if data for a specific date is in cache.

        Args:
            date_to_check: Date to check in cache

        Returns:
            DataFrame if data is in cache, None otherwise
        """
        if not self.cache_dir or not self.use_cache or not self.cache_manager:
            return None

        try:
            # Get cache information from the cache manager
            cache_path = self.cache_manager.get_cache_path(
                self.symbol, self.interval, date_to_check
            )

            # Check if cache file exists
            if not cache_path.exists():
                logger.debug(f"Cache file not found: {cache_path}")
                return None

            # Validate cache file integrity
            is_valid = CacheValidator.validate_cache_file(cache_path)
            if not is_valid:
                logger.warning(f"Cache file is invalid: {cache_path}")
                return None

            # Load data from cache
            logger.debug(f"Loading data from cache: {cache_path}")
            df = await CacheValidator.safely_read_arrow_file_async(cache_path)

            # Validate the loaded DataFrame
            self._validator.validate_dataframe(df)
            logger.info(
                f"Successfully loaded data from cache for {date_to_check.date()}"
            )
            return df
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
