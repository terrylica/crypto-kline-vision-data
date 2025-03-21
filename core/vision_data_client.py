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
from typing import Dict, Optional, Sequence, TypeVar
import pandas as pd
import warnings
from typing import Generic

from utils.logger_setup import get_logger
from utils.cache_validator import CacheKeyManager, CacheValidator, VisionCacheManager
from utils.validation import DataFrameValidator
from utils.market_constraints import Interval
from utils.time_alignment import TimeRangeManager
from utils.download_handler import VisionDownloadManager
from .vision_constraints import (
    TimestampedDataFrame,
    MAX_CONCURRENT_DOWNLOADS,
    FILES_PER_DAY,
)

# Define the type variable for VisionDataClient
T = TypeVar("T")

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


class CacheMetadata:
    """Manages metadata for cached data."""

    def __init__(self, cache_dir: Path) -> None:
        """Initialize metadata manager.

        Args:
            cache_dir: Root cache directory
        """
        self.cache_dir: Path = cache_dir
        self.metadata_file: Path = cache_dir / "metadata.json"
        self.metadata: Dict[str, Dict[str, str | int | float]] = self._load_metadata()

    def _load_metadata(self) -> Dict[str, Dict[str, str | int | float]]:
        """Load metadata from disk."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, "r") as f:
                    import json

                    return json.load(f)
            except json.JSONDecodeError:
                logger.error("Corrupted metadata file, creating new")
                return {}
        return {}

    def _save_metadata(self) -> None:
        """Save metadata to disk."""
        with open(self.metadata_file, "w") as f:
            import json

            json.dump(self.metadata, f, indent=2)

    def get_cache_key(self, symbol: str, interval: str, date: datetime) -> str:
        """Generate cache key for a specific data point."""
        return CacheKeyManager.get_cache_key(symbol, interval, date)

    def register_cache(
        self,
        symbol: str,
        interval: str,
        date: datetime,
        file_path: Path,
        checksum: str,
        record_count: int,
    ) -> None:
        """Register a new cache entry."""
        cache_key = self.get_cache_key(symbol, interval, date)
        self.metadata[cache_key] = {
            "symbol": symbol,
            "interval": interval,
            "year_month": date.strftime("%Y%m"),
            "file_path": str(file_path.relative_to(self.cache_dir)),
            "checksum": checksum,
            "record_count": record_count,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        self._save_metadata()

    def get_cache_info(
        self, symbol: str, interval: str, date: datetime
    ) -> Optional[Dict[str, str | int | float]]:
        """Get cache information for a specific data point."""
        cache_key = self.get_cache_key(symbol, interval, date)
        return self.metadata.get(cache_key)


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
            self.interval_obj = Interval(interval)
        except ValueError:
            logger.warning(
                f"Could not parse interval {interval}, using SECOND_1 as default"
            )
            self.interval_obj = Interval.SECOND_1

        self.cache_dir = cache_dir
        self.use_cache = use_cache
        self.max_concurrent_downloads = (
            max_concurrent_downloads or MAX_CONCURRENT_DOWNLOADS
        )
        self._current_mmap = None
        self._current_mmap_path = None

        # Initialize cache-related components if caching is enabled
        if cache_dir and use_cache:
            warnings.warn(
                "Direct caching through VisionDataClient is deprecated. "
                "Use DataSourceManager with caching enabled instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            try:
                # Setup cache directory
                sample_date = datetime.now(timezone.utc)
                self.symbol_cache_dir = CacheKeyManager.get_cache_path(
                    cache_dir, self.symbol, self.interval, sample_date
                ).parent
                self.symbol_cache_dir.mkdir(parents=True, exist_ok=True)

                # Initialize metadata manager
                self.metadata = CacheMetadata(cache_dir)
            except Exception as e:
                logger.error(f"Failed to initialize cache components: {e}")
                self.cache_dir = None
                self.use_cache = False
                self.metadata = None
                raise
        else:
            self.metadata = None

        # Initialize HTTP client
        max_connections = MAX_CONCURRENT_DOWNLOADS * FILES_PER_DAY
        self.client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            ),
            timeout=httpx.Timeout(10.0),
        )

        # Initialize utility managers
        self.download_manager = VisionDownloadManager(
            self.client, self.symbol, self.interval
        )

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
        if self._current_mmap is not None:
            try:
                self._current_mmap.close()
            except Exception as e:
                logger.warning(f"Error closing memory map: {e}")
            finally:
                self._current_mmap = None
                self._current_mmap_path = None

        # Close HTTP client
        try:
            await self.client.aclose()
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
        if not self.cache_dir or not self.use_cache or not self.metadata:
            return False

        try:
            # Validate cache for each day in range
            current_day = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
            last_day = end_time.replace(hour=0, minute=0, second=0, microsecond=0)

            while current_day <= last_day:
                # Get cache information
                cache_info = self.metadata.get_cache_info(
                    symbol=self.symbol, interval=self.interval, date=current_day
                )
                cache_path = self._get_cache_path(current_day)

                # Perform validation checks
                if not CacheValidator.validate_cache_metadata(cache_info):
                    return False
                if not CacheValidator.validate_cache_records(
                    cache_info["record_count"]
                ):
                    return False
                error = CacheValidator.validate_cache_integrity(cache_path)
                if error:
                    return False
                if not CacheValidator.validate_cache_checksum(
                    cache_path, cache_info["checksum"]
                ):
                    return False

                current_day += timedelta(days=1)

            return True

        except Exception as e:
            logger.error(f"Error validating cache: {e}")
            return False

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with correct structure."""
        columns = [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "taker_buy_volume",
            "taker_buy_quote_volume",
        ]
        df = pd.DataFrame([], columns=columns)

        # Set correct data types
        df["open"] = df["open"].astype("float64")
        df["high"] = df["high"].astype("float64")
        df["low"] = df["low"].astype("float64")
        df["close"] = df["close"].astype("float64")
        df["volume"] = df["volume"].astype("float64")
        df["close_time"] = df["close_time"].astype("int64")
        df["quote_volume"] = df["quote_volume"].astype("float64")
        df["trades"] = df["trades"].astype("int64")
        df["taker_buy_volume"] = df["taker_buy_volume"].astype("float64")
        df["taker_buy_quote_volume"] = df["taker_buy_quote_volume"].astype("float64")

        # Set index
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        df.set_index("open_time", inplace=True)

        return df

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
            df = await self.download_manager.download_date(date)

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
                            if self.metadata:
                                self.metadata.register_cache(
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
        # Validate and normalize time range
        TimeRangeManager.validate_time_window(start_time, end_time)

        # Get time boundaries
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
                    # Apply consistent time filtering
                    df = TimeRangeManager.filter_dataframe(df, start_time, end_time)

                    # Validate data integrity
                    if not df.empty:
                        DataFrameValidator.validate_dataframe(df)
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
                # Apply consistent time filtering
                filtered_df = TimeRangeManager.filter_dataframe(
                    df, start_time, end_time
                )

                if not filtered_df.empty:
                    # Validate data integrity
                    DataFrameValidator.validate_dataframe(filtered_df)
                    TimeRangeManager.validate_boundaries(
                        filtered_df, start_time, end_time
                    )

                return filtered_df

            return self._create_empty_dataframe()

        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return self._create_empty_dataframe()

    async def prefetch(
        self, start_time: datetime, end_time: datetime, max_days: int = 5
    ) -> None:
        """Prefetch data in background for future use."""
        # Validate time range
        TimeRangeManager.validate_time_window(start_time, end_time)
