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

import hashlib
import httpx
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
from typing import Dict, Optional, Tuple, TypeVar, Generic, Sequence
import pandas as pd
import pyarrow as pa
import json
import asyncio
import time
import numpy as np
import warnings

from utils.logger_setup import get_logger
from .vision_constraints import (
    TimestampedDataFrame,
    validate_cache_path,
    enforce_utc_timestamp,
    validate_time_range,
    validate_data_availability,
    is_data_likely_available,
    validate_cache_checksum,
    validate_cache_records,
    validate_cache_metadata,
    get_cache_path,
    get_vision_url,
    FileType,
    classify_error,
    MAX_CONCURRENT_DOWNLOADS,
    FILES_PER_DAY,
    CANONICAL_INDEX_NAME,
    validate_time_boundaries,
    validate_symbol_format,
    validate_dataframe_integrity,
    detect_timestamp_unit,
)
from utils.download_handler import DownloadHandler
from utils.market_constraints import Interval, MarketType
from utils.time_alignment import (
    get_time_boundaries,
    filter_time_range,
)
from utils.validation import DataValidation

# Type variables for generic type hints
T = TypeVar("T", bound=TimestampedDataFrame)
PathLike = TypeVar("PathLike", str, Path)
MetadataDict = Dict[str, Dict[str, str | int | float]]

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
        self.metadata: MetadataDict = self._load_metadata()

    def _load_metadata(self) -> MetadataDict:
        """Load metadata from disk."""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.error("Corrupted metadata file, creating new")
                return {}
        return {}

    def _save_metadata(self) -> None:
        """Save metadata to disk."""
        with open(self.metadata_file, "w") as f:
            json.dump(self.metadata, f, indent=2)

    def get_cache_key(self, symbol: str, interval: str, date: datetime) -> str:
        """Generate cache key for a specific data point.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Date for the data

        Returns:
            Cache key string
        """
        return f"{symbol}_{interval}_{date.strftime('%Y%m')}"

    def register_cache(
        self,
        symbol: str,
        interval: str,
        date: datetime,
        file_path: Path,
        checksum: str,
        record_count: int,
    ) -> None:
        """Register a new cache entry.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Date for the data
            file_path: Path to cached file
            checksum: Data checksum
            record_count: Number of records in cache
        """
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
        """Get cache information for a specific data point.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Date for the data

        Returns:
            Cache information dictionary or None if not found
        """
        cache_key = self.get_cache_key(symbol, interval, date)
        return self.metadata.get(cache_key)


class SafeMemoryMap:
    """Context manager for safe memory map handling."""

    def __init__(self, path: Path):
        self.path = path
        self._mmap = None

    def __enter__(self) -> pa.MemoryMappedFile:
        self._mmap = pa.memory_map(str(self.path), "r")
        return self._mmap

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[object],
    ) -> None:
        if self._mmap is not None:
            self._mmap.close()


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
                f"Could not parse interval {interval} to Interval enum, using SECOND_1 as default"
            )
            self.interval_obj = Interval.SECOND_1

        self.cache_dir = cache_dir
        self.use_cache = use_cache
        self.max_concurrent_downloads = (
            max_concurrent_downloads
            if max_concurrent_downloads
            else MAX_CONCURRENT_DOWNLOADS
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
                # Create symbol-specific cache directory
                self.symbol_cache_dir = get_cache_path(
                    cache_dir, self.symbol, self.interval, datetime.now(timezone.utc)
                ).parent
                self.symbol_cache_dir.mkdir(parents=True, exist_ok=True)
                logger.info(
                    f"Created cache directory structure at: {self.symbol_cache_dir}"
                )

                # Initialize metadata manager
                self.metadata = CacheMetadata(cache_dir)
                logger.info(f"Initialized cache metadata manager for: {cache_dir}")
            except Exception as e:
                logger.error(f"Failed to initialize cache components: {e}")
                # Reset cache-related attributes on failure
                self.cache_dir = None
                self.use_cache = False
                self.metadata = None
                raise
        else:
            self.metadata = None

        # Initialize temporary file paths for downloads
        self._data_path = (
            Path(tempfile.gettempdir()) / f"{self.symbol}_{self.interval}_data.zip"
        )
        self._checksum_path = (
            Path(tempfile.gettempdir()) / f"{self.symbol}_{self.interval}_checksum"
        )

        # Initialize HTTP client with optimal settings for download constraints
        max_connections = MAX_CONCURRENT_DOWNLOADS * FILES_PER_DAY
        self.client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            ),
            timeout=httpx.Timeout(10.0),
        )

        # Initialize download handler
        self.download_handler = DownloadHandler(
            self.client, max_retries=5, min_wait=4, max_wait=60
        )

    async def __aenter__(self) -> "VisionDataClient[T]":
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[object],
    ) -> None:
        """Async context manager exit."""
        if self._current_mmap is not None:
            self._current_mmap.close()
            self._current_mmap = None
            self._current_mmap_path = None
        await self.client.aclose()

    def _get_cache_path(self, date: datetime) -> Path:
        """Get cache file path for a specific date.

        Args:
            date: Target date

        Returns:
            Path to cache file
        """
        return (
            get_cache_path(self.cache_dir, self.symbol, self.interval, date)
            if self.cache_dir
            else Path()
        )

    def _get_checksum_url(self, date: datetime) -> str:
        """Get checksum URL for a specific date.

        Args:
            date: Target date

        Returns:
            URL for the checksum file
        """
        return get_vision_url(self.symbol, self.interval, date, FileType.CHECKSUM)

    def _get_data_url(self, date: datetime) -> str:
        """Get data URL for a specific date.

        Args:
            date: Target date

        Returns:
            URL for the data file
        """
        return get_vision_url(self.symbol, self.interval, date, FileType.DATA)

    async def _download_file(self, url: str, local_path: Path) -> bool:
        """Download a file from URL to local path.

        Args:
            url: URL to download from
            local_path: Path to save to

        Returns:
            True if download successful, False otherwise
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()

                # Log response details
                logger.info(
                    f'HTTP Request: GET {url} "{response.status_code} {response.reason_phrase}"'
                )

                if response.status_code == 200:
                    # Check content length
                    content_length = int(response.headers.get("content-length", 0))
                    if content_length == 0:
                        logger.error(f"Empty response from {url}")
                        return False

                    # Write response content
                    with open(local_path, "wb") as f:
                        f.write(response.content)

                    # Verify file was written
                    if not local_path.exists() or local_path.stat().st_size == 0:
                        logger.error(f"Failed to write file: {local_path}")
                        return False

                    return True
                else:
                    logger.error(
                        f"Unexpected status code {response.status_code} from {url}"
                    )
                    return False

        except httpx.RequestError as e:
            logger.error(f"Network error downloading {url}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error downloading {url}: {str(e)}")
            return False

    def _verify_checksum(self, file_path: Path, checksum_path: Path) -> bool:
        """Verify file checksum.

        Args:
            file_path: Path to data file
            checksum_path: Path to checksum file

        Returns:
            Verification status
        """
        try:
            with open(checksum_path, "r") as f:
                expected = f.read().strip().split()[0]

            with open(file_path, "rb") as f:
                actual = hashlib.sha256(f.read()).hexdigest()

            return expected == actual
        except Exception as e:
            logger.error(f"Error verifying checksum: {e}")
            return False

    async def _save_to_cache(
        self,
        df: pd.DataFrame,
        cache_path: Path,
        start_time: datetime,
    ) -> Tuple[str, int]:
        """Save DataFrame to cache in Arrow format.

        Args:
            df: DataFrame to save
            cache_path: Target cache path
            start_time: Start time for the data

        Returns:
            Tuple of (checksum, record_count)

        Raises:
            pa.ArrowInvalid: If DataFrame cannot be converted to Arrow format
            pa.ArrowIOError: If there are I/O errors during file operations
            ValueError: If input validation fails
            OSError: If file system operations fail
        """
        try:
            # Validate inputs
            cache_path = validate_cache_path(cache_path)
            df = TimestampedDataFrame(df)  # Enforce constraints

            # Ensure we don't include the end date
            if not df.empty:
                last_date = df.index[-1].replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                df = df[df.index < last_date + timedelta(days=1)]

            # Reset index to include it in the Arrow table
            df_with_index = df.reset_index()

            try:
                # Convert to Arrow table
                table = pa.Table.from_pandas(df_with_index)
            except pa.ArrowInvalid as e:
                logger.error(f"Failed to convert DataFrame to Arrow format: {e}")
                raise

            try:
                # Create parent directory if it doesn't exist
                cache_path.parent.mkdir(parents=True, exist_ok=True)

                # Save to Arrow file with proper resource cleanup
                with pa.OSFile(str(cache_path), "wb") as sink:
                    with pa.ipc.new_file(sink, table.schema) as writer:
                        writer.write_table(table)
            except pa.ArrowIOError as e:
                logger.error(f"Failed to write Arrow file: {e}")
                raise

            try:
                # Calculate checksum and record count
                checksum = hashlib.sha256(cache_path.read_bytes()).hexdigest()
                record_count = len(df)
            except OSError as e:
                logger.error(f"Failed to calculate checksum: {e}")
                raise

            logger.info(f"Saved {record_count} records to {cache_path}")
            return checksum, record_count

        except Exception as e:
            logger.error(f"Unexpected error saving to cache: {e}")
            raise

    async def _load_from_cache(
        self, cache_path: Path, columns: Optional[Sequence[str]] = None
    ) -> TimestampedDataFrame:
        """Load data from cache with optimized memory usage."""
        start_time_perf = time.perf_counter()

        try:
            # Use memory mapping with zero-copy reads
            t0 = time.perf_counter()
            if self._current_mmap is None or self._current_mmap_path != cache_path:
                if self._current_mmap is not None:
                    self._current_mmap.close()
                self._current_mmap = pa.memory_map(str(cache_path), "r")
                self._current_mmap_path = cache_path
            logger.info(f"Memory map setup took: {time.perf_counter() - t0:.6f}s")

            # Read only required columns
            t0 = time.perf_counter()
            with pa.ipc.open_file(self._current_mmap) as reader:
                if columns:
                    cols_to_read = [CANONICAL_INDEX_NAME] + list(columns)
                    table = reader.read_all().select(cols_to_read)
                else:
                    table = reader.read_all()
            logger.info(f"Arrow table read took: {time.perf_counter() - t0:.6f}s")

            # Convert to pandas with zero-copy if possible
            t0 = time.perf_counter()
            df = table.to_pandas(
                zero_copy_only=True,
                date_as_object=False,
                use_threads=True,
                split_blocks=True,
                self_destruct=True,
            )
            logger.info(
                f"Arrow to pandas conversion took: {time.perf_counter() - t0:.6f}s"
            )

            # Set index and validate
            if CANONICAL_INDEX_NAME not in df.columns:
                raise ValueError(
                    f"Required index column {CANONICAL_INDEX_NAME} not found in data"
                )

            df.set_index(CANONICAL_INDEX_NAME, inplace=True)
            df.index = pd.to_datetime(df.index, utc=True)

            # Remove duplicates efficiently
            t0 = time.perf_counter()
            # Use drop_duplicates method instead of boolean indexing
            df = (
                df.reset_index()
                .drop_duplicates(subset=[CANONICAL_INDEX_NAME], keep="first")
                .set_index(CANONICAL_INDEX_NAME)
            )
            logger.info(f"Duplicate removal took: {time.perf_counter() - t0:.6f}s")

            total_time = time.perf_counter() - start_time_perf
            logger.info(f"Total cache loading time: {total_time:.6f}s")
            return TimestampedDataFrame(df)

        except Exception as e:
            logger.error(f"Failed to load from cache: {e}")
            raise

    def _validate_cache(self, start_time: datetime, end_time: datetime) -> bool:
        """Validate cache existence, integrity, and data completeness.

        Args:
            start_time: Start of time range to validate
            end_time: End of time range to validate

        Returns:
            True if cache exists, is valid, and contains required data

        Note:
            - Verifies cache directory existence
            - Validates metadata completeness
            - Checks cache file integrity
            - Verifies record count
            - Classifies and logs any validation errors
        """
        try:
            if not self.cache_dir:
                return False

            cache_path = get_cache_path(
                self.cache_dir, self.symbol, self.interval, start_time
            )

            # Check if we have metadata
            cache_info = self.metadata.get_cache_info(
                self.symbol, self.interval, start_time
            )
            if not validate_cache_metadata(cache_info):
                logger.warning("Invalid cache metadata")
                return False
            # Validate cache integrity
            if not validate_cache_checksum(
                cache_path, str(cache_info["checksum"] if cache_info else "")
            ):
                logger.warning("Cache checksum validation failed")
                return False
            # Check if cache contains required data
            record_count = cache_info["record_count"] if cache_info else 0
            if not validate_cache_records(int(record_count)):
                logger.warning("Cache contains no valid records")
                return False

            return True
        except Exception as e:
            error_type = classify_error(e)
            logger.error(f"{error_type.value} validating cache: {e}")
            return False

    def _validate_symbol(self) -> None:
        """Validate trading pair symbol."""
        validate_symbol_format(self.symbol)

    def _validate_data(self, df: pd.DataFrame) -> None:
        """Validate DataFrame meets all data integrity requirements.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If DataFrame fails integrity checks

        Note:
            - Validates through vision_constraints framework
            - Checks data structure
            - Verifies column types
            - Ensures index integrity
        """
        validate_dataframe_integrity(df)
        self._validate_timestamp_ordering(df)  # Add timestamp validation

    def _validate_time_boundaries(
        self, df: pd.DataFrame, start_time: datetime, end_time: datetime
    ) -> None:
        """Validate data completeness at time boundaries.

        Args:
            df: DataFrame to validate
            start_time: Start time of requested range
            end_time: End time of requested range

        Raises:
            ValueError: If data is missing at critical boundaries

        Note:
            - Checks day boundary completeness (00:00:00)
            - Validates hour boundary data
            - Ensures minimal interval coverage
            - Handles empty DataFrame cases
        """
        if df.empty:
            raise ValueError("No data available for the specified time range")

        # Validate day boundaries
        if start_time.time() == datetime.min.time():  # Day start (00:00:00)
            if df.index[0].time() != datetime.min.time():
                raise ValueError(f"Data missing at day boundary: {start_time}")

        # Validate hour boundaries
        hour_start = df[df.index.hour == start_time.hour].index
        if not hour_start.empty and hour_start[0].minute != start_time.minute:
            raise ValueError(f"Data missing at hour boundary: {start_time}")

        # Validate minimal intervals
        if (end_time - start_time) <= timedelta(seconds=1):
            if len(df) < 1:
                raise ValueError(
                    f"Insufficient data for minimal interval: {start_time} to {end_time}"
                )

    def _validate_timestamp_ordering(self, df: pd.DataFrame) -> None:
        """Validate timestamp ordering and uniqueness.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If timestamps are not ordered or have duplicates
        """
        if not df.index.is_monotonic_increasing:
            raise ValueError("Timestamps must be strictly increasing")
        if df.index.has_duplicates:
            logger.warning("Duplicate timestamps found - will keep first occurrence")

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> TimestampedDataFrame:
        """Fetch data with a cache-first strategy and comprehensive validation.

        This method follows a cache-first approach and includes validation of:
        - Time boundaries
        - Symbol validity
        - Data completeness

        Args:
            start_time: Start time
            end_time: End time
            columns: Optional list of columns to include

        Returns:
            DataFrame with requested data
        """
        # Validate inputs first
        self._validate_symbol()

        # Ensure we have a valid interval object
        if not hasattr(self, "interval_obj"):
            try:
                self.interval_obj = Interval(self.interval)
            except ValueError:
                logger.warning(
                    f"Could not parse interval {self.interval} to Interval enum, using SECOND_1 as default"
                )
                self.interval_obj = Interval.SECOND_1

        # Adjust time window using the centralized utility
        time_boundaries = get_time_boundaries(start_time, end_time, self.interval_obj)
        start_time = time_boundaries["adjusted_start"]
        end_time = time_boundaries["adjusted_end"]

        logger.info(
            f"Fetching {self.symbol} {self.interval} data: "
            f"{start_time.isoformat()} -> {end_time.isoformat()} (exclusive end)"
        )

        # Check if we should use cache
        if self.use_cache and self.cache_dir:
            try:
                # Get cache path for the date
                cache_path = self._get_cache_path(start_time)

                # Validate cache for this date
                if self._validate_cache(start_time, end_time):
                    # Attempt to read from cache first
                    logger.info(
                        f"Loading data from cache for {start_time} to {end_time}"
                    )
                    df = await self._load_from_cache(cache_path, columns)

                    # Filter to requested time range using centralized function
                    df = filter_time_range(df, start_time, end_time)

                    # Validate the filtered data
                    self._validate_data(df)
                    validate_time_boundaries(df, start_time, end_time)
                    return df
            except Exception as e:
                logger.warning(f"Cache read failed, falling back to download: {e}")

        # Cache miss or read failed - download fresh data
        logger.info(f"Downloading data for {start_time} to {end_time}")
        df = await self._download_and_cache(start_time, end_time, columns)
        self._validate_data(df)
        validate_time_boundaries(df, start_time, end_time)
        return df

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with correct structure.

        Returns:
            Empty DataFrame with correct columns and types
        """
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
        """Download and cache data for the specified time range.

        Args:
            start_time: Start time
            end_time: End time
            columns: Optional list of columns to include

        Returns:
            DataFrame with requested data

        Raises:
            ValueError: If no data is available
        """
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
            df = await self._download_date(date)
            if df is not None:
                # Filter to requested time range using centralized function
                filtered_df = filter_time_range(df, start_time, end_time)

                if not filtered_df.empty:
                    dfs.append(filtered_df)

                    # Save to cache if enabled
                    if self.cache_dir and self.use_cache:
                        try:
                            # Get cache path and ensure directory exists
                            cache_path = self._get_cache_path(date)
                            cache_path.parent.mkdir(parents=True, exist_ok=True)

                            # Save the full day's data to cache
                            logger.info(f"Saving data to cache: {cache_path}")
                            checksum, record_count = await self._save_to_cache(
                                df, cache_path, date
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
                                self.metadata._save_metadata()
                                logger.info(
                                    f"Updated cache metadata for {date.strftime('%Y-%m-%d')}"
                                )
                        except Exception as e:
                            logger.error(f"Failed to save to cache: {e}")
                            # Continue with the next date even if caching fails
                            continue

        if not dfs:
            logger.error(f"No data available for {start_time} to {end_time}")
            return TimestampedDataFrame(self._create_empty_dataframe())

        # Combine all data
        combined_df = pd.concat(dfs, axis=0)

        # Sort by index to ensure chronological order
        combined_df = combined_df.sort_index()

        # Filter columns if specified
        if columns is not None:
            combined_df = combined_df[columns]

        return TimestampedDataFrame(combined_df)

    async def _download_date(self, date: datetime) -> Optional[pd.DataFrame]:
        """Download data for a specific date.

        Args:
            date: Target date

        Returns:
            DataFrame with data or None if download failed
        """
        # Create temporary directory for downloads
        temp_dir = Path(tempfile.mkdtemp())
        data_file = (
            temp_dir
            / f"{self.symbol}_{self.interval}_{date.strftime('%Y%m%d')}_data.zip"
        )
        checksum_file = (
            temp_dir
            / f"{self.symbol}_{self.interval}_{date.strftime('%Y%m%d')}_checksum"
        )

        try:
            # Download data and checksum files
            data_url = self._get_data_url(date)
            checksum_url = self._get_checksum_url(date)

            logger.info(f"Downloading data for {date.strftime('%Y-%m-%d')} from:")
            logger.info(f"Data: {data_url}")
            logger.info(f"Checksum: {checksum_url}")

            success = await asyncio.gather(
                self._download_file(data_url, data_file),
                self._download_file(checksum_url, checksum_file),
            )

            if not all(success):
                logger.error(f"Failed to download files for {date}")
                return None

            # Verify checksum
            if not self._verify_checksum(data_file, checksum_file):
                logger.error(f"Checksum verification failed for {date}")
                return None

            # Read CSV data with detailed error handling
            try:
                logger.info(f"Reading CSV data from {data_file}")
                df = pd.read_csv(
                    data_file,
                    compression="zip",
                    names=[
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
                        "ignored",
                    ],
                )
                logger.info(f"Successfully read CSV with shape: {df.shape}")

                if df.empty:
                    logger.error(f"Empty DataFrame after reading CSV for {date}")
                    return None

                # Log raw data for debugging
                logger.info("Raw data sample (first 5 rows):")
                logger.info(df.head().to_string())
                logger.info("Column dtypes:")
                logger.info(df.dtypes.to_string())

                # Detect timestamp format with detailed logging
                sample_ts = df["open_time"].iloc[0]
                logger.info(
                    f"Sample timestamp value: {sample_ts} (type: {type(sample_ts)})"
                )
                ts_unit = detect_timestamp_unit(sample_ts)
                logger.info(f"Detected timestamp unit: {ts_unit}")

                # Convert timestamps with error checking
                try:
                    df["open_time"] = pd.to_datetime(df["open_time"], unit=ts_unit)
                    # Add microseconds to match REST API precision
                    df["open_time"] = df["open_time"].dt.floor("s") + pd.Timedelta(
                        microseconds=0
                    )
                    logger.info(
                        f"Converted open_time to datetime. First timestamp: {df['open_time'].min()}"
                    )
                except Exception as e:
                    logger.error(f"Error converting open_time to datetime: {e}")
                    logger.error(f"Sample open_time values: {df['open_time'].head()}")
                    raise

                # Ensure close_time has microsecond precision
                try:
                    # Add DEBUG logging for close_time conversion steps
                    logger.debug("\n=== Close Time Conversion Debug ===")
                    logger.debug(f"Original close_time dtype: {df['close_time'].dtype}")
                    logger.debug(
                        f"Original close_time sample: {df['close_time'].iloc[0]}"
                    )

                    # Convert close_time to match REST API format exactly
                    df["close_time"] = df["close_time"].astype(np.int64)
                    logger.debug(f"After int64 conversion: {df['close_time'].iloc[0]}")

                    if len(str(df["close_time"].iloc[0])) == 19:  # nanoseconds
                        logger.debug(
                            "Detected nanosecond precision, converting to microseconds"
                        )
                        df["close_time"] = (
                            df["close_time"] // 1000
                        )  # Convert to microseconds
                        logger.debug(
                            f"After nanosecond conversion: {df['close_time'].iloc[0]}"
                        )

                    # Add 999999 microseconds to match REST API behavior
                    before_final = df["close_time"].iloc[0]
                    # First add the microseconds, then multiply to preserve precision
                    df["close_time"] = (df["close_time"].astype(np.int64) + 999) * 1000
                    logger.debug(f"Before final conversion: {before_final}")
                    logger.debug(f"After final conversion: {df['close_time'].iloc[0]}")
                    logger.debug(f"Final close_time dtype: {df['close_time'].dtype}")

                    # Verify microsecond precision
                    sample_close = pd.Timestamp(df["close_time"].iloc[0])
                    logger.debug(f"Sample close time as timestamp: {sample_close}")
                    logger.debug(
                        f"Sample close time microseconds: {sample_close.microsecond}"
                    )

                    logger.info(
                        f"Converted close_time. Sample value: {df['close_time'].iloc[0]}"
                    )
                except Exception as e:
                    logger.error(f"Error converting close_time: {e}")
                    logger.error(f"Sample close_time values: {df['close_time'].head()}")
                    raise

                # Set index with validation
                df.set_index("open_time", inplace=True)
                if not isinstance(df.index, pd.DatetimeIndex):
                    raise ValueError(
                        f"Index is not DatetimeIndex after setting: {type(df.index)}"
                    )

                # Drop ignored column
                df = df.drop(columns=["ignored"])

                # Convert index to UTC and ensure microsecond precision
                if df.index.tz is None:
                    df.index = df.index.tz_localize("UTC")
                # Ensure index has consistent microsecond precision
                df.index = df.index.map(lambda x: x.replace(microsecond=0))
                logger.info(
                    "Localized index to UTC with consistent microsecond precision"
                )

                # Verify data continuity
                time_diffs = df.index.to_series().diff()
                gaps = time_diffs[time_diffs > timedelta(seconds=1)]
                if not gaps.empty:
                    logger.warning(f"Found {len(gaps)} gaps in data:")
                    for idx, gap in gaps.head().items():
                        logger.warning(f"Gap at {idx}: {gap}")

                return df

            except pd.errors.EmptyDataError:
                logger.error(f"Empty data file for {date}")
                return None
            except Exception as e:
                logger.error(f"Error processing data for {date}: {str(e)}")
                logger.error("Error details:", exc_info=True)
                return None

        except Exception as e:
            logger.error(f"Error downloading data for {date}: {str(e)}")
            logger.error("Error details:", exc_info=True)
            return None

        finally:
            # Cleanup temporary files
            try:
                data_file.unlink(missing_ok=True)
                checksum_file.unlink(missing_ok=True)
                temp_dir.rmdir()
            except Exception as e:
                logger.error(f"Error cleaning up temporary files: {str(e)}")

    def _slice_dataframe_to_exact_range(
        self, df: pd.DataFrame, start_time: datetime, end_time: datetime
    ) -> pd.DataFrame:
        """Slice the dataframe to exactly match the requested time range.

        Args:
            df: The dataframe to slice
            start_time: Start time
            end_time: End time

        Returns:
            Sliced dataframe
        """
        if df.empty:
            return df

        try:
            # Ensure UTC timezone
            start_time = start_time.astimezone(timezone.utc)
            end_time = end_time.astimezone(timezone.utc)

            # Use the centralized time filtering function
            return filter_time_range(df, start_time, end_time)

        except Exception as e:
            self.logger.error(f"Error slicing dataframe: {e}")
            return df

    async def prefetch(
        self, start_time: datetime, end_time: datetime, max_days: int = 5
    ) -> None:
        """Prefetch data for future use with concurrent downloads.

        Args:
            start_time: Start of time range to prefetch
            end_time: End of time range to prefetch
            max_days: Maximum number of days to prefetch

        Note:
            - Validates time range and data availability
            - Skips already cached dates
            - Creates concurrent download tasks
            - Awaits all tasks with error handling
            - Individual task failures don't stop other downloads
            - Blocks until all downloads complete or fail
        """
        # Validate inputs
        start_time = enforce_utc_timestamp(start_time)
        end_time = enforce_utc_timestamp(end_time)
        validate_time_range(start_time, end_time)
        validate_data_availability(start_time, end_time)

        # Get dates that need prefetching
        dates = []
        current = start_time
        days_checked = 0

        while current < end_time and days_checked < max_days:
            # Only prefetch if:
            # 1. Data isn't already cached
            # 2. Data is likely available based on consolidation window
            if not self._validate_cache(
                current, current + timedelta(days=1)
            ) and is_data_likely_available(current):
                dates.append(current)
            current += timedelta(days=1)
            days_checked += 1

        if not dates:
            return

        # Create and track prefetch tasks
        tasks = []
        for date in dates:
            task = asyncio.create_task(
                self._download_and_cache(date, date + timedelta(days=1), None)
            )
            tasks.append(task)
            logger.info(f"Started prefetch for {date.date()}")

        # Wait for tasks to complete, handling exceptions
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error during prefetch: {e}")
            # Let individual task exceptions be handled by error recovery
            pass

    def _create_temp_file(self, prefix: str) -> Path:
        """Create a safe temporary file with unique name.

        Args:
            prefix: Prefix for the temporary file name

        Returns:
            Path to the temporary file
        """
        temp_file = tempfile.NamedTemporaryFile(
            prefix=f"{self.symbol}_{self.interval}_{prefix}_",
            suffix=".tmp",
            delete=False,
            dir=tempfile.gettempdir(),
        )
        return Path(temp_file.name)
