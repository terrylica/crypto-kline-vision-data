"""Unified cache manager for market data."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple, Any, Sequence
import pandas as pd
import pyarrow as pa

from utils.logger_setup import get_logger
from utils.cache_validator import CacheValidator, SafeMemoryMap, CacheKeyManager
from utils.validation import DataFrameValidator
from utils.config import (
    OUTPUT_DTYPES,
    CANONICAL_INDEX_NAME,
    DEFAULT_TIMEZONE,
    standardize_column_names,
)
from utils.time_alignment import TimeRangeManager

logger = get_logger(__name__, "INFO", show_path=False)


class UnifiedCacheManager:
    """Centralized cache management with simplified directory structure.

    Directory Structure:
    /cache_dir
        /data
            /SYMBOL
                /INTERVAL
                    /YYYYMM.arrow
        /metadata
            cache_index.json
    """

    def __init__(self, cache_dir: Path):
        """Initialize cache manager.

        Args:
            cache_dir: Base cache directory
        """
        self.cache_dir = cache_dir
        self.data_dir = cache_dir / "data"
        self.metadata_dir = cache_dir / "metadata"

        # Create directory structure
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

        # Load metadata
        self.metadata = self._load_metadata()

    def _load_metadata(self) -> Dict[str, Any]:
        """Load cache metadata from disk."""
        metadata_file = self.metadata_dir / "cache_index.json"
        if metadata_file.exists():
            try:
                with open(metadata_file, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Corrupted metadata file, creating new")
                return {}
        return {}

    def _save_metadata(self) -> None:
        """Save cache metadata to disk."""
        metadata_file = self.metadata_dir / "cache_index.json"
        logger.debug(f"Saving metadata to {metadata_file}")
        try:
            with open(metadata_file, "w") as f:
                json.dump(self.metadata, f, indent=2)
            logger.debug(f"Metadata saved successfully to {metadata_file}")
        except Exception as e:
            logger.error(f"Error writing metadata to {metadata_file}: {e}")
            # Check if directory exists
            if not self.metadata_dir.exists():
                logger.error(f"Metadata directory does not exist: {self.metadata_dir}")
            # Check permissions
            try:
                if self.metadata_dir.exists():
                    logger.debug(
                        f"Metadata directory permissions: {self.metadata_dir.stat().st_mode & 0o777:o}"
                    )
            except Exception as perm_err:
                logger.error(
                    f"Error checking metadata directory permissions: {perm_err}"
                )
            raise

    def get_cache_path(self, symbol: str, interval: str, date: datetime) -> Path:
        """Get cache file path following the simplified structure.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            Path to cache file
        """
        return CacheKeyManager.get_cache_path(self.data_dir, symbol, interval, date)

    def get_cache_key(self, symbol: str, interval: str, date: datetime) -> str:
        """Generate cache key.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            Cache key string
        """
        return CacheKeyManager.get_cache_key(symbol, interval, date)

    async def save_to_cache(
        self, df: pd.DataFrame, symbol: str, interval: str, date: datetime
    ) -> Tuple[str, int]:
        """Save DataFrame to cache.

        Args:
            df: DataFrame to cache
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            Tuple of (checksum, record_count)
        """
        # Ensure date has proper timezone using TimeRangeManager
        date = TimeRangeManager.enforce_utc_timezone(date)

        # Log input data for debugging
        logger.debug(
            f"Attempting to cache data for {symbol} {interval} {date.strftime('%Y-%m-%d')}"
        )
        logger.debug(f"DataFrame shape before caching: {df.shape}")
        logger.debug(f"DataFrame index name: {df.index.name}")
        logger.debug(f"DataFrame has duplicates: {df.index.has_duplicates}")
        logger.debug(f"DataFrame is monotonic: {df.index.is_monotonic_increasing}")

        # Handle duplicate timestamps by keeping the first occurrence
        if not df.empty and df.index.has_duplicates:
            logger.debug(
                f"Removing {df.index.duplicated().sum()} duplicate timestamps before caching"
            )
            df = df[~df.index.duplicated(keep="first")]

        # Sort the DataFrame by index if it's not monotonically increasing
        if not df.empty and not df.index.is_monotonic_increasing:
            logger.debug("Sorting DataFrame by index before caching")
            df = df.sort_index()

        # Validate DataFrame before caching
        try:
            DataFrameValidator.validate_dataframe(df)
        except ValueError as e:
            logger.warning(f"Invalid DataFrame, not caching: {e}")
            # Log more details about the DataFrame for debugging
            if not df.empty:
                logger.debug(f"DataFrame dtypes: {df.dtypes}")
                logger.debug(f"DataFrame columns: {df.columns.tolist()}")
                logger.debug(
                    f"DataFrame index range: {df.index.min()} to {df.index.max()}"
                )
            raise

        cache_path = self.get_cache_path(symbol, interval, date)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Cache path: {cache_path}")

        # Convert to Arrow table - Handling reset_index carefully
        try:
            logger.debug("Converting DataFrame to Arrow table")

            # Check if the index name conflicts with a column name
            if df.index.name is not None and df.index.name in df.columns:
                logger.debug(
                    f"Index name '{df.index.name}' conflicts with column, renaming index before reset"
                )
                df.index.name = f"{df.index.name}_idx"

            # Reset index with a temporary name if it has no name
            if df.index.name is None:
                logger.debug("Index has no name, using temporary name for reset")
                df.index.name = "temp_index"

            # Reset index and check for column conflicts
            reset_df = df.reset_index()
            logger.debug(f"After reset_index, columns are: {reset_df.columns.tolist()}")

            table = pa.Table.from_pandas(reset_df)

            # Save to Arrow file
            with pa.OSFile(str(cache_path), "wb") as sink:
                with pa.ipc.new_file(sink, table.schema) as writer:
                    writer.write_table(table)

            logger.debug(f"Successfully wrote data to {cache_path}")
        except Exception as e:
            logger.error(f"Error saving cache file: {e}")
            raise

        # Calculate checksum and record count
        checksum = CacheValidator.calculate_checksum(cache_path)
        record_count = len(df)

        # Update metadata
        cache_key = self.get_cache_key(symbol, interval, date)
        self.metadata[cache_key] = {
            "symbol": symbol,
            "interval": interval,
            "year_month": date.strftime("%Y%m"),
            "file_path": str(cache_path.relative_to(self.cache_dir)),
            "checksum": checksum,
            "record_count": record_count,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

        # Log metadata before saving
        logger.debug(f"Updating metadata for key: {cache_key}")
        logger.debug(f"Metadata content: {self.metadata[cache_key]}")

        try:
            self._save_metadata()
            logger.debug("Successfully saved metadata")
        except Exception as e:
            logger.error(f"Error saving metadata: {e}")
            raise

        logger.info(f"Cached {record_count} records to {cache_path}")
        return checksum, record_count

    async def load_from_cache(
        self,
        symbol: str,
        interval: str,
        date: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> Optional[pd.DataFrame]:
        """Load data from cache if available.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date
            columns: Optional list of columns to load

        Returns:
            DataFrame if cache hit, None if cache miss or invalid
        """
        cache_key = self.get_cache_key(symbol, interval, date)
        cache_info = self.metadata.get(cache_key)

        if not cache_info:
            return None

        cache_path = self.cache_dir / cache_info["file_path"]

        # Validate cache
        if not cache_path.exists():
            logger.warning(f"Cache file missing: {cache_path}")
            return None

        if not CacheValidator.validate_cache_checksum(
            cache_path, cache_info["checksum"]
        ):
            logger.warning(f"Cache checksum mismatch: {cache_path}")
            return None

        # Use the async version of the safe reader for Arrow files for better performance
        df = await CacheValidator.safely_read_arrow_file_async(cache_path, columns)
        if df is None:
            return None

        # Ensure index has correct timezone using TimeRangeManager
        if isinstance(df.index, pd.DatetimeIndex):
            new_index = pd.DatetimeIndex(
                [
                    TimeRangeManager.enforce_utc_timezone(dt)
                    for dt in df.index.to_pydatetime()
                ],
                name=df.index.name,
            )
            df.index = new_index

        # Perform validation on loaded data
        try:
            DataFrameValidator.validate_dataframe(df)
        except ValueError as e:
            logger.warning(f"Invalid cached data: {e}")
            return None

        logger.info(f"Loaded {len(df)} records from cache: {cache_path}")
        return df

    def invalidate_cache(self, symbol: str, interval: str, date: datetime) -> None:
        """Invalidate cache entry.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date
        """
        cache_key = self.get_cache_key(symbol, interval, date)
        if cache_key in self.metadata:
            cache_path = self.cache_dir / self.metadata[cache_key]["file_path"]
            if cache_path.exists():
                cache_path.unlink()
            del self.metadata[cache_key]
            self._save_metadata()
            logger.info(f"Invalidated cache: {cache_key}")
