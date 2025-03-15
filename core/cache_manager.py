"""Unified cache manager for market data."""

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple, Any
import pandas as pd
import pyarrow as pa

from ml_feature_set.utils.logger_setup import get_logger
from .vision_constraints import validate_cache_checksum

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


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
        with open(metadata_file, "w") as f:
            json.dump(self.metadata, f, indent=2)

    def get_cache_path(self, symbol: str, interval: str, date: datetime) -> Path:
        """Get cache file path following the simplified structure.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            Path to cache file
        """
        year_month = date.strftime("%Y%m")
        return self.data_dir / symbol / interval / f"{year_month}.arrow"

    def get_cache_key(self, symbol: str, interval: str, date: datetime) -> str:
        """Generate cache key.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            Cache key string
        """
        return f"{symbol}_{interval}_{date.strftime('%Y%m')}"

    async def save_to_cache(self, df: pd.DataFrame, symbol: str, interval: str, date: datetime) -> Tuple[str, int]:
        """Save DataFrame to cache.

        Args:
            df: DataFrame to cache
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

        Returns:
            Tuple of (checksum, record_count)
        """
        cache_path = self.get_cache_path(symbol, interval, date)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to Arrow table
        table = pa.Table.from_pandas(df.reset_index())

        # Save to Arrow file
        with pa.OSFile(str(cache_path), "wb") as sink:
            with pa.ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)

        # Calculate checksum and record count
        checksum = hashlib.sha256(cache_path.read_bytes()).hexdigest()
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
        self._save_metadata()

        logger.info(f"Cached {record_count} records to {cache_path}")
        return checksum, record_count

    async def load_from_cache(self, symbol: str, interval: str, date: datetime) -> Optional[pd.DataFrame]:
        """Load data from cache if available.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Target date

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

        if not validate_cache_checksum(cache_path, cache_info["checksum"]):
            logger.warning(f"Cache checksum mismatch: {cache_path}")
            return None

        try:
            # Read Arrow file
            with pa.memory_map(str(cache_path), "r") as source:
                with pa.ipc.open_file(source) as reader:
                    table = reader.read_all()

            # Convert to DataFrame
            df = table.to_pandas()
            df.set_index("open_time", inplace=True)
            df.index = pd.to_datetime(df.index, utc=True)

            logger.info(f"Loaded {len(df)} records from cache: {cache_path}")
            return df

        except Exception as e:
            logger.error(f"Failed to load cache: {e}")
            return None

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
