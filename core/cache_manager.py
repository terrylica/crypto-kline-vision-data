"""Unified cache manager for market data."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Any
import pandas as pd
import os
import asyncio

from utils.logger_setup import logger


class UnifiedCacheManager:
    """Unified cache manager for all data sources.

    This class provides a single interface for caching data from multiple sources,
    with standardized cache keys, path generation, and metadata handling.

    Features:
    - Consistent cache key generation
    - Concurrent access support
    - Metadata storage
    - Integrity validation
    - Cache invalidation
    """

    def __init__(self, cache_dir: Path, create_dirs: bool = True):
        """Initialize the cache manager.

        Args:
            cache_dir: Base directory for cache storage
            create_dirs: Whether to create cache directory structure
        """
        self.cache_dir = Path(cache_dir)
        self.metadata: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = asyncio.Lock()

        # Create directories if needed
        if create_dirs:
            try:
                os.makedirs(self.cache_dir, exist_ok=True)
                logger.debug(f"Cache directory created: {self.cache_dir}")
            except OSError as e:
                logger.error(f"Failed to create cache directory: {e}")

        # Load existing metadata
        try:
            self._load_metadata()
        except Exception as e:
            logger.warning(f"Failed to load cache metadata, starting fresh: {e}")

    def _get_metadata_path(self) -> Path:
        """Get path to metadata file.

        Returns:
            Path to metadata file
        """
        return self.cache_dir / "cache_metadata.json"

    def _load_metadata(self) -> None:
        """Load metadata from file."""
        metadata_path = self._get_metadata_path()
        if metadata_path.exists():
            try:
                with open(metadata_path, "r") as f:
                    self.metadata = json.load(f)
                logger.debug(f"Loaded cache metadata: {len(self.metadata)} entries")
            except Exception as e:
                logger.error(f"Failed to load metadata: {e}")
                self.metadata = {}
        else:
            logger.debug("No metadata file found, starting fresh")
            self.metadata = {}

    async def _save_metadata(self) -> None:
        """Save metadata to file."""
        async with self._cache_lock:
            try:
                metadata_path = self._get_metadata_path()
                with open(metadata_path, "w") as f:
                    json.dump(self.metadata, f, indent=2)
                logger.debug(f"Saved cache metadata: {len(self.metadata)} entries")
            except Exception as e:
                logger.error(f"Failed to save metadata: {e}")

    def get_cache_key(
        self,
        symbol: str,
        interval: str,
        date: datetime,
        provider: str = "BINANCE",
        chart_type: str = "KLINES",
    ) -> str:
        """Generate a standardized cache key.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Reference date
            provider: Data provider
            chart_type: Chart type

        Returns:
            Cache key string
        """
        # Standardize inputs
        symbol = symbol.upper()
        interval = interval.lower()
        date_str = date.strftime("%Y-%m-%d")
        provider = provider.upper()
        chart_type = chart_type.upper()

        # Create a key that incorporates all components
        return f"{provider}_{chart_type}_{symbol}_{interval}_{date_str}"

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get filesystem path for a cache key.

        Args:
            cache_key: Cache key string

        Returns:
            Path to cache file
        """
        # Use cache key components to create a directory structure
        components = cache_key.split("_")

        if len(components) >= 5:
            provider, chart_type, symbol, interval, date_str = components[:5]

            # Create path with hierarchical structure
            return (
                self.cache_dir
                / provider
                / chart_type
                / symbol
                / interval
                / f"{date_str}.arrow"
            )
        else:
            # Fallback for legacy cache keys
            return self.cache_dir / f"{cache_key}.arrow"

    async def load_from_cache(
        self,
        symbol: str,
        interval: str,
        date: datetime,
        provider: str = "BINANCE",
        chart_type: str = "KLINES",
    ) -> Optional[pd.DataFrame]:
        """Load data from cache.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Reference date
            provider: Data provider
            chart_type: Chart type

        Returns:
            DataFrame with cached data or None if not found
        """
        cache_key = self.get_cache_key(symbol, interval, date, provider, chart_type)
        cache_path = self._get_cache_path(cache_key)

        if not cache_path.exists():
            logger.debug(f"Cache miss - file not found: {cache_path}")
            return None

        try:
            # Check metadata for validation info
            if cache_key in self.metadata:
                if not self.metadata[cache_key].get("is_valid", True):
                    logger.warning(f"Skipping invalid cache: {cache_key}")
                    return None

            # Load from Arrow format
            df = pd.read_feather(cache_path)

            # Ensure index is a DatetimeIndex for time-series data
            if "open_time" in df.columns:
                df = df.set_index("open_time")
                # Ensure timezone info
                if df.index.tzinfo is None:
                    df.index = df.index.tz_localize(timezone.utc)

            logger.debug(f"Cache hit: {cache_key}, shape: {df.shape}")
            return df

        except Exception as e:
            logger.error(f"Failed to load cache: {cache_key}, error: {e}")
            # Mark as invalid in metadata
            await self._mark_cache_invalid(cache_key, str(e))
            return None

    async def save_to_cache(
        self,
        df: pd.DataFrame,
        symbol: str,
        interval: str,
        date: datetime,
        provider: str = "BINANCE",
        chart_type: str = "KLINES",
    ) -> bool:
        """Save data to cache.

        Args:
            df: DataFrame to cache
            symbol: Trading pair symbol
            interval: Time interval
            date: Reference date
            provider: Data provider
            chart_type: Chart type

        Returns:
            True if successful, False otherwise
        """
        if df.empty:
            logger.debug("Not caching empty DataFrame")
            return False

        cache_key = self.get_cache_key(symbol, interval, date, provider, chart_type)
        cache_path = self._get_cache_path(cache_key)

        # Create parent directories if they don't exist
        try:
            os.makedirs(cache_path.parent, exist_ok=True)
        except Exception as e:
            logger.error(f"Failed to create cache directory: {e}")
            return False

        try:
            # Reset index to include open_time as a column
            df_reset = df.reset_index()

            # Save to Arrow format
            df_reset.to_feather(cache_path)

            # Update metadata
            metadata_entry = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol,
                "interval": interval,
                "provider": provider,
                "chart_type": chart_type,
                "date": date.isoformat(),
                "rows": len(df),
                "is_valid": True,
            }

            async with self._cache_lock:
                self.metadata[cache_key] = metadata_entry
                await self._save_metadata()

            logger.debug(f"Cached {len(df)} rows to {cache_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to save cache: {cache_key}, error: {e}")
            return False

    async def _mark_cache_invalid(self, cache_key: str, reason: str) -> None:
        """Mark a cache entry as invalid.

        Args:
            cache_key: Cache key to invalidate
            reason: Reason for invalidation
        """
        async with self._cache_lock:
            if cache_key in self.metadata:
                self.metadata[cache_key]["is_valid"] = False
                self.metadata[cache_key]["invalidation_reason"] = reason
                self.metadata[cache_key]["invalidated_at"] = datetime.now(
                    timezone.utc
                ).isoformat()
                await self._save_metadata()
                logger.debug(f"Marked cache as invalid: {cache_key}, reason: {reason}")

    def invalidate_cache(
        self,
        symbol: str,
        interval: str,
        date: datetime,
        provider: str = "BINANCE",
        chart_type: str = "KLINES",
    ) -> None:
        """Invalidate a cache entry.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Reference date
            provider: Data provider
            chart_type: Chart type
        """
        cache_key = self.get_cache_key(symbol, interval, date, provider, chart_type)
        cache_path = self._get_cache_path(cache_key)

        if cache_path.exists():
            try:
                # Try to delete the file
                os.remove(cache_path)
                logger.debug(f"Deleted invalidated cache: {cache_path}")
            except Exception as e:
                logger.error(f"Failed to delete cache file: {e}")

        # Also mark as invalid in metadata
        asyncio.create_task(self._mark_cache_invalid(cache_key, "Manual invalidation"))

    async def purge_expired_cache(self, max_age_days: int = 30) -> int:
        """Purge expired cache entries.

        Args:
            max_age_days: Maximum age of cache entries in days

        Returns:
            Number of entries purged
        """
        purged_count = 0
        now = datetime.now(timezone.utc)
        keys_to_purge = []

        async with self._cache_lock:
            # Identify keys to purge
            for key, metadata in self.metadata.items():
                try:
                    created_at = datetime.fromisoformat(metadata.get("created_at", ""))
                    age_days = (now - created_at).days
                    if age_days > max_age_days or not metadata.get("is_valid", True):
                        keys_to_purge.append(key)
                except Exception:
                    # If we can't parse the date, consider it expired
                    keys_to_purge.append(key)

            # Delete files and update metadata
            for key in keys_to_purge:
                try:
                    cache_path = self._get_cache_path(key)
                    if cache_path.exists():
                        os.remove(cache_path)
                    del self.metadata[key]
                    purged_count += 1
                except Exception as e:
                    logger.error(f"Failed to purge cache: {key}, error: {e}")

            # Save updated metadata
            if purged_count > 0:
                await self._save_metadata()
                logger.info(f"Purged {purged_count} expired cache entries")

        return purged_count

    async def get_cache_statistics(self) -> Dict[str, Any]:
        """Get statistics about the cache.

        Returns:
            Dictionary with cache statistics
        """
        stats = {
            "total_entries": len(self.metadata),
            "valid_entries": 0,
            "invalid_entries": 0,
            "total_rows": 0,
            "providers": set(),
            "chart_types": set(),
            "symbols": set(),
            "intervals": set(),
        }

        for key, metadata in self.metadata.items():
            if metadata.get("is_valid", True):
                stats["valid_entries"] += 1
                stats["total_rows"] += metadata.get("rows", 0)
            else:
                stats["invalid_entries"] += 1

            stats["providers"].add(metadata.get("provider", "UNKNOWN"))
            stats["chart_types"].add(metadata.get("chart_type", "UNKNOWN"))
            stats["symbols"].add(metadata.get("symbol", "UNKNOWN"))
            stats["intervals"].add(metadata.get("interval", "UNKNOWN"))

        # Convert sets to lists for JSON serialization
        for key in ["providers", "chart_types", "symbols", "intervals"]:
            stats[key] = list(stats[key])

        return stats
