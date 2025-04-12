"""Unified cache manager for market data."""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional, Any
import pandas as pd
import os
import time

from utils.logger_setup import logger


class UnifiedCacheManager:
    """Unified cache manager for all data sources.

    This class provides a single interface for caching data from multiple sources,
    with standardized cache keys, path generation, and metadata handling.

    Features:
    - Consistent cache key generation
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

    def _save_metadata(self) -> None:
        """Save metadata to file."""
        logger.debug(f"_save_metadata starting with {len(self.metadata)} entries")
        try:
            metadata_path = self._get_metadata_path()
            file_size_before = (
                os.path.getsize(metadata_path) if os.path.exists(metadata_path) else 0
            )
            logger.debug(
                f"Metadata will be saved to {metadata_path} (current size: {file_size_before} bytes)"
            )

            # Serialize metadata to JSON
            logger.debug("Serializing metadata to JSON")
            json_start = time.time()
            try:
                json_data = json.dumps(self.metadata, indent=2)
                json_size = len(json_data)
                json_elapsed = time.time() - json_start
                logger.debug(
                    f"JSON serialization completed in {json_elapsed:.4f}s for {json_size} bytes"
                )

                # Log warning if JSON size is very large
                if json_size > 10 * 1024 * 1024:  # 10MB
                    logger.warning(
                        f"Metadata JSON is extremely large: {json_size / (1024*1024):.2f} MB"
                    )
            except Exception as json_err:
                logger.error(f"JSON serialization failed: {json_err}")
                return

            # Write to temporary file first
            temp_path = metadata_path.with_suffix(".tmp")
            logger.debug(f"Writing metadata to temporary file at {temp_path}")
            write_start = time.time()

            try:
                with open(temp_path, "w") as f:
                    f.write(json_data)

                write_elapsed = time.time() - write_start
                logger.debug(
                    f"Temporary metadata file write completed in {write_elapsed:.4f}s"
                )

                # Verify the file was written correctly
                if not temp_path.exists():
                    logger.error(
                        f"Temporary metadata file was not created: {temp_path}"
                    )
                    return

                temp_size = os.path.getsize(temp_path)
                if temp_size == 0:
                    logger.error("Temporary metadata file is empty")
                    return

                # Rename temporary file to actual metadata file (atomic operation)
                logger.debug(f"Renaming temporary file to {metadata_path}")
                temp_path.replace(metadata_path)

            except Exception as write_err:
                logger.error(f"Metadata file write failed: {write_err}")
                return

            # Report detailed stats
            total_elapsed = json_elapsed + write_elapsed
            logger.debug(
                f"Saved cache metadata: {len(self.metadata)} entries in {total_elapsed:.4f}s total"
            )

            # Check file size after write
            if metadata_path.exists():
                file_size_after = os.path.getsize(metadata_path)
                logger.debug(
                    f"Final metadata file size: {file_size_after} bytes (change: {file_size_after - file_size_before} bytes)"
                )

        except Exception as e:
            logger.error(f"Error saving metadata: {e}")
            return

    def get_cache_key(
        self,
        symbol: str,
        interval: str,
        date: datetime,
        provider: str = "BINANCE",
        chart_type: str = "KLINES",
        market_type: str = "spot",
    ) -> str:
        """Generate a standardized cache key.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Date for the data
            provider: Data provider
            chart_type: Chart type
            market_type: Market type

        Returns:
            Standardized cache key
        """
        # Ensure all components are properly formatted
        symbol = symbol.upper()
        provider = provider.upper()
        chart_type = chart_type.upper()
        market_type = market_type.lower()
        interval = str(interval).lower()

        # Format date to YYYYMMDD format
        date_str = date.strftime("%Y%m%d")

        # Create a cache key that uniquely identifies this data
        cache_key = (
            f"{provider}_{chart_type}_{market_type}_{symbol}_{interval}_{date_str}"
        )

        return cache_key

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get the file path for a cache entry.

        Args:
            cache_key: Cache key

        Returns:
            Path to the cache file
        """
        # Parse components from the cache key
        parts = cache_key.split("_")
        if len(parts) < 6:
            logger.warning(f"Invalid cache key format: {cache_key}")
            # Fallback path
            return self.cache_dir / f"{cache_key}.parquet"

        provider, chart_type, market_type, symbol, interval, date_str = parts[0:6]

        # Create a directory structure based on components for better organization
        # provider/chart_type/market_type/symbol/interval/date_str.parquet
        cache_path = (
            self.cache_dir
            / provider
            / chart_type
            / market_type
            / symbol
            / interval
            / f"{date_str}.parquet"
        )

        # Ensure directory exists
        os.makedirs(cache_path.parent, exist_ok=True)

        return cache_path

    def load_from_cache(
        self,
        symbol: str,
        interval: str,
        date: datetime,
        provider: str = "BINANCE",
        chart_type: str = "KLINES",
        market_type: str = "spot",
    ) -> Optional[pd.DataFrame]:
        """Load data from cache.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Date for the data
            provider: Data provider
            chart_type: Chart type
            market_type: Market type

        Returns:
            DataFrame with the cached data, or None if not found or invalid
        """
        # Generate cache key
        cache_key = self.get_cache_key(
            symbol, interval, date, provider, chart_type, market_type
        )

        # Get the file path
        cache_path = self._get_cache_path(cache_key)

        # Check if the file exists
        if not cache_path.exists():
            logger.debug(f"Cache miss for {cache_key}")
            return None

        try:
            # Load metadata for this cache entry if available
            metadata = self.metadata.get(cache_key, {})

            # Check if this cache entry is marked as invalid
            if metadata.get("is_invalid", False):
                reason = metadata.get("invalid_reason", "unknown reason")
                logger.debug(f"Skipping invalid cache entry {cache_key}: {reason}")
                return None

            # Try to read the parquet file
            df = pd.read_parquet(cache_path)

            # Validate that the DataFrame has data
            if df.empty:
                logger.debug(f"Empty DataFrame in cache for {cache_key}")
                self._mark_cache_invalid(cache_key, "Empty DataFrame")
                return None

            # Basic validation checks
            if "open_time" in df.columns and chart_type == "KLINES":
                # Check if the data is properly sorted
                if not df["open_time"].is_monotonic_increasing:
                    logger.warning(f"Cache entry {cache_key} has unsorted timestamps")
                    # Sort the DataFrame for the user
                    df = df.sort_values("open_time").reset_index(drop=True)

            elif "funding_time" in df.columns and chart_type == "FUNDING_RATE":
                # Check funding rate data for appropriate columns
                required_columns = ["symbol", "funding_time", "funding_rate"]
                missing_columns = [
                    col for col in required_columns if col not in df.columns
                ]
                if missing_columns:
                    logger.warning(
                        f"Cache entry {cache_key} missing columns: {missing_columns}"
                    )
                    self._mark_cache_invalid(
                        cache_key, f"Missing columns: {missing_columns}"
                    )
                    return None

            logger.debug(f"Loaded {len(df)} rows from cache for {cache_key}")
            return df

        except Exception as e:
            logger.error(f"Error loading cache file for {cache_key}: {e}")
            # Mark this cache entry as invalid
            self._mark_cache_invalid(cache_key, str(e))
            return None

    def save_to_cache(
        self,
        df: pd.DataFrame,
        symbol: str,
        interval: str,
        date: datetime,
        provider: str = "BINANCE",
        chart_type: str = "KLINES",
        market_type: str = "spot",
    ) -> bool:
        """Save data to cache.

        Args:
            df: DataFrame to cache
            symbol: Trading pair symbol
            interval: Time interval
            date: Date for the data
            provider: Data provider
            chart_type: Chart type
            market_type: Market type

        Returns:
            True if successfully saved, False otherwise
        """
        if df.empty:
            logger.warning("Cannot cache empty DataFrame")
            return False

        # Generate cache key
        cache_key = self.get_cache_key(
            symbol, interval, date, provider, chart_type, market_type
        )

        # Get the file path
        cache_path = self._get_cache_path(cache_key)

        try:
            # Ensure the directory exists
            os.makedirs(cache_path.parent, exist_ok=True)

            # Prepare metadata
            metadata_entry = {
                "symbol": symbol,
                "interval": interval,
                "date": date.isoformat(),
                "provider": provider,
                "chart_type": chart_type,
                "market_type": market_type,
                "rows": len(df),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "is_invalid": False,
            }

            # Add column names to metadata
            metadata_entry["columns"] = list(df.columns)

            # Add time range information if available
            if "open_time" in df.columns and not df.empty:
                metadata_entry["start_time"] = df["open_time"].min().isoformat()
                metadata_entry["end_time"] = df["open_time"].max().isoformat()
            elif "funding_time" in df.columns and not df.empty:
                metadata_entry["start_time"] = df["funding_time"].min().isoformat()
                metadata_entry["end_time"] = df["funding_time"].max().isoformat()

            # Calculate file size estimate
            size_estimate = df.memory_usage(deep=True).sum()
            metadata_entry["size_estimate_bytes"] = int(size_estimate)
            logger.debug(
                f"Preparing to save {len(df)} rows (~{size_estimate / 1024 / 1024:.2f} MB) to {cache_path}"
            )

            # Write the DataFrame to parquet
            df.to_parquet(cache_path, index=False)

            # Update metadata
            metadata_entry["file_size_bytes"] = os.path.getsize(cache_path)
            self.metadata[cache_key] = metadata_entry

            # Save metadata
            self._save_metadata()

            logger.debug(
                f"Successfully cached {len(df)} rows to {cache_path} ({metadata_entry['file_size_bytes'] / 1024 / 1024:.2f} MB)"
            )
            return True

        except Exception as e:
            logger.error(f"Error saving to cache for {cache_key}: {e}")
            return False

    def invalidate_cache(
        self,
        symbol: str,
        interval: str,
        date: datetime,
        provider: str = "BINANCE",
        chart_type: str = "KLINES",
        market_type: str = "spot",
    ) -> None:
        """Invalidate a cache entry.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Date for the data
            provider: Data provider
            chart_type: Chart type
            market_type: Market type
        """
        # Generate cache key
        cache_key = self.get_cache_key(
            symbol, interval, date, provider, chart_type, market_type
        )

        # Get the file path
        cache_path = self._get_cache_path(cache_key)

        # Check if the file exists
        if not cache_path.exists():
            logger.debug(f"Cannot invalidate non-existent cache: {cache_key}")
            return

        try:
            # Delete the file
            cache_path.unlink()
            logger.debug(f"Deleted cache file: {cache_path}")

            # Update metadata
            if cache_key in self.metadata:
                self.metadata.pop(cache_key)
                self._save_metadata()

        except Exception as e:
            logger.error(f"Error invalidating cache for {cache_key}: {e}")

    def purge_expired_cache(self, max_age_days: int = 30) -> int:
        """Remove cache entries older than the specified age.

        Args:
            max_age_days: Maximum age in days for cache entries

        Returns:
            Number of entries purged
        """
        # Calculate the cutoff date
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        logger.info(f"Purging cache entries older than {cutoff_date.isoformat()}")

        # Track the number of entries purged
        purged_count = 0

        # Iterate through metadata entries
        keys_to_remove = []
        for cache_key, entry in self.metadata.items():
            try:
                # Parse the creation date
                if "created_at" in entry:
                    created_at = datetime.fromisoformat(entry["created_at"])
                    if created_at < cutoff_date:
                        # This entry is too old
                        cache_path = self._get_cache_path(cache_key)
                        if cache_path.exists():
                            # Delete the file
                            cache_path.unlink()
                            logger.debug(f"Purged expired cache file: {cache_path}")
                        keys_to_remove.append(cache_key)
                        purged_count += 1
            except Exception as e:
                logger.error(f"Error processing cache entry {cache_key}: {e}")
                # Mark for removal regardless due to error
                keys_to_remove.append(cache_key)

        # Remove the entries from metadata
        for key in keys_to_remove:
            self.metadata.pop(key, None)

        # Save updated metadata
        if keys_to_remove:
            self._save_metadata()

        logger.info(f"Purged {purged_count} expired cache entries")
        return purged_count

    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get statistics about the cache.

        Returns:
            Dictionary with cache statistics
        """
        stats = {
            "total_entries": len(self.metadata),
            "total_size_bytes": 0,
            "entry_count_by_provider": {},
            "entry_count_by_chart_type": {},
            "entry_count_by_market_type": {},
            "entry_count_by_symbol": {},
            "entry_count_by_interval": {},
            "invalid_entries": 0,
        }

        # Process metadata entries
        for cache_key, entry in self.metadata.items():
            # Add to total size
            if "file_size_bytes" in entry:
                stats["total_size_bytes"] += entry["file_size_bytes"]

            # Count by provider
            provider = entry.get("provider", "unknown")
            stats["entry_count_by_provider"][provider] = (
                stats["entry_count_by_provider"].get(provider, 0) + 1
            )

            # Count by chart type
            chart_type = entry.get("chart_type", "unknown")
            stats["entry_count_by_chart_type"][chart_type] = (
                stats["entry_count_by_chart_type"].get(chart_type, 0) + 1
            )

            # Count by market type
            market_type = entry.get("market_type", "unknown")
            stats["entry_count_by_market_type"][market_type] = (
                stats["entry_count_by_market_type"].get(market_type, 0) + 1
            )

            # Count by symbol
            symbol = entry.get("symbol", "unknown")
            stats["entry_count_by_symbol"][symbol] = (
                stats["entry_count_by_symbol"].get(symbol, 0) + 1
            )

            # Count by interval
            interval = entry.get("interval", "unknown")
            stats["entry_count_by_interval"][interval] = (
                stats["entry_count_by_interval"].get(interval, 0) + 1
            )

            # Count invalid entries
            if entry.get("is_invalid", False):
                stats["invalid_entries"] += 1

        # Add some helpful derived statistics
        stats["total_size_mb"] = stats["total_size_bytes"] / (1024 * 1024)
        stats["avg_entry_size_kb"] = stats["total_size_bytes"] / (
            1024 * max(1, len(self.metadata))
        )

        return stats

    def _mark_cache_invalid(self, cache_key: str, reason: str) -> None:
        """Mark a cache entry as invalid.

        Args:
            cache_key: Cache key
            reason: Reason for invalidation
        """
        if cache_key in self.metadata:
            self.metadata[cache_key]["is_invalid"] = True
            self.metadata[cache_key]["invalid_reason"] = reason
            self.metadata[cache_key]["invalidated_at"] = datetime.now(
                timezone.utc
            ).isoformat()
            self._save_metadata()
