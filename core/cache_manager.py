"""Unified cache manager for market data."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Any
import pandas as pd
import os
import asyncio
import time
import gc
import psutil

from utils.logger_setup import logger
from utils.config import MAX_TIMEOUT


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
            logger.debug(f"_save_metadata starting with {len(self.metadata)} entries")
            try:
                metadata_path = self._get_metadata_path()
                file_size_before = (
                    os.path.getsize(metadata_path)
                    if os.path.exists(metadata_path)
                    else 0
                )
                logger.debug(
                    f"Metadata will be saved to {metadata_path} (current size: {file_size_before} bytes)"
                )

                # Check system resources before proceeding
                process = psutil.Process()
                mem_before = process.memory_info().rss / 1024 / 1024
                logger.debug(f"Memory usage before metadata save: {mem_before:.2f} MB")
                logger.debug(
                    f"Available disk space: {psutil.disk_usage(metadata_path.parent).free / (1024*1024):.2f} MB"
                )

                # Serialize metadata to JSON - this can be slow with large metadata
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

                # Write to temporary file first with timeout protection
                temp_path = metadata_path.with_suffix(".tmp")
                logger.debug(f"Writing metadata to temporary file at {temp_path}")
                write_start = time.time()

                try:
                    # Use a timeout for the file write operation
                    async def write_with_timeout():
                        """Write metadata with timeout protection."""
                        with open(temp_path, "w") as f:
                            f.write(json_data)

                    # Set a timeout for the file write operation
                    await asyncio.wait_for(write_with_timeout(), timeout=MAX_TIMEOUT)

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

                except asyncio.TimeoutError:
                    logger.error(
                        f"Metadata file write operation timed out after {MAX_TIMEOUT}s"
                    )
                    # Force garbage collection to free resources
                    collected = gc.collect()
                    logger.debug(
                        f"Emergency garbage collection after timeout: collected {collected} objects"
                    )

                    # Try to clean up the temporary file if it exists
                    if temp_path.exists():
                        try:
                            temp_path.unlink()
                            logger.debug(
                                f"Cleaned up temporary metadata file after timeout"
                            )
                        except Exception as cleanup_err:
                            logger.error(
                                f"Failed to clean up temporary file: {cleanup_err}"
                            )
                    return
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

                # Check memory after operation
                mem_after = process.memory_info().rss / 1024 / 1024
                logger.debug(
                    f"Memory usage after metadata save: {mem_after:.2f} MB (change: {mem_after - mem_before:.2f} MB)"
                )

                # If the operation was slow, log more details
                if total_elapsed > 1.0:
                    logger.warning(
                        f"Metadata save operation was slow ({total_elapsed:.2f}s), size: {len(json_data)/1024:.1f}KB"
                    )
                    # Count entries by type for debugging
                    provider_counts = {}
                    for key in self.metadata:
                        parts = key.split("_")
                        if len(parts) > 0:
                            provider = parts[0]
                            if provider not in provider_counts:
                                provider_counts[provider] = 0
                            provider_counts[provider] += 1
                    logger.debug(f"Metadata entries by provider: {provider_counts}")
            except Exception as e:
                logger.error(f"Failed to save metadata: {e}")
                # Log more details about the exception
                import traceback

                logger.error(f"Metadata save error details: {traceback.format_exc()}")

                # Emergency garbage collection
                collected = gc.collect()
                logger.debug(
                    f"Emergency garbage collection after error: collected {collected} objects"
                )

    def get_cache_key(
        self,
        symbol: str,
        interval: str,
        date: datetime,
        provider: str = "BINANCE",
        chart_type: str = "KLINES",
        market_type: str = "spot",
    ) -> str:
        """Get cache key for a data point.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Reference date
            provider: Data provider
            chart_type: Chart type
            market_type: Market type (spot, futures_usdt, futures_coin)

        Returns:
            Cache key string
        """
        # Standardize inputs
        symbol = symbol.upper()
        interval = interval.lower()
        date_str = date.strftime("%Y-%m-%d")
        provider = provider.upper()
        chart_type = chart_type.upper()
        market_type = market_type.lower()

        # Create a key that incorporates all components
        return f"{provider}_{chart_type}_{market_type}_{symbol}_{interval}_{date_str}"

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get filesystem path for a cache key.

        Args:
            cache_key: Cache key string

        Returns:
            Path to cache file
        """
        # Use cache key components to create a directory structure
        components = cache_key.split("_")

        if len(components) >= 6:
            provider, chart_type, market_type, symbol, interval, date_str = components[
                :6
            ]

            # Create path with hierarchical structure according to specification
            return (
                self.cache_dir
                / provider
                / chart_type
                / market_type
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
        market_type: str = "spot",
    ) -> Optional[pd.DataFrame]:
        """Load data from cache.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Reference date
            provider: Data provider
            chart_type: Chart type
            market_type: Market type (spot, futures_usdt, futures_coin)

        Returns:
            DataFrame with cached data or None if not found
        """
        operation_start = time.time()
        logger.debug(
            f"=== BEGIN load_from_cache for {symbol} {interval} {date.date()} ==="
        )

        # Generate cache key
        key_gen_start = time.time()
        cache_key = self.get_cache_key(
            symbol, interval, date, provider, chart_type, market_type
        )
        cache_path = self._get_cache_path(cache_key)
        key_gen_elapsed = time.time() - key_gen_start
        logger.debug(f"Cache key generation completed in {key_gen_elapsed:.4f}s")

        logger.debug(f"Cache load attempt for {cache_key}")
        logger.debug(f"Looking for cache file at {cache_path}")

        # Check if file exists
        exists_check_start = time.time()
        file_exists = cache_path.exists()
        exists_check_elapsed = time.time() - exists_check_start
        logger.debug(f"File existence check completed in {exists_check_elapsed:.4f}s")

        if not file_exists:
            operation_elapsed = time.time() - operation_start
            logger.debug(
                f"Cache miss - file not found: {cache_path}, operation took {operation_elapsed:.4f}s"
            )
            logger.debug(f"=== END load_from_cache (file not found) ===")
            return None

        try:
            # Check metadata for validation info
            metadata_check_start = time.time()
            logger.debug(f"Checking metadata validity for {cache_key}")
            is_valid = True
            if cache_key in self.metadata:
                is_valid = self.metadata[cache_key].get("is_valid", True)
                if not is_valid:
                    logger.warning(f"Skipping invalid cache: {cache_key}")
                    operation_elapsed = time.time() - operation_start
                    logger.debug(
                        f"=== END load_from_cache (invalid cache) in {operation_elapsed:.4f}s ==="
                    )
                    return None
            metadata_check_elapsed = time.time() - metadata_check_start
            logger.debug(
                f"Metadata validity check completed in {metadata_check_elapsed:.4f}s, is_valid={is_valid}"
            )

            # Load from Arrow format
            logger.debug(f"Reading cache file: {cache_path}")
            read_start = time.time()
            df = pd.read_feather(cache_path)
            read_elapsed = time.time() - read_start
            logger.debug(
                f"Cache file read completed in {read_elapsed:.4f}s, shape: {df.shape}"
            )

            # Ensure index is a DatetimeIndex for time-series data
            index_conversion_start = time.time()
            if "open_time" in df.columns:
                logger.debug("Converting open_time column to index")
                df = df.set_index("open_time")
                # Ensure timezone info
                if df.index.tzinfo is None:
                    logger.debug("Adding UTC timezone to naive index")
                    df.index = df.index.tz_localize(timezone.utc)
            index_conversion_elapsed = time.time() - index_conversion_start
            logger.debug(
                f"Index conversion completed in {index_conversion_elapsed:.4f}s"
            )

            operation_elapsed = time.time() - operation_start
            logger.debug(
                f"Cache hit: {cache_key}, shape: {df.shape}, operation took {operation_elapsed:.4f}s"
            )
            logger.debug(
                f"=== END load_from_cache (success) in {operation_elapsed:.4f}s ==="
            )
            return df

        except Exception as e:
            operation_elapsed = time.time() - operation_start
            logger.error(
                f"Failed to load cache: {cache_key}, error: {e}, operation took {operation_elapsed:.4f}s"
            )
            # Mark as invalid in metadata
            logger.debug(f"Marking cache as invalid due to error: {e}")
            await self._mark_cache_invalid(cache_key, str(e))
            logger.debug(
                f"=== END load_from_cache (error) in {operation_elapsed:.4f}s ==="
            )
            return None

    async def save_to_cache(
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
            date: Reference date
            provider: Data provider
            chart_type: Chart type
            market_type: Market type (spot, futures_usdt, futures_coin)

        Returns:
            True if successful, False otherwise
        """
        operation_start = time.time()
        logger.debug(
            f"=== BEGIN save_to_cache for {symbol} {interval} {date.date()} ==="
        )

        if df.empty:
            logger.debug("Not caching empty DataFrame")
            logger.debug(f"=== END save_to_cache (empty DataFrame) ===")
            return False

        # Generate cache key and path
        key_gen_start = time.time()
        cache_key = self.get_cache_key(
            symbol, interval, date, provider, chart_type, market_type
        )
        cache_path = self._get_cache_path(cache_key)
        key_gen_elapsed = time.time() - key_gen_start
        logger.debug(f"Cache key generation completed in {key_gen_elapsed:.4f}s")

        logger.debug(f"Preparing to save cache for {cache_key}")
        logger.debug(f"Cache file path: {cache_path}")

        # Create parent directories if they don't exist
        try:
            dir_creation_start = time.time()
            logger.debug(f"Creating cache directory structure: {cache_path.parent}")
            os.makedirs(cache_path.parent, exist_ok=True)
            dir_creation_elapsed = time.time() - dir_creation_start
            logger.debug(f"Directory creation completed in {dir_creation_elapsed:.4f}s")
        except Exception as e:
            operation_elapsed = time.time() - operation_start
            logger.error(
                f"Failed to create cache directory: {e}, operation took {operation_elapsed:.4f}s"
            )
            logger.debug(f"=== END save_to_cache (directory creation error) ===")
            return False

        try:
            # Reset index to include open_time as a column
            index_reset_start = time.time()
            logger.debug("Resetting index to prepare for cache save")
            df_reset = df.reset_index()
            index_reset_elapsed = time.time() - index_reset_start
            logger.debug(f"Index reset completed in {index_reset_elapsed:.4f}s")

            # Save to Arrow format
            logger.debug(f"Writing DataFrame to cache file, shape: {df_reset.shape}")
            write_start = time.time()
            df_reset.to_feather(cache_path)
            write_elapsed = time.time() - write_start
            logger.debug(f"Cache file write completed in {write_elapsed:.4f}s")

            # Update metadata
            metadata_update_start = time.time()
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

            logger.debug(f"Updating metadata for {cache_key}")
            async with self._cache_lock:
                self.metadata[cache_key] = metadata_entry
                logger.debug("Saving metadata to disk")
                metadata_save_start = time.time()
                await self._save_metadata()
                metadata_save_elapsed = time.time() - metadata_save_start
                logger.debug(f"Metadata save completed in {metadata_save_elapsed:.4f}s")

            metadata_update_elapsed = time.time() - metadata_update_start
            logger.debug(f"Metadata update completed in {metadata_update_elapsed:.4f}s")

            operation_elapsed = time.time() - operation_start
            logger.debug(
                f"Cached {len(df)} rows to {cache_path}, operation took {operation_elapsed:.4f}s"
            )
            logger.debug(
                f"=== END save_to_cache (success) in {operation_elapsed:.4f}s ==="
            )
            return True

        except Exception as e:
            operation_elapsed = time.time() - operation_start
            logger.error(
                f"Failed to save cache: {cache_key}, error: {e}, operation took {operation_elapsed:.4f}s"
            )
            logger.debug(f"=== END save_to_cache (error) ===")
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
        market_type: str = "spot",
    ) -> None:
        """Mark cache entry as invalid (will be excluded from loading).

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            date: Reference date
            provider: Data provider
            chart_type: Chart type
            market_type: Market type (spot, futures_usdt, futures_coin)
        """
        logger.debug(f"Invalidating cache for {symbol} {interval} {date.date()}")
        cache_key = self.get_cache_key(
            symbol, interval, date, provider, chart_type, market_type
        )
        self._mark_cache_invalid(cache_key, "Manually invalidated")

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
