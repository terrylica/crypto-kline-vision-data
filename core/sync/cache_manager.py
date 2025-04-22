"""Unified cache manager for market data."""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Any
import pandas as pd
import time
import pyarrow as pa
import pyarrow.ipc

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
                self.cache_dir.mkdir(parents=True, exist_ok=True)
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
                metadata_path.stat().st_size if metadata_path.exists() else 0
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

                temp_size = temp_path.stat().st_size
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
                file_size_after = metadata_path.stat().st_size
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
            cache_key: Cache key to get path for

        Returns:
            Full path to the cache file
        """
        # Extract components from the cache key
        # Format: PROVIDER_CHARTTYPE_MARKETTYPE_SYMBOL_INTERVAL_DATESTR
        try:
            components = cache_key.split("_")
            if len(components) < 6:
                raise ValueError(f"Invalid cache key format: {cache_key}")

            provider = components[0].lower()
            chart_type = components[1].lower()
            market_type = components[2].lower()
            symbol = components[3].lower()
            interval = components[4].lower()
            date_str = components[5]

            # Format date for filename (YYYY-MM-DD format)
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            # Determine market path according to Vision API path structure
            if market_type == "spot":
                market_path = "spot"
            elif market_type in ("futures_usdt", "um"):
                market_path = "futures/um"
            elif market_type in ("futures_coin", "cm"):
                market_path = "futures/cm"
            else:
                market_path = market_type

            # Handle special symbols for coin-margined futures
            if market_type in ("futures_coin", "cm") and not symbol.endswith("_perp"):
                symbol_safe = f"{symbol}_perp"
            else:
                symbol_safe = symbol

            # Create path components similar to Vision API structure
            path_components = [
                provider,
                chart_type,
                market_path,
                "daily",
                chart_type,
                symbol_safe,
                interval,
                f"{symbol_safe.upper()}-{interval}-{formatted_date}.arrow",
            ]

            # Build and return the path
            result_path = self.cache_dir.joinpath(*path_components)

            # Ensure the directory exists
            result_path.parent.mkdir(parents=True, exist_ok=True)

            logger.debug(f"Cache path: {result_path}")
            return result_path

        except Exception as e:
            logger.error(f"Error generating cache path for key {cache_key}: {e}")
            # Fallback to a simple path if we can't parse the key
            fallback_path = self.cache_dir / "fallback" / f"{cache_key}.arrow"
            fallback_path.parent.mkdir(parents=True, exist_ok=True)
            return fallback_path

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
            DataFrame with cached data, or None if no cache exists
        """
        # Generate cache key
        cache_key = self.get_cache_key(
            symbol, interval, date, provider, chart_type, market_type
        )

        # Get the file path
        cache_path = self._get_cache_path(cache_key)

        # Enhanced logging about the cache path
        logger.debug(f"Cache key: {cache_key}")
        logger.debug(f"Looking for cache file at: {cache_path}")
        logger.debug(f"Cache path exists? {cache_path.exists()}")

        # Check directory contents if the file doesn't exist
        if not cache_path.exists() and cache_path.parent.exists():
            try:
                files = list(cache_path.parent.glob("*.arrow"))
                logger.debug(f"Directory contents: {[f.name for f in files]}")
            except Exception as e:
                logger.debug(f"Error listing directory: {e}")

        # Check if the file exists
        if not cache_path.exists():
            logger.debug(f"Cache miss for {cache_key} at {cache_path}")
            return None

        # Check if entry is marked as invalid in metadata
        if cache_key in self.metadata and self.metadata[cache_key].get(
            "is_invalid", False
        ):
            invalid_reason = self.metadata[cache_key].get(
                "invalid_reason", "Unknown reason"
            )
            invalidated_at = self.metadata[cache_key].get(
                "invalidated_at", "Unknown time"
            )
            logger.error(
                f"Invalid cache entry detected - Key: {cache_key}, Path: {cache_path}, "
                f"Reason: {invalid_reason}, Invalidated at: {invalidated_at}"
            )
            return None

        try:
            # Log the loading attempt
            logger.debug(f"Loading cache file: {cache_path}")

            # Read the Arrow file
            with pa.memory_map(str(cache_path), "r") as source:
                reader = pa.ipc.open_file(source)
                table = reader.read_all()

            # Convert to pandas DataFrame
            df = table.to_pandas()

            # Basic validation on the returned data
            if df.empty:
                logger.error(
                    f"Cache file {cache_path} returned empty DataFrame - Invalidating cache entry"
                )
                self._mark_cache_invalid(cache_key, "Empty DataFrame")
                return None

            # Update last access time in metadata
            if cache_key in self.metadata:
                self.metadata[cache_key]["last_accessed"] = datetime.now(
                    timezone.utc
                ).isoformat()
                self._save_metadata()

            logger.debug(f"Successfully loaded {len(df)} rows from {cache_path}")
            return df

        except Exception as e:
            logger.error(f"Error loading from cache for {cache_key}: {e}")
            # Mark as invalid for future reference
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

        # Ensure data is sorted by open_time before caching for KLINES chart type
        # This is the final safety check to prevent unsorted cache entries
        if "open_time" in df.columns and chart_type.upper() == "KLINES":
            if not df["open_time"].is_monotonic_increasing:
                logger.debug(
                    f"UnifiedCacheManager: Sorting data by open_time before caching for {symbol}"
                )
                df = df.sort_values("open_time").reset_index(drop=True)

        # Generate cache key
        cache_key = self.get_cache_key(
            symbol, interval, date, provider, chart_type, market_type
        )

        # Get the file path
        cache_path = self._get_cache_path(cache_key)

        try:
            # Ensure the directory exists
            cache_path.parent.mkdir(parents=True, exist_ok=True)

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

            # Convert to pyarrow table
            table = pa.Table.from_pandas(df)

            # Write to file using Arrow IPC format (not Parquet)
            with pa.OSFile(str(cache_path), "wb") as sink:
                with pa.ipc.new_file(sink, table.schema) as writer:
                    writer.write_table(table)

            # Update metadata
            metadata_entry["file_size_bytes"] = cache_path.stat().st_size
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

            # Log cache invalidation as ERROR to ensure it's prominently noticed
            cache_path = self._get_cache_path(cache_key)
            logger.error(
                f"Cache entry invalidated - Key: {cache_key}, Path: {cache_path}, Reason: {reason}"
            )

            self._save_metadata()
