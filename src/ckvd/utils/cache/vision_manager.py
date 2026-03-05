#!/usr/bin/env python
"""Vision-specific cache management utilities.

# polars-exception: VisionCacheManager returns pandas DataFrames for
# compatibility with existing CacheValidator and downstream consumers

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from cache_validator.py for modularity
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa

from ckvd.utils.cache.validator import CacheValidator
from ckvd.utils.loguru_setup import logger

__all__ = [
    "VisionCacheManager",
]


class VisionCacheManager:
    """Vision-specific cache management utilities."""

    FILE_EXTENSION = ".arrow"

    @staticmethod
    async def save_to_cache(
        df: pd.DataFrame,
        cache_path: Path,
        _date_or_unused: Any = None,
    ) -> tuple[str, int]:
        """Save data to cache in Arrow format.

        Args:
            df: DataFrame to cache
            cache_path: Path to cache file
            _date_or_unused: Optional date parameter (kept for backward compatibility)

        Returns:
            Tuple of (checksum, record count)
        """
        if df.empty:
            logger.warning("Empty dataframe, not saving to cache")
            return "", 0

        cache_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Use Polars for faster pandas→Arrow IPC conversion (avoids pa.Table.from_pandas overhead)
            import polars as pl

            pl.from_pandas(df).write_ipc(str(cache_path))

            checksum = CacheValidator.calculate_checksum(cache_path)
            record_count = len(df)

            logger.info(
                "Saved %d records to cache at %s. Size: %d bytes",
                record_count,
                cache_path,
                cache_path.stat().st_size,
            )

            return checksum, record_count
        except (OSError, pa.ArrowException, pa.ArrowInvalid, pa.ArrowIOError) as e:
            logger.error("Error saving to cache: %s", e)
            if cache_path.exists():
                try:
                    cache_path.unlink()
                    logger.info("Removed partial cache file after error: %s", cache_path)
                except (OSError, PermissionError) as cleanup_error:
                    logger.error("Failed to clean up partial cache file: %s", cleanup_error)
            return "", 0

    @staticmethod
    async def load_from_cache(cache_path: Path, columns: Sequence[str] | None = None) -> pd.DataFrame | None:
        """Load data from cache with proper error handling.

        Args:
            cache_path: Path to cache file
            columns: Optional list of columns to read

        Returns:
            DataFrame or None if cache is invalid or missing
        """
        try:
            error = CacheValidator.validate_cache_integrity(cache_path)
            if error:
                logger.warning("Cache validation failed: %s", error.message)
                return None

            df = await CacheValidator.safely_read_arrow_file_async(cache_path, columns)
            if df is None:
                return None

            if df.empty:
                logger.warning("Cache file is empty: %s", cache_path)
                return None

            logger.info("Successfully loaded %d records from cache: %s", len(df), cache_path)
            return df
        except (OSError, ValueError, pa.ArrowInvalid, pa.ArrowIOError) as e:
            logger.error("Error loading from cache: %s", e)
            return None
