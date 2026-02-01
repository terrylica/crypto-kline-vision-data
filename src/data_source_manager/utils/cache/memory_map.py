#!/usr/bin/env python
"""Safe memory map handling for Arrow files.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from cache_validator.py for modularity
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from pathlib import Path

import polars as pl
import pyarrow as pa

from data_source_manager.utils.loguru_setup import logger

__all__ = [
    "SafeMemoryMap",
]


class SafeMemoryMap:
    """Context manager for safe memory map handling."""

    def __init__(self, path: Path) -> None:
        """Initialize memory map.

        Args:
            path: Path to Arrow file
        """
        self.path = path
        self._mmap = None

    def __enter__(self) -> pa.MemoryMappedFile:
        """Enter context manager.

        Returns:
            Memory mapped file
        """
        self._mmap = pa.memory_map(str(self.path), "r")
        return self._mmap

    def __exit__(
        self,
        _exc_type: type | None,
        _exc_val: Exception | None,
        _exc_tb: object | None,
    ) -> None:
        """Exit context manager and clean up resources."""
        if self._mmap is not None:
            self._mmap.close()

    @classmethod
    async def safely_read_arrow_file(cls, path: Path, columns: Sequence[str] | None = None) -> pl.DataFrame | None:
        """Safely read an Arrow file without blocking the event loop.

        Args:
            path: Path to Arrow file
            columns: Optional list of columns to read

        Returns:
            DataFrame or None if read fails
        """
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: cls._read_arrow_file_impl(path, columns))
        except (OSError, pa.ArrowInvalid, pa.ArrowIOError, ValueError) as e:
            logger.error(f"Error reading Arrow file {path}: {e}")
            return None

    @staticmethod
    def _read_arrow_file_impl(path: Path, columns: Sequence[str] | None = None) -> pl.DataFrame:
        """Internal implementation for reading Arrow files.

        Args:
            path: Path to Arrow file
            columns: Optional list of columns to read

        Returns:
            DataFrame with data from Arrow file
        """
        with SafeMemoryMap(path) as source, pa.ipc.open_file(source) as reader:
            if columns:
                all_cols = reader.schema.names
                cols_to_read = (
                    ["open_time", *list(columns)]
                    if "open_time" in all_cols and "open_time" not in columns
                    else list(columns)
                )
                table = reader.read_all().select(cols_to_read)
            else:
                table = reader.read_all()

            # Convert PyArrow table to Polars DataFrame (zero-copy when possible)
            df = pl.from_arrow(table)

            # Ensure open_time is datetime with UTC timezone
            if "open_time" in df.columns:
                # Cast to datetime if not already, ensure UTC timezone
                if df["open_time"].dtype != pl.Datetime:
                    df = df.with_columns(pl.col("open_time").cast(pl.Datetime("us", "UTC")))
                elif df["open_time"].dtype.time_zone is None:
                    df = df.with_columns(pl.col("open_time").dt.replace_time_zone("UTC"))

            return df
