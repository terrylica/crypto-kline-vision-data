"""Utility functions for cache testing."""

import logging
import os
from pathlib import Path
import pyarrow as pa
from datetime import datetime
import asyncio

# Set up logging
logger = logging.getLogger(__name__)


def verify_arrow_format(file_path: Path, index_name: str = "open_time") -> None:
    """Verify Arrow file format in detail.

    Args:
        file_path: Path to the Arrow file
        index_name: Expected name of the index column
    """
    with open(file_path, "rb") as f:
        # Check magic number
        magic = f.read(6)
        logger.info(f"Magic number (hex): {magic.hex()}")
        assert magic == b"ARROW1", f"Invalid magic number: {magic}"

        # Check continuation
        continuation = f.read(2)
        logger.info(f"Continuation: {continuation.hex()}")
        assert continuation == b"\x00\x00", "Invalid continuation bytes"

        # Check metadata length
        metadata_length = int.from_bytes(f.read(4), "little")
        logger.info(f"Metadata length: {metadata_length}")
        # 0xFFFFFFFF (4294967295) is a special value indicating streaming format
        assert metadata_length == 4294967295 or (
            0 <= metadata_length < 1_000_000
        ), f"Invalid metadata length: {metadata_length}"

        # Read schema
        with pa.ipc.open_file(str(file_path)) as reader:
            schema = reader.schema
            logger.info(f"Schema:\n{schema}")
            assert index_name in str(schema), f"Missing index column: {index_name}"


def corrupt_cache_file(file_path: Path) -> None:
    """Deliberately corrupt a cache file for testing.

    Args:
        file_path: Path to the cache file to corrupt
    """
    logger.info(f"Corrupting cache file: {file_path}")
    with open(file_path, "ab") as f:
        f.write(b"CORRUPTED_DATA_FOR_TESTING")


def validate_cache_directory(cache_dir: Path) -> None:
    """Validate cache directory structure and permissions.

    Args:
        cache_dir: Path to the cache directory
    """
    logger.debug(f"Validating cache directory: {cache_dir}")
    try:
        # Check directory exists
        assert cache_dir.exists(), f"Cache directory does not exist: {cache_dir}"

        # Check permissions
        assert os.access(
            cache_dir, os.W_OK
        ), f"Cache directory not writable: {cache_dir}"
        assert os.access(
            cache_dir, os.R_OK
        ), f"Cache directory not readable: {cache_dir}"

        # Log directory structure
        logger.debug("Cache directory structure:")
        for path in cache_dir.rglob("*"):
            logger.debug(
                f"  {'D' if path.is_dir() else 'F'} {path.relative_to(cache_dir)}"
            )

        logger.debug("Cache directory validation successful")
    except Exception as e:
        logger.error(f"Cache directory validation failed: {e}")
        raise


async def wait_for_cache_file_change(
    file_path: Path, original_mtime: float, timeout: float = 10.0
) -> bool:
    """Wait for a cache file to change after modification.

    Args:
        file_path: Path to the cache file
        original_mtime: Original modification time
        timeout: Maximum time to wait in seconds

    Returns:
        True if the file changed, False if timed out
    """
    start_time = datetime.now()
    while (datetime.now() - start_time).total_seconds() < timeout:
        if file_path.exists():
            current_mtime = file_path.stat().st_mtime
            if current_mtime > original_mtime:
                return True
        await asyncio.sleep(0.1)
    return False


def get_cache_files(cache_dir: Path, pattern: str = "**/*.arrow") -> list[Path]:
    """Get all cache files in a directory.

    Args:
        cache_dir: Path to the cache directory
        pattern: Glob pattern for cache files

    Returns:
        List of cache file paths
    """
    return list(cache_dir.glob(pattern))
