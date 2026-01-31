#!/usr/bin/env python
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from time_validation.py for modularity
"""File validation and checksum utilities.

This module provides validation for file integrity including:
- SHA-256 checksum calculation
- Cache file integrity validation
"""

from datetime import timedelta
from pathlib import Path

from data_source_manager.utils.config import (
    MAX_CACHE_AGE,
    MIN_VALID_FILE_SIZE,
)
from data_source_manager.utils.loguru_setup import logger

__all__ = [
    "calculate_checksum",
    "validate_file_with_checksum",
]


def calculate_checksum(file_path: Path) -> str:
    """Calculate SHA-256 checksum of a file.

    Args:
        file_path: Path to the file

    Returns:
        Hexadecimal string of the SHA-256 checksum
    """
    import hashlib

    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def validate_file_with_checksum(
    file_path: Path,
    expected_checksum: str | None = None,
    min_size: int = MIN_VALID_FILE_SIZE,
    max_age: timedelta = MAX_CACHE_AGE,
) -> bool:
    """Validate file integrity with optional checksum verification.

    Args:
        file_path: Path to the file
        expected_checksum: Expected checksum to validate against
        min_size: Minimum valid file size in bytes
        max_age: Maximum valid file age

    Returns:
        True if file passes all integrity checks, False otherwise
    """
    from data_source_manager.utils.validation.dataframe_validation import DataFrameValidator

    integrity_result = DataFrameValidator.validate_cache_integrity(file_path, min_size, max_age)
    if integrity_result is not None:
        return False

    if expected_checksum:
        try:
            actual_checksum = calculate_checksum(file_path)
            return actual_checksum == expected_checksum
        except OSError as e:
            logger.error(f"Error calculating checksum for {file_path}: {e}")
            return False

    return True
