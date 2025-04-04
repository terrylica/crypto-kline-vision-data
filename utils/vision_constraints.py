#!/usr/bin/env python

from typing import NewType, Final, NamedTuple, Literal
from datetime import timedelta
import pandas as pd
from pathlib import Path
from enum import Enum, auto


# Type definitions for semantic clarity and safety
TimeseriesIndex = NewType("TimeseriesIndex", pd.DatetimeIndex)
CachePath = NewType("CachePath", Path)
TimestampUnit = Literal["ms", "us"]  # Supported timestamp units

# Timestamp format detection thresholds
MILLISECOND_DIGITS: Final[int] = 13
MICROSECOND_DIGITS: Final[int] = 16

# Data availability constraints
CONSOLIDATION_DELAY = timedelta(
    hours=48
)  # Time Binance needs to consolidate daily data (increased from 12h to 48h for safety margin)


def detect_timestamp_unit(sample_ts: int | str) -> TimestampUnit:
    """Detect timestamp unit based on number of digits.

    Args:
        sample_ts: Sample timestamp value

    Returns:
        "us" for microseconds (16 digits)
        "ms" for milliseconds (13 digits)

    Raises:
        ValueError: If timestamp format is not recognized

    Note:
        This is a core architectural feature to handle Binance Vision's
        evolution of timestamp formats:
        - Pre-2025: Millisecond timestamps (13 digits)
        - 2025 onwards: Microsecond timestamps (16 digits)
    """
    digits = len(str(int(sample_ts)))

    if digits == MICROSECOND_DIGITS:
        return "us"
    elif digits == MILLISECOND_DIGITS:
        return "ms"
    else:
        raise ValueError(
            f"Unrecognized timestamp format with {digits} digits. "
            f"Expected {MILLISECOND_DIGITS} for milliseconds or "
            f"{MICROSECOND_DIGITS} for microseconds."
        )


def validate_timestamp_unit(unit: TimestampUnit) -> None:
    """Validate that the timestamp unit is supported.

    Args:
        unit: Timestamp unit to validate

    Raises:
        ValueError: If unit is not supported
    """
    if unit not in ("ms", "us"):
        raise ValueError(f"Unsupported timestamp unit: {unit}. Must be 'ms' or 'us'.")


# File management constraints
class FileType(Enum):
    """Types of files managed by Vision client."""

    DATA = auto()
    CHECKSUM = auto()
    CACHE = auto()
    METADATA = auto()


class FileExtensions(NamedTuple):
    """Standard file extensions for Vision data."""

    DATA: str = ".zip"
    CHECKSUM: str = ".CHECKSUM"
    CACHE: str = ".arrow"
    METADATA: str = ".json"
