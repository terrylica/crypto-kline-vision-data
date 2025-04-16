#!/usr/bin/env python

from typing import NewType, NamedTuple
import pandas as pd
from pathlib import Path

# Import centralized utilities
from utils.time_utils import (
    TimestampUnit,
    MILLISECOND_DIGITS,
    MICROSECOND_DIGITS,
    detect_timestamp_unit,
    validate_timestamp_unit,
)
from utils.config import FileType, FILE_EXTENSIONS, CONSOLIDATION_DELAY

# Type definitions for semantic clarity and safety
TimeseriesIndex = NewType("TimeseriesIndex", pd.DatetimeIndex)
CachePath = NewType("CachePath", Path)

# Re-export for convenience
__all__ = [
    "TimeseriesIndex",
    "CachePath",
    "TimestampUnit",
    "MILLISECOND_DIGITS",
    "MICROSECOND_DIGITS",
    "CONSOLIDATION_DELAY",
    "detect_timestamp_unit",
    "validate_timestamp_unit",
    "FileType",
]

# File extensions simply reusing the constant from config
FileExtensions = NamedTuple(
    "FileExtensions",
    DATA=FILE_EXTENSIONS["DATA"],
    CHECKSUM=FILE_EXTENSIONS["CHECKSUM"],
    CACHE=FILE_EXTENSIONS["CACHE"],
    METADATA=FILE_EXTENSIONS["METADATA"],
)
