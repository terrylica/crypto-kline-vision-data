#!/usr/bin/env python

"""
Deprecation Rules and Standards for Pandas Operations

This module provides standardized rules and utilities for handling deprecated pandas operations,
particularly focusing on Timedelta string formats and other common deprecation warnings.

Motivation:
-----------
1. Consistency: Ensure consistent handling of pandas operations across the codebase
2. Future-proofing: Prevent deprecation warnings by using the latest pandas standards
3. Maintainability: Centralize all deprecation-related rules in one place
4. Documentation: Provide clear guidance on proper usage patterns

Key Areas Addressed:
------------------
1. Timedelta string formats
2. DataFrame operation standards
3. Date/time handling conventions

Standards are based on pandas>=2.1.0 requirements.

Notes:
------
This module follows Python best practices:
- Uses enums for type safety
- Leverages dataclasses for data validation
- Provides comprehensive type hints
- Implements robust error handling
- Uses descriptive custom exceptions
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import ClassVar
from functools import lru_cache
import pandas as pd
from utils.logger_setup import get_logger
from .market_constraints import Interval as MarketInterval

logger = get_logger(__name__, "INFO", show_path=False)


class IntervalParseError(ValueError):
    """Custom exception for interval parsing errors."""


class TimeUnit(str, enum.Enum):
    """Enumeration of valid time units with their pandas-compliant formats."""

    SECOND = "s"
    MINUTE = "min"
    HOUR = "h"
    DAY = "D"
    WEEK = "W"
    MONTH = "M"
    YEAR = "Y"

    @property
    def symbol(self) -> str:
        """Get the symbol representation of the time unit.

        Returns:
            String symbol for this time unit
        """
        # For most units, the symbol is the same as the value
        # For units that might have different representations, we could add special cases
        return self.value

    @property
    def micros(self) -> int:
        """Get the microseconds equivalent for this time unit.

        Returns:
            Number of microseconds in this time unit
        """
        _micros_map = {
            self.SECOND: 1_000_000,  # 1 second
            self.MINUTE: 60_000_000,  # 1 minute
            self.HOUR: 3_600_000_000,  # 1 hour
            self.DAY: 86_400_000_000,  # 1 day
            self.WEEK: 604_800_000_000,  # 1 week
            self.MONTH: 2_592_000_000_000,  # 30 days (approximate month)
            self.YEAR: 31_536_000_000_000,  # 365 days (approximate year)
        }
        return _micros_map[self]

    @classmethod
    def get_all_units(cls) -> list[TimeUnit]:
        """Return all available time units.

        Returns:
            List of all TimeUnit enum values.
        """
        return list(cls)

    @classmethod
    def from_shorthand(cls, shorthand: str) -> TimeUnit:
        """Convert shorthand notation to TimeUnit enum."""
        _shorthand_map = {
            "s": cls.SECOND,
            "m": cls.MINUTE,
            "h": cls.HOUR,
            "d": cls.DAY,
            "w": cls.WEEK,
            "M": cls.MONTH,
            "y": cls.YEAR,
        }
        try:
            return _shorthand_map[shorthand.lower()]
        except KeyError as exc:
            valid_units = ", ".join(_shorthand_map.keys())
            raise IntervalParseError(
                f"Invalid time unit '{shorthand}'. Valid units are: {valid_units}"
            ) from exc

    @classmethod
    def from_market_interval(cls, interval: MarketInterval) -> TimeUnit:
        """Convert a market interval to TimeUnit."""
        _interval_map = {
            MarketInterval.SECOND_1: cls.SECOND,
            MarketInterval.MINUTE_1: cls.MINUTE,
            MarketInterval.MINUTE_3: cls.MINUTE,
            MarketInterval.MINUTE_5: cls.MINUTE,
            MarketInterval.MINUTE_15: cls.MINUTE,
            MarketInterval.MINUTE_30: cls.MINUTE,
            MarketInterval.HOUR_1: cls.HOUR,
            MarketInterval.HOUR_2: cls.HOUR,
            MarketInterval.HOUR_4: cls.HOUR,
            MarketInterval.HOUR_6: cls.HOUR,
            MarketInterval.HOUR_8: cls.HOUR,
            MarketInterval.HOUR_12: cls.HOUR,
            MarketInterval.DAY_1: cls.DAY,
            MarketInterval.DAY_3: cls.DAY,
            MarketInterval.WEEK_1: cls.WEEK,
            MarketInterval.MONTH_1: cls.MONTH,
        }
        return _interval_map[interval]


@dataclass(frozen=True)
class Interval:
    """Immutable representation of a time interval with validation."""

    value: int
    unit: TimeUnit

    # Class-level constants
    MIN_VALUE: ClassVar[int] = 1
    MAX_VALUE: ClassVar[int] = 1000

    def __post_init__(self):
        """Validate interval value after initialization."""
        if not isinstance(self.value, int):
            raise IntervalParseError(
                f"Interval value must be an integer, got {type(self.value)}"
            )
        if not self.MIN_VALUE <= self.value <= self.MAX_VALUE:
            raise IntervalParseError(
                f"Interval value must be between {self.MIN_VALUE} and {self.MAX_VALUE}, got {self.value}"
            )

    @classmethod
    def from_string(cls, interval: str) -> Interval:
        """Create an Interval instance from a string representation."""
        if not interval:
            raise IntervalParseError("Interval string cannot be empty")

        # Extract numeric value and unit
        numeric = ""
        for i, char in enumerate(interval):
            if char.isdigit():
                numeric += char
            else:
                unit = interval[i:]
                break
        else:
            raise IntervalParseError(f"No time unit found in interval: {interval}")

        try:
            value = int(numeric)
        except ValueError as exc:
            raise IntervalParseError(
                f"Invalid numeric value in interval: {numeric}"
            ) from exc

        try:
            time_unit = TimeUnit.from_shorthand(unit)
        except IntervalParseError as e:
            raise IntervalParseError(
                f"Invalid interval format: {interval}. {str(e)}"
            ) from e

        return cls(value=value, unit=time_unit)

    @classmethod
    def from_market_interval(cls, interval: MarketInterval) -> Interval:
        """Create an Interval instance from a MarketInterval enum."""
        # Extract numeric value and unit from the interval value (e.g., "1s", "3m")
        value_str = interval.value[:-1]  # Remove the last character (unit)
        unit_str = interval.value[-1]  # Get the last character (unit)

        try:
            value = int(value_str)
            time_unit = TimeUnit.from_shorthand(unit_str)
        except (ValueError, IntervalParseError) as e:
            raise IntervalParseError(
                f"Invalid market interval: {interval.value}"
            ) from e

        return cls(value=value, unit=time_unit)

    def to_pandas_timedelta(self) -> pd.Timedelta:
        """Convert to pandas Timedelta using latest standards.

        Returns:
            pd.Timedelta: A pandas Timedelta object representing the interval.

        Raises:
            IntervalParseError: If the interval cannot be converted to a valid Timedelta.
        """
        timedelta_str = f"{self.value}{self.unit.value}"
        try:
            result = pd.Timedelta(timedelta_str)
            # Explicitly check if result is NaT and cast to ensure type safety
            if isinstance(result, pd.Timedelta) and not pd.isnull(result):
                return result
            raise ValueError("Conversion resulted in NaT")
        except ValueError as e:
            logger.error("Failed to create Timedelta from %s", timedelta_str)
            raise IntervalParseError(f"Invalid interval: {self}") from e

    def __str__(self) -> str:
        """String representation using pandas-compliant format."""
        return f"{self.value}{self.unit.value}"


@lru_cache(maxsize=128)
def convert_interval_to_timedelta(interval: str) -> pd.Timedelta:
    """Convert a trading interval string to a pandas Timedelta using the latest standards."""
    try:
        interval_obj = Interval.from_string(interval)
        return interval_obj.to_pandas_timedelta()
    except IntervalParseError as e:
        logger.error("Failed to parse interval: %s", interval)
        raise IntervalParseError(str(e)) from e


def validate_interval_format(interval: str) -> bool:
    """Validate if an interval string follows the latest pandas standards."""
    try:
        Interval.from_string(interval)
        return True
    except IntervalParseError:
        return False
