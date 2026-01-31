#!/usr/bin/env python3
"""
Date range calculation utilities for DataSourceManager.

This module provides robust date range calculation functions that support
various scenarios including:
1. End time with days (backward calculation)
2. Start time with days (forward calculation)
3. Explicit start and end times
4. Days-only calculation (backward from current time)

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
5. Default behavior (3 days backward from current time)
"""

from datetime import datetime

import pendulum
from pendulum import DateTime
from pendulum.parsing.exceptions import ParserError

from data_source_manager.utils.config import DATE_STRING_LENGTH
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import Interval
from data_source_manager.utils.time_utils import align_time_boundaries


def parse_datetime_string(dt_str: str | None) -> DateTime | None:
    """Parse datetime string in ISO format or human readable format using pendulum.

    Args:
        dt_str: Datetime string to parse, can be ISO format, YYYY-MM-DD or YYYY-MM-DD HH:MM:SS

    Returns:
        pendulum.DateTime: Parsed datetime object in UTC timezone or None if input is None

    Raises:
        ValueError: If the string cannot be parsed
    """
    logger.debug(f"Attempting to parse datetime string: {dt_str!r}")

    # If input is None, return None
    if dt_str is None:
        logger.debug("Input datetime string is None")
        return None

    try:
        # Use pendulum's powerful parse function which handles most formats
        dt = pendulum.parse(dt_str)
        # Ensure UTC timezone
        if dt.timezone_name != "UTC":
            dt = dt.in_timezone("UTC")
        logger.debug(f"Successfully parsed datetime: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')}")
        return dt
    except (ParserError, ValueError, TypeError) as e:
        try:
            # Try more explicitly with from_format for certain patterns
            if "T" not in dt_str and ":" in dt_str:
                # Try YYYY-MM-DD HH:MM:SS format
                dt = pendulum.from_format(dt_str, "YYYY-MM-DD HH:mm:ss", tz="UTC")
                logger.debug(f"Successfully parsed with from_format: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')}")
                return dt
            if len(dt_str) == DATE_STRING_LENGTH and "-" in dt_str:
                # Try YYYY-MM-DD format
                dt = pendulum.from_format(dt_str, "YYYY-MM-DD", tz="UTC")
                logger.debug(f"Successfully parsed date-only string: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')}")
                return dt
        except (ValueError, TypeError) as e2:
            logger.debug(f"Failed specific format parsing: {e2}")

        error_msg = f"Unable to parse datetime: {dt_str!r}. Error: {e!s}"
        logger.error(error_msg)
        raise ValueError(error_msg) from e


def calculate_date_range(
    start_time: str | DateTime | None = None,
    end_time: str | DateTime | None = None,
    days: int = 3,
    interval: Interval | None = None,
) -> tuple[DateTime, DateTime]:
    """Calculate a date range based on provided parameters with enhanced flexibility.

    This function implements comprehensive date range logic that can be used
    across the entire codebase, supporting all use cases:

    1. End time with days: Calculate range backward from end time
    2. Start time with days: Calculate range forward from start time
    3. Explicit start and end times: Use as provided
    4. Days only: Calculate backward from current time
    5. Default: 3 days backward from current time

    Args:
        start_time: Start time string or DateTime object, or None
        end_time: End time string or DateTime object, or None
        days: Number of days for the range if only start_time or end_time is provided
        interval: If provided, aligns the time boundaries to interval precision

    Returns:
        tuple: (start_datetime, end_datetime) as pendulum.DateTime objects

    Raises:
        ValueError: If both start_time and end_time are provided and start_time is after end_time
    """
    # Get current time in UTC
    current_time = pendulum.now("UTC")

    # Enhanced logging for inputs
    logger.debug(f"calculate_date_range received start_time: {start_time!r}")
    logger.debug(f"calculate_date_range received end_time: {end_time!r}")
    logger.debug(f"calculate_date_range received days: {days}")
    logger.debug(f"calculate_date_range received interval: {interval}")

    # Parse string inputs if needed
    if isinstance(start_time, str):
        start_time = parse_datetime_string(start_time)
    if isinstance(end_time, str):
        end_time = parse_datetime_string(end_time)

    # Calculate date range based on provided parameters
    if start_time and end_time:
        # Case 1: Both start and end times provided - use them directly
        logger.debug("Using explicit start and end times")
        start_datetime, end_datetime = start_time, end_time

        # Validate that start_time is before end_time
        if start_datetime >= end_datetime:
            error_msg = f"Start time ({start_datetime}) must be before end time ({end_datetime})"
            logger.error(error_msg)
            raise ValueError(error_msg)

    elif end_time and not start_time:
        # Case 2: End time with days - calculate start time backward from end time
        logger.debug(f"Using end time with days={days} to calculate start time")
        end_datetime = end_time
        start_datetime = end_datetime.subtract(days=days)
        logger.debug(f"Calculated date range: {start_datetime} to {end_datetime}")

    elif start_time and not end_time:
        # Case 3: Start time with days - calculate end time forward from start time
        logger.debug(f"Using start time with days={days} to calculate end time")
        start_datetime = start_time
        end_datetime = start_datetime.add(days=days)
        logger.debug(f"Calculated date range: {start_datetime} to {end_datetime}")

    else:
        # Case 4: Default to current time with days backward
        logger.debug(f"Using current time ({current_time}) with days={days} backward")
        end_datetime = current_time
        start_datetime = end_datetime.subtract(days=days)
        logger.debug(f"Calculated date range: {start_datetime} to {end_datetime}")

    # Align time boundaries with interval if requested
    if interval:
        logger.debug(f"Aligning time boundaries to {interval.value} interval")
        aligned_start, aligned_end = align_time_boundaries(start_datetime, end_datetime, interval)

        # Convert datetime objects back to pendulum.DateTime if needed
        if not isinstance(aligned_start, pendulum.DateTime):
            aligned_start = pendulum.instance(aligned_start)
        if not isinstance(aligned_end, pendulum.DateTime):
            aligned_end = pendulum.instance(aligned_end)

        # Check if alignment changed the dates
        if aligned_start != start_datetime or aligned_end != end_datetime:
            logger.debug(
                f"Aligned boundaries: {aligned_start.format('YYYY-MM-DD HH:mm:ss.SSS')} to {aligned_end.format('YYYY-MM-DD HH:mm:ss.SSS')}"
            )

        start_datetime, end_datetime = aligned_start, aligned_end

    # Final validation and logging
    logger.debug(
        f"Final date range: {start_datetime.format('YYYY-MM-DD HH:mm:ss.SSS')} to {end_datetime.format('YYYY-MM-DD HH:mm:ss.SSS')}"
    )

    return start_datetime, end_datetime


def get_date_range_description(
    start_time: DateTime | datetime,
    end_time: DateTime | datetime,
    original_params: dict,
) -> str:
    """Generate a human-readable description of how the date range was calculated.

    Args:
        start_time: Calculated start time (datetime or pendulum.DateTime)
        end_time: Calculated end time (datetime or pendulum.DateTime)
        original_params: Dictionary with original parameters (start_time, end_time, days)

    Returns:
        str: Human-readable description of date range calculation
    """
    # Ensure we have pendulum DateTime objects
    if not isinstance(start_time, pendulum.DateTime):
        start_time = pendulum.instance(start_time)
    if not isinstance(end_time, pendulum.DateTime):
        end_time = pendulum.instance(end_time)

    # Extract original parameters
    orig_start = original_params.get("start_time")
    orig_end = original_params.get("end_time")
    days = original_params.get("days", 3)

    start_date_str = start_time.format("YYYY-MM-DD")
    end_date_str = end_time.format("YYYY-MM-DD")

    if orig_start and orig_end:
        return f"Using explicit date range: {start_date_str} to {end_date_str}"
    if orig_end and not orig_start:
        return f"Using end time {end_date_str} and going back {days} days"
    if orig_start and not orig_end:
        return f"Using start time {start_date_str} and going forward {days} days"
    return f"Using current time as end time and going back {days} days"
