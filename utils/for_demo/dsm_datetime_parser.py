#!/usr/bin/env python3
"""
Utility functions for parsing datetime strings for the DSM Demo.

This module provides consistent datetime parsing functionality for all DSM Demos.
"""

import pendulum

from utils.config import DATE_STRING_LENGTH
from utils.logger_setup import logger


def parse_datetime(dt_str):
    """Parse datetime string in ISO format or human readable format using pendulum.

    Args:
        dt_str: Datetime string to parse, can be ISO format, YYYY-MM-DD or YYYY-MM-DD HH:MM:SS

    Returns:
        pendulum.DateTime: Parsed datetime object in UTC timezone

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
        logger.debug(
            f"Successfully parsed datetime: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')}"
        )
        return dt
    except Exception as e:
        try:
            # Try more explicitly with from_format for certain patterns
            if "T" not in dt_str and ":" in dt_str:
                # Try YYYY-MM-DD HH:MM:SS format
                dt = pendulum.from_format(dt_str, "YYYY-MM-DD HH:mm:ss", tz="UTC")
                logger.debug(
                    f"Successfully parsed with from_format: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')}"
                )
                return dt
            if len(dt_str) == DATE_STRING_LENGTH and "-" in dt_str:
                # Try YYYY-MM-DD format
                dt = pendulum.from_format(dt_str, "YYYY-MM-DD", tz="UTC")
                logger.debug(
                    f"Successfully parsed date-only string: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')}"
                )
                return dt
        except Exception as e2:
            logger.debug(f"Failed specific format parsing: {e2}")

        error_msg = f"Unable to parse datetime: {dt_str!r}. Error: {e!s}"
        logger.error(error_msg)
        raise ValueError(error_msg)


def calculate_date_range(start_time, end_time, days):
    """Calculate a date range based on provided parameters.

    Args:
        start_time: Start time string or None
        end_time: End time string or None
        days: Number of days for the range if start/end not provided

    Returns:
        tuple: (start_datetime, end_datetime) as pendulum.DateTime objects
    """
    current_time = pendulum.now("UTC")

    # Enhanced logging for date parsing
    logger.debug(f"calculate_date_range received start_time: {start_time!r}")
    logger.debug(f"calculate_date_range received end_time: {end_time!r}")

    # Determine the date range
    if start_time and end_time:
        try:
            # Use the parse_datetime function to handle different formats
            start_datetime = parse_datetime(start_time)
            end_datetime = parse_datetime(end_time)
            logger.debug(
                f"Successfully parsed dates: {start_datetime.format('YYYY-MM-DD HH:mm:ss.SSS')} to {end_datetime.format('YYYY-MM-DD HH:mm:ss.SSS')}"
            )
            return start_datetime, end_datetime
        except ValueError as e:
            logger.error(f"Error parsing dates: {e}")
            # Fallback to default date range
            end_datetime = current_time
            start_datetime = end_datetime.subtract(days=days)
            logger.warning(
                f"Using fallback date range: {start_datetime.format('YYYY-MM-DD HH:mm:ss.SSS')} to {end_datetime.format('YYYY-MM-DD HH:mm:ss.SSS')}"
            )
            return start_datetime, end_datetime
    else:
        # Default to provided days with end_time as now
        end_datetime = current_time
        start_datetime = end_datetime.subtract(days=days)
        logger.debug(f"Using default date range based on days={days}")
        return start_datetime, end_datetime
