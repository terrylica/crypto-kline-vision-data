#!/usr/bin/env python3
"""
Time utilities for the Failover Composition Priority (FCP) mechanism.
"""

import pendulum
from utils.logger_setup import logger


def parse_datetime(dt_str):
    """
    Parse datetime string in ISO format or human readable format using pendulum.

    Args:
        dt_str: Datetime string to parse

    Returns:
        pendulum.DateTime: Parsed datetime object in UTC timezone

    Raises:
        ValueError: If the datetime string cannot be parsed
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
            elif len(dt_str) == 10 and "-" in dt_str:
                # Try YYYY-MM-DD format
                dt = pendulum.from_format(dt_str, "YYYY-MM-DD", tz="UTC")
                logger.debug(
                    f"Successfully parsed date-only string: {dt.format('YYYY-MM-DD HH:mm:ss.SSS')}"
                )
                return dt
        except Exception as e2:
            logger.debug(f"Failed specific format parsing: {e2}")

        error_msg = f"Unable to parse datetime: {dt_str!r}. Error: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
