"""Utility functions for debugging test cases.

This module provides helper functions to improve debugging experience
during test execution, particularly for common issues like date/time handling.
"""

import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def debug_datetime_comparison(
    dt1: datetime,
    dt2: datetime,
    label1: str = "datetime1",
    label2: str = "datetime2",
    log_level: int = logging.DEBUG,
) -> None:
    """Debug helper for datetime comparison issues.

    Logs detailed information about two datetime objects to help diagnose
    comparison issues, timezone differences, or other datetime-related problems.

    Args:
        dt1: First datetime object
        dt2: Second datetime object
        label1: Label for the first datetime (default: "datetime1")
        label2: Label for the second datetime (default: "datetime2")
        log_level: Logging level to use (default: logging.DEBUG)
    """
    logger.log(
        log_level, "--- Datetime Comparison Debug [%s vs %s] ---", label1, label2
    )

    # Compare basic properties
    logger.log(log_level, "%s: %s", label1, dt1)
    logger.log(log_level, "%s: %s", label2, dt2)
    logger.log(log_level, "%s > %s: %s", label1, label2, dt1 > dt2)
    logger.log(log_level, "%s == %s: %s", label1, label2, dt1 == dt2)
    logger.log(log_level, "%s < %s: %s", label1, label2, dt1 < dt2)

    # Time difference
    diff = dt1 - dt2
    logger.log(log_level, "Difference (%s - %s): %s", label1, label2, diff)

    # Check timezone info
    logger.log(log_level, "%s timezone: %s", label1, dt1.tzinfo)
    logger.log(log_level, "%s timezone: %s", label2, dt2.tzinfo)

    # Check for timezone-naive datetimes
    if dt1.tzinfo is None:
        logger.log(log_level, "WARNING: %s is timezone-naive!", label1)
    if dt2.tzinfo is None:
        logger.log(log_level, "WARNING: %s is timezone-naive!", label2)

    # Check for microsecond precision differences
    if abs(diff) < timedelta(seconds=1):
        logger.log(log_level, "%s microseconds: %d", label1, dt1.microsecond)
        logger.log(log_level, "%s microseconds: %d", label2, dt2.microsecond)

    logger.log(log_level, "--- End Datetime Comparison Debug ---")
