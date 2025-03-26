#!/usr/bin/env python
"""Utilities for testing in Binance Data Services.

This module provides common utilities for testing, including
functions to generate safe test time ranges that are beyond
the API consolidation delay.
"""

from datetime import datetime, timedelta, timezone
from typing import Tuple

from utils.logger_setup import get_logger
from core.vision_constraints import CONSOLIDATION_DELAY

logger = get_logger(__name__, "INFO", show_path=False)


def get_safe_test_time_range(
    duration: timedelta = timedelta(hours=1),
) -> Tuple[datetime, datetime]:
    """Generate a time range that's safely beyond the Vision API consolidation delay.

    This function returns a start and end time that are far enough in the past
    to ensure that data is available in the Binance Vision API. It uses a known
    historical date for which data is available.

    Args:
        duration: Duration of the time range (default: 1 hour)

    Returns:
        Tuple of (start_time, end_time) in UTC, rounded to nearest second
    """
    # Use a known historical date for which data is definitely available
    # January 1, 2023 at 00:00:00 UTC
    start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end_time = start_time + duration
    logger.info(f"Generated safe test time range: {start_time} to {end_time}")
    return start_time, end_time
