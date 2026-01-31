#!/usr/bin/env python
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from time_validation.py for modularity
"""Data availability validation utilities.

This module provides validation for data availability including:
- Checking if data is likely to be available for a given time range
- Validating data consolidation delays
"""

from datetime import datetime, timedelta, timezone

from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import Interval

__all__ = [
    "is_data_likely_available",
    "validate_data_availability",
]


def enforce_utc_timestamp(dt: datetime) -> datetime:
    """Ensures datetime object is timezone aware and in UTC.

    Args:
        dt: Input datetime, can be naive or timezone-aware

    Returns:
        UTC timezone-aware datetime
    """
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def validate_data_availability(
    start_time: datetime, end_time: datetime, buffer_hours: int = 24
) -> tuple[datetime, datetime]:
    """Validate that data is likely to be available for the requested time range.

    Args:
        start_time: Start time of the data
        end_time: End time of the data
        buffer_hours: Number of hours before now that data might not be available

    Returns:
        Tuple of (normalized_start_time, normalized_end_time)
    """
    start_time = enforce_utc_timestamp(start_time)
    end_time = enforce_utc_timestamp(end_time)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=buffer_hours)

    if end_time > cutoff:
        logger.warning(
            f"Requested data includes recent time ({end_time}) that may not be fully consolidated. "
            f"Data is typically available with a {buffer_hours} hour delay."
        )

    return start_time, end_time


def is_data_likely_available(
    target_date: datetime,
    interval: str | Interval | None = None,
    consolidation_delay: timedelta | None = None,
) -> bool:
    """Check if data is likely available for the specified date and interval.

    Args:
        target_date: Date to check data availability for
        interval: Optional interval to use for more precise availability determination
        consolidation_delay: Optional explicit delay override

    Returns:
        True if data is likely available, False otherwise
    """
    target_date = enforce_utc_timestamp(target_date)
    now = datetime.now(timezone.utc)

    logger.debug(f"Checking data availability for target_date={target_date.isoformat()}, interval={interval}, now={now.isoformat()}")

    if target_date > now:
        logger.debug(f"Target date {target_date.isoformat()} is in the future - data not available")
        return False

    if consolidation_delay is not None:
        consolidation_threshold = now - consolidation_delay
        is_available = target_date <= consolidation_threshold
        logger.debug(
            f"Using explicit consolidation_delay={consolidation_delay}, "
            f"threshold={consolidation_threshold.isoformat()}, is_available={is_available}"
        )
        return is_available

    if interval is not None:
        if isinstance(interval, str):
            try:
                logger.debug(f"Converting string interval '{interval}' to Interval enum")
                interval = Interval(interval)
            except (ValueError, ImportError) as e:
                logger.debug(f"Could not parse interval '{interval}' due to {type(e).__name__}: {e!s}, using default delay")
                consolidation_delay = timedelta(minutes=5)
        else:
            try:
                from data_source_manager.utils.time_utils import (
                    align_time_boundaries,
                    get_interval_seconds,
                )

                interval_seconds = get_interval_seconds(interval)
                logger.debug(f"Interval {interval} is {interval_seconds} seconds")

                aligned_target, _ = align_time_boundaries(target_date, target_date, interval)
                logger.debug(f"Aligned target date to {aligned_target.isoformat()}")

                if aligned_target > target_date:
                    logger.debug(f"Target date is {target_date.isoformat()}, which is between intervals")
                    aligned_target = aligned_target - timedelta(seconds=interval_seconds)
                    logger.debug(f"Adjusted to previous interval: {aligned_target.isoformat()}")

                time_since_target = now - target_date
                seconds_since_target = time_since_target.total_seconds()
                logger.debug(f"Time since target: {seconds_since_target:.2f} seconds")

                buffer_seconds = max(30, interval_seconds * 0.2)
                consolidation_buffer = timedelta(seconds=buffer_seconds)
                logger.debug(f"Using consolidation buffer of {buffer_seconds} seconds")

                is_available = (aligned_target + consolidation_buffer) <= now
                logger.debug(f"Threshold time is {(aligned_target + consolidation_buffer).isoformat()}, is_available={is_available}")

                if is_available and seconds_since_target < buffer_seconds:
                    logger.debug(f"Very recent target date ({seconds_since_target:.2f}s ago), treating as potentially not consolidated")
                    is_available = False

                return is_available
            except ImportError as e:
                logger.debug(f"Import error in interval calculation: {e!s}")
                logger.warning("Could not import time utils, using default delay")
                consolidation_delay = timedelta(minutes=5)

    if consolidation_delay is None:
        consolidation_delay = timedelta(minutes=5)
        logger.debug(f"Using default consolidation_delay={consolidation_delay}")

    consolidation_threshold = now - consolidation_delay
    is_available = target_date <= consolidation_threshold
    logger.debug(f"Default check: threshold={consolidation_threshold.isoformat()}, is_available={is_available}")
    return is_available
