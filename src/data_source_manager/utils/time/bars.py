#!/usr/bin/env python
"""Bar/candle completion detection utilities for market data.

This module provides functions for determining bar close times and
checking if bars are complete based on current time.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from time_utils.py for modularity
# polars-exception: extracted from existing pandas-based time_utils.py - migration is separate task
"""

from datetime import datetime, timedelta, timezone

from data_source_manager.utils.market_constraints import Interval as MarketInterval
from data_source_manager.utils.time.intervals import get_interval_timedelta

__all__ = [
    "get_bar_close_time",
    "is_bar_complete",
]


def get_bar_close_time(open_time: datetime, interval: MarketInterval) -> datetime:
    """Get the close time for a bar given its open time.

    Args:
        open_time: The bar's open time
        interval: The interval specification

    Returns:
        datetime: Close time (interval - 1 microsecond after open time)
    """
    interval_delta = get_interval_timedelta(interval)
    return open_time + interval_delta - timedelta(microseconds=1)


def is_bar_complete(
    timestamp: datetime,
    interval: MarketInterval,
    current_time: datetime | None = None,
) -> bool:
    """Check if a bar is complete based on the current time.

    Args:
        timestamp: The bar's timestamp
        interval: The interval specification
        current_time: Optional current time for testing or comparison.
                     If None, uses the current UTC time.

    Returns:
        bool: True if the bar is complete
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc)

    # Calculate interval timedelta based on interval seconds
    interval_td = get_interval_timedelta(interval)

    # A bar is complete if current time is at least one interval after its start
    return current_time >= (timestamp + interval_td)
