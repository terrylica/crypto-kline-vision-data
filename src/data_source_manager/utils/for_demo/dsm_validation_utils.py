#!/usr/bin/env python3
"""Validation utilities for DSM demo.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""

import sys

import pendulum
from rich.console import Console

from data_source_manager.core.sync.data_source_manager import DataSourceManager
from data_source_manager.utils.for_core.dsm_date_range_utils import parse_datetime_string as parse_datetime
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import (
    Interval,
    MarketType,
    get_market_capabilities,
    is_interval_supported,
)


def validate_interval(market_type: MarketType, interval: Interval) -> None:
    """Validate if the interval is supported by the market type.

    Args:
        market_type: Market type to validate against
        interval: Interval to validate

    Raises:
        SystemExit: If interval is not supported
    """
    if not is_interval_supported(market_type, interval):
        console = Console()
        capabilities = get_market_capabilities(market_type)
        supported = [i.value for i in capabilities.supported_intervals]

        console.print(f"[bold red]ERROR: Interval {interval.value} is not supported by {market_type.name} market.[/bold red]")
        console.print(f"[yellow]Supported intervals: {', '.join(supported)}[/yellow]")
        console.print("[cyan]Please choose a supported interval and try again.[/cyan]")

        logger.error(f"Interval {interval.value} not supported by {market_type.name} market. Supported intervals: {supported}")
        sys.exit(1)


def calculate_date_range(
    start_time: str | None,
    end_time: str | None,
    days: int,
    interval: Interval,
) -> tuple[pendulum.DateTime, pendulum.DateTime]:
    """Calculate the date range for data fetching.

    Args:
        start_time: Start time string
        end_time: End time string
        days: Number of days to fetch
        interval: Data interval

    Returns:
        Tuple of start and end datetime

    Raises:
        SystemExit: If date range calculation fails
    """
    try:
        # Parse datetime strings if provided
        st = parse_datetime(start_time) if start_time else None
        et = parse_datetime(end_time) if end_time else None

        # Use the core data source manager utility
        return DataSourceManager.calculate_time_range(start_time=st, end_time=et, days=days, interval=interval)
    except (ValueError, TypeError, AttributeError) as e:
        print(f"[bold red]Error calculating date range: {e}[/bold red]")
        sys.exit(1)
