#!/usr/bin/env python3
"""Validation utilities for DSM demo."""

import sys
from typing import Tuple
import pendulum
from rich.console import Console

from utils.logger_setup import logger
from utils.market_constraints import (
    MarketType,
    Interval,
    is_interval_supported,
    get_market_capabilities,
)
from core.sync.data_source_manager import DataSourceManager
from utils.for_demo.dsm_datetime_parser import parse_datetime


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

        console.print(
            f"[bold red]ERROR: Interval {interval.value} is not supported by {market_type.name} market.[/bold red]"
        )
        console.print(f"[yellow]Supported intervals: {', '.join(supported)}[/yellow]")
        console.print("[cyan]Please choose a supported interval and try again.[/cyan]")

        logger.error(
            f"Interval {interval.value} not supported by {market_type.name} market. "
            f"Supported intervals: {supported}"
        )
        sys.exit(1)


def calculate_date_range(
    start_time: str | None,
    end_time: str | None,
    days: int,
    interval: Interval,
) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
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
        return DataSourceManager.calculate_time_range(
            start_time=st, end_time=et, days=days, interval=interval
        )
    except Exception as e:
        print(f"[bold red]Error calculating date range: {e}[/bold red]")
        sys.exit(1)
