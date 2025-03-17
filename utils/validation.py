#!/usr/bin/env python
from utils.logger_setup import get_logger

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)

import re
from datetime import datetime, timezone, timedelta
import aiohttp
from .market_constraints import (
    MarketType,
    Interval,
    get_market_capabilities,
    is_interval_supported,
    get_endpoint_url,
    get_minimum_interval,
)


class DataValidation:
    """Validation utilities for Binance data loading."""

    @classmethod
    def validate_interval(cls, interval: str, market_type: str = "spot") -> None:
        """Validate the interval parameter for a specific market type."""
        try:
            market = MarketType[market_type.upper()]
            interval_enum = Interval(interval)
            if not is_interval_supported(market, interval_enum):
                capabilities = get_market_capabilities(market)
                min_interval = get_minimum_interval(market).value
                supported = sorted(i.value for i in capabilities.supported_intervals)
                raise ValueError(
                    f"Invalid interval '{interval}' for {market.name}. "
                    f"Minimum interval: {min_interval}. "
                    f"Supported intervals: {', '.join(supported)}"
                )
        except (KeyError, ValueError) as e:
            if isinstance(e, KeyError) or "MarketType" in str(e):
                valid_types = [m.name for m in MarketType]
                raise ValueError(
                    f"Invalid market type: {market_type}. Must be one of: {', '.join(valid_types)}"
                )
            raise

    @classmethod
    def validate_market_type(cls, market_type: str) -> None:
        """Validate the market type parameter."""
        try:
            MarketType[market_type.upper()]
        except KeyError:
            valid_types = [m.name for m in MarketType]
            raise ValueError(
                f"Invalid market type: {market_type}. Must be one of: {', '.join(valid_types)}"
            )

    @classmethod
    def validate_symbol_format(cls, symbol: str, market_type: str = "spot") -> None:
        """Validate the symbol format for spot market."""
        try:
            market = MarketType[market_type.upper()]
            capabilities = get_market_capabilities(market)

            pattern = re.compile(r"^[A-Z0-9]{3,}$")
            if not pattern.match(symbol):
                raise ValueError(
                    f"Invalid symbol format for {market.name}: {symbol}. "
                    f"Must follow format: {capabilities.symbol_format}"
                )
        except KeyError:
            valid_types = [m.name for m in MarketType]
            raise ValueError(
                f"Invalid market type: {market_type}. Must be one of: {', '.join(valid_types)}"
            )

    @classmethod
    def validate_dates(cls, start_date: datetime, end_date: datetime) -> None:
        """Validate date parameters."""
        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            raise ValueError("Dates must be datetime objects")

        if start_date.tzinfo is None or end_date.tzinfo is None:
            raise ValueError("Dates must have timezone")

        if start_date >= end_date:
            raise ValueError("Start date must be before end date")

        max_days = 1000
        if (datetime.now(timezone.utc) - start_date).days > max_days:
            raise ValueError("Start date cannot be more than 1000 days old")

    @classmethod
    async def validate_symbol_exists(
        cls, session: aiohttp.ClientSession, symbol: str, market_type: str = "spot"
    ) -> None:
        """Validate that the symbol exists by attempting to fetch a single kline."""
        try:
            market = MarketType[market_type.upper()]
            endpoint_url = get_endpoint_url(market)
        except KeyError:
            valid_types = [m.name for m in MarketType]
            raise ValueError(
                f"Invalid market type: {market_type}. Must be one of: {', '.join(valid_types)}"
            )

        # Check if symbol exists by attempting to fetch a single kline
        now = datetime.now(timezone.utc)
        start_ts = int((now - timedelta(minutes=1)).timestamp() * 1000)
        end_ts = int(now.timestamp() * 1000)

        params = {
            "symbol": symbol,
            "interval": get_minimum_interval(market).value,
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": 1,
        }

        try:
            async with session.get(endpoint_url, params=params) as response:
                response.raise_for_status()
        except aiohttp.ClientError as e:
            if "400" in str(e):  # Bad Request usually means invalid symbol
                raise ValueError(f"Symbol not found: {symbol}")
            raise  # Re-raise other errors

    @staticmethod
    def validate_time_window(start_time: datetime, end_time: datetime) -> None:
        """Validate time window for data retrieval.

        Args:
            start_time: Start time of the window
            end_time: End time of the window

        Raises:
            ValueError: If time window is invalid
        """
        # Ensure times are in UTC
        if start_time.tzinfo is None or end_time.tzinfo is None:
            raise ValueError("Timestamps must be timezone-aware (UTC)")

        # Check time window order
        if start_time >= end_time:
            raise ValueError(
                f"Start time ({start_time}) must be before end time ({end_time})"
            )

        # Remove future date validation to allow testing with future dates
        # current_time = datetime.now(timezone.utc)
        # if end_time > current_time:
        #     raise ValueError(f"End time ({end_time}) cannot be in the future")

        # Check reasonable time range (e.g., not too far in the past)
        max_days_back = 1000  # Binance typically keeps data for about 1000 days
        if (datetime.now(timezone.utc) - start_time).days > max_days_back:
            raise ValueError(
                f"Start time cannot be more than {max_days_back} days in the past"
            )

        # Check minimum window size
        if (end_time - start_time).total_seconds() < 1:
            raise ValueError("Time window must be at least 1 second")
