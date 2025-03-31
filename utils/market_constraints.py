#!/usr/bin/env python

from dataclasses import dataclass
from enum import Enum, auto
from typing import Dict, List, Optional
import re

from utils.logger_setup import get_logger

logger = get_logger(__name__, "DEBUG", show_path=False)


class MarketType(Enum):
    SPOT = auto()
    FUTURES_USDT = auto()  # USDT-margined futures (UM)
    FUTURES_COIN = auto()  # Coin-margined futures (CM)
    FUTURES = auto()  # Legacy/generic futures type for backward compatibility

    @property
    def is_futures(self) -> bool:
        """Check if this is any type of futures market."""
        return self in (self.FUTURES, self.FUTURES_USDT, self.FUTURES_COIN)

    @property
    def vision_api_path(self) -> str:
        """Get the corresponding path component for Binance Vision API."""
        if self == self.SPOT:
            return "spot"
        elif self == self.FUTURES_USDT:
            return "futures/um"
        elif self == self.FUTURES_COIN:
            return "futures/cm"
        elif self == self.FUTURES:
            return "futures/um"  # Default to UM for backward compatibility
        else:
            raise ValueError(f"Unknown market type: {self}")

    @classmethod
    def from_string(cls, market_type_str: str) -> "MarketType":
        """Convert string representation to MarketType enum.

        Args:
            market_type_str: String representation of market type

        Returns:
            MarketType enum value

        Raises:
            ValueError: If the string doesn't match any known market type
        """
        mapping = {
            "spot": cls.SPOT,
            "futures": cls.FUTURES,
            "futures_usdt": cls.FUTURES_USDT,
            "um": cls.FUTURES_USDT,
            "futures_coin": cls.FUTURES_COIN,
            "cm": cls.FUTURES_COIN,
        }

        market_type_str = market_type_str.lower()
        if market_type_str in mapping:
            return mapping[market_type_str]

        raise ValueError(f"Unknown market type string: {market_type_str}")


class Interval(Enum):
    SECOND_1 = "1s"
    MINUTE_1 = "1m"
    MINUTE_3 = "3m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    HOUR_2 = "2h"
    HOUR_4 = "4h"
    HOUR_6 = "6h"
    HOUR_8 = "8h"
    HOUR_12 = "12h"
    DAY_1 = "1d"
    DAY_3 = "3d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"

    def to_seconds(self) -> int:
        """Convert interval to seconds."""
        value = self.value
        match = re.match(r"(\d+)([smhdwM])", value)
        if not match:
            raise ValueError(f"Invalid interval format: {value}")

        num, unit = match.groups()
        num = int(num)

        multipliers = {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
            "w": 604800,
            "M": 2592000,
        }  # Approximate - using 30 days

        return num * multipliers[unit]

    @classmethod
    def get_default(cls) -> "Interval":
        """Get default interval (1 second)."""
        return cls.SECOND_1

    def __str__(self) -> str:
        return self.value


@dataclass
class MarketCapabilities:
    """Encapsulates the capabilities and constraints of a market type."""

    primary_endpoint: str  # Primary API endpoint
    backup_endpoints: List[str]  # List of backup endpoints
    data_only_endpoint: Optional[str]  # Endpoint for market data only
    api_version: str  # API version to use
    supported_intervals: List[Interval]  # List of supported intervals
    symbol_format: str  # Example format for symbols
    description: str  # Detailed description of market capabilities
    max_limit: int  # Maximum number of records per request
    endpoint_reliability: str  # Description of endpoint reliability

    @property
    def api_base_url(self) -> str:
        """Get the base URL for API requests."""
        return f"{self.primary_endpoint}/{self.api_version}/klines"


MARKET_CAPABILITIES: Dict[MarketType, MarketCapabilities] = {
    MarketType.SPOT: MarketCapabilities(
        primary_endpoint="https://api.binance.com",
        backup_endpoints=[
            "https://api-gcp.binance.com",
            "https://api1.binance.com",
            "https://api2.binance.com",
            "https://api3.binance.com",
            "https://api4.binance.com",
        ],
        data_only_endpoint="https://data-api.binance.vision",
        api_version="v3",
        supported_intervals=[
            interval for interval in Interval
        ],  # All intervals including 1s
        symbol_format="BTCUSDT",
        description=(
            "Spot market with comprehensive support for all intervals including 1-second data. "
            "Perfect time alignment with exactly 1.00s for 1s data and 60.00s for 1m data. "
            "All endpoints consistently return exactly 1000 records when requested."
        ),
        max_limit=1000,
        endpoint_reliability="All endpoints (primary, backup, and data-only) are reliable and support all features.",
    ),
    MarketType.FUTURES_USDT: MarketCapabilities(
        primary_endpoint="https://fapi.binance.com",
        backup_endpoints=[
            "https://fapi-gcp.binance.com",
            "https://fapi1.binance.com",
            "https://fapi2.binance.com",
            "https://fapi3.binance.com",
        ],
        data_only_endpoint=None,  # No dedicated data-only endpoint for futures
        api_version="v1",
        supported_intervals=[
            interval for interval in Interval if interval.value != "1s"
        ],  # All intervals except 1s
        symbol_format="BTCUSDT",
        description=(
            "USDT-margined futures (UM) market with support for most intervals except 1-second data. "
            "Returns up to 1500 records when requested. Vision API uses futures/um path."
        ),
        max_limit=1500,
        endpoint_reliability="Primary and backup endpoints are reliable and support all features.",
    ),
    MarketType.FUTURES_COIN: MarketCapabilities(
        primary_endpoint="https://dapi.binance.com",
        backup_endpoints=[
            "https://dapi-gcp.binance.com",
            "https://dapi1.binance.com",
            "https://dapi2.binance.com",
            "https://dapi3.binance.com",
        ],
        data_only_endpoint=None,  # No dedicated data-only endpoint for futures
        api_version="v1",
        supported_intervals=[
            interval for interval in Interval if interval.value != "1s"
        ],  # All intervals except 1s
        symbol_format="BTCUSD_PERP",  # Using _PERP suffix for perpetual contracts
        description=(
            "Coin-margined futures (CM) market with support for most intervals except 1-second data. "
            "Returns up to 1500 records when requested. Symbol format uses _PERP suffix. "
            "Vision API uses futures/cm path."
        ),
        max_limit=1500,
        endpoint_reliability="Primary and backup endpoints are reliable and support all features.",
    ),
    # Keep legacy FUTURES type for backward compatibility
    MarketType.FUTURES: MarketCapabilities(
        primary_endpoint="https://fapi.binance.com",
        backup_endpoints=[
            "https://fapi-gcp.binance.com",
            "https://fapi1.binance.com",
            "https://fapi2.binance.com",
            "https://fapi3.binance.com",
        ],
        data_only_endpoint=None,  # No dedicated data-only endpoint for futures
        api_version="v1",
        supported_intervals=[
            interval for interval in Interval if interval.value != "1s"
        ],  # All intervals except 1s
        symbol_format="BTCUSDT",
        description=(
            "Generic futures market type (kept for backward compatibility). "
            "Defaults to USDT-margined futures behavior. "
            "For specific futures types, use FUTURES_USDT or FUTURES_COIN instead."
        ),
        max_limit=1500,
        endpoint_reliability="Primary and backup endpoints are reliable and support all features.",
    ),
}


def get_market_capabilities(market_type: MarketType) -> MarketCapabilities:
    """Get the capabilities for a specific market type."""
    return MARKET_CAPABILITIES[market_type]


def is_interval_supported(market_type: MarketType, interval: Interval) -> bool:
    """Check if an interval is supported by a specific market type."""
    return interval in MARKET_CAPABILITIES[market_type].supported_intervals


def get_minimum_interval(market_type: MarketType) -> Interval:
    """Get the minimum supported interval for a market type."""
    return min(
        MARKET_CAPABILITIES[market_type].supported_intervals,
        key=lambda x: x.to_seconds(),
    )


def get_endpoint_url(market_type: MarketType, use_data_only: bool = False) -> str:
    """Get the appropriate endpoint URL for a market type.

    Args:
        market_type: The type of market to get the endpoint for
        use_data_only: Whether to use the data-only endpoint (only available for spot market)

    Returns:
        The complete base URL for the API endpoint
    """
    capabilities = get_market_capabilities(market_type)

    if use_data_only and capabilities.data_only_endpoint:
        base_url = capabilities.data_only_endpoint
    else:
        base_url = capabilities.primary_endpoint

    return f"{base_url}/api/{capabilities.api_version}/klines"
