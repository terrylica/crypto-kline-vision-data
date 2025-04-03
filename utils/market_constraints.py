#!/usr/bin/env python

from dataclasses import dataclass
from enum import Enum, auto
from typing import Dict, List, Optional
import re

from utils.logger_setup import logger


class MarketType(Enum):
    SPOT = auto()
    FUTURES_USDT = auto()  # USDT-margined futures (UM)
    FUTURES_COIN = auto()  # Coin-margined futures (CM)
    FUTURES = auto()  # Legacy/generic futures type for backward compatibility
    OPTIONS = auto()  # Options trading

    @property
    def is_futures(self) -> bool:
        """Check if this is any type of futures market."""
        return self.name in ("FUTURES", "FUTURES_USDT", "FUTURES_COIN")

    @property
    def vision_api_path(self) -> str:
        """Get the corresponding path component for Binance Vision API."""
        # Use name comparison instead of direct comparison to avoid module reloading issues
        if self.name == "SPOT":
            return "spot"
        elif self.name == "FUTURES_USDT":
            return "futures/um"
        elif self.name == "FUTURES_COIN":
            return "futures/cm"
        elif self.name == "FUTURES":
            return "futures/um"  # Default to UM for backward compatibility
        elif self.name == "OPTIONS":
            return "options"  # Options path (if supported)
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
            "options": cls.OPTIONS,
            "eapi": cls.OPTIONS,
        }

        market_type_str = market_type_str.lower()
        if market_type_str in mapping:
            return mapping[market_type_str]

        raise ValueError(f"Unknown market type string: {market_type_str}")


class ChartType(Enum):
    """Types of chart data available from Binance API."""

    KLINES = "klines"  # Standard candlestick data
    UI_KLINES = "uiKlines"  # Optimized klines for UI applications
    MARK_PRICE_KLINES = "markPriceKlines"  # Mark price klines (futures)
    PREMIUM_INDEX_KLINES = "premiumIndexKlines"  # Premium index klines (futures)
    CONTINUOUS_KLINES = "continuousKlines"  # Continuous contract klines (futures)

    @property
    def endpoint(self) -> str:
        """Get the API endpoint name for this chart type."""
        return self.value

    @property
    def vision_api_path(self) -> str:
        """Get the corresponding path component for Binance Vision API."""
        # Use name comparison instead of direct comparison to avoid module reloading issues
        if self.name == "KLINES":
            return "klines"
        elif self.name == "UI_KLINES":
            return "ui-klines"
        elif self.name == "MARK_PRICE_KLINES":
            return "markPriceKlines"
        elif self.name == "PREMIUM_INDEX_KLINES":
            return "premiumIndexKlines"
        elif self.name == "CONTINUOUS_KLINES":
            return "continuousKlines"
        else:
            raise ValueError(f"Unknown chart type: {self}")

    @property
    def supported_markets(self) -> List[MarketType]:
        """Get list of market types that support this chart type."""
        # Use name comparison instead of direct comparison to avoid module reloading issues
        if self.name == "KLINES":
            return [
                MarketType.SPOT,
                MarketType.FUTURES_USDT,
                MarketType.FUTURES_COIN,
                MarketType.FUTURES,
                MarketType.OPTIONS,
            ]
        elif self.name == "UI_KLINES":
            return [MarketType.SPOT]
        elif self.name in (
            "MARK_PRICE_KLINES",
            "PREMIUM_INDEX_KLINES",
            "CONTINUOUS_KLINES",
        ):
            return [
                MarketType.FUTURES_USDT,
                MarketType.FUTURES_COIN,
                MarketType.FUTURES,
            ]
        else:
            return []

    @classmethod
    def from_string(cls, chart_type_str: str) -> "ChartType":
        """Convert string representation to ChartType enum.

        Args:
            chart_type_str: String representation of chart type

        Returns:
            ChartType enum value

        Raises:
            ValueError: If the string doesn't match any known chart type
        """
        mapping = {
            "klines": cls.KLINES,
            "uiklines": cls.UI_KLINES,
            "markpriceklines": cls.MARK_PRICE_KLINES,
            "premiumindexklines": cls.PREMIUM_INDEX_KLINES,
            "continuousklines": cls.CONTINUOUS_KLINES,
        }

        chart_type_str = chart_type_str.lower()
        if chart_type_str in mapping:
            return mapping[chart_type_str]

        raise ValueError(f"Unknown chart type string: {chart_type_str}")

    def is_supported_by_market(self, market_type: MarketType) -> bool:
        """Check if this chart type is supported by the specified market type.

        Args:
            market_type: Market type to check

        Returns:
            True if this chart type is supported by the specified market type
        """
        # Use name-based comparison for compatibility with module reloading
        for supported_market in self.supported_markets:
            if safe_enum_compare(market_type, supported_market):
                return True
        return False


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
        """Get the base URL for API requests.

        Returns:
            Base URL for the market
        """
        # Return the base domain without path components
        return self.primary_endpoint


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
    # Add Options market type
    MarketType.OPTIONS: MarketCapabilities(
        primary_endpoint="https://eapi.binance.com",
        backup_endpoints=[
            "https://eapi1.binance.com",
            "https://eapi2.binance.com",
            "https://eapi3.binance.com",
        ],
        data_only_endpoint=None,  # No dedicated data-only endpoint for options
        api_version="v1",
        supported_intervals=[
            interval for interval in Interval if interval.value != "1s"
        ],  # All intervals except 1s
        symbol_format="BTC-230630-60000-C",  # BTC-YYMMDD-STRIKE-C/P format
        description=(
            "Options market with structured contract naming. "
            "Supports standard intervals but not 1-second data. "
            "Uses different response format with named fields instead of arrays."
        ),
        max_limit=1000,
        endpoint_reliability="Primary endpoints are reliable for options data.",
    ),
}


def get_market_capabilities(market_type: MarketType) -> MarketCapabilities:
    """Get capabilities for a specific market type.

    Args:
        market_type: Market type to get capabilities for

    Returns:
        MarketCapabilities object with API info for the market type

    Raises:
        ValueError: If the market type is not found in capabilities dictionary
    """
    # Check if the market type is in our predefined capabilities
    # Log debug information to help diagnose enum comparison issues
    logger.debug(
        f"Getting capabilities for market_type={market_type}, type={type(market_type)}"
    )
    logger.debug(f"Available keys: {[k.name for k in MARKET_CAPABILITIES.keys()]}")

    # First try direct lookup by name
    for key, value in MARKET_CAPABILITIES.items():
        # Log each comparison to help debug
        logger.debug(
            f"Comparing: id(market_type)={id(market_type)}, id of first key={id(key)}"
        )
        logger.debug(
            f"Modules: market_type from {market_type.__module__}, key from {key.__module__}"
        )

        # Use name-based comparison for compatibility with module reloading
        if market_type.name == key.name:
            logger.debug(f"Found by name match: {key.name}")
            return value

    # If we get here, we couldn't find the market by name
    raise ValueError(f"Unknown market type: {market_type}")


def is_interval_supported(market_type: MarketType, interval: Interval) -> bool:
    """Check if an interval is supported by a specific market type."""
    return interval in MARKET_CAPABILITIES[market_type].supported_intervals


def get_minimum_interval(market_type: MarketType) -> Interval:
    """Get the minimum supported interval for a market type."""
    return min(
        MARKET_CAPABILITIES[market_type].supported_intervals,
        key=lambda x: x.to_seconds(),
    )


def safe_enum_compare(enum1: Enum, enum2: Enum) -> bool:
    """Compare enums by name to handle module reloading issues.

    When modules are reloaded during testing, new instances of Enum constants are created,
    making direct identity checks fail. This function compares enums by their names instead.

    Args:
        enum1: First enum to compare
        enum2: Second enum to compare

    Returns:
        True if enums have the same name, False otherwise
    """
    return enum1.name == enum2.name


def get_endpoint_url(
    market_type: MarketType, chart_type: str | ChartType, version: str = None
) -> str:
    """Get the URL for a specific endpoint based on market type.

    Args:
        market_type: Type of market (spot, futures, etc.)
        chart_type: Chart data type (e.g., "klines", "uiKlines", or ChartType enum)
        version: API version to use, defaults to the market's default version

    Returns:
        Full URL to the endpoint
    """
    capabilities = get_market_capabilities(market_type)
    base_url = capabilities.api_base_url

    # Extract endpoint string from ChartType enum if needed
    endpoint = None
    if isinstance(chart_type, ChartType):
        endpoint = chart_type.endpoint
    elif isinstance(chart_type, str):
        endpoint = chart_type
    else:
        endpoint = str(chart_type)  # Fallback

    # Default to the market's default API version if not specified
    if version is None:
        version = capabilities.api_version

    # Construct appropriate path based on market type name instead of direct comparison
    market_name = market_type.name
    if market_name == "SPOT":
        path = f"/api/{version}/{endpoint}"
    elif market_name == "FUTURES_USDT":
        path = f"/fapi/{version}/{endpoint}"
    elif market_name == "FUTURES_COIN":
        path = f"/dapi/{version}/{endpoint}"
    elif market_name == "FUTURES":
        path = f"/fapi/{version}/{endpoint}"  # Use /fapi/ for generic futures too
    elif market_name == "OPTIONS":
        path = f"/eapi/{version}/{endpoint}"
    else:
        path = f"/api/{version}/{endpoint}"  # Fallback for unknown markets

    url = f"{base_url}{path}"
    return url
