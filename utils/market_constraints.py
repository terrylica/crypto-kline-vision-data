#!/usr/bin/env python
"""Market constraints and configuration for data retrieval operations.

This module defines the core enums, constants, and utility functions that govern
market-specific behaviors throughout the Data Source Manager. It serves as the
single source of truth for market types, intervals, and data providers.

Key components:
- DataProvider: Enum for different data providers (Binance, OKX, etc.)
- MarketType: Enum for market types (SPOT, FUTURES_USDT, FUTURES_COIN, etc.)
- ChartType: Enum for different types of chart data (klines, funding rate, etc.)
- Interval: Enum for time intervals (1m, 5m, 1h, etc.)
- MarketCapabilities: Class that defines capabilities and constraints of markets
- Utility functions for symbol validation, endpoint construction, etc.

The module is designed to abstract away provider-specific details while providing
a consistent interface for the rest of the application.

Example:
    >>> from utils.market_constraints import DataProvider, MarketType, Interval, ChartType
    >>>
    >>> # Check if an interval is supported for a market type
    >>> is_supported = is_interval_supported(MarketType.SPOT, Interval.MINUTE_1)
    >>> print(f"Is 1m supported for SPOT? {is_supported}")
    Is 1m supported for SPOT? True
    >>>
    >>> # Get the default symbol for a market type
    >>> symbol = get_default_symbol(MarketType.FUTURES_USDT)
    >>> print(f"Default symbol for FUTURES_USDT: {symbol}")
    Default symbol for FUTURES_USDT: BTCUSDT
    >>>
    >>> # Convert string representations to enum values
    >>> market = MarketType.from_string("spot")
    >>> interval = Interval("1h")
    >>> chart = ChartType.from_string("klines")
"""

import re
from enum import Enum, auto

import attrs  # Add this import

from utils.config import (
    MIN_LONG_SYMBOL_LENGTH,
    MIN_SHORT_SYMBOL_LENGTH,
    OPTIONS_SYMBOL_PARTS,
)
from utils.loguru_setup import logger


class DataProvider(Enum):
    """Enum for data provider types."""

    BINANCE = auto()  # Binance data provider
    TRADESTATION = auto()  # TradeStation data provider
    OKX = auto()  # OKX data provider

    @classmethod
    def from_string(cls, provider_str: str) -> "DataProvider":
        """Convert string representation to DataProvider enum.

        Args:
            provider_str: String representation of data provider

        Returns:
            DataProvider enum value

        Raises:
            ValueError: If the string doesn't match any known data provider
        """
        mapping = {
            "binance": cls.BINANCE,
            "tradestation": cls.TRADESTATION,
            "okx": cls.OKX,
        }

        provider_str = provider_str.lower()
        if provider_str in mapping:
            return mapping[provider_str]

        raise ValueError(f"Unknown data provider string: {provider_str}")


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
        if self.name == "FUTURES_USDT":
            return "futures/um"
        if self.name == "FUTURES_COIN":
            return "futures/cm"
        if self.name == "FUTURES":
            return "futures/um"  # Default to UM for backward compatibility
        if self.name == "OPTIONS":
            return "options"  # Options path (if supported)
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
    """Types of chart data available from providers."""

    KLINES = "klines"  # Standard candlestick data
    FUNDING_RATE = "fundingRate"  # Funding rate data (futures)
    # OKX-specific chart types
    OKX_CANDLES = "market/candles"  # OKX standard candlestick data
    OKX_HISTORY_CANDLES = "market/history-candles"  # OKX historical candlestick data

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
        if self.name == "FUNDING_RATE":
            return "fundingRate"
        raise ValueError(f"Unknown chart type: {self}")

    @property
    def supported_markets(self) -> list[MarketType]:
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
        if self.name == "FUNDING_RATE":
            return [
                MarketType.FUTURES_USDT,
                MarketType.FUTURES_COIN,
                MarketType.FUTURES,
            ]
        if self.name in ("OKX_CANDLES", "OKX_HISTORY_CANDLES"):
            return [
                MarketType.SPOT,
                MarketType.FUTURES_USDT,
            ]
        return []

    @property
    def supported_providers(self) -> list["DataProvider"]:
        """Get list of data providers that support this chart type."""
        # Use name comparison instead of direct comparison to avoid module reloading issues
        if self.name in ("KLINES", "FUNDING_RATE"):
            return [DataProvider.BINANCE]
        if self.name in ("OKX_CANDLES", "OKX_HISTORY_CANDLES"):
            return [DataProvider.OKX]
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
            "fundingrate": cls.FUNDING_RATE,
            "candles": cls.OKX_CANDLES,
            "history-candles": cls.OKX_HISTORY_CANDLES,
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
        return any(safe_enum_compare(market_type, supported_market) for supported_market in self.supported_markets)

    def is_supported_by_provider(self, provider: DataProvider) -> bool:
        """Check if this chart type is supported by the specified data provider.

        Args:
            provider: Data provider to check

        Returns:
            True if this chart type is supported by the specified data provider
        """
        # Use name-based comparison for compatibility with module reloading
        return any(safe_enum_compare(provider, supported_provider) for supported_provider in self.supported_providers)


class Interval(Enum):
    """Time intervals for market data retrieval.

    This enum defines the standard time intervals available for market data,
    from 1 second to 1 month. Each enum value maps to the string representation
    used in API requests.

    Most market types support all intervals except for 1-second data, which is
    typically only available for SPOT markets.

    Attributes:
        SECOND_1: 1-second interval (available only for some SPOT markets)
        MINUTE_1: 1-minute interval (common default for most operations)
        MINUTE_3: 3-minute interval
        MINUTE_5: 5-minute interval
        MINUTE_15: 15-minute interval
        MINUTE_30: 30-minute interval
        HOUR_1: 1-hour interval
        HOUR_2: 2-hour interval
        HOUR_4: 4-hour interval
        HOUR_6: 6-hour interval
        HOUR_8: 8-hour interval
        HOUR_12: 12-hour interval
        DAY_1: 1-day interval
        DAY_3: 3-day interval
        WEEK_1: 1-week interval
        MONTH_1: 1-month interval (approximate - uses 30 days)

    Example:
        >>> from utils.market_constraints import Interval
        >>>
        >>> # Get the interval enum from string
        >>> interval = Interval("1m")
        >>> print(interval)
        1m
        >>>
        >>> # Convert interval to seconds for calculations
        >>> seconds = interval.to_seconds()
        >>> print(f"A 1-minute interval is {seconds} seconds")
        A 1-minute interval is 60 seconds
        >>>
        >>> # Check if one interval is longer than another
        >>> hour = Interval.HOUR_1
        >>> minute = Interval.MINUTE_1
        >>> print(f"Is hour longer than minute? {hour.to_seconds() > minute.to_seconds()}")
        Is hour longer than minute? True
    """

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
        """Convert interval to seconds for duration calculations.

        This method parses the interval string and converts it to a total
        number of seconds, which is useful for time range calculations.

        Returns:
            int: Number of seconds in this interval

        Raises:
            ValueError: If the interval format is invalid

        Note:
            For MONTH_1, this uses an approximation of 30 days (2,592,000 seconds).

        Example:
            >>> from utils.market_constraints import Interval
            >>>
            >>> # Calculate seconds for different intervals
            >>> minute = Interval.MINUTE_1.to_seconds()
            >>> hour = Interval.HOUR_1.to_seconds()
            >>> day = Interval.DAY_1.to_seconds()
            >>> print(f"1m = {minute}s, 1h = {hour}s, 1d = {day}s")
            1m = 60s, 1h = 3600s, 1d = 86400s
        """
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
        """Get default interval (1 second).

        Returns:
            Interval: The default interval (SECOND_1)

        Example:
            >>> from utils.market_constraints import Interval
            >>>
            >>> # Get default interval
            >>> default = Interval.get_default()
            >>> print(f"Default interval: {default}")
            Default interval: 1s
        """
        return cls.SECOND_1

    def __str__(self) -> str:
        """Return the string representation of the interval.

        Returns:
            str: String representation (e.g., "1m", "1h")

        Example:
            >>> from utils.market_constraints import Interval
            >>>
            >>> # Convert interval to string
            >>> interval = Interval.HOUR_4
            >>> print(f"String representation: {str(interval)}")
            String representation: 4h
        """
        return self.value


@attrs.define
class MarketCapabilities:
    """Encapsulates the capabilities and constraints of a market type."""

    primary_endpoint: str = attrs.field()  # Primary API endpoint
    backup_endpoints: list[str] = attrs.field()  # List of backup endpoints
    data_only_endpoint: str | None = attrs.field()  # Endpoint for market data only
    api_version: str = attrs.field()  # API version to use
    supported_intervals: list[Interval] = attrs.field()  # List of supported intervals
    symbol_format: str = attrs.field()  # Example format for symbols
    description: str = attrs.field()  # Detailed description of market capabilities
    max_limit: int = attrs.field()  # Maximum number of records per request
    endpoint_reliability: str = attrs.field()  # Description of endpoint reliability
    default_symbol: str = attrs.field()  # Default symbol for this market type

    @property
    def api_base_url(self) -> str:
        """Get the base URL for API requests.

        Returns:
            Base URL for the market
        """
        # Return the base domain without path components
        return self.primary_endpoint


MARKET_CAPABILITIES: dict[MarketType, MarketCapabilities] = {
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
        supported_intervals=list(Interval),  # All intervals including 1s
        symbol_format="BTCUSDT",
        description=(
            "Spot market with comprehensive support for all intervals including 1-second data. "
            "Perfect time alignment with exactly 1.00s for 1s data and 60.00s for 1m data. "
            "All endpoints consistently return exactly 1000 records when requested."
        ),
        max_limit=1000,
        endpoint_reliability="All endpoints (primary, backup, and data-only) are reliable and support all features.",
        default_symbol="BTCUSDT",  # Default symbol for SPOT market
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
        supported_intervals=[interval for interval in Interval if interval.value != "1s"],  # All intervals except 1s
        symbol_format="BTCUSDT",
        description=(
            "USDT-margined futures (UM) market with support for most intervals except 1-second data. "
            "Returns up to 1500 records when requested. Vision API uses futures/um path."
        ),
        max_limit=1500,
        endpoint_reliability="Primary and backup endpoints are reliable and support all features.",
        default_symbol="BTCUSDT",  # Default symbol for USDT-margined futures
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
        supported_intervals=[interval for interval in Interval if interval.value != "1s"],  # All intervals except 1s
        symbol_format="BTCUSD_PERP",  # Using _PERP suffix for perpetual contracts
        description=(
            "Coin-margined futures (CM) market with support for most intervals except 1-second data. "
            "Returns up to 1500 records when requested. Symbol format uses _PERP suffix. "
            "Vision API uses futures/cm path."
        ),
        max_limit=1500,
        endpoint_reliability="Primary and backup endpoints are reliable and support all features.",
        default_symbol="BTCUSD_PERP",  # Default symbol for coin-margined futures
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
        supported_intervals=[interval for interval in Interval if interval.value != "1s"],  # All intervals except 1s
        symbol_format="BTCUSDT",
        description=(
            "Generic futures market type (kept for backward compatibility). "
            "Defaults to USDT-margined futures behavior. "
            "For specific futures types, use FUTURES_USDT or FUTURES_COIN instead."
        ),
        max_limit=1500,
        endpoint_reliability="Primary and backup endpoints are reliable and support all features.",
        default_symbol="BTCUSDT",  # Default symbol for legacy futures
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
        supported_intervals=[interval for interval in Interval if interval.value != "1s"],  # All intervals except 1s
        symbol_format="BTC-230630-60000-C",  # BTC-YYMMDD-STRIKE-C/P format
        description=(
            "Options market with structured contract naming. "
            "Supports standard intervals but not 1-second data. "
            "Uses different response format with named fields instead of arrays."
        ),
        max_limit=1000,
        endpoint_reliability="Primary endpoints are reliable for options data.",
        default_symbol="BTC-230630-60000-C",  # Default symbol for options
    ),
}

# Define OKX-specific market capabilities
OKX_MARKET_CAPABILITIES: dict[MarketType, MarketCapabilities] = {
    MarketType.SPOT: MarketCapabilities(
        primary_endpoint="https://www.okx.com",
        backup_endpoints=[],  # No documented backup endpoints
        data_only_endpoint=None,  # No dedicated data-only endpoint
        api_version="v5",
        supported_intervals=[
            Interval.MINUTE_1,
            Interval.MINUTE_3,
            Interval.MINUTE_5,
            Interval.MINUTE_15,
            Interval.MINUTE_30,
            Interval.HOUR_1,
            Interval.HOUR_2,
            Interval.HOUR_4,
            Interval.HOUR_6,
            Interval.HOUR_12,
            Interval.DAY_1,
            Interval.WEEK_1,
            Interval.MONTH_1,
        ],  # All intervals except 1s
        symbol_format="BTC-USDT",
        description=(
            "OKX SPOT market with support for most intervals except 1-second data. "
            "Returns up to 300 records when requested. "
            "Uses instId parameter with hyphen format (BTC-USDT) instead of concatenated symbols."
        ),
        max_limit=300,
        endpoint_reliability="Primary endpoint is reliable for all data features.",
        default_symbol="BTC-USDT",  # Default symbol for OKX SPOT market
    ),
    MarketType.FUTURES_USDT: MarketCapabilities(
        primary_endpoint="https://www.okx.com",
        backup_endpoints=[],  # No documented backup endpoints
        data_only_endpoint=None,  # No dedicated data-only endpoint
        api_version="v5",
        supported_intervals=[
            Interval.MINUTE_1,
            Interval.MINUTE_3,
            Interval.MINUTE_5,
            Interval.MINUTE_15,
            Interval.MINUTE_30,
            Interval.HOUR_1,
            Interval.HOUR_2,
            Interval.HOUR_4,
            Interval.HOUR_6,
            Interval.HOUR_12,
            Interval.DAY_1,
            Interval.WEEK_1,
            Interval.MONTH_1,
        ],  # All intervals except 1s
        symbol_format="BTC-USD-SWAP",
        description=(
            "OKX USD-margined perpetual swaps (SWAP) market with support for most intervals except 1-second data. "
            "Returns up to 300 records when requested. "
            "Uses instId parameter with hyphen format (BTC-USD-SWAP) for perpetual contracts."
        ),
        max_limit=300,
        endpoint_reliability="Primary endpoint is reliable for all data features.",
        default_symbol="BTC-USD-SWAP",  # Default symbol for OKX SWAP market
    ),
}


def get_market_capabilities(market_type: MarketType, data_provider: DataProvider = DataProvider.BINANCE) -> MarketCapabilities:
    """Get capabilities for a specific market type.

    Args:
        market_type: Market type to get capabilities for
        data_provider: Data provider to get capabilities for, defaults to BINANCE

    Returns:
        MarketCapabilities object with API info for the market type

    Raises:
        ValueError: If the market type is not found in capabilities dictionary
    """
    # Check if the market type is in our predefined capabilities
    # Log debug information to help diagnose enum comparison issues
    logger.debug(f"Getting capabilities for market_type={market_type}, type={type(market_type)}, provider={data_provider}")

    # Select the appropriate capabilities dictionary based on the provider
    if data_provider.name == "OKX":
        capabilities_dict = OKX_MARKET_CAPABILITIES
        logger.debug(f"Using OKX capabilities, keys: {[k.name for k in capabilities_dict]}")
    else:
        capabilities_dict = MARKET_CAPABILITIES
        logger.debug(f"Using standard capabilities, keys: {[k.name for k in capabilities_dict]}")

    # First try direct lookup by name
    for key, value in capabilities_dict.items():
        # Log each comparison to help debug
        logger.debug(f"Comparing: id(market_type)={id(market_type)}, id of first key={id(key)}")
        logger.debug(f"Modules: market_type from {market_type.__module__}, key from {key.__module__}")

        # Use name-based comparison for compatibility with module reloading
        if market_type.name == key.name:
            logger.debug(f"Found by name match: {key.name}")
            return value

    # If we get here, we couldn't find the market by name
    raise ValueError(f"Unknown market type: {market_type} for provider: {data_provider.name}")


def is_interval_supported(market_type: MarketType, interval: Interval) -> bool:
    """Check if a specific interval is supported for a market type.

    This function checks if the given interval is available for the specified market type
    by looking up the supported intervals in the market capabilities.

    Args:
        market_type: The market type to check (e.g., SPOT, FUTURES_USDT)
        interval: The interval to check (e.g., MINUTE_1, HOUR_1)

    Returns:
        bool: True if the interval is supported for the market type, False otherwise

    Example:
        >>> from utils.market_constraints import MarketType, Interval, is_interval_supported
        >>>
        >>> # Check if 1-second data is supported for SPOT market
        >>> is_interval_supported(MarketType.SPOT, Interval.SECOND_1)
        True
        >>>
        >>> # Check if 1-second data is supported for futures market
        >>> is_interval_supported(MarketType.FUTURES_USDT, Interval.SECOND_1)
        False
    """
    capabilities = get_market_capabilities(market_type)
    return interval in capabilities.supported_intervals


def get_minimum_interval(market_type: MarketType) -> Interval:
    """Get the minimum supported interval for a market type.

    This function returns the smallest time interval supported by the specified market type.
    For example, SPOT markets typically support 1-second data, while futures markets
    only support down to 1-minute data.

    Args:
        market_type: The market type to check (e.g., SPOT, FUTURES_USDT)

    Returns:
        Interval: The minimum supported interval for the market type

    Example:
        >>> from utils.market_constraints import MarketType, get_minimum_interval
        >>>
        >>> # Get minimum interval for SPOT market
        >>> min_spot = get_minimum_interval(MarketType.SPOT)
        >>> print(f"Minimum interval for SPOT: {min_spot}")
        Minimum interval for SPOT: 1s
        >>>
        >>> # Get minimum interval for futures market
        >>> min_futures = get_minimum_interval(MarketType.FUTURES_USDT)
        >>> print(f"Minimum interval for FUTURES_USDT: {min_futures}")
        Minimum interval for FUTURES_USDT: 1m
    """
    capabilities = get_market_capabilities(market_type)
    return min(capabilities.supported_intervals, key=lambda x: x.to_seconds())


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


def get_default_symbol(market_type: MarketType) -> str:
    """Get the default trading symbol for a market type.

    This function returns the standard default symbol used for the specified
    market type. This is useful when no specific symbol is provided.

    Args:
        market_type: The market type to get the default symbol for

    Returns:
        str: The default symbol for the market type
            SPOT/FUTURES_USDT: "BTCUSDT"
            FUTURES_COIN: "BTCUSD_PERP"
            OPTIONS: "BTC-230630-25000-C"

    Example:
        >>> from utils.market_constraints import MarketType, get_default_symbol
        >>>
        >>> # Get default symbols for different market types
        >>> spot_symbol = get_default_symbol(MarketType.SPOT)
        >>> futures_symbol = get_default_symbol(MarketType.FUTURES_USDT)
        >>> coin_futures_symbol = get_default_symbol(MarketType.FUTURES_COIN)
        >>> print(f"SPOT: {spot_symbol}, UM: {futures_symbol}, CM: {coin_futures_symbol}")
        SPOT: BTCUSDT, UM: BTCUSDT, CM: BTCUSD_PERP
    """
    capabilities = get_market_capabilities(market_type)
    return capabilities.default_symbol


def get_market_symbol_format(
    symbol: str | None,
    market_type: MarketType,
    data_provider: DataProvider = DataProvider.BINANCE,
) -> str:
    """Transform a standard symbol to the format required by the specified market type.

    This function serves as the single source of truth for symbol transformations
    across all market types.

    Args:
        symbol: Base symbol (e.g., "BTCUSDT") or None for default
        market_type: Target market type
        data_provider: Data provider to use, defaults to BINANCE

    Returns:
        str: Properly formatted symbol for the specified market type
    """
    # If symbol is None or empty, use default symbol for the market type
    if not symbol:
        return get_default_symbol(market_type)

    # Get the capabilities for the market type to access the expected format
    capabilities = get_market_capabilities(market_type, data_provider)

    # Log the expected format from capabilities - will be used in future multi-provider extensions
    # Currently the format is handled inline for each provider, but capabilities will be used
    # for more sophisticated format detection in future versions
    expected_format = capabilities.symbol_format

    # Log the expected format to help with debugging
    if data_provider.name == "OKX" and "-" not in expected_format:
        logger.debug(f"Expected format '{expected_format}' may not match OKX format requirements")

    # For OKX provider, handle hyphenated symbols
    if data_provider.name == "OKX":
        # Already has hyphens? Keep as is
        if "-" in symbol:
            return symbol

        # Handle SPOT market (convert BTCUSDT to BTC-USDT)
        if market_type.name == "SPOT":
            # Try to find standard patterns of base/quote currency
            if len(symbol) >= MIN_LONG_SYMBOL_LENGTH and symbol.endswith(("USDT", "BUSD", "USDC")):
                base = symbol[:-4]
                quote = symbol[-4:]
                return f"{base}-{quote}"
            if len(symbol) >= MIN_SHORT_SYMBOL_LENGTH and symbol.endswith(("BTC", "ETH", "USD")):
                base = symbol[:-3]
                quote = symbol[-3:]
                return f"{base}-{quote}"
            # Default approach: assume last 4 characters are quote currency
            return f"{symbol[:-4]}-{symbol[-4:]}" if len(symbol) > MIN_SHORT_SYMBOL_LENGTH else symbol

        # Handle FUTURES_USDT market (convert to BTC-USD-SWAP format)
        if market_type.name == "FUTURES_USDT":
            # Convert BTCUSDT format to BTC-USD-SWAP
            if symbol.endswith("USDT"):
                base = symbol[:-4]
                return f"{base}-USD-SWAP"
            # If not a standard format, return as is
            return symbol

    # For Binance provider, use the original logic
    else:
        # Use name-based comparison for stability with module reloading
        market_name = market_type.name

        # For CM futures (FUTURES_COIN), perform special transformations
        if market_name == "FUTURES_COIN":
            # Already has _PERP suffix? Keep as is
            if symbol.endswith("_PERP"):
                return symbol

            # Contains digits? Likely a quarterly contract, keep as is
            if any(c.isdigit() for c in symbol):
                return symbol

            # BTCUSDT format -> BTCUSD_PERP
            if symbol.endswith("USDT"):
                return symbol[:-4] + "USD_PERP"

            # BTCUSD format -> BTCUSD_PERP
            if symbol.endswith("USD"):
                return symbol + "_PERP"

            # Other format -> symbol_PERP
            return symbol + "_PERP"

    # For other market types, default to returning the original symbol
    # (Usually no transformation needed for SPOT or FUTURES_USDT)
    return symbol


def validate_symbol_for_market_type(
    symbol: str | None,
    market_type: MarketType,
    data_provider: DataProvider = DataProvider.BINANCE,
) -> bool:
    """Validate that a symbol follows the correct format for a market type.

    This function checks if the provided symbol matches the expected format
    for the specified market type. Different market types have different
    symbol format requirements:

    - SPOT/FUTURES_USDT: Base asset + Quote asset (e.g., "BTCUSDT")
    - FUTURES_COIN: Base asset + Quote asset + "_PERP" (e.g., "BTCUSD_PERP")
    - OPTIONS: Base asset + expiration date + strike price + call/put (e.g., "BTC-230630-25000-C")

    Args:
        symbol: Symbol to validate (e.g., "BTCUSDT" or "BTCUSD_PERP")
        market_type: Market type to validate the symbol against
        data_provider: Data provider (defaults to BINANCE)

    Returns:
        bool: True if the symbol is valid for the market type, False otherwise

    Example:
        >>> from utils.market_constraints import MarketType, DataProvider, validate_symbol_for_market_type
        >>>
        >>> # Validate symbols for different market types
        >>> validate_symbol_for_market_type("BTCUSDT", MarketType.SPOT)
        True
        >>> validate_symbol_for_market_type("BTCUSD_PERP", MarketType.FUTURES_COIN)
        True
        >>> validate_symbol_for_market_type("BTCUSDT", MarketType.FUTURES_COIN)
        False
    """
    # If symbol is None, use default symbol for validation
    # But empty strings should raise an error
    if symbol == "":
        raise ValueError("Symbol cannot be empty")
    if symbol is None:
        symbol = get_default_symbol(market_type)

    # Get the expected format from market capabilities
    capabilities = get_market_capabilities(market_type, data_provider)
    market_name = market_type.name

    # Log market capabilities attributes to aid debugging
    logger.debug(
        f"Validating symbol '{symbol}' for {market_name} with {data_provider.name} provider (expected format: {capabilities.symbol_format})"
    )

    # The capabilities object provides full market information
    # Future versions will dynamically construct endpoints from capabilities
    # Currently using direct market_name based logic for clarity

    # OKX symbol validation
    if data_provider.name == "OKX":
        # OKX symbols should have hyphen format
        if "-" not in symbol:
            suggested_symbol = get_market_symbol_format(symbol, market_type, data_provider)
            raise ValueError(
                f"Invalid symbol format for OKX {market_name} market: '{symbol}'. "
                f"OKX symbols should use hyphen format. "
                f"Try using '{suggested_symbol}' instead."
            )

        # Special validation for FUTURES_USDT (SWAP) market
        if market_name == "FUTURES_USDT" and not symbol.endswith("-SWAP"):
            suggested_symbol = symbol if symbol.endswith("-SWAP") else f"{symbol}-SWAP"
            if "-USD-" not in suggested_symbol:
                base = suggested_symbol.split("-")[0]
                suggested_symbol = f"{base}-USD-SWAP"
            raise ValueError(
                f"Invalid symbol format for OKX {market_name} market: '{symbol}'. "
                f"OKX SWAP symbols should end with '-SWAP'. "
                f"Try using '{suggested_symbol}' instead."
            )

    # Binance symbol validation
    # Special validation for FUTURES_COIN market
    elif market_name == "FUTURES_COIN":
        # Check if symbol has PERP suffix for perpetual contracts
        if not symbol.endswith("_PERP") and not any(c.isdigit() for c in symbol):
            suggested_symbol = get_market_symbol_format(symbol, market_type, data_provider)
            raise ValueError(
                f"Invalid symbol format for {market_name} market: '{symbol}'. "
                f"FUTURES_COIN symbols should end with '_PERP' for perpetual contracts. "
                f"Try using '{suggested_symbol}' instead."
            )

    # Special validation for SPOT market
    elif market_name == "SPOT":
        # SPOT symbols should not have _PERP suffix
        if symbol.endswith("_PERP"):
            # Strip _PERP suffix to suggest a valid SPOT symbol
            suggested_symbol = symbol[:-5]
            if suggested_symbol.endswith("USD"):
                suggested_symbol += "T"  # Convert BTCUSD to BTCUSDT for SPOT

            raise ValueError(
                f"Invalid symbol format for {market_name} market: '{symbol}'. "
                f"'{symbol}' appears to be a FUTURES_COIN symbol. "
                f"For SPOT market, try using '{suggested_symbol}' instead."
            )

    # Special validation for OPTIONS market
    elif market_name == "OPTIONS":
        # Options symbols should follow the BTC-YYMMDD-STRIKE-C/P format
        if not ("-" in symbol and (symbol.endswith("-C") or symbol.endswith("-P")) and len(symbol.split("-")) == OPTIONS_SYMBOL_PARTS):
            raise ValueError(
                f"Invalid symbol format for {market_name} market: '{symbol}'. "
                f"OPTIONS symbols should follow the format: BTC-YYMMDD-STRIKE-C/P"
            )

    return True


def get_endpoint_url(
    market_type: MarketType,
    chart_type: str | ChartType,
    version: str | None = None,
    data_provider: DataProvider = DataProvider.BINANCE,
) -> str:
    """Get the URL for a specific endpoint based on market type.

    Args:
        market_type: Type of market (spot, futures, etc.)
        chart_type: Chart data type (e.g., "klines", "uiKlines", or ChartType enum)
        version: API version to use, defaults to the market's default version
        data_provider: Data provider to use, defaults to BINANCE

    Returns:
        Full URL to the endpoint
    """
    capabilities = get_market_capabilities(market_type, data_provider)
    base_url = capabilities.api_base_url

    # Verify the chart type is compatible with the market capabilities
    if isinstance(chart_type, ChartType) and not chart_type.is_supported_by_market(market_type):
        logger.warning(
            f"Chart type {chart_type.name} may not be supported for {market_type.name} market. "
            f"Supported intervals: {[i.value for i in capabilities.supported_intervals]}"
        )

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

    # Handle different providers
    if data_provider.name == "OKX":
        # OKX uses a standard API pattern for all market types
        path = f"/api/{version}/{endpoint}"
    else:
        # Binance endpoints based on market type
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

    return f"{base_url}{path}"
