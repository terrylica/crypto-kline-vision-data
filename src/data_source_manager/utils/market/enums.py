#!/usr/bin/env python
"""Core market enum definitions.

This module defines the core enums used throughout the Data Source Manager:
- DataProvider: Data provider types (Binance, OKX, etc.)
- MarketType: Market types (SPOT, FUTURES_USDT, FUTURES_COIN, etc.)
- ChartType: Chart data types (klines, funding rate, etc.)
- Interval: Time intervals (1m, 5m, 1h, etc.)

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from market_constraints.py for modularity
"""

import re
from enum import Enum, auto

__all__ = [
    "ChartType",
    "DataProvider",
    "Interval",
    "MarketType",
    "safe_enum_compare",
]


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
    """Enum for market types across different exchanges."""

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

        Returns:
            int: Number of seconds in this interval

        Raises:
            ValueError: If the interval format is invalid
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
        """
        return cls.SECOND_1

    def __str__(self) -> str:
        """Return the string representation of the interval.

        Returns:
            str: String representation (e.g., "1m", "1h")
        """
        return self.value


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
