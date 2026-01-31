#!/usr/bin/env python
"""Market capabilities and constraints definitions.

This module defines the capabilities and constraints for different market types
across various data providers (Binance, OKX, etc.).

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from market_constraints.py for modularity
"""

from __future__ import annotations

import attrs

from data_source_manager.utils.market.enums import DataProvider, Interval, MarketType

__all__ = [
    "MARKET_CAPABILITIES",
    "OKX_MARKET_CAPABILITIES",
    "MarketCapabilities",
    "get_market_capabilities",
]


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
        default_symbol="BTCUSDT",
    ),
    MarketType.FUTURES_USDT: MarketCapabilities(
        primary_endpoint="https://fapi.binance.com",
        backup_endpoints=[
            "https://fapi-gcp.binance.com",
            "https://fapi1.binance.com",
            "https://fapi2.binance.com",
            "https://fapi3.binance.com",
        ],
        data_only_endpoint=None,
        api_version="v1",
        supported_intervals=[i for i in Interval if i.value != "1s"],
        symbol_format="BTCUSDT",
        description=(
            "USDT-margined futures (UM) market with support for most intervals except 1-second data. "
            "Returns up to 1500 records when requested. Vision API uses futures/um path."
        ),
        max_limit=1500,
        endpoint_reliability="Primary and backup endpoints are reliable and support all features.",
        default_symbol="BTCUSDT",
    ),
    MarketType.FUTURES_COIN: MarketCapabilities(
        primary_endpoint="https://dapi.binance.com",
        backup_endpoints=[
            "https://dapi-gcp.binance.com",
            "https://dapi1.binance.com",
            "https://dapi2.binance.com",
            "https://dapi3.binance.com",
        ],
        data_only_endpoint=None,
        api_version="v1",
        supported_intervals=[i for i in Interval if i.value != "1s"],
        symbol_format="BTCUSD_PERP",
        description=(
            "Coin-margined futures (CM) market with support for most intervals except 1-second data. "
            "Returns up to 1500 records when requested. Symbol format uses _PERP suffix. "
            "Vision API uses futures/cm path."
        ),
        max_limit=1500,
        endpoint_reliability="Primary and backup endpoints are reliable and support all features.",
        default_symbol="BTCUSD_PERP",
    ),
    MarketType.FUTURES: MarketCapabilities(
        primary_endpoint="https://fapi.binance.com",
        backup_endpoints=[
            "https://fapi-gcp.binance.com",
            "https://fapi1.binance.com",
            "https://fapi2.binance.com",
            "https://fapi3.binance.com",
        ],
        data_only_endpoint=None,
        api_version="v1",
        supported_intervals=[i for i in Interval if i.value != "1s"],
        symbol_format="BTCUSDT",
        description=(
            "Generic futures market type (kept for backward compatibility). "
            "Defaults to USDT-margined futures behavior. "
            "For specific futures types, use FUTURES_USDT or FUTURES_COIN instead."
        ),
        max_limit=1500,
        endpoint_reliability="Primary and backup endpoints are reliable and support all features.",
        default_symbol="BTCUSDT",
    ),
    MarketType.OPTIONS: MarketCapabilities(
        primary_endpoint="https://eapi.binance.com",
        backup_endpoints=[
            "https://eapi1.binance.com",
            "https://eapi2.binance.com",
            "https://eapi3.binance.com",
        ],
        data_only_endpoint=None,
        api_version="v1",
        supported_intervals=[i for i in Interval if i.value != "1s"],
        symbol_format="BTC-230630-60000-C",
        description=(
            "Options market with structured contract naming. "
            "Supports standard intervals but not 1-second data. "
            "Uses different response format with named fields instead of arrays."
        ),
        max_limit=1000,
        endpoint_reliability="Primary endpoints are reliable for options data.",
        default_symbol="BTC-230630-60000-C",
    ),
}

OKX_MARKET_CAPABILITIES: dict[MarketType, MarketCapabilities] = {
    MarketType.SPOT: MarketCapabilities(
        primary_endpoint="https://www.okx.com",
        backup_endpoints=[],
        data_only_endpoint=None,
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
        ],
        symbol_format="BTC-USDT",
        description=(
            "OKX SPOT market with support for most intervals except 1-second data. "
            "Returns up to 300 records when requested. "
            "Uses instId parameter with hyphen format (BTC-USDT) instead of concatenated symbols."
        ),
        max_limit=300,
        endpoint_reliability="Primary endpoint is reliable for all data features.",
        default_symbol="BTC-USDT",
    ),
    MarketType.FUTURES_USDT: MarketCapabilities(
        primary_endpoint="https://www.okx.com",
        backup_endpoints=[],
        data_only_endpoint=None,
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
        ],
        symbol_format="BTC-USD-SWAP",
        description=(
            "OKX USD-margined perpetual swaps (SWAP) market with support for most intervals except 1-second data. "
            "Returns up to 300 records when requested. "
            "Uses instId parameter with hyphen format (BTC-USD-SWAP) for perpetual contracts."
        ),
        max_limit=300,
        endpoint_reliability="Primary endpoint is reliable for all data features.",
        default_symbol="BTC-USD-SWAP",
    ),
}


def get_market_capabilities(
    market_type: MarketType,
    data_provider: DataProvider = DataProvider.BINANCE,
) -> MarketCapabilities:
    """Get capabilities for a specific market type.

    Args:
        market_type: Market type to get capabilities for
        data_provider: Data provider to get capabilities for, defaults to BINANCE

    Returns:
        MarketCapabilities object with API info for the market type

    Raises:
        ValueError: If the market type is not found in capabilities dictionary
    """
    # Select the appropriate capabilities dictionary based on the provider
    capabilities_dict = OKX_MARKET_CAPABILITIES if data_provider.name == "OKX" else MARKET_CAPABILITIES

    # Use name-based comparison for compatibility with module reloading
    for key, value in capabilities_dict.items():
        if market_type.name == key.name:
            return value

    raise ValueError(f"Unknown market type: {market_type} for provider: {data_provider.name}")
