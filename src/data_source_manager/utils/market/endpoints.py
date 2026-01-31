#!/usr/bin/env python
"""Endpoint URL construction for market data APIs.

This module provides functions for constructing API endpoint URLs
for different market types and data providers.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from market_constraints.py for modularity
"""

from __future__ import annotations

from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market.capabilities import get_market_capabilities
from data_source_manager.utils.market.enums import ChartType, DataProvider, MarketType

__all__ = [
    "get_endpoint_url",
]


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
    if isinstance(chart_type, ChartType):
        endpoint = chart_type.endpoint
    elif isinstance(chart_type, str):
        endpoint = chart_type
    else:
        endpoint = str(chart_type)

    # Default to the market's default API version if not specified
    if version is None:
        version = capabilities.api_version

    # Handle different providers
    if data_provider.name == "OKX":
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
            path = f"/fapi/{version}/{endpoint}"
        elif market_name == "OPTIONS":
            path = f"/eapi/{version}/{endpoint}"
        else:
            path = f"/api/{version}/{endpoint}"

    return f"{base_url}{path}"
