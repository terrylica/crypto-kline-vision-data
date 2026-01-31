#!/usr/bin/env python
"""Market constraints and configuration subpackage.

This subpackage provides market-specific enums, capabilities, validation,
and endpoint construction for the Data Source Manager.

Modules:
    enums: Core enum definitions (DataProvider, MarketType, ChartType, Interval)
    capabilities: Market capabilities and constraints (MarketCapabilities, MARKET_CAPABILITIES)
    validation: Symbol validation and format transformation
    endpoints: API endpoint URL construction

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Split from market_constraints.py (1009 lines) for modularity
"""

from data_source_manager.utils.market.capabilities import (
    MARKET_CAPABILITIES,
    OKX_MARKET_CAPABILITIES,
    MarketCapabilities,
    get_market_capabilities,
)
from data_source_manager.utils.market.endpoints import get_endpoint_url
from data_source_manager.utils.market.enums import (
    ChartType,
    DataProvider,
    Interval,
    MarketType,
    safe_enum_compare,
)
from data_source_manager.utils.market.validation import (
    get_default_symbol,
    get_market_symbol_format,
    get_minimum_interval,
    is_interval_supported,
    validate_symbol_for_market_type,
)

__all__ = [
    # Capabilities
    "MARKET_CAPABILITIES",
    "OKX_MARKET_CAPABILITIES",
    # Enums
    "ChartType",
    "DataProvider",
    "Interval",
    "MarketCapabilities",
    "MarketType",
    # Functions
    "get_default_symbol",
    "get_endpoint_url",
    "get_market_capabilities",
    "get_market_symbol_format",
    "get_minimum_interval",
    "is_interval_supported",
    "safe_enum_compare",
    "validate_symbol_for_market_type",
]
