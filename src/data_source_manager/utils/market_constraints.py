#!/usr/bin/env python
"""Market constraints and configuration for data retrieval operations.

DEPRECATED: This module is a backward-compatibility re-export from the market/ subpackage.
Import directly from data_source_manager.utils.market for new code.

This module re-exports all symbols from the market/ subpackage to maintain
backward compatibility with existing code that imports from this location.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Original 1009-line module split into market/ subpackage
"""

# Re-export everything from the market subpackage
from data_source_manager.utils.market import (
    MARKET_CAPABILITIES,
    OKX_MARKET_CAPABILITIES,
    ChartType,
    DataProvider,
    Interval,
    MarketCapabilities,
    MarketType,
    get_default_symbol,
    get_endpoint_url,
    get_market_capabilities,
    get_market_symbol_format,
    get_minimum_interval,
    is_interval_supported,
    safe_enum_compare,
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
