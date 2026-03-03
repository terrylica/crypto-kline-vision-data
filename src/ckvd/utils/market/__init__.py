#!/usr/bin/env python
"""Market constraints and configuration subpackage.

This subpackage provides market-specific enums, capabilities, validation,
and endpoint construction for the Crypto Kline Vision Data.

Modules:
    enums: Core enum definitions (DataProvider, MarketType, ChartType, Interval)
    capabilities: Market capabilities and constraints (MarketCapabilities, MARKET_CAPABILITIES)
    validation: Symbol validation and format transformation
    endpoints: API endpoint URL construction

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Split from market_constraints.py (1009 lines) for modularity
# Round 14: Lazy imports via __getattr__ to reduce cold start time.
"""

import importlib
from typing import Any

# Map each exported name to its source module (relative to this package)
_LAZY_IMPORTS: dict[str, str] = {
    # From .capabilities
    "MARKET_CAPABILITIES": ".capabilities",
    "OKX_MARKET_CAPABILITIES": ".capabilities",
    "MarketCapabilities": ".capabilities",
    "get_market_capabilities": ".capabilities",
    # From .endpoints
    "get_endpoint_url": ".endpoints",
    # From .enums
    "ChartType": ".enums",
    "DataProvider": ".enums",
    "Interval": ".enums",
    "MarketType": ".enums",
    "safe_enum_compare": ".enums",
    # From .validation
    "get_default_symbol": ".validation",
    "get_market_symbol_format": ".validation",
    "get_minimum_interval": ".validation",
    "is_interval_supported": ".validation",
    "validate_symbol_for_market_type": ".validation",
}


def __getattr__(name: str) -> Any:
    if name in _LAZY_IMPORTS:
        module = importlib.import_module(_LAZY_IMPORTS[name], __name__)
        val = getattr(module, name)
        # Cache in module globals for subsequent access
        globals()[name] = val
        return val
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
