#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""OKX provider implementation.

This module provides OKX-specific implementations for the data provider factory.
OKX does not have a Vision API (bulk historical S3 storage), so FCP uses:
    Cache → REST (history-candles) → REST (candles)

Symbol formats:
- SPOT: BTC-USDT (hyphenated)
- SWAP/Futures: BTC-USD-SWAP

API endpoints:
- /api/v5/market/candles - Recent data (up to 300 records)
- /api/v5/market/history-candles - Historical data (up to 100 records)
"""

from data_source_manager.core.providers.okx.okx_rest_client import OKXRestClient

__all__ = [
    "OKXRestClient",
]
