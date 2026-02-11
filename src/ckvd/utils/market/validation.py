#!/usr/bin/env python
"""Symbol validation and format transformation functions.

This module provides functions for validating and transforming symbols
across different market types and data providers.

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from market_constraints.py for modularity
"""

from __future__ import annotations

import re

from ckvd.utils.config import (
    MIN_LONG_SYMBOL_LENGTH,
    MIN_SHORT_SYMBOL_LENGTH,
    OPTIONS_SYMBOL_PARTS,
)
from ckvd.utils.loguru_setup import logger
from ckvd.utils.market.capabilities import get_market_capabilities
from ckvd.utils.market.enums import DataProvider, Interval, MarketType

# Security: allowlist regex for symbol characters (CWE-22 path traversal prevention)
# Allows: uppercase letters, digits, underscore (BTCUSD_PERP), hyphen (BTC-USDT, BTC-240315-50000-C)
_SYMBOL_SAFE_PATTERN = re.compile(r"^[A-Z0-9_-]{1,30}$")

__all__ = [
    "get_default_symbol",
    "get_market_symbol_format",
    "get_minimum_interval",
    "is_interval_supported",
    "validate_symbol_for_market_type",
]


def is_interval_supported(market_type: MarketType, interval: Interval) -> bool:
    """Check if a specific interval is supported for a market type.

    Args:
        market_type: The market type to check (e.g., SPOT, FUTURES_USDT)
        interval: The interval to check (e.g., MINUTE_1, HOUR_1)

    Returns:
        bool: True if the interval is supported for the market type, False otherwise
    """
    capabilities = get_market_capabilities(market_type)
    return interval in capabilities.supported_intervals


def get_minimum_interval(market_type: MarketType) -> Interval:
    """Get the minimum supported interval for a market type.

    Args:
        market_type: The market type to check (e.g., SPOT, FUTURES_USDT)

    Returns:
        Interval: The minimum supported interval for the market type
    """
    capabilities = get_market_capabilities(market_type)
    return min(capabilities.supported_intervals, key=lambda x: x.to_seconds())


def get_default_symbol(market_type: MarketType) -> str:
    """Get the default trading symbol for a market type.

    Args:
        market_type: The market type to get the default symbol for

    Returns:
        str: The default symbol for the market type
    """
    capabilities = get_market_capabilities(market_type)
    return capabilities.default_symbol


def _format_okx_symbol(symbol: str, market_type: MarketType) -> str:
    """Format symbol for OKX exchange."""
    if "-" in symbol:
        return symbol

    if market_type.name == "SPOT":
        if len(symbol) >= MIN_LONG_SYMBOL_LENGTH and symbol.endswith(("USDT", "BUSD", "USDC")):
            return f"{symbol[:-4]}-{symbol[-4:]}"
        if len(symbol) >= MIN_SHORT_SYMBOL_LENGTH and symbol.endswith(("BTC", "ETH", "USD")):
            return f"{symbol[:-3]}-{symbol[-3:]}"
        return f"{symbol[:-4]}-{symbol[-4:]}" if len(symbol) > MIN_SHORT_SYMBOL_LENGTH else symbol

    if market_type.name == "FUTURES_USDT" and symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USD-SWAP"

    return symbol


def _format_binance_symbol(symbol: str, market_type: MarketType) -> str:
    """Format symbol for Binance exchange."""
    if market_type.name != "FUTURES_COIN":
        return symbol

    if symbol.endswith("_PERP") or any(c.isdigit() for c in symbol):
        return symbol
    if symbol.endswith("USDT"):
        return symbol[:-4] + "USD_PERP"
    if symbol.endswith("USD"):
        return symbol + "_PERP"
    return symbol + "_PERP"


def get_market_symbol_format(
    symbol: str | None,
    market_type: MarketType,
    data_provider: DataProvider = DataProvider.BINANCE,
) -> str:
    """Transform a standard symbol to the format required by the specified market type.

    Args:
        symbol: Base symbol (e.g., "BTCUSDT") or None for default
        market_type: Target market type
        data_provider: Data provider to use, defaults to BINANCE

    Returns:
        str: Properly formatted symbol for the specified market type
    """
    if not symbol:
        return get_default_symbol(market_type)

    capabilities = get_market_capabilities(market_type, data_provider)
    expected_format = capabilities.symbol_format

    if data_provider.name == "OKX" and "-" not in expected_format:
        logger.debug(f"Expected format '{expected_format}' may not match OKX format requirements")

    if data_provider.name == "OKX":
        return _format_okx_symbol(symbol, market_type)

    return _format_binance_symbol(symbol, market_type)


def validate_symbol_for_market_type(
    symbol: str | None,
    market_type: MarketType,
    data_provider: DataProvider = DataProvider.BINANCE,
) -> bool:
    """Validate that a symbol follows the correct format for a market type.

    Args:
        symbol: Symbol to validate (e.g., "BTCUSDT" or "BTCUSD_PERP")
        market_type: Market type to validate the symbol against
        data_provider: Data provider (defaults to BINANCE)

    Returns:
        bool: True if the symbol is valid for the market type

    Raises:
        ValueError: If symbol is empty or format is invalid for market type
    """
    if symbol == "":
        raise ValueError("Symbol cannot be empty")
    if symbol is None:
        symbol = get_default_symbol(market_type)

    # Security: reject path traversal and unsafe characters (CWE-22)
    if not _SYMBOL_SAFE_PATTERN.match(symbol.upper()):
        raise ValueError(
            f"Symbol contains invalid characters: '{symbol}'. "
            f"Symbols must match [A-Z0-9_-]{{1,30}}."
        )

    capabilities = get_market_capabilities(market_type, data_provider)
    market_name = market_type.name

    logger.debug(
        f"Validating symbol '{symbol}' for {market_name} with {data_provider.name} provider (expected format: {capabilities.symbol_format})"
    )

    # OKX symbol validation
    if data_provider.name == "OKX":
        if "-" not in symbol:
            suggested_symbol = get_market_symbol_format(symbol, market_type, data_provider)
            raise ValueError(
                f"Invalid symbol format for OKX {market_name} market: '{symbol}'. "
                f"OKX symbols should use hyphen format. "
                f"Try using '{suggested_symbol}' instead."
            )

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
    elif market_name == "FUTURES_COIN":
        if not symbol.endswith("_PERP") and not any(c.isdigit() for c in symbol):
            suggested_symbol = get_market_symbol_format(symbol, market_type, data_provider)
            raise ValueError(
                f"Invalid symbol format for {market_name} market: '{symbol}'. "
                f"FUTURES_COIN symbols should end with '_PERP' for perpetual contracts. "
                f"Try using '{suggested_symbol}' instead."
            )

    elif market_name == "SPOT":
        if symbol.endswith("_PERP"):
            suggested_symbol = symbol[:-5]
            if suggested_symbol.endswith("USD"):
                suggested_symbol += "T"

            raise ValueError(
                f"Invalid symbol format for {market_name} market: '{symbol}'. "
                f"'{symbol}' appears to be a FUTURES_COIN symbol. "
                f"For SPOT market, try using '{suggested_symbol}' instead."
            )

    elif market_name == "OPTIONS":
        if not ("-" in symbol and (symbol.endswith("-C") or symbol.endswith("-P")) and len(symbol.split("-")) == OPTIONS_SYMBOL_PARTS):
            raise ValueError(
                f"Invalid symbol format for {market_name} market: '{symbol}'. "
                f"OPTIONS symbols should follow the format: BTC-YYMMDD-STRIKE-C/P"
            )

    return True
