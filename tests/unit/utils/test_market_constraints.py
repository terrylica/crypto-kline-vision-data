#!/usr/bin/env python3
"""Unit tests for market constraints utilities."""

import pytest

from ckvd.utils.market_constraints import (
    MarketType,
    validate_symbol_for_market_type,
)


def test_validate_symbol_for_market_type_valid_cases():
    """Test that valid symbol-market combinations pass validation."""
    # Valid SPOT symbols
    assert validate_symbol_for_market_type("BTCUSDT", MarketType.SPOT) is True
    assert validate_symbol_for_market_type("ETHUSDT", MarketType.SPOT) is True
    assert validate_symbol_for_market_type("BTCBUSD", MarketType.SPOT) is True

    # Valid FUTURES_USDT symbols
    assert validate_symbol_for_market_type("BTCUSDT", MarketType.FUTURES_USDT) is True
    assert validate_symbol_for_market_type("ETHUSDT", MarketType.FUTURES_USDT) is True

    # Valid FUTURES_COIN symbols
    assert validate_symbol_for_market_type("BTCUSD_PERP", MarketType.FUTURES_COIN) is True
    assert validate_symbol_for_market_type("ETHUSD_PERP", MarketType.FUTURES_COIN) is True
    # Valid quarterly contracts
    assert validate_symbol_for_market_type("BTCUSD_220930", MarketType.FUTURES_COIN) is True


def test_validate_symbol_for_market_type_invalid_cases():
    """Test that invalid symbol-market combinations raise ValueError."""
    # Test SPOT market with FUTURES_COIN symbols
    with pytest.raises(ValueError) as excinfo:
        validate_symbol_for_market_type("BTCUSD_PERP", MarketType.SPOT)
    assert "Invalid symbol format for SPOT market" in str(excinfo.value)
    assert "appears to be a FUTURES_COIN symbol" in str(excinfo.value)

    # Test FUTURES_COIN market with invalid symbols (missing _PERP)
    with pytest.raises(ValueError) as excinfo:
        validate_symbol_for_market_type("BTCUSDT", MarketType.FUTURES_COIN)
    assert "Invalid symbol format for FUTURES_COIN market" in str(excinfo.value)
    assert "should end with '_PERP'" in str(excinfo.value)

    # Test OPTIONS market with invalid symbols
    with pytest.raises(ValueError) as excinfo:
        validate_symbol_for_market_type("BTCUSDT", MarketType.OPTIONS)
    assert "Invalid symbol format for OPTIONS market" in str(excinfo.value)
    assert "OPTIONS symbols should follow the format" in str(excinfo.value)


def test_validate_symbol_for_market_type_empty():
    """Test that empty symbols are rejected."""
    with pytest.raises(ValueError) as excinfo:
        validate_symbol_for_market_type("", MarketType.SPOT)
    assert "Symbol cannot be empty" in str(excinfo.value)


def test_validate_symbol_suggested_format():
    """Test that validation errors provide helpful suggestions."""
    # Test SPOT market with FUTURES_COIN symbols
    with pytest.raises(ValueError) as excinfo:
        validate_symbol_for_market_type("BTCUSD_PERP", MarketType.SPOT)
    # Should suggest BTCUSDT for SPOT
    assert "For SPOT market, try using 'BTCUSDT' instead" in str(excinfo.value)

    # Test FUTURES_COIN market with SPOT symbols
    with pytest.raises(ValueError) as excinfo:
        validate_symbol_for_market_type("BTCUSDT", MarketType.FUTURES_COIN)
    # Should suggest BTCUSD_PERP for FUTURES_COIN
    assert "Try using 'BTCUSD_PERP' instead" in str(excinfo.value)
