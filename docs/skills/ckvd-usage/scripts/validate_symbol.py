#!/usr/bin/env python3
"""
Validate symbol format for a given market type.

Usage:
    uv run -p 3.13 python docs/skills/ckvd-usage/scripts/validate_symbol.py BTCUSDT FUTURES_COIN
"""

import sys

from ckvd import MarketType
from ckvd.utils.market_constraints import validate_symbol_for_market_type


def main() -> None:
    """Validate symbol format."""
    if len(sys.argv) != 3:
        print("Usage: validate_symbol.py <symbol> <market_type>")
        print("Market types: SPOT, FUTURES_USDT, FUTURES_COIN")
        sys.exit(1)

    symbol = sys.argv[1]
    market_type_str = sys.argv[2].upper()

    try:
        market_type = MarketType[market_type_str]
    except KeyError:
        print(f"Invalid market type: {market_type_str}")
        print("Valid types: SPOT, FUTURES_USDT, FUTURES_COIN")
        sys.exit(1)

    try:
        validate_symbol_for_market_type(symbol, market_type)
        # Function returns True for valid symbols, raises ValueError for invalid
        print(f"✓ {symbol} is valid for {market_type.name}")
    except ValueError as e:
        # Function raises ValueError for invalid symbols
        print(f"✗ {symbol} is NOT valid for {market_type.name}")
        print(f"  {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
