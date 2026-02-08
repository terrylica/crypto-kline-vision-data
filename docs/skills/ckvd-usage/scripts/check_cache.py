#!/usr/bin/env python3
"""
Check cache status for a symbol.

Usage:
    uv run -p 3.13 python docs/skills/ckvd-usage/scripts/check_cache.py BTCUSDT FUTURES_USDT 1h
"""

import sys
from pathlib import Path

from platformdirs import user_cache_dir


def main() -> None:
    """Check cache status for a symbol."""
    if len(sys.argv) < 3:
        print("Usage: check_cache.py <symbol> <market_type> [interval]")
        print("Market types: spot, futures_usdt, futures_coin")
        sys.exit(1)

    symbol = sys.argv[1].upper()
    market_type = sys.argv[2].lower()
    interval = sys.argv[3] if len(sys.argv) > 3 else "1h"

    cache_base = Path(user_cache_dir("ckvd"))
    cache_path = cache_base / "binance" / market_type / "klines" / "daily" / symbol / interval

    print(f"Cache path: {cache_path}")
    print()

    if not cache_path.exists():
        print("✗ Cache directory does not exist")
        print(f"  Create with: mkdir -p {cache_path}")
        sys.exit(0)

    # List cached files
    arrow_files = sorted(cache_path.glob("*.arrow"))
    if not arrow_files:
        print("✗ No cached files found")
        sys.exit(0)

    print(f"✓ Found {len(arrow_files)} cached files")
    print()
    print("First 5 files:")
    for f in arrow_files[:5]:
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name} ({size_kb:.1f} KB)")

    if len(arrow_files) > 5:
        print(f"  ... and {len(arrow_files) - 5} more")

    print()
    print("Last 5 files:")
    for f in arrow_files[-5:]:
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name} ({size_kb:.1f} KB)")

    # Total size
    total_size = sum(f.stat().st_size for f in arrow_files)
    print()
    print(f"Total cache size: {total_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    main()
