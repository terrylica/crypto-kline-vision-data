#!/usr/bin/env python3
"""Diagnose FCP (Failover Control Protocol) behavior for a symbol.

Usage:
    uv run -p 3.13 python docs/skills/ckvd-usage/scripts/diagnose_fcp.py BTCUSDT FUTURES_USDT 1h
    uv run -p 3.13 python docs/skills/ckvd-usage/scripts/diagnose_fcp.py ETHUSDT SPOT 1h --days 7
"""

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Enable debug logging before imports
logging.basicConfig(level=logging.DEBUG, format="%(name)s - %(levelname)s - %(message)s")

from ckvd import DataProvider, CryptoKlineVisionData, Interval, MarketType


def get_market_type(name: str) -> MarketType:
    """Convert string to MarketType enum."""
    mapping = {
        "spot": MarketType.SPOT,
        "futures_usdt": MarketType.FUTURES_USDT,
        "futures_coin": MarketType.FUTURES_COIN,
    }
    return mapping[name.lower()]


def get_interval(name: str) -> Interval:
    """Convert string to Interval enum."""
    mapping = {
        "1m": Interval.MINUTE_1,
        "5m": Interval.MINUTE_5,
        "15m": Interval.MINUTE_15,
        "1h": Interval.HOUR_1,
        "4h": Interval.HOUR_4,
        "1d": Interval.DAY_1,
    }
    return mapping[name.lower()]


def check_cache_status(symbol: str, market_type: MarketType, interval: Interval) -> None:
    """Check local cache for symbol data."""
    cache_base = Path.home() / ".cache" / "ckvd" / "binance"
    market_path = cache_base / market_type.value / "klines" / "daily" / symbol / interval.value

    print(f"\n📂 Cache Path: {market_path}")

    if not market_path.exists():
        print("   Status: No cache directory exists")
        return

    arrow_files = list(market_path.glob("*.arrow"))
    if not arrow_files:
        print("   Status: Cache directory exists but empty")
        return

    arrow_files.sort()
    print(f"   Status: {len(arrow_files)} cached days")
    print(f"   Oldest: {arrow_files[0].stem}")
    print(f"   Newest: {arrow_files[-1].stem}")


def diagnose_fetch(symbol: str, market_type: MarketType, interval: Interval, days: int) -> None:
    """Perform diagnostic fetch and report FCP decisions."""
    print(f"\n🔍 Diagnosing FCP for {symbol} ({market_type.name}, {interval.value})")
    print(f"   Requesting: last {days} days")

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days)

    print(f"   Start: {start_time.isoformat()}")
    print(f"   End:   {end_time.isoformat()}")

    # Check cache first
    check_cache_status(symbol, market_type, interval)

    # Create manager and fetch
    print("\n📡 Fetching data...")
    manager = CryptoKlineVisionData.create(DataProvider.BINANCE, market_type)

    try:
        df = manager.get_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
        )

        print("\n✅ Success!")
        print(f"   Rows: {len(df)}")
        if len(df) > 0:
            print(f"   First: {df['open_time'].min()}")
            print(f"   Last:  {df['open_time'].max()}")
            print(f"   Columns: {list(df.columns)}")

    except Exception as e:
        print(f"\n❌ Failed: {type(e).__name__}: {e}")
        raise
    finally:
        manager.close()


def main() -> None:
    """Run FCP diagnostics."""
    parser = argparse.ArgumentParser(description="Diagnose FCP behavior")
    parser.add_argument("symbol", help="Trading symbol (e.g., BTCUSDT)")
    parser.add_argument(
        "market_type",
        choices=["spot", "futures_usdt", "futures_coin"],
        help="Market type",
    )
    parser.add_argument("interval", choices=["1m", "5m", "15m", "1h", "4h", "1d"], help="Interval")
    parser.add_argument("--days", type=int, default=3, help="Days of data to fetch (default: 3)")

    args = parser.parse_args()

    market_type = get_market_type(args.market_type)
    interval = get_interval(args.interval)

    diagnose_fetch(args.symbol, market_type, interval, args.days)


if __name__ == "__main__":
    main()
