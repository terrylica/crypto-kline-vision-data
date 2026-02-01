#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""
Quick Start Example for Data Source Manager.

Run with:
    uv run -p 3.13 python examples/quick_start.py

This demonstrates basic DSM usage with automatic FCP fallback.
"""

from datetime import datetime, timedelta, timezone

from data_source_manager import DataProvider, DataSourceManager, Interval, MarketType


def main() -> None:
    """Fetch recent BTCUSDT hourly data."""
    # Create manager for USDT-margined futures
    manager = DataSourceManager.create(
        DataProvider.BINANCE,
        MarketType.FUTURES_USDT,
    )

    # Time range: last 7 days
    # IMPORTANT: Always use UTC timezone-aware datetimes
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=7)

    print(f"Fetching BTCUSDT 1H data from {start_time} to {end_time}")
    print("Using FCP: Cache → Vision API → REST API")
    print()

    # Fetch data with automatic failover
    df = manager.get_data(
        symbol="BTCUSDT",
        interval=Interval.HOUR_1,
        start_time=start_time,
        end_time=end_time,
    )

    # Display results
    if df is not None and len(df) > 0:
        print(f"Loaded {len(df)} bars")
        print(f"Columns: {list(df.columns)}")
        print(f"Date range: {df.index.min()} to {df.index.max()}")
        print()
        print("First 5 rows:")
        print(df.head())
        print()
        print("Last 5 rows:")
        print(df.tail())
    else:
        print("No data returned. Check:")
        print("  - Symbol format matches market type")
        print("  - Date range is in the past")
        print("  - Network connectivity")

    # Cleanup
    manager.close()


if __name__ == "__main__":
    main()
