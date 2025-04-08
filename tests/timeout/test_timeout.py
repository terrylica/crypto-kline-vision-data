#!/usr/bin/env python

import asyncio
from datetime import datetime, timezone, timedelta
from pathlib import Path

from core.data_source_manager import DataSourceManager, DataSource
from utils.market_constraints import MarketType, Interval


async def test_timeout():
    print("Starting timeout test...")

    # Create the directory if it doesn't exist
    Path("logs/timeout_incidents").mkdir(parents=True, exist_ok=True)

    # Create a DataSourceManager
    manager = DataSourceManager(market_type=MarketType.SPOT)

    # Use a reasonable time range (1 day)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=6)  # 6 hours is reasonable

    print(f"Fetching data for BTCUSDT from {start_time} to {end_time}...")
    print("This should timeout and log detailed diagnostic information.")

    # Force a timeout by using a very large data request with REST API enforced
    try:
        df = await manager.get_data(
            "BTCUSDT",
            start_time,
            end_time,
            interval=Interval.SECOND_1,
            enforce_source=DataSource.REST,  # Force REST API
        )

        if df.empty:
            print("Received empty DataFrame as expected due to timeout")
        else:
            print(f"Unexpected: Received {len(df)} rows")
    except Exception as e:
        print(f"Exception: {e}")

    # Small delay to ensure log is written
    await asyncio.sleep(1)

    # Check if timeout log exists
    log_path = Path("logs/timeout_incidents/timeout_log.txt")
    if log_path.exists():
        print(f"Log file created: {log_path}")
        with open(log_path, "r") as f:
            content = f.read()
            print("\nTimeout log content:")
            print("=" * 50)
            print(content)
            print("=" * 50)
    else:
        print("No timeout log file found")

    print("Test complete.")


if __name__ == "__main__":
    asyncio.run(test_timeout())
