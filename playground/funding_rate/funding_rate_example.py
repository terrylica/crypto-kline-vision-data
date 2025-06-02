#!/usr/bin/env python
"""Example of fetching funding rate data using the unified data source manager."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from core.sync.data_source_manager import DataSourceManager
from utils.loguru_setup import logger
from utils.market_constraints import ChartType, DataProvider, Interval, MarketType


def fetch_funding_rates():
    """Fetch and display funding rate data."""
    # Create a temporary cache directory
    cache_dir = Path("tmp/funding_rate_cache")
    cache_dir.mkdir(exist_ok=True, parents=True)

    logger.info("Creating DataSourceManager with FUTURES_USDT market type")

    # Create a DataSourceManager configured for funding rate data
    with DataSourceManager(
        provider=DataProvider.BINANCE,
        market_type=MarketType.FUTURES_USDT,
        chart_type=ChartType.FUNDING_RATE,
        cache_dir=cache_dir,
        use_cache=True,
    ) as dsm:
        # Define time range (5 days ago until now)
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=5)

        logger.info(
            f"Fetching funding rate data for BTCUSDT from {start_time} to {end_time}"
        )

        # Fetch funding rate data
        df = dsm.get_data(
            symbol="BTCUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_8,  # Funding rates are typically 8-hour intervals
        )

        if df.empty:
            logger.warning("No funding rate data found")
        else:
            logger.info(f"Retrieved {len(df)} funding rate data points")

            # Display the data
            pd.set_option("display.max_rows", 20)
            print("\nFunding Rate Data:")
            print(df)

            # Show some basic statistics
            print("\nFunding Rate Statistics:")
            print(f"Mean funding rate: {df['funding_rate'].mean():.6f}")
            print(f"Max funding rate: {df['funding_rate'].max():.6f}")
            print(f"Min funding rate: {df['funding_rate'].min():.6f}")

            # Save to CSV for reference
            csv_path = "tmp/btcusdt_funding_rates.csv"
            df.to_csv(csv_path)
            logger.info(f"Saved funding rate data to {csv_path}")

        # You can also fetch for multiple symbols if needed
        logger.info("Fetching funding rate data for ETHUSDT")
        df_eth = dsm.get_data(
            symbol="ETHUSDT",
            start_time=start_time,
            end_time=end_time,
            interval=Interval.HOUR_8,
        )

        if not df_eth.empty:
            logger.info(f"Retrieved {len(df_eth)} funding rate data points for ETHUSDT")
        else:
            logger.warning("No funding rate data found for ETHUSDT")


if __name__ == "__main__":
    fetch_funding_rates()
