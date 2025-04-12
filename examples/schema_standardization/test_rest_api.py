#!/usr/bin/env python
from utils.logger_setup import logger
from rich import print
import argparse
from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from utils.market_constraints import MarketType, DataProvider, Interval
from core.sync.data_source_manager import DataSourceManager
from core.sync.rest_data_client import RestDataClient


def main():
    parser = argparse.ArgumentParser(description="Test REST API directly")
    parser.add_argument(
        "--market-type",
        "-m",
        type=str,
        choices=["SPOT", "UM", "CM"],
        default="SPOT",
        help="Market type (SPOT, UM, CM)",
    )
    parser.add_argument(
        "--symbol",
        "-s",
        type=str,
        default="BTCUSDT",
        help="Trading symbol (e.g. BTCUSDT)",
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=str,
        default="1m",
        help="Time interval (e.g. 1m, 5m, 1h)",
    )

    args = parser.parse_args()

    # Convert market type string to enum
    market_type = MarketType[args.market_type]

    # Map interval string to enum
    interval_map = {
        "1s": Interval.SECOND_1,
        "1m": Interval.MINUTE_1,
        "3m": Interval.MINUTE_3,
        "5m": Interval.MINUTE_5,
        "15m": Interval.MINUTE_15,
        "30m": Interval.MINUTE_30,
        "1h": Interval.HOUR_1,
        "2h": Interval.HOUR_2,
        "4h": Interval.HOUR_4,
        "6h": Interval.HOUR_6,
        "8h": Interval.HOUR_8,
        "12h": Interval.HOUR_12,
        "1d": Interval.DAY_1,
        "3d": Interval.DAY_3,
        "1w": Interval.WEEK_1,
        "1M": Interval.MONTH_1,
    }
    interval_enum = interval_map.get(args.interval, Interval.MINUTE_1)

    # Calculate time range (last 10 minutes)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=10)

    logger.info(
        f"Testing REST API for {args.symbol} ({args.market_type}) from {start_time} to {end_time}"
    )

    # Create REST client directly
    client = RestDataClient(market_type=market_type)

    try:
        # Fetch data
        logger.info("Fetching data from REST API...")

        # Call fetch directly with the correct signature
        df = client.fetch(
            symbol=args.symbol,
            interval=interval_enum,
            start_time=start_time,
            end_time=end_time,
        )

        if df is not None and not df.empty:
            logger.info(f"REST API successfully returned {len(df)} records!")
            logger.info("\nColumns:")
            for col in df.columns:
                logger.info(f"  - {col}: {df[col].dtype}")

            logger.info("\nSample data:")
            print(df.head(5))
        else:
            logger.error("REST API returned no data.")

    except Exception as e:
        logger.error(f"ERROR accessing REST API: {e}")

    # Try with DataSourceManager
    logger.info("\nNow trying with DataSourceManager...")

    try:
        dsm = DataSourceManager(market_type=market_type, provider=DataProvider.BINANCE)

        df_dsm = dsm.get_data(
            symbol=args.symbol,
            start_time=start_time,
            end_time=end_time,
            interval=interval_enum,
        )

        if df_dsm is not None and not df_dsm.empty:
            logger.info(
                f"DataSourceManager successfully returned {len(df_dsm)} records!"
            )
            logger.info("\nColumns:")
            for col in df_dsm.columns:
                logger.info(f"  - {col}: {df_dsm[col].dtype}")

            logger.info("\nSample data:")
            print(df_dsm.head(5))
        else:
            logger.error("DataSourceManager returned no data.")

    except Exception as e:
        logger.error(f"ERROR with DataSourceManager: {e}")


if __name__ == "__main__":
    main()
