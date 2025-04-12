#!/usr/bin/env python
from utils.logger_setup import logger
from rich import print
import argparse
from pathlib import Path
import sys
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from utils.market_constraints import MarketType, DataProvider
from utils.schema_standardizer import SchemaStandardizer


def parse_date(date_str: str) -> datetime:
    """Parse date string in format YYYY-MM-DD"""
    return datetime.strptime(date_str, "%Y-%m-%d")


def main():
    parser = argparse.ArgumentParser(
        description="Standardize cache data to match REST API schema"
    )
    parser.add_argument(
        "--market-type",
        "-m",
        type=str,
        choices=["SPOT", "UM", "CM"],
        required=True,
        help="Market type (SPOT, UM, CM)",
    )
    parser.add_argument(
        "--symbol", "-s", type=str, required=True, help="Trading symbol (e.g. BTCUSDT)"
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=str,
        required=True,
        help="Time interval (e.g. 1m, 5m, 1h)",
    )
    parser.add_argument(
        "--start-date", type=str, default=None, help="Start date in format YYYY-MM-DD"
    )
    parser.add_argument(
        "--end-date", type=str, default=None, help="End date in format YYYY-MM-DD"
    )
    parser.add_argument(
        "--days",
        "-d",
        type=int,
        default=7,
        help="Number of days to standardize (default: 7)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show changes without applying them"
    )

    args = parser.parse_args()

    # Convert market type string to enum
    market_type = MarketType[args.market_type]

    # Parse date range
    if args.start_date and args.end_date:
        start_time = parse_date(args.start_date)
        end_time = parse_date(args.end_date) + timedelta(days=1) - timedelta(seconds=1)
    else:
        end_time = datetime.now()
        start_time = end_time - timedelta(days=args.days)

    logger.info(
        f"Standardizing cache data for {args.symbol} ({args.market_type}) from {start_time} to {end_time}"
    )

    # Create standardizer
    standardizer = SchemaStandardizer(
        market_type=market_type, symbol=args.symbol, interval=args.interval
    )

    # Get reference schema
    reference_schema = standardizer.get_reference_schema()
    if not reference_schema:
        logger.error("Failed to retrieve reference schema from REST API")
        return

    logger.info(f"Reference schema from REST API: {reference_schema}")

    if args.dry_run:
        logger.info("Dry run mode - showing changes without applying them")
        # Get data from cache
        from core.sync.data_source_manager import DataSourceManager

        dsm = DataSourceManager(market_type=market_type, provider=DataProvider.BINANCE)
        df_cache = dsm.get_data_from_cache(
            market_type=market_type,
            symbol=args.symbol,
            interval=args.interval,
            start_time=start_time,
            end_time=end_time,
            use_rest_for_recent=False,
            use_vision_for_history=False,
        )

        if df_cache is None or len(df_cache) == 0:
            logger.warning("No cache data found for the specified parameters")
            return

        # Get cache schema
        cache_schema = {col: df_cache[col].dtype for col in df_cache.columns}
        logger.info(f"Cache schema: {cache_schema}")

        # Find differences
        missing_cols = [col for col in reference_schema if col not in cache_schema]
        extra_cols = [col for col in cache_schema if col not in reference_schema]
        different_type_cols = [
            col
            for col in reference_schema
            if col in cache_schema and reference_schema[col] != cache_schema[col]
        ]

        if not missing_cols and not extra_cols and not different_type_cols:
            logger.info("✅ Schemas are already consistent - no changes needed")
            return

        if missing_cols:
            logger.warning(f"❌ Missing columns in cache: {missing_cols}")
        if extra_cols:
            logger.warning(f"❌ Extra columns in cache: {extra_cols}")
        if different_type_cols:
            logger.warning(f"❌ Columns with different types:")
            for col in different_type_cols:
                logger.warning(
                    f"  - {col}: cache={cache_schema[col]}, reference={reference_schema[col]}"
                )

        # Show sample of standardized data
        standardized_df = standardizer.standardize_dataframe(df_cache.head(5))
        logger.info("Sample of standardized data (first 5 rows):")
        print(standardized_df)
    else:
        # Apply standardization to cache
        standardizer.standardize_cache_data(start_time, end_time)
        logger.info("✅ Cache data standardization completed")


if __name__ == "__main__":
    main()
