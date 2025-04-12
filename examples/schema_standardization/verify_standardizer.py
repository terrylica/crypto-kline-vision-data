#!/usr/bin/env python
from utils.logger_setup import logger
import argparse
from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from utils.market_constraints import MarketType, DataProvider, Interval
from utils.schema_standardizer import SchemaStandardizer
from core.sync.data_source_manager import DataSourceManager
from core.sync.rest_data_client import RestDataClient


def main():
    parser = argparse.ArgumentParser(
        description="Verify SchemaStandardizer functionality"
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
        "--days",
        "-d",
        type=int,
        default=3,
        help="Number of days to look back (default: 3)",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default="./schema_test",
        help="Directory to save test output files",
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable DEBUG level logging"
    )

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        import logging

        logger.setLevel(logging.DEBUG)
        logger.debug("DEBUG logging enabled")

    # Convert market type string to enum
    if args.market_type == "UM":
        market_type = MarketType.FUTURES_USDT
    elif args.market_type == "CM":
        market_type = MarketType.FUTURES_COIN
    else:
        market_type = MarketType[args.market_type]
    logger.info(f"Using market type: {market_type.name}")

    # Setup output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    logger.info(f"Output directory: {output_dir}")

    # Calculate time ranges - use longer ranges to increase chances of finding data
    now = datetime.now(timezone.utc)
    logger.info(f"Current time: {now}")

    # REST API - use recent data (last 3 hours)
    rest_end = now - timedelta(
        minutes=5
    )  # 5 minutes in the past to ensure data is available
    rest_start = rest_end - timedelta(hours=3)
    logger.info(f"REST API time range: {rest_start} to {rest_end}")

    # VISION API - use data from 2 days ago (should be available in VISION)
    vision_end = now - timedelta(days=2, hours=2)  # 2 days + 2 hours in the past
    vision_start = vision_end - timedelta(hours=3)
    logger.info(f"VISION API time range: {vision_start} to {vision_end}")

    # CACHE - use older data (7+ days ago)
    cache_end = now - timedelta(days=args.days)
    cache_start = cache_end - timedelta(hours=3)
    logger.info(f"Cache time range: {cache_start} to {cache_end}")

    # Convert interval string to Interval enum
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
    logger.info(f"Using interval: {args.interval} (enum: {interval_enum})")

    # Try direct access to REST API first to ensure we have the reference schema
    logger.info("Trying to get reference schema directly from REST API")
    try:
        rest_client = RestDataClient(
            market_type=market_type,
            retry_count=3,
            symbol=args.symbol,
            interval=interval_enum,
        )

        df_direct = rest_client.fetch(
            symbol=args.symbol,
            interval=interval_enum,
            start_time=rest_start,
            end_time=rest_end,
        )

        if df_direct is not None and not df_direct.empty:
            logger.info(
                f"Successfully fetched reference schema directly: {len(df_direct)} records"
            )
            reference_schema = {col: df_direct[col].dtype for col in df_direct.columns}
            logger.info(f"Reference schema columns: {list(reference_schema.keys())}")
        else:
            logger.warning("Failed to fetch reference schema directly")
    except Exception as e:
        logger.error(f"Error fetching reference schema directly: {e}")

    # Create standardizer
    logger.info("Creating SchemaStandardizer")
    standardizer = SchemaStandardizer(
        market_type=market_type, symbol=args.symbol, interval=args.interval
    )

    # Get reference schema
    logger.info("Getting reference schema from SchemaStandardizer")
    reference_schema = standardizer.get_reference_schema(rest_start, rest_end)
    if not reference_schema:
        logger.error("Failed to retrieve reference schema from REST API")

        # Try with a different time range as a last resort
        logger.info("Trying alternative time range for reference schema")
        alt_end = now - timedelta(hours=1)
        alt_start = alt_end - timedelta(hours=6)
        reference_schema = standardizer.get_reference_schema(alt_start, alt_end)

        if not reference_schema:
            logger.error(
                "Failed to retrieve reference schema with alternative time range"
            )
            if "reference_schema" in locals() and reference_schema:
                logger.info("Using directly fetched reference schema instead")
            else:
                logger.error(
                    "No reference schema available. Verification cannot proceed."
                )
                return

    logger.info(f"Reference schema from REST API: {reference_schema}")

    # Create DataSourceManager
    logger.info("Creating DataSourceManager")
    dsm = DataSourceManager(market_type=market_type, provider=DataProvider.BINANCE)

    # Create results dictionary
    results = {
        "rest": {"raw": None, "standardized": None, "diff": []},
        "vision": {"raw": None, "standardized": None, "diff": []},
        "cache": {"raw": None, "standardized": None, "diff": []},
    }

    # Get REST API data
    logger.info(f"Getting REST API data from {rest_start} to {rest_end}...")
    try:
        df_rest = dsm.get_data(
            symbol=args.symbol,
            start_time=rest_start,
            end_time=rest_end,
            interval=interval_enum,
        )

        if df_rest is not None and not df_rest.empty:
            logger.info(f"Retrieved {len(df_rest)} records from REST API")
            results["rest"]["raw"] = df_rest
            results["rest"]["standardized"] = standardizer.standardize_dataframe(
                df_rest
            )

            # Get the differences
            raw_schema = {col: df_rest[col].dtype for col in df_rest.columns}
            std_schema = {
                col: results["rest"]["standardized"][col].dtype
                for col in results["rest"]["standardized"].columns
            }

            results["rest"]["diff"].extend(
                [
                    f"REST API: Raw columns: {list(raw_schema.keys())}",
                    f"REST API: Standardized columns: {list(std_schema.keys())}",
                ]
            )
        else:
            logger.warning("Could not retrieve data from REST API")
    except Exception as e:
        logger.error(f"Error getting REST API data: {e}")

    # Get VISION API data
    logger.info(f"Getting VISION API data from {vision_start} to {vision_end}...")
    try:
        # Try to force use of Vision API by using a time range that should be available in Vision
        # This is a bit of a hack, but we're trying to test Vision API specifically
        df_vision = None

        # Check if Vision API data is available
        if dsm._should_use_vision_api(vision_start, vision_end):
            # Try to get historical data from Vision API
            df_vision = dsm.get_data(
                symbol=args.symbol,
                start_time=vision_start,
                end_time=vision_end,
                interval=interval_enum,
            )

        if df_vision is not None and not df_vision.empty:
            logger.info(f"Retrieved {len(df_vision)} records from VISION API")
            results["vision"]["raw"] = df_vision
            results["vision"]["standardized"] = standardizer.standardize_dataframe(
                df_vision
            )

            # Get the differences
            raw_schema = {col: df_vision[col].dtype for col in df_vision.columns}
            std_schema = {
                col: results["vision"]["standardized"][col].dtype
                for col in results["vision"]["standardized"].columns
            }

            results["vision"]["diff"].extend(
                [
                    f"VISION API: Raw columns: {list(raw_schema.keys())}",
                    f"VISION API: Standardized columns: {list(std_schema.keys())}",
                ]
            )
        else:
            logger.warning(
                "Could not retrieve data from VISION API (expected in test environment)"
            )
    except Exception as e:
        logger.error(f"Error getting VISION API data: {e}")

    # Get data from cache
    logger.info(f"Getting cache data from {cache_start} to {cache_end}...")
    try:
        # Attempt to get data from cache using DSM's get_data method with cache only
        if hasattr(dsm, "get_data"):
            logger.debug("Using DSM.get_data to fetch cache data")
            # First try using DSM's get_data method
            cached_df = dsm.get_data(
                symbol=args.symbol,
                start_time=cache_start,
                end_time=cache_end,
                interval=interval_enum,
            )

            if cached_df is not None and not cached_df.empty:
                logger.info(
                    f"Retrieved {len(cached_df)} records from cache via DSM.get_data"
                )
                results["cache"]["raw"] = cached_df
                results["cache"]["standardized"] = standardizer.standardize_dataframe(
                    cached_df
                )

                # Get the differences
                raw_schema = {col: cached_df[col].dtype for col in cached_df.columns}
                std_schema = {
                    col: results["cache"]["standardized"][col].dtype
                    for col in results["cache"]["standardized"].columns
                }

                results["cache"]["diff"].extend(
                    [
                        f"Cache: Raw columns: {list(raw_schema.keys())}",
                        f"Cache: Standardized columns: {list(std_schema.keys())}",
                    ]
                )
            else:
                logger.warning("No data found in cache via DSM.get_data")

        # Try alternative approach using cache_manager if available
        elif dsm.cache_manager is not None:
            logger.debug("Attempting to use cache_manager directly")
            cache_manager = dsm.cache_manager

            try:
                # Try to use get_cache_file_path if available
                if hasattr(cache_manager, "get_cache_file_path"):
                    logger.debug("Using cache_manager.get_cache_file_path")
                    cache_file_path = cache_manager.get_cache_file_path(
                        market_type=market_type,
                        symbol=args.symbol,
                        interval=str(interval_enum),
                    )

                    if cache_file_path.exists():
                        logger.debug(f"Cache file found: {cache_file_path}")

                        if hasattr(cache_manager, "get_from_cache"):
                            logger.debug("Using cache_manager.get_from_cache")
                            df_cache = cache_manager.get_from_cache(
                                market_type=market_type,
                                symbol=args.symbol,
                                interval=str(interval_enum),
                                start_time=cache_start,
                                end_time=cache_end,
                            )
                        else:
                            logger.warning(
                                "cache_manager.get_from_cache method not available"
                            )
                            df_cache = None

                        if df_cache is not None and not df_cache.empty:
                            logger.info(
                                f"Retrieved {len(df_cache)} records from cache using cache_manager"
                            )
                            results["cache"]["raw"] = df_cache
                            results["cache"]["standardized"] = (
                                standardizer.standardize_dataframe(df_cache)
                            )

                            # Get the differences
                            raw_schema = {
                                col: df_cache[col].dtype for col in df_cache.columns
                            }
                            std_schema = {
                                col: results["cache"]["standardized"][col].dtype
                                for col in results["cache"]["standardized"].columns
                            }

                            results["cache"]["diff"].extend(
                                [
                                    f"Cache: Raw columns: {list(raw_schema.keys())}",
                                    f"Cache: Standardized columns: {list(std_schema.keys())}",
                                ]
                            )
                        else:
                            logger.warning(
                                "Cache file exists but no data could be retrieved for the specified time range"
                            )
                    else:
                        logger.warning(f"No cache file found at {cache_file_path}")
                else:
                    logger.warning(
                        "cache_manager.get_cache_file_path method not available"
                    )
            except Exception as e:
                logger.warning(f"Error accessing cache via cache_manager: {e}")
        else:
            logger.warning("Cache manager is not available")
    except Exception as e:
        logger.error(f"Error getting cache data: {e}")

    # Analyze the results
    logger.info("\nAnalyzing results...")
    success = True

    # Check if any data was retrieved
    sources_with_data = [
        source
        for source, data in results.items()
        if data["raw"] is not None and not data["raw"].empty
    ]

    if not sources_with_data:
        logger.error("Could not retrieve data from any source. Verification failed.")
        return

    logger.info(f"Successfully retrieved data from: {sources_with_data}")

    # Save raw and standardized data for inspection
    for source in sources_with_data:
        raw_file = output_dir / f"{args.market_type}_{args.symbol}_{source}_raw.csv"
        std_file = (
            output_dir / f"{args.market_type}_{args.symbol}_{source}_standardized.csv"
        )

        results[source]["raw"].to_csv(raw_file, index=False)
        results[source]["standardized"].to_csv(std_file, index=False)

        logger.info(f"Saved {source} data to {raw_file} and {std_file}")

    # Verify all standardized DataFrames have the same schema
    std_schemas = {}
    for source in sources_with_data:
        df_std = results[source]["standardized"]
        std_schemas[source] = {
            "columns": list(df_std.columns),
            "dtypes": {col: df_std[col].dtype for col in df_std.columns},
        }

    # Compare standardized schemas
    reference_source = sources_with_data[0]
    reference_columns = std_schemas[reference_source]["columns"]

    for source in sources_with_data[1:]:
        source_columns = std_schemas[source]["columns"]

        if source_columns != reference_columns:
            logger.error(
                f"❌ Standardized column order mismatch between {reference_source} and {source}"
            )
            logger.error(f"  {reference_source}: {reference_columns}")
            logger.error(f"  {source}: {source_columns}")
            success = False

        # Compare data types
        for col in reference_columns:
            ref_dtype = std_schemas[reference_source]["dtypes"][col]
            src_dtype = std_schemas[source]["dtypes"][col]

            if str(ref_dtype) != str(src_dtype):
                logger.error(
                    f"❌ Data type mismatch for column '{col}' between {reference_source} and {source}"
                )
                logger.error(f"  {reference_source}: {ref_dtype}")
                logger.error(f"  {source}: {src_dtype}")
                success = False

    if success:
        logger.info("✅ SUCCESS: All standardized DataFrames have consistent schema")
    else:
        logger.error("❌ FAILURE: Standardized DataFrames have inconsistent schema")

    # Print summary of changes made by standardizer
    logger.info("\nSchema standardization summary:")
    for source in sources_with_data:
        logger.info(f"\nChanges for {source} data:")
        for diff in results[source]["diff"]:
            logger.info(f"  {diff}")

    logger.info("\nVerification complete!")


if __name__ == "__main__":
    main()
