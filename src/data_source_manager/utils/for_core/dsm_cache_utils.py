#!/usr/bin/env python3
"""Cache utilities for DataSourceManager."""

from datetime import datetime
from pathlib import Path

import pandas as pd
import pendulum

from data_source_manager.core.providers.binance.vision_path_mapper import (
    FSSpecVisionHandler,
)
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import ChartType, DataProvider, Interval, MarketType


def get_from_cache(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
    cache_dir: Path,
    market_type: MarketType,
    chart_type: ChartType = ChartType.KLINES,
    provider: DataProvider = DataProvider.BINANCE,
) -> tuple[pd.DataFrame, list[tuple[datetime, datetime]]]:
    """Get data from cache for the specified time range.

    Args:
        symbol: Trading symbol
        start_time: Start time
        end_time: End time
        interval: Time interval
        cache_dir: Cache directory
        market_type: Market type (spot, um, cm)
        chart_type: Chart type (klines, funding_rate)
        provider: Data provider - currently supports Binance only,
                 retained for future multi-provider support

    Returns:
        Tuple of (DataFrame with data, List of missing time ranges)
    """
    # Initialize FSSpecVisionHandler for path mapping
    fs_handler = FSSpecVisionHandler(base_cache_dir=cache_dir)

    # TODO: When adding support for multiple providers, update the cache
    # path structure to include the provider information.
    # Currently, only Binance is supported.
    if provider != DataProvider.BINANCE:
        logger.warning(f"Provider {provider.name} cache retrieval not yet implemented, falling back to Binance format")

    # Calculate the days we need to query
    current_date = pendulum.instance(start_time).start_of("day")
    end_date = pendulum.instance(end_time).start_of("day")

    # Prepare result DataFrame
    result_df = pd.DataFrame()

    # Track which days we were able to load from cache
    loaded_days = []

    # Iterate through days
    while current_date <= end_date:
        try:
            # Get cache path for this day
            cache_path = fs_handler.get_local_path_for_data(
                symbol=symbol,
                interval=interval,
                date=current_date,
                market_type=market_type,
                chart_type=chart_type,
            )

            # Check if cache file exists
            if fs_handler.exists(cache_path):
                logger.info(f"Loading from cache: {cache_path}")

                # In real implementation, load from Arrow file
                try:
                    daily_df = pd.read_parquet(cache_path)
                    if not daily_df.empty:
                        logger.info(f"Loaded {len(daily_df)} records from cache for {current_date.format('YYYY-MM-DD')}")
                        loaded_days.append(current_date.date())

                        # Filter to the requested time range before merging
                        daily_df = daily_df[(daily_df["open_time"] >= start_time) & (daily_df["open_time"] <= end_time)]

                        # Add source information
                        daily_df["_data_source"] = "CACHE"

                        # Append to result
                        result_df = pd.concat([result_df, daily_df])
                    else:
                        logger.warning(f"Cache file exists but is empty: {cache_path}")
                except Exception as e:
                    logger.error(f"Error loading cache file {cache_path}: {e}")
            else:
                logger.info(f"No cache file found for {current_date.format('YYYY-MM-DD')}")
        except Exception as e:
            logger.error(f"Error processing cache for {current_date.format('YYYY-MM-DD')}: {e}")

        # Move to next day
        current_date = current_date.add(days=1)

    # Calculate missing time ranges using proper gap detection
    missing_ranges = []
    if result_df.empty:
        # If nothing was found in cache, the entire range is missing
        missing_ranges.append((start_time, end_time))
    else:
        # Sort by open_time to ensure proper range detection
        result_df = result_df.sort_values("open_time")
        
        # Use the proper gap detection function to identify missing segments
        # This will detect both missing days and intraday gaps
        from data_source_manager.utils.for_core.dsm_time_range_utils import identify_missing_segments
        
        logger.debug(f"[CACHE] Using gap detection to find missing ranges between {start_time} and {end_time}")
        missing_ranges = identify_missing_segments(result_df, start_time, end_time, interval)
        
        if missing_ranges:
            logger.debug(f"[CACHE] Gap detection found {len(missing_ranges)} missing segments:")
            for i, (miss_start, miss_end) in enumerate(missing_ranges):
                logger.debug(f"[CACHE]   Missing segment {i+1}: {miss_start} to {miss_end}")
        else:
            logger.debug("[CACHE] Gap detection found no missing segments - cache provides complete coverage")

    # Log summary
    if result_df.empty:
        logger.info("No data found in cache for the requested time range")
    else:
        logger.info(f"Loaded {len(result_df)} total records from cache")

    if missing_ranges:
        logger.info(f"Missing {len(missing_ranges)} time ranges in cache")

    return result_df, missing_ranges


def save_to_cache(
    df: pd.DataFrame,
    symbol: str,
    interval: Interval,
    market_type: MarketType,
    cache_dir: Path,
    chart_type: ChartType = ChartType.KLINES,
    provider: DataProvider = DataProvider.BINANCE,
) -> bool:
    """Save DataFrame to cache.

    Args:
        df: DataFrame to save
        symbol: Trading symbol
        interval: Time interval
        market_type: Market type
        cache_dir: Cache directory
        chart_type: Chart type
        provider: Data provider - currently supports Binance only,
                 retained for future multi-provider support

    Returns:
        True if successful, False otherwise
    """
    if df.empty:
        logger.warning("Cannot save empty DataFrame to cache")
        return False

    try:
        # Initialize FSSpecVisionHandler for path mapping
        fs_handler = FSSpecVisionHandler(base_cache_dir=cache_dir)

        # TODO: When adding support for multiple providers, update the cache
        # path structure to include the provider information.
        # Currently, only Binance is supported.
        if provider != DataProvider.BINANCE:
            logger.warning(f"Provider {provider.name} cache save not yet implemented, using Binance format")

        # Group by day to save daily files
        df["date"] = pd.to_datetime(df["open_time"]).dt.date
        grouped = df.groupby(df["date"])

        saved_files = 0

        for date, day_df in grouped:
            try:
                # Convert date to pendulum DateTime object with UTC timezone
                # This ensures the object has the tzinfo attribute needed by FSSpecVisionHandler
                year, month, day = date.year, date.month, date.day
                pdate = pendulum.datetime(year, month, day, 0, 0, 0, tz="UTC")

                # Get cache path for this day
                cache_path = fs_handler.get_local_path_for_data(
                    symbol=symbol,
                    interval=interval,
                    date=pdate,
                    market_type=market_type,
                    chart_type=chart_type,
                )

                # Ensure directory exists
                cache_path.parent.mkdir(parents=True, exist_ok=True)

                # Remove the temporary date column before saving
                save_df = day_df.drop(columns=["date"])

                # Save to parquet format
                save_df.to_parquet(cache_path)
                logger.info(f"Saved {len(save_df)} records to cache: {cache_path}")
                saved_files += 1

            except Exception as e:
                logger.error(f"Error saving cache file for {date}: {e}")

        if saved_files > 0:
            logger.info(f"Saved data to {saved_files} cache files")
            return True
        logger.warning("No cache files were saved")
        return False

    except Exception as e:
        logger.error(f"Error saving to cache: {e}")
        return False
