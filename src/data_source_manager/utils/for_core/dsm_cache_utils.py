#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
# Memory optimization: Polars LazyFrame for predicate pushdown (2026-02-04)
"""Cache utilities for DataSourceManager.

Provides provider-agnostic cache path generation and cache I/O operations.
Uses Polars LazyFrame for memory-efficient file reading with predicate pushdown.
"""

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pendulum
import polars as pl

from data_source_manager.core.providers.binance.vision_path_mapper import (
    FSSpecVisionHandler,
)
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import ChartType, DataProvider, Interval, MarketType


# =============================================================================
# Provider-Agnostic Cache Path Generation
# =============================================================================


def get_cache_path(
    provider: DataProvider,
    market_type: MarketType,
    symbol: str,
    interval: Interval,
    cache_date: date,
    cache_root: Path,
    chart_type: ChartType = ChartType.KLINES,
) -> Path:
    """Generate provider-agnostic cache path.

    This function creates a consistent cache path structure that works
    for any data provider (Binance, OKX, TradeStation, etc.).

    Args:
        provider: Data provider (e.g., BINANCE, OKX)
        market_type: Market type (e.g., SPOT, FUTURES_USDT)
        symbol: Trading symbol (e.g., "BTCUSDT")
        interval: Time interval (e.g., Interval.HOUR_1)
        cache_date: Date for the cache file
        cache_root: Root cache directory
        chart_type: Type of chart data (e.g., KLINES, FUNDING_RATE)

    Returns:
        Path to the cache file

    Example:
        >>> from pathlib import Path
        >>> from datetime import date
        >>> from data_source_manager import DataProvider, MarketType, Interval, ChartType
        >>>
        >>> path = get_cache_path(
        ...     provider=DataProvider.BINANCE,
        ...     market_type=MarketType.FUTURES_USDT,
        ...     symbol="BTCUSDT",
        ...     interval=Interval.HOUR_1,
        ...     cache_date=date(2024, 1, 15),
        ...     cache_root=Path("~/.cache/dsm"),
        ... )
        >>> # Returns: ~/.cache/dsm/binance/futures_usdt/klines/daily/BTCUSDT/1h/2024-01-15.arrow
    """
    # Normalize values for path construction
    # Note: DataProvider uses int values, so use .name for string representation
    provider_dir = provider.name.lower()
    market_dir = market_type.name.lower()
    chart_dir = chart_type.name.lower()
    interval_str = interval.value

    # Construct path components
    return (
        cache_root
        / provider_dir
        / market_dir
        / chart_dir
        / "daily"
        / symbol.upper()
        / interval_str
        / f"{cache_date.isoformat()}.arrow"
    )


def get_cache_dir_for_symbol(
    provider: DataProvider,
    market_type: MarketType,
    symbol: str,
    interval: Interval,
    cache_root: Path,
    chart_type: ChartType = ChartType.KLINES,
) -> Path:
    """Get the cache directory for a symbol/interval combination.

    Args:
        provider: Data provider
        market_type: Market type
        symbol: Trading symbol
        interval: Time interval
        cache_root: Root cache directory
        chart_type: Type of chart data

    Returns:
        Path to the cache directory (without date)
    """
    provider_dir = provider.name.lower()
    market_dir = market_type.name.lower()
    chart_dir = chart_type.name.lower()
    interval_str = interval.value

    return cache_root / provider_dir / market_dir / chart_dir / "daily" / symbol.upper() / interval_str


# =============================================================================
# Cache I/O Operations
# =============================================================================


def get_cache_lazyframes(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
    cache_dir: Path,
    market_type: MarketType,
    chart_type: ChartType = ChartType.KLINES,
    provider: DataProvider = DataProvider.BINANCE,
) -> list[pl.LazyFrame]:
    """Get LazyFrames from cache for use with PolarsDataPipeline.

    This function returns a list of filtered LazyFrames, one per cache file found.
    The caller (PolarsDataPipeline) is responsible for concatenation and merge.

    This enables predicate pushdown and lazy evaluation through the entire pipeline.

    Args:
        symbol: Trading symbol
        start_time: Start time
        end_time: End time
        interval: Time interval
        cache_dir: Cache directory
        market_type: Market type (spot, um, cm)
        chart_type: Chart type (klines, funding_rate)
        provider: Data provider - currently supports Binance only

    Returns:
        List of LazyFrames with time-filtered data and _data_source="CACHE" column
    """
    # Initialize FSSpecVisionHandler for path mapping
    fs_handler = FSSpecVisionHandler(base_cache_dir=cache_dir)

    if provider != DataProvider.BINANCE:
        logger.warning(f"Provider {provider.name} cache retrieval not yet implemented, falling back to Binance format")

    # Calculate the days we need to query
    current_date = pendulum.instance(start_time).start_of("day")
    end_date = pendulum.instance(end_time).start_of("day")

    lazy_frames: list[pl.LazyFrame] = []

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
                logger.debug(f"Found cache file: {cache_path}")

                try:
                    # Scan Parquet lazily with time filter (predicate pushdown)
                    # Note: Cache files use .arrow extension but are written as Parquet
                    # format by save_to_cache() for better compression. scan_parquet()
                    # supports predicate pushdown to row groups for efficient reads.
                    lf = pl.scan_parquet(cache_path).filter(
                        (pl.col("open_time") >= start_time) & (pl.col("open_time") <= end_time)
                    ).with_columns(pl.lit("CACHE").alias("_data_source"))

                    lazy_frames.append(lf)
                    logger.debug(f"Added LazyFrame for {current_date.format('YYYY-MM-DD')}")
                except (OSError, pl.exceptions.ComputeError, ValueError, KeyError) as e:
                    logger.error(f"Error scanning cache file {cache_path}: {e}")
            else:
                logger.debug(f"No cache file found for {current_date.format('YYYY-MM-DD')}")
        except (OSError, ValueError, TypeError) as e:
            logger.error(f"Error processing cache for {current_date.format('YYYY-MM-DD')}: {e}")

        # Move to next day
        current_date = current_date.add(days=1)

    logger.debug(f"Returning {len(lazy_frames)} cache LazyFrames")
    return lazy_frames


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

    # MEMORY OPTIMIZATION: Collect DataFrames in list, single concat at end
    # This avoids O(n²) memory allocation from repeated pd.concat() calls
    # See: /tmp/memory_audit_findings.md - Priority 1 fix
    daily_dfs: list[pd.DataFrame] = []

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

                # MEMORY OPTIMIZATION: Use Polars LazyFrame with predicate pushdown
                # This filters at read time instead of loading entire file then filtering.
                # Cache files are Arrow IPC format (.arrow), NOT Parquet.
                # scan_ipc enables lazy evaluation with predicate pushdown.
                # Source: https://docs.pola.rs/api/python/stable/reference/api/polars.scan_ipc.html
                try:
                    # Scan Arrow IPC lazily, apply filter, then collect only matching rows
                    lf = pl.scan_ipc(cache_path)

                    # Apply time range filter with predicate pushdown
                    # Note: Polars datetime comparison requires proper type handling
                    filtered_lf = lf.filter(
                        (pl.col("open_time") >= start_time) & (pl.col("open_time") <= end_time)
                    )

                    # Collect filtered data using streaming engine for better memory efficiency
                    # Source: https://pola.rs/posts/polars-in-aggregate-dec25/ (3-7x faster, less memory)
                    daily_pl = filtered_lf.collect(engine="streaming")

                    if len(daily_pl) > 0:
                        logger.info(f"Loaded {len(daily_pl)} records from cache for {current_date.format('YYYY-MM-DD')}")
                        loaded_days.append(current_date.date())

                        # Convert to pandas and add source information
                        daily_df = daily_pl.to_pandas()
                        daily_df["_data_source"] = "CACHE"

                        # Collect for batch concat (memory efficient)
                        daily_dfs.append(daily_df)
                    else:
                        logger.warning(f"Cache file exists but no data in requested range: {cache_path}")
                except (OSError, pl.exceptions.ComputeError, ValueError, KeyError) as e:
                    logger.error(f"Error loading cache file {cache_path}: {e}")
            else:
                logger.info(f"No cache file found for {current_date.format('YYYY-MM-DD')}")
        except (OSError, ValueError, TypeError) as e:
            logger.error(f"Error processing cache for {current_date.format('YYYY-MM-DD')}: {e}")

        # Move to next day
        current_date = current_date.add(days=1)

    # Single concat at end - O(n) instead of O(n²)
    result_df = pd.concat(daily_dfs, ignore_index=True) if daily_dfs else pd.DataFrame()

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
                logger.debug(f"[CACHE]   Missing segment {i + 1}: {miss_start} to {miss_end}")
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

            except (OSError, PermissionError, pd.errors.ParserError) as e:
                logger.error(f"Error saving cache file for {date}: {e}")

        if saved_files > 0:
            logger.info(f"Saved data to {saved_files} cache files")
            return True
        logger.warning("No cache files were saved")
        return False

    except (OSError, PermissionError, ValueError) as e:
        logger.error(f"Error saving to cache: {e}")
        return False
