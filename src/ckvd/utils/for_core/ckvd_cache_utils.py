#!/usr/bin/env python3
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
# Memory optimization: Polars LazyFrame for predicate pushdown (2026-02-04)
"""Cache utilities for CryptoKlineVisionData.

Provides provider-agnostic cache path generation and cache I/O operations.
Uses Polars LazyFrame for memory-efficient file reading with predicate pushdown.
"""

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pendulum
import polars as pl
import pyarrow as pa

from ckvd.core.providers.binance.vision_path_mapper import (
    FSSpecVisionHandler,
)
from ckvd.utils.loguru_setup import logger
from ckvd.utils.market_constraints import ChartType, DataProvider, Interval, MarketType


def _batch_scan_ipc(paths: list[str | Path]) -> pl.LazyFrame:
    """Batch-scan multiple Arrow IPC files in a single call.

    More efficient than N individual pl.scan_ipc() calls because Polars builds
    a single query plan for all files.

    Args:
        paths: List of Arrow IPC file paths.

    Returns:
        Single Polars LazyFrame scanning all files.
    """
    return pl.scan_ipc(paths)


def _scan_cache_file(cache_path: str | Path) -> pl.LazyFrame:
    """Detect cache file format via magic bytes and return a LazyFrame scanner.

    Arrow IPC files start with "ARROW1" (6 bytes), Parquet files start with "PAR1".
    Falls back to trying IPC then Parquet if magic bytes are unrecognized.

    Args:
        cache_path: Path to the cache file.

    Returns:
        Polars LazyFrame scanning the file.

    Raises:
        OSError: If the file cannot be read.
        pl.exceptions.ComputeError: If the file format is invalid.
    """
    with open(cache_path, "rb") as f:
        magic = f.read(6)

    if magic == b"ARROW1":
        return pl.scan_ipc(cache_path)
    if magic[:4] == b"PAR1":
        logger.debug(f"Cache file {cache_path} is Parquet format (legacy)")
        return pl.scan_parquet(cache_path)

    # Unknown format, try IPC first then Parquet
    try:
        lf = pl.scan_ipc(cache_path)
        _ = lf.collect_schema()  # Force schema check
        return lf
    except pl.exceptions.ComputeError:
        return pl.scan_parquet(cache_path)


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
        >>> from ckvd import DataProvider, MarketType, Interval, ChartType
        >>>
        >>> path = get_cache_path(
        ...     provider=DataProvider.BINANCE,
        ...     market_type=MarketType.FUTURES_USDT,
        ...     symbol="BTCUSDT",
        ...     interval=Interval.HOUR_1,
        ...     cache_date=date(2024, 1, 15),
        ...     cache_root=Path("~/.cache/ckvd"),
        ... )
        >>> # Returns: ~/.cache/ckvd/binance/futures_usdt/klines/daily/BTCUSDT/1h/2024-01-15.arrow
    """
    # Normalize values for path construction
    # Note: DataProvider uses int values, so use .name for string representation
    provider_dir = provider.name.lower()
    market_dir = market_type.name.lower()
    chart_dir = chart_type.name.lower()
    interval_str = interval.value

    # Construct path components
    return cache_root / provider_dir / market_dir / chart_dir / "daily" / symbol.upper() / interval_str / f"{cache_date.isoformat()}.arrow"


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

    Uses batch pl.scan_ipc() with a list of paths for efficient multi-file scanning
    instead of per-file scan calls. Returns a single filtered LazyFrame in a list.
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

    # Collect all valid cache file paths first
    valid_paths: list[Path] = []

    while current_date <= end_date:
        try:
            cache_path = fs_handler.get_local_path_for_data(
                symbol=symbol,
                interval=interval,
                date=current_date,
                market_type=market_type,
                chart_type=chart_type,
            )

            if fs_handler.exists(cache_path):
                logger.debug(f"Found cache file: {cache_path}")
                valid_paths.append(cache_path)
            else:
                logger.debug(f"No cache file found for {current_date.format('YYYY-MM-DD')}")
        except (OSError, ValueError, TypeError) as e:
            logger.error(f"Error processing cache for {current_date.format('YYYY-MM-DD')}: {e}")

        current_date = current_date.add(days=1)

    if not valid_paths:
        logger.debug("Returning 0 cache LazyFrames")
        return []

    # Batch scan: single pl.scan_ipc() call with all paths instead of N individual calls.
    # Cache always writes Arrow IPC format, so magic byte detection is unnecessary.
    try:
        lf = _batch_scan_ipc(valid_paths)

        # Use < end_time (exclusive) for consistency with OHLCV semantics:
        # open_time represents the START of a candle period, so a candle with
        # open_time == end_time would represent data AFTER the requested range.
        lf = lf.filter(
            (pl.col("open_time") >= start_time) & (pl.col("open_time") < end_time)
        ).with_columns(pl.lit("CACHE").alias("_data_source"))

        logger.debug(f"Returning 1 batch LazyFrame from {len(valid_paths)} cache file(s)")
        return [lf]
    except (OSError, pl.exceptions.ComputeError, ValueError, KeyError) as e:
        logger.error(f"Error batch-scanning {len(valid_paths)} cache files: {e}")
        return []


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

    # Collect all valid cache file paths first
    valid_paths: list[Path] = []

    while current_date <= end_date:
        try:
            cache_path = fs_handler.get_local_path_for_data(
                symbol=symbol,
                interval=interval,
                date=current_date,
                market_type=market_type,
                chart_type=chart_type,
            )

            if fs_handler.exists(cache_path):
                logger.info(f"Loading from cache: {cache_path}")
                valid_paths.append(cache_path)
            else:
                logger.info(f"No cache file found for {current_date.format('YYYY-MM-DD')}")
        except (OSError, ValueError, TypeError) as e:
            logger.error(f"Error processing cache for {current_date.format('YYYY-MM-DD')}: {e}")

        current_date = current_date.add(days=1)

    # Batch scan: single pl.scan_ipc() call with all paths, single collect.
    # Cache always writes Arrow IPC format, so magic byte detection is unnecessary.
    if valid_paths:
        try:
            lf = _batch_scan_ipc(valid_paths)

            # Apply time range filter and add source info as lazy operations
            combined_lf = lf.filter(
                (pl.col("open_time") >= start_time) & (pl.col("open_time") <= end_time)
            ).with_columns(pl.lit("CACHE").alias("_data_source"))

            combined_pl = combined_lf.collect(engine="streaming")
        except (OSError, pl.exceptions.ComputeError, ValueError, KeyError) as e:
            logger.error(f"Error batch-scanning {len(valid_paths)} cache files: {e}")
            combined_pl = pl.DataFrame()
        if len(combined_pl) > 0:
            result_df = combined_pl.to_pandas()
            # Ensure datetime columns are timezone-aware (Polars to_pandas may lose tz info)
            if "open_time" in result_df.columns and pd.api.types.is_datetime64_any_dtype(result_df["open_time"]):
                if result_df["open_time"].dt.tz is None:
                    result_df["open_time"] = result_df["open_time"].dt.tz_localize("UTC")
            logger.info(f"Loaded {len(result_df)} total records from {len(valid_paths)} cache file(s)")
        else:
            result_df = pd.DataFrame()
    else:
        result_df = pd.DataFrame()

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
        from ckvd.utils.for_core.ckvd_time_range_utils import identify_missing_segments

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

        # MEMORY OPTIMIZATION (Round 5): Group by day using a local Series instead of
        # adding a "date" column to the caller's DataFrame. Uses .dt.normalize() to stay
        # in datetime64 dtype instead of creating Python date objects via .dt.date.
        groupby_dates = pd.to_datetime(df["open_time"]).dt.normalize()
        grouped = df.groupby(groupby_dates)

        saved_files = 0

        for date_key, day_df in grouped:
            try:
                # Convert date_key (pd.Timestamp at midnight) to pendulum DateTime
                # This ensures the object has the tzinfo attribute needed by FSSpecVisionHandler
                year, month, day = date_key.year, date_key.month, date_key.day
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

                # Save to Arrow IPC format (not Parquet) for consistency with
                # cache_manager.py and vision_manager.py, and to enable memory
                # mapping and predicate pushdown via scan_ipc()
                # No need to drop a temp column — groupby key is a separate Series
                table = pa.Table.from_pandas(day_df)
                with pa.OSFile(str(cache_path), "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
                    writer.write_table(table)
                logger.info(f"Saved {len(day_df)} records to cache: {cache_path}")
                saved_files += 1

            except (OSError, PermissionError, pd.errors.ParserError) as e:
                logger.error(f"Error saving cache file for {date_key}: {e}")

        if saved_files > 0:
            logger.info(f"Saved data to {saved_files} cache files")
            return True
        logger.warning("No cache files were saved")
        return False

    except (OSError, PermissionError, ValueError) as e:
        logger.error(f"Error saving to cache: {e}")
        return False
