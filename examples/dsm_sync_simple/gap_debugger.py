#!/usr/bin/env python
"""Gap Debugger Tool for Binance Data Services.

This script is designed to identify, analyze, and debug gaps in data retrieval
from both VisionDataClient and RestDataClient, with particular focus on
timestamp validation issues and day boundary transitions.
"""

from datetime import datetime, timezone, timedelta
import os
import pandas as pd
import argparse
from pathlib import Path
import time
from typing import Dict, List, Tuple, Any

# Try to import visualization libraries but make them optional
try:
    pass

    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False
    print(
        "Note: Visualization libraries (matplotlib, seaborn) not available. Continuing without visualization capabilities."
    )

# Import from the project
from utils.logger_setup import logger
from utils.market_constraints import Interval, MarketType
from utils.time_utils import (
    align_time_boundaries,
    get_interval_seconds,
)
from core.sync.vision_data_client import VisionDataClient
from core.sync.rest_data_client import RestDataClient
from core.sync.data_source_manager import DataSourceManager, DataSource

# Configure logging to file for detailed analysis
import logging
from rich.logging import RichHandler

# Ensure logs directory exists
log_dir = Path("logs/gap_debugger")
os_makedirs = getattr(os, "makedirs", None)  # Use os.makedirs if available
if os_makedirs:
    os_makedirs(log_dir, exist_ok=True)
else:
    log_dir.mkdir(parents=True, exist_ok=True)

# Setup file handler for detailed logging
log_file = log_dir / f"gap_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Add rich handler for console output
console_handler = RichHandler(rich_tracebacks=True)
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)
logger.setLevel(logging.DEBUG)


def analyze_timestamp_continuity(
    df: pd.DataFrame, interval: Interval
) -> Tuple[bool, List[Dict[str, Any]]]:
    """Analyze a dataframe for timestamp continuity issues.

    Args:
        df: DataFrame with open_time column
        interval: Expected interval between timestamps

    Returns:
        Tuple of (has_gaps, list_of_gaps)
    """
    if df.empty:
        return False, []

    # Ensure dataframe is sorted by open_time
    df = df.sort_values("open_time").reset_index(drop=True)

    # Calculate expected interval in seconds
    expected_interval_sec = get_interval_seconds(interval)

    # Calculate actual time differences
    df["time_diff"] = df["open_time"].diff().dt.total_seconds()

    # First row will have NaN diff, set to expected interval
    df.loc[0, "time_diff"] = expected_interval_sec

    # Find gaps where time difference is significantly more than expected
    # Allow a small tolerance (10%) for minor deviations
    gap_tolerance = expected_interval_sec * 1.1
    gaps = df[df["time_diff"] > gap_tolerance].copy()

    if gaps.empty:
        return False, []

    # Prepare gap information for analysis
    gap_info = []
    for _, row in gaps.iterrows():
        previous_time = row["open_time"] - timedelta(seconds=row["time_diff"])
        gap_info.append(
            {
                "previous_time": previous_time,
                "current_time": row["open_time"],
                "gap_seconds": row["time_diff"],
                "expected_interval": expected_interval_sec,
                "missing_points": int(row["time_diff"] / expected_interval_sec) - 1,
                "day_boundary": previous_time.day != row["open_time"].day,
                "month_boundary": previous_time.month != row["open_time"].month,
                "year_boundary": previous_time.year != row["open_time"].year,
            }
        )

    return True, gap_info


def debug_vision_client(
    symbol: str,
    interval: Interval,
    start_time: datetime,
    end_time: datetime,
    market_type: MarketType = MarketType.SPOT,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Debug the VisionDataClient's data retrieval process.

    Args:
        symbol: Trading pair symbol
        interval: Kline interval
        start_time: Start time
        end_time: End time
        market_type: Market type

    Returns:
        Tuple of (dataframe, gap_info)
    """
    logger.info(
        f"Debugging VisionDataClient for {symbol} {interval.value} from {start_time} to {end_time}"
    )

    # Create a Vision client with debug flags enabled
    vision_client = VisionDataClient(
        symbol=symbol, interval=interval.value, market_type=market_type
    )

    # Fetch data - VisionDataClient.fetch only takes start_time and end_time
    start_fetch = time.time()
    df = vision_client.fetch(start_time, end_time)
    fetch_time = time.time() - start_fetch

    # Log data retrieval statistics
    rows_count = len(df) if df is not None else 0
    logger.info(
        f"VisionDataClient retrieved {rows_count} records in {fetch_time:.2f} seconds"
    )

    # Check for timestamp continuity issues
    has_gaps, gap_info = analyze_timestamp_continuity(df, interval)

    if has_gaps:
        logger.warning(f"Found {len(gap_info)} gaps in VisionDataClient data")
        for i, gap in enumerate(gap_info):
            logger.warning(
                f"Gap {i+1}: {gap['previous_time']} → {gap['current_time']} "
                f"({gap['gap_seconds']:.1f}s, missing {gap['missing_points']} points)"
            )

    return df, gap_info


def debug_rest_client(
    symbol: str,
    interval: Interval,
    start_time: datetime,
    end_time: datetime,
    market_type: MarketType = MarketType.SPOT,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Debug the RestDataClient's data retrieval process.

    Args:
        symbol: Trading pair symbol
        interval: Kline interval
        start_time: Start time
        end_time: End time
        market_type: Market type

    Returns:
        Tuple of (dataframe, gap_info)
    """
    logger.info(
        f"Debugging RestDataClient for {symbol} {interval.value} from {start_time} to {end_time}"
    )

    # Create a REST client
    rest_client = RestDataClient(
        market_type=market_type, symbol=symbol, interval=interval
    )

    # Fetch data
    start_fetch = time.time()
    df = rest_client.fetch(symbol, interval, start_time, end_time)
    fetch_time = time.time() - start_fetch

    # Log data retrieval statistics
    rows_count = len(df) if df is not None else 0
    logger.info(
        f"RestDataClient retrieved {rows_count} records in {fetch_time:.2f} seconds"
    )

    # Check for timestamp continuity issues
    has_gaps, gap_info = analyze_timestamp_continuity(df, interval)

    if has_gaps:
        logger.warning(f"Found {len(gap_info)} gaps in RestDataClient data")
        for i, gap in enumerate(gap_info):
            logger.warning(
                f"Gap {i+1}: {gap['previous_time']} → {gap['current_time']} "
                f"({gap['gap_seconds']:.1f}s, missing {gap['missing_points']} points)"
            )

    return df, gap_info


def debug_dsm_with_cache(
    symbol: str,
    interval: Interval,
    start_time: datetime,
    end_time: datetime,
    market_type: MarketType = MarketType.SPOT,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Debug the DataSourceManager with caching enabled.

    Args:
        symbol: Trading pair symbol
        interval: Kline interval
        start_time: Start time
        end_time: End time
        market_type: Market type

    Returns:
        Tuple of (dataframe, gap_info)
    """
    logger.info(
        f"Debugging DataSourceManager (with cache) for {symbol} {interval.value}"
    )

    # Create a DSM with caching enabled
    dsm = DataSourceManager(market_type=market_type, use_cache=True)

    # Ensure cache directory exists
    cache_dir = Path("./cache/gap_debug")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Fetch data
    start_fetch = time.time()
    df = dsm.get_data(
        symbol=symbol, start_time=start_time, end_time=end_time, interval=interval
    )
    fetch_time = time.time() - start_fetch

    # Log statistics
    rows_count = len(df) if df is not None else 0
    logger.info(
        f"DataSourceManager (cached) retrieved {rows_count} records in {fetch_time:.2f} seconds"
    )

    # Check for timestamp continuity issues
    has_gaps, gap_info = analyze_timestamp_continuity(df, interval)

    if has_gaps:
        logger.warning(f"Found {len(gap_info)} gaps in DataSourceManager (cached) data")
        for i, gap in enumerate(gap_info):
            logger.warning(
                f"Gap {i+1}: {gap['previous_time']} → {gap['current_time']} "
                f"({gap['gap_seconds']:.1f}s, missing {gap['missing_points']} points)"
            )

    return df, gap_info


def debug_dsm_without_cache(
    symbol: str,
    interval: Interval,
    start_time: datetime,
    end_time: datetime,
    market_type: MarketType = MarketType.SPOT,
    enforce_source: DataSource = DataSource.AUTO,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Debug the DataSourceManager without caching.

    Args:
        symbol: Trading pair symbol
        interval: Kline interval
        start_time: Start time
        end_time: End time
        market_type: Market type
        enforce_source: Force specific data source

    Returns:
        Tuple of (dataframe, gap_info)
    """
    source_str = enforce_source.name if enforce_source != DataSource.AUTO else "AUTO"
    logger.info(
        f"Debugging DataSourceManager (no cache, source={source_str}) for {symbol} {interval.value}"
    )

    # Create a DSM with caching disabled
    dsm = DataSourceManager(market_type=market_type, use_cache=False)

    # Fetch data
    start_fetch = time.time()
    df = dsm.get_data(
        symbol=symbol, start_time=start_time, end_time=end_time, interval=interval
    )
    fetch_time = time.time() - start_fetch

    # Log statistics
    rows_count = len(df) if df is not None else 0
    logger.info(
        f"DataSourceManager (no cache) retrieved {rows_count} records in {fetch_time:.2f} seconds"
    )

    # Check for timestamp continuity issues
    has_gaps, gap_info = analyze_timestamp_continuity(df, interval)

    if has_gaps:
        logger.warning(
            f"Found {len(gap_info)} gaps in DataSourceManager (no cache) data"
        )
        for i, gap in enumerate(gap_info):
            logger.warning(
                f"Gap {i+1}: {gap['previous_time']} → {gap['current_time']} "
                f"({gap['gap_seconds']:.1f}s, missing {gap['missing_points']} points)"
            )

    return df, gap_info


def debug_multi_day_retrieval(
    symbol: str, interval: Interval, market_type: MarketType = MarketType.SPOT
) -> Dict[str, Any]:
    """Debug data retrieval across the 2024-2025 transition.

    Args:
        symbol: Trading pair symbol
        interval: Kline interval
        market_type: Market type

    Returns:
        Dictionary with debug results
    """
    logger.info(f"Debugging 2024-2025 transition for {symbol} {interval.value}")

    # Set up time range around 2024-2025 transition
    start_time = datetime(2024, 12, 30, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2025, 1, 2, 23, 59, 59, tzinfo=timezone.utc)

    results = {}

    # Test with different clients
    vision_df, vision_gaps = debug_vision_client(
        symbol, interval, start_time, end_time, market_type
    )
    rest_df, rest_gaps = debug_rest_client(
        symbol, interval, start_time, end_time, market_type
    )
    dsm_df, dsm_gaps = debug_dsm_without_cache(
        symbol, interval, start_time, end_time, market_type
    )

    # Analyze gaps specifically at the year boundary
    year_boundary = datetime(2024, 12, 31, 23, 0, 0, tzinfo=timezone.utc)
    next_year_start = datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc)

    # Filter for records around the boundary
    if vision_df is not None and not vision_df.empty:
        vision_boundary = vision_df[
            (vision_df["open_time"] >= year_boundary)
            & (vision_df["open_time"] <= next_year_start)
        ]
        logger.info(f"Vision API has {len(vision_boundary)} records at year boundary")

    if rest_df is not None and not rest_df.empty:
        rest_boundary = rest_df[
            (rest_df["open_time"] >= year_boundary)
            & (rest_df["open_time"] <= next_year_start)
        ]
        logger.info(f"REST API has {len(rest_boundary)} records at year boundary")

    results = {
        "vision": {
            "df": vision_df,
            "gaps": vision_gaps,
            "rows": len(vision_df) if vision_df is not None else 0,
        },
        "rest": {
            "df": rest_df,
            "gaps": rest_gaps,
            "rows": len(rest_df) if rest_df is not None else 0,
        },
        "dsm": {
            "df": dsm_df,
            "gaps": dsm_gaps,
            "rows": len(dsm_df) if dsm_df is not None else 0,
        },
    }

    return results


def debug_chunked_retrieval(
    symbol: str,
    interval: Interval,
    start_time: datetime,
    end_time: datetime,
    chunk_days: int = 5,
    market_type: MarketType = MarketType.SPOT,
) -> Dict[str, Any]:
    """Debug data retrieval with chunking to identify where gaps are introduced.

    Args:
        symbol: Trading pair symbol
        interval: Kline interval
        start_time: Start time
        end_time: End time
        chunk_days: Number of days per chunk
        market_type: Market type

    Returns:
        Dictionary with debug results
    """
    logger.info(
        f"Debugging chunked retrieval for {symbol} {interval.value} with {chunk_days}-day chunks"
    )

    # Align time boundaries
    aligned_start, aligned_end = align_time_boundaries(start_time, end_time, interval)

    # Divide the time range into chunks
    chunk_delta = timedelta(days=chunk_days)
    current_start = aligned_start
    chunks = []

    while current_start < aligned_end:
        current_end = min(current_start + chunk_delta, aligned_end)
        chunks.append((current_start, current_end))
        current_start = current_end

    logger.info(f"Created {len(chunks)} chunks for time range")

    # Process each chunk and gather results
    chunk_results = []
    merged_df = None

    for i, (chunk_start, chunk_end) in enumerate(chunks):
        logger.info(
            f"Processing chunk {i+1}/{len(chunks)}: {chunk_start} to {chunk_end}"
        )

        # Get data for this chunk using Vision API
        df, gaps = debug_vision_client(
            symbol, interval, chunk_start, chunk_end, market_type
        )

        # Analyze this chunk
        chunk_result = {
            "chunk_index": i,
            "start_time": chunk_start,
            "end_time": chunk_end,
            "rows": len(df) if df is not None else 0,
            "gaps": gaps,
            "has_gaps": len(gaps) > 0,
        }
        chunk_results.append(chunk_result)

        # Merge with previous chunks
        if df is not None and not df.empty:
            if merged_df is None:
                merged_df = df.copy()
            else:
                # Check for gaps at the merge point
                pre_merge_rows = len(merged_df)
                merged_df = (
                    pd.concat([merged_df, df])
                    .drop_duplicates(subset=["open_time"])
                    .sort_values("open_time")
                )
                post_merge_rows = len(merged_df)

                logger.info(
                    f"Merged chunk {i+1}: Added {post_merge_rows - pre_merge_rows} rows"
                )

                # Check for gaps in the merged dataframe
                has_gaps, merge_gaps = analyze_timestamp_continuity(merged_df, interval)
                if has_gaps:
                    # Find new gaps introduced during merging
                    new_merge_gaps = []
                    for gap in merge_gaps:
                        # Check if this gap occurs at the chunk boundary
                        if (
                            abs((gap["previous_time"] - chunk_start).total_seconds())
                            < 3600
                            or abs((gap["current_time"] - chunk_start).total_seconds())
                            < 3600
                        ):
                            gap["is_merge_gap"] = True
                            new_merge_gaps.append(gap)

                    if new_merge_gaps:
                        logger.warning(
                            f"Found {len(new_merge_gaps)} gaps at merge points for chunk {i+1}"
                        )
                        for gap in new_merge_gaps:
                            logger.warning(
                                f"Merge gap: {gap['previous_time']} → {gap['current_time']} "
                                f"({gap['gap_seconds']:.1f}s, missing {gap['missing_points']} points)"
                            )

    # Final analysis of the complete merged dataset
    if merged_df is not None:
        has_gaps, final_gaps = analyze_timestamp_continuity(merged_df, interval)
        logger.info(
            f"Final merged dataset has {len(merged_df)} rows with {len(final_gaps)} gaps"
        )

    return {
        "chunks": chunk_results,
        "merged_df": merged_df,
        "final_gaps": final_gaps if merged_df is not None else [],
    }


def main():
    """Run the gap debugger with command line arguments."""
    parser = argparse.ArgumentParser(description="Binance Data Services Gap Debugger")
    parser.add_argument(
        "--symbol", type=str, default="BTCUSDT", help="Trading pair symbol"
    )
    parser.add_argument("--interval", type=str, default="1m", help="Kline interval")
    parser.add_argument(
        "--market",
        type=str,
        default="spot",
        help="Market type (spot, futures_usdt, futures_coin)",
    )
    parser.add_argument(
        "--start-date", type=str, help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument("--end-date", type=str, help="End date in YYYY-MM-DD format")
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to analyze if start/end not specified",
    )
    parser.add_argument("--use-cache", action="store_true", help="Enable cache usage")
    parser.add_argument(
        "--year-transition",
        action="store_true",
        help="Debug 2024-2025 transition specifically",
    )
    parser.add_argument(
        "--chunked", action="store_true", help="Debug with chunked retrieval"
    )
    parser.add_argument(
        "--chunk-days", type=int, default=5, help="Days per chunk for chunked retrieval"
    )
    parser.add_argument("--full-test", action="store_true", help="Run all tests")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="logs/gap_debugger",
        help="Output directory for reports",
    )
    args = parser.parse_args()

    # Setup market type
    market_type_map = {
        "spot": MarketType.SPOT,
        "futures_usdt": MarketType.FUTURES_USDT,
        "futures_coin": MarketType.FUTURES_COIN,
    }
    market_type = market_type_map.get(args.market.lower(), MarketType.SPOT)

    # Parse interval
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
    interval = interval_map.get(args.interval, Interval.MINUTE_1)

    # Setup time range
    end_time = datetime.now(timezone.utc)
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        end_time = datetime(
            end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc
        )

    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        start_time = datetime(
            start_date.year,
            start_date.month,
            start_date.day,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        )
    else:
        start_time = end_time - timedelta(days=args.days)

    logger.info(f"Gap Debugger starting with parameters:")
    logger.info(f"  Symbol: {args.symbol}")
    logger.info(f"  Interval: {interval.value}")
    logger.info(f"  Market: {market_type.name}")
    logger.info(f"  Time range: {start_time} to {end_time}")
    logger.info(f"  Cache enabled: {args.use_cache}")

    # Run the specified tests
    if args.year_transition or args.full_test:
        results = debug_multi_day_retrieval(args.symbol, interval, market_type)
        logger.info("Year transition test completed")

    if args.chunked or args.full_test:
        chunk_results = debug_chunked_retrieval(
            args.symbol, interval, start_time, end_time, args.chunk_days, market_type
        )
        logger.info("Chunked retrieval test completed")

    # Run the basic tests
    vision_df, vision_gaps = debug_vision_client(
        args.symbol, interval, start_time, end_time, market_type
    )
    rest_df, rest_gaps = debug_rest_client(
        args.symbol, interval, start_time, end_time, market_type
    )

    if args.use_cache or args.full_test:
        dsm_cached_df, dsm_cached_gaps = debug_dsm_with_cache(
            args.symbol, interval, start_time, end_time, market_type
        )

    dsm_fresh_df, dsm_fresh_gaps = debug_dsm_without_cache(
        args.symbol, interval, start_time, end_time, market_type
    )

    # Output summary
    logger.info("====== Gap Analysis Summary ======")
    logger.info(
        f"VisionDataClient: {len(vision_df) if vision_df is not None else 0} rows, {len(vision_gaps)} gaps"
    )
    logger.info(
        f"RestDataClient: {len(rest_df) if rest_df is not None else 0} rows, {len(rest_gaps)} gaps"
    )
    logger.info(
        f"DataSourceManager (fresh): {len(dsm_fresh_df) if dsm_fresh_df is not None else 0} rows, {len(dsm_fresh_gaps)} gaps"
    )

    if args.use_cache or args.full_test:
        logger.info(
            f"DataSourceManager (cached): {len(dsm_cached_df) if dsm_cached_df is not None else 0} rows, {len(dsm_cached_gaps)} gaps"
        )

    logger.info("================================")
    logger.info(f"Full gap analysis log saved to: {log_file}")


if __name__ == "__main__":
    main()
