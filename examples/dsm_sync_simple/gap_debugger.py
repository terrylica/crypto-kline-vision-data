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
from rich import print
from utils.market_constraints import Interval, MarketType
from utils.time_utils import (
    align_time_boundaries,
    get_interval_seconds,
)
from core.sync.vision_data_client import VisionDataClient
from core.sync.rest_data_client import RestDataClient
from core.sync.data_source_manager import DataSourceManager, DataSource
from utils.gap_detector import detect_gaps, Gap, format_gaps_for_display

# Ensure logs directory exists
log_dir = Path("logs/gap_debugger")
log_dir.mkdir(parents=True, exist_ok=True)

# Ensure error logs directory exists
error_log_dir = Path("logs/error_logs")
error_log_dir.mkdir(parents=True, exist_ok=True)

# Setup file handler for detailed logging
log_file = log_dir / f"gap_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
error_log_file = (
    error_log_dir / f"gap_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

# Configure logger to log to file and console
logger.setup_root(level="DEBUG")  # Configure root logger first
logger.enable_error_logging(str(error_log_file))
logger.show_filename(True)
logger.use_rich(True)
logger.setLevel("DEBUG")

# Set up file handler for the log file using the enhanced logger method
logger.add_file_handler(str(log_file), level="DEBUG", mode="w")

# Log the file paths using the logger
logger.info(f"Log file: {log_file}")
logger.info(f"Error log file: {error_log_file}")


def convert_gaps_to_legacy_format(gaps: List[Gap]) -> List[Dict[str, Any]]:
    """Convert gap_detector.Gap objects to the legacy gap info format for compatibility.

    Args:
        gaps: List of Gap objects from gap_detector

    Returns:
        List of gap dictionaries in the legacy format
    """
    legacy_gaps = []
    for gap in gaps:
        previous_time = gap.start_time
        current_time = gap.end_time
        gap_seconds = gap.duration.total_seconds()
        legacy_gaps.append(
            {
                "previous_time": previous_time,
                "current_time": current_time,
                "gap_seconds": gap_seconds,
                "expected_interval": gap_seconds / (gap.missing_points + 1),
                "missing_points": gap.missing_points,
                "day_boundary": gap.crosses_day_boundary,
                "month_boundary": previous_time.month != current_time.month,
                "year_boundary": previous_time.year != current_time.year,
            }
        )
    return legacy_gaps


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

    # Make sure the dataframe is usable for gap detection
    if df is not None and not df.empty:
        # If df has an index with name open_time_us, reset the index
        if (
            hasattr(df, "index")
            and hasattr(df.index, "name")
            and df.index.name == "open_time_us"
        ):
            df = df.reset_index()

        # Check for open_time in both index and columns (ambiguous case)
        if (
            hasattr(df, "index")
            and hasattr(df.index, "name")
            and df.index.name == "open_time"
            and "open_time" in df.columns
        ):
            # Create a new DataFrame without the ambiguous index
            df = pd.DataFrame(df.to_dict())

        # If no open_time column, create it from open_time_us if available
        if "open_time" not in df.columns and "open_time_us" in df.columns:
            df["open_time"] = pd.to_datetime(
                df["open_time_us"] // 1000, unit="ms", utc=True
            )

    # Skip gap detection if no data or no open_time column
    if df is None or df.empty or "open_time" not in df.columns:
        logger.warning("Cannot perform gap detection: missing data or open_time column")
        return df, []

    # Check for timestamp continuity issues using gap_detector
    # Only enforce min span requirement if we're querying a longer timeframe
    time_span_days = (end_time - start_time).total_seconds() / 86400
    enforce_min_span = time_span_days >= 1.0

    try:
        gaps, gap_stats = detect_gaps(
            df,
            interval,
            time_column="open_time",
            gap_threshold=0.3,  # 30% threshold
            day_boundary_threshold=1.5,  # Higher threshold for day boundaries
            enforce_min_span=enforce_min_span,  # Only enforce for longer timeframes
        )

        # Convert gaps to legacy format for compatibility
        gap_info = convert_gaps_to_legacy_format(gaps)

        if gaps:
            logger.warning(f"Found {len(gaps)} gaps in VisionDataClient data")
            for i, gap in enumerate(gaps):
                logger.warning(
                    f"Gap {i+1}: {gap.start_time} → {gap.end_time} "
                    f"(duration: {gap.duration}, missing {gap.missing_points} points, "
                    f"crosses day boundary: {gap.crosses_day_boundary})"
                )
    except Exception as e:
        logger.error(f"Error during gap detection: {e}")
        gaps = []
        gap_info = []

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

    # Make sure the dataframe is usable for gap detection
    if df is not None and not df.empty:
        # If df has an index with name open_time_us, reset the index
        if (
            hasattr(df, "index")
            and hasattr(df.index, "name")
            and df.index.name == "open_time_us"
        ):
            df = df.reset_index()

        # Check for open_time in both index and columns (ambiguous case)
        if (
            hasattr(df, "index")
            and hasattr(df.index, "name")
            and df.index.name == "open_time"
            and "open_time" in df.columns
        ):
            # Create a new DataFrame without the ambiguous index
            df = pd.DataFrame(df.to_dict())

        # If no open_time column, create it from open_time_us if available
        if "open_time" not in df.columns and "open_time_us" in df.columns:
            df["open_time"] = pd.to_datetime(
                df["open_time_us"] // 1000, unit="ms", utc=True
            )

    # Skip gap detection if no data or no open_time column
    if df is None or df.empty or "open_time" not in df.columns:
        logger.warning("Cannot perform gap detection: missing data or open_time column")
        return df, []

    # Check for timestamp continuity issues using gap_detector
    # Only enforce min span requirement if we're querying a longer timeframe
    time_span_days = (end_time - start_time).total_seconds() / 86400
    enforce_min_span = time_span_days >= 1.0

    try:
        gaps, gap_stats = detect_gaps(
            df,
            interval,
            time_column="open_time",
            gap_threshold=0.3,  # 30% threshold
            day_boundary_threshold=1.5,  # Higher threshold for day boundaries
            enforce_min_span=enforce_min_span,  # Only enforce for longer timeframes
        )

        # Convert gaps to legacy format for compatibility
        gap_info = convert_gaps_to_legacy_format(gaps)

        if gaps:
            logger.warning(f"Found {len(gaps)} gaps in RestDataClient data")
            for i, gap in enumerate(gaps):
                logger.warning(
                    f"Gap {i+1}: {gap.start_time} → {gap.end_time} "
                    f"(duration: {gap.duration}, missing {gap.missing_points} points, "
                    f"crosses day boundary: {gap.crosses_day_boundary})"
                )
    except Exception as e:
        logger.error(f"Error during gap detection: {e}")
        gaps = []
        gap_info = []

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

    # Log data retrieval statistics
    rows_count = len(df) if df is not None else 0
    logger.info(
        f"DataSourceManager retrieved {rows_count} records in {fetch_time:.2f} seconds"
    )

    # Make sure the dataframe is usable for gap detection
    if df is not None and not df.empty:
        # If df has an index with name open_time_us, reset the index
        if (
            hasattr(df, "index")
            and hasattr(df.index, "name")
            and df.index.name == "open_time_us"
        ):
            df = df.reset_index()

        # Check for open_time in both index and columns (ambiguous case)
        if (
            hasattr(df, "index")
            and hasattr(df.index, "name")
            and df.index.name == "open_time"
            and "open_time" in df.columns
        ):
            # Create a new DataFrame without the ambiguous index
            df = pd.DataFrame(df.to_dict())

        # If no open_time column, create it from open_time_us if available
        if "open_time" not in df.columns and "open_time_us" in df.columns:
            df["open_time"] = pd.to_datetime(
                df["open_time_us"] // 1000, unit="ms", utc=True
            )

    # Skip gap detection if no data or no open_time column
    if df is None or df.empty or "open_time" not in df.columns:
        logger.warning("Cannot perform gap detection: missing data or open_time column")
        return df, []

    # Check for timestamp continuity issues using gap_detector
    # Only enforce min span requirement if we're querying a longer timeframe
    time_span_days = (end_time - start_time).total_seconds() / 86400
    enforce_min_span = time_span_days >= 1.0

    try:
        gaps, gap_stats = detect_gaps(
            df,
            interval,
            time_column="open_time",
            gap_threshold=0.3,  # 30% threshold
            day_boundary_threshold=1.5,  # Higher threshold for day boundaries
            enforce_min_span=enforce_min_span,  # Only enforce for longer timeframes
        )

        # Convert gaps to legacy format for compatibility
        gap_info = convert_gaps_to_legacy_format(gaps)

        if gaps:
            logger.warning(f"Found {len(gaps)} gaps in DataSourceManager (cached) data")
            for i, gap in enumerate(gaps):
                logger.warning(
                    f"Gap {i+1}: {gap.start_time} → {gap.end_time} "
                    f"(duration: {gap.duration}, missing {gap.missing_points} points, "
                    f"crosses day boundary: {gap.crosses_day_boundary})"
                )
    except Exception as e:
        logger.error(f"Error during gap detection: {e}")
        gaps = []
        gap_info = []

    return df, gap_info


def debug_dsm_without_cache(
    symbol: str,
    interval: Interval,
    start_time: datetime,
    end_time: datetime,
    market_type: MarketType = MarketType.SPOT,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Debug the DataSourceManager without caching.

    Args:
        symbol: Trading pair symbol
        interval: Kline interval
        start_time: Start time
        end_time: End time
        market_type: Market type

    Returns:
        Tuple of (dataframe, gap_info)
    """
    logger.info(f"Debugging DataSourceManager (no cache) for {symbol} {interval.value}")

    # Create a DSM without caching
    dsm = DataSourceManager(market_type=market_type, use_cache=False)

    # Fetch data
    start_fetch = time.time()
    df = dsm.get_data(
        symbol=symbol, start_time=start_time, end_time=end_time, interval=interval
    )
    fetch_time = time.time() - start_fetch

    # Log data retrieval statistics
    rows_count = len(df) if df is not None else 0
    logger.info(
        f"DataSourceManager retrieved {rows_count} records in {fetch_time:.2f} seconds"
    )

    # Make sure the dataframe is usable for gap detection
    if df is not None and not df.empty:
        # If df has an index with name open_time_us, reset the index
        if (
            hasattr(df, "index")
            and hasattr(df.index, "name")
            and df.index.name == "open_time_us"
        ):
            df = df.reset_index()

        # Check for open_time in both index and columns (ambiguous case)
        if (
            hasattr(df, "index")
            and hasattr(df.index, "name")
            and df.index.name == "open_time"
            and "open_time" in df.columns
        ):
            # Create a new DataFrame without the ambiguous index
            df = pd.DataFrame(df.to_dict())

        # If no open_time column, create it from open_time_us if available
        if "open_time" not in df.columns and "open_time_us" in df.columns:
            df["open_time"] = pd.to_datetime(
                df["open_time_us"] // 1000, unit="ms", utc=True
            )

    # Skip gap detection if no data or no open_time column
    if df is None or df.empty or "open_time" not in df.columns:
        logger.warning("Cannot perform gap detection: missing data or open_time column")
        return df, []

    # Check for timestamp continuity issues using gap_detector
    # Only enforce min span requirement if we're querying a longer timeframe
    time_span_days = (end_time - start_time).total_seconds() / 86400
    enforce_min_span = time_span_days >= 1.0

    try:
        gaps, gap_stats = detect_gaps(
            df,
            interval,
            time_column="open_time",
            gap_threshold=0.3,  # 30% threshold
            day_boundary_threshold=1.5,  # Higher threshold for day boundaries
            enforce_min_span=enforce_min_span,  # Only enforce for longer timeframes
        )

        # Convert gaps to legacy format for compatibility
        gap_info = convert_gaps_to_legacy_format(gaps)

        if gaps:
            logger.warning(
                f"Found {len(gaps)} gaps in DataSourceManager (non-cached) data"
            )
            for i, gap in enumerate(gaps):
                logger.warning(
                    f"Gap {i+1}: {gap.start_time} → {gap.end_time} "
                    f"(duration: {gap.duration}, missing {gap.missing_points} points, "
                    f"crosses day boundary: {gap.crosses_day_boundary})"
                )
    except Exception as e:
        logger.error(f"Error during gap detection: {e}")
        gaps = []
        gap_info = []

    return df, gap_info


def debug_multi_day_retrieval(
    symbol: str, interval: Interval, market_type: MarketType = MarketType.SPOT
) -> Dict[str, Any]:
    """Debug multi-day retrieval to identify day boundary issues.

    Args:
        symbol: Trading pair symbol
        interval: Kline interval
        market_type: Market type

    Returns:
        Dictionary with debug info
    """
    logger.info(f"Debugging multi-day retrieval for {symbol} {interval.value}")

    # Use a time span that crosses multiple day boundaries
    # Start 3 days ago at 22:00 and end 1 day ago at 02:00 to capture multiple day transitions
    end_time = datetime.now(timezone.utc).replace(
        hour=2, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    start_time = end_time - timedelta(days=2, hours=4)  # Start 2 days earlier at 22:00

    logger.info(f"Time range: {start_time} to {end_time}")

    # Debug each data source separately
    vision_df, vision_gaps = debug_vision_client(
        symbol, interval, start_time, end_time, market_type
    )
    rest_df, rest_gaps = debug_rest_client(
        symbol, interval, start_time, end_time, market_type
    )
    dsm_df, dsm_gaps = debug_dsm_without_cache(
        symbol, interval, start_time, end_time, market_type
    )

    # Compare day boundary handling between sources
    vision_day_boundaries = [g for g in vision_gaps if g["day_boundary"]]
    rest_day_boundaries = [g for g in rest_gaps if g["day_boundary"]]
    dsm_day_boundaries = [g for g in dsm_gaps if g["day_boundary"]]

    logger.info("Day boundary comparison:")
    logger.info(f"Vision API: {len(vision_day_boundaries)} day boundary gaps")
    logger.info(f"REST API: {len(rest_day_boundaries)} day boundary gaps")
    logger.info(f"DSM: {len(dsm_day_boundaries)} day boundary gaps")

    # Check for midnight records in each source
    vision_midnights = []
    rest_midnights = []
    dsm_midnights = []

    if vision_df is not None and not vision_df.empty:
        # Make sure open_time is a column, not an index
        if vision_df.index.name == "open_time":
            vision_df = vision_df.reset_index()

        # Find any records that are exactly at midnight
        vision_midnights = vision_df[
            vision_df["open_time"].dt.time == datetime.min.time()
        ]
        logger.info(f"Vision API midnight records: {len(vision_midnights)}")
    else:
        logger.warning("No data from Vision API")

    if rest_df is not None and not rest_df.empty:
        # Make sure open_time is a column, not an index
        if rest_df.index.name == "open_time":
            rest_df = rest_df.reset_index()

        rest_midnights = rest_df[rest_df["open_time"].dt.time == datetime.min.time()]
        logger.info(f"REST API midnight records: {len(rest_midnights)}")
    else:
        logger.warning("No data from REST API")

    if dsm_df is not None and not dsm_df.empty:
        # Make sure open_time is a column, not an index
        if dsm_df.index.name == "open_time":
            dsm_df = dsm_df.reset_index()

        dsm_midnights = dsm_df[dsm_df["open_time"].dt.time == datetime.min.time()]
        logger.info(f"DSM midnight records: {len(dsm_midnights)}")
    else:
        logger.warning("No data from DSM")

    # Prepare results
    return {
        "start_time": start_time,
        "end_time": end_time,
        "vision_rows": len(vision_df) if vision_df is not None else 0,
        "rest_rows": len(rest_df) if rest_df is not None else 0,
        "dsm_rows": len(dsm_df) if dsm_df is not None else 0,
        "vision_gaps": len(vision_gaps),
        "rest_gaps": len(rest_gaps),
        "dsm_gaps": len(dsm_gaps),
        "vision_day_boundaries": len(vision_day_boundaries),
        "rest_day_boundaries": len(rest_day_boundaries),
        "dsm_day_boundaries": len(dsm_day_boundaries),
    }


def debug_chunked_retrieval(
    symbol: str,
    interval: Interval,
    start_time: datetime,
    end_time: datetime,
    chunk_days: int = 5,
    market_type: MarketType = MarketType.SPOT,
) -> Dict[str, Any]:
    """Debug chunked retrieval to identify merging issues.

    Args:
        symbol: Trading pair symbol
        interval: Kline interval
        start_time: Start time
        end_time: End time
        chunk_days: Number of days per chunk
        market_type: Market type

    Returns:
        Dictionary with debug info
    """
    logger.info(
        f"Debugging chunked retrieval for {symbol} {interval.value} with {chunk_days}-day chunks"
    )
    logger.info(f"Time range: {start_time} to {end_time}")

    # Calculate the total days in the request
    total_days = (end_time - start_time).days + 1
    logger.info(f"Total time span: {total_days} days")

    # Calculate number of chunks needed
    num_chunks = (total_days + chunk_days - 1) // chunk_days
    logger.info(f"Dividing into {num_chunks} chunks of {chunk_days} days each")

    # Initialize variables for chunked retrieval
    merged_df = None
    chunk_start = start_time
    chunk_results = []

    # Process each chunk
    for i in range(num_chunks):
        # Calculate this chunk's end time
        if i == num_chunks - 1:
            # Last chunk, use the original end time
            chunk_end = end_time
        else:
            # Regular chunk, add chunk_days
            chunk_end = chunk_start + timedelta(days=chunk_days)
            # Make sure we don't exceed the overall end time
            if chunk_end > end_time:
                chunk_end = end_time

        logger.info(
            f"Processing chunk {i+1}/{num_chunks}: {chunk_start} to {chunk_end}"
        )

        # Get data for this chunk
        dsm = DataSourceManager(market_type=market_type, use_cache=False)
        chunk_df = dsm.get_data(
            symbol=symbol,
            start_time=chunk_start,
            end_time=chunk_end,
            interval=interval,
        )

        # Make sure the dataframe is usable for gap detection
        if chunk_df is not None and not chunk_df.empty:
            # Reset index to make sure it doesn't conflict with the open_time column
            if chunk_df.index.name is not None:
                chunk_df = chunk_df.reset_index()

            # Ensure open_time column exists
            if "open_time" not in chunk_df.columns:
                # Try to derive it from open_time_us if available
                if "open_time_us" in chunk_df.columns:
                    chunk_df["open_time"] = pd.to_datetime(
                        chunk_df["open_time_us"] // 1000, unit="ms", utc=True
                    )

        # Check for gaps in this chunk
        # Only enforce min span requirement if we're querying a longer timeframe
        time_span_days = (chunk_end - chunk_start).total_seconds() / 86400
        enforce_min_span = time_span_days >= 1.0

        gap_list = []
        gap_stats = {}

        if chunk_df is not None and not chunk_df.empty:
            gaps, gap_stats = detect_gaps(
                chunk_df,
                interval,
                time_column="open_time",
                gap_threshold=0.3,  # 30% threshold
                day_boundary_threshold=1.5,  # Higher threshold for day boundaries
                enforce_min_span=enforce_min_span,  # Only enforce for longer timeframes
            )
            gap_list = gaps

        # Save chunk results
        rows_count = len(chunk_df) if chunk_df is not None else 0
        chunk_results.append(
            {
                "chunk_start": chunk_start,
                "chunk_end": chunk_end,
                "rows": rows_count,
                "gaps": len(gap_list),
                "day_boundary_gaps": len(
                    [g for g in gap_list if g.crosses_day_boundary]
                ),
            }
        )

        # Merge with previous chunks
        if merged_df is None:
            # First chunk, just use it directly
            merged_df = chunk_df
        else:
            # Merge with existing data
            if chunk_df is not None and not chunk_df.empty:
                # Before merging, check if there might be a gap at the chunk boundary
                if not merged_df.empty:
                    # Get the last record of the merged df
                    last_merged = merged_df["open_time"].max()
                    # Get the first record of the chunk
                    first_chunk = chunk_df["open_time"].min()
                    # Calculate time difference
                    time_diff = (first_chunk - last_merged).total_seconds()
                    # Expected interval in seconds
                    expected_interval_sec = interval.to_seconds()

                    # Check if there's a gap at the boundary
                    if time_diff > expected_interval_sec * 1.5:
                        logger.warning(
                            f"Gap detected at chunk boundary: {last_merged} → {first_chunk} "
                            f"({time_diff:.1f}s, expected {expected_interval_sec:.1f}s)"
                        )

                # Merge the dataframes
                merged_df = pd.concat([merged_df, chunk_df], ignore_index=True)
                # Remove duplicates and sort
                merged_df = merged_df.drop_duplicates(subset=["open_time"])
                merged_df = merged_df.sort_values("open_time").reset_index(drop=True)

                # Check for gaps in the merged dataframe
                merged_gaps = []

                if merged_df is not None and not merged_df.empty:
                    merged_gaps, gap_stats = detect_gaps(
                        merged_df,
                        interval,
                        time_column="open_time",
                        gap_threshold=0.3,  # 30% threshold
                        day_boundary_threshold=1.5,  # Higher threshold for day boundaries
                        enforce_min_span=enforce_min_span,
                    )

                if merged_gaps:
                    # Find new gaps introduced during merging
                    # These will be gaps that occur near the chunk boundary
                    new_merge_gaps = []
                    for gap in merged_gaps:
                        # Check if this gap occurs at the chunk boundary
                        if (
                            abs((gap.start_time - chunk_start).total_seconds()) < 3600
                            or abs((gap.end_time - chunk_start).total_seconds()) < 3600
                        ):
                            # Mark this as a merge-related gap
                            new_merge_gaps.append(gap)

                    # Log any new gaps
                    if new_merge_gaps:
                        logger.warning(
                            f"Found {len(new_merge_gaps)} new gaps after merging with chunk {i+1}"
                        )
                        for gap in new_merge_gaps:
                            logger.warning(
                                f"Merge gap: {gap.start_time} → {gap.end_time} "
                                f"(duration: {gap.duration}, missing {gap.missing_points} points, "
                                f"crosses day boundary: {gap.crosses_day_boundary})"
                            )

        # Move to the next chunk
        chunk_start = chunk_end + timedelta(seconds=1)

    # Final analysis of the complete merged dataset
    final_gaps = []
    if merged_df is not None and not merged_df.empty:
        final_gaps, gap_stats = detect_gaps(
            merged_df,
            interval,
            time_column="open_time",
            gap_threshold=0.3,  # 30% threshold
            day_boundary_threshold=1.5,  # Higher threshold for day boundaries
            enforce_min_span=True,  # Enforce for the complete dataset
        )

        logger.info(
            f"Final merged dataset has {len(merged_df)} rows with {len(final_gaps)} gaps"
        )
    else:
        logger.warning("No data retrieved in any chunk")

    # Report chunk-wise statistics
    logger.info("Chunk-wise statistics:")
    for i, chunk in enumerate(chunk_results):
        logger.info(
            f"Chunk {i+1}: {chunk['chunk_start']} to {chunk['chunk_end']} - "
            f"{chunk['rows']} rows, {chunk['gaps']} gaps, "
            f"{chunk['day_boundary_gaps']} day boundary gaps"
        )

    # Prepare final results
    result = {
        "start_time": start_time,
        "end_time": end_time,
        "total_chunks": num_chunks,
        "chunk_days": chunk_days,
        "total_rows": len(merged_df) if merged_df is not None else 0,
        "chunk_results": chunk_results,
        "final_gaps_count": len(final_gaps) if merged_df is not None else 0,
    }

    return result


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
