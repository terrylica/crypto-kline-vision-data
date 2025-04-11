#!/usr/bin/env python
"""
Arrow Cache Builder Script (Synchronous Version)

This script builds a local Arrow cache of market data from Binance Vision API using direct file operations.
It avoids async operations entirely to prevent hanging issues.

Usage:
    python scripts/arrow_cache/cache_builder_sync.py --symbols BTCUSDT,ETHUSDT --intervals 1m,5m --start-date 2024-01-01
"""

import os
import sys
import csv
import time
import signal
import argparse
import threading
import urllib.request
import zipfile
import json
import io
import hashlib
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple, Optional, Set

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils.logger_setup import logger
from rich import print
from utils.market_constraints import Interval

# Constants
CACHE_DIR = Path("./cache")
BINANCE_VISION_BASE_URL = "https://data.binance.vision"
COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]
SHUTDOWN_REQUESTED = False
MAX_WORKERS = 10  # Maximum number of concurrent downloads
CHECKSUM_FAILURES_DIR = Path("./logs/checksum_failures")


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""
    global SHUTDOWN_REQUESTED

    def handle_interrupt(*args):
        global SHUTDOWN_REQUESTED
        logger.warning("Received interrupt signal, initiating graceful shutdown...")
        SHUTDOWN_REQUESTED = True

    # Set up signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handle_interrupt)


def get_interval_from_string(interval_str):
    """Convert string interval to Interval enum.

    Args:
        interval_str: String interval (e.g., '1s', '1m')

    Returns:
        Interval enum or None if invalid
    """
    try:
        # Method 1: Direct lookup
        for interval in Interval:
            if interval.value == interval_str:
                return interval

        # Method 2: Fallback using matching
        raise ValueError(f"Unknown interval: {interval_str}")
    except ValueError as e:
        logger.error(f"Invalid interval: {interval_str} - {e}")
        return None


def parse_symbols_csv(file_path, limit=None):
    """Parse the symbols CSV file.

    Args:
        file_path: Path to the CSV file
        limit: Optional limit on number of symbols to return

    Returns:
        List of dictionaries with symbol info
    """
    symbols_data = []
    try:
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break
                # Convert string intervals to list
                row["available_intervals"] = (
                    row["available_intervals"].strip('"').split(",")
                )
                symbols_data.append(row)
        logger.info(f"Parsed {len(symbols_data)} symbols from {file_path}")
        return symbols_data
    except Exception as e:
        logger.error(f"Error parsing CSV file {file_path}: {e}")
        return []


def get_binance_vision_url(symbol, interval, date, market_type="spot"):
    """Get Binance Vision API URL for the given parameters.

    Args:
        symbol: Trading pair symbol
        interval: Interval string
        date: Date in datetime format
        market_type: Market type (spot, futures_usdt, futures_coin)

    Returns:
        Full URL to the ZIP file
    """
    date_str = date.strftime("%Y-%m-%d")
    month_str = date.strftime("%Y-%m")

    # Determine path based on market type and interval
    if market_type == "spot":
        path = f"data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"
    elif market_type == "futures_usdt":
        path = f"data/futures/um/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"
    elif market_type == "futures_coin":
        path = f"data/futures/cm/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"
    else:
        raise ValueError(f"Unsupported market type: {market_type}")

    return f"{BINANCE_VISION_BASE_URL}/{path}"


def download_and_extract_file(url, timeout=30):
    """Download a file from URL and extract ZIP content.

    Args:
        url: URL to download
        timeout: Timeout in seconds

    Returns:
        Tuple of (content, success)
    """
    try:
        logger.debug(f"Downloading {url}")
        response = urllib.request.urlopen(url, timeout=timeout)
        zip_content = response.read()

        # Extract the CSV content from ZIP
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            csv_file_name = zip_file.namelist()[0]  # Get the first file
            csv_content = zip_file.read(csv_file_name)
            return csv_content, True

    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return None, False


def parse_kline_csv(csv_content):
    """Parse kline CSV content to DataFrame.

    Args:
        csv_content: Raw CSV content

    Returns:
        DataFrame with parsed data
    """
    try:
        df = pd.read_csv(
            io.StringIO(csv_content.decode("utf-8")), header=None, names=COLUMNS
        )

        # Convert timestamp columns from milliseconds to datetime
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

        # Convert numeric columns
        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "taker_buy_volume",
            "taker_buy_quote_volume",
        ]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

        # Set index
        df.set_index("open_time", inplace=True)

        return df
    except Exception as e:
        logger.error(f"Error parsing CSV: {e}")
        return pd.DataFrame()


def save_to_arrow_cache(df, symbol, interval_str, date):
    """Save DataFrame to Arrow cache file.

    Args:
        df: DataFrame to save
        symbol: Symbol name
        interval_str: Interval string
        date: Date for the file

    Returns:
        bool: True if successful
    """
    try:
        # Create cache directory structure
        cache_path = CACHE_DIR / "BINANCE" / "KLINES" / symbol / interval_str
        cache_path.mkdir(parents=True, exist_ok=True)

        # Generate file path
        date_str = date.strftime("%Y-%m-%d")
        file_path = cache_path / f"{date_str}.arrow"

        # Prepare DataFrame (reset index for Arrow)
        save_df = df.copy()
        if save_df.index.name:
            save_df = save_df.reset_index()

        # Convert to Arrow table
        table = pa.Table.from_pandas(save_df)

        # Write to Arrow file - Convert path to string
        with pa.OSFile(str(file_path), "wb") as f:
            with pa.RecordBatchFileWriter(f, table.schema) as writer:
                writer.write_table(table)

        logger.info(f"Saved {len(df)} records to {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving Arrow cache: {e}")
        return False


def load_from_arrow_cache(symbol, interval_str, date):
    """Load DataFrame from Arrow cache.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        date: Date for the file

    Returns:
        Tuple of (DataFrame, success)
    """
    try:
        # Generate file path
        date_str = date.strftime("%Y-%m-%d")
        file_path = (
            CACHE_DIR
            / "BINANCE"
            / "KLINES"
            / symbol
            / interval_str
            / f"{date_str}.arrow"
        )

        if not file_path.exists():
            return None, False

        # Read Arrow file - Convert path to string
        with pa.OSFile(str(file_path), "rb") as f:
            reader = pa.RecordBatchFileReader(f)
            table = reader.read_all()

        # Convert to DataFrame
        df = table.to_pandas()

        # Set index if needed
        if "open_time" in df.columns:
            df.set_index("open_time", inplace=True)

        logger.debug(f"Loaded {len(df)} records from {file_path}")
        return df, True
    except Exception as e:
        logger.error(f"Error loading Arrow cache: {e}")
        return None, False


def check_cache_file_exists(symbol, interval_str, date):
    """Check if cache file exists.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        date: Date for the file

    Returns:
        Tuple of (exists, file_path)
    """
    date_str = date.strftime("%Y-%m-%d")
    file_path = (
        CACHE_DIR / "BINANCE" / "KLINES" / symbol / interval_str / f"{date_str}.arrow"
    )
    exists = file_path.exists()
    logger.debug(f"Cache file check: {file_path} {'exists' if exists else 'not found'}")
    return exists, file_path


def calculate_sha256(file_path):
    """Calculate SHA-256 checksum of a file.

    Args:
        file_path: Path to the file

    Returns:
        Hexadecimal string of the SHA-256 checksum
    """
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Read in 64k chunks for efficiency
        for chunk in iter(lambda: f.read(65536), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def verify_checksum(data_file, checksum_file, symbol, interval_str, date):
    """Verify data file against its checksum.

    Args:
        data_file: Path to the data file
        checksum_file: Path to the checksum file
        symbol: Symbol name
        interval_str: Interval string
        date: Date for the file

    Returns:
        True if checksum matches, False otherwise
    """
    try:
        # Read checksum file and normalize whitespace
        with open(checksum_file, "r") as f:
            content = f.read().strip()
            # Split on whitespace and take first part (the checksum)
            expected = content.split()[0]
            logger.debug(f"Raw checksum file content: '{content}'")
            logger.debug(f"Expected checksum: '{expected}'")

        # Calculate checksum of the zip file directly
        actual = calculate_sha256(data_file)
        logger.debug(f"Calculated checksum: '{actual}'")

        if actual != expected:
            # Log detailed error information
            logger.error(
                f"Checksum mismatch for {symbol} {interval_str} {date.strftime('%Y-%m-%d')}:"
            )
            logger.error(f"Expected: '{expected}'")
            logger.error(f"Actual  : '{actual}'")

            # Record failure in the registry
            record_checksum_failure(
                symbol, interval_str, date, expected, actual, "skipped"
            )
            return False

        logger.debug(
            f"Checksum verification successful for {symbol} {interval_str} {date.strftime('%Y-%m-%d')}"
        )
        return True
    except Exception as e:
        logger.error(f"Error verifying checksum: {e}")
        # Record this as a failure also
        try:
            record_checksum_failure(
                symbol, interval_str, date, "unknown", "error", f"error: {str(e)}"
            )
        except Exception as inner_e:
            logger.error(f"Error recording checksum failure: {inner_e}")
        return False


def record_checksum_failure(symbol, interval_str, date, expected, actual, action):
    """Record a checksum failure in the registry.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        date: Date for the file
        expected: Expected checksum
        actual: Actual checksum
        action: Action taken (skipped, cached_anyway, etc.)
    """
    # Ensure directory exists
    CHECKSUM_FAILURES_DIR.mkdir(parents=True, exist_ok=True)

    failures_file = CHECKSUM_FAILURES_DIR / "registry.json"

    # Load existing failures if file exists
    failures = []
    if failures_file.exists():
        try:
            with open(failures_file, "r") as f:
                failures = json.load(f)
        except Exception as e:
            logger.error(f"Error loading checksum failures registry: {e}")

    # Add new failure entry
    failures.append(
        {
            "symbol": symbol,
            "interval": interval_str,
            "date": (
                date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)
            ),
            "expected_checksum": expected,
            "actual_checksum": actual,
            "timestamp": datetime.now().isoformat(),
            "action_taken": action,
        }
    )

    # Save updated failures registry
    try:
        with open(failures_file, "w") as f:
            json.dump(failures, f, indent=2)

        # Also log to dedicated checksum failures log
        with open(CHECKSUM_FAILURES_DIR / "checksum_failures.log", "a") as f:
            f.write(
                f"{datetime.now().isoformat()} - {symbol} {interval_str} {date} - "
                f"Expected: {expected}, Actual: {actual}, Action: {action}\n"
            )
    except Exception as e:
        logger.error(f"Error saving checksum failures registry: {e}")


def get_failed_checksum_dates(symbol, interval_str):
    """Get list of dates with previously failed checksums.

    Args:
        symbol: Symbol name
        interval_str: Interval string

    Returns:
        List of dates with failed checksums
    """
    failures_file = CHECKSUM_FAILURES_DIR / "registry.json"

    if not failures_file.exists():
        return []

    try:
        with open(failures_file, "r") as f:
            failures = json.load(f)

        # Filter failures for the specific symbol and interval
        matching_failures = [
            failure
            for failure in failures
            if failure["symbol"] == symbol and failure["interval"] == interval_str
        ]

        # Return the dates
        return [failure["date"] for failure in matching_failures]
    except Exception as e:
        logger.error(f"Error retrieving failed checksum dates: {e}")
        return []


def download_data_with_checksum(
    symbol, interval_str, date, skip_checksum=False, proceed_on_failure=False
):
    """Download data for symbol/interval/date with checksum verification.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        date: Date to download
        skip_checksum: Whether to skip checksum verification
        proceed_on_failure: Whether to proceed with caching even if checksum fails

    Returns:
        tuple: (DataFrame or None, success boolean)
    """
    date_str = date.strftime("%Y-%m-%d")
    temp_dir = Path(
        f"./tmp/download_{symbol}_{interval_str}_{date_str}_{int(time.time())}"
    )
    temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Construct URLs for data and checksum files
        base_url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{interval_str}"
        data_url = f"{base_url}/{symbol}-{interval_str}-{date_str}.zip"
        checksum_url = f"{data_url}.CHECKSUM"

        # Download paths
        data_file = temp_dir / f"{symbol}-{interval_str}-{date_str}.zip"
        checksum_file = temp_dir / f"{symbol}-{interval_str}-{date_str}.zip.CHECKSUM"

        logger.debug(
            f"Downloading {symbol} {interval_str} for {date_str} from {data_url}"
        )

        # Download data file
        try:
            logger.debug(f"Downloading {data_url}")
            urllib.request.urlretrieve(data_url, data_file)
        except Exception as e:
            logger.error(f"Error downloading data file {data_url}: {e}")
            return None, False

        # Download checksum file if not skipping checksum verification
        checksum_verified = False
        if not skip_checksum:
            try:
                logger.debug(f"Downloading {checksum_url}")
                urllib.request.urlretrieve(checksum_url, checksum_file)

                # Verify checksum
                checksum_verified = verify_checksum(
                    data_file, checksum_file, symbol, interval_str, date
                )

                if not checksum_verified and not proceed_on_failure:
                    logger.error(
                        f"Checksum verification failed for {symbol} {interval_str} {date_str}"
                    )
                    return None, False

                if not checksum_verified and proceed_on_failure:
                    logger.warning(
                        f"Proceeding despite checksum failure for {symbol} {interval_str} {date_str}"
                    )
                    # Record that we proceeded anyway
                    try:
                        with open(checksum_file, "r") as f:
                            expected = f.read().strip().split()[0]
                        actual = calculate_sha256(data_file)
                        record_checksum_failure(
                            symbol,
                            interval_str,
                            date,
                            expected,
                            actual,
                            "cached_anyway",
                        )
                    except Exception as e:
                        logger.error(f"Error recording 'cached_anyway' action: {e}")
            except Exception as e:
                logger.error(f"Error in checksum verification process: {e}")
                if not proceed_on_failure:
                    return None, False
                logger.warning(
                    f"Proceeding despite checksum process error for {symbol} {interval_str} {date_str}"
                )

        # Extract and process data
        try:
            with zipfile.ZipFile(data_file, "r") as zip_file:
                # Get the first file in the zip (should be the CSV)
                csv_filename = zip_file.namelist()[0]
                with zip_file.open(csv_filename) as csv_file:
                    # Read CSV content
                    csv_content = csv_file.read().decode("utf-8")

                    # Process CSV data
                    rows = []
                    for row in csv.reader(csv_content.splitlines()):
                        # Binance format: open_time, open, high, low, close, volume, close_time, ...
                        rows.append(row)

                    # Convert to DataFrame
                    df = pd.DataFrame(
                        rows,
                        columns=[
                            "open_time",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "close_time",
                            "quote_volume",
                            "count",
                            "taker_buy_volume",
                            "taker_buy_quote_volume",
                            "ignore",
                        ],
                    )

                    # Convert numeric columns
                    for col in [
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "quote_volume",
                        "taker_buy_volume",
                        "taker_buy_quote_volume",
                    ]:
                        df[col] = pd.to_numeric(df[col])

                    # Convert timestamp columns
                    for col in ["open_time", "close_time"]:
                        # Fix for pandas FutureWarning by explicitly converting to numeric first
                        df[col] = pd.to_numeric(df[col])
                        df[col] = pd.to_datetime(df[col], unit="ms")

                    # Set index
                    df.set_index("open_time", inplace=True)

                    return df, True
        except Exception as e:
            logger.error(f"Error processing zip file: {e}")
            return None, False

    finally:
        # Clean up temporary directory
        try:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            logger.error(f"Error cleaning up temp directory: {e}")

    return None, False


def process_date(symbol, interval_str, date, args):
    """Process a single date for a symbol and interval.

    Args:
        symbol: Symbol to process
        interval_str: Interval string
        date: Date to process
        args: Command line arguments

    Returns:
        tuple: (Number of records, success boolean)
    """
    if SHUTDOWN_REQUESTED:
        return 0, False

    # Check if we should be processing this date based on retry flag
    if args.retry_failed_checksums and date.strftime(
        "%Y-%m-%d"
    ) not in get_failed_checksum_dates(symbol, interval_str):
        logger.debug(
            f"Skipping {symbol} {interval_str} {date.strftime('%Y-%m-%d')} - not in failed checksums list"
        )
        return 0, True

    # Check if cache file already exists
    cache_path = CACHE_DIR / "BINANCE" / "KLINES" / symbol / interval_str
    cache_file = cache_path / f"{date.strftime('%Y-%m-%d')}.arrow"
    cache_exists = cache_file.exists()

    logger.debug(
        f"Cache file check: {cache_file} {'exists' if cache_exists else 'not found'}"
    )

    if cache_exists:
        try:
            # Read from cache
            table = pa.ipc.open_file(pa.memory_map(str(cache_file), "r")).read_all()
            df = table.to_pandas()
            num_records = len(df)
            logger.debug(f"Loaded {num_records} records from {cache_file}")
            logger.info(
                f"Using cached data for {symbol} {interval_str} {date.strftime('%Y-%m-%d')}: {num_records} records"
            )
            return num_records, True
        except Exception as e:
            logger.error(f"Error reading cache file {cache_file}: {e}")
            # If we can't read the cache file, try downloading again
            cache_exists = False

    if not cache_exists or args.retry_failed_checksums:
        # Download with checksum verification
        df, success = download_data_with_checksum(
            symbol,
            interval_str,
            date,
            skip_checksum=args.skip_checksum,
            proceed_on_failure=args.proceed_on_checksum_failure,
        )

        if not success:
            logger.error(
                f"Failed to download data for {symbol} {interval_str} {date.strftime('%Y-%m-%d')}"
            )
            return 0, False

        # Save to cache
        num_records = len(df)
        success = save_to_arrow_cache(df, symbol, interval_str, date)

        if success:
            logger.info(
                f"Processed {symbol} {interval_str} {date.strftime('%Y-%m-%d')}: {num_records} records"
            )
            return num_records, True
        else:
            logger.error(
                f"Failed to save cache for {symbol} {interval_str} {date.strftime('%Y-%m-%d')}"
            )
            return 0, False

    # This should not be reached but just in case
    return 0, False


def get_date_range(start_date, end_date):
    """Generate a list of dates between start_date and end_date.

    Args:
        start_date: Start date
        end_date: End date

    Returns:
        List of dates
    """
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def cache_symbol_data(symbol, intervals, start_date, end_date, args):
    """Cache data for a symbol across multiple intervals and dates.

    Args:
        symbol: Symbol to cache
        intervals: List of intervals
        start_date: Start date
        end_date: End date
        args: Command line arguments

    Returns:
        dict: Statistics about the caching operation
    """
    # Get the date range
    dates = get_date_range(start_date, end_date)

    total_records = 0
    interval_stats = {}

    for interval_str in intervals:
        logger.info(f"Processing {symbol} with interval {interval_str}")
        interval_start_time = time.time()
        interval_records = 0

        if SHUTDOWN_REQUESTED:
            logger.warning(f"Shutdown requested, skipping interval {interval_str}")
            continue

        # Process each date with controlled parallelism
        with ThreadPoolExecutor(max_workers=min(len(dates), MAX_WORKERS)) as executor:
            futures = {
                executor.submit(process_date, symbol, interval_str, date, args): date
                for date in dates
            }

            for future in as_completed(futures):
                date = futures[future]
                try:
                    records, success = future.result()
                    interval_records += records
                except Exception as e:
                    logger.error(
                        f"Error processing {symbol} {interval_str} {date.strftime('%Y-%m-%d')}: {e}"
                    )

        interval_duration = time.time() - interval_start_time
        records_per_second = (
            interval_records / interval_duration if interval_duration > 0 else 0
        )

        logger.info(
            f"Completed {symbol} {interval_str}: {interval_records} records in {interval_duration:.2f}s ({records_per_second:.2f} records/s)"
        )

        interval_stats[interval_str] = {
            "records": interval_records,
            "duration": interval_duration,
            "records_per_second": records_per_second,
        }

        total_records += interval_records

    return {
        "symbol": symbol,
        "intervals": len(intervals),
        "total_records": total_records,
        "interval_stats": interval_stats,
    }


def setup_argparse():
    """Set up argument parsing."""
    parser = argparse.ArgumentParser(
        description="Build Arrow cache from Binance Vision API"
    )
    parser.add_argument("--symbols", type=str, help="Comma-separated list of symbols")
    parser.add_argument(
        "--intervals", type=str, default="5m", help="Comma-separated list of intervals"
    )
    parser.add_argument(
        "--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=str, help="End date (YYYY-MM-DD), default: today"
    )
    parser.add_argument("--csv-file", type=str, help="Path to symbols CSV file")
    parser.add_argument("--limit", type=int, help="Limit number of symbols to process")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    # Add checksum-related arguments
    parser.add_argument(
        "--skip-checksum",
        action="store_true",
        help="Skip checksum verification entirely",
    )
    parser.add_argument(
        "--proceed-on-checksum-failure",
        action="store_true",
        help="Proceed with caching even when checksum verification fails",
    )
    parser.add_argument(
        "--retry-failed-checksums",
        action="store_true",
        help="Retry downloading files with previously failed checksums",
    )

    return parser.parse_args()


def main():
    """Main entry point with CLI parsing."""
    global SHUTDOWN_REQUESTED

    # Set up signal handlers
    setup_signal_handlers()

    # Parse arguments
    args = setup_argparse()

    # Setup logging
    if args.debug:
        logger.setLevel("DEBUG")
        logger.debug("Debug logging enabled")

    # Process arguments
    symbols_to_process = []

    if args.csv_file:
        try:
            symbols_data = parse_symbols_csv(args.csv_file, args.limit)
            logger.info(f"Parsed {len(symbols_data)} symbols from {args.csv_file}")

            for data in symbols_data:
                symbol = data["symbol"]
                # If intervals specified on command line, use those
                if args.intervals:
                    intervals = args.intervals.split(",")
                else:
                    # Otherwise use intervals from CSV
                    intervals = data.get("available_intervals", "").split(",")
                    if not intervals or intervals == [""]:
                        intervals = ["5m"]  # Default interval

                symbols_to_process.append(
                    {
                        "symbol": symbol,
                        "intervals": intervals,
                        "earliest_date": data.get("earliest_date", args.start_date),
                    }
                )
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")
            return
    elif args.symbols:
        symbols = args.symbols.split(",")
        intervals = args.intervals.split(",")
        for symbol in symbols:
            symbols_to_process.append(
                {
                    "symbol": symbol,
                    "intervals": intervals,
                    "earliest_date": args.start_date,
                }
            )
    else:
        logger.error("Either --symbols or --csv-file must be specified")
        return

    # Ensure directories exist
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CHECKSUM_FAILURES_DIR.mkdir(parents=True, exist_ok=True)

    # Handle retry-failed-checksums option
    if args.retry_failed_checksums:
        failures_file = CHECKSUM_FAILURES_DIR / "registry.json"
        if not failures_file.exists():
            logger.warning("No checksum failures registry found, nothing to retry")
            return
        try:
            with open(failures_file, "r") as f:
                failures = json.load(f)
            logger.info(f"Found {len(failures)} checksum failures to retry")
        except Exception as e:
            logger.error(f"Error loading checksum failures registry: {e}")
            return

    # Main execution
    logger.info("[bold cyan]Arrow Cache Builder Started[/bold cyan]")

    # Determine start/end dates
    end_date = (
        datetime.fromisoformat(args.end_date) if args.end_date else datetime.now()
    )
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    logger.info(f"Processing {len(symbols_to_process)} symbols")
    logger.info(f"Date range: up to {end_date.strftime('%Y-%m-%d')}")

    # Process each symbol
    total_records = 0
    symbol_count = len(symbols_to_process)

    for i, data in enumerate(symbols_to_process, 1):
        if SHUTDOWN_REQUESTED:
            logger.warning("Shutdown requested, stopping processing")
            break

        symbol = data["symbol"]
        intervals = data["intervals"]
        start_date = datetime.fromisoformat(
            data["earliest_date"] if args.start_date is None else args.start_date
        )
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)

        logger.info(
            f"[bold magenta]Processing symbol {i}/{symbol_count}: {symbol}[/bold magenta]"
        )
        logger.info(
            f"Processing {symbol} with {len(intervals)} intervals from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        )

        try:
            stats = cache_symbol_data(symbol, intervals, start_date, end_date, args)
            total_records += stats["total_records"]

            # Log completion percentage
            completion_pct = (i / symbol_count) * 100
            logger.info(
                f"[bold green]Completed {symbol}: {stats['total_records']} records across {stats['intervals']} intervals ({completion_pct:.1f}% of symbols complete)[/bold green]"
            )
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")

    # Log final statistics
    logger.info("[bold cyan]Arrow Cache Building Complete[/bold cyan]")
    logger.info(f"Total records cached: {total_records}")
    logger.info(
        f"Total interval-symbol combinations: {sum(len(data['intervals']) for data in symbols_to_process)}"
    )


if __name__ == "__main__":
    main()
