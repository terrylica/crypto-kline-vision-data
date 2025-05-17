#!/usr/bin/env python
"""
Arrow Cache Builder Script (Synchronous Version)

This script builds a local Arrow cache of market data from Binance Vision API using direct file operations.
It avoids async operations entirely to prevent hanging issues.

Usage:
    # Small footprint test (3 symbols, 5m interval, recent data)
    python scripts/arrow_cache/cache_builder_sync.py --symbols BTCUSDT,ETHUSDT,BNBUSDT --intervals 5m --start-date 2024-01-01

    # Specific symbols and intervals
    python scripts/arrow_cache/cache_builder_sync.py --symbols BTCUSDT,ETHUSDT --intervals 1m,5m --start-date 2024-01-01
"""

import argparse
import csv
import hashlib
import io
import json
import signal
import sqlite3
import sys
import time
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa

from utils.config import HTTP_NOT_FOUND, SMALL_FILE_SIZE
from utils.logger_setup import logger
from utils.market_constraints import Interval, MarketType


# Class to manage shutdown state instead of using a global variable
class ShutdownState:
    """Class to track shutdown state."""

    def __init__(self):
        self.requested = False

    def request_shutdown(self):
        """Request a shutdown."""
        self.requested = True

    def is_shutdown_requested(self):
        """Check if shutdown was requested."""
        return self.requested


# Add re module for pattern matching in JSON validation

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
SHUTDOWN_STATE = ShutdownState()  # Replace global with instance
MAX_WORKERS = 10  # Maximum number of concurrent downloads
CHECKSUM_FAILURES_DIR = Path("./logs/checksum_failures")
CACHE_INDEX_FILE = Path("./logs/cache_index.json")  # Legacy JSON file, for backward compatibility
CACHE_INDEX_DB = Path("./logs/cache_index.db")  # SQLite database file
LOGS_DIR = Path("./logs")

# Timestamp format detection thresholds
MILLISECOND_DIGITS = 13
MICROSECOND_DIGITS = 16

# Create necessary directories
LOGS_DIR.mkdir(parents=True, exist_ok=True)
CHECKSUM_FAILURES_DIR.mkdir(parents=True, exist_ok=True)


def initialize_cache_db():
    """Initialize the SQLite database for cache index if it doesn't exist."""
    try:
        # Create directory if needed
        if not CACHE_INDEX_DB.parent.exists():
            CACHE_INDEX_DB.parent.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory for cache DB: {CACHE_INDEX_DB.parent}")

        # Connect to the database (will create if it doesn't exist)
        conn = sqlite3.connect(CACHE_INDEX_DB)
        cursor = conn.cursor()

        # Create tables if they don't exist
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS cache_metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
        )

        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS cache_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            interval TEXT,
            date TEXT,
            file_size INTEGER,
            num_records INTEGER,
            last_updated TEXT,
            path TEXT,
            UNIQUE(symbol, interval, date)
        )
        """
        )

        # Set the last_update metadata if it doesn't exist
        cursor.execute(
            "INSERT OR IGNORE INTO cache_metadata (key, value) VALUES (?, ?)",
            ("last_update", datetime.now().isoformat()),
        )

        # Commit changes and close connection
        conn.commit()
        conn.close()

        logger.info(f"Initialized cache index database at {CACHE_INDEX_DB}")

        # If legacy JSON file exists and database is new, migrate data
        if CACHE_INDEX_FILE.exists() and CACHE_INDEX_DB.stat().st_size < SMALL_FILE_SIZE:  # Check if DB is small/new
            logger.info("Legacy JSON cache index found, attempting to migrate data")
            migrate_json_to_sqlite()

    except Exception as e:
        logger.error(f"Error initializing cache database: {e}")


def migrate_json_to_sqlite():
    """Migrate data from legacy JSON format to SQLite database."""
    try:
        # Check if JSON file exists and has content
        if not CACHE_INDEX_FILE.exists() or CACHE_INDEX_FILE.stat().st_size == 0:
            logger.info("No legacy data to migrate")
            return

        # Try to read the JSON file
        try:
            with open(CACHE_INDEX_FILE) as f:
                content = f.read().strip()
                if not content:
                    logger.info("Legacy JSON file is empty, nothing to migrate")
                    return

                index = json.loads(content)
        except json.JSONDecodeError as e:
            logger.warning(f"Error parsing legacy cache index: {e}, skipping migration")
            return

        # Connect to the database
        conn = sqlite3.connect(CACHE_INDEX_DB)
        cursor = conn.cursor()

        # Update last_update metadata
        if "last_update" in index:
            cursor.execute(
                "UPDATE cache_metadata SET value = ? WHERE key = ?",
                (index["last_update"], "last_update"),
            )

        # Migrate symbol data
        if "symbols" in index:
            for symbol, intervals in index["symbols"].items():
                for interval_str, interval_data in intervals.items():
                    if "dates" in interval_data:
                        for date_str, entry in interval_data["dates"].items():
                            # Insert entry into database
                            cursor.execute(
                                """
                            INSERT OR REPLACE INTO cache_entries
                            (symbol, interval, date, file_size, num_records, last_updated, path)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                                (
                                    symbol,
                                    interval_str,
                                    date_str,
                                    entry.get("file_size", 0),
                                    entry.get("num_records", 0),
                                    entry.get("last_updated", datetime.now().isoformat()),
                                    entry.get("path", ""),
                                ),
                            )

        # Commit changes and close connection
        conn.commit()
        conn.close()

        # Create backup of the JSON file
        backup_path = CACHE_INDEX_FILE.with_suffix(f".json.migrated.{int(time.time())}")
        import shutil

        shutil.copy2(CACHE_INDEX_FILE, backup_path)
        logger.info(f"Created backup of migrated JSON file at {backup_path}")

        logger.info("Successfully migrated legacy JSON cache index to SQLite database")
    except Exception as e:
        logger.error(f"Error during migration: {e}")


def update_cache_index(
    symbol,
    interval_str,
    date,
    file_size,
    num_records,
    market_type="spot",
    data_provider="BINANCE",
    chart_type="KLINES",
):
    """Update the cache index with information about a newly cached file.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        date: Date of the data
        file_size: Size of the file in bytes
        num_records: Number of records in the file
        market_type: Market type (spot, futures_usdt, futures_coin)
        data_provider: Data provider (default: BINANCE)
        chart_type: Chart type (default: KLINES)
    """
    try:
        # Initialize database if needed
        initialize_cache_db()

        # Convert date to string
        date_str = date.strftime("%Y-%m-%d")

        # Connect to the database
        conn = sqlite3.connect(CACHE_INDEX_DB)
        cursor = conn.cursor()

        # Update or insert entry
        cursor.execute(
            """
        INSERT OR REPLACE INTO cache_entries
        (symbol, interval, date, file_size, num_records, last_updated, path)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                symbol,
                interval_str,
                date_str,
                file_size,
                num_records,
                datetime.now().isoformat(),
                str(
                    get_cache_path(
                        symbol,
                        interval_str,
                        date,
                        market_type,
                        data_provider,
                        chart_type,
                    )
                ),
            ),
        )

        # Update last_update metadata
        cursor.execute(
            "UPDATE cache_metadata SET value = ? WHERE key = ?",
            (datetime.now().isoformat(), "last_update"),
        )

        # Commit changes and close connection
        conn.commit()
        conn.close()

        logger.debug(f"Updated cache index for {symbol}/{interval_str}/{date_str}")
    except Exception as e:
        logger.error(f"Error updating cache index: {e}")


def get_cache_path(
    symbol,
    interval_str,
    date,
    market_type="spot",
    data_provider="BINANCE",
    chart_type="KLINES",
):
    """Get the path for a cache file.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        date: Date for the file
        market_type: Market type (spot, futures_usdt, futures_coin)
        data_provider: Data provider (default: BINANCE)
        chart_type: Chart type (default: KLINES)

    Returns:
        Path object for the cache file
    """
    # Convert market_type to a directory-friendly format
    if market_type == "spot":
        market_type_str = "spot"
    elif market_type == "futures_usdt":
        market_type_str = "futures_um"
    elif market_type == "futures_coin":
        market_type_str = "futures_cm"
    else:
        market_type_str = market_type.replace("/", "_")

    # Create cache directory structure matching how ArrowCacheReader expects it
    cache_path = CACHE_DIR / data_provider / chart_type / market_type_str / symbol / interval_str

    # Format date for filename (YYYY-MM-DD format)
    date_str = date.strftime("%Y-%m-%d")
    return cache_path / f"{date_str}.arrow"


def detect_cache_gaps(symbol, interval_str, start_date, end_date):
    """Detect gaps in the cache for a symbol and interval.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        start_date: Start date for checking
        end_date: End date for checking

    Returns:
        List of dates that are missing from the cache
    """
    # Get date range
    date_range = get_date_range(start_date, end_date)
    missing_dates = []

    try:
        # Connect to the database
        conn = sqlite3.connect(CACHE_INDEX_DB)
        cursor = conn.cursor()

        # Get all dates in the range from the database
        cursor.execute(
            """
        SELECT date FROM cache_entries
        WHERE symbol = ? AND interval = ?
        AND date >= ? AND date <= ?
        """,
            (
                symbol,
                interval_str,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
            ),
        )

        existing_dates = [datetime.strptime(row[0], "%Y-%m-%d").date() for row in cursor.fetchall()]
        conn.close()

        # Find missing dates
        for date in date_range:
            if date not in existing_dates and not check_cache_file_exists(symbol, interval_str, date)[0]:
                missing_dates.append(date)
    except Exception as e:
        logger.error(f"Error detecting cache gaps: {e}")
        # Fall back to file system check
        for date in date_range:
            if not check_cache_file_exists(symbol, interval_str, date)[0]:
                missing_dates.append(date)

    if missing_dates:
        logger.info(f"Found {len(missing_dates)} missing dates for {symbol}/{interval_str} between {start_date} and {end_date}")

    return missing_dates


def get_last_date_in_cache(symbol, interval_str):
    """Get the latest date available in the cache for a symbol and interval.

    Args:
        symbol: Symbol name
        interval_str: Interval string

    Returns:
        Latest date in cache or None if no cache exists
    """
    # Check if directory exists first for backward compatibility
    cache_path = CACHE_DIR / "BINANCE" / "KLINES" / symbol / interval_str
    if not cache_path.exists():
        return None

    try:
        # Connect to the database
        conn = sqlite3.connect(CACHE_INDEX_DB)
        cursor = conn.cursor()

        # Query for the latest date
        cursor.execute(
            """
        SELECT date FROM cache_entries
        WHERE symbol = ? AND interval = ?
        ORDER BY date DESC LIMIT 1
        """,
            (symbol, interval_str),
        )

        result = cursor.fetchone()
        conn.close()

        if result:
            return datetime.strptime(result[0], "%Y-%m-%d").date()

        # If not found in database, fall back to file system check (legacy method)
        if cache_path.exists():
            date_files = list(cache_path.glob("*.arrow"))
            if date_files:
                # Get date from filenames (expected format: YYYY-MM-DD.arrow)
                dates = [datetime.strptime(f.stem, "%Y-%m-%d").date() for f in date_files]
                if dates:
                    return max(dates)

        return None
    except Exception as e:
        logger.error(f"Error getting last date in cache: {e}")
        return None


def get_first_date_in_cache(symbol, interval_str):
    """Get the earliest date available in the cache for a symbol and interval.

    Args:
        symbol: Symbol name
        interval_str: Interval string

    Returns:
        Earliest date in cache or None if no cache exists
    """
    # Check if directory exists first for backward compatibility
    cache_path = CACHE_DIR / "BINANCE" / "KLINES" / symbol / interval_str
    if not cache_path.exists():
        return None

    try:
        # Connect to the database
        conn = sqlite3.connect(CACHE_INDEX_DB)
        cursor = conn.cursor()

        # Query for the earliest date
        cursor.execute(
            """
        SELECT date FROM cache_entries
        WHERE symbol = ? AND interval = ?
        ORDER BY date ASC LIMIT 1
        """,
            (symbol, interval_str),
        )

        result = cursor.fetchone()
        conn.close()

        if result:
            return datetime.strptime(result[0], "%Y-%m-%d").date()

        # If not found in database, fall back to file system check (legacy method)
        if cache_path.exists():
            date_files = list(cache_path.glob("*.arrow"))
            if date_files:
                # Get date from filenames (expected format: YYYY-MM-DD.arrow)
                dates = [datetime.strptime(f.stem, "%Y-%m-%d").date() for f in date_files]
                if dates:
                    return min(dates)

        return None
    except Exception as e:
        logger.error(f"Error getting first date in cache: {e}")
        return None


def setup_signal_handlers():
    """Set up signal handlers for graceful shutdown."""

    def handle_interrupt(*_args):
        """
        Signal handler for graceful shutdown.

        Args:
            *_args: Signal number and frame, required by the signal handler interface
                  but not used in this implementation
        """
        logger.warning("Received interrupt signal, initiating graceful shutdown...")
        SHUTDOWN_STATE.request_shutdown()

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
        with open(file_path) as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if limit and i >= limit:
                    break
                # Convert string intervals to list
                row["available_intervals"] = row["available_intervals"].strip('"').split(",")
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


def download_data_with_checksum(
    symbol,
    interval_str,
    date,
    skip_checksum=False,
    proceed_on_failure=False,
    market_type="spot",
    data_provider="BINANCE",
    chart_type="KLINES",
):
    """Download data with checksum verification (if applicable).

    Args:
        symbol: Trading symbol
        interval_str: Interval string (e.g., "1m", "1h")
        date: Date to download
        skip_checksum: Whether to skip checksum verification
        proceed_on_failure: Whether to proceed on download failure
        market_type: Market type (spot, futures, etc.)
        data_provider: Data provider (e.g., "BINANCE")
        chart_type: Chart type (e.g., "KLINES")

    Returns:
        Tuple of (success, df)
    """
    # Log download attempt with provider information
    logger.info(f"Downloading {symbol} {interval_str} data for {date} from {data_provider} ({market_type} market, {chart_type})")

    # Currently only Binance is fully supported, but log the provider for future extension
    if data_provider != "BINANCE":
        logger.warning(f"Provider {data_provider} support is experimental")

    date_str = date.strftime("%Y-%m-%d")
    # Check if date is a datetime object or already a date object
    if hasattr(date, "date"):
        compare_date = date.date()
    else:
        # Already a date object
        compare_date = date
    is_current_day = compare_date == datetime.now().date()

    if is_current_day:
        logger.warning(f"Attempting to download current-day data for {symbol} {interval_str} {date_str}")
        logger.warning("Current-day data may not be available yet from Binance Vision API")

    # Create temporary directory
    temp_dir = Path(f"./tmp/download_{symbol}_{interval_str}_{date_str}_{int(time.time())}")
    temp_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Map market_type string to the correct Vision API path
        # Convert market_type string to enum if needed
        if isinstance(market_type, str):
            try:
                market_type_enum = MarketType.from_string(market_type)
                vision_api_path = market_type_enum.vision_api_path
            except ValueError:
                logger.warning(f"Unknown market type: {market_type}, defaulting to 'spot'")
                vision_api_path = "spot"
        else:
            # Already an enum
            vision_api_path = market_type.vision_api_path

        # Handle symbol name adjustments for certain market types
        adjusted_symbol = symbol
        if "futures_coin" in market_type or "futures/cm" in vision_api_path:
            if not adjusted_symbol.endswith("_PERP"):
                adjusted_symbol = f"{adjusted_symbol}_PERP"

        # Construct URLs for data and checksum files
        base_url = f"{BINANCE_VISION_BASE_URL}/data/{vision_api_path}/daily/{chart_type.lower()}/{adjusted_symbol}/{interval_str}"
        data_filename = f"{adjusted_symbol}-{interval_str}-{date_str}.zip"
        data_url = f"{base_url}/{data_filename}"
        checksum_url = f"{data_url}.CHECKSUM"

        logger.debug(f"Using Vision API path: {vision_api_path}")
        logger.debug(f"Using adjusted symbol: {adjusted_symbol}")
        logger.debug(f"Using URL base: {base_url}")

        # Download paths
        data_file = temp_dir / data_filename
        checksum_file = temp_dir / f"{data_filename}.CHECKSUM"

        logger.debug(f"Downloading {data_url}")

        # Download data file
        try:
            # Use urllib.request with a specific HTTP error handler
            try:
                urllib.request.urlretrieve(data_url, data_file)
            except urllib.error.HTTPError as http_err:
                if http_err.code == HTTP_NOT_FOUND:
                    if is_current_day:
                        logger.warning(f"Current-day data not available for {symbol} {interval_str} {date_str} (404 Not Found)")
                    else:
                        logger.error(f"Error downloading data file {data_url}: HTTP Error 404: Not Found")
                    return False, None, 0
                # Re-raise for other HTTP errors
                raise
        except Exception as e:
            logger.error(f"Error downloading data file {data_url}: {e}")
            return False, None, 0

        if not data_file.exists() or data_file.stat().st_size == 0:
            logger.error(f"Failed to download data from {data_url}")
            return False, None, 0

        # Download checksum file if not skipping checksum verification
        checksum_verified = True
        if not skip_checksum:
            try:
                logger.debug(f"Downloading checksum {checksum_url}")
                try:
                    urllib.request.urlretrieve(checksum_url, checksum_file)
                except urllib.error.HTTPError as http_err:
                    if http_err.code == HTTP_NOT_FOUND:
                        logger.warning(f"Checksum file not found for {symbol} {interval_str} {date_str} (404 Not Found)")
                        if not proceed_on_failure:
                            return False, None, 0
                        logger.warning(f"Proceeding without checksum verification for {symbol} {interval_str} {date_str}")
                        skip_checksum = True
                    else:
                        # Re-raise for other HTTP errors
                        raise

                # Verify checksum if we have the checksum file
                if not skip_checksum and checksum_file.exists() and checksum_file.stat().st_size > 0:
                    # Read expected checksum
                    with open(checksum_file) as f:
                        content = f.read().strip()
                        # Split on whitespace and take first part (the checksum)
                        expected = content.split()[0]
                        content_length = len(content)
                        preview_length = min(40, content_length)
                        logger.debug(
                            f"Raw checksum file content: '{content[:preview_length]}' (+ {content_length - preview_length} more chars, {content_length} total)"
                        )
                        logger.debug(f"Expected checksum: '{expected}'")

                    # Calculate actual checksum on the ZIP file
                    actual_checksum = calculate_sha256(data_file)

                    # Verify checksum
                    checksum_verified = actual_checksum == expected

                    if not checksum_verified:
                        logger.warning(f"Checksum verification failed for {symbol} {interval_str} {date_str}")
                        logger.warning(f"Expected: {expected}")
                        logger.warning(f"Actual:   {actual_checksum}")

                        # Record the failure
                        action = "cached_anyway" if proceed_on_failure else "skipped"
                        record_checksum_failure(
                            symbol,
                            interval_str,
                            date,
                            expected,
                            actual_checksum,
                            action,
                        )

                        if not proceed_on_failure:
                            logger.error(f"Skipping file due to checksum failure: {symbol} {interval_str} {date_str}")
                            return False, None, 0
                        logger.warning(f"Proceeding despite checksum failure for {symbol} {interval_str} {date_str}")
            except Exception as e:
                logger.error(f"Error in checksum verification process: {e}")
                if not proceed_on_failure:
                    return False, None, 0
                logger.warning(f"Proceeding despite checksum process error: {e}")

        # Extract and parse data
        try:
            # Extract CSV from ZIP file
            with zipfile.ZipFile(data_file, "r") as zip_ref:
                csv_file_name = zip_ref.namelist()[0]  # Get the first file
                with zip_ref.open(csv_file_name) as csv_file:
                    csv_content = csv_file.read()

            # Parse data
            df = parse_kline_csv(csv_content)
            num_records = len(df)
            logger.debug(f"Parsed {num_records} records for {symbol} {interval_str} {date_str}")

            if num_records == 0:
                logger.warning(f"No data found for {symbol} {interval_str} {date_str}")
                return False, None, 0

            return True, df, num_records
        except Exception as e:
            logger.error(f"Error extracting or parsing data: {e}")
            return False, None, 0
    finally:
        # Clean up temporary directory
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


def parse_kline_csv(csv_content):
    """Parse kline CSV content to DataFrame.

    Args:
        csv_content: Raw CSV content

    Returns:
        DataFrame with parsed data
    """
    try:
        df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")), header=None, names=COLUMNS)

        # Detect timestamp unit from the first row
        timestamp_unit = "ms"  # Default milliseconds

        if not df.empty:
            try:
                sample_ts = df["open_time"].iloc[0]
                digits = len(str(int(sample_ts)))

                if digits == MICROSECOND_DIGITS:
                    # Already in microseconds, no conversion needed
                    timestamp_unit = "us"
                    logger.debug("Detected microsecond precision (16 digits)")
                elif digits == MILLISECOND_DIGITS:
                    # Standard millisecond format
                    timestamp_unit = "ms"
                    logger.debug("Detected millisecond precision (13 digits)")
                else:
                    logger.warning(
                        f"Unexpected timestamp format with {digits} digits. "
                        f"Expected {MILLISECOND_DIGITS} for milliseconds or "
                        f"{MICROSECOND_DIGITS} for microseconds. Defaulting to milliseconds."
                    )
            except (ValueError, TypeError) as e:
                logger.warning(f"Error detecting timestamp format: {e}")

        # Convert timestamp columns with detected unit
        df["open_time"] = pd.to_datetime(df["open_time"], unit=timestamp_unit, utc=True)
        df["close_time"] = pd.to_datetime(df["close_time"], unit=timestamp_unit, utc=True)

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

        # Set index and return directly
        return df.set_index("open_time")
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

        # Ensure open_time and close_time are timezone-aware
        for ts_col in ["open_time", "close_time"]:
            if ts_col in save_df.columns and save_df[ts_col].dt.tz is None:
                save_df[ts_col] = save_df[ts_col].dt.tz_localize("UTC")

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


def load_from_arrow_cache(
    symbol,
    interval_str,
    date,
    market_type="spot",
    data_provider="BINANCE",
    chart_type="KLINES",
):
    """Load DataFrame from Arrow cache.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        date: Date for the file
        market_type: Market type (spot, futures_usdt, futures_coin)
        data_provider: Data provider (default: BINANCE)
        chart_type: Chart type (default: KLINES)

    Returns:
        Tuple of (DataFrame, success)
    """
    try:
        # Get file path using get_cache_path
        file_path = get_cache_path(symbol, interval_str, date, market_type, data_provider, chart_type)

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
            # Ensure timezone aware
            if df["open_time"].dt.tz is None:
                df["open_time"] = df["open_time"].dt.tz_localize("UTC")
            df = df.set_index("open_time")

        logger.debug(f"Loaded {len(df)} records from {file_path}")
        return df, True
    except Exception as e:
        logger.error(f"Error loading Arrow cache: {e}")
        return None, False


def check_cache_file_exists(
    symbol,
    interval_str,
    date,
    market_type="spot",
    data_provider="BINANCE",
    chart_type="KLINES",
):
    """Check if a cache file exists for the given symbol, interval, and date.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        date: Date to check
        market_type: Market type (spot, futures_usdt, futures_coin)
        data_provider: Data provider (default: BINANCE)
        chart_type: Chart type (default: KLINES)

    Returns:
        Tuple of (exists, path)
    """
    cache_path = get_cache_path(symbol, interval_str, date, market_type, data_provider, chart_type)
    exists = cache_path.exists() and cache_path.stat().st_size > 0
    return exists, cache_path


def calculate_sha256(file_path):
    """Calculate SHA-256 hash of a file.

    Args:
        file_path: Path to the file

    Returns:
        SHA-256 hash of the file
    """
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        # Read the file in chunks to handle large files efficiently
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


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
        with open(checksum_file) as f:
            content = f.read().strip()
            # Split on whitespace and take first part (the checksum)
            expected = content.split()[0]
            content_length = len(content)
            preview_length = min(40, content_length)
            logger.debug(
                f"Raw checksum file content: '{content[:preview_length]}' (+ {content_length - preview_length} more chars, {content_length} total)"
            )
            logger.debug(f"Expected checksum: '{expected}'")

        # Calculate checksum of the zip file directly
        actual = calculate_sha256(data_file)
        logger.debug(f"Calculated checksum: '{actual}'")

        if actual != expected:
            # Log detailed error information
            logger.error(f"Checksum mismatch for {symbol} {interval_str} {date.strftime('%Y-%m-%d')}:")
            logger.error(f"Expected: '{expected}'")
            logger.error(f"Actual  : '{actual}'")

            # Record failure in the registry
            record_checksum_failure(symbol, interval_str, date, expected, actual, "skipped")
            return False

        logger.debug(f"Checksum verification successful for {symbol} {interval_str} {date.strftime('%Y-%m-%d')}")
        return True
    except Exception as e:
        logger.error(f"Error verifying checksum: {e}")
        # Record this as a failure also
        try:
            record_checksum_failure(symbol, interval_str, date, "unknown", "error", f"error: {e!s}")
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
            with open(failures_file) as f:
                failures = json.load(f)
        except Exception as e:
            logger.error(f"Error loading checksum failures registry: {e}")

    # Add new failure entry
    failures.append(
        {
            "symbol": symbol,
            "interval": interval_str,
            "date": (date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date)),
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
        with open(failures_file) as f:
            failures = json.load(f)

        # Filter failures for the specific symbol and interval
        matching_failures = [failure for failure in failures if failure["symbol"] == symbol and failure["interval"] == interval_str]

        # Return the dates
        return [failure["date"] for failure in matching_failures]
    except Exception as e:
        logger.error(f"Error retrieving failed checksum dates: {e}")
        return []


def process_date(
    symbol,
    interval_str,
    date,
    args,
    market_type="spot",
    data_provider="BINANCE",
    chart_type="KLINES",
):
    """Process a single date for a symbol and interval.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        date: Date to process
        args: Command line arguments
        market_type: Market type (spot, futures_usdt, futures_coin)
        data_provider: Data provider (default: BINANCE)
        chart_type: Chart type (default: KLINES)

    Returns:
        True if successful, False otherwise
    """
    if SHUTDOWN_STATE.is_shutdown_requested():
        logger.debug(f"Shutdown requested, skipping {symbol} {interval_str} {date}")
        return False

    # Check if date is in the future
    today = datetime.now().date()
    if date > today:
        logger.warning(f"Skipping future date {symbol} {interval_str} {date} (date > today)")
        return False

    # Check if date is current day
    is_current_day = date.year == today.year and date.month == today.month and date.day == today.day
    if is_current_day and not args.force_update:
        logger.warning(f"Skipping current-day data for {symbol} {interval_str} {date}")
        logger.warning("Current-day data may not be available yet from Binance Vision API")
        logger.warning("Use --force-update to attempt download anyway")
        return False

    logger.debug(f"Processing {symbol} {interval_str} {date}")

    # Skip if already exists and we're in incremental mode
    if args.incremental and not args.force_update and check_cache_file_exists(symbol, interval_str, date)[0]:
        logger.debug(f"Skipping {symbol} {interval_str} {date} (already exists in cache)")
        return True

    # Handle retry failed checksums mode
    if args.retry_failed_checksums:
        # Check if this date had a checksum failure
        failed_dates = get_failed_checksum_dates(symbol, interval_str)
        date_str = date.strftime("%Y-%m-%d")
        if date_str not in failed_dates:
            logger.debug(f"Skipping {symbol} {interval_str} {date} (no checksum failure)")
            return True
        logger.info(f"Retrying previously failed checksum for {symbol} {interval_str} {date}")

    # Attempt to download and process the data
    try:
        # Download data with checksum verification
        success, data, num_records = download_data_with_checksum(
            symbol,
            interval_str,
            date,
            skip_checksum=args.skip_checksum,
            proceed_on_failure=args.proceed_on_checksum_failure,
            market_type=market_type,
            data_provider=data_provider,
            chart_type=chart_type,
        )

        if success and data is not None:
            # Save to cache
            cache_path = get_cache_path(symbol, interval_str, date, market_type, data_provider, chart_type)
            cache_path.parent.mkdir(parents=True, exist_ok=True)

            # Convert to Arrow table and save
            table = pa.Table.from_pandas(data)
            with pa.OSFile(str(cache_path), "wb") as f:
                with pa.RecordBatchFileWriter(f, table.schema) as writer:
                    writer.write_table(table)

            # Get file size
            file_size = cache_path.stat().st_size

            # Update cache index
            update_cache_index(
                symbol,
                interval_str,
                date,
                file_size,
                num_records,
                market_type,
                data_provider,
                chart_type,
            )

            logger.debug(f"Saved {symbol} {interval_str} {date} to cache ({num_records} records, {file_size} bytes)")
            return True
        logger.warning(f"Failed to process {symbol} {interval_str} {date}")
        return False
    except Exception as e:
        logger.error(f"Error processing {symbol} {interval_str} {date}: {e}")
        return False


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


def cache_symbol_data(
    symbol,
    intervals,
    start_date,
    end_date,
    args,
    market_type="spot",
    data_provider="BINANCE",
    chart_type="KLINES",
):
    """Cache data for a symbol across multiple intervals and dates.

    Args:
        symbol: Symbol to cache
        intervals: List of intervals
        start_date: Start date
        end_date: End date
        args: Command line arguments
        market_type: Market type (spot, futures_usdt, futures_coin)
        data_provider: Data provider name
        chart_type: Chart type

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

        if SHUTDOWN_STATE.is_shutdown_requested():
            logger.warning(f"Shutdown requested, skipping interval {interval_str}")
            continue

        # Process each date with controlled parallelism
        with ThreadPoolExecutor(max_workers=min(len(dates), MAX_WORKERS)) as executor:
            futures = {
                executor.submit(
                    process_date,
                    symbol,
                    interval_str,
                    date,
                    args,
                    market_type,
                    data_provider,
                    chart_type,
                ): date
                for date in dates
            }

            for future in as_completed(futures):
                date = futures[future]
                try:
                    success = future.result()
                    if success:
                        interval_records += 1
                except Exception as e:
                    logger.error(f"Error processing {symbol} {interval_str} {date.strftime('%Y-%m-%d')}: {e}")

        interval_duration = time.time() - interval_start_time
        records_per_second = interval_records / interval_duration if interval_duration > 0 else 0

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
    """Set up argument parser."""
    parser = argparse.ArgumentParser(description="Arrow Cache Builder (Synchronous)")
    parser.add_argument("--symbols", help="Comma-separated list of symbols (e.g., BTCUSDT,ETHUSDT)")
    parser.add_argument(
        "--intervals",
        help="Comma-separated list of intervals (e.g., 1m,5m,1h)",
        default="5m",
    )
    parser.add_argument("--csv-file", help="Path to symbols CSV file")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)", required=True)
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)", required=True)
    parser.add_argument("--limit", help="Limit to N symbols", type=int)
    parser.add_argument("--debug", help="Enable debug logging", action="store_true")
    parser.add_argument("--skip-checksum", help="Skip checksum verification", action="store_true")
    parser.add_argument(
        "--proceed-on-checksum-failure",
        help="Proceed with caching even when checksum verification fails",
        action="store_true",
    )
    parser.add_argument(
        "--retry-failed-checksums",
        help="Retry downloading files with previously failed checksums",
        action="store_true",
    )
    parser.add_argument(
        "--incremental",
        help="Incremental update mode (only download missing data)",
        action="store_true",
    )
    parser.add_argument(
        "--detect-gaps",
        help="Detect and fill gaps in the cache",
        action="store_true",
    )
    parser.add_argument(
        "--force-update",
        help="Re-download data even if it exists in cache",
        action="store_true",
    )
    parser.add_argument(
        "--auto",
        help="Automatic mode (all symbols, determine dates, fill gaps)",
        action="store_true",
    )
    parser.add_argument(
        "--error-log",
        help="Path to file for logging errors, warnings, and critical messages",
        type=str,
    )
    parser.add_argument(
        "--market-type",
        help="Market type (spot, um, cm)",
        default="spot",
    )
    parser.add_argument(
        "--data-provider",
        help="Data provider (default: BINANCE)",
        default="BINANCE",
    )
    parser.add_argument(
        "--chart-type",
        help="Chart type (default: KLINES)",
        default="KLINES",
    )
    return parser


def main():
    """Main function."""
    # Set up signal handlers for graceful shutdown
    setup_signal_handlers()

    # Parse command line arguments
    parser = setup_argparse()
    args = parser.parse_args()

    # Configure logging
    if args.debug:
        logger.setLevel("DEBUG")
    else:
        logger.setLevel("INFO")

    # Configure error logging if specified
    if args.error_log:
        logger.enable_error_logging(args.error_log)
        logger.info(f"Error logging enabled to {args.error_log}")

    # Create cache directory if it doesn't exist
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    CHECKSUM_FAILURES_DIR.mkdir(parents=True, exist_ok=True)

    # Log a test warning and error to verify error logging
    if args.error_log:
        logger.debug("Debug message - should not appear in error log")
        logger.info("Info message - should not appear in error log")
        logger.warning("Test warning - should appear in error log")
        logger.error("Test error - should appear in error log")

    # Initialize cache database
    initialize_cache_db()

    logger.info("Arrow Cache Builder started")

    # Auto mode - determine start and end dates
    if args.auto:
        logger.info("Running in automatic mode")
        args.incremental = True
        args.detect_gaps = True

    # Parse dates
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    # Load symbols from CSV if provided
    symbols_data = []
    if args.csv_file:
        symbols_data = parse_symbols_csv(args.csv_file, args.limit)
        if not symbols_data:
            logger.error("No symbols found in CSV file or error parsing CSV")
            return 1

    # Get market type, data provider, and chart type from args
    market_type = args.market_type
    data_provider = args.data_provider
    chart_type = args.chart_type

    # Process symbols
    if args.symbols:
        # User specified symbols directly
        symbols = args.symbols.split(",")
        intervals = args.intervals.split(",")

        # Process each symbol
        for symbol in symbols:
            if args.detect_gaps:
                # Get first and last dates in cache
                first_date = get_first_date_in_cache(symbol, intervals[0])
                last_date = get_last_date_in_cache(symbol, intervals[0])

                if first_date is not None and first_date > start_date:
                    # Fill gap at the beginning
                    logger.info(f"Filling gap for {symbol} from {start_date} to {first_date}")
                    cache_symbol_data(
                        symbol,
                        intervals,
                        start_date,
                        first_date - timedelta(days=1),
                        args,
                        market_type,
                        data_provider,
                        chart_type,
                    )

                if last_date is not None and last_date < end_date:
                    # Fill gap at the end
                    logger.info(f"Filling gap for {symbol} from {last_date} to {end_date}")
                    cache_symbol_data(
                        symbol,
                        intervals,
                        last_date + timedelta(days=1),
                        end_date,
                        args,
                        market_type,
                        data_provider,
                        chart_type,
                    )

                # Find and fill internal gaps
                for interval in intervals:
                    missing_dates = detect_cache_gaps(symbol, interval, start_date, end_date)
                    if missing_dates and len(missing_dates) > 0:
                        logger.info(f"Filling {len(missing_dates)} internal gaps for {symbol}/{interval}")
                        for date in missing_dates:
                            process_date(
                                symbol,
                                interval,
                                date,
                                args,
                                market_type,
                                data_provider,
                                chart_type,
                            )
            else:
                # Normal processing
                cache_symbol_data(
                    symbol,
                    intervals,
                    start_date,
                    end_date,
                    args,
                    market_type,
                    data_provider,
                    chart_type,
                )
    else:
        # Use symbols from CSV
        for symbol_info in symbols_data:
            symbol = symbol_info["symbol"]

            # Get intervals for this symbol
            if args.intervals:
                # User specified intervals
                intervals = args.intervals.split(",")
                # Filter to only include intervals available for this symbol
                intervals = [i for i in intervals if i in symbol_info["available_intervals"]]
            else:
                # Use all available intervals for this symbol
                intervals = symbol_info["available_intervals"]

            if not intervals:
                logger.warning(f"No valid intervals found for {symbol}, skipping")
                continue

            # Process symbol with available intervals
            if args.detect_gaps:
                # Process each interval separately for gap detection
                for interval in intervals:
                    first_date = get_first_date_in_cache(symbol, interval)
                    last_date = get_last_date_in_cache(symbol, interval)

                    if first_date is not None and first_date > start_date:
                        # Fill gap at the beginning
                        logger.info(f"Filling gap for {symbol}/{interval} from {start_date} to {first_date}")
                        earliest_date = datetime.strptime(
                            symbol_info.get("earliest_date", args.start_date),
                            "%Y-%m-%d",
                        ).date()
                        cache_start = max(earliest_date, start_date)
                        cache_symbol_data(
                            symbol,
                            [interval],
                            cache_start,
                            first_date - timedelta(days=1),
                            args,
                            market_type,
                            data_provider,
                            chart_type,
                        )

                    if last_date is not None and last_date < end_date:
                        # Fill gap at the end
                        logger.info(f"Filling gap for {symbol}/{interval} from {last_date} to {end_date}")
                        cache_symbol_data(
                            symbol,
                            [interval],
                            last_date + timedelta(days=1),
                            end_date,
                            args,
                            market_type,
                            data_provider,
                            chart_type,
                        )

                    # Find and fill internal gaps
                    missing_dates = detect_cache_gaps(symbol, interval, start_date, end_date)
                    if missing_dates and len(missing_dates) > 0:
                        logger.info(f"Filling {len(missing_dates)} internal gaps for {symbol}/{interval}")
                        for date in missing_dates:
                            process_date(
                                symbol,
                                interval,
                                date,
                                args,
                                market_type,
                                data_provider,
                                chart_type,
                            )
            else:
                # Get earliest date for this symbol
                earliest_date = datetime.strptime(symbol_info.get("earliest_date", args.start_date), "%Y-%m-%d").date()

                # Use the later of earliest_date and start_date
                cache_start = max(earliest_date, start_date)

                # Normal processing
                cache_symbol_data(
                    symbol,
                    intervals,
                    cache_start,
                    end_date,
                    args,
                    market_type,
                    data_provider,
                    chart_type,
                )

    logger.info("Arrow Cache Builder completed")
    return 0


# Function to check if we should process a date in incremental mode
def should_process_date(symbol, interval_str, date, args):
    """Check if we should process a date based on incremental mode and force update.

    Args:
        symbol: Symbol name
        interval_str: Interval string
        date: Date to check
        args: Command line arguments

    Returns:
        True if we should process this date, False otherwise
    """
    # Always process if force update is enabled
    if args.force_update:
        return True

    # Skip if the file exists and we're in incremental mode
    if args.incremental and check_cache_file_exists(symbol, interval_str, date):
        return False

    # Process all other cases
    return True


if __name__ == "__main__":
    sys.exit(main())
