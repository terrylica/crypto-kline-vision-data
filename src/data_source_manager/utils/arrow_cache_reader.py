#!/usr/bin/env python
# polars-exception: ArrowCacheReader returns pandas DataFrames for
# compatibility with existing consumers that expect pandas
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""Arrow Cache Reader - Utility for reading from the Arrow Cache database.

This module provides a class for interacting with the Arrow Cache SQLite database
and reading data from the Arrow cache files. It's designed to be used by external
modules that need to efficiently determine what data is available in the cache.

Example usage:

    # Check if data is available in cache
    cache_reader = ArrowCacheReader()
    availability = cache_reader.check_availability(
        symbol="BTCUSDT",
        interval=Interval.MINUTE_5,
        market_type=MarketType.SPOT,
        start_date="2023-01-01",
        end_date="2023-01-10"
    )

    # Read data directly from cache
    df = cache_reader.read_symbol_data(
        symbol="BTCUSDT",
        interval=Interval.MINUTE_5,
        market_type=MarketType.SPOT,
        start_date="2023-01-01",
        end_date="2023-01-10"
    )
"""

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
from rich import print

from data_source_manager.utils.config import MAX_PREVIEW_ITEMS, MIN_FILES_FOR_README
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import ChartType, DataProvider, Interval, MarketType


class ArrowCacheReader:
    """Class for reading from the Arrow Cache database."""

    def __init__(
        self,
        cache_db_path: str | Path = "./logs/cache_index.db",
        data_provider: DataProvider = DataProvider.BINANCE,
        chart_type: ChartType = ChartType.KLINES,
    ) -> None:
        """Initialize the Arrow Cache Reader.

        Args:
            cache_db_path: Path to the SQLite database
            data_provider: Data provider (default: DataProvider.BINANCE)
            chart_type: Chart type (default: ChartType.KLINES)
        """
        self.cache_db_path = Path(cache_db_path)
        self.data_provider = data_provider
        self.chart_type = chart_type

    def _get_connection(self) -> sqlite3.Connection:
        """Get a connection to the SQLite database.

        Returns:
            SQLite connection object

        Raises:
            FileNotFoundError: If the cache database doesn't exist
        """
        if not self.cache_db_path.exists():
            raise FileNotFoundError(f"Cache database not found at {self.cache_db_path}")

        return sqlite3.connect(self.cache_db_path)

    def _get_cache_path_components(
        self,
        symbol: str,
        interval: str | Interval,
        market_type: MarketType | None = MarketType.SPOT,
    ) -> tuple[str, str]:
        """Get the cache path components for a symbol and interval.

        Args:
            symbol: Trading pair symbol
            interval: Time interval (Interval enum or string)
            market_type: Market type (default: MarketType.SPOT)

        Returns:
            Tuple of (provider_str, path_pattern) for finding files in the database
        """
        # Convert interval to string if it's an Interval enum
        interval_str = interval.value if isinstance(interval, Interval) else str(interval)

        # Format: BINANCE/KLINES/{market_type}/{symbol}/{interval}/
        provider_str = self.data_provider.name
        chart_type_str = self.chart_type.vision_api_path
        market_type_str = market_type.vision_api_path.replace("/", "_")

        path_pattern = f"cache/{provider_str}/{chart_type_str}/{market_type_str}/{symbol}/{interval_str}/%"

        return provider_str, path_pattern

    def check_availability(
        self,
        symbol: str,
        interval: str | Interval,
        start_date: str | datetime,
        end_date: str | datetime,
        market_type: MarketType = MarketType.SPOT,
    ) -> dict[str, Any]:
        """Check if data is available in cache for the given parameters.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Time interval (e.g., "5m" or Interval.MINUTE_5)
            start_date: Start date as datetime or string (YYYY-MM-DD)
            end_date: End date as datetime or string (YYYY-MM-DD)
            market_type: Market type (default: MarketType.SPOT)

        Returns:
            dict: Information about cache availability
                - available_dates: List of dates with data
                - missing_dates: List of dates without data
                - coverage_percentage: Percentage of requested dates available
                - total_records: Total number of records available
                - paths: Dict mapping dates to file paths
        """
        # Convert interval to string if it's an Interval enum
        interval_str = interval.value if isinstance(interval, Interval) else str(interval)

        # Convert dates to strings if needed
        if isinstance(start_date, datetime):
            start_date = start_date.strftime("%Y-%m-%d")
        if isinstance(end_date, datetime):
            end_date = end_date.strftime("%Y-%m-%d")

        # Generate list of all dates in the range (PERF401 optimization)
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        days_count = (end_dt - start_dt).days + 1
        all_dates = [(start_dt + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days_count)]

        # Query the database for available dates
        conn = self._get_connection()
        cursor = conn.cursor()

        if all_dates:
            placeholders = ", ".join(["?"] * len(all_dates))

            # Get the path pattern for this type of data
            _, path_pattern = self._get_cache_path_components(symbol, interval_str, market_type)

            cursor.execute(
                f"""
                SELECT date, num_records, path
                FROM cache_entries
                WHERE symbol = ? AND interval = ? AND date IN ({placeholders})
                AND path LIKE ?
                """,
                [symbol, interval_str, *all_dates, path_pattern],
            )

            results = cursor.fetchall()
        else:
            results = []

        conn.close()

        # Process results
        available_dates = [row[0] for row in results]
        missing_dates = [date for date in all_dates if date not in available_dates]
        total_records = sum(row[1] for row in results)

        coverage = 0 if not all_dates else (len(available_dates) / len(all_dates)) * 100

        return {
            "available_dates": available_dates,
            "missing_dates": missing_dates,
            "coverage_percentage": coverage,
            "total_records": total_records,
            "paths": {row[0]: row[2] for row in results},
        }

    def get_file_path(
        self,
        symbol: str,
        interval: str | Interval,
        date: str | datetime,
        market_type: MarketType = MarketType.SPOT,
    ) -> str | None:
        """Get the path to an Arrow file in the cache.

        Args:
            symbol: Trading pair symbol
            interval: Time interval (Interval enum or string)
            date: Date as string (YYYY-MM-DD) or datetime
            market_type: Market type (default: MarketType.SPOT)

        Returns:
            Path to the Arrow file or None if not in cache
        """
        # Convert interval to string if it's an Interval enum
        interval_str = interval.value if isinstance(interval, Interval) else str(interval)

        if isinstance(date, datetime):
            date = date.strftime("%Y-%m-%d")

        conn = self._get_connection()
        cursor = conn.cursor()

        # Get the path pattern for this type of data
        _, path_pattern = self._get_cache_path_components(symbol, interval_str, market_type)

        cursor.execute(
            """
            SELECT path FROM cache_entries
            WHERE symbol = ? AND interval = ? AND date = ? AND path LIKE ?
            """,
            (symbol, interval_str, date, path_pattern),
        )

        result = cursor.fetchone()
        conn.close()

        return result[0] if result else None

    def read_arrow_file(self, file_path: str | Path) -> pd.DataFrame:
        """Read an Arrow file from the cache.

        Args:
            file_path: Path to the Arrow file

        Returns:
            pandas DataFrame with the data

        Raises:
            FileNotFoundError: If the arrow file doesn't exist
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Arrow file not found: {file_path}")

        try:
            with pa.OSFile(str(path), "rb") as f:
                reader = pa.RecordBatchFileReader(f)
                table = reader.read_all()

            df = table.to_pandas()

            # Set index if needed
            if "open_time" in df.columns:
                # Ensure timezone aware
                if df["open_time"].dt.tz is None:
                    df["open_time"] = df["open_time"].dt.tz_localize("UTC")
                df = df.set_index("open_time")

            return df
        except (OSError, pa.ArrowInvalid, pa.ArrowIOError, ValueError) as e:
            logger.error(f"Error reading arrow file {file_path}: {e}")
            raise

    def read_symbol_data(
        self,
        symbol: str,
        interval: str | Interval,
        start_date: str | datetime,
        end_date: str | datetime,
        market_type: MarketType = MarketType.SPOT,
    ) -> pd.DataFrame:
        """Read all available data for a symbol within a date range.

        Args:
            symbol: Trading pair symbol
            interval: Time interval (Interval enum or string)
            start_date: Start date (datetime or string)
            end_date: End date (datetime or string)
            market_type: Market type (default: MarketType.SPOT)

        Returns:
            pandas DataFrame with combined data
        """
        # Check what's available
        availability = self.check_availability(symbol, interval, start_date, end_date, market_type)

        if not availability["available_dates"]:
            logger.debug(f"No data available in cache for {symbol} {interval} {market_type.name} from {start_date} to {end_date}")
            return pd.DataFrame()

        logger.debug(
            f"Found {len(availability['available_dates'])}/{len(availability['available_dates']) + len(availability['missing_dates'])} "
            f"dates in cache for {symbol} {interval} {market_type.name} ({availability['coverage_percentage']:.1f}% coverage)"
        )

        # Read and combine data
        dfs = []
        for date in availability["available_dates"]:
            file_path = availability["paths"][date]
            try:
                df = self.read_arrow_file(file_path)
                if not df.empty:
                    dfs.append(df)
            except (OSError, pa.ArrowInvalid, pa.ArrowIOError, ValueError) as e:
                logger.error(f"Error reading file for {date}: {e}")

        if not dfs:
            return pd.DataFrame()

        # Combine data and sort by index
        combined_df = pd.concat(dfs).sort_index()
        logger.debug(f"Read {len(combined_df)} records from cache for {symbol} {interval} {market_type.name}")
        return combined_df

    def get_cache_statistics(self) -> dict[str, Any]:
        """Get statistics about the cache.

        Returns:
            dict: Statistics about the cache
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Last update time
        cursor.execute("SELECT value FROM cache_metadata WHERE key = 'last_update'")
        result = cursor.fetchone()
        last_update = result[0] if result else "Unknown"

        # Total number of entries
        cursor.execute("SELECT COUNT(*) FROM cache_entries")
        total_entries = cursor.fetchone()[0]

        # Total number of symbols
        cursor.execute("SELECT COUNT(DISTINCT symbol) FROM cache_entries")
        total_symbols = cursor.fetchone()[0]

        # Total number of intervals
        cursor.execute("SELECT COUNT(DISTINCT interval) FROM cache_entries")
        total_intervals = cursor.fetchone()[0]

        # Count by market type (inferred from path)
        cursor.execute(
            """
            SELECT
                CASE
                    WHEN path LIKE '%/spot/%' THEN 'SPOT'
                    WHEN path LIKE '%/futures_um/%' THEN 'FUTURES_USDT'
                    WHEN path LIKE '%/futures_cm/%' THEN 'FUTURES_COIN'
                    ELSE 'OTHER'
                END as market_type,
                COUNT(*) as count
            FROM cache_entries
            GROUP BY market_type
        """
        )
        market_counts = dict(cursor.fetchall())

        # Total storage size
        cursor.execute("SELECT SUM(file_size) FROM cache_entries")
        result = cursor.fetchone()
        total_size = result[0] or 0

        # Total records
        cursor.execute("SELECT SUM(num_records) FROM cache_entries")
        result = cursor.fetchone()
        total_records = result[0] or 0

        conn.close()

        return {
            "last_update": last_update,
            "total_entries": total_entries,
            "total_symbols": total_symbols,
            "total_intervals": total_intervals,
            "market_type_counts": market_counts,
            "total_size_bytes": total_size,
            "total_size_mb": total_size / (1024 * 1024),
            "total_records": total_records,
        }

    def list_available_symbols(self, market_type: MarketType | None = None) -> list[str]:
        """Get a list of all symbols available in the cache.

        Args:
            market_type: Optional market type to filter by

        Returns:
            List of symbol strings
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if market_type:
            # Get pattern based on market type
            market_type_pattern = f"%/{market_type.vision_api_path.replace('/', '_')}/%"
            cursor.execute(
                "SELECT DISTINCT symbol FROM cache_entries WHERE path LIKE ?",
                (market_type_pattern,),
            )
        else:
            cursor.execute("SELECT DISTINCT symbol FROM cache_entries")

        symbols = [row[0] for row in cursor.fetchall()]
        conn.close()
        return symbols

    def list_available_intervals(self, symbol: str | None = None, market_type: MarketType | None = None) -> list[str]:
        """Get a list of all intervals available in the cache.

        Args:
            symbol: Optional symbol to filter intervals by
            market_type: Optional market type to filter by

        Returns:
            List of interval strings
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        query_parts = []
        params = []

        if symbol:
            query_parts.append("symbol = ?")
            params.append(symbol)

        if market_type:
            # Get pattern based on market type
            market_type_pattern = f"%/{market_type.vision_api_path.replace('/', '_')}/%"
            query_parts.append("path LIKE ?")
            params.append(market_type_pattern)

        if query_parts:
            query = f"SELECT DISTINCT interval FROM cache_entries WHERE {' AND '.join(query_parts)}"
            cursor.execute(query, params)
        else:
            cursor.execute("SELECT DISTINCT interval FROM cache_entries")

        intervals = [row[0] for row in cursor.fetchall()]
        conn.close()
        return intervals

    def list_available_dates(
        self,
        symbol: str,
        interval: str | Interval,
        market_type: MarketType = MarketType.SPOT,
    ) -> list[str]:
        """Get a list of all dates available for a symbol and interval.

        Args:
            symbol: Trading pair symbol
            interval: Time interval (Interval enum or string)
            market_type: Market type (default: MarketType.SPOT)

        Returns:
            List of date strings (YYYY-MM-DD)
        """
        # Convert interval to string if it's an Interval enum
        interval_str = interval.value if isinstance(interval, Interval) else str(interval)

        conn = self._get_connection()
        cursor = conn.cursor()

        # Get the path pattern for this type of data
        _, path_pattern = self._get_cache_path_components(symbol, interval_str, market_type)

        cursor.execute(
            """
            SELECT date FROM cache_entries
            WHERE symbol = ? AND interval = ? AND path LIKE ?
            ORDER BY date
            """,
            (symbol, interval_str, path_pattern),
        )

        dates = [row[0] for row in cursor.fetchall()]
        conn.close()
        return dates

    def list_available_market_types(self) -> list[str]:
        """Get a list of all market types available in the cache.

        Returns:
            List of market type strings
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT DISTINCT
                CASE
                    WHEN path LIKE '%/spot/%' THEN 'SPOT'
                    WHEN path LIKE '%/futures_um/%' THEN 'FUTURES_USDT'
                    WHEN path LIKE '%/futures_cm/%' THEN 'FUTURES_COIN'
                    ELSE path
                END as market_type
            FROM cache_entries
        """
        )

        market_types = [row[0] for row in cursor.fetchall()]
        conn.close()
        return market_types


if __name__ == "__main__":
    # Example usage
    cache_reader = ArrowCacheReader()

    # Print cache statistics
    stats = cache_reader.get_cache_statistics()
    print("[bold blue]Arrow Cache Statistics:[/bold blue]")
    for key, value in stats.items():
        print(f"{key}: {value}")

    # List available market types
    market_types = cache_reader.list_available_market_types()
    print(f"\n[bold green]Available market types:[/bold green] {', '.join(market_types)}")

    # List available symbols
    symbols = cache_reader.list_available_symbols()
    if symbols:
        print(f"\n[bold green]Available symbols:[/bold green] {', '.join(symbols[:MAX_PREVIEW_ITEMS])}")
        if len(symbols) > MAX_PREVIEW_ITEMS:
            print(f"...and {len(symbols) - MAX_PREVIEW_ITEMS} more")

        # Example with the first symbol
        symbol = symbols[0]
        intervals = cache_reader.list_available_intervals(symbol)
        print(f"\n[bold green]Available intervals for {symbol}:[/bold green] {', '.join(intervals)}")

        if intervals:
            interval = intervals[0]

            # Try to convert string interval to Interval enum for better code example
            try:
                from data_source_manager.utils.market_constraints import Interval

                interval_enum = next(i for i in Interval if i.value == interval)
                print(f"Interval string '{interval}' converted to enum: {interval_enum}")
            except (StopIteration, ImportError):
                interval_enum = interval
                print(f"Using interval as string: {interval}")

            dates = cache_reader.list_available_dates(symbol, interval_enum)

            if dates:
                print(f"\n[bold green]Available dates for {symbol} {interval}:[/bold green]")
                print(f"First date: {dates[0]}")
                print(f"Last date: {dates[-1]}")
                print(f"Total dates: {len(dates)}")

                # Check availability for a date range
                if len(dates) >= MIN_FILES_FOR_README:
                    start_date = dates[0]
                    end_date = dates[-1]
                    availability = cache_reader.check_availability(symbol, interval_enum, start_date, end_date)

                    print(f"\n[bold green]Availability for {symbol} {interval} from {start_date} to {end_date}:[/bold green]")
                    print(f"Coverage: {availability['coverage_percentage']:.1f}%")
                    print(f"Total records: {availability['total_records']}")

                    # Load some data as an example
                    print("\n[bold green]Loading sample data...[/bold green]")
                    sample_start = dates[0]
                    sample_end = min(dates[MAX_PREVIEW_ITEMS], dates[-1]) if len(dates) > MAX_PREVIEW_ITEMS else dates[-1]
                    df = cache_reader.read_symbol_data(symbol, interval_enum, sample_start, sample_end)
                    print(f"Loaded {len(df)} records from {sample_start} to {sample_end}")
                    if not df.empty:
                        print("\nSample data:")
                        print(df.head(5))
    else:
        print("[bold yellow]No data available in cache[/bold yellow]")
