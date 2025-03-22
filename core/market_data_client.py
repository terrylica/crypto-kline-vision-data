#!/usr/bin/env python

"""Unified market data client with optimized 1-second data handling."""

import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any, Set, Union
import pandas as pd
import numpy as np
from utils.logger_setup import get_logger
from utils.market_constraints import Interval, MarketType, get_market_capabilities
from utils.time_alignment import (
    get_bar_close_time,
    get_interval_floor,
    is_bar_complete,
    TimeRangeManager,
)
from utils.hardware_monitor import HardwareMonitor
from utils.validation import DataValidation
from utils.http_client_factory import create_aiohttp_client
from utils.config import (
    KLINE_COLUMNS,
    standardize_column_names,
    TIMESTAMP_UNIT,
    CLOSE_TIME_ADJUSTMENT,
    CANONICAL_INDEX_NAME,
)

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


def process_kline_data(raw_data: List[List]) -> pd.DataFrame:
    """Process raw kline data into a DataFrame.

    Args:
        raw_data: List of kline data from Binance API

    Returns:
        Processed DataFrame
    """
    if not raw_data:
        return pd.DataFrame()

    # Use centralized column definitions
    df = pd.DataFrame(raw_data, columns=pd.Index(KLINE_COLUMNS))

    # Add DEBUG logging for timestamp conversion
    logger.debug("\n=== Timestamp Conversion Debug ===")
    if len(raw_data) > 0:
        logger.debug(f"Sample raw close_time: {raw_data[0][6]}")
        logger.debug(f"Number of digits: {len(str(raw_data[0][6]))}")

    # Convert timestamps with microsecond precision
    for col in ["open_time", "close_time"]:
        # Convert milliseconds to microseconds by multiplying by 1000
        df[col] = df[col].astype(np.int64) * 1000
        df[col] = pd.to_datetime(df[col], unit=TIMESTAMP_UNIT, utc=True)

        # For close_time, add microseconds to match REST API behavior
        if col == "close_time":
            df[col] = df[col] + pd.Timedelta(microseconds=CLOSE_TIME_ADJUSTMENT)

        if len(raw_data) > 0:
            logger.debug(f"Converted {col}: {df[col].iloc[0]}")
            logger.debug(f"{col} microseconds: {df[col].iloc[0].microsecond}")

    # Convert numeric columns efficiently
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
    df[numeric_cols] = df[numeric_cols].astype(np.float64)
    df["trades"] = df["trades"].astype(np.int32)

    # Standardize column names using centralized function
    df = standardize_column_names(df)

    # Check for duplicate timestamps and sort by open_time
    if "open_time" in df.columns:
        logger.debug(f"Shape before dropping duplicates: {df.shape}")

        # First, sort by open_time to ensure chronological order
        df = df.sort_values("open_time")

        # Then check for duplicates and drop them if necessary
        if df.duplicated(subset=["open_time"]).any():
            duplicates_count = df.duplicated(subset=["open_time"]).sum()
            logger.debug(
                f"Found {duplicates_count} duplicate timestamps, keeping first occurrence"
            )
            df = df.drop_duplicates(subset=["open_time"], keep="first")

        logger.debug(f"Shape after sorting and dropping duplicates: {df.shape}")
        logger.debug(
            f"open_time is monotonic: {df['open_time'].is_monotonic_increasing}"
        )

    # Save close_time and open_time before setting the index
    close_time_values = None
    open_time_values = None
    if "close_time" in df.columns:
        close_time_values = df["close_time"].copy()
    if "open_time" in df.columns:
        open_time_values = df["open_time"].copy()

    # Set the index to open_time and ensure it has the canonical name
    if "open_time" in df.columns:
        df = df.set_index("open_time")
        df.index.name = CANONICAL_INDEX_NAME

    # Always ensure close_time column exists
    if "close_time" not in df.columns:
        if close_time_values is not None:
            # Restore from saved values if available
            df["close_time"] = close_time_values
        else:
            # Calculate close_time based on open_time if we don't have the original values
            logger.debug("Calculating close_time from index values")
            df["close_time"] = (
                pd.Series(df.index.to_numpy(), index=df.index)
                + pd.Timedelta(seconds=1)
                - pd.Timedelta(microseconds=1)
            )

    return df


class EnhancedRetriever:
    """Unified data retriever optimized for 1-second data."""

    CHUNK_SIZE = 1000  # Maximum records per request allowed by Binance API
    MAX_RETRIES = 3  # Maximum number of retries for failed requests
    RETRY_DELAY = 1  # Delay in seconds between retries

    def __init__(
        self,
        market_type: MarketType = MarketType.SPOT,
        client: Optional[aiohttp.ClientSession] = None,
        hw_monitor: Optional[HardwareMonitor] = None,
    ):
        """Initialize the retriever.

        Args:
            market_type: Type of market (SPOT only for 1-second data)
            client: Optional pre-configured HTTP client
            hw_monitor: Optional hardware monitor instance
        """
        self.market_type = market_type
        self.client = client
        self._capabilities = get_market_capabilities(market_type)
        self._hw_monitor = hw_monitor or HardwareMonitor()
        self._endpoint_index = 0  # For round-robin endpoint selection
        self._endpoint_lock = asyncio.Lock()  # For thread-safe endpoint selection

        # Validate market capabilities
        if market_type != MarketType.SPOT:
            raise ValueError("Only SPOT market type supports 1-second data")
        if self._capabilities.max_limit != self.CHUNK_SIZE:
            raise ValueError(
                f"API limit {self._capabilities.max_limit} doesn't match CHUNK_SIZE {self.CHUNK_SIZE}"
            )

    async def __aenter__(self):
        """Async context manager entry."""
        if not self.client:
            self.client = self._create_optimized_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.client:
            await self.client.close()
            self.client = None

    async def _get_next_endpoint(self) -> str:
        """Get next endpoint in round-robin fashion."""
        async with self._endpoint_lock:
            # Combine all available endpoints
            all_endpoints = (
                [self._capabilities.primary_endpoint]
                + self._capabilities.backup_endpoints
                + (
                    [self._capabilities.data_only_endpoint]
                    if self._capabilities.data_only_endpoint
                    else []
                )
            )

            endpoint = all_endpoints[self._endpoint_index]
            self._endpoint_index = (self._endpoint_index + 1) % len(all_endpoints)

            # Use the correct endpoint format
            return f"{endpoint}/api/v3/klines"

    async def _fetch_chunk_with_retry(
        self,
        symbol: str,
        interval: Interval,
        start_ms: int,
        end_ms: int,
        sem: asyncio.Semaphore,
    ) -> Tuple[List[List[Any]], str]:
        """Fetch a single chunk with retries and endpoint failover.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            start_ms: Start time in milliseconds
            end_ms: End time in milliseconds
            sem: Semaphore for concurrency control

        Returns:
            Tuple of (chunk data, endpoint used)
        """
        retries = 0
        last_error = None

        # Log chunk boundary details
        start_time = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        end_time = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
        logger.debug(  # Changed from info to debug
            f"\n=== Chunk Details ===\n"
            f"Symbol: {symbol}\n"
            f"Interval: {interval.value}\n"
            f"Start time: {start_time} ({start_ms})\n"
            f"End time: {end_time} ({end_ms})\n"
            f"Time window: {end_time - start_time}"
        )

        while retries < self.MAX_RETRIES:
            try:
                async with sem:
                    endpoint_url = await self._get_next_endpoint()
                    params = {
                        "symbol": symbol,
                        "interval": interval.value,
                        "startTime": start_ms,
                        "endTime": end_ms,
                        "limit": self.CHUNK_SIZE,
                    }

                    logger.debug(
                        f"Fetching chunk: {start_ms} -> {end_ms} from {endpoint_url}"
                    )
                    async with self.client.get(endpoint_url, params=params) as response:
                        response.raise_for_status()
                        data: List[List[Any]] = await response.json()

                        # Validate and log response
                        if not isinstance(data, list):
                            logger.warning(
                                f"Unexpected response format from {endpoint_url}: {data}"
                            )
                            raise ValueError(
                                f"Expected list response, got {type(data)}"
                            )

                        logger.debug(
                            f"Received {len(data)} records"
                        )  # Changed from info to debug
                        if data:
                            first_ts = datetime.fromtimestamp(
                                int(data[0][0]) / 1000, tz=timezone.utc
                            )
                            last_ts = datetime.fromtimestamp(
                                int(data[-1][0]) / 1000, tz=timezone.utc
                            )
                            logger.debug(
                                f"First timestamp: {first_ts}\nLast timestamp: {last_ts}\nTime span: {last_ts - first_ts}"
                            )

                        return data, endpoint_url

            except (aiohttp.ClientError, ValueError) as e:
                last_error = e
                logger.warning(f"Failed to fetch chunk from {endpoint_url}: {str(e)}")
                retries += 1
                if retries < self.MAX_RETRIES:
                    await asyncio.sleep(self.RETRY_DELAY)
                continue

        logger.error(
            f"Failed to fetch chunk after {self.MAX_RETRIES} retries: {str(last_error)}"
        )
        raise last_error or Exception("Failed to fetch chunk after all retries")

    def _validate_request_params(
        self, symbol: str, interval: Interval, start_time: datetime, end_time: datetime
    ) -> None:
        """Validate request parameters using utility functions.

        Args:
            symbol: Trading pair symbol
            interval: Time interval for data
            start_time: Start time
            end_time: End time
        """
        # Use validation utilities through TimeRangeManager
        TimeRangeManager.validate_dates(start_time, end_time)
        DataValidation.validate_interval(interval.value, self.market_type.name)
        DataValidation.validate_symbol_format(symbol, self.market_type.name)

    def _create_optimized_client(self) -> aiohttp.ClientSession:
        """Create an optimized client based on hardware capabilities."""
        concurrency_info = self._hw_monitor.calculate_optimal_concurrency()
        return create_aiohttp_client(
            max_connections=concurrency_info["optimal_concurrency"],
            timeout=30,  # Increased for large datasets
        )

    def _calculate_chunks(
        self, start_ms: int, end_ms: int, interval: Interval
    ) -> List[Tuple[int, int]]:
        """Calculate optimal chunk sizes for the time window.

        Args:
            start_ms: Start timestamp in milliseconds
            end_ms: End timestamp in milliseconds
            interval: Time interval for data

        Returns:
            List of (start_ms, end_ms) tuples for each chunk
        """
        # Count available endpoints
        endpoint_count = (
            1
            + len(self._capabilities.backup_endpoints)
            + (1 if self._capabilities.data_only_endpoint else 0)  # Primary endpoint
        )

        # Calculate total records needed
        interval_ms = interval.to_seconds() * 1000  # Convert interval to milliseconds
        total_ms = end_ms - start_ms
        total_records = total_ms // interval_ms

        # Determine number of chunks:
        # - At least as many chunks as endpoints to maximize endpoint usage
        # - But no more chunks than records (can't split a record)
        # - And no chunk larger than max_limit
        min_chunks = min(endpoint_count, total_records)
        max_chunks_by_limit = (
            total_records + self._capabilities.max_limit - 1
        ) // self._capabilities.max_limit
        num_chunks = max(min_chunks, max_chunks_by_limit)

        # Calculate chunk size in milliseconds, ensuring it's a multiple of the interval
        chunk_size_ms = (total_ms // num_chunks) // interval_ms * interval_ms
        if chunk_size_ms == 0:
            chunk_size_ms = interval_ms  # Ensure minimum chunk size is one interval

        logger.debug(
            f"Chunking {total_records} records into {num_chunks} chunks "
            f"(using {endpoint_count} endpoints, chunk_size={chunk_size_ms}ms, interval={interval_ms}ms)"
        )

        # Create chunks with consistent boundary handling
        chunks = []
        current_ms = start_ms
        while current_ms < end_ms:
            chunk_end = min(current_ms + chunk_size_ms, end_ms)
            if chunk_end > current_ms:
                # Use exact boundary (no -1ms subtraction) for consistent exclusive end
                chunks.append((current_ms, chunk_end))
            current_ms = chunk_end

        # Log chunk boundaries for verification
        for i, (chunk_start, chunk_end) in enumerate(chunks):
            start_time = datetime.fromtimestamp(chunk_start / 1000, tz=timezone.utc)
            end_time = datetime.fromtimestamp(
                chunk_end / 1000, tz=timezone.utc
            )  # No adjustment needed for logging
            logger.debug(
                f"Chunk {i + 1}/{len(chunks)}: {start_time} -> {end_time} "
                f"({(chunk_end - chunk_start)/interval_ms:.0f} intervals, end exclusive)"
            )

        return chunks

    def _validate_bar_duration(self, open_time: datetime, interval: Interval) -> float:
        """Validate a single bar's duration.

        Args:
            open_time: Bar's open time
            interval: Time interval

        Returns:
            Bar duration in seconds
        """
        close_time = get_bar_close_time(open_time, interval)
        duration = (close_time - open_time).total_seconds()
        expected_duration = interval.to_seconds()

        # Allow 1ms tolerance
        if abs(duration - expected_duration) > 0.001:
            logger.warning(
                f"Irregular bar duration at {open_time}: {duration}s (expected {expected_duration}s)"
            )
        return duration

    def _validate_historical_bars(
        self, df: pd.DataFrame, current_time: datetime
    ) -> int:
        """Validate historical bar completion.

        Args:
            df: DataFrame with market data
            current_time: Current time for validation

        Returns:
            Number of incomplete historical bars
        """
        cutoff_time = current_time - timedelta(minutes=5)
        incomplete_count = 0
        for ts in df["open_time"]:
            if ts < cutoff_time and not is_bar_complete(ts, current_time):
                logger.warning(f"Found incomplete historical bar at {ts}")
                incomplete_count += 1
        return incomplete_count

    def _validate_bar_alignment(self, df: pd.DataFrame, interval: Interval) -> None:
        """Validate bar alignment and completeness.

        Args:
            df: DataFrame with market data
            interval: Time interval
        """
        if df.empty:
            return

        # Check bar durations
        for idx, row in df.iterrows():
            open_time = row["open_time"]
            close_time = row["close_time"]
            expected_close = get_bar_close_time(open_time, interval)

            if close_time != expected_close:
                logger.warning(
                    f"Bar at {open_time} has incorrect close time: "
                    f"{close_time} (expected {expected_close})"
                )

        # Verify time alignment
        for ts in df["open_time"]:
            floor_time = get_interval_floor(ts, interval)
            if ts != floor_time:
                logger.warning(
                    f"Bar at {ts} is not properly aligned (should be {floor_time})"
                )

    async def fetch(
        self,
        symbol: str,
        interval: Interval,
        start_time: datetime,
        end_time: datetime,
        concurrency: Optional[int] = None,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Fetch historical kline data with automatic optimization.

        Args:
            symbol: Trading symbol
            interval: Time interval
            start_time: Start time
            end_time: End time
            concurrency: Optional override for concurrency limit

        Returns:
            Tuple of (DataFrame, stats dictionary)
        """
        stats = {"start_time": datetime.now(timezone.utc)}
        df = pd.DataFrame()

        try:
            # Validate time window using centralized utility
            TimeRangeManager.validate_time_window(start_time, end_time)

            # Get time boundaries with standard handling through TimeRangeManager
            time_boundaries = TimeRangeManager.get_time_boundaries(
                start_time, end_time, interval
            )
            start_time = time_boundaries["adjusted_start"]
            end_time = time_boundaries["adjusted_end"]
            start_ms = time_boundaries["start_ms"]
            end_ms = time_boundaries["end_ms"]

            logger.debug(
                f"Fetching {symbol} {interval.value} data: "
                f"{start_time.isoformat()} -> {end_time.isoformat()} (exclusive end)"
            )

            # Calculate optimal chunks
            chunks = self._calculate_chunks(start_ms, end_ms, interval)

            # Set concurrency
            if concurrency is None:
                concurrency = min(
                    len(chunks),
                    self._hw_monitor.calculate_optimal_concurrency()[
                        "optimal_concurrency"
                    ],
                    (len(self._capabilities.backup_endpoints) + 2),
                )

            # Fetch in parallel
            tasks = []
            semaphore = asyncio.Semaphore(concurrency)
            for chunk_start, chunk_end in chunks:
                tasks.append(
                    self._fetch_chunk_with_retry(
                        symbol=symbol,
                        interval=interval,
                        start_ms=chunk_start,
                        end_ms=chunk_end,
                        sem=semaphore,
                    )
                )

            # Execute all tasks
            chunk_results = await asyncio.gather(*tasks)

            # Process results
            dfs = []
            records_fetched = 0
            for chunk_data, endpoint in chunk_results:
                if chunk_data:
                    df = process_kline_data(chunk_data)
                    dfs.append(df)
                    records_fetched += len(df)

            # Combine and sort all data
            if dfs:
                df = pd.concat(dfs)
                df = df.sort_index()

                # Apply consistent time filtering using TimeRangeManager
                df = TimeRangeManager.filter_dataframe(df, start_time, end_time)

                # Ensure close_time is present as a column
                if "close_time" not in df.columns:
                    logger.debug("close_time column is missing, adding it back")
                    # Calculate close_time based on open_time for 1-second interval
                    df["close_time"] = (
                        df.index
                        + pd.Timedelta(seconds=1)
                        - pd.Timedelta(microseconds=1)
                    )

                # Add open_time as a column (for tests that expect it)
                if df.index.name == "open_time" and "open_time" not in df.columns:
                    logger.debug("Adding open_time as a column for compatibility")
                    # Make a copy of the dataframe before resetting the index
                    temp_df = df.copy()
                    # Reset index to make open_time a regular column
                    df = temp_df.reset_index()

                    # Ensure the open_time column is sorted
                    if not df["open_time"].is_monotonic_increasing:
                        logger.debug(
                            "Sorting open_time column to ensure monotonic order"
                        )
                        df = df.sort_values("open_time")

            # Update stats
            stats.update(
                {
                    "end_time": datetime.now(timezone.utc),
                    "records_fetched": records_fetched,
                    "records_returned": len(df),
                    "total_records": len(df),  # Add for test compatibility
                    "chunks": len(chunks),
                    "chunks_processed": len(chunks),  # Add for test compatibility
                    "chunks_failed": 0,  # Add for test compatibility
                    "concurrency": concurrency,
                    "endpoint_used": endpoint,
                }
            )

            logger.info(
                f"Fetched {len(df)} records for {symbol} {interval.value} "
                f"({stats['end_time'] - stats['start_time']})"
            )

            # Validate data
            self._validate_bar_alignment(df, interval)
            self._validate_historical_bars(df, datetime.now(timezone.utc))

            return df, stats

        except Exception as e:
            stats["error"] = str(e)
            stats["end_time"] = datetime.now(timezone.utc)
            logger.error(f"Error fetching data: {e}")
            raise
