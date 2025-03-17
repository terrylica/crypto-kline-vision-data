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
    adjust_time_window,
    get_bar_close_time,
    get_interval_floor,
    is_bar_complete,
)
from utils.hardware_monitor import HardwareMonitor
from utils.validation import DataValidation

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)


def create_http_client(
    timeout: int = 10,
    max_connections: int = 20,
) -> aiohttp.ClientSession:
    """Create an optimized HTTP client for 1-second data retrieval.

    Args:
        timeout: Total timeout in seconds
        max_connections: Maximum number of connections

    Returns:
        Configured aiohttp ClientSession
    """
    timeout = aiohttp.ClientTimeout(total=timeout, connect=3, sock_connect=3, sock_read=5)  # type: ignore

    connector = aiohttp.TCPConnector(limit=max_connections, force_close=False)

    return aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={"Accept": "application/json", "User-Agent": "EnhancedRetriever/2.0"},
    )


def process_kline_data(raw_data: List[List]) -> pd.DataFrame:  # type: ignore
    """Process raw kline data into a DataFrame.

    Args:
        raw_data: List of kline data from Binance API

    Returns:
        Processed DataFrame
    """
    if not raw_data:
        return pd.DataFrame()

    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
    ]

    # Convert list to pandas Index
    df = pd.DataFrame(raw_data, columns=pd.Index(columns))

    # Add DEBUG logging for timestamp conversion
    logger.debug("\n=== Timestamp Conversion Debug ===")
    if len(raw_data) > 0:
        logger.debug(f"Sample raw close_time: {raw_data[0][6]}")
        logger.debug(f"Number of digits: {len(str(raw_data[0][6]))}")

    # Convert timestamps with microsecond precision
    for col in ["open_time", "close_time"]:
        # Convert milliseconds to microseconds by multiplying by 1000
        df[col] = df[col].astype(np.int64) * 1000  # type: ignore
        df[col] = pd.to_datetime(df[col], unit="us", utc=True)  # type: ignore

        # For close_time, add 999 microseconds to match REST API behavior
        if col == "close_time":
            df[col] = df[col] + pd.Timedelta(microseconds=999)  # type: ignore

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
        "taker_buy_base",
        "taker_buy_quote",
    ]
    df[numeric_cols] = df[numeric_cols].astype(np.float64)  # type: ignore
    df["trades"] = df["trades"].astype(np.int32)  # type: ignore

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

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # type: ignore
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

    async def _fetch_chunk_with_retry(  # type: ignore
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
                    async with self.client.get(endpoint_url, params=params) as response:  # type: ignore
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

                        return data, endpoint_url  # type: ignore

            except (aiohttp.ClientError, ValueError) as e:
                last_error = e
                logger.warning(f"Failed to fetch chunk from {endpoint_url}: {str(e)}")  # type: ignore
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
        # Use validation utilities
        DataValidation.validate_dates(start_time, end_time)
        DataValidation.validate_interval(interval.value, self.market_type.name)
        DataValidation.validate_symbol_format(symbol, self.market_type.name)

    def _create_optimized_client(self) -> aiohttp.ClientSession:
        """Create an optimized client based on hardware capabilities."""
        concurrency_info = self._hw_monitor.calculate_optimal_concurrency()  # type: ignore
        return create_http_client(
            max_connections=concurrency_info["optimal_concurrency"],  # type: ignore
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

        # Create chunks with non-overlapping boundaries
        chunks = []
        current_ms = start_ms
        while current_ms < end_ms:
            chunk_end = min(current_ms + chunk_size_ms, end_ms)
            if chunk_end > current_ms:
                # Subtract 1ms from end time to avoid overlap with next chunk's start time
                chunks.append((current_ms, chunk_end - 1))  # type: ignore
            current_ms = chunk_end

        # Log chunk boundaries for verification
        for i, (chunk_start, chunk_end) in enumerate(chunks):
            start_time = datetime.fromtimestamp(chunk_start / 1000, tz=timezone.utc)
            end_time = datetime.fromtimestamp(
                (chunk_end + 1) / 1000, tz=timezone.utc
            )  # Add 1ms back for logging
            logger.debug(
                f"Chunk {i + 1}/{len(chunks)}: {start_time} -> {end_time} "
                f"({(chunk_end - chunk_start + 1)/interval_ms:.0f} intervals)"
            )

        return chunks  # type: ignore

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
        for ts in df["open_time"]:  # type: ignore
            if ts < cutoff_time and not is_bar_complete(ts, current_time):  # type: ignore
                logger.warning(f"Found incomplete historical bar at {ts}")  # type: ignore
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
        for idx, row in df.iterrows():  # type: ignore
            open_time = row["open_time"]  # type: ignore
            close_time = row["close_time"]  # type: ignore
            expected_close = get_bar_close_time(open_time, interval)  # type: ignore

            if close_time != expected_close:
                logger.warning(
                    f"Bar at {open_time} has incorrect close time: "
                    f"{close_time} (expected {expected_close})"
                )

        # Verify time alignment
        for ts in df["open_time"]:  # type: ignore
            floor_time = get_interval_floor(ts, interval)  # type: ignore
            if ts != floor_time:  # type: ignore
                logger.warning(f"Bar at {ts} is not properly aligned (should be {floor_time})")  # type: ignore

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
            symbol: Trading pair symbol
            interval: Time interval for data
            start_time: Start time
            end_time: End time
            concurrency: Optional override for concurrent requests

        Returns:
            Tuple of (DataFrame with data, metrics dictionary)
        """
        # Validate parameters
        self._validate_request_params(symbol, interval, start_time, end_time)

        # Ensure times are in UTC and properly aligned
        start_time = start_time.astimezone(timezone.utc)
        end_time = end_time.astimezone(timezone.utc)
        adjusted_start, adjusted_end = adjust_time_window(
            start_time, end_time, interval
        )

        if adjusted_start != start_time or adjusted_end != end_time:
            logger.info(
                f"Adjusted time window for proper alignment:"
                f"\nOriginal:  {start_time} -> {end_time}"
                f"\nAdjusted:  {adjusted_start} -> {adjusted_end}"
            )

        # Calculate chunks
        start_ts = int(adjusted_start.timestamp() * 1000)  # Convert to milliseconds
        end_ts = int(adjusted_end.timestamp() * 1000)  # Convert to milliseconds
        chunks = self._calculate_chunks(start_ts, end_ts, interval)
        total_span = end_ts - start_ts

        # Use hardware-optimized concurrency if not specified
        if concurrency is None:
            concurrency = self._hw_monitor.calculate_optimal_concurrency()["optimal_concurrency"]  # type: ignore

        # Create semaphore for concurrency control
        sem = asyncio.Semaphore(concurrency)  # type: ignore

        # Fetch all chunks concurrently with retries and endpoint failover
        tasks = [self._fetch_chunk_with_retry(symbol, interval, start, end, sem) for start, end in chunks]  # type: ignore
        chunk_results: List[Union[Tuple[List[List[Any]], str], Exception]] = await asyncio.gather(*tasks, return_exceptions=True)  # type: ignore

        # Process results and track endpoints used
        all_data: List[List[Any]] = []
        endpoints_used: Set[str] = set()
        failed_chunks = 0

        for result in chunk_results:
            if isinstance(result, Exception):
                failed_chunks += 1
                logger.warning(f"Failed to fetch chunk: {result}")
                continue

            chunk_data, endpoint = result
            if chunk_data:
                all_data.extend(chunk_data)
                endpoints_used.add(endpoint)

        # Convert to DataFrame
        if all_data:
            df = process_kline_data(all_data)

            # Log initial state before deduplication
            logger.debug(
                f"Initial DataFrame shape before deduplication: {df.shape}"
            )  # Changed from info to debug

            # Check for duplicates and log details
            duplicates = df.duplicated(subset=["open_time"], keep=False)
            if duplicates.any():  # type: ignore
                duplicate_df = df[duplicates].sort_values("open_time")  # type: ignore
                logger.error(  # Keep as error - this is important
                    f"Found {len(duplicate_df) // 2} duplicate pairs at following timestamps:"
                    f"\n{duplicate_df['open_time'].unique().tolist()}"
                )

                # Log a sample of duplicate records for analysis
                for ts in duplicate_df["open_time"].unique()[
                    :2
                ]:  # Show first 2 duplicate pairs
                    dup_rows = duplicate_df[duplicate_df["open_time"] == ts]
                    logger.error(f"\nDuplicate records at {ts}:")  # Keep as error
                    for _, row in dup_rows.iterrows():
                        logger.error(f"Row data: {row.to_dict()}")  # Keep as error

            # Remove duplicates and sort
            df = df.drop_duplicates(subset=["open_time"], keep="first")
            df = df.sort_values("open_time").reset_index(drop=True)  # type: ignore

            logger.debug(
                f"Final DataFrame shape after deduplication: {df.shape}"
            )  # Changed from info to debug

            # Validate time alignment
            self._validate_bar_alignment(df, interval)

            # Verify data completeness
            current_time = datetime.now(timezone.utc)
            incomplete_bars = [ts for ts in df["open_time"] if not is_bar_complete(ts, current_time)]  # type: ignore
            if incomplete_bars:
                logger.error(
                    f"Found {len(incomplete_bars)} incomplete bars"
                )  # Keep as error
        else:
            df = pd.DataFrame()

        # Return results with enhanced metadata
        metadata = {  # type: ignore
            "chunks_processed": len(chunks),
            "chunks_failed": failed_chunks,
            "total_records": len(df),
            "time_span_seconds": total_span // 1000,
            "interval": interval.value,
            "endpoints_used": list(endpoints_used),  # type: ignore
            "endpoint_count": len(endpoints_used),  # type: ignore
            "start_time": adjusted_start.isoformat(),
            "end_time": adjusted_end.isoformat(),
            "incomplete_bars": len(incomplete_bars) if "incomplete_bars" in locals() else 0,  # type: ignore
        }

        return df, metadata  # type: ignore
