#!/usr/bin/env python

"""Unified market data client with optimized 1-second data handling."""

import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any, Set, Union
import pandas as pd
import numpy as np
from utils.logger_setup import get_logger
from utils.market_constraints import (
    Interval,
    MarketType,
    get_market_capabilities,
    get_endpoint_url,
)
from utils.time_utils import (
    get_bar_close_time,
    get_interval_floor,
    is_bar_complete,
)
from utils.hardware_monitor import HardwareMonitor
from utils.validation import DataValidation
from utils.network_utils import create_client
from utils.config import (
    KLINE_COLUMNS,
    standardize_column_names,
    TIMESTAMP_UNIT,
    CLOSE_TIME_ADJUSTMENT,
    CANONICAL_INDEX_NAME,
)

logger = get_logger(__name__, "INFO", show_path=False)


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
        self.endpoint_url = get_endpoint_url(market_type)
        self._client = client
        self._client_is_external = client is not None
        self.CHUNK_SIZE_MS = self.CHUNK_SIZE * 1000  # Convert to milliseconds
        self.hw_monitor = hw_monitor or HardwareMonitor()
        self.stats = {"total_records": 0, "chunks_processed": 0, "chunks_failed": 0}
        self._capabilities = get_market_capabilities(market_type)

        # Validate market capabilities
        if market_type != MarketType.SPOT:
            raise ValueError("Only SPOT market type supports 1-second data")
        if self._capabilities.max_limit != self.CHUNK_SIZE:
            raise ValueError(
                f"API limit {self._capabilities.max_limit} doesn't match CHUNK_SIZE {self.CHUNK_SIZE}"
            )

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._client:
            self._client = self._create_optimized_client()
            self._client_is_external = False
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._client:
            await self._client.close()
            self._client = None

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
                    async with self._client.get(
                        endpoint_url, params=params
                    ) as response:
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
        """Validate request parameters.

        This validation ensures parameters are valid but does not apply any
        manual time alignment. The REST API will handle interval alignment.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            start_time: Start time
            end_time: End time

        Raises:
            ValueError: For invalid parameters
        """
        if not symbol:
            raise ValueError("Symbol must be provided.")
        if not isinstance(interval, Interval):
            raise TypeError("Interval must be an Interval enum.")
        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise TypeError("Start and end times must be datetime objects.")
        if start_time >= end_time:
            raise ValueError("Start time must be before end time.")

        # Removed all alignment-specific validations
        # The Binance REST API will handle interval alignment according to its behavior

    def _create_optimized_client(self) -> aiohttp.ClientSession:
        """Create an optimized client based on hardware capabilities."""
        concurrency_info = self.hw_monitor.calculate_optimal_concurrency()
        return create_client(
            client_type="aiohttp",
            max_connections=concurrency_info["optimal_concurrency"],
            timeout=30,  # Increased for large datasets
        )

    def _calculate_chunks(
        self, start_ms: int, end_ms: int, interval: Interval
    ) -> List[Tuple[int, int]]:
        """Calculate chunk ranges based on start and end times.

        This method creates chunks based solely on the CHUNK_SIZE parameter
        without applying any manual time alignment. The REST API will handle
        interval alignment.

        Args:
            start_ms: Start time in milliseconds
            end_ms: End time in milliseconds
            interval: Time interval

        Returns:
            List of (start_ms, end_ms) pairs for each chunk
        """
        chunks = []
        current_start = start_ms

        while current_start < end_ms:
            chunk_end = min(current_start + self.CHUNK_SIZE - 1, end_ms)
            chunks.append((current_start, chunk_end))
            current_start = chunk_end + 1

        return chunks

    def _validate_bar_duration(self, open_time: datetime, interval: Interval) -> float:
        """Validate a single bar's duration.

        Args:
            open_time: Bar's open time
            interval: Time interval

        Returns:
            Bar duration in seconds
        """
        # Use get_bar_close_time through TimeRangeManager if it's available there
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
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Fetch market data from Binance API.

        Args:
            symbol: The trading pair symbol
            interval: Time interval enum
            start_time: Start datetime (timezone-aware)
            end_time: End datetime (timezone-aware)

        Returns:
            Tuple of (DataFrame with market data, statistics dictionary)
        """
        # Initialize client if needed
        if not self._client:
            self._client = self._create_optimized_client()
            self._client_is_external = False

        # Convert datetime objects to milliseconds since epoch
        self._validate_request_params(symbol, interval, start_time, end_time)
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)

        # Reset stats for this fetch
        self.stats = {"total_records": 0, "chunks_processed": 0, "chunks_failed": 0}

        # Calculate chunk boundaries
        chunks = self._calculate_chunks(start_ms, end_ms, interval)
        num_chunks = len(chunks)

        logger.info(
            f"Fetching {symbol} {interval.value} data from "
            f"{start_time} to {end_time} in {num_chunks} chunks"
        )

        # Get optimal concurrency value
        optimal_concurrency_result = self.hw_monitor.calculate_optimal_concurrency()
        optimal_concurrency = optimal_concurrency_result["optimal_concurrency"]

        # Limit semaphore to optimal concurrency
        sem = asyncio.Semaphore(optimal_concurrency)

        # Create tasks for all chunks
        tasks = []
        for chunk_start, chunk_end in chunks:
            task = asyncio.create_task(
                self._fetch_chunk_with_retry(
                    symbol, interval, chunk_start, chunk_end, sem
                )
            )
            tasks.append(task)

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        successful_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Chunk {i+1}/{num_chunks} failed: {result}")
                self.stats["chunks_failed"] += 1
            else:
                klines, endpoint = result
                self.stats["chunks_processed"] += 1
                self.stats["total_records"] += len(klines)
                successful_results.append(klines)
                if i == 0 or i == len(results) - 1:
                    logger.debug(
                        f"Chunk {i+1}/{num_chunks} retrieved {len(klines)} records from {endpoint}"
                    )

        # Combine results and create DataFrame
        if not successful_results:
            logger.warning(
                f"No data retrieved for {symbol} from {start_time} to {end_time}"
            )
            return self.create_empty_dataframe(), self.stats

        # Combine all chunks
        all_klines = [item for sublist in successful_results for item in sublist]

        # Process into DataFrame
        df = process_kline_data(all_klines)

        # Ensure we have data
        if df.empty:
            logger.warning(f"Processed DataFrame is empty for {symbol}")
            return self.create_empty_dataframe(), self.stats

        logger.info(
            f"Successfully retrieved {len(df)} records for {symbol} "
            f"from {start_time} to {end_time}"
        )

        return df, self.stats

    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the expected structure.

        Returns:
            Empty DataFrame with proper column structure
        """
        columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
        ]
        return pd.DataFrame(columns=columns)
