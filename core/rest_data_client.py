#!/usr/bin/env python

"""Unified REST API data client with optimized 1-second data handling."""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np

# Import curl_cffi for better performance
from curl_cffi.requests import AsyncSession

from utils.logger_setup import get_logger
from utils.market_constraints import (
    Interval,
    MarketType,
)
from utils.time_utils import (
    get_bar_close_time,
    get_interval_floor,
    is_bar_complete,
)
from utils.hardware_monitor import HardwareMonitor
from utils.network_utils import create_client, safely_close_client, test_connectivity
from utils.config import (
    KLINE_COLUMNS,
    standardize_column_names,
    TIMESTAMP_UNIT,
    CLOSE_TIME_ADJUSTMENT,
    CANONICAL_INDEX_NAME,
    create_empty_dataframe,
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
        "quote_asset_volume",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]
    df[numeric_cols] = df[numeric_cols].astype(np.float64)
    df["number_of_trades"] = df["number_of_trades"].astype(np.int32)

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


class RestDataClient:
    """RestDataClient for market data with chunking, retries, and rate limiting.

    This class handles fetching klines data with proper rate limit handling,
    automatical chunking for large time ranges, and endpoint rotation for
    better performance.
    """

    def __init__(
        self,
        market_type: MarketType = MarketType.SPOT,
        max_concurrent: int = 50,
        retry_count: int = 5,
        client: Optional[AsyncSession] = None,
    ):
        """Initialize the RestDataClient.

        Args:
            market_type: Market type (spot, futures, etc.)
            max_concurrent: Maximum concurrent API requests
            retry_count: Number of retries for failed requests
            client: Optional existing client session (curl_cffi AsyncSession)
        """
        self.market_type = market_type
        self.CHUNK_SIZE = 1000  # Maximum number of records per API request
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._retry_count = retry_count

        # Get endpoints from market_constraints
        from utils.market_constraints import get_endpoint_url

        self._base_url = get_endpoint_url(market_type)
        # Use multiple API endpoints for rotation
        self._endpoints = [
            self._base_url,
            self._base_url.replace("api.", "api1."),
            self._base_url.replace("api.", "api2."),
            self._base_url.replace("api.", "api3."),
        ]

        # Initialize endpoint rotation attributes
        self._endpoint_lock = asyncio.Lock()
        self._endpoint_index = 0

        # Initialize client
        self._client = client
        self._client_is_external = client is not None

        # Initialize hardware monitor for resource optimization
        self.hw_monitor = HardwareMonitor()

        # Log initialization
        logger.debug(
            f"Initialized RestDataClient with market_type={market_type}, "
            f"max_concurrent={max_concurrent}, retry_count={retry_count}"
        )

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._client:
            # Create a client with default timeout
            from utils.network_utils import create_client

            self._client = create_client(timeout=30.0)
            logger.debug("Created new HTTP client with default settings")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit method."""
        # Only close client if we created it internally
        if self._client and not self._client_is_external:
            try:
                await safely_close_client(self._client)
                logger.debug("RestDataClient closed HTTP client")
            except Exception as e:
                logger.warning(f"Error closing RestDataClient HTTP client: {str(e)}")
            finally:
                self._client = None

    async def _fetch_chunk_with_retry(
        self, endpoint: str, params: Dict[str, Any], retry_count: int = 0
    ) -> List[List[Any]]:
        """Fetch a chunk of data with retry logic.

        Args:
            endpoint: API endpoint URL
            params: API parameters
            retry_count: Current retry count

        Returns:
            List of klines data

        Raises:
            Exception: If all retries fail
        """
        try:
            logger.debug(
                f"Fetching chunk from endpoint: {endpoint} with params: {params}"
            )

            # Make the API request using curl_cffi
            response = await self._client.get(endpoint, params=params)

            # Check for errors
            if response.status_code >= 400:
                # Handle rate limiting specifically
                if response.status_code in (418, 429):
                    retry_after = int(response.headers.get("Retry-After", 1))
                    logger.warning(f"Rate limited by API. Retry after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return await self._fetch_chunk_with_retry(
                        endpoint, params, retry_count
                    )

                # Other error codes
                logger.error(f"API error {response.status_code}: {response.text}")
                raise Exception(f"API error {response.status_code}: {response.text}")

            # Parse response
            data = response.json()

            # Validate response format
            if not isinstance(data, list):
                logger.error(f"Unexpected API response format: {type(data)}")
                raise ValueError(f"Unexpected API response format: {type(data)}")

            return data

        except Exception as e:
            if retry_count >= self._retry_count:
                logger.error(f"All {self._retry_count} retries failed: {str(e)}")
                raise

            # Increment retry counter and wait with exponential backoff
            retry_count += 1
            wait_time = min(2**retry_count, 60)  # Cap at 60 seconds
            logger.warning(
                f"Error fetching chunk: {str(e)}. Retry {retry_count}/{self._retry_count} in {wait_time}s"
            )
            await asyncio.sleep(wait_time)

            # Try with a different endpoint
            async with self._endpoint_lock:
                self._endpoint_index = (self._endpoint_index + 1) % len(self._endpoints)
                new_endpoint = self._endpoints[self._endpoint_index]

            # Log the endpoint rotation
            logger.info(f"Rotating to endpoint: {new_endpoint}")

            # Retry with new endpoint
            return await self._fetch_chunk_with_retry(new_endpoint, params, retry_count)

    async def _fetch_chunk_with_retry(
        self,
        symbol: str,
        interval: Interval,
        chunk_start: int,
        chunk_end: int,
        semaphore: asyncio.Semaphore,
        retry_count: int = 0,
    ) -> Tuple[List[List[Any]], str]:
        """Fetch a chunk of klines data with retry logic and semaphore control.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            chunk_start: Start time in milliseconds
            chunk_end: End time in milliseconds
            semaphore: Semaphore for concurrency control
            retry_count: Current retry count

        Returns:
            Tuple of (klines data, endpoint URL)
        """
        # Get the current endpoint with rotation
        async with self._endpoint_lock:
            endpoint_index = self._endpoint_index
            endpoint = self._endpoints[endpoint_index]

        # Prepare request parameters
        params = {
            "symbol": symbol,
            "interval": interval.value,
            "startTime": chunk_start,
            "endTime": chunk_end,
            "limit": self.CHUNK_SIZE,
        }

        # Use semaphore to limit concurrent requests
        async with semaphore:
            try:
                logger.debug(
                    f"Fetching chunk from {endpoint}: {chunk_start} to {chunk_end}"
                )

                # Make API request using curl_cffi
                response = await self._client.get(endpoint, params=params)

                # Handle response
                if response.status_code >= 400:
                    # Handle rate limiting
                    if response.status_code in (418, 429):
                        retry_after = int(response.headers.get("Retry-After", 1))
                        logger.warning(
                            f"Rate limited by API. Retry after {retry_after}s"
                        )
                        await asyncio.sleep(retry_after)
                        # Try with a different endpoint
                        async with self._endpoint_lock:
                            self._endpoint_index = (self._endpoint_index + 1) % len(
                                self._endpoints
                            )

                        return await self._fetch_chunk_with_retry(
                            symbol,
                            interval,
                            chunk_start,
                            chunk_end,
                            semaphore,
                            retry_count,
                        )

                    # Handle other errors
                    logger.error(f"API error {response.status_code}: {response.text}")
                    raise Exception(
                        f"API error {response.status_code}: {response.text}"
                    )

                # Parse response
                data = response.json()

                # Validate response format
                if not isinstance(data, list):
                    logger.error(f"Unexpected API response format: {type(data)}")
                    raise ValueError(f"Unexpected API response format: {type(data)}")

                logger.debug(f"Retrieved {len(data)} records from chunk")
                return data, endpoint

            except Exception as e:
                if retry_count >= self._retry_count:
                    logger.error(f"All {self._retry_count} retries failed: {str(e)}")
                    raise

                # Increment retry counter and wait with exponential backoff
                retry_count += 1
                wait_time = min(2**retry_count, 60)  # Cap at 60 seconds
                logger.warning(
                    f"Error fetching chunk: {str(e)}. Retry {retry_count}/{self._retry_count} in {wait_time}s"
                )
                await asyncio.sleep(wait_time)

                # Try with a different endpoint
                async with self._endpoint_lock:
                    self._endpoint_index = (self._endpoint_index + 1) % len(
                        self._endpoints
                    )

                # Retry
                return await self._fetch_chunk_with_retry(
                    symbol, interval, chunk_start, chunk_end, semaphore, retry_count
                )

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

    def _create_optimized_client(self) -> AsyncSession:
        """Create an optimized client based on hardware capabilities."""
        concurrency_info = self.hw_monitor.calculate_optimal_concurrency()
        return create_client(
            max_connections=concurrency_info["optimal_concurrency"],
            timeout=30,  # Increased for large datasets
        )

    def _calculate_chunks(
        self, start_ms: int, end_ms: int, interval: Interval
    ) -> List[Tuple[int, int]]:
        """Calculate chunk ranges based on start and end times.

        This method divides the time range into chunks that respect the API limit.

        Args:
            start_ms: Start time in milliseconds
            end_ms: End time in milliseconds
            interval: Time interval

        Returns:
            List of (chunk_start, chunk_end) tuples for each chunk
        """
        chunks = []
        current_start = start_ms

        # Determine the appropriate chunk size based on interval
        # For 1s data, we need smaller chunks to avoid exceeding API limits
        is_small_interval = interval in (Interval.SECOND_1, Interval.MINUTE_1)

        # While there's still time range to process
        while current_start < end_ms:
            # Calculate end of this chunk
            # Use a smaller chunk size for small intervals to avoid hitting API limits
            chunk_duration = min(
                end_ms - current_start,  # Don't go beyond the requested end time
                self.CHUNK_SIZE
                * (60 * 1000 if is_small_interval else 60 * 60 * 1000),  # Convert to ms
            )

            chunk_end = current_start + chunk_duration
            if chunk_end > end_ms:
                chunk_end = end_ms

            chunks.append((current_start, chunk_end))

            # Move to next chunk
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

        # Test connectivity to Binance API before proceeding
        api_status = await test_connectivity(
            self._client,
            url=self._base_url,  # Use our base API URL for the test
            timeout=10.0,
        )

        if not api_status:
            logger.error(f"Cannot connect to Binance API at {self._base_url}")
            return self.create_empty_dataframe(), {"error": "connectivity_failed"}

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
        return create_empty_dataframe()
