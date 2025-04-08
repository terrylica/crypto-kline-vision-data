#!/usr/bin/env python

"""Unified REST API data client with optimized 1-second data handling."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Any
import pandas as pd
import time
import gc
import math

# Import curl_cffi for better performance

from utils.logger_setup import logger
from utils.market_constraints import (
    Interval,
    MarketType,
    ChartType,
)
from utils.time_utils import (
    get_bar_close_time,
    get_interval_floor,
    is_bar_complete,
    align_time_boundaries,
    TimeseriesDataProcessor,
)
from utils.hardware_monitor import HardwareMonitor
from utils.async_cleanup import direct_resource_cleanup
from utils.config import (
    KLINE_COLUMNS,
    standardize_column_names,
    create_empty_dataframe,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    MAX_TIMEOUT,
)
from utils.validation import DataValidation


def process_kline_data(raw_data: List[List]) -> pd.DataFrame:
    """Process raw kline data into a DataFrame.

    Uses the centralized TimeseriesDataProcessor to ensure consistent handling
    of timestamp formats between REST and Vision APIs.

    Args:
        raw_data: List of kline data from Binance API

    Returns:
        Processed DataFrame with proper types and index
    """
    # Use the centralized processor with standardized column names
    df = TimeseriesDataProcessor.process_kline_data(raw_data, KLINE_COLUMNS)

    # Apply standard column naming
    df = standardize_column_names(df)

    # Perform any REST API-specific post-processing here if needed

    return df


class RestDataClient:
    """RestDataClient for market data with chunking, retries, and rate limiting.

    This class handles fetching klines data with proper rate limit handling,
    automatical chunking for large time ranges, and endpoint rotation for
    better performance.
    """

    # Constants for chunk sizing
    CHUNK_SIZE = 1000  # Default chunk size (max records per request for most endpoints)

    def __init__(
        self,
        market_type: MarketType,
        max_concurrent: int = 5,
        retry_count: int = 3,
        fetch_timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
        client=None,
    ):
        """Initialize the REST data client.

        Args:
            market_type: Market type to use (spot, futures_usdt, futures_coin)
            max_concurrent: Maximum number of concurrent requests
            retry_count: Number of retry attempts for failed requests
            fetch_timeout: Timeout in seconds for fetch operations
            client: Optional pre-configured HTTP client
        """
        self.market_type = market_type
        self.max_concurrent = max_concurrent
        self.retry_count = retry_count
        self.fetch_timeout = fetch_timeout
        self._client = client
        self._client_is_external = client is not None
        self._active_tasks = []

        # Set base URL based on market type
        if market_type == MarketType.SPOT:
            self.base_url = "https://api.binance.com"
        elif market_type == MarketType.FUTURES_USDT:
            self.base_url = "https://fapi.binance.com"
        elif market_type == MarketType.FUTURES_COIN:
            self.base_url = "https://dapi.binance.com"
        else:
            raise ValueError(f"Unsupported market type: {market_type}")

        # Constants for chunking and pagination
        self.CHUNK_SIZE = 1000  # Maximum number of records per request

        # Initialize hardware monitor
        self.hw_monitor = HardwareMonitor()

        # Set up proper endpoint based on market type
        self._endpoint = self._get_klines_endpoint()
        # Initialize endpoint rotation variables
        self._endpoints = [self._endpoint]  # List with just the main endpoint for now
        self._endpoint_index = 0
        self._endpoint_lock = asyncio.Lock()

        logger.debug(
            f"Initialized RestDataClient with market_type={market_type.name}, "
            f"max_concurrent={max_concurrent}, retry_count={retry_count}"
        )

    def _get_klines_endpoint(self):
        """Get the appropriate endpoint URL for klines data based on market type.

        Returns:
            URL string for the klines endpoint
        """
        # Base API URLs
        if self.market_type == MarketType.SPOT:
            return f"{self.base_url}/api/v3/klines"
        elif self.market_type == MarketType.FUTURES_USDT:
            return f"{self.base_url}/fapi/v1/klines"
        elif self.market_type == MarketType.FUTURES_COIN:
            return f"{self.base_url}/dapi/v1/klines"
        else:
            raise ValueError(f"Unsupported market type: {self.market_type}")

    async def __aenter__(self):
        """Initialize the client session when entering the context."""
        # Proactively clean up any force_timeout tasks that might cause hanging
        await self._cleanup_force_timeout_tasks()

        if self._client is None:
            self._client = self._create_optimized_client()
            self._client_is_external = False
        else:
            self._client_is_external = True
        return self

    async def _cleanup_force_timeout_tasks(self):
        """Force cleanup of any hanging tasks during timeout.

        This is a special method called when timeout occurs to ensure
        we don't leave any hanging tasks or connections in the background.
        """
        if hasattr(self, "_active_tasks") and self._active_tasks:
            logger.debug(f"Force cleanup of {len(self._active_tasks)} active tasks")
            for task in self._active_tasks:
                if not task.done():
                    task.cancel()

            # Wait briefly for tasks to cancel
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._active_tasks, return_exceptions=True),
                    timeout=0.5,
                )
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

            # Clear the list
            self._active_tasks.clear()

        # Ensure client session is closed if we created it internally
        if hasattr(self, "_client") and self._client and not self._client_is_external:
            try:
                # Use close() instead of aclose() for curl_cffi AsyncSession
                await self._client.close()
                self._client = None
                logger.debug("Forcibly closed client session during timeout cleanup")
            except Exception as e:
                logger.error(f"Error closing client during force cleanup: {e}")

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        """Clean up resources when exiting the context."""
        logger.debug("RestDataClient starting __aexit__ cleanup")

        # STEP 1: Pre-emptively clean up problematic objects that might cause hanging
        if hasattr(self, "_client") and self._client:
            # Clean up _curlm which causes circular references
            if hasattr(self._client, "_curlm") and self._client._curlm:
                logger.debug("Pre-emptively cleaning _curlm object in _client")
                try:
                    self._client._curlm = None
                except Exception as e:
                    logger.warning(f"Error pre-emptively clearing _curlm: {e}")

            # Clean up _asynccurl which can also cause issues
            if hasattr(self._client, "_asynccurl") and self._client._asynccurl:
                logger.debug("Pre-emptively cleaning _asynccurl object in _client")
                try:
                    self._client._asynccurl = None
                except Exception as e:
                    logger.warning(f"Error pre-emptively clearing _asynccurl: {e}")

            # Clean up _timeout_handle which can cause hanging
            if (
                hasattr(self._client, "_timeout_handle")
                and self._client._timeout_handle
            ):
                logger.debug("Pre-emptively cleaning _timeout_handle in _client")
                try:
                    self._client._timeout_handle = None
                except Exception as e:
                    logger.warning(f"Error pre-emptively clearing _timeout_handle: {e}")

        # STEP 2: Cancel any force_timeout tasks that might be hanging
        try:
            from utils.async_cleanup import cleanup_all_force_timeout_tasks

            await cleanup_all_force_timeout_tasks()
        except Exception as e:
            logger.warning(f"Error during force_timeout task cleanup: {e}")

        # STEP 3: Use direct resource cleanup for consistent management of resources
        await direct_resource_cleanup(
            self,
            ("_client", "HTTP client", self._client_is_external),
        )

        # STEP 4: Force garbage collection to help with circular references
        try:
            gc.collect()
        except Exception as e:
            logger.warning(f"Error during garbage collection: {e}")

        logger.debug("RestDataClient completed __aexit__ cleanup")

    async def _fetch_chunk_with_endpoint(
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
                    return await self._fetch_chunk_with_endpoint(
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
            if retry_count >= self.retry_count:
                logger.error(f"All {self.retry_count} retries failed: {str(e)}")
                raise

            # Increment retry counter and wait with exponential backoff
            retry_count += 1
            wait_time = min(2**retry_count, 60)  # Cap at 60 seconds
            logger.warning(
                f"Error fetching chunk: {str(e)}. Retry {retry_count}/{self.retry_count} in {wait_time}s"
            )
            await asyncio.sleep(wait_time)

            # Try with a different endpoint
            async with self._endpoint_lock:
                self._endpoint_index = (self._endpoint_index + 1) % len(self._endpoints)
                new_endpoint = self._endpoints[self._endpoint_index]

            # Log the endpoint rotation
            logger.info(f"Rotating to endpoint: {new_endpoint}")

            # Retry with new endpoint
            return await self._fetch_chunk_with_endpoint(
                new_endpoint, params, retry_count
            )

    async def _fetch_chunk_with_semaphore(
        self,
        symbol: str,
        interval: Interval,
        start_ms: int,
        end_ms: int,
        semaphore: asyncio.Semaphore,
    ) -> Tuple[List[List[Any]], str]:
        """Fetch a chunk of data using semaphore for concurrency control.

        Args:
            symbol: The trading pair symbol
            interval: Time interval
            start_ms: Start time in milliseconds
            end_ms: End time in milliseconds
            semaphore: Semaphore for controlling concurrency

        Returns:
            Tuple of (klines data, endpoint used)
        """
        try:
            async with semaphore:
                # Get market-specific parameters
                formatted_symbol = symbol
                if (
                    self.market_type == MarketType.FUTURES_COIN
                    and "_PERP" not in symbol
                ):
                    formatted_symbol = f"{symbol}_PERP"

                # Handle case where FUTURES_USDT might need slightly more aggressive retry
                retry_count = self.retry_count
                if self.market_type in (
                    MarketType.FUTURES_USDT,
                    MarketType.FUTURES_COIN,
                    MarketType.FUTURES,
                ):
                    retry_count += 2  # Add extra retries for futures markets

                # Determine the limit to use (different for different market types)
                limit = self.CHUNK_SIZE
                if self.market_type in (
                    MarketType.FUTURES_USDT,
                    MarketType.FUTURES_COIN,
                    MarketType.FUTURES,
                ):
                    limit = 1500  # Futures markets support 1500 records per request

                # Prepare parameters
                params = {
                    "symbol": formatted_symbol,
                    "interval": interval.value,
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": limit,
                }

                # Try to fetch with rotation and retry logic
                return await self._fetch_chunk_with_rotation(
                    params, retry_count=retry_count
                )
        except Exception as e:
            logger.error(f"Error fetching chunk: {str(e)}")
            raise

    async def _fetch_chunk_with_rotation(
        self,
        params: Dict[str, Any],
        retry_count: int = 0,
        current_attempt: int = 0,
    ) -> Tuple[List[List[Any]], str]:
        """Fetch a chunk of data with endpoint rotation and retry logic.

        Args:
            params: API parameters to use
            retry_count: Maximum number of retries
            current_attempt: Current retry attempt

        Returns:
            Tuple of (klines data, endpoint used)
        """
        # Get the current endpoint with rotation
        async with self._endpoint_lock:
            endpoint_index = self._endpoint_index
            endpoint = self._endpoints[endpoint_index]

        try:
            # Add detailed logging including symbol and interval for easier debugging
            symbol = params.get("symbol", "UNKNOWN")
            interval = params.get("interval", "UNKNOWN")
            start_time = (
                datetime.fromtimestamp(
                    params.get("startTime", 0) / 1000, tz=timezone.utc
                )
                if "startTime" in params
                else "UNKNOWN"
            )
            end_time = (
                datetime.fromtimestamp(params.get("endTime", 0) / 1000, tz=timezone.utc)
                if "endTime" in params
                else "UNKNOWN"
            )

            logger.debug(
                f"Fetching {symbol} {interval} chunk (attempt {current_attempt+1}/{retry_count+1}) "
                f"from {endpoint}: {start_time} to {end_time}"
            )

            # Record start time for timing
            fetch_start_time = time.monotonic()

            # Make the API request using curl_cffi
            response = await self._client.get(endpoint, params=params)

            # Calculate elapsed time
            elapsed = time.monotonic() - fetch_start_time
            logger.debug(
                f"Request completed in {elapsed:.2f}s with status {response.status_code}"
            )

            # Handle response
            if response.status_code >= 400:
                # Handle rate limiting specifically
                if response.status_code in (418, 429):
                    retry_after = int(response.headers.get("Retry-After", 1))
                    logger.warning(
                        f"{symbol} {interval}: Rate limited by API. "
                        f"Retry after {retry_after}s (attempt {current_attempt+1}/{retry_count+1})"
                    )
                    await asyncio.sleep(retry_after)

                    # Rotate to next endpoint
                    async with self._endpoint_lock:
                        self._endpoint_index = (self._endpoint_index + 1) % len(
                            self._endpoints
                        )

                    # Try again with next endpoint
                    return await self._fetch_chunk_with_rotation(
                        params, retry_count, current_attempt
                    )

                # Handle other errors
                logger.error(
                    f"{symbol} {interval}: API error {response.status_code}: {response.text}"
                )
                if current_attempt < retry_count:
                    # Increment retry counter
                    wait_time = min(2**current_attempt, 30)  # Cap at 30 seconds
                    logger.warning(
                        f"{symbol} {interval}: Error fetching chunk: API error {response.status_code}. "
                        f"Retry {current_attempt+1}/{retry_count} in {wait_time}s"
                    )
                    await asyncio.sleep(wait_time)

                    # Rotate to next endpoint
                    async with self._endpoint_lock:
                        self._endpoint_index = (self._endpoint_index + 1) % len(
                            self._endpoints
                        )

                    # Try again with next attempt
                    return await self._fetch_chunk_with_rotation(
                        params, retry_count, current_attempt + 1
                    )

                raise Exception(f"API error {response.status_code}: {response.text}")

            # Parse response
            data = response.json()

            # Validate response format
            if not isinstance(data, list):
                logger.error(
                    f"{symbol} {interval}: Unexpected API response format: {type(data)}"
                )
                raise ValueError(f"Unexpected API response format: {type(data)}")

            # Log first and last timestamps if data is available
            if data and len(data) > 0:
                first_ts = datetime.fromtimestamp(data[0][0] / 1000, tz=timezone.utc)
                last_ts = datetime.fromtimestamp(data[-1][0] / 1000, tz=timezone.utc)
                logger.debug(
                    f"{symbol} {interval}: Retrieved {len(data)} records "
                    f"from {first_ts} to {last_ts} in {elapsed:.2f}s"
                )
            else:
                logger.debug(
                    f"{symbol} {interval}: Retrieved empty chunk (no records) in {elapsed:.2f}s"
                )

            return data, endpoint

        except Exception as e:
            # Retry logic for general exceptions (network errors, etc.)
            if current_attempt < retry_count:
                wait_time = min(2**current_attempt, 30)  # Cap at 30 seconds
                logger.warning(
                    f"Error fetching chunk for {params.get('symbol', 'UNKNOWN')} {params.get('interval', 'UNKNOWN')}: {str(e)}. "
                    f"Retry {current_attempt+1}/{retry_count} in {wait_time}s"
                )
                await asyncio.sleep(wait_time)

                # Rotate to next endpoint
                async with self._endpoint_lock:
                    self._endpoint_index = (self._endpoint_index + 1) % len(
                        self._endpoints
                    )

                # Try again with next attempt
                return await self._fetch_chunk_with_rotation(
                    params, retry_count, current_attempt + 1
                )

            # All retries exhausted
            logger.error(
                f"All {retry_count} retries failed for {params.get('symbol', 'UNKNOWN')} "
                f"{params.get('interval', 'UNKNOWN')}: {str(e)}"
            )
            raise

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

    def _create_optimized_client(self) -> Any:
        """Create an optimized HTTP client for data retrieval.

        Returns:
            An async HTTP client optimized for data retrieval
        """
        from utils.network_utils import create_client

        # Create a client with optimized settings
        client = create_client(
            timeout=self.fetch_timeout,
            max_connections=self.max_concurrent,
            use_httpx=False,
            # Set optimized options for data retrieval
            impersonate="chrome",  # Use Chrome's TLS fingerprint
            h2=True,  # Enable HTTP/2 if available
        )

        logger.debug(f"Created curl_cffi client for data retrieval")
        return client

    def _calculate_chunks(
        self, start_ms: int, end_ms: int, interval: Interval
    ) -> List[Tuple[int, int]]:
        """Calculate chunk ranges based on start and end times.

        This method divides the time range into chunks that respect the API limit
        of 1000 records per request. It accounts for the API's boundary behavior
        where startTime is rounded up and endTime is rounded down to interval
        boundaries.

        Args:
            start_ms: Start time in milliseconds
            end_ms: End time in milliseconds
            interval: Time interval

        Returns:
            List of (chunk_start, chunk_end) tuples for each chunk
        """
        chunks = []
        current_start = start_ms

        # Get interval duration in milliseconds
        interval_ms = interval.to_seconds() * 1000

        # Calculate records per chunk - API max is 1000
        records_per_chunk = self.CHUNK_SIZE  # default 1000

        # For futures markets, use 1500 as the chunk size
        if self.market_type in (
            MarketType.FUTURES_USDT,
            MarketType.FUTURES_COIN,
            MarketType.FUTURES,
        ):
            records_per_chunk = 1500  # Futures markets support 1500 records per request

        # Calculate chunk duration based on interval - simple approach
        # The chunk size is determined by the maximum number of records (1000 or 1500)
        # multiplied by the interval duration
        chunk_ms = records_per_chunk * interval_ms

        logger.debug(
            f"Using chunk size: {chunk_ms/(24*60*60*1000):.4f}d ({records_per_chunk} records) for {interval.value}"
        )

        # Process chunks with proper boundary alignment
        while current_start < end_ms:
            # Calculate end of this chunk
            chunk_end = min(current_start + chunk_ms, end_ms)

            # Add the chunk
            chunks.append((current_start, chunk_end))

            # Move to next chunk (add 1ms to avoid overlap)
            current_start = chunk_end + 1

        logger.debug(
            f"Calculated {len(chunks)} chunks for time range spanning {(end_ms - start_ms) / (24*60*60*1000):.2f} days"
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

    def _align_interval_boundaries(
        self, start_time: datetime, end_time: datetime, interval: Interval
    ) -> Tuple[datetime, datetime]:
        """Align time boundaries according to Binance REST API behavior.

        The Binance REST API applies specific boundary handling:
        - startTime: Rounds UP to the next interval boundary if not exactly on boundary
        - endTime: Rounds DOWN to the previous interval boundary if not exactly on boundary

        This method pre-aligns times to match the API's natural behavior, which helps
        with accurate pagination and chunk calculations.

        Args:
            start_time: Start time to align
            end_time: End time to align
            interval: Time interval

        Returns:
            Tuple of (aligned_start_time, aligned_end_time)
        """
        # Use the unified implementation from time_utils
        aligned_start, aligned_end = align_time_boundaries(
            start_time, end_time, interval
        )

        logger.debug(
            f"REST client aligned boundaries: {start_time} -> {aligned_start}, {end_time} -> {aligned_end}"
        )

        return aligned_start, aligned_end

    async def fetch(
        self,
        symbol: str,
        interval: Interval,
        start_time: datetime,
        end_time: datetime,
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Fetch klines data for given symbol and time range.

        This method handles:
        - Chunking large time ranges into multiple requests
        - Handling 1-second data with special optimizations
        - Validating and filtering results
        - Rate limit handling and endpoint rotation

        Args:
            symbol: Trading pair symbol (e.g. 'BTCUSDT')
            interval: Time interval (e.g. Interval.MINUTE_1)
            start_time: Start time for data retrieval
            end_time: End time for data retrieval

        Returns:
            Tuple of (DataFrame with klines data, stats dictionary)
        """
        # Validate inputs
        symbol = symbol.upper()
        self._validate_request_params(symbol, interval, start_time, end_time)

        # Apply comprehensive time boundary validation - avoids redundant checks
        start_time, end_time, metadata = DataValidation.validate_query_time_boundaries(
            start_time, end_time, handle_future_dates="error", interval=interval
        )

        # Get data availability info but don't log warnings yet
        data_availability_message = metadata.get("data_availability_message", "")
        is_data_likely_available = metadata.get("data_likely_available", True)

        # Log other warnings from validation
        for warning in metadata.get("warnings", []):
            logger.warning(warning)

        # Align boundaries for consistency
        aligned_start, aligned_end = align_time_boundaries(
            start_time, end_time, interval
        )
        logger.debug(
            f"REST client aligned boundaries: {start_time} -> {aligned_start}, {end_time} -> {aligned_end}"
        )

        # Use centralized interval->seconds conversion
        seconds_per_bar = interval.to_seconds()

        # Decide on chunk size based on interval
        records_per_day = 86400 // seconds_per_bar  # seconds in day / seconds per bar
        days_per_chunk = self.CHUNK_SIZE / records_per_day
        logger.debug(
            f"Using chunk size: {days_per_chunk:.4f}d ({self.CHUNK_SIZE} records) for {interval.value}"
        )

        # Calculate time range in days
        time_range_days = (aligned_end - aligned_start).total_seconds() / 86400
        logger.debug(f"Calculated time range: {time_range_days:.2f} days")

        # Calculate number of chunks needed
        total_chunks = math.ceil(time_range_days / days_per_chunk)
        # Limit number of chunks to avoid excessive requests
        MAX_CHUNKS = 5
        if total_chunks > MAX_CHUNKS and total_chunks * self.CHUNK_SIZE > 10000:
            total_chunks = MAX_CHUNKS
            logger.warning(
                f"Time range too large, limiting to {MAX_CHUNKS} chunks to avoid excessive requests"
            )

        logger.info(
            f"Fetching {symbol} {interval.value} data from {aligned_start} to {aligned_end} in {total_chunks} chunks"
        )

        # Stats to track request info
        stats = {
            "requests": 0,
            "errors": 0,
            "records": 0,
            "chunks": total_chunks,
            "completed_chunks": 0,
        }

        # Convert times to milliseconds for API
        start_ms = int(aligned_start.timestamp() * 1000)
        end_ms = int(aligned_end.timestamp() * 1000)

        # Calculate chunks
        chunks = self._calculate_chunks(start_ms, end_ms, interval)

        # Track time for performance analysis
        t_start = time.time()

        # Set up timeout for the overall fetch operation
        effective_timeout = min(
            MAX_TIMEOUT, self.fetch_timeout * 2
        )  # Double normal timeout, but cap at MAX_TIMEOUT

        # Create a task for the chunked fetch operation
        all_chunks_task = asyncio.create_task(
            self._fetch_all_chunks(symbol, interval, chunks, stats)
        )

        try:
            # Wait for the task with timeout
            results = await asyncio.wait_for(all_chunks_task, timeout=effective_timeout)

            # Calculate the actual time taken
            t_end = time.time()
            fetch_time = t_end - t_start
            records_per_sec = stats["records"] / fetch_time if fetch_time > 0 else 0

            # Log success if we have data
            if results and len(results) > 0:
                logger.info(
                    f"Successfully retrieved {stats['records']} records for {symbol} from {start_time} to {end_time}"
                )
                logger.debug(
                    f"Fetch completed in {fetch_time:.2f}s ({records_per_sec:.2f} records/s)"
                )

                # Process the results into a DataFrame
                if results:
                    all_data = []
                    # Only try to iterate through results if it's not a CancelledError
                    if not isinstance(results, asyncio.CancelledError) and isinstance(
                        results, list
                    ):
                        for chunk_data in results:
                            if chunk_data and isinstance(chunk_data, list):
                                all_data.extend(chunk_data)

                        # Process the accumulated data
                        if all_data:
                            try:
                                df = process_kline_data(all_data)
                                if not df.empty:
                                    # Filter for the exact requested time range
                                    final_df = df[
                                        (df.index >= start_time)
                                        & (df.index <= end_time)
                                    ].copy()

                                    # Validate result
                                    if not final_df.empty:
                                        stats["records"] = len(final_df)
                                        return final_df, stats
                            except Exception as e:
                                logger.error(f"Error processing data: {str(e)}")
                                stats["errors"] += 1
                    else:
                        if isinstance(results, asyncio.CancelledError):
                            logger.debug(
                                f"Cannot process results: operation was cancelled (normal during concurrent operations)"
                            )
                        else:
                            logger.warning(
                                f"Cannot process results: expected list but got {type(results)}"
                            )

            # Return empty DataFrame if no results
            if all_chunks_task.cancelled():
                # This is expected during cancellations
                logger.debug(
                    f"No data available for {symbol} from {start_time} to {end_time} due to task cancellation"
                )
            else:
                # Only log at debug level since empty results are common and expected
                logger.debug(
                    f"No data returned for {symbol} from {start_time} to {end_time}"
                )

                # If we have a data availability warning, log it now since the retrieval failed
                if data_availability_message and not is_data_likely_available:
                    # Calculate expected records more precisely based on interval
                    time_diff = (end_time - start_time).total_seconds()
                    interval_seconds = interval.to_seconds()
                    expected_records = max(1, int(time_diff / interval_seconds))
                    actual_records = stats.get("records", 0)

                    # Calculate the shortage and percentage
                    records_shortage = expected_records - actual_records
                    completion_pct = (
                        (actual_records / expected_records * 100)
                        if expected_records > 0
                        else 0
                    )

                    # Only show warning if we're actually missing records
                    if records_shortage > 0:
                        # Get time range information if available
                        time_range_info = ""
                        if "time_range" in metadata:
                            tr = metadata["time_range"]
                            start_time_str = tr.get("start_time", "unknown")
                            end_time_str = tr.get("end_time", "unknown")
                            span_seconds = tr.get("time_span_seconds", 0)
                            time_range_info = f" [Range: {start_time_str} -> {end_time_str}, span: {span_seconds:.1f}s]"

                        logger.warning(
                            f"{data_availability_message} Retrieved {actual_records}/{expected_records} records "
                            f"({completion_pct:.1f}% complete, missing {records_shortage} records) "
                            f"for symbol {symbol} with interval {interval.value}{time_range_info}"
                        )

                        # Try to retrieve partial data for historical intervals only
                        if "time_range" in metadata and metadata.get("time_range"):
                            # If end time is very recent, try fetching with truncated end time
                            tr = metadata["time_range"]
                            current_time = datetime.now(timezone.utc)
                            cutoff_time = current_time - timedelta(
                                seconds=60
                            )  # Use 1 minute as buffer

                            # If end time is extremely recent, truncate to safe boundary
                            if end_time > cutoff_time:
                                logger.info(
                                    f"Attempting to retrieve historical data up to {cutoff_time.isoformat()} for {symbol}"
                                )
                                # Create new chunks for the truncated range
                                modified_end_ms = int(cutoff_time.timestamp() * 1000)
                                historical_chunks = self._calculate_chunks(
                                    start_ms, modified_end_ms, interval
                                )

                                # Get only historical data
                                try:
                                    t_start_historical = time.time()
                                    historical_task = asyncio.create_task(
                                        self._fetch_all_chunks(
                                            symbol, interval, historical_chunks, stats
                                        )
                                    )
                                    historical_results = await asyncio.wait_for(
                                        historical_task, timeout=effective_timeout
                                    )

                                    # Process historical results
                                    logger.debug(
                                        "Creating empty DataFrame for fallback"
                                    )
                                    df = self.create_empty_dataframe()

                                    if (
                                        historical_results
                                        and isinstance(historical_results, list)
                                        and all(
                                            isinstance(x, list)
                                            for x in historical_results
                                        )
                                    ):
                                        logger.debug("Processing historical results")
                                        all_data = []
                                        for chunk in historical_results:
                                            if chunk:
                                                all_data.extend(chunk)

                                        if all_data:
                                            logger.debug(
                                                f"Processing {len(all_data)} historical records"
                                            )
                                            historical_df = process_kline_data(all_data)

                                            # Filter for exact time range
                                            filtered_df = historical_df[
                                                (historical_df.index >= start_time)
                                                & (historical_df.index <= cutoff_time)
                                            ].copy()

                                            if not filtered_df.empty:
                                                logger.debug(
                                                    f"Successfully processed {len(filtered_df)} historical records"
                                                )
                                                df = filtered_df
                                        else:
                                            logger.debug("No historical data available")
                                    else:
                                        logger.debug(
                                            f"Invalid historical results format: {type(historical_results)}"
                                        )

                                    # Always return what we have, even if empty
                                    t_end_historical = time.time()
                                    logger.debug(
                                        f"Historical data fetch completed in {t_end_historical - t_start_historical:.2f}s"
                                    )
                                    return df, stats
                                except Exception as e:
                                    logger.error(
                                        f"Error retrieving historical data: {str(e)}"
                                    )
                                    # Continue with return of empty DataFrame

            # Create empty DataFrame with proper structure if we've reached this point
            logger.debug("Creating empty DataFrame before final return")
            df = self.create_empty_dataframe()
            logger.debug("Returning empty DataFrame")
            return df, stats

        except asyncio.TimeoutError:
            # Calculate elapsed time
            elapsed = time.time() - t_start

            # Log timeout to both console and dedicated log file
            logger.log_timeout(
                operation=f"REST API fetch for {symbol} {interval.value}",
                timeout_value=effective_timeout,
                details={
                    "symbol": symbol,
                    "interval": interval.value,
                    "market_type": self.market_type.name,
                    "start_time": str(start_time),
                    "end_time": str(end_time),
                    "chunks": len(chunks),
                    "elapsed": f"{elapsed:.2f}s",
                    "completed_chunks": stats.get("completed_chunks", 0),
                },
            )

            # Cancel the task
            if not all_chunks_task.done():
                logger.warning("Cancelling REST API fetch task due to timeout")
                all_chunks_task.cancel()

                # Wait briefly for cancellation to complete
                try:
                    await asyncio.wait_for(
                        asyncio.gather(all_chunks_task, return_exceptions=True),
                        timeout=0.5,
                    )
                    logger.debug("Successfully cancelled REST API fetch task")
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    logger.warning("Failed to cancel REST API fetch task in time")

            # Clean up any hanging tasks
            await self._cleanup_force_timeout_tasks()

            logger.error(f"REST API fetch timed out after {effective_timeout}s")
            return self.create_empty_dataframe(), stats

        except asyncio.CancelledError as e:
            # Properly handle cancellation without trying to iterate the error
            logger.debug(
                f"REST API fetch was cancelled (expected during concurrent operations)"
            )

            # Clean up any hanging tasks
            await self._cleanup_force_timeout_tasks()

            stats["errors"] += 1
            return self.create_empty_dataframe(), stats

        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            stats["errors"] += 1

            # Clean up any hanging tasks
            await self._cleanup_force_timeout_tasks()

            return self.create_empty_dataframe(), stats

    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the expected structure.

        Returns:
            Empty DataFrame with proper column structure
        """
        return create_empty_dataframe(ChartType.KLINES)

    async def _fetch_all_chunks(self, symbol, interval, chunks, stats):
        """Fetch all chunks and aggregate results.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            chunks: List of (start_ms, end_ms) tuples representing chunks
            stats: Dictionary to track statistics

        Returns:
            List of chunk results
        """
        # Track active tasks for cleanup in case of timeout
        self._active_tasks = []

        # Create a semaphore to limit concurrent requests
        sem = asyncio.Semaphore(self.max_concurrent)

        # Process chunks sequentially to avoid unnecessary concurrent operations
        successful_results = []

        for i, (chunk_start, chunk_end) in enumerate(chunks):
            try:
                # Fetch each chunk one at a time using the semaphore
                logger.debug(
                    f"Processing chunk {i+1}/{len(chunks)} ({symbol} {interval.value})"
                )

                result = await self._fetch_chunk_with_semaphore(
                    symbol, interval, chunk_start, chunk_end, sem, i, len(chunks)
                )

                # Process the result
                if isinstance(result, Exception):
                    if isinstance(result, asyncio.CancelledError):
                        logger.debug(f"Chunk {i+1}/{len(chunks)} was cancelled")
                    else:
                        logger.error(
                            f"Chunk {i+1}/{len(chunks)} failed: {type(result).__name__}: {result}"
                        )
                    stats["errors"] += 1
                else:
                    # Only process valid results (lists of kline data)
                    if isinstance(result, list) and result:
                        successful_results.append(result)
                        stats["records"] += len(result)
                        stats["completed_chunks"] += 1
                    else:
                        if not result:
                            logger.debug(f"Chunk {i+1}/{len(chunks)} returned no data")
                        elif not isinstance(result, list):
                            logger.debug(
                                f"Expected list result but got {type(result)} (normal during concurrent operations)"
                            )

            except asyncio.CancelledError:
                # Handle cancellation of the entire operation
                logger.debug(
                    f"Operation cancelled while processing chunk {i+1}/{len(chunks)}"
                )
                stats["errors"] += 1
                break

            except Exception as e:
                logger.error(f"Error processing chunk {i+1}/{len(chunks)}: {str(e)}")
                stats["errors"] += 1

        # Return the results
        if successful_results:
            return successful_results
        else:
            logger.debug(f"No successful results obtained from any chunks")
            return []

    def _force_cleanup_active_tasks(self):
        """Force cleanup of active tasks."""
        try:
            for task in self._active_tasks:
                if not task.done():
                    task.cancel()
            self._active_tasks = []
        except Exception as e:
            logger.error(f"Error during active tasks cleanup: {e}")

    async def _fetch_chunk_with_semaphore(
        self, symbol, interval, start_ms, end_ms, semaphore, chunk_idx=0, total_chunks=1
    ):
        """Fetch a chunk of data with semaphore control.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            start_ms: Chunk start time in milliseconds
            end_ms: Chunk end time in milliseconds
            semaphore: Semaphore for controlling concurrency
            chunk_idx: Index of current chunk (for logging)
            total_chunks: Total number of chunks (for logging)

        Returns:
            List of klines data for this chunk
        """
        async with semaphore:
            try:
                logger.debug(
                    f"Fetching chunk {chunk_idx+1}/{total_chunks} ({symbol} {interval.value})"
                )

                # Actual fetch logic
                chunk_data = await self._fetch_klines_chunk(
                    symbol, interval, start_ms, end_ms
                )

                # Validate response is a list
                if not isinstance(chunk_data, list):
                    logger.warning(
                        f"Chunk {chunk_idx+1}/{total_chunks}: Expected list result but got {type(chunk_data)}"
                    )
                    return []

                stats_msg = f"Chunk {chunk_idx+1}/{total_chunks}: got {len(chunk_data) if chunk_data else 0} records"
                if chunk_idx == 0 or chunk_idx == total_chunks - 1 or total_chunks <= 5:
                    logger.info(stats_msg)
                else:
                    logger.debug(stats_msg)

                return chunk_data
            except asyncio.CancelledError:
                # This is expected during high concurrency operations
                logger.debug(
                    f"Chunk {chunk_idx+1}/{total_chunks} was cancelled (normal during concurrent operations)"
                )
                # Important: Re-raise CancelledError to properly propagate cancellation
                raise
            except Exception as e:
                logger.warning(
                    f"Error fetching chunk {chunk_idx+1}/{total_chunks}: {str(e)}"
                )
                return []

    async def _fetch_klines_chunk(self, symbol, interval, start_ms, end_ms):
        """Fetch a single chunk of klines data from the REST API.

        Args:
            symbol: Trading pair symbol (e.g. 'BTCUSDT')
            interval: Time interval (e.g. Interval.MINUTE_1)
            start_ms: Chunk start time in milliseconds
            end_ms: Chunk end time in milliseconds

        Returns:
            List of klines data for this chunk
        """
        # Initialize client if needed
        if not self._client:
            self._client = self._create_optimized_client()
            self._client_is_external = False

        # Format symbol properly for the market type
        formatted_symbol = symbol
        if self.market_type == MarketType.FUTURES_COIN and "_PERP" not in symbol:
            formatted_symbol = f"{symbol}_PERP"

        # Prepare request parameters
        params = {
            "symbol": formatted_symbol,
            "interval": interval.value,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": self.CHUNK_SIZE,
        }

        # Get appropriate endpoint URL
        endpoint_url = self._get_klines_endpoint()

        # Track request stats
        attempt = 0
        max_attempts = self.retry_count + 1  # +1 because first attempt isn't a retry

        # Retry logic for robustness
        while attempt < max_attempts:
            attempt += 1

            try:
                # Set up per-request timeout
                request_timeout = min(
                    self.fetch_timeout, MAX_TIMEOUT / 2
                )  # Use half of MAX_TIMEOUT as max request timeout

                # Make the actual API request
                response = await asyncio.wait_for(
                    self._client.get(
                        endpoint_url,
                        params=params,
                        headers={"Content-Type": "application/json"},
                    ),
                    timeout=request_timeout,
                )

                # Check status code
                if response.status_code != 200:
                    error_info = f"HTTP {response.status_code}"
                    try:
                        error_data = response.json()
                        if isinstance(error_data, dict):
                            error_info = f"{error_info}: {error_data.get('msg', 'Unknown error')}"
                    except:
                        pass

                    logger.warning(f"API error: {error_info}")

                    # Handle specific error cases
                    if response.status_code == 429:  # Rate limit
                        retry_after = response.headers.get("Retry-After", "5")
                        wait_time = min(int(retry_after), 5)  # Cap at 5 seconds
                        logger.warning(
                            f"Rate limited. Waiting {wait_time}s before retry."
                        )
                        await asyncio.sleep(wait_time)
                    elif response.status_code in [418, 403]:  # IP ban or forbidden
                        logger.error(
                            f"Access denied (code {response.status_code}). API key may be restricted."
                        )
                        raise ValueError(f"Access denied: {error_info}")
                    elif response.status_code >= 500:  # Server error
                        logger.warning(f"Server error. Retrying in 2s.")
                        await asyncio.sleep(2)
                    else:  # Other errors
                        logger.warning(f"Request failed. Retrying in 1s.")
                        await asyncio.sleep(1)

                    # Only retry if we haven't exceeded max attempts
                    if attempt >= max_attempts:
                        logger.error(
                            f"Max retry attempts ({max_attempts}) reached for {symbol}."
                        )
                        return []
                    continue

                # Parse response data
                data = response.json()

                # Validate data structure
                if not isinstance(data, list):
                    logger.warning(f"Unexpected response format: {type(data).__name__}")
                    return []

                # Return the data
                return data

            except asyncio.CancelledError:
                # Important: Re-raise CancelledError to properly propagate it
                logger.debug(
                    f"API request was cancelled (normal during high concurrency)"
                )
                raise

            except asyncio.TimeoutError:
                logger.warning(f"Request timeout (attempt {attempt}/{max_attempts})")
                if attempt >= max_attempts:
                    logger.error(f"Max retry attempts reached after timeouts")
                    return []
                # Backoff on retry
                await asyncio.sleep(min(1 * attempt, 3))

            except Exception as e:
                logger.warning(
                    f"Error during API request (attempt {attempt}/{max_attempts}): {str(e)}"
                )
                if attempt >= max_attempts:
                    logger.error(f"Max retry attempts reached after errors")
                    return []
                # Backoff on retry
                await asyncio.sleep(min(1 * attempt, 3))

        # If we get here, all attempts failed
        return []
