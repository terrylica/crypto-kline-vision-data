#!/usr/bin/env python

"""Unified REST API data client with optimized 1-second data handling."""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
import time

# Import curl_cffi for better performance
from curl_cffi.requests import AsyncSession

from utils.logger_setup import logger
from utils.market_constraints import (
    Interval,
    MarketType,
    ChartType,
    get_endpoint_url,
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
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    API_TIMEOUT,
)
from utils.validation_utils import validate_date_range_for_api


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
        fetch_timeout: float = API_TIMEOUT,  # Use standardized API_TIMEOUT from config
    ):
        """Initialize the RestDataClient.

        Args:
            market_type: Market type (spot, futures, etc.)
            max_concurrent: Maximum concurrent API requests
            retry_count: Number of retries for failed requests
            client: Optional existing client session (curl_cffi AsyncSession)
            fetch_timeout: Timeout in seconds for API fetch operations (default: from config)
        """
        self.market_type = market_type
        self.CHUNK_SIZE = 1000  # Maximum number of records per API request
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._retry_count = retry_count
        self._fetch_timeout = min(fetch_timeout, 5.0)  # Cap at 5 seconds per guidelines

        # Get endpoints from market_constraints
        self._base_url = get_endpoint_url(market_type, ChartType.KLINES)
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

            self._client = create_client(timeout=DEFAULT_HTTP_TIMEOUT_SECONDS)
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
                retry_count = self._retry_count
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

    def _create_optimized_client(self) -> AsyncSession:
        """Create an optimized client based on hardware capabilities."""
        concurrency_info = self.hw_monitor.calculate_optimal_concurrency()
        return create_client(
            max_connections=concurrency_info["optimal_concurrency"],
            timeout=DEFAULT_HTTP_TIMEOUT_SECONDS,  # Using standard timeout
        )

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
        # Get interval in seconds
        interval_seconds = interval.to_seconds()

        # Extract seconds since epoch for calculations
        start_seconds = start_time.timestamp()
        end_seconds = end_time.timestamp()

        # Calculate floor of each timestamp to interval boundary
        start_floor = int(start_seconds) - (int(start_seconds) % interval_seconds)
        end_floor = int(end_seconds) - (int(end_seconds) % interval_seconds)

        # Apply Binance API boundary rules:
        # - startTime: Round UP to next interval boundary if not exactly on boundary
        # - endTime: Round DOWN to previous interval boundary if not exactly on boundary
        if start_seconds != start_floor:
            aligned_start = datetime.fromtimestamp(
                start_floor + interval_seconds, tz=timezone.utc
            )
        else:
            aligned_start = datetime.fromtimestamp(start_floor, tz=timezone.utc)

        aligned_end = datetime.fromtimestamp(end_floor, tz=timezone.utc)

        logger.debug(
            f"Aligned boundaries: {start_time} -> {aligned_start}, {end_time} -> {aligned_end}"
        )

        return aligned_start, aligned_end

    async def fetch(
        self,
        symbol: str,
        interval: Interval,
        start_time: datetime,
        end_time: datetime,
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Fetch market data from Binance API.

        This method implements time-based chunking pagination to handle large data requests
        efficiently. It splits the time range into appropriate chunks based on the interval
        and fetches them concurrently with proper rate limit handling.

        The pagination strategy:
        1. Divides the time range into optimal chunks based on interval size
        2. Executes concurrent requests for all chunks with semaphore control
        3. Handles rate limiting with endpoint rotation and exponential backoff
        4. Aggregates results from all chunks into a single DataFrame

        This approach is robust across different interval types and handles API boundary
        behaviors where startTime is rounded up and endTime is rounded down to interval
        boundaries.

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

        # Validate date range to prevent requesting future data
        is_valid, error_message = validate_date_range_for_api(start_time, end_time)
        if not is_valid:
            logger.warning(f"Invalid date range: {error_message}")
            return self.create_empty_dataframe(), {
                "error": "invalid_date_range",
                "error_message": error_message,
                "chunks": 0,
                "total_records": 0,
            }

        # Handle symbol formatting for FUTURES_COIN market type
        formatted_symbol = symbol
        if self.market_type == MarketType.FUTURES_COIN and "_PERP" not in symbol:
            # Append _PERP suffix for coin-margined futures
            formatted_symbol = f"{symbol}_PERP"
            logger.debug(f"Adjusted symbol for FUTURES_COIN market: {formatted_symbol}")

        # Test connectivity to Binance API with actual endpoint we'll use
        # Use the klines endpoint with actual parameters for a more accurate test
        # Get the properly constructed URL using get_endpoint_url with ChartType.KLINES
        test_url = f"{get_endpoint_url(self.market_type, ChartType.KLINES)}?symbol={formatted_symbol}&interval={interval.value}&limit=1"

        logger.debug(f"Testing connectivity to {test_url}")

        try:
            api_status = await asyncio.wait_for(
                test_connectivity(
                    self._client,
                    url=test_url,
                    timeout=DEFAULT_HTTP_TIMEOUT_SECONDS,
                    retry_count=3,  # Increase retry count for more reliability
                ),
                timeout=self._fetch_timeout,
            )
        except asyncio.TimeoutError:
            logger.error(
                f"Connectivity test timed out after {self._fetch_timeout}s for {test_url}"
            )
            return self.create_empty_dataframe(), {
                "error": "connectivity_timeout",
                "chunks": 0,
                "total_records": 0,
            }

        if not api_status:
            logger.error(f"Cannot connect to Binance API at {test_url}")
            # Even if connectivity test fails, try to proceed with the actual data fetch
            # as the test might be failing due to environment restrictions but actual
            # data retrieval might work

        # Ensure timezone awareness
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        # Validate request parameters
        self._validate_request_params(symbol, interval, start_time, end_time)

        # Align boundaries to match API behavior
        # This ensures proper time slicing and avoids rounding issues
        aligned_start, aligned_end = self._align_interval_boundaries(
            start_time, end_time, interval
        )

        # Convert aligned datetime objects to milliseconds since epoch
        start_ms = int(aligned_start.timestamp() * 1000)
        end_ms = int(aligned_end.timestamp() * 1000)

        # Reset stats for this fetch
        self.stats = {
            "total_records": 0,
            "chunks_processed": 0,
            "chunks_failed": 0,
            "chunks": 0,
        }

        # Calculate chunk boundaries
        chunks = self._calculate_chunks(start_ms, end_ms, interval)
        num_chunks = len(chunks)
        self.stats["chunks"] = num_chunks

        logger.info(
            f"Fetching {symbol} {interval.value} data from "
            f"{aligned_start} to {aligned_end} in {num_chunks} chunks"
        )

        # Get optimal concurrency value
        optimal_concurrency_result = self.hw_monitor.calculate_optimal_concurrency()
        optimal_concurrency = optimal_concurrency_result["optimal_concurrency"]

        # Limit semaphore to optimal concurrency
        sem = asyncio.Semaphore(optimal_concurrency)

        # Create tasks for all chunks with timeout
        tasks = []
        for chunk_start, chunk_end in chunks:
            task = asyncio.create_task(
                asyncio.wait_for(
                    self._fetch_chunk_with_semaphore(
                        symbol, interval, chunk_start, chunk_end, sem
                    ),
                    timeout=self._fetch_timeout,
                )
            )
            tasks.append(task)

        # Wait for all tasks to complete
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching data for {symbol} {interval.value}")
            return self.create_empty_dataframe(), {
                "error": "fetch_timeout",
                "chunks": num_chunks,
                "chunks_processed": self.stats["chunks_processed"],
                "chunks_failed": self.stats["chunks_failed"],
                "total_records": 0,
            }

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
