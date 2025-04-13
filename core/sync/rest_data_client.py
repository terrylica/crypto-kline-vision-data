#!/usr/bin/env python
"""Client for fetching market data from REST APIs with synchronous implementation."""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
import pandas as pd

from utils.logger_setup import logger
from utils.time_utils import (
    datetime_to_milliseconds,
    milliseconds_to_datetime,
    filter_dataframe_by_time,
    align_time_boundaries,
)
from utils.market_constraints import (
    Interval,
    MarketType,
    ChartType,
    DataProvider,
)
from utils.validation import DataFrameValidator
from utils.config import (
    OUTPUT_DTYPES,
    REST_MAX_CHUNKS,
    DEFAULT_HTTP_TIMEOUT_SECONDS,
)
from utils.hardware_monitor import HardwareMonitor
from core.sync.data_client_interface import DataClientInterface

# Define the column names as a constant since they aren't in config.py
OUTPUT_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
]


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to ensure consistent naming.

    Args:
        df: DataFrame to standardize

    Returns:
        DataFrame with standardized column names
    """
    # Define mappings for column name standardization
    column_mapping = {
        # Quote volume variants
        "quote_volume": "quote_asset_volume",
        "quote_vol": "quote_asset_volume",
        # Trade count variants
        "trades": "count",
        "number_of_trades": "count",
        # Taker buy base volume variants
        "taker_buy_base": "taker_buy_volume",
        "taker_buy_base_volume": "taker_buy_volume",
        "taker_buy_base_asset_volume": "taker_buy_volume",
        # Taker buy quote volume variants
        "taker_buy_quote": "taker_buy_quote_volume",
        "taker_buy_quote_asset_volume": "taker_buy_quote_volume",
        # Time field variants
        "time": "open_time",
        "timestamp": "open_time",
        "date": "open_time",
    }

    # Rename columns that need standardization
    for col in df.columns:
        if col.lower() in column_mapping:
            df = df.rename(columns={col: column_mapping[col.lower()]})

    return df


def process_kline_data(raw_data: List[List]) -> pd.DataFrame:
    """Process raw kline data into a structured DataFrame.

    Args:
        raw_data: Raw kline data from the API

    Returns:
        Processed DataFrame with standardized columns
    """
    # Create DataFrame from raw data
    df = pd.DataFrame(
        raw_data,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
            "ignore",
        ],
    )

    # Convert times to datetime
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    # Convert strings to floats
    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]:
        df[col] = df[col].astype(float)

    # Convert number of trades to integer
    df["number_of_trades"] = df["number_of_trades"].astype(int)

    # Drop the ignore column
    df = df.drop(columns=["ignore"])

    # Add extended columns based on existing data
    df = standardize_column_names(df)

    # Ensure we consistently return a DataFrame with open_time as a column, never as an index
    # This prevents downstream ambiguity
    if (
        hasattr(df, "index")
        and hasattr(df.index, "name")
        and df.index.name == "open_time"
    ):
        logger.debug("Resetting index to ensure open_time is a column, not an index")
        df = df.reset_index()

    # Ensure there's only one open_time (column takes precedence over index)
    if (
        hasattr(df, "index")
        and hasattr(df.index, "name")
        and df.index.name == "open_time"
        and "open_time" in df.columns
    ):
        logger.debug("Resolving ambiguous open_time by keeping only the column version")
        df = df.reset_index(drop=True)

    # Perform any REST API-specific post-processing here if needed
    return df


class RestDataClient(DataClientInterface):
    """RestDataClient for market data with chunking, retries, and rate limiting.

    This class handles fetching klines data with proper rate limit handling,
    automatic chunking for large time ranges, and simple retry logic.
    """

    # Constants for chunk sizing
    CHUNK_SIZE = 1000  # Default chunk size (max records per request for most endpoints)

    def __init__(
        self,
        market_type: MarketType,
        retry_count: int = 3,
        fetch_timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
        client=None,
        symbol: str = "BTCUSDT",
        interval: Interval = Interval.MINUTE_1,
    ):
        """Initialize the REST data client.

        Args:
            market_type: Market type to use (spot, futures_usdt, futures_coin)
            retry_count: Number of retry attempts for failed requests
            fetch_timeout: Timeout in seconds for fetch operations
            client: Optional pre-configured HTTP client
            symbol: Default symbol to use if not specified in fetch calls
            interval: Default interval to use if not specified in fetch calls
        """
        self.market_type = market_type
        self.retry_count = retry_count
        self.fetch_timeout = fetch_timeout
        self._client = client
        self._symbol = symbol
        self._interval = interval

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

        logger.debug(
            f"Initialized RestDataClient with market_type={market_type.name}, "
            f"retry_count={retry_count}"
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

    def __enter__(self):
        """Initialize the client session when entering the context."""
        if self._client is None:
            self._client = self._create_optimized_client()
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Clean up resources when exiting the context."""
        if self._client and hasattr(self._client, "close"):
            self._client.close()
            self._client = None
            logger.debug("Closed HTTP client")

    def _fetch_chunk(
        self, endpoint: str, params: Dict[str, Any], retry_count: int = 0
    ) -> List[List[Any]]:
        """Fetch a chunk of data with retry logic.

        Args:
            endpoint: API endpoint URL
            params: Request parameters
            retry_count: Current retry attempt

        Returns:
            List of data points from the API

        Raises:
            Exception: If all retry attempts fail
        """
        current_attempt = 0
        last_error = None

        # Import random only if needed for jitter
        import random

        while current_attempt <= retry_count:
            try:
                if current_attempt > 0:
                    # Exponential backoff with jitter
                    wait_time = min(
                        0.1 * (2**current_attempt) + 0.1 * random.random(), 5.0
                    )
                    logger.warning(
                        f"Retrying request in {wait_time:.2f}s (attempt {current_attempt+1}/{retry_count+1})"
                    )
                    time.sleep(wait_time)

                # Initialize client if not already done
                if self._client is None:
                    self._client = self._create_optimized_client()

                # Send the request with proper headers and explicit timeout
                response = self._client.get(
                    endpoint,
                    params=params,
                    timeout=self.fetch_timeout,
                )

                # Check for HTTP error codes
                if response.status_code != 200:
                    error_msg = f"HTTP error {response.status_code}: {response.text}"
                    logger.warning(
                        f"Error response from {endpoint}, attempt {current_attempt+1}/{retry_count+1}: {error_msg}"
                    )
                    last_error = Exception(error_msg)
                    current_attempt += 1
                    continue

                # Parse JSON response
                data = response.json()

                # Check for API error
                if (
                    isinstance(data, dict)
                    and "code" in data
                    and data.get("code", 0) != 0
                ):
                    error_msg = f"API error {data.get('code')}: {data.get('msg', 'Unknown error')}"
                    logger.warning(
                        f"API error from {endpoint}, attempt {current_attempt+1}/{retry_count+1}: {error_msg}"
                    )
                    last_error = Exception(error_msg)
                    current_attempt += 1
                    continue

                return data

            except Exception as e:
                logger.warning(
                    f"Error fetching data from {endpoint}, attempt {current_attempt+1}/{retry_count+1}: {e}"
                )
                last_error = e
                current_attempt += 1

        # If we reach here, all retries failed
        logger.error(f"All retry attempts failed: {last_error}")
        raise last_error or Exception("Unknown error during data fetch")

    def _fetch_chunk_data(
        self,
        symbol: str,
        interval: Interval,
        start_ms: int,
        end_ms: int,
    ) -> List[List[Any]]:
        """Fetch a chunk of data for a specific time range.

        Args:
            symbol: Trading pair symbol
            interval: Kline interval
            start_ms: Start time in milliseconds
            end_ms: End time in milliseconds

        Returns:
            List of kline data
        """
        # Set up parameters for the request
        params = {
            "symbol": symbol,
            "interval": interval.value,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": self.CHUNK_SIZE,
        }

        # Get the appropriate endpoint based on market type
        endpoint = self._endpoint

        # Fetch the chunk
        try:
            data = self._fetch_chunk(endpoint, params, self.retry_count)
            if not data:
                logger.debug(
                    f"No data returned for {symbol} in range {start_ms} to {end_ms}"
                )
                return []

            return data
        except Exception as e:
            logger.error(f"Error fetching chunk data: {e}")
            return []

    def _validate_request_params(
        self, symbol: str, interval: Interval, start_time: datetime, end_time: datetime
    ) -> None:
        """Validate request parameters for debugging.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            start_time: Start time
            end_time: End time

        Raises:
            ValueError: If parameters are invalid
        """
        # Validate that we have string parameters where needed
        if not isinstance(symbol, str) or not symbol:
            raise ValueError(f"Symbol must be a non-empty string, got {symbol}")

        # Validate time ranges
        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise ValueError(
                f"Start and end times must be datetime objects, got start={type(start_time)}, end={type(end_time)}"
            )

        if start_time >= end_time:
            raise ValueError(
                f"Start time ({start_time}) must be before end time ({end_time})"
            )

        # Validate interval
        if not isinstance(interval, Interval):
            raise ValueError(f"Interval must be an Interval enum, got {type(interval)}")

    def _create_optimized_client(self) -> Any:
        """Create an optimized HTTP client for REST API requests.

        Returns:
            HTTP client instance optimized for performance
        """
        # Use the requests library for synchronous HTTP requests
        import requests

        session = requests.Session()

        # Configure the session with reasonable defaults
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        return session

    def _calculate_chunks(
        self, start_ms: int, end_ms: int, interval: Interval
    ) -> List[Tuple[int, int]]:
        """Calculate chunk boundaries for a time range.

        This is needed because Binance API limits the number of records per request,
        so we need to break large time ranges into smaller chunks.

        Args:
            start_ms: Start time in milliseconds
            end_ms: End time in milliseconds
            interval: Time interval

        Returns:
            List of (chunk_start_ms, chunk_end_ms) tuples
        """
        # Calculate the interval duration in milliseconds
        interval_ms = self._get_interval_ms(interval)

        # Calculate max time range per request (in milliseconds)
        # This is based on the chunk size limit and interval duration
        max_range_ms = interval_ms * self.CHUNK_SIZE

        # Calculate the number of chunks needed
        chunks = []
        current_start = start_ms

        # Initialize a safety counter to prevent infinite loops
        loop_count = 0
        max_loops = REST_MAX_CHUNKS

        while current_start < end_ms and loop_count < max_loops:
            # Calculate the end of this chunk
            chunk_end = min(current_start + max_range_ms, end_ms)

            # Add the chunk to our list
            chunks.append((current_start, chunk_end))

            # Move to the next chunk
            current_start = chunk_end

            # Safety counter
            loop_count += 1

        if loop_count >= max_loops:
            logger.warning(
                f"Reached maximum chunk limit ({max_loops}) for time range {start_ms} to {end_ms}"
            )

        return chunks

    def _get_interval_ms(self, interval: Interval) -> int:
        """Get the interval duration in milliseconds.

        Args:
            interval: Time interval

        Returns:
            Interval duration in milliseconds
        """
        # Map of interval values to milliseconds
        interval_map = {
            Interval.SECOND_1: 1000,  # 1 second
            Interval.MINUTE_1: 60 * 1000,  # 1 minute
            Interval.MINUTE_3: 3 * 60 * 1000,  # 3 minutes
            Interval.MINUTE_5: 5 * 60 * 1000,  # 5 minutes
            Interval.MINUTE_15: 15 * 60 * 1000,  # 15 minutes
            Interval.MINUTE_30: 30 * 60 * 1000,  # 30 minutes
            Interval.HOUR_1: 60 * 60 * 1000,  # 1 hour
            Interval.HOUR_2: 2 * 60 * 60 * 1000,  # 2 hours
            Interval.HOUR_4: 4 * 60 * 60 * 1000,  # 4 hours
            Interval.HOUR_6: 6 * 60 * 60 * 1000,  # 6 hours
            Interval.HOUR_8: 8 * 60 * 60 * 1000,  # 8 hours
            Interval.HOUR_12: 12 * 60 * 60 * 1000,  # 12 hours
            Interval.DAY_1: 24 * 60 * 60 * 1000,  # 1 day
            Interval.DAY_3: 3 * 24 * 60 * 60 * 1000,  # 3 days
            Interval.WEEK_1: 7 * 24 * 60 * 60 * 1000,  # 1 week
            Interval.MONTH_1: 30 * 24 * 60 * 60 * 1000,  # 1 month (approximation)
        }

        # Return the interval duration
        return interval_map.get(interval, 60 * 1000)  # Default to 1 minute if unknown

    def _validate_bar_duration(self, open_time: datetime, interval: Interval) -> float:
        """Validate the expected duration of a single bar.

        Args:
            open_time: Bar open time
            interval: Time interval

        Returns:
            Expected bar duration in seconds
        """
        # Implementation omitted, retained as a placeholder for future use
        return 0.0

    def _validate_historical_bars(
        self, df: pd.DataFrame, current_time: datetime
    ) -> int:
        """Validate and count historical bars.

        Args:
            df: DataFrame with bar data
            current_time: Current time

        Returns:
            Number of historical bars
        """
        # Implementation omitted, retained as a placeholder for future use
        return 0

    def _validate_bar_alignment(self, df: pd.DataFrame, interval: Interval) -> None:
        """Validate that bars are properly aligned to interval boundaries.

        Args:
            df: DataFrame with bar data
            interval: Time interval
        """
        # Implementation omitted, retained as a placeholder for future use

    def _align_interval_boundaries(
        self, start_time: datetime, end_time: datetime, interval: Interval
    ) -> Tuple[datetime, datetime]:
        """Align time boundaries to interval boundaries.

        Args:
            start_time: Start time
            end_time: End time
            interval: Time interval

        Returns:
            Aligned start and end times
        """
        return align_time_boundaries(start_time, end_time, interval)

    def fetch(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch kline data for a symbol and time range.

        This method implements the DataClientInterface fetch method.
        It retrieves data based on the provided parameters and handles chunking
        for large time ranges to stay within API limits.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Time interval string (e.g., "1m", "1h") - must be a valid Interval value
            start_time: Start time for data retrieval (timezone-aware datetime)
            end_time: End time for data retrieval (timezone-aware datetime)
            **kwargs: Additional parameters (unused, for interface compatibility)

        Returns:
            DataFrame with kline data indexed by open_time

        Raises:
            ValueError: If parameters are invalid or inconsistent
        """
        # Convert interval string to Interval enum if needed
        interval_enum = None
        if isinstance(interval, str):
            try:
                # Try direct value lookup first
                interval_enum = next((i for i in Interval if i.value == interval), None)
                if interval_enum is None:
                    # Try by enum name if value lookup failed
                    try:
                        interval_enum = Interval[interval.upper()]
                    except KeyError:
                        raise ValueError(f"Invalid interval: {interval}")
            except Exception as e:
                logger.warning(f"Error converting interval string '{interval}': {e}")
                interval_enum = self._interval  # Fall back to instance default
        else:
            # If it's already an Interval enum, use it directly
            interval_enum = (
                interval if isinstance(interval, Interval) else self._interval
            )

        # Validate request parameters
        self._validate_request_params(symbol, interval_enum, start_time, end_time)

        # Align time boundaries
        aligned_start, aligned_end = align_time_boundaries(
            start_time, end_time, interval_enum
        )

        logger.info(
            f"Fetching {interval_enum.value} data for {symbol} from "
            f"{aligned_start.isoformat()} to {aligned_end.isoformat()}"
        )

        # Convert times to milliseconds
        start_ms = datetime_to_milliseconds(aligned_start)
        end_ms = datetime_to_milliseconds(aligned_end)

        # Calculate chunk boundaries
        chunks = self._calculate_chunks(start_ms, end_ms, interval_enum)

        # Track stats
        stats = {
            "total_chunks": len(chunks),
            "successful_chunks": 0,
            "total_data_points": 0,
        }

        # Fetch data in chunks
        all_data = []
        for i, (chunk_start, chunk_end) in enumerate(chunks):
            logger.debug(
                f"Fetching chunk {i+1}/{len(chunks)} for {symbol}: "
                f"{milliseconds_to_datetime(chunk_start).isoformat()} to "
                f"{milliseconds_to_datetime(chunk_end).isoformat()}"
            )

            # Fetch the chunk
            chunk_data = self._fetch_chunk_data(
                symbol, interval_enum, chunk_start, chunk_end
            )

            if chunk_data:
                all_data.extend(chunk_data)
                stats["successful_chunks"] += 1
                stats["total_data_points"] += len(chunk_data)
                logger.debug(f"Retrieved {len(chunk_data)} records for chunk {i+1}")
            else:
                logger.warning(f"No data returned for chunk {i+1}")

        # If no data was retrieved, return empty DataFrame
        if not all_data:
            logger.warning(
                f"No data retrieved for {symbol} in the specified time range"
            )
            return self.create_empty_dataframe()

        # Process the data into a DataFrame
        df = process_kline_data(all_data)

        # Filter to requested time range - check signature before calling
        try:
            # First try with 4 arguments (newer signature)
            filtered_df = filter_dataframe_by_time(
                df, aligned_start, aligned_end, "open_time"
            )
        except TypeError:
            # Fall back to 3 arguments (older signature)
            filtered_df = filter_dataframe_by_time(df, aligned_start, aligned_end)

        # Log success stats
        logger.info(
            f"Successfully retrieved {len(filtered_df)} records for {symbol} "
            f"(from {stats['successful_chunks']}/{stats['total_chunks']} chunks)"
        )

        return filtered_df

    def _estimate_expected_records(
        self, start_time: datetime, end_time: datetime, interval: Interval
    ) -> int:
        """Estimate the expected number of records for a time range.

        Args:
            start_time: Start time
            end_time: End time
            interval: Time interval

        Returns:
            Estimated number of records
        """
        # Calculate time difference in seconds
        time_diff = (end_time - start_time).total_seconds()

        # Get interval duration in seconds
        interval_seconds = self._get_interval_ms(interval) / 1000

        # Calculate expected records
        expected_records = int(time_diff / interval_seconds) + 1

        return expected_records

    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct structure.

        Returns:
            Empty DataFrame
        """
        # Create an empty DataFrame with the right columns and types
        df = pd.DataFrame(columns=OUTPUT_COLUMNS)
        for col, dtype in OUTPUT_DTYPES.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
        return df

    def close(self) -> None:
        """Close the client and release resources."""
        if self._client and hasattr(self._client, "close"):
            self._client.close()
            self._client = None
            logger.debug("Closed HTTP client")

    @property
    def symbol(self) -> str:
        """Get the symbol.

        Returns:
            The trading pair symbol
        """
        return self._symbol

    @property
    def interval(self) -> Union[str, object]:
        """Get the interval.

        Returns:
            The time interval string
        """
        return (
            self._interval.value
            if hasattr(self._interval, "value")
            else str(self._interval)
        )

    @property
    def provider(self) -> DataProvider:
        """Get the data provider.

        Returns:
            The data provider (always BINANCE for this client)
        """
        return DataProvider.BINANCE

    @property
    def chart_type(self) -> ChartType:
        """Get the chart type.

        Returns:
            The chart type (always KLINES for this client)
        """
        return ChartType.KLINES

    def is_data_available(self, start_time: datetime, end_time: datetime) -> bool:
        """Check if data is available for the specified time range.

        Args:
            start_time: Start time
            end_time: End time

        Returns:
            True if data is available, False otherwise
        """
        # For Binance, data is available from the start of the exchange
        # (September 2017 for most pairs)
        launch_date = datetime(2017, 9, 1, tzinfo=timezone.utc)

        # Check if the requested time range is after the launch date
        if end_time < launch_date:
            return False

        # Check if the requested time range is in the future
        now = datetime.now(timezone.utc)
        if start_time > now:
            return False

        # Otherwise, data should be available
        return True

    def validate_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate that a DataFrame contains valid market data.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        validator = DataFrameValidator(df)
        return validator.validate_klines_data()
