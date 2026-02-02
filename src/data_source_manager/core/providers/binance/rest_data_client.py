#!/usr/bin/env python
# polars-exception: RestDataClient returns pandas DataFrames for DSM pipeline compatibility
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Fix silent failure patterns (BLE001)
"""Client for fetching market data from REST APIs with synchronous implementation."""

from datetime import datetime
from typing import Any

import pandas as pd

from data_source_manager.core.providers.binance.data_client_interface import DataClientInterface
from data_source_manager.utils.config import (
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    REST_MAX_CHUNKS,
)
from data_source_manager.utils.for_core.rest_client_utils import (
    calculate_chunks,
    create_optimized_client,
    fetch_chunk,
    get_interval_ms,
    log_rest_metrics,
    parse_interval_string,
    validate_request_params,
)
from data_source_manager.utils.for_core.rest_data_processing import (
    REST_OUTPUT_COLUMNS,
    create_empty_dataframe,
    process_kline_data,
)
from data_source_manager.utils.for_core.rest_exceptions import (
    APIError,
    HTTPError,
    JSONDecodeError,
    NetworkError,
    RateLimitError,
    RestAPIError,
)
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import (
    ChartType,
    DataProvider,
    Interval,
    MarketType,
    get_market_capabilities,
)
from data_source_manager.utils.time_utils import (
    align_time_boundaries,
    datetime_to_milliseconds,
    filter_dataframe_by_time,
    milliseconds_to_datetime,
)
from data_source_manager.utils.validation import DataFrameValidator

# Define the column names as a constant since they aren't in config.py
OUTPUT_COLUMNS = REST_OUTPUT_COLUMNS


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
    ) -> None:
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

        # Get base URL from centralized market capabilities (DRY)
        capabilities = get_market_capabilities(market_type)
        self.base_url = capabilities.api_base_url

        # Set up proper endpoint based on market type
        self._endpoint = self._get_klines_endpoint()

        logger.debug(f"Initialized RestDataClient with market_type={market_type.name}, retry_count={retry_count}")

    def _get_klines_endpoint(self):
        """Get the appropriate endpoint URL for klines data based on market type.

        Returns:
            URL string for the klines endpoint
        """
        # Base API URLs
        if self.market_type == MarketType.SPOT:
            return f"{self.base_url}/api/v3/klines"
        if self.market_type == MarketType.FUTURES_USDT:
            return f"{self.base_url}/fapi/v1/klines"
        if self.market_type == MarketType.FUTURES_COIN:
            return f"{self.base_url}/dapi/v1/klines"
        raise ValueError(f"Unsupported market type: {self.market_type}")

    def __enter__(self) -> "RestDataClient":
        """Initialize the client session when entering the context."""
        if self._client is None:
            self._client = create_optimized_client()
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        """Clean up resources when exiting the context."""
        if self._client and hasattr(self._client, "close"):
            self._client.close()
            self._client = None
            logger.debug("Closed HTTP client")

    def _fetch_chunk(self, endpoint: str, params: dict[str, Any], retry_count: int = 0) -> list[list[Any]]:
        """Fetch a chunk of data with retry logic.

        Args:
            endpoint: API endpoint URL
            params: Request parameters
            retry_count: Current retry attempt - parameter preserved for backward compatibility
                       with legacy code, no longer used as retry logic is now handled by tenacity
                       decorators in the utility functions

        Returns:
            List of data points from the API

        Raises:
            Exception: If all retry attempts fail
        """
        # Initialize client if not already done
        if self._client is None:
            self._client = create_optimized_client()

        # Log retry attempt if non-zero (for debugging legacy code references)
        if retry_count > 0:
            logger.debug(f"Legacy retry_count parameter used: {retry_count} (ignored)")

        # Use the utility function to fetch the chunk
        return fetch_chunk(self._client, endpoint, params, self.fetch_timeout)

    def _fetch_chunk_data(
        self,
        symbol: str,
        interval: Interval,
        start_ms: int,
        end_ms: int,
    ) -> list[list[Any]]:
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
                logger.debug(f"No data returned for {symbol} in range {start_ms} to {end_ms}")
                return []

            return data
        except RateLimitError:
            # Rate limiting should propagate - caller should handle backoff
            logger.error(f"Rate limited when fetching chunk for {symbol}")
            raise
        except (HTTPError, APIError) as e:
            # API errors may be transient - log and return empty for this chunk
            logger.error(f"API error when fetching chunk for {symbol}: {e}")
            return []
        except (NetworkError, TimeoutError) as e:
            # Network errors may be transient - log and return empty for this chunk
            logger.error(f"Network error when fetching chunk for {symbol}: {e}")
            return []
        except JSONDecodeError as e:
            # JSON decode errors indicate corrupted response - log and skip chunk
            logger.error(f"JSON decode error when fetching chunk for {symbol}: {e}")
            return []
        except RestAPIError as e:
            # Other REST API errors - log and return empty for this chunk
            logger.error(f"REST API error when fetching chunk for {symbol}: {e}")
            return []
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            # Data processing errors indicate bugs - log with traceback
            logger.error(f"Data processing error for {symbol}: {e}", exc_info=True)
            return []

    def _create_optimized_client(self) -> Any:
        """Create an optimized HTTP client for REST API requests.

        Returns:
            HTTP client instance optimized for performance
        """
        return create_optimized_client()

    def _calculate_chunks(self, start_ms: int, end_ms: int, interval: Interval) -> list[tuple[int, int]]:
        """Calculate chunk boundaries for a time range.

        Args:
            start_ms: Start time in milliseconds
            end_ms: End time in milliseconds
            interval: Time interval

        Returns:
            List of (chunk_start_ms, chunk_end_ms) tuples
        """
        # Calculate the interval duration in milliseconds
        interval_ms = get_interval_ms(interval)

        # Use the utility function to calculate chunks
        return calculate_chunks(start_ms, end_ms, interval_ms, self.CHUNK_SIZE, REST_MAX_CHUNKS)

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
        # Convert interval string to Interval enum using the utility function
        interval_enum = (
            parse_interval_string(interval, self._interval)
            if isinstance(interval, str)
            else (interval if isinstance(interval, Interval) else self._interval)
        )

        # Validate request parameters
        validate_request_params(symbol, interval_enum, start_time, end_time)

        # Align time boundaries
        aligned_start, aligned_end = align_time_boundaries(start_time, end_time, interval_enum)

        logger.info(f"Fetching {interval_enum.value} data for {symbol} from {aligned_start.isoformat()} to {aligned_end.isoformat()}")

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
                f"Fetching chunk {i + 1}/{len(chunks)} for {symbol}: "
                f"{milliseconds_to_datetime(chunk_start).isoformat()} to "
                f"{milliseconds_to_datetime(chunk_end).isoformat()}"
            )

            # Fetch the chunk - RateLimitError propagates to caller
            try:
                chunk_data = self._fetch_chunk_data(symbol, interval_enum, chunk_start, chunk_end)
            except RateLimitError:
                # Rate limiting should stop the entire fetch operation
                logger.error(f"Rate limited at chunk {i + 1}/{len(chunks)} for {symbol}")
                raise

            if chunk_data:
                all_data.extend(chunk_data)
                stats["successful_chunks"] += 1
                stats["total_data_points"] += len(chunk_data)
                logger.debug(f"Retrieved {len(chunk_data)} records for chunk {i + 1}")
            else:
                logger.warning(f"No data returned for chunk {i + 1}")

        # If no data was retrieved, return empty DataFrame
        if not all_data:
            logger.warning(f"No data retrieved for {symbol} in the specified time range")
            return create_empty_dataframe()

        # Process the data into a DataFrame
        df = process_kline_data(all_data)

        # Filter to requested time range
        filtered_df = filter_dataframe_by_time(df, aligned_start, aligned_end, "open_time")

        # Log success stats
        logger.info(
            f"Successfully retrieved {len(filtered_df)} records for {symbol} "
            f"(from {stats['successful_chunks']}/{stats['total_chunks']} chunks)"
        )

        # Log REST metrics (for troubleshooting and monitoring)
        if kwargs.get("log_metrics", False):
            log_rest_metrics()

        return filtered_df

    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct structure.

        Returns:
            Empty DataFrame
        """
        return create_empty_dataframe()

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
    def interval(self) -> str | object:
        """Get the interval.

        Returns:
            The time interval string
        """
        return self._interval.value if hasattr(self._interval, "value") else str(self._interval)

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

    def validate_data(self, df: pd.DataFrame) -> tuple[bool, str | None]:
        """Validate that a DataFrame contains valid market data.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        validator = DataFrameValidator(df)
        return validator.validate_klines_data()
