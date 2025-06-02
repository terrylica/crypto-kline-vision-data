#!/usr/bin/env python
"""Client for Binance funding rate data."""

import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from core.providers.binance.cache_manager import UnifiedCacheManager
from core.providers.binance.data_client_interface import DataClientInterface
from utils.config import (
    FUNDING_RATE_DTYPES,
    MAX_FUNDING_RATE,
    MIN_FUNDING_RATE,
    create_empty_funding_rate_dataframe,
)
from utils.loguru_setup import logger
from utils.market_constraints import ChartType, DataProvider, Interval, MarketType
from utils.market_utils import get_market_type_str
from utils.network_utils import create_client
from utils.time_utils import filter_dataframe_by_time


class BinanceFundingRateClient(DataClientInterface):
    """Client for retrieving funding rate data from Binance."""

    def __init__(
        self,
        symbol: str,
        interval: str | Interval = Interval.HOUR_8,
        market_type: MarketType = MarketType.FUTURES_USDT,
        use_cache: bool = True,
        cache_dir: Path | None = None,
        retry_count: int = 5,
    ):
        """Initialize the Binance funding rate client.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Time interval for funding rate data
            market_type: Market type (FUTURES_USDT or FUTURES_COIN)
            use_cache: Whether to use caching
            cache_dir: Cache directory
            retry_count: Number of retries for failed requests
        """
        self._symbol = symbol.upper()

        # Handle interval as string or Interval enum
        if isinstance(interval, str):
            try:
                self._interval = Interval(interval)
            except ValueError:
                logger.warning(f"Invalid interval: {interval}, using HOUR_8")
                self._interval = Interval.HOUR_8
        else:
            self._interval = interval

        self._market_type = market_type
        self._retry_count = retry_count

        # Validate market type
        if market_type not in (MarketType.FUTURES_USDT, MarketType.FUTURES_COIN):
            raise ValueError(f"Invalid market type for funding rate: {market_type}. Must be FUTURES_USDT or FUTURES_COIN.")

        # Set up client
        self._client = create_client()

        # Set up cache if enabled
        self._use_cache = use_cache
        if use_cache:
            if cache_dir is None:
                cache_dir = Path("./cache")
            self._cache_manager = UnifiedCacheManager(cache_dir=cache_dir)
        else:
            self._cache_manager = None

        # Set base URL based on market type
        if market_type == MarketType.FUTURES_USDT:
            self._base_url = "https://fapi.binance.com"
        else:  # MarketType.FUTURES_COIN
            self._base_url = "https://dapi.binance.com"

        logger.debug(f"Initialized BinanceFundingRateClient for {symbol} with interval {interval}, market type {market_type.name}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit."""
        # Close the HTTP client if it has a close method
        if hasattr(self._client, "close") and callable(self._client.close):
            self._client.close()

    @property
    def provider(self) -> DataProvider:
        """Get the data provider for this client."""
        return DataProvider.BINANCE

    @property
    def market_type(self) -> MarketType:
        """Get the market type for this client."""
        return self._market_type

    @property
    def chart_type(self) -> ChartType:
        """Get the chart type for this client."""
        return ChartType.FUNDING_RATE

    @property
    def symbol(self) -> str:
        """Get the trading symbol for this client."""
        return self._symbol

    @property
    def interval(self) -> str | object:
        """Get the interval for this client."""
        if hasattr(self._interval, "value"):
            return self._interval.value
        return str(self._interval)

    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct structure for funding rate data.

        Returns:
            Empty DataFrame with correct columns and types
        """
        return create_empty_funding_rate_dataframe()

    def validate_data(self, df: pd.DataFrame) -> tuple[bool, str | None]:
        """Validate that a DataFrame contains valid funding rate data.

        This method checks the structure, data types, and integrity of a
        funding rate DataFrame against expected standards. It ensures
        all required columns are present with correct data types, and
        that time-related columns are properly formatted.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not isinstance(df, pd.DataFrame):
            return False, f"Expected DataFrame object, got {type(df).__name__}"

        try:
            # Check if DataFrame is empty
            if df.empty:
                logger.debug("Validating empty DataFrame - no data to validate")
                return True, None

            # Check basic structure
            required_columns = list(FUNDING_RATE_DTYPES.keys())
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                return False, f"Missing required columns: {missing_columns}"

            # Validate data types
            dtype_errors = []
            for col, dtype in FUNDING_RATE_DTYPES.items():
                if col in df.columns:
                    try:
                        # Skip validation for empty series
                        if df[col].empty:
                            continue

                        # Check if dtype is compatible
                        if not pd.api.types.is_dtype_equal(df[col].dtype, dtype):
                            # Try to convert and check for data loss
                            original_values = df[col].dropna().to_numpy()
                            if len(original_values) > 0:
                                try:
                                    converted = df[col].astype(dtype)
                                    # For numeric types, check for data loss in conversion
                                    if (
                                        pd.api.types.is_numeric_dtype(df[col])
                                        and pd.api.types.is_numeric_dtype(converted)
                                        and not (converted.dropna().to_numpy() == original_values).all()
                                    ):
                                        dtype_errors.append(
                                            f"Column {col} values would lose precision if converted from {df[col].dtype} to {dtype}"
                                        )

                                except Exception as e:
                                    dtype_errors.append(f"Column {col} cannot be converted from {df[col].dtype} to {dtype}: {e}")

                            dtype_errors.append(f"Column {col} has dtype {df[col].dtype}, expected {dtype}")
                    except Exception as e:
                        dtype_errors.append(f"Error validating dtype for column {col}: {e}")

            if dtype_errors:
                return False, f"Data type validation errors: {', '.join(dtype_errors)}"

            # Check if funding_time is in ascending order (if present)
            if "funding_time" in df.columns and len(df) > 1:
                is_sorted = df["funding_time"].is_monotonic_increasing
                if not is_sorted:
                    return False, "funding_time is not in ascending order"

                # Check for duplicates in funding_time
                if df["funding_time"].duplicated().any():
                    return False, "DataFrame contains duplicate funding_time values"

            # Check if all funding rates are within reasonable bounds (-10% to 10%)
            if "funding_rate" in df.columns:
                min_rate = df["funding_rate"].min()
                max_rate = df["funding_rate"].max()
                if min_rate < MIN_FUNDING_RATE:
                    logger.warning(f"Unusually low funding rate detected: {min_rate}")
                if max_rate > MAX_FUNDING_RATE:
                    logger.warning(f"Unusually high funding rate detected: {max_rate}")

            return True, None
        except Exception as e:
            logger.error(f"Error validating funding rate data: {e}")
            return False, f"Validation error: {e!s}"

    def fetch(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch funding rate data for the specified symbol and time range.

        Args:
            symbol: Trading pair symbol (uses provided value or falls back to instance symbol)
            interval: Time interval (uses provided value or falls back to instance interval)
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            **kwargs: Additional parameters, preserved for API compatibility with other clients
                     (not used by this implementation but needed for interface consistency)

        Returns:
            DataFrame with funding rate data

        Raises:
            ValueError: If parameters are invalid
        """
        # Extract any useful parameters from kwargs for future extensions
        cache_mode = kwargs.get("cache_mode")
        if cache_mode:
            logger.debug(f"Cache mode hint: {cache_mode} (ignored by funding rate client)")

        # Validate input parameters (keeping backward compatibility)
        if not isinstance(symbol, str) or not symbol:
            symbol = self._symbol
            logger.debug(f"Using instance symbol: {symbol}")

        # Convert interval to Interval enum if it's a string
        try:
            interval_obj = next((i for i in Interval if i.value == interval), None)
            if interval_obj is None:
                # Try by enum name
                try:
                    interval_obj = Interval[interval.upper()]
                except KeyError:
                    logger.warning(f"Invalid interval: {interval}, using instance interval")
                    interval_obj = self._interval

        except Exception:
            logger.warning(f"Error converting interval '{interval}', using instance interval")
            interval_obj = self._interval

        # Validate time parameters
        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise ValueError("Start time and end time must be datetime objects")

        if start_time >= end_time:
            raise ValueError(f"Start time {start_time} must be before end time {end_time}")

        # Use cache if enabled and available
        if self._use_cache and self._cache_manager:
            # Check if data is available in cache
            cached_df = self._cache_manager.load_from_cache(
                symbol=symbol,
                interval=interval_obj.value,
                date=start_time,
                provider="BINANCE",
                chart_type="FUNDING_RATE",
                market_type=self._get_market_type_str(),
            )

            if cached_df is not None:
                # Filter to the requested time range
                filtered_df = filter_dataframe_by_time(cached_df, start_time, end_time, "funding_time")
                if not filtered_df.empty:
                    return filtered_df

        # Fetch data from API
        df = self._fetch_funding_rate(symbol, start_time, end_time)

        # Save to cache if enabled
        if self._use_cache and self._cache_manager and not df.empty:
            self._cache_manager.save_to_cache(
                df=df,
                symbol=symbol,
                interval=interval_obj.value,
                date=start_time,
                provider="BINANCE",
                chart_type="FUNDING_RATE",
                market_type=self._get_market_type_str(),
            )

        return df

    def _get_market_type_str(self) -> str:
        """Get the market type as a string.

        Returns:
            Market type string
        """
        return get_market_type_str(self._market_type)

    def _fetch_funding_rate(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> pd.DataFrame:
        """Fetch funding rate data directly from Binance API.

        Args:
            symbol: Trading pair symbol
            start_time: Start time
            end_time: End time

        Returns:
            DataFrame with funding rate data
        """
        logger.debug(f"Fetching funding rate data for {symbol}: {start_time.isoformat()} - {end_time.isoformat()}")

        # Create an empty DataFrame with the correct structure
        result_df = create_empty_funding_rate_dataframe()

        # Set API endpoint based on market type
        if self._market_type == MarketType.FUTURES_USDT:
            endpoint = f"{self._base_url}/fapi/v1/fundingRate"
        else:  # MarketType.FUTURES_COIN
            endpoint = f"{self._base_url}/dapi/v1/fundingRate"

        # Convert times to milliseconds
        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms = int(end_time.timestamp() * 1000)

        # Binance limits to 1000 records per request, so we may need multiple requests
        limit = 1000
        retry_count = 0
        current_start_ms = start_time_ms

        while current_start_ms < end_time_ms:
            try:
                # Prepare request parameters
                params = {
                    "symbol": symbol,
                    "startTime": current_start_ms,
                    "endTime": end_time_ms,
                    "limit": limit,
                }

                # Make the request
                response = self._client.get(endpoint, params=params)
                response.raise_for_status()
                data = response.json()

                if not data:
                    logger.debug("No more funding rate data available")
                    break

                # Process results
                for item in data:
                    funding_time = datetime.fromtimestamp(int(item["fundingTime"]) / 1000, tz=timezone.utc)
                    funding_rate = float(item["fundingRate"])

                    # Append to DataFrame
                    result_df = pd.concat(
                        [
                            result_df,
                            pd.DataFrame(
                                {
                                    "symbol": [symbol],
                                    "funding_time": [funding_time],
                                    "funding_rate": [funding_rate],
                                    "interval": [self._interval.value],
                                }
                            ),
                        ],
                        ignore_index=True,
                    )

                # Update start time for next batch
                if len(data) < limit:
                    # We got fewer results than requested, so we're done
                    break
                # Use the funding time of the last record plus 1 millisecond
                last_time = int(data[-1]["fundingTime"])
                current_start_ms = last_time + 1

                # Reset retry counter after successful request
                retry_count = 0

            except Exception as e:
                retry_count += 1
                if retry_count > self._retry_count:
                    logger.error(f"Failed to fetch funding rate data after {self._retry_count} retries: {e}")
                    break

                # Add exponential backoff with jitter
                wait_time = min(0.5 * (2**retry_count) + (0.1 * retry_count), 10)
                logger.warning(
                    f"Error fetching funding rate data (retry {retry_count}/{self._retry_count}): {e}. "
                    f"Waiting {wait_time:.2f}s before retry."
                )
                time.sleep(wait_time)

        # Sort by funding time
        if not result_df.empty:
            result_df = result_df.sort_values("funding_time").reset_index(drop=True)

            # Filter to the requested time range
            result_df = filter_dataframe_by_time(result_df, start_time, end_time, "funding_time")

        return result_df

    def close(self) -> None:
        """Close the client and release resources."""
        if hasattr(self._client, "close") and callable(self._client.close):
            self._client.close()
