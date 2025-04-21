#!/usr/bin/env python
"""API Boundary Validator to handle Binance API time boundaries.

This module provides the ApiBoundaryValidator class, which is responsible for validating
time boundaries and data ranges against the actual Binance REST API behavior rather than
using manual time alignment logic. It directly calls the Binance API to determine the actual
data boundaries for given time ranges, ensuring alignment with real API responses.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Tuple, List, Any
import random

import pandas as pd

from utils.logger_setup import logger
from utils.market_constraints import MarketType, Interval, ChartType, get_endpoint_url
from utils.time_utils import (
    enforce_utc_timezone,
    align_time_boundaries as time_utils_align_time_boundaries,
)
from utils.network_utils import create_client, safely_close_client


# Constants for API interaction
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # seconds
RATE_LIMIT_STATUS = 429

# Deprecation warning message template
DEPRECATION_WARNING = (
    "{} is deprecated and will be moved to utils.time_utils in a future version. "
    "Use utils.time_utils.{} instead."
)


class ApiBoundaryValidator:
    """Validates time boundaries and data ranges against actual Binance API behavior.

    This class makes direct calls to the Binance API to determine the actual boundaries
    of data for given time ranges, eliminating the need for manual time alignment logic.
    It provides methods to validate time ranges, get API-defined boundaries, and check
    if DataFrame contents match what would be returned by the API.
    """

    def __init__(self, market_type: MarketType = MarketType.SPOT):
        """Initialize API Boundary Validator.

        Args:
            market_type: The type of market to validate against (default: SPOT)
        """
        # Only SPOT is supported as per market_constraints.py
        if market_type.name != MarketType.SPOT.name:
            raise ValueError(f"Unsupported market type: {market_type}")

        self.market_type = market_type
        # Use httpx client
        self.http_client = create_client(timeout=10.0)
        logger.debug(f"Initialized ApiBoundaryValidator for {market_type} market")

    async def __aenter__(self):
        """Context manager entry for async with statements."""
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit for async with statements - ensures client is closed."""
        await self.close()
        logger.debug("Closed ApiBoundaryValidator HTTP client")

    async def close(self):
        """Close the HTTP client."""
        await safely_close_client(self.http_client)

    async def is_valid_time_range(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> bool:
        """Validate if the given time range and interval are valid according to Binance API.

        This method calls the Binance API with the provided parameters and checks if the
        API returns valid data for the requested time range.

        Args:
            start_time: The start time for data retrieval
            end_time: The end time for data retrieval
            interval: The data interval
            symbol: The trading pair symbol to check

        Returns:
            True if the time range is valid for the API, False otherwise
        """
        logger.debug(
            f"Validating time range: {start_time} -> {end_time} for {symbol} {interval}"
        )
        try:
            # Call API to check if data exists for this range
            api_data = await self._call_api(
                start_time, end_time, interval, limit=1, symbol=symbol
            )

            is_valid = len(api_data) > 0
            logger.debug(
                f"Time range validation result: {'Valid' if is_valid else 'Invalid'}"
            )
            return is_valid
        except Exception as e:
            logger.warning(f"Error validating time range: {e}")
            return False

    async def get_api_boundaries(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> Dict[str, Any]:
        """Call Binance API and determine the actual boundaries of returned data.

        This method analyzes the API response to determine the actual start and end times
        of the data returned by the API for the given parameters.

        Args:
            start_time: The requested start time
            end_time: The requested end time
            interval: The data interval
            symbol: The trading pair symbol to check

        Returns:
            Dictionary containing API-aligned boundaries:
            {
                'api_start_time': datetime,  # Actual first timestamp in API response
                'api_end_time': datetime,    # Actual last timestamp in API response
                'record_count': int,         # Number of records returned
                'matches_request': bool      # Whether API boundaries match requested boundaries
            }
        """
        logger.debug(
            f"Getting API boundaries for {symbol} {interval}: {start_time} -> {end_time}"
        )

        # Ensure timezone awareness for input times
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)

        try:
            # Call API to get data for the requested range
            api_data = await self._call_api(
                start_time, end_time, interval, limit=1000, symbol=symbol
            )

            if not api_data:
                logger.warning("API returned no data for the requested range")
                return {
                    "api_start_time": None,
                    "api_end_time": None,
                    "record_count": 0,
                    "matches_request": False,
                }

            # Extract timestamps from first and last records
            first_timestamp_ms = api_data[0][0]
            last_timestamp_ms = api_data[-1][0]

            # Convert to datetime objects
            api_start_time = datetime.fromtimestamp(
                first_timestamp_ms / 1000, tz=timezone.utc
            )
            api_end_time = datetime.fromtimestamp(
                last_timestamp_ms / 1000, tz=timezone.utc
            )

            # Check if API boundaries match requested boundaries (within millisecond precision)
            start_matches = abs((api_start_time - start_time).total_seconds()) < 0.001
            end_within_range = api_end_time <= end_time

            result = {
                "api_start_time": api_start_time,
                "api_end_time": api_end_time,
                "record_count": len(api_data),
                "matches_request": start_matches and end_within_range,
            }

            logger.debug(
                f"API boundaries found - Start: {api_start_time}, End: {api_end_time}, "
                f"Records: {len(api_data)}, Matches Request: {start_matches and end_within_range}"
            )

            return result
        except Exception as e:
            logger.warning(f"Error getting API boundaries: {e}")
            return {
                "api_start_time": None,
                "api_end_time": None,
                "record_count": 0,
                "matches_request": False,
                "error": str(e),
            }

    def align_time_boundaries(
        self, start_time: datetime, end_time: datetime, interval: Interval
    ) -> Tuple[datetime, datetime]:
        """Align time boundaries according to Binance REST API behavior.

        This method is maintained for compatibility with existing code.
        For new code, use utils.time_utils.align_time_boundaries instead.

        Args:
            start_time: Start time
            end_time: End time
            interval: Data interval

        Returns:
            Tuple of (aligned_start, aligned_end)
        """
        logger.debug(
            f"Aligning time boundaries: {start_time} -> {end_time} for interval {interval}"
        )
        return time_utils_align_time_boundaries(start_time, end_time, interval)

    async def does_data_range_match_api_response(
        self,
        df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> bool:
        """Check if the DataFrame contains the same data that would be returned by the API.

        This method compares the DataFrame's contents with what would be returned
        by directly calling the Binance API with the same parameters.

        Args:
            df: DataFrame to check
            start_time: Start time for data request
            end_time: End time for data request
            interval: Data interval
            symbol: Trading pair symbol

        Returns:
            True if DataFrame matches what would be returned by the API
        """
        logger.debug(
            f"Checking if data matches API response for {symbol} {interval}: "
            f"{start_time} -> {end_time}"
        )

        # Ensure timezone awareness for input times
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)

        # First check if DataFrame is empty
        if df.empty:
            # If empty, check if API would return any data
            api_response = await self._call_api(
                start_time, end_time, interval, symbol=symbol
            )
            return len(api_response) == 0

        # Get API boundaries to check time alignment
        api_boundaries = await self.get_api_boundaries(
            start_time, end_time, interval, symbol
        )

        # If API couldn't return valid boundaries, we can't validate
        if not api_boundaries.get("api_start_time"):
            logger.warning("Couldn't determine API boundaries, validation skipped")
            return False

        # Compare first and last timestamps
        df_start_time = df.index.min()
        df_end_time = df.index.max()

        api_start_time = api_boundaries["api_start_time"]
        api_end_time = api_boundaries["api_end_time"]

        # Allow a small tolerance (1 millisecond) for timestamp comparisons
        start_time_match = abs((df_start_time - api_start_time).total_seconds()) < 0.001
        end_time_match = abs((df_end_time - api_end_time).total_seconds()) < 0.001

        # Also check record count
        df_record_count = len(df)
        api_record_count = api_boundaries["record_count"]
        record_count_match = df_record_count == api_record_count

        result = start_time_match and end_time_match and record_count_match

        logger.debug(
            f"DataFrame validation result: {'Valid' if result else 'Invalid'} "
            f"(Start: {start_time_match}, End: {end_time_match}, "
            f"Count: {df_record_count} vs {api_record_count} - {record_count_match})"
        )

        return result

    async def get_api_response(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        limit: int = 1000,
        symbol: str = "BTCUSDT",
    ) -> pd.DataFrame:
        """Call Binance API and return the response as a DataFrame.

        This method serves as a reference for what the API would return for
        the given parameters, for validation or comparison purposes.

        Args:
            start_time: Start time for the request
            end_time: End time for the request
            interval: Data interval
            limit: Maximum number of records to retrieve
            symbol: Trading pair symbol

        Returns:
            DataFrame containing the API response
        """
        logger.debug(
            f"Getting API response for {symbol} {interval}: {start_time} -> {end_time}"
        )

        # Ensure timezone awareness for input times
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)

        try:
            # Call API
            api_data = await self._call_api(
                start_time, end_time, interval, limit, symbol
            )

            if not api_data:
                logger.warning("API returned no data")
                # Return empty DataFrame with the right structure
                empty_df = pd.DataFrame(
                    columns=[
                        "open_time",
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
                )
                empty_df["open_time"] = pd.to_datetime(
                    empty_df["open_time"], unit="ms", utc=True
                )
                empty_df.set_index("open_time", inplace=True)
                return empty_df

            # Convert to DataFrame
            df = pd.DataFrame(
                api_data,
                columns=[
                    "open_time",
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
                    "ignore",
                ],
            )

            # Convert timestamp to datetime
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df.set_index("open_time", inplace=True)

            # Convert numeric columns
            numeric_cols = [
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
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col])

            # Drop 'ignore' column
            df = df.drop(columns=["ignore"])

            logger.info(
                f"API returned {len(df)} records with index range "
                f"{df.index.min()} -> {df.index.max()}"
            )

            return df

        except Exception as e:
            logger.error(f"Error getting API response: {e}")
            # Return empty DataFrame
            empty_df = pd.DataFrame(
                columns=[
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
            )
            empty_df.index.name = "open_time"
            return empty_df

    async def _call_api(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        limit: int = 1000,
        symbol: str = "BTCUSDT",
    ) -> List[List[Any]]:
        """Call the Binance API with retry logic.

        Args:
            start_time: The start time for data retrieval
            end_time: The end time for data retrieval
            interval: The data interval
            limit: Maximum number of records to retrieve
            symbol: The trading pair symbol

        Returns:
            List of klines data

        Raises:
            Exception: If API call fails after retries
        """
        # Ensure timezone awareness and convert to milliseconds
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)
        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms = int(end_time.timestamp() * 1000)

        # Prepare API parameters
        params = {
            "symbol": symbol,
            "interval": interval.value,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
            "limit": limit,
        }

        # Determine base URL based on market type
        base_url = get_endpoint_url(self.market_type, ChartType.KLINES.endpoint)

        # Retry logic
        retries = 0
        while retries <= MAX_RETRIES:
            try:
                # Fetch data from API
                logger.debug(
                    f"Calling API: {base_url} with params {params}, retry {retries}/{MAX_RETRIES}"
                )

                response = await self.http_client.get(base_url, params=params)

                # Handle response
                if response.status_code == RATE_LIMIT_STATUS:
                    retry_after = int(response.headers.get("Retry-After", 1))
                    logger.warning(f"Rate limited by API. Retry after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    retries += 1
                    continue

                if response.status_code >= 400:
                    logger.error(f"API error {response.status_code}: {response.text}")
                    raise Exception(
                        f"API error {response.status_code}: {response.text}"
                    )

                # Parse the response data
                data = response.json()

                logger.debug(f"API returned {len(data)} records")
                return data

            except Exception as e:
                # Handle retries
                retries += 1
                logger.warning(
                    f"API call failed (retry {retries}/{MAX_RETRIES}): {str(e)}"
                )

                if retries <= MAX_RETRIES:
                    # Exponential backoff with jitter
                    wait_time = (
                        RETRY_DELAY * (2 ** (retries - 1)) * (0.5 + random.random())
                    )
                    logger.debug(f"Waiting {wait_time:.2f}s before retrying")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"All {MAX_RETRIES} API call retries failed: {str(e)}")
                    raise Exception(
                        f"Failed to fetch data from API after {MAX_RETRIES} retries"
                    ) from e

        # This should never be reached
        raise Exception("API call retry loop exited unexpectedly")

    def is_valid_time_range_sync(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> bool:
        """Synchronous version that validates if the given time range and interval are valid according to Binance API.

        This method provides a synchronous interface to validate time ranges.
        For new code, prefer using this method over the async version.

        Args:
            start_time: The start time for data retrieval
            end_time: The end time for data retrieval
            interval: The data interval
            symbol: The trading pair symbol to check

        Returns:
            True if the time range is valid for the API, False otherwise
        """
        logger.debug(
            f"Validating time range (sync): {start_time} -> {end_time} for {symbol} {interval}"
        )
        try:
            # Call API to check if data exists for this range
            api_data = self._call_api_sync(
                start_time, end_time, interval, limit=1, symbol=symbol
            )

            is_valid = len(api_data) > 0
            logger.debug(
                f"Time range validation result (sync): {'Valid' if is_valid else 'Invalid'}"
            )
            return is_valid
        except Exception as e:
            logger.warning(f"Error validating time range (sync): {e}")
            return False

    def get_api_boundaries_sync(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> Dict[str, Any]:
        """Synchronous version that determines the actual boundaries of API data.

        This method provides a synchronous interface to get API boundaries.
        For new code, prefer using this method over the async version.

        Args:
            start_time: The requested start time
            end_time: The requested end time
            interval: The data interval
            symbol: The trading pair symbol to check

        Returns:
            Dictionary containing API-aligned boundaries:
            {
                'api_start_time': datetime,  # Actual first timestamp in API response
                'api_end_time': datetime,    # Actual last timestamp in API response
                'record_count': int,         # Number of records returned
                'matches_request': bool      # Whether API boundaries match requested boundaries
            }
        """
        logger.debug(
            f"Getting API boundaries (sync) for {symbol} {interval}: {start_time} -> {end_time}"
        )

        # Ensure timezone awareness for input times
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)

        try:
            # Call API to get data for the requested range
            api_data = self._call_api_sync(
                start_time, end_time, interval, limit=1000, symbol=symbol
            )

            if not api_data:
                logger.warning("API returned no data for the requested range (sync)")
                return {
                    "api_start_time": None,
                    "api_end_time": None,
                    "record_count": 0,
                    "matches_request": False,
                }

            # Extract timestamps from first and last records
            first_timestamp_ms = api_data[0][0]
            last_timestamp_ms = api_data[-1][0]

            # Convert to datetime objects
            api_start_time = datetime.fromtimestamp(
                first_timestamp_ms / 1000, tz=timezone.utc
            )
            api_end_time = datetime.fromtimestamp(
                last_timestamp_ms / 1000, tz=timezone.utc
            )

            # Check if API boundaries match requested boundaries (within millisecond precision)
            start_matches = abs((api_start_time - start_time).total_seconds()) < 0.001
            end_within_range = api_end_time <= end_time

            result = {
                "api_start_time": api_start_time,
                "api_end_time": api_end_time,
                "record_count": len(api_data),
                "matches_request": start_matches and end_within_range,
            }

            logger.debug(
                f"API boundaries found (sync) - Start: {api_start_time}, End: {api_end_time}, "
                f"Records: {len(api_data)}, Matches Request: {start_matches and end_within_range}"
            )

            return result
        except Exception as e:
            logger.warning(f"Error getting API boundaries (sync): {e}")
            return {
                "api_start_time": None,
                "api_end_time": None,
                "record_count": 0,
                "matches_request": False,
                "error": str(e),
            }

    def _call_api_sync(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        limit: int = 1000,
        symbol: str = "BTCUSDT",
    ) -> List[List[Any]]:
        """Synchronous version of _call_api that makes a direct HTTP request to the Binance API.

        This method handles the low-level API call logic for synchronous operations.
        For new code, prefer using this method over the async version.

        Args:
            start_time: Start time for data request
            end_time: End time for data request
            interval: Data interval
            limit: Maximum number of records to retrieve
            symbol: Trading pair symbol

        Returns:
            List of data points from the API response

        Raises:
            Exception: If API call fails after MAX_RETRIES attempts
        """
        # Implementation similar to async _call_api but using synchronous HTTP client methods
        import time
        import requests

        # Format timestamps for API request (in milliseconds)
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)

        # Construct API endpoint URL for the specific market type
        endpoint = get_endpoint_url(
            self.market_type, ChartType.KLINES, symbol, interval
        )

        params = {
            "symbol": symbol,
            "interval": interval.value,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }

        logger.debug(f"Making sync API call to {endpoint} with params {params}")

        # Retry logic
        retries = 0
        while retries < MAX_RETRIES:
            try:
                response = requests.get(endpoint, params=params, timeout=10.0)

                if response.status_code == 200:
                    data = response.json()
                    logger.debug(f"Received {len(data)} records from API (sync)")
                    return data
                elif response.status_code == RATE_LIMIT_STATUS:
                    # Rate limited, wait and retry
                    wait_time = RETRY_DELAY * (2**retries) * (0.5 + random.random())
                    logger.warning(
                        f"Rate limited (sync), retrying in {wait_time:.2f}s (attempt {retries+1}/{MAX_RETRIES})"
                    )
                    time.sleep(wait_time)
                else:
                    # Other error, log and retry
                    logger.warning(
                        f"API error (sync): {response.status_code} - {response.text}, "
                        f"retrying in {RETRY_DELAY}s (attempt {retries+1}/{MAX_RETRIES})"
                    )
                    time.sleep(RETRY_DELAY)
            except Exception as e:
                # Connection error or timeout, retry
                logger.warning(
                    f"Connection error (sync): {str(e)}, "
                    f"retrying in {RETRY_DELAY}s (attempt {retries+1}/{MAX_RETRIES})"
                )
                time.sleep(RETRY_DELAY)

            retries += 1

        # If we get here, all retries failed
        raise Exception(f"Failed to call API after {MAX_RETRIES} attempts")
