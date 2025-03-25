#!/usr/bin/env python
"""API Boundary Validator to handle Binance API time boundaries.

This module provides the ApiBoundaryValidator class, which is responsible for validating
time boundaries and data ranges against the actual Binance REST API behavior rather than
using manual time alignment logic. It directly calls the Binance API to determine the actual
data boundaries for given time ranges, ensuring alignment with real API responses.
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List, Any

import httpx
import pandas as pd

from utils.logger_setup import get_logger
from utils.market_constraints import MarketType, Interval, get_endpoint_url
from utils.config import DEFAULT_TIMEZONE

logger = get_logger(__name__, "INFO")


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
        self.market_type = market_type
        self.http_client = httpx.AsyncClient(timeout=10.0)

    async def __aenter__(self):
        """Context manager entry for async with statements."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit for async with statements - ensures client is closed."""
        await self.close()

    async def close(self):
        """Close the HTTP client."""
        await self.http_client.aclose()

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
        try:
            # Call API to check if data exists for this range
            api_data = await self._call_api(
                start_time, end_time, interval, limit=1, symbol=symbol
            )

            # If we get a valid response with data, the time range is valid
            return len(api_data) > 0
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
        # Ensure timezone awareness for input times
        start_time = self._ensure_timezone(start_time)
        end_time = self._ensure_timezone(end_time)

        try:
            # Call API to get data for the requested range
            api_data = await self._call_api(
                start_time, end_time, interval, limit=1000, symbol=symbol
            )

            if not api_data:
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

            return {
                "api_start_time": api_start_time,
                "api_end_time": api_end_time,
                "record_count": len(api_data),
                "matches_request": start_matches and end_within_range,
            }
        except Exception as e:
            logger.warning(f"Error getting API boundaries: {e}")
            return {
                "api_start_time": None,
                "api_end_time": None,
                "record_count": 0,
                "matches_request": False,
                "error": str(e),
            }

    async def does_data_range_match_api_response(
        self,
        df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        symbol: str = "BTCUSDT",
    ) -> bool:
        """Validate if DataFrame's time range matches what would be returned by the API.

        This method checks if the provided DataFrame's data range aligns with what would be
        returned by the Binance API for the specified parameters.

        Args:
            df: The DataFrame to validate
            start_time: The start time to check
            end_time: The end time to check
            interval: The data interval
            symbol: The trading pair symbol

        Returns:
            True if the DataFrame's range matches the expected API response, False otherwise
        """
        if df.empty:
            return False

        # Get the actual API boundaries for these parameters
        try:
            boundaries = await self.get_api_boundaries(
                start_time, end_time, interval, symbol=symbol
            )

            if boundaries["record_count"] == 0:
                # API would return no data
                return False

            # Check if DataFrame's first and last timestamps match API boundaries
            df_start_time = df.index[0].to_pydatetime()
            df_end_time = df.index[-1].to_pydatetime()

            # Allow small tolerance for timestamp comparison (1ms)
            start_matches = (
                abs((df_start_time - boundaries["api_start_time"]).total_seconds())
                < 0.001
            )
            end_matches = (
                abs((df_end_time - boundaries["api_end_time"]).total_seconds()) < 0.001
            )

            # Check if record count matches
            count_matches = len(df) == boundaries["record_count"]

            return start_matches and end_matches and count_matches
        except Exception as e:
            logger.warning(f"Error comparing data range with API: {e}")
            return False

    async def get_api_response(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        limit: int = 1000,
        symbol: str = "BTCUSDT",
    ) -> pd.DataFrame:
        """Get actual API response for the given parameters as a DataFrame.

        This is a utility method that calls the API and returns the result as a properly
        formatted DataFrame.

        Args:
            start_time: The start time for data retrieval
            end_time: The end time for data retrieval
            interval: The data interval
            limit: Maximum number of records to retrieve
            symbol: The trading pair symbol

        Returns:
            DataFrame containing the API response data
        """
        try:
            # Call API
            api_data = await self._call_api(
                start_time, end_time, interval, limit, symbol=symbol
            )

            if not api_data:
                # Return empty DataFrame with correct structure
                return pd.DataFrame(
                    [],
                    columns=[
                        "open_time",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "close_time",
                        "quote_volume",
                        "trades",
                        "taker_buy_volume",
                        "taker_buy_quote_volume",
                        "ignore",
                    ],
                )

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
                    "quote_volume",
                    "trades",
                    "taker_buy_volume",
                    "taker_buy_quote_volume",
                    "ignore",
                ],
            )

            # Convert timestamp columns to datetime and set as index
            df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

            # Set index
            df = df.set_index("open_time")

            # Convert numeric columns
            numeric_columns = [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_volume",
                "trades",
                "taker_buy_volume",
                "taker_buy_quote_volume",
            ]
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col])

            # Drop the 'ignore' column
            df = df.drop(columns=["ignore"])

            return df
        except Exception as e:
            logger.warning(f"Error getting API response: {e}")
            # Return empty DataFrame with correct structure on error
            empty_df = pd.DataFrame(
                [],
                columns=[
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "quote_volume",
                    "trades",
                    "taker_buy_volume",
                    "taker_buy_quote_volume",
                ],
            )
            empty_df.index = pd.DatetimeIndex([], name="open_time")
            return empty_df

    async def _call_api(
        self,
        start_time: datetime,
        end_time: datetime,
        interval: Interval,
        limit: int = 1000,
        symbol: str = "BTCUSDT",
    ) -> List[List[Any]]:
        """Call the Binance API with the provided parameters.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Data interval
            limit: Maximum number of records (default: 1000)
            symbol: Trading pair symbol (default: BTCUSDT)

        Returns:
            List of kline data from the API
        """
        # Ensure timezone awareness
        start_time = self._ensure_timezone(start_time)
        end_time = self._ensure_timezone(end_time)

        # Convert to milliseconds for API
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)

        # Get the endpoint URL
        endpoint = get_endpoint_url(self.market_type)

        # Prepare request parameters
        params = {
            "symbol": symbol,
            "interval": interval.value,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }

        # Make the API call
        response = await self.http_client.get(endpoint, params=params)
        response.raise_for_status()  # Raise an exception for HTTP errors

        return response.json()

    @staticmethod
    def _ensure_timezone(dt: datetime) -> datetime:
        """Ensure datetime is timezone aware.

        Args:
            dt: A datetime object

        Returns:
            Timezone-aware datetime in UTC
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=DEFAULT_TIMEZONE)
        return dt.astimezone(DEFAULT_TIMEZONE)
