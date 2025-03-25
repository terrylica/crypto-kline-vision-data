#!/usr/bin/env python
"""API Boundary Validator to handle Binance API time boundaries.

This module provides the ApiBoundaryValidator class, which is responsible for validating
time boundaries and data ranges against the actual Binance REST API behavior rather than
using manual time alignment logic. It directly calls the Binance API to determine the actual
data boundaries for given time ranges, ensuring alignment with real API responses.
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple, List, Any
import httpx
import pandas as pd

from utils.logger_setup import get_logger
from utils.market_constraints import MarketType, Interval, get_endpoint_url
from utils.config import DEFAULT_TIMEZONE

logger = get_logger(__name__, "INFO")

# Constants for API interaction
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # seconds
RATE_LIMIT_STATUS = 429


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
        if market_type != MarketType.SPOT:
            raise ValueError(f"Unsupported market type: {market_type}")

        self.market_type = market_type
        self.http_client = httpx.AsyncClient(timeout=10.0)
        logger.info(f"Initialized ApiBoundaryValidator for {market_type} market")

    async def __aenter__(self):
        """Context manager entry for async with statements."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit for async with statements - ensures client is closed."""
        await self.close()
        logger.debug("Closed ApiBoundaryValidator HTTP client")

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
        logger.info(
            f"Validating time range: {start_time} -> {end_time} for {symbol} {interval}"
        )
        try:
            # Call API to check if data exists for this range
            api_data = await self._call_api(
                start_time, end_time, interval, limit=1, symbol=symbol
            )

            is_valid = len(api_data) > 0
            logger.info(
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
        logger.info(
            f"Getting API boundaries for {symbol} {interval}: {start_time} -> {end_time}"
        )

        # Ensure timezone awareness for input times
        start_time = self._ensure_timezone(start_time)
        end_time = self._ensure_timezone(end_time)

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

            logger.info(
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

        This method implements the exact boundary alignment behavior of the Binance REST API
        as documented in binance_rest_api_boundary_behaviour.md:
        - startTime: Rounds UP to the next interval boundary if not exactly on a boundary
        - endTime: Rounds DOWN to the previous interval boundary if not exactly on a boundary

        Args:
            start_time: User-provided start time
            end_time: User-provided end time
            interval: Data interval

        Returns:
            Tuple of (aligned_start_time, aligned_end_time) mimicking Binance API behavior
        """
        logger.info(
            f"Aligning time boundaries: {start_time} -> {end_time} for interval {interval}"
        )

        # Ensure timezone awareness
        start_time = self._ensure_timezone(start_time)
        end_time = self._ensure_timezone(end_time)

        # Get interval in microseconds for precise calculations
        interval_microseconds = self._get_interval_microseconds(interval)

        # Extract microseconds since epoch for calculations
        start_microseconds = int(start_time.timestamp() * 1_000_000)
        end_microseconds = int(end_time.timestamp() * 1_000_000)

        # Calculate floor of each timestamp to interval boundary
        start_floor = start_microseconds - (start_microseconds % interval_microseconds)
        end_floor = end_microseconds - (end_microseconds % interval_microseconds)

        # Apply Binance API boundary rules:
        # - startTime: Round UP to next interval boundary if not exactly on boundary
        # - endTime: Round DOWN to previous interval boundary if not exactly on boundary
        aligned_start_microseconds = (
            start_floor
            if start_microseconds == start_floor
            else start_floor + interval_microseconds
        )
        aligned_end_microseconds = end_floor

        # Convert back to datetime
        aligned_start = datetime.fromtimestamp(
            aligned_start_microseconds / 1_000_000, tz=timezone.utc
        )
        aligned_end = datetime.fromtimestamp(
            aligned_end_microseconds / 1_000_000, tz=timezone.utc
        )

        logger.info(
            f"Aligned boundaries: {aligned_start} -> {aligned_end} for interval {interval}"
        )

        return aligned_start, aligned_end

    def estimate_record_count(
        self, start_time: datetime, end_time: datetime, interval: Interval
    ) -> int:
        """Estimate the number of records that would be returned by the Binance API.

        This implements the record counting logic described in binance_rest_api_boundary_behaviour.md,
        taking into account the boundary alignment behavior.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Data interval

        Returns:
            Estimated number of records based on interval and time range
        """
        # First align the boundaries according to API behavior
        aligned_start, aligned_end = self.align_time_boundaries(
            start_time, end_time, interval
        )

        # Calculate interval in seconds
        interval_seconds = self._get_interval_seconds(interval)

        # Calculate number of records
        time_diff_seconds = (aligned_end - aligned_start).total_seconds()
        estimated_records = (
            int(time_diff_seconds / interval_seconds) + 1
        )  # +1 because endpoints are inclusive

        logger.info(
            f"Estimated {estimated_records} records for time range {aligned_start} -> {aligned_end}"
        )

        return estimated_records

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

    def _get_interval_seconds(self, interval: Interval) -> int:
        """Get interval duration in seconds.

        Args:
            interval: The interval type

        Returns:
            Number of seconds in the interval
        """
        # Map of interval to seconds
        interval_seconds_map = {
            Interval.SECOND_1: 1,
            Interval.MINUTE_1: 60,
            Interval.MINUTE_3: 3 * 60,
            Interval.MINUTE_5: 5 * 60,
            Interval.MINUTE_15: 15 * 60,
            Interval.MINUTE_30: 30 * 60,
            Interval.HOUR_1: 60 * 60,
            Interval.HOUR_2: 2 * 60 * 60,
            Interval.HOUR_4: 4 * 60 * 60,
            Interval.HOUR_6: 6 * 60 * 60,
            Interval.HOUR_8: 8 * 60 * 60,
            Interval.HOUR_12: 12 * 60 * 60,
            Interval.DAY_1: 24 * 60 * 60,
            Interval.DAY_3: 3 * 24 * 60 * 60,
            Interval.WEEK_1: 7 * 24 * 60 * 60,
            Interval.MONTH_1: 30 * 24 * 60 * 60,  # Approximate
        }

        return interval_seconds_map.get(interval, 60)  # Default to 1 minute if unknown

    def _get_interval_microseconds(self, interval: Interval) -> int:
        """Get interval duration in microseconds for precise calculations.

        Args:
            interval: The interval type

        Returns:
            Number of microseconds in the interval
        """
        return self._get_interval_seconds(interval) * 1_000_000

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

        logger.debug(f"Making API call to {endpoint} with params: {params}")

        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                response = await self.http_client.get(endpoint, params=params)

                if response.status_code == RATE_LIMIT_STATUS:
                    retry_count += 1
                    logger.warning(
                        f"Rate limit hit, attempt {retry_count}/{MAX_RETRIES}. "
                        f"Waiting {RETRY_DELAY} seconds..."
                    )
                    await asyncio.sleep(RETRY_DELAY)
                    continue

                response.raise_for_status()
                data = response.json()
                logger.debug(f"API call successful, received {len(data)} records")
                return data

            except httpx.HTTPStatusError as e:
                logger.error(
                    f"HTTP error occurred: {e.response.status_code} - {e.response.text}"
                )
                raise
            except Exception as e:
                logger.error(f"Error calling API: {str(e)}")
                raise

        logger.error("Max retries exceeded for API call")
        raise Exception("Max retries exceeded for API call")

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
