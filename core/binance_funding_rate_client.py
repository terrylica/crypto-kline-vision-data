#!/usr/bin/env python
"""Client for Binance funding rate data."""

import asyncio
import pandas as pd
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple, Union

from curl_cffi.requests import AsyncSession

from utils.logger_setup import logger
from utils.market_constraints import DataProvider, MarketType, ChartType, Interval
from utils.time_utils import filter_dataframe_by_time, enforce_utc_timezone
from utils.network_utils import create_client, safely_close_client
from utils.config import (
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    FUNDING_RATE_DTYPES,
    CANONICAL_INDEX_NAME,
    DEFAULT_TIMEZONE,
    create_empty_funding_rate_dataframe,
)
from core.data_client_interface import DataClientInterface


class BinanceFundingRateClient(DataClientInterface):
    """Client for Binance funding rate data."""

    def __init__(
        self,
        symbol: str = "BTCUSDT",
        interval: Union[str, Interval] = Interval.HOUR_8,
        market_type: MarketType = MarketType.FUTURES_USDT,
        use_cache: bool = True,
        cache_dir: Optional[Path] = None,
        max_concurrent: int = 50,
        retry_count: int = 5,
        max_concurrent_downloads: Optional[int] = None,
        client: Optional[AsyncSession] = None,
    ):
        """Initialize the BinanceFundingRateClient.

        Args:
            symbol: Trading pair symbol
            interval: Time interval (for funding rates, this is usually 8h)
            market_type: Market type (must be a futures market)
            use_cache: Whether to use cache
            cache_dir: Path to cache directory
            max_concurrent: Maximum concurrent requests
            retry_count: Number of retries
            max_concurrent_downloads: Maximum concurrent downloads
            client: Optional pre-configured client
        """
        # Validate market type
        if not market_type.is_futures:
            raise ValueError(
                f"Funding rate data is only available for futures markets, not {market_type}"
            )

        # Store parameters
        self._symbol = symbol
        self._interval = (
            interval if isinstance(interval, Interval) else Interval(interval)
        )
        self._market_type = market_type
        self._use_cache = use_cache
        self._cache_dir = cache_dir
        self._max_concurrent = max_concurrent
        self._retry_count = retry_count
        self._max_concurrent_downloads = max_concurrent_downloads

        # Ensure the interval is valid for funding rates (typically 8h for Binance)
        if self._interval != Interval.HOUR_8:
            logger.warning(
                f"Funding rate interval is typically 8h, but {self._interval} was specified"
            )

        # Client for HTTP requests
        self._client = client
        self._client_is_external = client is not None
        self._semaphore = asyncio.Semaphore(max_concurrent)

        # Vision API base URL
        self._base_url = f"https://data.binance.vision/data/{market_type.vision_api_path}/daily/fundingRate"
        logger.debug(
            f"Initialized BinanceFundingRateClient with base URL: {self._base_url}"
        )

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
    def interval(self) -> Interval:
        """Get the interval for this client."""
        return self._interval

    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct structure for funding rate data.

        Returns:
            Empty DataFrame with correct columns and types
        """
        return create_empty_funding_rate_dataframe()

    async def validate_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate that a DataFrame contains valid funding rate data.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if df.empty:
            return True, None  # Empty DataFrame is valid but empty

        # Check index
        if not isinstance(df.index, pd.DatetimeIndex):
            return False, "DataFrame index must be DatetimeIndex"

        if df.index.name != CANONICAL_INDEX_NAME:
            return False, f"DataFrame index name must be {CANONICAL_INDEX_NAME}"

        # Check for required columns
        for col in FUNDING_RATE_DTYPES:
            if col not in df.columns:
                return False, f"Required column '{col}' is missing"

        # Check data types
        for col, dtype in FUNDING_RATE_DTYPES.items():
            if col in df.columns and not pd.api.types.is_dtype_equal(
                df[col].dtype, dtype
            ):
                return (
                    False,
                    f"Column '{col}' has incorrect data type: {df[col].dtype}, expected {dtype}",
                )

        return True, None

    async def is_data_available(self, start_time: datetime, end_time: datetime) -> bool:
        """Check if funding rate data is available for the specified time range.

        Args:
            start_time: Start time
            end_time: End time

        Returns:
            True if data is available, False otherwise
        """
        # For funding rates, we check availability based on date (not time)
        # and the typical 48-hour delay in Vision API data
        now = datetime.now(timezone.utc)
        cutoff_time = now - timedelta(hours=48)  # 48-hour delay

        # If the entire range is after the cutoff, data won't be available
        if start_time > cutoff_time:
            logger.debug(
                f"Funding rate data not available for {start_time} (less than 48 hours old)"
            )
            return False

        # If the range spans the cutoff, truncate it
        if end_time > cutoff_time:
            logger.debug(
                f"Truncating end time from {end_time} to {cutoff_time} due to 48-hour delay"
            )
            end_time = cutoff_time

        return True

    async def _parse_funding_rate_csv(self, csv_text: str) -> pd.DataFrame:
        """Parse funding rate CSV data into a DataFrame.

        Args:
            csv_text: CSV text from funding rate file

        Returns:
            DataFrame with funding rate data
        """
        # Parse the CSV text
        try:
            # Use pandas to parse CSV
            df = pd.read_csv(pd.StringIO(csv_text))

            # Standardize column names
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]

            # Convert time to datetime and set as index
            df["time"] = pd.to_datetime(df["time"])
            df = df.set_index("time")
            df.index.name = CANONICAL_INDEX_NAME  # Standardize index name

            # Filter to the symbol we want
            symbol_pattern = re.compile(f"{self._symbol}", re.IGNORECASE)
            df = df[df["contracts"].str.contains(symbol_pattern)]

            # Convert funding rate from percentage string to float
            df["funding_rate"] = (
                df["funding_rate"].str.rstrip("%").astype(float) / 100.0
            )

            # Ensure timezone awareness
            if df.index.tzinfo is None:
                df.index = df.index.tz_localize(DEFAULT_TIMEZONE)

            return df

        except Exception as e:
            logger.error(f"Error parsing funding rate CSV: {e}")
            return self.create_empty_dataframe()

    async def fetch(
        self, start_time: datetime, end_time: datetime, **kwargs
    ) -> pd.DataFrame:
        """Fetch funding rate data for the specified time range.

        Args:
            start_time: Start time
            end_time: End time
            **kwargs: Additional parameters (unused)

        Returns:
            DataFrame with funding rate data
        """
        # Ensure start_time and end_time are timezone aware
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)

        # Check data availability
        is_available = await self.is_data_available(start_time, end_time)
        if not is_available:
            logger.warning(
                f"Funding rate data not available for {start_time} to {end_time}"
            )
            return self.create_empty_dataframe()

        # Create a client if needed
        if not self._client:
            self._client = create_client(timeout=DEFAULT_HTTP_TIMEOUT_SECONDS)

        # We need to fetch daily files for each day in the range
        current_date = start_time.date()
        end_date = end_time.date()

        # Track all data frames to concatenate at the end
        dfs = []

        while current_date <= end_date:
            # Construct URL for daily funding rate file
            date_str = current_date.strftime("%Y-%m-%d")
            symbol_upper = self._symbol.upper()

            # Format for USDT-margined futures is like: "BTCUSDT"
            # Format for COIN-margined futures is like: "BTCUSD_PERP"
            symbol_formatted = symbol_upper
            if (
                self._market_type == MarketType.FUTURES_COIN
                and not symbol_upper.endswith("_PERP")
            ):
                symbol_formatted = f"{symbol_upper}_PERP"

            url = f"{self._base_url}/{date_str}/{symbol_formatted}-fundingRate-{date_str}.zip"

            try:
                async with self._semaphore:
                    logger.debug(f"Fetching funding rate data from {url}")
                    response = await self._client.get(url)

                    if response.status_code == 200:
                        # Parse the response (CSV in a zip file)
                        from io import BytesIO
                        import zipfile

                        zip_content = BytesIO(response.content)
                        with zipfile.ZipFile(zip_content) as zf:
                            # Extract CSV file
                            csv_filename = zf.namelist()[0]  # Should have only one file
                            csv_text = zf.read(csv_filename).decode("utf-8")

                            # Parse CSV
                            df = await self._parse_funding_rate_csv(csv_text)
                            if not df.empty:
                                dfs.append(df)
                    else:
                        logger.warning(
                            f"Failed to fetch funding rate data: HTTP {response.status_code}"
                        )

            except Exception as e:
                logger.error(f"Error fetching funding rate data for {date_str}: {e}")

            # Move to the next day
            current_date += timedelta(days=1)

        # Combine all data frames
        if not dfs:
            logger.warning("No funding rate data found for the specified time range")
            return self.create_empty_dataframe()

        result_df = pd.concat(dfs)

        # Filter to the exact time range requested
        result_df = filter_dataframe_by_time(result_df, start_time, end_time)

        # Sort by time
        result_df = result_df.sort_index()

        return result_df

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._client:
            self._client = create_client(timeout=DEFAULT_HTTP_TIMEOUT_SECONDS)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._client and not self._client_is_external:
            await safely_close_client(self._client)
            self._client = None
