#!/usr/bin/env python
"""VisionDataClient provides direct access to Binance Vision API for historical data.

This module implements a client for retrieving historical market data from the
Binance Vision API. It provides functions for fetching, validating, and processing data.

Functionality:
- Fetch historical market data by symbol, interval, and time range
- Validate data integrity and structure
- Process data into pandas DataFrames for analysis

The VisionDataClient is primarily used through the DataSourceManager, which provides
a unified interface for data retrieval with automatic source selection and caching.

For most use cases, users should interact with the DataSourceManager rather than
directly with this client.
"""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Sequence, TypeVar, Generic, Union

import pandas as pd
import warnings

from utils.logger_setup import get_logger
from utils.validation_utils import validate_dataframe
from utils.market_constraints import Interval, MarketType
from utils.time_utils import (
    validate_time_window,
    align_time_boundaries,
    enforce_utc_timezone,
    filter_dataframe_by_time,
)
from utils.network_utils import (
    create_client,
    VisionDownloadManager,
    safely_close_client,
)
from utils.config import create_empty_dataframe
from core.vision_constraints import (
    TimestampedDataFrame,
    MAX_CONCURRENT_DOWNLOADS,
)

# Define the type variable for VisionDataClient
T = TypeVar("T")

logger = get_logger(__name__, "DEBUG", show_path=False)


class VisionDataClient(Generic[T]):
    """Vision Data Client for direct access to Binance historical data."""

    def __init__(
        self,
        symbol: str,
        interval: str = "1s",
        market_type: Union[str, MarketType] = MarketType.SPOT,
        cache_dir: Optional[Path] = None,
        use_cache: bool = False,
        max_concurrent_downloads: Optional[int] = None,
    ):
        """Initialize Vision Data Client.

        Args:
            symbol: Trading symbol e.g. 'BTCUSDT'
            interval: Kline interval e.g. '1s', '1m'
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN) or string
            cache_dir: Legacy parameter, no longer used (use DataSourceManager for caching)
            use_cache: Legacy parameter, no longer used (use DataSourceManager for caching)
            max_concurrent_downloads: Maximum concurrent downloads
        """
        self.symbol = symbol.upper()
        self.interval = interval
        self.market_type = market_type

        # Convert MarketType enum to string if needed
        market_type_str = market_type
        if isinstance(market_type, MarketType):
            if market_type == MarketType.SPOT:
                market_type_str = "spot"
            elif market_type == MarketType.FUTURES_USDT:
                market_type_str = "futures_usdt"
            elif market_type == MarketType.FUTURES_COIN:
                market_type_str = "futures_coin"
            elif market_type == MarketType.FUTURES:
                market_type_str = "futures_usdt"  # Default to USDT for legacy type
            else:
                raise ValueError(f"Unsupported market type: {market_type}")

        # Parse interval string to Interval object
        try:
            # Try to find the interval enum by value
            self.interval_obj = next((i for i in Interval if i.value == interval), None)
            if self.interval_obj is None:
                # Try by enum name (upper case with _ instead of number)
                try:
                    self.interval_obj = Interval[interval.upper()]
                except KeyError:
                    raise ValueError(f"Invalid interval: {interval}")
        except Exception as e:
            logger.warning(
                f"Could not parse interval {interval}, using SECOND_1 as default: {e}"
            )
            self.interval_obj = Interval.SECOND_1

        # Legacy parameters (no longer used)
        self.use_cache = use_cache
        self.cache_dir = cache_dir
        self._current_mmap = None
        self._current_mmap_path = None

        # Emit deprecation warning if caching is enabled
        if use_cache and cache_dir:
            warnings.warn(
                "Direct caching through VisionDataClient is deprecated. "
                "Use DataSourceManager with caching enabled instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.cache_manager = None
            self.symbol_cache_dir = None
        else:
            self.cache_manager = None
            self.symbol_cache_dir = None

        # Configure download concurrency
        self._max_concurrent_downloads = (
            max_concurrent_downloads or MAX_CONCURRENT_DOWNLOADS
        )
        # Prepare HTTP client for API access - use curl_cffi for better performance
        self._client = create_client(
            timeout=3.0
        )  # Reduced from 30s to 3s for optimal performance (benchmark best practice)
        # Initialize download manager
        self._download_manager = VisionDownloadManager(
            client=self._client,
            symbol=self.symbol,
            interval=self.interval,
            market_type=market_type_str,
        )

    async def __aenter__(self) -> "VisionDataClient":
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[object],
    ) -> None:
        """Async context manager exit."""
        # Clean up memory map resources if they exist
        if hasattr(self, "_current_mmap") and self._current_mmap is not None:
            try:
                logger.debug("Closing memory map resources")
                self._current_mmap.close()
            except Exception as e:
                logger.warning(f"Error closing memory map: {str(e)}")
            finally:
                self._current_mmap = None
                self._current_mmap_path = None

        # Close HTTP client using the standardized safely_close_client function
        if hasattr(self, "_client") and self._client is not None:
            try:
                logger.debug("Closing VisionDataClient HTTP client")
                await safely_close_client(self._client)
            except Exception as e:
                logger.warning(f"Error closing VisionDataClient HTTP client: {str(e)}")
            finally:
                self._client = None

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with correct structure.

        Returns:
            Empty DataFrame with standardized structure
        """
        return create_empty_dataframe()

    def _adjust_concurrency(self, batch_size: int) -> int:
        """Dynamically adjust concurrency based on batch size.

        Args:
            batch_size: Number of items in the batch

        Returns:
            Adjusted concurrency value optimized for the batch size
        """
        if batch_size <= 10:
            return min(10, self._max_concurrent_downloads)
        elif batch_size <= 50:
            return min(50, self._max_concurrent_downloads)
        else:
            return min(100, self._max_concurrent_downloads)

    async def _download_data(
        self,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> TimestampedDataFrame:
        """Download data for the specified time range.

        This method applies manual alignment to Vision API requests to match
        REST API's natural boundary behavior.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            columns: Optional list of columns to return

        Returns:
            DataFrame with market data
        """
        # Use time_utils to enforce timezone
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)

        # Use align_time_boundaries directly from time_utils
        aligned_start, aligned_end = align_time_boundaries(
            start_time, end_time, self.interval_obj
        )
        start_time = aligned_start
        end_time = aligned_end

        logger.debug(
            f"Vision API request with aligned boundaries: {aligned_start} -> {aligned_end} "
            f"(to match REST API behavior)"
        )

        # Get list of dates to download
        current_date = aligned_start.replace(hour=0, minute=0, second=0, microsecond=0)
        dates = []
        while current_date <= aligned_end.replace(
            hour=0, minute=0, second=0, microsecond=0
        ):
            dates.append(current_date)
            current_date += timedelta(days=1)

        # Dynamically adjust concurrency based on batch size
        batch_size = len(dates)
        adjusted_concurrency = self._adjust_concurrency(batch_size)

        if adjusted_concurrency != self._max_concurrent_downloads:
            logger.debug(
                f"Adjusting concurrency to {adjusted_concurrency} for {batch_size} dates"
            )

        # Download data for each date
        all_dfs = []
        semaphore = asyncio.Semaphore(adjusted_concurrency)
        download_tasks = []

        for date in dates:
            download_tasks.append(self._download_date(date, semaphore, columns))

        try:
            results = await asyncio.gather(*download_tasks)
            for df in results:
                if df is not None and not df.empty:
                    all_dfs.append(df)
        except Exception as e:
            logger.error(f"Error downloading data: {e}")

        if not all_dfs:
            return TimestampedDataFrame(self._create_empty_dataframe())

        # Combine all data
        combined_df = pd.concat(all_dfs).sort_index()

        # Apply final filtering to match original requested time range
        result_df = filter_dataframe_by_time(combined_df, start_time, end_time)

        return TimestampedDataFrame(result_df)

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> TimestampedDataFrame:
        """Fetch data directly from Binance Vision API.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            columns: Optional list of specific columns to retrieve

        Returns:
            DataFrame containing requested data
        """
        # Validate and normalize time range
        validate_time_window(start_time, end_time)

        # Use align_time_boundaries directly from time_utils
        aligned_start, aligned_end = align_time_boundaries(
            start_time, end_time, self.interval_obj
        )
        start_time = aligned_start
        end_time = aligned_end

        logger.debug(
            f"Fetching {self.symbol} {self.interval} data: "
            f"{start_time.isoformat()} -> {end_time.isoformat()}"
        )

        # If caching is enabled, emit deprecation warning
        if self.use_cache:
            warnings.warn(
                "Direct caching through VisionDataClient is deprecated. "
                "Use DataSourceManager with caching enabled instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        # Download data directly
        try:
            df = await self._download_data(start_time, end_time, columns=columns)
            if not df.empty:
                # Validate data integrity
                validate_dataframe(df)
                # Filter DataFrame to ensure it's within the requested time boundaries
                df = filter_dataframe_by_time(df, start_time, end_time)
                logger.debug(f"Successfully fetched {len(df)} records")
                return df

            logger.warning(f"No data available for {start_time} to {end_time}")
            return TimestampedDataFrame(self._create_empty_dataframe())

        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return TimestampedDataFrame(self._create_empty_dataframe())

    async def prefetch(
        self, start_time: datetime, end_time: datetime, max_days: int = 5
    ) -> None:
        """Prefetch data in background for future use.

        This downloads data for later use but does not cache it.
        For caching, use DataSourceManager instead.

        Args:
            start_time: Start time for prefetch
            end_time: End time for prefetch
            max_days: Maximum number of days to prefetch
        """
        # Validate time range and enforce UTC timezone using time_utils
        start_time = enforce_utc_timezone(start_time)
        end_time = enforce_utc_timezone(end_time)
        validate_time_window(start_time, end_time)

        # Limit prefetch to max_days
        limited_end = min(end_time, start_time + timedelta(days=max_days))
        logger.debug(f"Prefetching data from {start_time} to {limited_end}")

        # Download data directly
        try:
            await self._download_data(start_time, limited_end)
            logger.debug(f"Prefetch completed for {start_time} to {limited_end}")
        except Exception as e:
            logger.error(f"Error during prefetch: {e}")

    async def _download_date(
        self,
        date: datetime,
        semaphore: asyncio.Semaphore,
        columns: Optional[Sequence[str]] = None,
    ) -> Optional[pd.DataFrame]:
        """Download data for a specific date using direct download-first approach.

        Args:
            date: Date to download data for
            semaphore: Semaphore for concurrency control
            columns: Optional list of columns to return

        Returns:
            DataFrame with data for the date, or None if download failed
        """
        async with semaphore:
            try:
                logger.debug(f"Downloading data for date: {date.strftime('%Y-%m-%d')}")

                # Directly attempt to download without pre-checking
                # This is faster than checking if the file exists first
                df = await self._download_manager.download_date(date)

                if df is None or df.empty:
                    logger.debug(f"No data for date: {date.strftime('%Y-%m-%d')}")
                    return None

                return df

            except Exception as e:
                logger.error(
                    f"Error downloading data for {date.strftime('%Y-%m-%d')}: {e}"
                )
                return None
