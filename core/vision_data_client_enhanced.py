#!/usr/bin/env python
"""Enhanced Vision Data Client with comprehensive interval support.

This client implements a high-performance data fetching system with:
- Comprehensive error classification and recovery
- Multi-layered validation framework
- Full support for all Binance Vision API intervals

Supported intervals:
- 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M

IMPORTANT: For caching needs, use DataSourceManager instead of direct VisionDataClient.

Recommended Usage:
```python
manager = DataSourceManager(
    cache_dir=Path("./cache"),
    use_cache=True
)
df = await manager.get_data(
    symbol="BTCUSDT",
    start_time=start_time,
    end_time=end_time,
    enforce_source=DataSource.VISION  # If Vision API is specifically needed
)
```

Benefits of using DataSourceManager:
- Centralized caching through UnifiedCacheManager
- Smart source selection between REST and Vision APIs
- Consistent data format and validation
- Better error handling and retry logic
- Optimized memory usage with MMAP
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Sequence, TypeVar, Generic, Dict, Any, List, Tuple
import pandas as pd
import asyncio
import re

from utils.logger_setup import get_logger
from utils.validation import DataFrameValidator, DataValidation
from utils.market_constraints import Interval, MarketType
from utils.time_alignment import TimeRangeManager, get_interval_floor
from utils.download_handler import VisionDownloadManager
from utils.config import create_empty_dataframe
from utils.http_client_factory import create_client
from core.vision_constraints import (
    TimestampedDataFrame,
    MAX_CONCURRENT_DOWNLOADS,
    get_vision_url,
    FileType,
    validate_symbol_format,
    CONSOLIDATION_DELAY,
)

# Define the type variable for VisionDataClient
T = TypeVar("T")

logger = get_logger(__name__, "INFO", show_path=False)


class VisionDataClient(Generic[T]):
    """Enhanced Vision Data Client with comprehensive interval support."""

    def __init__(
        self,
        symbol: str,
        interval: str = "1s",
        cache_dir: Optional[Path] = None,
        use_cache: bool = False,
        max_concurrent_downloads: Optional[int] = None,
        market_type: str = "spot",
    ):
        """Initialize Vision Data Client.

        Args:
            symbol: Trading symbol e.g. 'BTCUSDT'
            interval: Kline interval e.g. '1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'
            cache_dir: [DEPRECATED] Optional directory for caching data
            use_cache: [DEPRECATED] Whether to use cache - no longer supported
            max_concurrent_downloads: Maximum concurrent downloads
            market_type: Market type (spot, futures_usdt, futures_coin)
        """
        # Validate symbol format
        self.symbol = symbol.upper()
        validate_symbol_format(self.symbol)

        self.interval = interval
        self.market_type = market_type.lower()

        # Parse interval string to Interval object with enhanced validation
        try:
            # Try to find the interval enum by value
            self.interval_obj = next((i for i in Interval if i.value == interval), None)
            if self.interval_obj is None:
                # Try by enum name (upper case with _ instead of number)
                try:
                    self.interval_obj = Interval[interval.upper()]
                except KeyError:
                    # More detailed error message with supported intervals
                    supported_intervals = [i.value for i in Interval]
                    raise ValueError(
                        f"Invalid interval: {interval}. Supported intervals: {', '.join(supported_intervals)}"
                    )
        except Exception as e:
            logger.warning(
                f"Could not parse interval {interval}, using SECOND_1 as default: {e}"
            )
            self.interval_obj = Interval.SECOND_1

        # Validate interval compatibility with market type
        if not self.is_interval_available_for_market(interval, market_type):
            logger.warning(
                f"Interval {interval} may not be available for {market_type} market. "
                f"Some data requests may fail."
            )

        # If caching parameters are provided, log a warning - caching is now handled by DataSourceManager
        if use_cache or cache_dir:
            logger.warning(
                "Direct caching through VisionDataClient has been removed. "
                "Use DataSourceManager with caching enabled instead."
            )

        # These will always be None/False
        self.use_cache = False
        self.cache_dir = None
        self.cache_manager = None
        self.symbol_cache_dir = None

        # Configure download concurrency
        self._max_concurrent_downloads = (
            max_concurrent_downloads or MAX_CONCURRENT_DOWNLOADS
        )
        # Prepare HTTP client for API access
        self._client = create_client(client_type="httpx", timeout=30)
        # Initialize download manager
        self._download_manager = VisionDownloadManager(
            client=self._client,
            symbol=self.symbol,
            interval=self.interval,
            market_type=self.market_type,
        )

        # Validator for checking results
        self._validator = DataFrameValidator()

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
        # Close HTTP client
        try:
            await self._client.aclose()
        except Exception as e:
            logger.warning(f"Error closing HTTP client: {e}")

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with correct structure.

        Returns:
            Empty DataFrame with standardized structure
        """
        return create_empty_dataframe()

    def _get_expected_records_per_day(self) -> int:
        """Get expected number of records per day based on interval.

        Returns:
            Expected number of records per day
        """
        seconds_per_day = 24 * 60 * 60  # 86400 seconds in a day
        interval_seconds = self.interval_obj.to_seconds()
        return seconds_per_day // interval_seconds

    def get_expected_records_for_timerange(
        self, start_time: datetime, end_time: datetime
    ) -> int:
        """Calculate expected number of records for a time range.

        Args:
            start_time: Start time
            end_time: End time

        Returns:
            Expected number of records
        """
        # Calculate the time difference in seconds
        time_diff = (end_time - start_time).total_seconds()
        # Calculate expected records based on interval
        interval_seconds = self.interval_obj.to_seconds()
        return int(time_diff / interval_seconds)

    def get_interval_seconds(self) -> int:
        """Get the number of seconds in the interval.

        Returns:
            Number of seconds in the interval
        """
        return self.interval_obj.to_seconds()

    def validate_interval(self, interval: str) -> bool:
        """Validate if an interval is supported.

        Args:
            interval: Interval to validate

        Returns:
            True if interval is valid, False otherwise
        """
        try:
            # Check if it's in the list of supported intervals
            return interval in [i.value for i in Interval]
        except Exception:
            return False

    @staticmethod
    def get_supported_intervals() -> List[str]:
        """Get a list of all available intervals supported by Vision API.

        Returns:
            List of supported intervals
        """
        return [i.value for i in Interval]

    @staticmethod
    def is_interval_available_for_market(
        interval: str, market_type: str = "spot"
    ) -> bool:
        """Check if an interval is available for a specific market type.

        Args:
            interval: Interval to check
            market_type: Market type (spot, futures_usdt, futures_coin)

        Returns:
            True if interval is available, False otherwise
        """
        market_type = market_type.lower()

        # All intervals are available for spot
        if market_type == "spot":
            return interval in VisionDataClient.get_supported_intervals()

        # Futures markets don't support 1s interval
        if market_type in ["futures_usdt", "futures_coin"] and interval == "1s":
            return False

        # Check if it's a valid interval for other markets
        return interval in VisionDataClient.get_supported_intervals()

    async def _download_and_cache(
        self,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> TimestampedDataFrame:
        """Download data for the specified time range (without caching).

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            columns: Optional list of specific columns to retrieve

        Returns:
            DataFrame containing requested data
        """
        # Use TimeRangeManager to validate and normalize time range
        start_time = TimeRangeManager.enforce_utc_timezone(start_time)
        end_time = TimeRangeManager.enforce_utc_timezone(end_time)

        # Get list of dates to download
        current_date = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        dates = []
        while current_date < end_time:
            dates.append(current_date)
            current_date += timedelta(days=1)

        logger.info(
            f"Downloading {self.symbol} {self.interval} data for dates: {[d.strftime('%Y-%m-%d') for d in dates]}"
        )

        # Download data for each date
        dfs = []
        download_tasks = []

        # Create download tasks with concurrency limit
        for date in dates:
            # Check if data is likely available based on consolidation delay
            if not DataValidation.is_data_likely_available(date, CONSOLIDATION_DELAY):
                logger.warning(
                    f"Data for {date.strftime('%Y-%m-%d')} may not be available yet due to consolidation delay"
                )
                continue

            download_tasks.append(self._download_manager.download_date(date))

            # Process downloads in batches to limit concurrency
            if len(download_tasks) >= self._max_concurrent_downloads:
                results = await asyncio.gather(*download_tasks, return_exceptions=True)
                download_tasks = []

                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Download error: {result}")
                        continue
                    if result is not None and not result.empty:
                        dfs.append(result)

        # Process any remaining download tasks
        if download_tasks:
            results = await asyncio.gather(*download_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Download error: {result}")
                    continue
                if result is not None and not result.empty:
                    dfs.append(result)

        # Process downloaded data
        processed_dfs = []
        for df in dfs:
            try:
                # Filter to requested time range using TimeRangeManager
                filtered_df = TimeRangeManager.filter_dataframe(
                    df, start_time, end_time
                )

                if not filtered_df.empty:
                    processed_dfs.append(filtered_df)
            except Exception as e:
                logger.error(f"Error processing downloaded data: {e}")
                continue

        if not processed_dfs:
            logger.error(f"No data available for {start_time} to {end_time}")
            return TimestampedDataFrame(self._create_empty_dataframe())

        # Combine all data
        combined_df = pd.concat(processed_dfs, axis=0)

        # Sort by index and remove duplicates
        combined_df = combined_df.sort_index()
        if combined_df.index.has_duplicates:
            logger.warning(
                f"Found {combined_df.index.duplicated().sum()} duplicate timestamps, keeping first occurrence"
            )
            combined_df = combined_df[~combined_df.index.duplicated(keep="first")]

        # Filter columns if specified
        if columns is not None:
            available_columns = set(combined_df.columns)
            requested_columns = set(columns)
            missing_columns = requested_columns - available_columns

            if missing_columns:
                logger.warning(f"Requested columns not available: {missing_columns}")
                # Only filter by available requested columns
                columns_to_use = list(requested_columns & available_columns)
                combined_df = combined_df[columns_to_use]
            else:
                combined_df = combined_df[columns]

        return TimestampedDataFrame(combined_df)

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> TimestampedDataFrame:
        """Fetch data from Binance Vision API with enhanced interval support.

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            columns: Optional list of specific columns to retrieve

        Returns:
            DataFrame containing requested data
        """
        # Validate and normalize time range using centralized utility
        TimeRangeManager.validate_time_window(start_time, end_time)

        # Get time boundaries using the centralized manager
        time_boundaries = TimeRangeManager.get_time_boundaries(
            start_time, end_time, self.interval_obj
        )
        start_time = time_boundaries["adjusted_start"]
        end_time = time_boundaries["adjusted_end"]

        logger.info(
            f"Fetching {self.symbol} {self.interval} data: "
            f"{start_time.isoformat()} -> {end_time.isoformat()} (exclusive end)"
        )

        # Check if the requested data is likely available based on consolidation delay
        if not self._check_data_availability(start_time, end_time):
            logger.warning(
                f"Requested data for {start_time} to {end_time} may not be available yet "
                f"due to Vision API consolidation delay (~48 hours)"
            )
            # Return empty DataFrame with correct structure
            return TimestampedDataFrame(self._create_empty_dataframe())

        # Direct fetch (no caching)
        try:
            df = await self._download_and_cache(start_time, end_time, columns=columns)
            if not df.empty:
                # Validate data integrity
                self._validator.validate_dataframe(df)
                TimeRangeManager.validate_boundaries(df, start_time, end_time)
                logger.info(f"Successfully fetched {len(df)} records")
                return df

            logger.warning(f"No data available for {start_time} to {end_time}")
            return TimestampedDataFrame(self._create_empty_dataframe())

        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return TimestampedDataFrame(self._create_empty_dataframe())

    def _check_data_availability(
        self, start_time: datetime, end_time: datetime
    ) -> bool:
        """Check if data is likely available for the specified time range.

        Args:
            start_time: Start time
            end_time: End time

        Returns:
            True if data is likely available, False otherwise
        """
        # Check if any part of the requested range is too recent (within consolidation delay)
        now = datetime.now(timezone.utc)
        consolidation_time = now - CONSOLIDATION_DELAY

        # If end_time is more recent than consolidation_time, data might not be available
        if end_time > consolidation_time:
            logger.warning(
                f"Requested end time {end_time} is within consolidation delay window "
                f"(data available up to approximately {consolidation_time})"
            )
            # We still return True but log a warning, as some data might be available
            return True

        return True

    async def prefetch(
        self, start_time: datetime, end_time: datetime, max_days: int = 5
    ) -> None:
        """Prefetch data in background for future use (without caching).

        Note: This method performs the download operations but doesn't cache results.
        For cached prefetching, use DataSourceManager.

        Args:
            start_time: Start time for prefetch
            end_time: End time for prefetch
            max_days: Maximum number of days to prefetch
        """
        # Validate time range and enforce UTC timezone using TimeRangeManager
        start_time = TimeRangeManager.enforce_utc_timezone(start_time)
        end_time = TimeRangeManager.enforce_utc_timezone(end_time)
        TimeRangeManager.validate_time_window(start_time, end_time)

        # Limit prefetch to max_days
        limited_end = min(end_time, start_time + timedelta(days=max_days))
        logger.info(f"Prefetching data from {start_time} to {limited_end}")

        # Just call the download method which will handle fetching
        try:
            await self._download_and_cache(start_time, limited_end)
            logger.info(f"Prefetch completed for {start_time} to {limited_end}")
        except Exception as e:
            logger.error(f"Error during prefetch: {e}")

    async def batch_fetch(
        self,
        symbols: List[str],
        start_time: datetime,
        end_time: datetime,
        interval: Optional[str] = None,
    ) -> Dict[str, TimestampedDataFrame]:
        """Fetch data for multiple symbols in a single batch operation.

        Args:
            symbols: List of symbols to fetch
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            interval: Optional interval override (defaults to instance interval)

        Returns:
            Dictionary mapping symbols to their respective DataFrames
        """
        results = {}
        interval_to_use = interval or self.interval

        # Validate inputs
        TimeRangeManager.validate_time_window(start_time, end_time)
        if not symbols:
            logger.warning("Empty symbols list provided to batch_fetch")
            return {}

        # Process symbols in parallel with concurrency limit
        tasks = []
        for symbol in symbols:
            # Create a temporary client for each symbol
            client = VisionDataClient(
                symbol=symbol,
                interval=interval_to_use,
                market_type=self.market_type,
            )
            tasks.append(client.fetch(start_time, end_time))

        # Process in batches to limit concurrency
        batch_size = self._max_concurrent_downloads
        for i in range(0, len(tasks), batch_size):
            batch_tasks = tasks[i : i + batch_size]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Process batch results
            for j, result in enumerate(batch_results):
                symbol = symbols[i + j]
                if isinstance(result, Exception):
                    logger.error(f"Error fetching data for {symbol}: {result}")
                    results[symbol] = TimestampedDataFrame(
                        self._create_empty_dataframe()
                    )
                else:
                    results[symbol] = result

        return results
