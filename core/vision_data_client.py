#!/usr/bin/env python
r"""VisionDataClient provides direct access to Binance Vision API for historical data.

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
from typing import Optional, Sequence, TypeVar, Generic, Union
import gc
import time

import pandas as pd

from utils.logger_setup import logger
from utils.validation import DataFrameValidator, DataValidation
from utils.market_constraints import Interval, MarketType, ChartType
from utils.time_utils import (
    align_time_boundaries,
    filter_dataframe_by_time,
    TimeseriesDataProcessor,
)
from utils.network_utils import (
    create_client,
    VisionDownloadManager,
    safely_close_client,
)
from utils.config import (
    create_empty_dataframe,
    KLINE_COLUMNS,
    standardize_column_names,
    MAX_TIMEOUT,
)
from utils.async_cleanup import direct_resource_cleanup, cleanup_file_handle
from core.vision_constraints import (
    TimestampedDataFrame,
    MAX_CONCURRENT_DOWNLOADS,
)

# Define the type variable for VisionDataClient
T = TypeVar("T")


class VisionDataClient(Generic[T]):
    """Vision Data Client for direct access to Binance historical data."""

    def __init__(
        self,
        symbol: str,
        interval: str = "1s",
        market_type: Union[str, MarketType] = MarketType.SPOT,
        max_concurrent_downloads: Optional[int] = None,
    ):
        """Initialize Vision Data Client.

        Args:
            symbol: Trading symbol e.g. 'BTCUSDT'
            interval: Kline interval e.g. '1s', '1m'
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN) or string
            max_concurrent_downloads: Maximum concurrent downloads
        """
        self.symbol = symbol.upper()
        self.interval = interval
        self.market_type = market_type

        # Convert MarketType enum to string if needed
        market_type_str = market_type
        if isinstance(market_type, MarketType):
            try:
                market_name = market_type.name
                if market_name == "SPOT":
                    market_type_str = "spot"
                elif market_name == "FUTURES_USDT":
                    market_type_str = "futures_usdt"
                elif market_name == "FUTURES_COIN":
                    market_type_str = "futures_coin"
                elif market_name == "FUTURES":
                    market_type_str = "futures_usdt"  # Default to USDT for legacy type
                else:
                    raise ValueError(f"Unsupported market type: {market_type}")
            except (AttributeError, TypeError):
                # Fallback to string representation for safer comparison
                market_str = str(market_type).upper()
                if "SPOT" in market_str:
                    market_type_str = "spot"
                elif "FUTURES_USDT" in market_str or "FUTURES" == market_str:
                    market_type_str = "futures_usdt"
                elif "FUTURES_COIN" in market_str:
                    market_type_str = "futures_coin"
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
        # Proactively clean up any force_timeout tasks that might cause hanging
        await self._cleanup_force_timeout_tasks()
        return self

    async def _cleanup_force_timeout_tasks(self):
        """Find and clean up any _force_timeout tasks that might cause hanging.

        This is a proactive approach to prevent hanging issues caused by
        lingering force_timeout tasks in curl_cffi AsyncCurl objects.

        Returns:
            int: Number of tasks cancelled
        """
        # Find all tasks that might be related to _force_timeout
        force_timeout_tasks = []
        for task in asyncio.all_tasks():
            task_str = str(task)
            # Look specifically for _force_timeout tasks and any curl_cffi related tasks
            if (
                "_force_timeout" in task_str or "curl_cffi" in task_str
            ) and not task.done():
                force_timeout_tasks.append(task)

        cancelled_count = 0
        if force_timeout_tasks:
            logger.warning(
                f"[ProgressIndicator] VisionDataClient: Proactively cancelling {len(force_timeout_tasks)} tasks (force_timeout/curl_cffi)"
            )
            # First cancel any futures being waited on
            for task in force_timeout_tasks:
                # Try to cancel the future the task is waiting on
                if (
                    hasattr(task, "_fut_waiter")
                    and task._fut_waiter
                    and not task._fut_waiter.done()
                ):
                    try:
                        logger.debug(
                            f"[ProgressIndicator] VisionDataClient: Cancelling future for task: {task}"
                        )
                        task._fut_waiter.cancel()
                    except Exception as e:
                        logger.debug(f"Error cancelling future: {e}")

                # Then cancel the task itself
                task.cancel()
                cancelled_count += 1

            # Wait for cancellation to complete with timeout
            try:
                # Use wait with timeout instead of wait_for to avoid exceptions
                done, pending = await asyncio.wait(force_timeout_tasks, timeout=0.5)

                # Check if there are still pending tasks after the timeout
                if pending:
                    logger.warning(
                        f"[ProgressIndicator] VisionDataClient: {len(pending)} tasks still pending after cancellation attempt"
                    )
                    # More aggressive cancellation for stubborn tasks
                    for task in pending:
                        task.cancel()

                        # Try to force the task to wake up from any wait state
                        if hasattr(task, "_coro") and task._coro:
                            try:
                                if hasattr(task._coro, "throw"):
                                    task._coro.throw(asyncio.CancelledError())
                            except Exception:
                                pass
            except Exception as e:
                logger.warning(
                    f"[ProgressIndicator] VisionDataClient: Error during task cancellation wait: {e}"
                )

            # Force garbage collection to help with lingering references
            gc.collect()

            # Log the final state of the tasks
            still_pending = [t for t in force_timeout_tasks if not t.done()]
            if still_pending:
                logger.warning(
                    f"[ProgressIndicator] VisionDataClient: {len(still_pending)} tasks still not cancelled after cleanup"
                )
            else:
                logger.debug(
                    "[ProgressIndicator] VisionDataClient: All targeted tasks successfully cancelled"
                )

        # Also clean _timeout_handle if it exists on the client
        if (
            hasattr(self, "_client")
            and self._client
            and hasattr(self._client, "_timeout_handle")
            and self._client._timeout_handle
        ):
            logger.debug(
                "[ProgressIndicator] VisionDataClient: Pre-emptively cleaning _timeout_handle to prevent hanging"
            )
            try:
                self._client._timeout_handle = None
            except Exception as e:
                logger.warning(
                    f"[ProgressIndicator] VisionDataClient: Error pre-emptively clearing _timeout_handle: {e}"
                )

        return cancelled_count

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        """Python 3.13 compatible cleanup implementation.

        This uses the direct resource cleanup pattern to guarantee immediate resource
        release without relying on background tasks, preventing hanging during cleanup.
        """
        logger.debug("[ProgressIndicator] VisionDataClient: Starting __aexit__ cleanup")

        cleanup_errors = []
        cleanup_start_time = time.time()

        # Track all active tasks before cleanup for diagnosis
        pre_cleanup_tasks = [t for t in asyncio.all_tasks() if not t.done()]
        logger.debug(
            f"[ProgressIndicator] VisionDataClient: {len(pre_cleanup_tasks)} active tasks before cleanup"
        )
        for i, task in enumerate(pre_cleanup_tasks[:5]):  # Show up to 5 tasks
            task_str = str(task)
            short_task = task_str[:200] + "..." if len(task_str) > 200 else task_str
            logger.debug(
                f"[ProgressIndicator] VisionDataClient: Pre-cleanup task {i+1}: {short_task}"
            )

        # STEP 1: Pre-emptively clean up _curlm objects that might cause hanging
        if hasattr(self, "_client") and self._client:
            if hasattr(self._client, "_curlm") and self._client._curlm:
                logger.debug(
                    "[ProgressIndicator] VisionDataClient: Breaking _curlm circular reference"
                )
                try:
                    self._client._curlm = None
                except Exception as e:
                    error_msg = f"Error clearing _curlm: {e}"
                    logger.warning(error_msg)
                    cleanup_errors.append(error_msg)

            # Also clear _timeout_handle if it exists
            if (
                hasattr(self._client, "_timeout_handle")
                and self._client._timeout_handle
            ):
                logger.debug(
                    "[ProgressIndicator] VisionDataClient: Breaking _timeout_handle reference"
                )
                try:
                    self._client._timeout_handle = None
                except Exception as e:
                    error_msg = f"Error clearing _timeout_handle: {e}"
                    logger.warning(error_msg)
                    cleanup_errors.append(error_msg)

        # STEP 2: Clean up tasks that might cause hanging
        try:
            logger.debug(
                "[ProgressIndicator] VisionDataClient: Cancelling _force_timeout tasks"
            )
            cancelled_count = await self._cleanup_force_timeout_tasks()
            logger.debug(
                f"[ProgressIndicator] VisionDataClient: Cancelled {cancelled_count} _force_timeout tasks"
            )
        except Exception as e:
            error_msg = f"Error during force timeout task cleanup: {e}"
            logger.error(error_msg)
            cleanup_errors.append(error_msg)

        # STEP 3: Cancel any other curl_cffi related tasks directly
        try:
            # Find any curl_cffi related tasks that might be still running
            curl_tasks = [
                t
                for t in asyncio.all_tasks()
                if not t.done()
                and (
                    "curl_cffi" in str(t)
                    or "vision" in str(t).lower()
                    or "download" in str(t).lower()
                )
            ]

            if curl_tasks:
                logger.warning(
                    f"[ProgressIndicator] VisionDataClient: Found {len(curl_tasks)} lingering curl/vision tasks, cancelling"
                )
                for task in curl_tasks:
                    task.cancel()

                try:
                    # Wait briefly for cancellation with a shorter timeout
                    await asyncio.wait(curl_tasks, timeout=0.5)

                    # Log tasks that are still running after cancellation attempt
                    still_running = [t for t in curl_tasks if not t.done()]
                    if still_running:
                        logger.warning(
                            f"[ProgressIndicator] VisionDataClient: {len(still_running)} tasks still running after cancellation"
                        )
                except Exception as e:
                    logger.warning(
                        f"[ProgressIndicator] VisionDataClient: Error waiting for task cancellation: {e}"
                    )
        except Exception as e:
            error_msg = f"Error cancelling lingering tasks: {e}"
            logger.error(error_msg)
            cleanup_errors.append(error_msg)

        # STEP 4: Use direct resource cleanup for better reliability
        try:
            # A. Clean up download manager first (most important)
            if hasattr(self, "_download_manager") and self._download_manager:
                logger.debug(
                    "[ProgressIndicator] VisionDataClient: Cleaning up download manager"
                )
                try:
                    # First try using its cleanup method if available
                    if hasattr(
                        self._download_manager, "_cleanup_resources"
                    ) and callable(self._download_manager._cleanup_resources):
                        await self._download_manager._cleanup_resources()

                    # Then clear all possible references within the download manager
                    if hasattr(self._download_manager, "client"):
                        if (
                            self._download_manager.client is not self._client
                        ):  # Only if it's a different client
                            from utils.network_utils import safely_close_client

                            logger.debug(
                                "[ProgressIndicator] VisionDataClient: Closing download manager's client"
                            )
                            await safely_close_client(self._download_manager.client)
                        self._download_manager.client = None

                    # Then clear the reference to the download manager
                    self._download_manager = None
                    logger.debug(
                        "[ProgressIndicator] VisionDataClient: Download manager cleaned up"
                    )
                except Exception as e:
                    error_msg = f"Error cleaning up download manager: {e}"
                    logger.error(error_msg)
                    cleanup_errors.append(error_msg)

            # B. Clean up HTTP client using safely_close_client
            if hasattr(self, "_client") and self._client:
                logger.debug(
                    "[ProgressIndicator] VisionDataClient: Cleaning up HTTP client"
                )
                try:
                    from utils.network_utils import safely_close_client

                    await safely_close_client(self._client)
                    self._client = None
                    logger.debug(
                        "[ProgressIndicator] VisionDataClient: HTTP client cleaned up"
                    )
                except Exception as e:
                    error_msg = f"Error cleaning up HTTP client: {e}"
                    logger.error(error_msg)
                    cleanup_errors.append(error_msg)

        except Exception as e:
            error_msg = f"Unexpected error during resource cleanup: {e}"
            logger.error(error_msg)
            cleanup_errors.append(error_msg)

        # STEP 5: Multiple rounds of garbage collection to fix curl references in memory
        logger.debug(
            "[ProgressIndicator] VisionDataClient: Running multiple rounds of garbage collection"
        )
        collected_total = 0
        for i in range(3):
            collected = gc.collect()
            collected_total += collected
            logger.debug(
                f"[ProgressIndicator] VisionDataClient: GC round {i+1} collected {collected} objects"
            )

        # Log active tasks for diagnostics
        active_tasks = [t for t in asyncio.all_tasks() if not t.done()]
        logger.debug(
            f"[ProgressIndicator] VisionDataClient: {len(active_tasks)} tasks active after cleanup (was {len(pre_cleanup_tasks)})"
        )

        # Final status report
        cleanup_duration = time.time() - cleanup_start_time
        logger.debug(
            f"[ProgressIndicator] VisionDataClient: Cleanup completed in {cleanup_duration:.3f}s with {len(cleanup_errors)} errors"
        )
        if cleanup_errors:
            logger.warning(
                f"VisionDataClient cleanup encountered {len(cleanup_errors)} errors"
            )
            for i, error in enumerate(cleanup_errors, 1):
                logger.debug(f"Cleanup error {i}: {error}")
        else:
            logger.debug(
                "[ProgressIndicator] VisionDataClient: Clean exit with no errors"
            )

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with correct structure.

        Returns:
            Empty DataFrame with standardized structure
        """
        return create_empty_dataframe(ChartType.KLINES)

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

        Args:
            start_time: Start time for data retrieval
            end_time: End time for data retrieval
            columns: Optional list of columns to return

        Returns:
            DataFrame with market data
        """
        # Use DataValidation to enforce timezone
        start_time = DataValidation.enforce_utc_timestamp(start_time)
        end_time = DataValidation.enforce_utc_timestamp(end_time)

        # Use unified align_time_boundaries directly from time_utils
        aligned_start, aligned_end = align_time_boundaries(
            start_time, end_time, self.interval_obj
        )

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
        self, start_time: datetime, end_time: datetime, max_days: int = 90
    ) -> TimestampedDataFrame:
        """Fetch data for the specified time range.

        Args:
            start_time: Start time
            end_time: End time
            max_days: Maximum days to fetch in single request

        Returns:
            TimestampedDataFrame with the data

        Notes:
            - Handles validation and alignment of time boundaries
            - Applies appropriate timeout mechanisms
            - Returns TimestampedDataFrame for type safety
        """
        # Comprehensive validation of time boundaries
        try:
            start_time, end_time, metadata = (
                DataValidation.validate_query_time_boundaries(
                    start_time,
                    end_time,
                    handle_future_dates="truncate",
                    interval=self.interval,
                )
            )

            # Get data availability info but don't log warnings yet
            data_availability_message = metadata.get("data_availability_message", "")
            is_data_likely_available = metadata.get("data_likely_available", True)

            # Log other warnings from validation
            for warning in metadata.get("warnings", []):
                logger.warning(warning)

        except ValueError as e:
            logger.error(f"Invalid time boundaries for fetch: {str(e)}")
            return TimestampedDataFrame(self._create_empty_dataframe())

        # Download data with timeout protection
        try:
            # Create a task for the download operation
            download_task = asyncio.create_task(
                self._download_data(start_time, end_time)
            )

            # Set timeout based on MAX_TIMEOUT
            effective_timeout = min(
                MAX_TIMEOUT, 8.0
            )  # Use slightly less than MAX_TIMEOUT

            # Start timing the operation
            start_time_op = asyncio.get_event_loop().time()

            try:
                # Wait for the download task with timeout
                df = await asyncio.wait_for(download_task, timeout=effective_timeout)

                if not df.empty:
                    # Validate data integrity
                    DataFrameValidator.validate_dataframe(df)
                    # Filter DataFrame to ensure it's within the requested time boundaries
                    df = filter_dataframe_by_time(df, start_time, end_time)
                    logger.debug(f"Successfully fetched {len(df)} records")
                    return df

                logger.warning(f"No data available for {start_time} to {end_time}")
                # If we have a data availability warning, log it now since the retrieval failed
                if data_availability_message and not is_data_likely_available:
                    # Calculate expected records more precisely based on interval
                    time_diff = (end_time - start_time).total_seconds()
                    interval_seconds = getattr(
                        self.interval_obj, "to_seconds", lambda: 60
                    )()
                    expected_records = max(1, int(time_diff / interval_seconds))
                    actual_records = 0

                    # Calculate the shortage and percentage
                    records_shortage = expected_records - actual_records
                    completion_pct = 0.0

                    # Only show warning if we're actually missing records
                    if records_shortage > 0:
                        logger.warning(
                            f"{data_availability_message} Retrieved {actual_records}/{expected_records} records "
                            f"({completion_pct:.1f}% complete, missing {records_shortage} records) "
                            f"for interval {self.interval}"
                        )
                return TimestampedDataFrame(self._create_empty_dataframe())

            except asyncio.TimeoutError:
                # Calculate elapsed time
                elapsed = asyncio.get_event_loop().time() - start_time_op

                # Log timeout to both console and dedicated log file
                logger.log_timeout(
                    operation=f"Vision API fetch for {self.symbol} {self.interval}",
                    timeout_value=effective_timeout,
                    details={
                        "symbol": self.symbol,
                        "interval": self.interval,
                        "market_type": str(self.market_type),
                        "start_time": str(start_time),
                        "end_time": str(end_time),
                        "elapsed": f"{elapsed:.2f}s",
                    },
                )

                # Cancel the task
                if not download_task.done():
                    logger.warning("Cancelling Vision API download task due to timeout")
                    download_task.cancel()

                    # Wait briefly for cancellation to complete
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(download_task, return_exceptions=True),
                            timeout=0.5,
                        )
                        logger.debug("Successfully cancelled Vision API download task")
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        logger.warning(
                            "Failed to cancel Vision API download task in time"
                        )

                # Clean up any hanging tasks
                await self._cleanup_force_timeout_tasks()

                logger.error(f"Vision API fetch timed out after {effective_timeout}s")
                # If we have a data availability warning, log it now since the retrieval failed
                if data_availability_message and not is_data_likely_available:
                    # Calculate expected records more precisely based on interval
                    time_diff = (end_time - start_time).total_seconds()
                    interval_seconds = getattr(
                        self.interval_obj, "to_seconds", lambda: 60
                    )()
                    expected_records = max(1, int(time_diff / interval_seconds))
                    actual_records = 0

                    # Calculate the shortage and percentage
                    records_shortage = expected_records - actual_records
                    completion_pct = 0.0

                    # Only show warning if we're actually missing records
                    if records_shortage > 0:
                        logger.warning(
                            f"{data_availability_message} Retrieved {actual_records}/{expected_records} records "
                            f"({completion_pct:.1f}% complete, missing {records_shortage} records) "
                            f"for interval {self.interval}"
                        )
                return TimestampedDataFrame(self._create_empty_dataframe())

        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            # Ensure cleanup of any hanging tasks
            await self._cleanup_force_timeout_tasks()
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
        # Comprehensive validation of time boundaries
        try:
            start_time, end_time, metadata = (
                DataValidation.validate_query_time_boundaries(
                    start_time,
                    end_time,
                    handle_future_dates="truncate",
                    interval=self.interval,
                )
            )

            # Get data availability info but don't log warnings yet
            data_availability_message = metadata.get("data_availability_message", "")
            is_data_likely_available = metadata.get("data_likely_available", True)

            # Log other warnings from validation
            for warning in metadata.get("warnings", []):
                logger.warning(warning)

        except ValueError as e:
            logger.error(f"Invalid time boundaries for prefetch: {str(e)}")
            return

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
                raw_data = await self._download_manager.download_date(date)

                if raw_data is None or not raw_data:
                    logger.debug(f"No data for date: {date.strftime('%Y-%m-%d')}")
                    return None

                # Process the raw data using the centralized processor
                df = TimeseriesDataProcessor.process_kline_data(raw_data, KLINE_COLUMNS)

                # Apply standard column naming
                df = standardize_column_names(df)

                # Apply any Vision API-specific post-processing here if needed

                # Ensure consistent DataFrame structure and convert to TimestampedDataFrame
                df = TimeseriesDataProcessor.standardize_dataframe(df)

                # Convert to TimestampedDataFrame for type safety
                return TimestampedDataFrame(df)

            except Exception as e:
                logger.error(
                    f"Error downloading data for {date.strftime('%Y-%m-%d')}: {e}"
                )
                return None
