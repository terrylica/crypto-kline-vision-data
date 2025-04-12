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

from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence, TypeVar, Generic, Union, List, Dict, Any, Tuple
import os
import tempfile
import zipfile
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

import pandas as pd

from utils.logger_setup import logger
from utils.market_constraints import Interval, MarketType
from utils.time_utils import (
    filter_dataframe_by_time,
)
from utils.config import (
    standardize_column_names,
    KLINE_COLUMNS,
    MAXIMUM_CONCURRENT_DOWNLOADS,
)
from core.sync.vision_constraints import (
    TimestampedDataFrame,
    FileType,
    get_vision_url,
    detect_timestamp_unit,
    MICROSECOND_DIGITS,
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
    ):
        """Initialize Vision Data Client.

        Args:
            symbol: Trading symbol e.g. 'BTCUSDT'
            interval: Kline interval e.g. '1s', '1m'
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN) or string
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

        self.market_type_str = market_type_str

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

        # Create httpx client instead of requests Session
        self._client = httpx.Client(
            timeout=30.0,  # Increased timeout for better reliability
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, application/zip",
            },
            follow_redirects=True,  # Automatically follow redirects
        )

    def __enter__(self) -> "VisionDataClient":
        """Context manager entry."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit."""
        # Release resources
        if hasattr(self, "_client") and self._client:
            if hasattr(self._client, "close") and callable(self._client.close):
                self._client.close()

    def _create_empty_dataframe(self) -> TimestampedDataFrame:
        """Create an empty dataframe with the correct structure.

        Returns:
            Empty TimestampedDataFrame with the correct columns
        """
        # Define standard OHLCV columns directly
        columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_asset_volume",
            "taker_buy_quote_asset_volume",
        ]

        df = pd.DataFrame(columns=columns)
        df["open_time_us"] = pd.Series(dtype="int64")
        df["close_time_us"] = pd.Series(dtype="int64")

        # Set index to open_time_us and convert to TimestampedDataFrame
        df = df.set_index("open_time_us")

        return TimestampedDataFrame(df)

    def _get_interval_seconds(self, interval: str) -> int:
        """Get interval duration in seconds from interval string.

        This method handles converting string intervals directly to seconds
        without requiring the MarketInterval enum object.

        Args:
            interval: Interval string (e.g., "1s", "1m", "1h")

        Returns:
            Number of seconds in the interval
        """
        # Parse interval value and unit
        match = re.match(r"(\d+)([smhdwM])", interval)
        if not match:
            raise ValueError(f"Invalid interval format: {interval}")

        num, unit = match.groups()
        num = int(num)

        # Define multipliers for each unit
        multipliers = {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
            "w": 604800,
            "M": 2592000,  # Approximate - using 30 days
        }

        if unit not in multipliers:
            raise ValueError(f"Unknown interval unit: {unit}")

        return num * multipliers[unit]

    def _validate_timestamp_safety(self, date: datetime) -> bool:
        """Check if a given timestamp is safe to use with pandas datetime conversion.

        Args:
            date: The datetime to check

        Returns:
            True if the timestamp is safe, False if it might cause out-of-bounds errors

        Note:
            Pandas can have issues with timestamps very far in the future due to
            nanosecond conversion limitations. This check helps prevent those issues.
        """
        try:
            # Check if date is within pandas timestamp limits
            # The max timestamp supported is approximately year 2262
            max_safe_year = 2262
            if date.year > max_safe_year:
                logger.warning(
                    f"Date {date.isoformat()} exceeds pandas timestamp safe year limit ({max_safe_year})"
                )
                return False

            # Test conversion to pandas timestamp to see if it would raise an error
            _ = pd.Timestamp(date)
            return True
        except (OverflowError, ValueError, pd.errors.OutOfBoundsDatetime) as e:
            logger.warning(
                f"Date {date.isoformat()} caused timestamp validation error: {e}"
            )
            return False

    def _download_file(self, date: datetime) -> Optional[pd.DataFrame]:
        """Download and process data file for a specific date.

        Args:
            date: Date to download

        Returns:
            DataFrame with data or None if download failed
        """
        try:
            # First check if the date is safe to process with pandas
            if not self._validate_timestamp_safety(date):
                logger.error(
                    f"Skipping date {date.date()} due to potential timestamp overflow"
                )
                return None

            # Generate URL for the data - ensure we use interval string value
            interval_str = self.interval

            # Debug for interval
            logger.debug(
                f"Self.interval is {self.interval} of type {type(self.interval)}"
            )
            logger.debug(
                f"Self.interval_obj is {self.interval_obj} of type {type(self.interval_obj)}"
            )

            # Check if interval is an enum object with a value attribute
            if hasattr(self.interval_obj, "value"):
                interval_str = self.interval_obj.value
                logger.debug(f"Using interval string value: {interval_str}")
            else:
                logger.debug(
                    f"interval_obj does not have 'value' attribute, using {interval_str}"
                )

            url = get_vision_url(
                symbol=self.symbol,
                interval=interval_str,  # Use string value
                date=date,
                file_type=FileType.DATA,  # Explicitly pass proper FileType.DATA enum
                market_type=self.market_type_str,
            )

            logger.debug(f"Downloading data from {url}")

            # Download the file using httpx client
            try:
                # Make request using httpx
                response = self._client.get(url)

                # Check response status
                if response.status_code != 200:
                    # Calculate days difference between date and now
                    now = datetime.now(timezone.utc)
                    days_difference = (now.date() - date.date()).days

                    # For 404 (Not Found) status, check if it's within 2 days of now
                    if response.status_code == 404 and days_difference <= 2:
                        # We expect recent data might not be available yet, so just show a warning
                        logger.warning(
                            f"Recent data not yet available from Vision API: {date.date()} (HTTP 404)"
                        )
                    else:
                        # For data that should be available or other error codes, log as error
                        logger.error(
                            f"Failed to download data: HTTP {response.status_code}"
                        )
                    return None

                # Get response content
                content = response.content
                logger.debug(
                    f"Successfully downloaded {url} - size: {len(content)} bytes"
                )

            except httpx.RequestError as e:
                # Calculate days difference between date and now for request errors too
                now = datetime.now(timezone.utc)
                days_difference = (now.date() - date.date()).days

                if days_difference <= 2:
                    logger.warning(
                        f"Error downloading recent data for {date.date()}: {e}"
                    )
                else:
                    logger.error(f"Error downloading data for {date.date()}: {e}")
                return None

            # Create a temporary file to store the zip
            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_file:
                temp_file.write(content)
                temp_file_path = temp_file.name

            try:
                # Extract the zip file
                with zipfile.ZipFile(temp_file_path, "r") as zip_ref:
                    # Get the CSV file name (should be the only file in the zip)
                    file_list = zip_ref.namelist()
                    if not file_list:
                        logger.error("Zip file is empty")
                        return None

                    csv_file = file_list[0]
                    logger.debug(f"Found CSV file in zip: {csv_file}")

                    # Extract to a temporary directory
                    with tempfile.TemporaryDirectory() as temp_dir:
                        zip_ref.extract(csv_file, temp_dir)
                        csv_path = os.path.join(temp_dir, csv_file)

                        # Read the CSV file
                        df = pd.read_csv(csv_path)
                        logger.debug(f"Read {len(df)} rows from CSV")

                        # Process the data
                        if not df.empty:
                            # Check timestamp format - determine if microseconds or milliseconds
                            if len(df) > 0:
                                first_ts = df.iloc[
                                    0, 0
                                ]  # First timestamp in first column

                                try:
                                    # Detect timestamp unit using the standardized function
                                    timestamp_unit = detect_timestamp_unit(first_ts)

                                    # Log the first and last timestamps for debugging
                                    logger.debug(
                                        f"First timestamp: {first_ts} ({timestamp_unit})"
                                    )
                                    if len(df) > 1:
                                        last_ts = df.iloc[-1, 0]
                                        logger.debug(
                                            f"Last timestamp: {last_ts} ({timestamp_unit})"
                                        )

                                    # Standardize column names from the KLINE_COLUMNS in config

                                    # If number of columns match, use the standard names
                                    if len(df.columns) == len(KLINE_COLUMNS):
                                        df.columns = KLINE_COLUMNS
                                    else:
                                        logger.warning(
                                            f"Column count mismatch: expected {len(KLINE_COLUMNS)}, got {len(df.columns)}"
                                        )

                                    # Directly convert timestamps to datetime using the detected unit
                                    # No need for intermediate columns
                                    if "open_time" in df.columns:
                                        df["open_time"] = pd.to_datetime(
                                            df["open_time"],
                                            unit=timestamp_unit,
                                            utc=True,
                                        )
                                    if "close_time" in df.columns:
                                        df["close_time"] = pd.to_datetime(
                                            df["close_time"],
                                            unit=timestamp_unit,
                                            utc=True,
                                        )

                                    logger.debug(
                                        f"Converted timestamps to datetime using {timestamp_unit} unit"
                                    )

                                except ValueError as e:
                                    logger.warning(
                                        f"Error detecting timestamp unit: {e}"
                                    )
                                    # Fall back to default handling with standard column names

                                    # If number of columns match, use the standard names
                                    if len(df.columns) == len(KLINE_COLUMNS):
                                        df.columns = KLINE_COLUMNS

                                    # Use microsecond as default (safer)
                                    if "open_time" in df.columns:
                                        df["open_time"] = pd.to_datetime(
                                            df["open_time"], unit="us", utc=True
                                        )
                                    if "close_time" in df.columns:
                                        df["close_time"] = pd.to_datetime(
                                            df["close_time"], unit="us", utc=True
                                        )

                            # Standardize column names if not already done
                            if df.columns[0] != "open_time":
                                # Only rename if column counts match
                                if len(df.columns) == len(KLINE_COLUMNS):
                                    df.columns = KLINE_COLUMNS

                                    # Convert timestamp columns to datetime if not already done
                                    if not pd.api.types.is_datetime64_dtype(
                                        df["open_time"]
                                    ):
                                        # Use a safe default of microseconds
                                        df["open_time"] = pd.to_datetime(
                                            df["open_time"], unit="us", utc=True
                                        )
                                    if not pd.api.types.is_datetime64_dtype(
                                        df["close_time"]
                                    ):
                                        df["close_time"] = pd.to_datetime(
                                            df["close_time"], unit="us", utc=True
                                        )

                            return df
                        else:
                            logger.warning(f"Empty dataframe for {date.date()}")
                            return None
            except Exception as e:
                logger.error(
                    f"Error processing zip file {temp_file_path}: {str(e)}",
                    exc_info=True,
                )
                return None
            finally:
                # Clean up temp file
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)

        except Exception as e:
            logger.error(f"Unexpected error processing {date.date()}: {str(e)}")
            return None

    def _download_data(
        self,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> TimestampedDataFrame:
        """Download data for a specific time range.

        Args:
            start_time: Start time for data
            end_time: End time for data
            columns: Optional columns to include in the result

        Returns:
            TimestampedDataFrame with data or empty DataFrame if download failed
        """
        try:
            # Ensure start and end times are in UTC
            start_time = start_time.astimezone(timezone.utc)
            end_time = end_time.astimezone(timezone.utc)

            # Calculate date range
            start_date = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
            days_delta = (end_date - start_date).days + 1
            logger.debug(f"Requested date range spans {days_delta} days")

            # Log information about large requests but don't limit them
            if days_delta > 90:
                logger.info(
                    f"Processing a large date range of {days_delta} days with parallel downloads."
                )

            # Log the date range
            logger.debug(
                f"Fetching Vision data: {self.symbol} {self.interval} - {start_date.date()} to {end_date.date()}"
            )

            # Skip the time boundary alignment since it seems incompatible with string intervals
            try:
                logger.debug(
                    f"Skipping time boundary alignment for interval: {self.interval}"
                )
                # Just use the original times
                aligned_start, aligned_end = start_time, end_time
            except Exception as e:
                logger.error(f"Error with time handling: {e}")
                # Fall back to original times
                aligned_start, aligned_end = start_time, end_time

            # Prepare to download each day in parallel (handles both single and multi-day cases)
            logger.debug(f"Will download {days_delta} days of data")
            dates_to_download = []

            # Prepare date list up front
            current_date = start_date
            while current_date <= end_date:
                dates_to_download.append(current_date)
                current_date += timedelta(days=1)

            # Calculate the number of days to download in parallel
            max_workers = min(MAXIMUM_CONCURRENT_DOWNLOADS, days_delta)
            logger.debug(
                f"Using ThreadPoolExecutor with {max_workers} workers for parallel downloads"
            )

            # Initialize results container
            day_results: Dict[datetime, Optional[pd.DataFrame]] = {}
            day_dates = []

            # Download data in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all download tasks
                future_to_date = {
                    executor.submit(self._download_file, date): date
                    for date in dates_to_download
                }

                # Track completed downloads
                completed = 0

                # Process results as they complete
                for future in as_completed(future_to_date):
                    date = future_to_date[future]
                    completed += 1

                    try:
                        df = future.result()
                        day_results[date] = df

                        if df is not None and not df.empty:
                            # Store the date for analysis
                            try:
                                day_date = df["open_time"].iloc[0].date()
                                day_dates.append(day_date)
                            except (KeyError, IndexError, AttributeError) as e:
                                logger.warning(
                                    f"Error extracting date from dataframe: {e}"
                                )

                            # Store original timestamp info for later analysis if not already present
                            if "original_timestamp" not in df.columns:
                                df["original_timestamp"] = df["open_time"].astype(str)

                            logger.debug(
                                f"Downloaded data for {date.date()}: {len(df)} records ({completed}/{len(dates_to_download)})"
                            )
                        else:
                            # Calculate days difference for a more informative message
                            now = datetime.now(timezone.utc)
                            days_difference = (now.date() - date.date()).days

                            if days_difference <= 2:
                                logger.warning(
                                    f"No Vision API data found for {self.symbol} on {date.date()} ({completed}/{len(dates_to_download)}) - recent data will failover to REST API"
                                )
                            else:
                                logger.warning(
                                    f"No data found for {self.symbol} on {date.date()} ({completed}/{len(dates_to_download)})"
                                )
                    except Exception as e:
                        logger.error(f"Error downloading data for {date.date()}: {e}")
                        day_results[date] = None

            # Extract all valid dataframes
            dfs = [df for df in day_results.values() if df is not None and not df.empty]

            # Check if we got any data
            if not dfs:
                # Check if the date range is recent (within 2 days of now)
                now = datetime.now(timezone.utc)
                end_day_diff = (now.date() - end_date.date()).days

                if end_day_diff <= 2:
                    logger.warning(
                        f"No Vision API data found for {self.symbol} in date range {start_date.date()} to {end_date.date()} - failover to REST API will be attempted"
                    )
                else:
                    logger.warning(
                        f"No data found for {self.symbol} in date range {start_date.date()} to {end_date.date()}"
                    )
                return self._create_empty_dataframe()

            # Combine all dataframes
            logger.debug(f"Concatenating {len(dfs)} DataFrames")
            combined_df = pd.concat(dfs, ignore_index=True)

            try:
                # First, sort by open_time to ensure chronological order
                combined_df = combined_df.sort_values("open_time")

                # Calculate time differences between consecutive rows
                combined_df.loc[:, "time_diff"] = (
                    combined_df["open_time"].diff().dt.total_seconds()
                )

                # Get expected interval in seconds
                expected_interval = self._get_interval_seconds(self.interval)

                # Look for gaps significantly larger than expected interval at day boundaries
                # Focus on midnight transitions (23:00-01:00)
                combined_df.loc[:, "hour"] = combined_df["open_time"].dt.hour
                combined_df.loc[:, "minute"] = combined_df["open_time"].dt.minute
                combined_df.loc[:, "day"] = combined_df["open_time"].dt.day
                combined_df.loc[:, "month"] = combined_df["open_time"].dt.month
                combined_df.loc[:, "year"] = combined_df["open_time"].dt.year

                # Before day boundary analysis, check for 00:00:00 records at each day boundary
                # since these are critical for continuous data
                day_boundaries = {}
                current_days = sorted(combined_df["open_time"].dt.date.unique())

                for day in current_days:
                    next_day = day + timedelta(days=1)
                    # Check if next day data is available in our dataset
                    if next_day in current_days:
                        # Check for 23:59 in current day
                        has_2359 = False
                        day_data = combined_df[combined_df["open_time"].dt.date == day]
                        if not day_data.empty:
                            last_record = day_data.iloc[-1]
                            if (
                                last_record["hour"] == 23
                                and last_record["minute"] == 59
                            ):
                                has_2359 = True

                        # Check for 00:00 in next day
                        has_0000 = False
                        next_day_data = combined_df[
                            combined_df["open_time"].dt.date == next_day
                        ]
                        if not next_day_data.empty:
                            first_record = next_day_data.iloc[0]
                            if (
                                first_record["hour"] == 0
                                and first_record["minute"] == 0
                            ):
                                has_0000 = True

                        # Check for 00:01 in next day (backup check)
                        has_0001 = False
                        if not next_day_data.empty and len(next_day_data) > 1:
                            second_record = (
                                next_day_data.iloc[1]
                                if len(next_day_data) > 1
                                else None
                            )
                            if (
                                second_record is not None
                                and second_record["hour"] == 0
                                and second_record["minute"] == 1
                            ):
                                has_0001 = True

                        day_boundaries[day] = {
                            "has_2359": has_2359,
                            "has_0000": has_0000,
                            "has_0001": has_0001,
                            "continuous": has_2359 and has_0000,
                            "needs_interpolation": has_2359
                            and not has_0000
                            and has_0001,
                        }

                        logger.debug(
                            f"Day boundary {day} -> {next_day}: 23:59={has_2359}, 00:00={has_0000}, 00:01={has_0001}"
                        )

                # Properly fix the Series boolean comparison issue
                # Filter for records at midnight
                midnight_filter = (combined_df["hour"] == 0) & (
                    combined_df["minute"] == 0
                )
                # Filter for records at last minute of day
                last_minute_filter = (combined_df["hour"] == 23) & (
                    combined_df["minute"] == 59
                )

                # Get rows matching these filters
                midnight_rows = combined_df[midnight_filter]
                last_minute_rows = combined_df[last_minute_filter]

                if not midnight_rows.empty:
                    logger.debug(f"Found {len(midnight_rows)} midnight (00:00) records")
                if not last_minute_rows.empty:
                    logger.debug(
                        f"Found {len(last_minute_rows)} last minute (23:59) records"
                    )

                # List to store any midnight records that actually need to be added
                midnight_records = []

                # Detect true gaps at day boundaries where 00:00 is missing but should exist
                for i in range(1, len(combined_df)):
                    prev_row = combined_df.iloc[i - 1]
                    curr_row = combined_df.iloc[i]

                    prev_time = prev_row["open_time"]
                    curr_time = curr_row["open_time"]

                    # Only look at day transitions (where days differ)
                    if prev_time.date() != curr_time.date():
                        # Check if this is a 23:59 -> 00:01 transition (skipping 00:00)
                        if (
                            prev_row["hour"] == 23
                            and prev_row["minute"] == 59
                            and curr_row["hour"] == 0
                            and curr_row["minute"] == 1
                        ):

                            # Check our day boundary analysis
                            day = prev_time.date()
                            if (
                                day in day_boundaries
                                and day_boundaries[day]["needs_interpolation"]
                            ):
                                # Calculate expected midnight datetime
                                midnight = datetime.combine(
                                    curr_time.date(),
                                    datetime.min.time(),
                                    tzinfo=timezone.utc,
                                )

                                logger.warning(
                                    f"True midnight gap detected: {prev_time} → {curr_time}, "
                                    f"missing {midnight}"
                                )

                                # Now we need to create a midnight record through interpolation
                                interpolated_row = prev_row.copy()
                                interpolated_row["open_time"] = midnight

                                # Calculate interpolation weight (what % of the way from prev to curr time)
                                time_diff_seconds = (
                                    curr_time - prev_time
                                ).total_seconds()
                                prev_to_midnight_seconds = (
                                    midnight - prev_time
                                ).total_seconds()
                                weight = prev_to_midnight_seconds / time_diff_seconds

                                # Linear interpolation for numeric columns
                                for col in ["open", "high", "low", "close", "volume"]:
                                    if col in prev_row and col in curr_row:
                                        try:
                                            interpolated_row[col] = prev_row[
                                                col
                                            ] + weight * (curr_row[col] - prev_row[col])
                                        except Exception as e:
                                            logger.warning(
                                                f"Error interpolating column {col}: {e}"
                                            )

                                # Add boundary flag for reference
                                interpolated_row["boundary_record"] = (
                                    "interpolated_midnight"
                                )
                                midnight_records.append(interpolated_row)
                            else:
                                logger.debug(
                                    f"Day boundary transition: {prev_time} → {curr_time} "
                                    f"(not missing 00:00, no interpolation needed)"
                                )
                    # For general gap detection (not at day boundaries)
                    elif (
                        curr_row["time_diff"] is not None
                        and curr_row["time_diff"] > expected_interval * 1.5
                    ):
                        logger.warning(
                            f"Non-boundary gap detected: {prev_time} → {curr_time} "
                            f"({curr_row['time_diff']:.1f}s, expected {expected_interval:.1f}s)"
                        )

                # Add any interpolated midnight records
                if midnight_records:
                    logger.debug(
                        f"Adding {len(midnight_records)} interpolated midnight records"
                    )
                    combined_df = pd.concat(
                        [combined_df, pd.DataFrame(midnight_records)], ignore_index=True
                    )

                    # Sort and reset index
                    combined_df = combined_df.sort_values("open_time").reset_index(
                        drop=True
                    )

            except Exception as e:
                logger.warning(f"Error during day boundary analysis: {e}")

            # Filter by time range
            filtered_df = filter_dataframe_by_time(
                combined_df, aligned_start, aligned_end, "open_time"
            )

            # Report data coverage
            if not filtered_df.empty:
                actual_start = filtered_df["open_time"].iloc[0]
                actual_end = filtered_df["open_time"].iloc[-1]
                record_count = len(filtered_df)
                # Calculate expected count based on interval
                interval_seconds = self._get_interval_seconds(self.interval)
                expected_count = (
                    int((actual_end - actual_start).total_seconds() / interval_seconds)
                    + 1
                )
                coverage_percent = (record_count / expected_count) * 100
                logger.debug(
                    f"Data coverage: {record_count} records / {expected_count} expected ({coverage_percent:.1f}%)"
                )
                logger.debug(f"Time range: {actual_start} to {actual_end}")
            else:
                logger.warning(
                    f"No data found for {self.symbol} in filtered range {aligned_start} to {aligned_end}"
                )

            # Ensure the DataFrame is sorted by time
            filtered_df = filtered_df.sort_values("open_time").reset_index(drop=True)

            logger.debug(
                f"Downloaded {len(filtered_df)} records for {self.symbol} from {aligned_start} to {aligned_end}"
            )

            # Drop temporary columns used for analysis
            cols_to_drop = [
                "time_diff",
                "hour",
                "minute",
                "day",
                "month",
                "year",
                "boundary_record",
                "original_timestamp",
            ]
            for col in cols_to_drop:
                if col in filtered_df.columns:
                    filtered_df = filtered_df.drop(columns=[col])

            # Standardize column names
            filtered_df = standardize_column_names(filtered_df)

            # Select specific columns if requested
            if columns is not None:
                all_cols = set(filtered_df.columns)
                missing_cols = set(columns) - all_cols
                if missing_cols:
                    logger.warning(
                        f"Requested columns not found: {missing_cols}. Available: {all_cols}"
                    )
                filtered_df = filtered_df[[col for col in columns if col in all_cols]]

            return filtered_df

        except Exception as e:
            logger.error(f"Error downloading data: {e}")
            return self._create_empty_dataframe()

    def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> TimestampedDataFrame:
        """Fetch data for a specific time range.

        Args:
            start_time: Start time for data
            end_time: End time for data

        Returns:
            TimestampedDataFrame with data
        """
        try:
            # Enforce consistent timezone for time boundaries
            start_time = start_time.astimezone(timezone.utc)
            end_time = end_time.astimezone(timezone.utc)

            # Calculate date range
            delta_days = (end_time - start_time).days + 1
            logger.debug(
                f"Requested date range spans {delta_days} days from {start_time} to {end_time}"
            )

            # Log if it's a large request
            if delta_days > 90:
                logger.info(
                    f"Processing a large date range of {delta_days} days with parallel downloads."
                )

            # Download data
            try:
                logger.debug(f"Calling _download_data from {start_time} to {end_time}")
                return self._download_data(start_time, end_time)
            except Exception as e:
                logger.error(f"Error in _download_data: {e}")
                import traceback

                logger.error(f"Traceback: {traceback.format_exc()}")
                raise
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return self._create_empty_dataframe()

    def close(self) -> None:
        """Close the client and release resources."""
        if hasattr(self, "_client") and self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.warning(f"Error closing httpx client: {e}")

    @staticmethod
    def fetch_multiple(
        symbols: List[str],
        start_time: datetime,
        end_time: datetime,
        interval: str = "1m",
        market_type: Union[str, MarketType] = MarketType.SPOT,
        max_workers: Optional[int] = None,
    ) -> Dict[str, TimestampedDataFrame]:
        """Fetch data for multiple symbols in parallel.

        Args:
            symbols: List of trading symbols to fetch data for
            start_time: Start time for data
            end_time: End time for data
            interval: Kline interval e.g. '1s', '1m'
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN) or string
            max_workers: Maximum number of parallel workers (defaults to min(MAXIMUM_CONCURRENT_DOWNLOADS, len(symbols)))

        Returns:
            Dictionary mapping symbols to their respective DataFrames
        """
        if not symbols:
            logger.warning("No symbols provided to fetch_multiple")
            return {}

        # Calculate effective number of workers
        if max_workers is None:
            max_workers = min(MAXIMUM_CONCURRENT_DOWNLOADS, len(symbols))
        else:
            max_workers = min(max_workers, MAXIMUM_CONCURRENT_DOWNLOADS, len(symbols))

        # Calculate date range for logging
        delta_days = (end_time - start_time).days + 1

        # Log large requests but don't limit them
        if delta_days > 90:
            logger.info(
                f"Processing a large date range of {delta_days} days for {len(symbols)} symbols. This is supported with parallel downloads."
            )

        logger.info(
            f"Fetching data for {len(symbols)} symbols using {max_workers} parallel workers"
        )

        results: Dict[str, TimestampedDataFrame] = {}

        # Define worker function to download data for a single symbol
        def download_worker(symbol: str) -> Tuple[str, TimestampedDataFrame]:
            try:
                client = VisionDataClient(
                    symbol=symbol, interval=interval, market_type=market_type
                )
                df = client.fetch(start_time, end_time)
                client.close()
                return symbol, df
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
                # Return empty dataframe on error
                return symbol, VisionDataClient._create_empty_dataframe(None)

        # Use ThreadPoolExecutor to parallelize downloads across symbols
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(download_worker, symbol): symbol for symbol in symbols
            }

            # Process results as they complete
            for i, future in enumerate(as_completed(future_to_symbol)):
                symbol = future_to_symbol[future]
                try:
                    symbol_result, df = future.result()
                    results[symbol_result] = df
                    logger.info(
                        f"Completed download for {symbol} ({i+1}/{len(symbols)}): {len(df)} records"
                    )
                except Exception as e:
                    logger.error(f"Error processing result for {symbol}: {e}")
                    # Create empty dataframe for failed symbols
                    results[symbol] = VisionDataClient._create_empty_dataframe(None)

        return results
