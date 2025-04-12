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
from typing import Optional, Sequence, TypeVar, Generic, Union
import os
import tempfile
import zipfile
import requests

import pandas as pd

from utils.logger_setup import logger
from utils.market_constraints import Interval, MarketType
from utils.time_utils import (
    align_time_boundaries,
    filter_dataframe_by_time,
    get_interval_seconds,
)
from utils.config import (
    create_empty_dataframe,
    standardize_column_names,
)
from core.sync.vision_constraints import (
    TimestampedDataFrame,
    FileType,
    get_vision_url,
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

        # Create a proper synchronous HTTP client
        self._client = requests.Session()
        self._client.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, application/zip",
            }
        )
        self._client.timeout = 10.0

    def __enter__(self) -> "VisionDataClient":
        """Context manager entry."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit."""
        # Release resources
        if hasattr(self, "_client") and self._client:
            if hasattr(self._client, "close") and callable(self._client.close):
                self._client.close()

    def _create_empty_dataframe(self) -> pd.DataFrame:
        """Create empty DataFrame with proper structure."""
        return create_empty_dataframe()

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

            # Generate URL for the data
            url = get_vision_url(
                symbol=self.symbol,
                interval=self.interval,
                date=date,
                file_type=FileType.DATA,
                market_type=self.market_type_str,
            )

            logger.debug(f"Downloading data from {url}")

            # Download the file - ensure synchronous request
            try:
                # Make a proper synchronous HTTP request
                response = self._client.get(url, timeout=3.0)

                # Check response status
                if response.status_code != 200:
                    logger.error(
                        f"Failed to download data: HTTP {response.status_code}"
                    )
                    return None

                # Get response content
                content = response.content

            except Exception as e:
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
                    csv_file = zip_ref.namelist()[0]

                    # Extract to a temporary directory
                    with tempfile.TemporaryDirectory() as temp_dir:
                        zip_ref.extract(csv_file, temp_dir)
                        csv_path = os.path.join(temp_dir, csv_file)

                        # Read the CSV file
                        df = pd.read_csv(csv_path)

                        # Process the data
                        if not df.empty:
                            # Convert the data to proper types
                            df.columns = [
                                "open_time",
                                "open",
                                "high",
                                "low",
                                "close",
                                "volume",
                                "close_time",
                                "quote_asset_volume",
                                "number_of_trades",
                                "taker_buy_base_asset_volume",
                                "taker_buy_quote_asset_volume",
                                "ignore",
                            ]

                            # Detect timestamp format - check if we're dealing with microseconds or milliseconds
                            # Binance Vision API sometimes returns timestamps in milliseconds (13 digits)
                            # and sometimes in microseconds (16 digits), especially across different time periods.
                            # This logic automatically detects the format to avoid invalid timestamp errors.
                            timestamp_unit = "ms"  # default to milliseconds
                            if (
                                df["open_time"].iloc[0] > 1e15
                            ):  # If value is > 1 quadrillion, it's likely microseconds
                                timestamp_unit = "us"
                                logger.debug(
                                    f"Using microsecond timestamp unit for {self.symbol}"
                                )

                            # Store original timestamps before conversion for debugging
                            df["original_open_time"] = df["open_time"].copy()
                            df["original_close_time"] = df["close_time"].copy()

                            # Store format info for debugging
                            df["timestamp_format"] = timestamp_unit

                            # Check for boundary timestamps (23:59 and 00:00/00:01)
                            # These are critical for day transitions
                            boundary_times = []

                            # For millisecond timestamps
                            if timestamp_unit == "ms":
                                boundary_times = [
                                    # 23:59:00 timestamp in milliseconds
                                    int(
                                        datetime(
                                            date.year, date.month, date.day, 23, 59, 0
                                        ).timestamp()
                                        * 1000
                                    ),
                                    # 00:00:00 timestamp in milliseconds for next day
                                    int(
                                        (date + timedelta(days=1))
                                        .replace(hour=0, minute=0, second=0)
                                        .timestamp()
                                        * 1000
                                    ),
                                    # 00:01:00 timestamp in milliseconds for next day
                                    int(
                                        (date + timedelta(days=1))
                                        .replace(hour=0, minute=1, second=0)
                                        .timestamp()
                                        * 1000
                                    ),
                                ]
                            else:  # microsecond timestamps
                                boundary_times = [
                                    # 23:59:00 timestamp in microseconds
                                    int(
                                        datetime(
                                            date.year, date.month, date.day, 23, 59, 0
                                        ).timestamp()
                                        * 1000000
                                    ),
                                    # 00:00:00 timestamp in microseconds for next day
                                    int(
                                        (date + timedelta(days=1))
                                        .replace(hour=0, minute=0, second=0)
                                        .timestamp()
                                        * 1000000
                                    ),
                                    # 00:01:00 timestamp in microseconds for next day
                                    int(
                                        (date + timedelta(days=1))
                                        .replace(hour=0, minute=1, second=0)
                                        .timestamp()
                                        * 1000000
                                    ),
                                ]

                            # Check for boundary timestamps in the data
                            has_23_59 = (df["open_time"] == boundary_times[0]).any()
                            has_00_00 = (df["open_time"] == boundary_times[1]).any()
                            has_00_01 = (df["open_time"] == boundary_times[2]).any()

                            logger.debug(
                                f"File for {date.date()} has 23:59 record: {has_23_59}"
                            )
                            logger.debug(
                                f"File for {date.date()} has 00:00 record: {has_00_00}"
                            )
                            logger.debug(
                                f"File for {date.date()} has 00:01 record: {has_00_01}"
                            )

                            # Now convert timestamps to datetime
                            # Safely convert open_time
                            df["open_time"] = pd.to_datetime(
                                df["open_time"],
                                unit=timestamp_unit,
                                utc=True,
                                errors="coerce",
                            )

                            # Safely convert close_time
                            df["close_time"] = pd.to_datetime(
                                df["close_time"],
                                unit=timestamp_unit,
                                utc=True,
                                errors="coerce",
                            )

                            # Drop any rows with NaT times from conversion errors
                            invalid_rows = df[
                                df["open_time"].isna() | df["close_time"].isna()
                            ]
                            if not invalid_rows.empty:
                                # Add debug print to show the problematic rows
                                logger.debug(
                                    f"First invalid row data before dropping: {invalid_rows.iloc[0] if len(invalid_rows) > 0 else 'None'}"
                                )
                                logger.warning(
                                    f"Dropped {len(invalid_rows)} rows with invalid timestamps"
                                )
                                df = df.dropna(subset=["open_time", "close_time"])

                            # Special handling for boundary data (23:59 and 00:01)
                            # This is critical to prevent gaps during day transitions
                            if date.month == 12 and date.day == 31:
                                # Handle year boundary (2024-12-31)
                                # Keep the 23:59 record with special marking
                                midnight_rows = df[df["open_time"].dt.hour == 23]
                                if not midnight_rows.empty:
                                    logger.debug(
                                        f"Year boundary file has {len(midnight_rows)} records at hour 23"
                                    )
                                    # Mark these records for special handling during merge
                                    df.loc[midnight_rows.index, "boundary_record"] = (
                                        "year_end"
                                    )

                            # For first day of month, check if we have 00:00 or 00:01 records
                            if date.day == 1:
                                # Check for first hour records
                                hour0_rows = df[df["open_time"].dt.hour == 0]
                                if not hour0_rows.empty:
                                    logger.debug(
                                        f"Month start file has {len(hour0_rows)} records at hour 0"
                                    )
                                    # Mark these records for special handling during merge
                                    df.loc[hour0_rows.index, "boundary_record"] = (
                                        "month_start"
                                    )

                            # Drop the 'ignore' column
                            df = df.drop(columns=["ignore"])

                            return df

                        return None
            finally:
                # Clean up the temporary zip file
                try:
                    os.unlink(temp_file_path)
                except Exception as e:
                    logger.warning(
                        f"Failed to delete temporary file {temp_file_path}: {e}"
                    )

        except Exception as e:
            logger.error(f"Error downloading data for {date.date()}: {e}")
            return None

    def _download_data(
        self,
        start_time: datetime,
        end_time: datetime,
        columns: Optional[Sequence[str]] = None,
    ) -> TimestampedDataFrame:
        """Download data for a specific date range.

        Args:
            start_time: Start time
            end_time: End time
            columns: Columns to include (defaults to all)

        Returns:
            DataFrame with data
        """
        logger.debug(
            f"Fetching Vision data: {self.symbol} {self.interval} - {start_time.date()} to {end_time.date()}"
        )

        # Align time boundaries
        aligned_start, aligned_end = align_time_boundaries(
            start_time, end_time, self.interval_obj
        )

        # Generate list of dates
        dates = []
        current_date = aligned_start.replace(hour=0, minute=0, second=0, microsecond=0)
        while current_date <= aligned_end:
            dates.append(current_date)
            current_date += timedelta(days=1)

        # Check if we have any dates to download
        if not dates:
            logger.warning(
                f"No valid dates to download for range: {aligned_start} to {aligned_end}"
            )
            return self._create_empty_dataframe()

        logger.debug(f"Will download {len(dates)} days of data")

        # Process dates sequentially
        results = []
        for date in dates:
            try:
                df = self._download_file(date)
                if df is not None and not df.empty:
                    results.append(df)
            except Exception as e:
                logger.error(f"Error downloading data for {date.date()}: {e}")

        # If no data was found, return empty DataFrame
        if not results:
            logger.warning(f"No data found for {self.symbol} in date range")
            return self._create_empty_dataframe()

        # Concatenate all DataFrames
        logger.debug(f"Concatenating {len(results)} DataFrames")
        # Before concatenation, ensure consistent timestamp formats across all daily dataframes
        normalized_results = []

        # First, check if we have results spanning the 2024-2025 boundary or any day boundaries
        has_day_boundary = False
        has_format_transition = False
        day_dates = []
        timestamp_formats = []

        for i, df in enumerate(results):
            if not df.empty:
                # Store the date and detected format for analysis
                day_date = df["open_time"].iloc[0].date()
                day_dates.append(day_date)

                # Determine timestamp format based on examining raw values
                sample_ts = None
                if "original_timestamp" in df.columns:
                    sample_ts = df["original_timestamp"].iloc[0]
                else:
                    # Store original timestamp info for later analysis if not already present
                    # This helps in debugging format transition issues
                    df["original_timestamp"] = df["open_time"].astype(str)

                if i > 0 and day_dates[i] != day_dates[i - 1] + timedelta(days=1):
                    has_day_boundary = True
                    logger.debug(
                        f"Day boundary detected between {day_dates[i-1]} and {day_dates[i]}"
                    )

                # Store the dataframe with metadata
                normalized_results.append(df)

        # Proceed with concatenation
        combined_df = pd.concat(normalized_results, ignore_index=True)

        # Special handling for day transitions - fill in any missing data points
        # This specifically addresses the issue where 23:59 and 00:01 records get dropped
        # during day transitions, especially at the year boundary (2024-2025)
        try:
            # First, sort by open_time to ensure chronological order
            combined_df = combined_df.sort_values("open_time")

            # Calculate time differences between consecutive rows
            combined_df["time_diff"] = (
                combined_df["open_time"].diff().dt.total_seconds()
            )

            # Expected interval for this data (e.g., 60 seconds for 1m)
            expected_interval = get_interval_seconds(self.interval_obj)

            # Look for gaps significantly larger than expected interval at day boundaries
            # Focus on midnight transitions (23:00-01:00)
            combined_df["hour"] = combined_df["open_time"].dt.hour
            combined_df["minute"] = combined_df["open_time"].dt.minute
            combined_df["day"] = combined_df["open_time"].dt.day
            combined_df["month"] = combined_df["open_time"].dt.month
            combined_df["year"] = combined_df["open_time"].dt.year

            # List to store any midnight records that actually need to be added
            missing_midnight_records = []

            # Get potential day transition gaps
            # These would be where consecutive hours are 23 and 0/1, with a gap larger than expected
            for i in range(1, len(combined_df)):
                curr_row = combined_df.iloc[i]
                prev_row = combined_df.iloc[i - 1]

                # Check for day boundary transition (23:XX -> 00:XX/01:XX)
                if prev_row["hour"] == 23 and curr_row["hour"] in [0, 1]:
                    # Calculate the expected midnight timestamp
                    prev_date = prev_row["open_time"].date()
                    curr_date = curr_row["open_time"].date()

                    # Only proceed if dates are consecutive
                    if (curr_date - prev_date).days == 1:
                        midnight = datetime.combine(
                            curr_date, datetime.min.time(), tzinfo=timezone.utc
                        )

                        # Check if a 00:00:00 record exists by looking for exact midnight timestamps
                        # or timestamps very close to midnight (within 1 second)
                        midnight_records = combined_df[
                            (combined_df["hour"] == 0)
                            & (combined_df["minute"] == 0)
                            & (combined_df["day"] == curr_date.day)
                            & (combined_df["month"] == curr_date.month)
                            & (combined_df["year"] == curr_date.year)
                        ]

                        # If we found a midnight record, there's no gap
                        if not midnight_records.empty:
                            logger.debug(
                                f"Day boundary at index {i} has midnight record: "
                                f"{prev_row['open_time']} → {midnight} → {curr_row['open_time']}"
                            )
                            continue

                        # Check for a true gap (consecutive records with time diff > expected)
                        if curr_row["time_diff"] > expected_interval * 1.5:
                            logger.warning(
                                f"True day boundary gap detected at index {i}: "
                                f"{prev_row['open_time']} → {curr_row['open_time']} "
                                f"({curr_row['time_diff']:.1f}s, expected {expected_interval:.1f}s)"
                            )

                            logger.debug(
                                f"Gap between dates: {prev_date} and {curr_date}"
                            )

                            # Now we need to create a midnight record through interpolation
                            interpolated_row = prev_row.copy()
                            interpolated_row["open_time"] = midnight

                            # Calculate interpolation weight (what % of the way from prev to curr time)
                            time_diff_seconds = (
                                curr_row["open_time"] - prev_row["open_time"]
                            ).total_seconds()
                            prev_to_midnight_seconds = (
                                midnight - prev_row["open_time"]
                            ).total_seconds()
                            weight = prev_to_midnight_seconds / time_diff_seconds

                            # Interpolate numeric values
                            for col in ["open", "high", "low", "close", "volume"]:
                                if col in prev_row and col in curr_row:
                                    interpolated_row[col] = prev_row[col] + weight * (
                                        curr_row[col] - prev_row[col]
                                    )

                            # Add the interpolated midnight record to our list of records to add
                            missing_midnight_records.append(interpolated_row)

                            logger.info(
                                f"Created interpolated record for midnight: {midnight}"
                            )
                        else:
                            logger.debug(
                                f"Day boundary transition without gap at index {i}: "
                                f"{prev_row['open_time']} → {curr_row['open_time']} "
                                f"({curr_row['time_diff']:.1f}s)"
                            )

            # Add any interpolated midnight records if needed
            if missing_midnight_records:
                # Convert list of rows to DataFrame
                missing_df = pd.DataFrame(missing_midnight_records)
                logger.info(
                    f"Adding {len(missing_df)} interpolated midnight records to fill gaps"
                )

                # Combine with the original DataFrame
                combined_df = pd.concat([combined_df, missing_df], ignore_index=True)

                # Sort and reset index
                combined_df = combined_df.sort_values("open_time").reset_index(
                    drop=True
                )

            # Clean up any diagnostic columns to avoid interfering with further processing
            for col in ["time_diff", "hour", "minute", "day", "month", "year"]:
                if col in combined_df.columns:
                    combined_df = combined_df.drop(columns=[col])

        except Exception as e:
            logger.error(f"Error analyzing day transitions: {e}")
            # Continue with processing even if analysis fails

        # Standardize column names
        combined_df = standardize_column_names(combined_df)

        # Filter by time range
        filtered_df = filter_dataframe_by_time(
            combined_df, aligned_start, aligned_end, "open_time"
        )

        # Verify the result
        if filtered_df.empty:
            logger.warning(f"No data found after filtering to time range")
            return self._create_empty_dataframe()

        # Ensure the DataFrame is sorted by time
        filtered_df = filtered_df.sort_values("open_time").reset_index(drop=True)

        logger.debug(
            f"Downloaded {len(filtered_df)} records for {self.symbol} from {aligned_start} to {aligned_end}"
        )
        return filtered_df

    def fetch(
        self, start_time: datetime, end_time: datetime, max_days: int = 90
    ) -> TimestampedDataFrame:
        """Fetch data for the specified time range.

        Args:
            start_time: Start time
            end_time: End time
            max_days: Maximum number of days to fetch (to prevent very large requests)

        Returns:
            DataFrame with data
        """
        # Validate time range
        if start_time >= end_time:
            logger.warning(
                f"Start time {start_time} must be before end time {end_time}, returning empty DataFrame"
            )
            return self._create_empty_dataframe()

        # Calculate the date range
        date_diff = (end_time.date() - start_time.date()).days + 1
        logger.debug(f"Requested date range spans {date_diff} days")

        # Check if the date range exceeds the maximum
        if date_diff > max_days:
            logger.warning(
                f"Date range exceeds {max_days} days limit, truncating to {max_days} days"
            )
            new_end_time = start_time + timedelta(days=max_days)
            # Ensure we don't exceed the original end_time
            if new_end_time > end_time:
                new_end_time = end_time
            end_time = new_end_time
            logger.debug(f"Adjusted end time to {end_time}")

        return self._download_data(start_time, end_time)

    def close(self) -> None:
        """Close the client and release resources."""
        if hasattr(self, "_client") and self._client:
            try:
                # Close the session
                self._client.close()
                logger.debug("Closed Vision API HTTP client")
            except Exception as e:
                logger.warning(f"Error closing Vision API client: {e}")
            finally:
                # Ensure the client reference is cleared
                self._client = None
