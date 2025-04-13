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
from typing import Optional, Sequence, TypeVar, Generic, Union, List, Dict, Tuple
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
    KLINE_COLUMNS,
    MAXIMUM_CONCURRENT_DOWNLOADS,
)
from core.sync.vision_constraints import (
    TimestampedDataFrame,
    FileType,
    get_vision_url,
    detect_timestamp_unit,
)
from utils.gap_detector import detect_gaps, Gap

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

    def close(self):
        """Close the client and release resources."""
        if hasattr(self, "_client") and self._client:
            if hasattr(self._client, "close") and callable(self._client.close):
                self._client.close()
                self._client = None
                logger.debug("Closed Vision API HTTP client")

    @staticmethod
    def _create_empty_dataframe() -> TimestampedDataFrame:
        """Create an empty dataframe with the correct structure.

        Returns:
            Empty TimestampedDataFrame with the correct columns
        """
        # Use the standardized empty dataframe function from config
        from utils.config import create_empty_dataframe

        # Create empty dataframe and convert to TimestampedDataFrame format
        df = create_empty_dataframe()

        # Set index to open_time_us (required by TimestampedDataFrame)
        if "open_time_us" not in df.columns:
            df["open_time_us"] = pd.Series(dtype="int64")
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

    def _process_timestamp_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process timestamp columns in the dataframe, handling various formats.

        Args:
            df: DataFrame with timestamp columns to process

        Returns:
            DataFrame with processed timestamp columns
        """
        if df.empty:
            return df

        try:
            # Check timestamp format if dataframe has rows
            if len(df) > 0:
                first_ts = df.iloc[0, 0]  # First timestamp in first column

                try:
                    # Detect timestamp unit using the standardized function
                    timestamp_unit = detect_timestamp_unit(first_ts)

                    # Log the first and last timestamps for debugging
                    logger.debug(f"First timestamp: {first_ts} ({timestamp_unit})")
                    if len(df) > 1:
                        last_ts = df.iloc[-1, 0]
                        logger.debug(f"Last timestamp: {last_ts} ({timestamp_unit})")

                    # Convert timestamps to datetime using the detected unit
                    if "open_time" in df.columns:
                        df["open_time"] = pd.to_datetime(
                            df["open_time"], unit=timestamp_unit, utc=True
                        )
                    if "close_time" in df.columns:
                        df["close_time"] = pd.to_datetime(
                            df["close_time"], unit=timestamp_unit, utc=True
                        )

                    logger.debug(
                        f"Converted timestamps to datetime using {timestamp_unit} unit"
                    )

                except ValueError as e:
                    logger.warning(f"Error detecting timestamp unit: {e}")
                    # Fall back to default handling with microseconds as unit
                    if "open_time" in df.columns:
                        df["open_time"] = pd.to_datetime(
                            df["open_time"], unit="us", utc=True
                        )
                    if "close_time" in df.columns:
                        df["close_time"] = pd.to_datetime(
                            df["close_time"], unit="us", utc=True
                        )

        except Exception as e:
            logger.error(f"Error processing timestamp columns: {e}")

        return df

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
                            # If number of columns match, use the standard names
                            if len(df.columns) == len(KLINE_COLUMNS):
                                df.columns = KLINE_COLUMNS
                            else:
                                logger.warning(
                                    f"Column count mismatch: expected {len(KLINE_COLUMNS)}, got {len(df.columns)}"
                                )

                            # Store original timestamp info for later analysis if not already present
                            if "original_timestamp" not in df.columns:
                                df["original_timestamp"] = df.iloc[:, 0].astype(str)

                            # Process timestamp columns
                            df = self._process_timestamp_columns(df)

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
        """Download data from Binance Vision API for a specific time range.

        Args:
            start_time: Start time
            end_time: End time
            columns: Optional column names to use

        Returns:
            TimestampedDataFrame with downloaded data
        """
        logger.info(
            f"Downloading data for {self.symbol} {self.interval} from {start_time.isoformat()} to {end_time.isoformat()}"
        )

        # Convert start and end times to date objects for file-based lookups
        start_date = start_time.date()
        end_date = end_time.date()

        date_range = []
        current_date = start_date
        while current_date <= end_date:
            date_range.append(current_date)
            current_date = current_date + timedelta(days=1)

        logger.info(f"Need to check {len(date_range)} dates for data")

        # Use ThreadPoolExecutor to download files in parallel
        max_workers = min(MAXIMUM_CONCURRENT_DOWNLOADS, len(date_range))
        downloaded_dfs = []

        # For very short intervals like 1s, avoid too many concurrent downloads
        if self.interval == "1s" and max_workers > 10:
            max_workers = 10
            logger.info(
                f"Limited concurrent downloads to {max_workers} for 1s interval"
            )

        # Create date objects to pass to ThreadPoolExecutor
        date_objects = [
            datetime(d.year, d.month, d.day, tzinfo=timezone.utc) for d in date_range
        ]

        # Get data files
        if len(date_objects) == 0:
            logger.warning("No dates to download")
            return self._create_empty_dataframe()

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit download tasks
                future_to_date = {
                    executor.submit(self._download_file, date_obj): date_obj
                    for date_obj in date_objects
                }

                # Process results as they complete
                for future in as_completed(future_to_date):
                    date = future_to_date[future]
                    try:
                        df = future.result()
                        if df is not None and not df.empty:
                            downloaded_dfs.append(df)
                    except Exception as exc:
                        logger.error(f"Error downloading data for {date}: {exc}")
        except Exception as e:
            logger.error(f"Error in ThreadPoolExecutor: {e}")

        # If no data was downloaded, return an empty dataframe
        if not downloaded_dfs:
            logger.warning("No data downloaded from Binance Vision API")
            return self._create_empty_dataframe()

        logger.info(f"Downloaded {len(downloaded_dfs)} daily files")

        # Concatenate all dataframes
        concatenated_df = pd.concat(downloaded_dfs, ignore_index=True)

        # If the dataframe is empty, return early
        if concatenated_df.empty:
            logger.warning("No data in downloaded files")
            return self._create_empty_dataframe()

        # Ensure timestamps are in datetime format
        if "open_time" not in concatenated_df.columns:
            logger.error(
                f"Missing 'open_time' column in downloaded data. Columns: {concatenated_df.columns}"
            )
            return self._create_empty_dataframe()

        # Sort the dataframe by timestamp
        concatenated_df = concatenated_df.sort_values("open_time").reset_index(
            drop=True
        )

        # Filter by the exact time range requested
        filtered_df = filter_dataframe_by_time(
            concatenated_df, start_time, end_time, time_column="open_time"
        )

        # Use gap_detector to find gaps
        # Convert the interval string to Interval enum for proper gap detection
        try:
            interval_obj = next((i for i in Interval if i.value == self.interval), None)
            if interval_obj is None:
                interval_obj = Interval.MINUTE_1
                logger.warning(
                    f"Could not find interval {self.interval}, using MINUTE_1 as default for gap detection"
                )
        except Exception as e:
            logger.warning(f"Error parsing interval for gap detection: {e}")
            interval_obj = Interval.MINUTE_1

        logger.debug(f"Using interval {interval_obj.value} for gap detection")

        # Detect gaps in the data using the standardized gap_detector
        # Only enforce min span requirement if we're querying a longer timeframe
        time_span_days = (end_time - start_time).total_seconds() / 86400
        enforce_min_span = time_span_days >= 1.0

        gaps, gap_stats = detect_gaps(
            filtered_df,
            interval_obj,
            time_column="open_time",
            gap_threshold=0.3,  # 30% threshold
            day_boundary_threshold=1.5,  # Higher threshold for day boundaries
            enforce_min_span=enforce_min_span,  # Only enforce for longer timeframes
        )

        # Log gap statistics if any gaps were found
        if gaps:
            boundary_gaps = [gap for gap in gaps if gap.crosses_day_boundary]
            regular_gaps = [gap for gap in gaps if not gap.crosses_day_boundary]

            logger.info(f"Gap detection results: {gap_stats['total_gaps']} gaps found")
            logger.info(f"- Day boundary gaps: {len(boundary_gaps)}")
            logger.info(f"- Regular gaps: {len(regular_gaps)}")

            # Log details of each day boundary gap
            for i, gap in enumerate(boundary_gaps):
                logger.debug(
                    f"Day boundary gap {i+1}: {gap.start_time} → {gap.end_time}, "
                    f"duration: {gap.duration}, missing points: {gap.missing_points}"
                )

            # Fill gaps at day boundaries if any were detected
            if boundary_gaps:
                filled_df = self._fill_boundary_gaps_with_rest(
                    filtered_df, boundary_gaps
                )
                if filled_df is not None:
                    filtered_df = filled_df
                    logger.info(
                        f"Filled {len(boundary_gaps)} day boundary gaps with REST API data"
                    )
                else:
                    logger.warning(
                        "Failed to fill day boundary gaps with REST API data"
                    )

        # Convert to TimestampedDataFrame format
        try:
            # Create a new column for microsecond precision timestamp to use as index
            if "open_time_us" not in filtered_df.columns:
                filtered_df["open_time_us"] = (
                    filtered_df["open_time"].astype(int).mul(1000000)
                )

            # Set the index
            filtered_df = filtered_df.set_index("open_time_us")

            return TimestampedDataFrame(filtered_df)
        except Exception as e:
            logger.error(f"Error creating TimestampedDataFrame: {e}")
            return self._create_empty_dataframe()

    def _fill_boundary_gaps_with_rest(
        self, df: pd.DataFrame, boundary_gaps: List[Gap]
    ) -> Optional[pd.DataFrame]:
        """Fill day boundary gaps using REST API data.

        This method is used to fill specific gaps that occur at day boundaries
        by fetching the missing data directly from the REST API.

        Args:
            df: DataFrame with Vision API data that has gaps
            boundary_gaps: List of Gap objects representing day boundary gaps

        Returns:
            DataFrame with gaps filled, or None if filling failed
        """
        if not boundary_gaps:
            return df

        # Import RestDataClient here to avoid circular import
        from core.sync.rest_data_client import RestDataClient

        try:
            # Create a REST client with the same parameters
            rest_client = RestDataClient(
                market_type=self.market_type,
                symbol=self.symbol,
                interval=self.interval_obj,
            )

            # Create a list to hold the gap data we'll fetch
            gap_dfs = []
            gap_dfs.append(df)

            # For each gap, fetch the specific missing data
            for gap in boundary_gaps:
                # Add a small buffer around the gap to ensure we get the needed data
                # Use 50% of the interval duration as buffer
                interval_seconds = self.interval_obj.to_seconds()
                buffer_seconds = interval_seconds * 0.5

                # Fetch a bit before and after the actual gap to ensure we get the needed data
                gap_start = gap.start_time - timedelta(seconds=buffer_seconds)
                gap_end = gap.end_time + timedelta(seconds=buffer_seconds)

                logger.debug(
                    f"Fetching gap data from REST API: {gap_start} to {gap_end} "
                    f"(to fill missing data)"
                )

                # Fetch the gap data using REST API
                gap_data = rest_client.fetch(
                    self.symbol,
                    self.interval_obj,
                    start_time=gap_start,
                    end_time=gap_end,
                )

                if not gap_data.empty:
                    # Check if we got data around midnight
                    expected_midnight = (
                        gap.start_time + (gap.end_time - gap.start_time) / 2
                    )
                    midnight_time = datetime(
                        expected_midnight.year,
                        expected_midnight.month,
                        expected_midnight.day,
                        0,
                        0,
                        0,
                        tzinfo=expected_midnight.tzinfo,
                    )

                    # Look for records near midnight
                    midnight_records = gap_data[
                        (gap_data["open_time"] - midnight_time).abs()
                        < timedelta(seconds=interval_seconds)
                    ]

                    if not midnight_records.empty:
                        logger.debug(
                            f"Found {len(midnight_records)} records near midnight in REST API data"
                        )
                    else:
                        logger.debug("No midnight records found in REST API data")

                    gap_dfs.append(gap_data)
                else:
                    logger.warning(f"No data retrieved from REST API for gap")

            # If we have gap data, merge it with the original data
            if len(gap_dfs) > 1:  # More than just the original df
                # Concatenate all dataframes and remove duplicates
                merged_df = pd.concat(gap_dfs, ignore_index=True)
                merged_df = merged_df.drop_duplicates(
                    subset=["open_time"], keep="first"
                )
                merged_df = merged_df.sort_values("open_time").reset_index(drop=True)
                return merged_df

            # If we didn't add any gap data, return the original
            return df
        except Exception as e:
            logger.error(f"Error filling boundary gaps with REST API: {e}")
            return None

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
                with VisionDataClient(
                    symbol=symbol, interval=interval, market_type=market_type
                ) as client:
                    df = client.fetch(start_time, end_time)
                return symbol, df
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
                # Return empty dataframe on error
                return symbol, VisionDataClient._create_empty_dataframe()

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
                    results[symbol] = VisionDataClient._create_empty_dataframe()

        return results
