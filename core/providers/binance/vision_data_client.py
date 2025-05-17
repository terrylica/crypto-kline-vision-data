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

import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generic, TypeVar

import httpx
import pandas as pd
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_incrementing,
)

from core.providers.binance.data_client_interface import DataClientInterface
from core.providers.binance.vision_path_mapper import (
    FSSpecVisionHandler,
)
from utils.config import (
    CONCURRENT_DOWNLOADS_LIMIT_1S,
    HTTP_NOT_FOUND,
    HTTP_OK,
    KLINE_COLUMNS,
    LARGE_REQUEST_DAYS,
    MAXIMUM_CONCURRENT_DOWNLOADS,
    MIN_CHECKSUM_SIZE,
    VISION_DATA_DELAY_HOURS,
    FileType,
)
from utils.dataframe_types import TimestampedDataFrame
from utils.dataframe_utils import ensure_open_time_as_column
from utils.for_core.vision_constraints import (
    get_vision_url,
    is_date_too_fresh_for_vision,
)
from utils.for_core.vision_file_utils import (
    fill_boundary_gaps_with_rest,
    find_day_boundary_gaps,
)
from utils.for_core.vision_timestamp import parse_interval, process_timestamp_columns
from utils.gap_detector import detect_gaps
from utils.logger_setup import logger
from utils.market_constraints import (
    ChartType,
    DataProvider,
    Interval,
    MarketType,
    get_market_capabilities,
)
from utils.time_utils import filter_dataframe_by_time
from utils.validation import DataFrameValidator

# Define the type variable for VisionDataClient
T = TypeVar("T")


class VisionDataClient(DataClientInterface, Generic[T]):
    """Vision Data Client for direct access to Binance historical data.

    Important note on timestamp semantics:
    - open_time represents the BEGINNING of the candle period (standard in financial data)
    - close_time represents the END of the candle period
    - This implementation preserves this semantic meaning across all interval types
    """

    def __init__(
        self,
        symbol: str,
        interval: str = "1s",
        market_type: str | MarketType = MarketType.SPOT,
        chart_type: ChartType = ChartType.KLINES,
        base_url: str = "https://data.binance.vision",
        cache_dir: str | Path | None = None,
    ):
        """Initialize Vision Data Client.

        Args:
            symbol: Trading pair to retrieve data for
            interval: Kline interval e.g. '1s', '1m'
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN) or string
            chart_type: Chart type (KLINES, FUNDING_RATE)
            base_url: Base URL for Binance Vision API
            cache_dir: Directory to store cached files
        """
        self._symbol = symbol.upper()
        self._interval_str = interval
        self.market_type = market_type
        self._chart_type = chart_type  # Store chart_type as instance variable
        self.base_url = base_url

        # Convert MarketType enum to string if needed
        if isinstance(market_type, MarketType):
            self._market_type_str = market_type.name
            self._market_type_obj = market_type
        else:
            self._market_type_str = market_type
            try:
                self._market_type_obj = MarketType[market_type.upper()]
            except (KeyError, AttributeError):
                try:
                    self._market_type_obj = MarketType.from_string(market_type)
                except ValueError:
                    logger.error(f"Invalid market type: {market_type}")
                    raise ValueError(f"Invalid market type: {market_type}")

        # Parse interval string to Interval object using imported function
        self.interval_obj = parse_interval(interval)

        # Set up cache directory
        if cache_dir is not None:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path("./cache")

        # Initialize FSSpecVisionHandler for path handling
        self.fs_handler = FSSpecVisionHandler(base_cache_dir=self.cache_dir)

        # Create httpx client instead of requests Session
        self._client = httpx.Client(
            timeout=30.0,  # Increased timeout for better reliability
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, application/zip",
            },
            follow_redirects=True,  # Automatically follow redirects
        )
        logger.debug(f"Initialized Vision client for {self._symbol} {self._interval_str} ({self._market_type_str})")

    def __enter__(self) -> "VisionDataClient":
        """Context manager entry."""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit."""
        # Release resources
        if hasattr(self, "_client") and self._client:
            if hasattr(self._client, "close") and callable(self._client.close):
                self._client.close()

    def close(self) -> None:
        """Close the client and release resources."""
        if hasattr(self, "_client") and self._client:
            if hasattr(self._client, "close") and callable(self._client.close):
                self._client.close()
                self._client = None
                logger.debug("Closed Vision API HTTP client")

    @property
    def provider(self) -> DataProvider:
        """Get the data provider for this client."""
        return DataProvider.BINANCE

    @property
    def chart_type(self) -> ChartType:
        """Get chart type."""
        return self._chart_type

    @property
    def symbol(self) -> str:
        """Get symbol."""
        return self._symbol

    @property
    def interval(self) -> str:
        """Get interval string."""
        return self._interval_str

    @property
    def market_type_str(self) -> str:
        """Get market type string."""
        return self._market_type_str

    @property
    def market_type_obj(self) -> MarketType:
        """Get market type object."""
        return self._market_type_obj

    def create_empty_dataframe(self) -> pd.DataFrame:
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

    def validate_data(self, df: pd.DataFrame) -> tuple[bool, str | None]:
        """Validate that a DataFrame contains valid market data."""
        validator = DataFrameValidator(df)
        return validator.validate_klines_data()

    def _should_skip_retry_for_fresh_date(self, date: datetime) -> bool:
        """Check if retries should be skipped for a date that might be too fresh.

        Args:
            date: The date to check

        Returns:
            bool: True if retries should be skipped, False otherwise
        """
        # Skip retries for dates within the freshness window
        if is_date_too_fresh_for_vision(date):
            logger.warning(
                f"Skipping retry for {date.date()} as it's within the Vision data delay window "
                f"({VISION_DATA_DELAY_HOURS} hours). This failure is expected for fresh data."
            )
            return True
        return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_incrementing(start=1, increment=1, max=3),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException)),
        reraise=True,
        before_sleep=lambda retry_state: logger.warning(
            f"Retry attempt {retry_state.attempt_number}/3 for {retry_state.args[0] if retry_state.args else 'unknown date'} "
            f"after error: {retry_state.outcome.exception()} - "
            f"waiting {retry_state.attempt_number} seconds"
        ),
    )
    def _download_file(self, date: datetime) -> tuple[pd.DataFrame | None, str | None]:
        """Download a data file for a specific date.

        Args:
            date: Date to download data for

        Returns:
            Tuple of (DataFrame, warning message). DataFrame is None if download failed.
        """
        logger.debug(f"Downloading data for {date.date()} for {self._symbol} {self._interval_str}")

        temp_file_path = None
        temp_checksum_path = None
        checksum_failed = False

        try:
            # Create the file URL
            # Get proper interval based on market capabilities
            market_type_enum = MarketType.from_string(self.market_type_str)
            market_caps = get_market_capabilities(market_type_enum)

            # Convert string interval to enum for validation
            try:
                interval_enum = parse_interval(self._interval_str)

                # Validate if interval is supported by market type
                if interval_enum not in market_caps.supported_intervals:
                    supported_intervals = [i.value for i in market_caps.supported_intervals]
                    error_msg = (
                        f"Interval {self._interval_str} not supported by {market_type_enum.name} market. "
                        f"Supported intervals: {supported_intervals}"
                    )
                    logger.error(error_msg)

                    # Create a detailed error message with suggestions
                    min_interval = min(market_caps.supported_intervals, key=lambda x: x.to_seconds())
                    suggestion = (
                        f"Consider using {min_interval.value} (minimum supported interval) or another supported interval from the list."
                    )

                    from utils.for_core.vision_exceptions import (
                        UnsupportedIntervalError,
                    )

                    raise UnsupportedIntervalError(f"{error_msg} {suggestion}")

                # Use the validated interval for URL construction
                base_interval = self._interval_str
            except ValueError as e:
                logger.error(f"Invalid interval format: {self._interval_str}. Error: {e}")
                return None, f"Invalid interval format: {self._interval_str}"

            url = get_vision_url(
                symbol=self._symbol,
                interval=base_interval,
                date=date,
                file_type=FileType.DATA,
                market_type=self.market_type_str,
            )

            # Create the checksum URL
            checksum_url = get_vision_url(
                symbol=self._symbol,
                interval=base_interval,
                date=date,
                file_type=FileType.CHECKSUM,
                market_type=self.market_type_str,
            )

            # Create temporary files with meaningful names
            filename = f"{self._symbol}-{base_interval}-{date.strftime('%Y-%m-%d')}"
            temp_dir = tempfile.gettempdir()

            temp_file_path = Path(temp_dir) / f"{filename}.zip"
            temp_checksum_path = Path(temp_dir) / f"{filename}.zip.CHECKSUM"

            # Make sure we're not reusing existing files
            if temp_file_path.exists():
                temp_file_path.unlink()
            if temp_checksum_path.exists():
                temp_checksum_path.unlink()

            # Download the data file
            response = self._client.get(url)
            if response.status_code == HTTP_NOT_FOUND:
                # For 404 errors, check if the date is too fresh
                if self._should_skip_retry_for_fresh_date(date):
                    # If date is too fresh, don't retry and return gracefully
                    return (
                        None,
                        f"404: Data not available for {date.date()} - within freshness window",
                    )
                return None, f"404: Data not available for {date.date()}"

            if response.status_code != HTTP_OK:
                # For non-200 responses, also check if the date is too fresh
                if self._should_skip_retry_for_fresh_date(date):
                    # If date is too fresh, don't retry and return gracefully
                    return (
                        None,
                        f"HTTP error {response.status_code} for {date.date()} - within freshness window",
                    )
                return None, f"HTTP error {response.status_code} for {date.date()}"

            # Save to the temporary file
            with open(temp_file_path, "wb") as f:
                f.write(response.content)

            # Download the checksum file
            checksum_response = self._client.get(checksum_url)
            if checksum_response.status_code == HTTP_NOT_FOUND:
                logger.warning(f"Checksum file not available for {date.date()}")
            elif checksum_response.status_code != HTTP_OK:
                logger.warning(f"HTTP error {checksum_response.status_code} when getting checksum for {date.date()}")
            else:
                # Get checksum content
                checksum_content = checksum_response.content

                # Save checksum to the temporary file
                with open(temp_checksum_path, "wb") as f:
                    f.write(checksum_content)

                # Log file size after saving
                checksum_file_size = temp_checksum_path.stat().st_size

                # If the checksum file is not empty or too small, verify checksum
                if checksum_file_size >= MIN_CHECKSUM_SIZE:
                    # Verify checksum if available
                    try:
                        import time

                        from utils.for_core.vision_checksum import (
                            calculate_sha256_direct,
                        )

                        # Small delay to ensure filesystem sync
                        time.sleep(0.1)

                        # Calculate the checksum
                        actual_checksum = calculate_sha256_direct(temp_file_path)

                        # Extract expected checksum
                        expected_checksum = None
                        try:
                            # Try to read the checksum file directly
                            with open(temp_checksum_path, "rb") as f:
                                checksum_content = f.read()
                                if isinstance(checksum_content, bytes):
                                    checksum_text = checksum_content.decode("utf-8", errors="replace").strip()
                                else:
                                    checksum_text = checksum_content.strip()

                                # Look for a SHA-256 hash pattern (64 hex chars)
                                import re

                                hash_match = re.search(r"([a-fA-F0-9]{64})", checksum_text)
                                if hash_match:
                                    expected_checksum = hash_match.group(1)

                                    if expected_checksum.lower() == actual_checksum.lower():
                                        logger.info(f"Checksum verification passed for {date.date()}")
                                    else:
                                        logger.critical(
                                            f"Checksum verification failed for {date.date()}. "
                                            f"Expected: {expected_checksum}, Actual: {actual_checksum}"
                                        )
                                        checksum_failed = True

                                        # Check if the date is too fresh before treating checksum failures as critical
                                        if self._should_skip_retry_for_fresh_date(date):
                                            # For fresh dates, just log the warning and continue
                                            logger.warning(
                                                f"Checksum verification failed for recent data ({date.date()}). "
                                                f"This may be expected for data within the freshness window."
                                            )
                        except Exception as extract_e:
                            # Extraction failed, but we can still self-verify
                            logger.debug(f"Could not extract checksum from file: {extract_e}")

                    except Exception as e:
                        # Only log a warning, don't set checksum_failed
                        logger.debug(f"Error in checksum verification for {date.date()}: {e}")

            # Process the zip file
            try:
                with zipfile.ZipFile(temp_file_path, "r") as zip_ref:
                    # Find the CSV file in the zip
                    csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
                    if not csv_files:
                        # Check if date is too fresh when no CSV files found
                        if self._should_skip_retry_for_fresh_date(date):
                            return (
                                None,
                                f"No CSV file found in zip for {date.date()} - within freshness window",
                            )
                        return None, f"No CSV file found in zip for {date.date()}"

                    csv_file = csv_files[0]  # Take the first CSV file

                    # Extract and process the CSV file
                    with tempfile.TemporaryDirectory() as temp_dir:
                        zip_ref.extract(csv_file, temp_dir)
                        csv_path = Path(temp_dir) / csv_file

                        # Read the CSV file
                        with open(csv_path) as f:
                            first_lines = [next(f) for _ in range(3)]

                            # Check if the first line contains headers (e.g., 'high' keyword)
                            has_header = any("high" in line.lower() for line in first_lines[:1])
                            logger.debug(f"Headers detected: {has_header}")

                            # Reopen the file to read from the beginning
                            f.seek(0)

                        # Read CSV with or without header based on detection
                        if has_header:
                            logger.info("Headers detected in CSV, reading with header=0")
                            df = pd.read_csv(csv_path, header=0)
                            # Map column names to standard names if needed
                            if "open_time" not in df.columns and len(df.columns) == len(KLINE_COLUMNS):
                                df.columns = KLINE_COLUMNS
                        else:
                            # No headers detected, use the standard column names
                            logger.info("No headers detected in CSV, reading with header=None")
                            df = pd.read_csv(csv_path, header=None, names=KLINE_COLUMNS)

                        logger.debug(f"Read {len(df)} rows from CSV")

                        # Process the data
                        if not df.empty:
                            # Store original timestamp info for later analysis if not already present
                            if "original_timestamp" not in df.columns:
                                df["original_timestamp"] = df.iloc[:, 0].astype(str)

                            # Process timestamp columns using the imported utility function
                            df = process_timestamp_columns(df, self._interval_str)

                            # Add warning to data if checksum failed (only if really failed)
                            warning_msg = None
                            if checksum_failed:
                                warning_msg = f"Data used despite checksum verification failure for {date.date()}"
                                logger.warning(warning_msg)

                            return df, warning_msg
                        return None, f"Empty dataframe for {date.date()}"
            except Exception as e:
                logger.error(
                    f"Error processing zip file {temp_file_path}: {e!s}",
                    exc_info=True,
                )
                return None, f"Error processing zip file: {e!s}"
        except Exception as e:
            logger.error(f"Unexpected error processing {date.date()}: {e!s}")
            return None, f"Unexpected error: {e!s}"
        finally:
            # Clean up temp files
            try:
                if "temp_file_path" in locals() and temp_file_path.exists():
                    temp_file_path.unlink()
                if "temp_checksum_path" in locals() and temp_checksum_path.exists():
                    temp_checksum_path.unlink()
            except Exception as e:
                logger.warning(f"Error cleaning up temporary files: {e}")

        return None, None

    def _download_data(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> TimestampedDataFrame:
        """Download data from Binance Vision API for a specific time range.

        Args:
            start_time: Start time
            end_time: End time

        Returns:
            TimestampedDataFrame with downloaded data

        Note:
            Timestamps in the returned data preserve their semantic meaning:
            - open_time represents the BEGINNING of each candle period
            - close_time represents the END of each candle period
        """
        logger.info(f"Downloading data for {self._symbol} {self._interval_str} from {start_time.isoformat()} to {end_time.isoformat()}")

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
        warning_messages = []  # Collect warning messages
        checksum_failures = []  # Track checksum failures
        fresh_date_failures = []  # Track date failures due to freshness

        # For very short intervals like 1s, avoid too many concurrent downloads
        if self._interval_str == "1s" and max_workers > CONCURRENT_DOWNLOADS_LIMIT_1S:
            max_workers = CONCURRENT_DOWNLOADS_LIMIT_1S
            logger.info(f"Limited concurrent downloads to {max_workers} for 1s interval")

        # Create date objects to pass to ThreadPoolExecutor
        date_objects = [datetime(d.year, d.month, d.day, tzinfo=timezone.utc) for d in date_range]

        # Get data files
        if len(date_objects) == 0:
            logger.warning("No dates to download")
            return self.create_empty_dataframe()

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit download tasks
                future_to_date = {executor.submit(self._download_file, date_obj): date_obj for date_obj in date_objects}

                # Process results as they complete
                for future in as_completed(future_to_date):
                    date = future_to_date[future]
                    try:
                        df, warning = future.result()
                        if warning:
                            # Handle warnings about fresh data differently
                            if "freshness window" in warning:
                                fresh_date_failures.append((date, warning))
                                logger.info(f"Expected failure for {date}: {warning} (within VISION_DATA_DELAY_HOURS window)")
                            # Only track actual checksum failures as warnings
                            elif "Checksum verification failed" in warning and "extraction" not in warning:
                                checksum_failures.append((date, warning))
                                logger.critical(f"Checksum failure for {date}: {warning}")
                            else:
                                warning_messages.append(warning)

                        if df is not None and not df.empty:
                            # Ensure each dataframe is properly sorted by open_time before adding it
                            if "open_time" in df.columns and not df["open_time"].is_monotonic_increasing:
                                df = df.sort_values("open_time").reset_index(drop=True)
                            downloaded_dfs.append(df)
                    except Exception as exc:
                        # Check if this date is too fresh
                        if self._should_skip_retry_for_fresh_date(date):
                            fresh_date_failures.append((date, f"Error: {exc}"))
                            logger.info(f"Expected failure for {date}: {exc} - Date is within the freshness window, skipping retries")
                        else:
                            logger.error(f"Error downloading data for {date}: {exc} - This date will be treated as unavailable")

            # After all downloads, check if there were any checksum failures
            if checksum_failures:
                failed_dates = [d.strftime("%Y-%m-%d") for d, _ in checksum_failures]
                logger.critical(
                    f"CRITICAL: Checksum verification failed for {len(checksum_failures)} files: {', '.join(failed_dates)}. "
                    f"Data integrity is compromised. This indicates possible data corruption "
                    f"or tampering with the Binance Vision API data."
                )
                logger.warning("Proceeding with data despite checksum verification failures")

            # Report on fresh date failures, but treat them as expected
            if fresh_date_failures:
                fresh_dates = [d.strftime("%Y-%m-%d") for d, _ in fresh_date_failures]
                logger.info(
                    f"Expected failures for {len(fresh_date_failures)} recent dates: {', '.join(fresh_dates)}. "
                    f"These dates are within the {VISION_DATA_DELAY_HOURS}h freshness window. "
                    f"Higher-level components may fall back to REST API."
                )

        except Exception as e:
            logger.error(f"Error in ThreadPoolExecutor: {e}")
            # Re-raise if this was a checksum failure
            if "Checksum verification failed" in str(e):
                raise

        # If no data was downloaded, return empty dataframe with consolidated warning
        if not downloaded_dfs:
            if warning_messages:
                # Group warnings about 404 (missing data)
                missing_dates = []
                for msg in warning_messages:
                    if "404" in msg:
                        parts = msg.split("for ")
                        if len(parts) > 1:
                            missing_dates.append(parts[1].strip())

                if missing_dates:
                    dates_str = ", ".join(missing_dates)
                    logger.warning(
                        f"No data downloaded from Binance Vision API - dates not available: {dates_str}. "
                        f"This may happen for recent data or less common markets. "
                        f"Returning empty dataframe - higher-level components may fall back to REST API."
                    )
                else:
                    logger.warning(
                        "No data downloaded from Binance Vision API - this may happen for recent data or less common markets. "
                        "Returning empty dataframe - higher-level components may fall back to REST API."
                    )
            else:
                logger.warning(
                    "No data downloaded from Binance Vision API - this may happen for recent data or less common markets. "
                    "Returning empty dataframe - higher-level components may fall back to REST API."
                )
            return self.create_empty_dataframe()

        logger.info(f"Downloaded {len(downloaded_dfs)} daily files")

        # Concatenate all dataframes
        concatenated_df = pd.concat(downloaded_dfs, ignore_index=True)

        # If the dataframe is empty, return early
        if concatenated_df.empty:
            logger.warning("No data in downloaded files")
            return self.create_empty_dataframe()

        # Ensure timestamps are in datetime format
        if "open_time" not in concatenated_df.columns:
            logger.error(f"Missing 'open_time' column in downloaded data. Columns: {concatenated_df.columns}")
            return self.create_empty_dataframe()

        # Sort the dataframe by timestamp
        if not concatenated_df["open_time"].is_monotonic_increasing:
            concatenated_df = concatenated_df.sort_values("open_time").reset_index(drop=True)

        # Filter data to requested time range
        filtered_df = filter_dataframe_by_time(concatenated_df, start_time, end_time, "open_time")

        # Log filtering results for debugging
        if not filtered_df.empty:
            logger.debug(f"Filtered dataframe contains {len(filtered_df)} rows")
        else:
            logger.warning("Filtered dataframe is empty - no data within requested time range")

        # Find gaps in the data
        try:
            interval_obj = next((i for i in Interval if i.value == self._interval_str), None)
            if interval_obj is None:
                interval_obj = Interval.MINUTE_1
                logger.warning(f"Could not find interval {self._interval_str}, using MINUTE_1 as default for gap detection")
        except Exception as e:
            logger.warning(f"Error parsing interval for gap detection: {e}")
            interval_obj = Interval.MINUTE_1

        # Detect gaps in the data
        time_span_days = (end_time - start_time).total_seconds() / 86400
        min_span_required = time_span_days > 1

        if filtered_df.empty:
            gaps = []
        else:
            try:
                df_for_gap_detection = filtered_df.copy()

                # Ensure open_time is present and is a datetime type
                if "open_time" not in df_for_gap_detection.columns and isinstance(df_for_gap_detection.index, pd.DatetimeIndex):
                    df_for_gap_detection["open_time"] = df_for_gap_detection.index
                elif "open_time" in df_for_gap_detection.columns and not pd.api.types.is_datetime64_any_dtype(
                    df_for_gap_detection["open_time"]
                ):
                    try:
                        df_for_gap_detection["open_time"] = pd.to_datetime(df_for_gap_detection["open_time"], unit="ms", utc=True)
                    except Exception as e:
                        logger.warning(f"Failed to convert open_time to datetime: {e}")

                # Check if we have a valid time column now
                if "open_time" in df_for_gap_detection.columns:
                    gaps, stats = detect_gaps(
                        df_for_gap_detection,
                        interval_obj,
                        time_column="open_time",
                        enforce_min_span=min_span_required,
                    )
                    if gaps:
                        logger.debug(f"Detected {len(gaps)} gaps in Vision data")
                else:
                    logger.warning("No open_time column available for gap detection")
                    gaps = []
            except Exception as e:
                logger.error(f"Error detecting gaps: {e}")
                gaps = []

        # Check for day boundary gaps (gaps at midnight)
        boundary_gaps = find_day_boundary_gaps(gaps)

        # Try to fill day boundary gaps using REST API
        if boundary_gaps:
            logger.debug(f"Detected {len(boundary_gaps)} day boundary gaps. Attempting to fill with REST API data.")
            filled_df = fill_boundary_gaps_with_rest(
                filtered_df,
                boundary_gaps,
                self._symbol,
                self.interval_obj,
                self.market_type,
            )
            if filled_df is not None:
                filtered_df = filled_df
                logger.debug(f"Successfully filled boundary gaps with REST API. New row count: {len(filtered_df)}")
            else:
                logger.warning(f"Failed to fill {len(boundary_gaps)} boundary gaps with REST API.")

        # Store original timestamp for reference
        if "open_time" in filtered_df.columns and "original_timestamp" not in filtered_df.columns:
            filtered_df["original_timestamp"] = filtered_df["open_time"]

        # Create TimestampedDataFrame based on available columns
        if "open_time_us" not in filtered_df.columns and "open_time" in filtered_df.columns:
            df_for_index = filtered_df.copy()
            df_for_index = df_for_index.set_index("open_time")
            return TimestampedDataFrame(df_for_index)
        if "open_time_us" in filtered_df.columns:
            df_for_index = filtered_df.copy()
            if "open_time" in df_for_index.columns:
                df_for_index = df_for_index.drop(columns=["open_time"])
            df_for_index = df_for_index.set_index("open_time_us")
            return TimestampedDataFrame(df_for_index)
        if filtered_df.empty:
            return self.create_empty_dataframe()

        # Check if open_time column exists, add it if necessary
        if "open_time" not in filtered_df.columns:
            time_cols = [col for col in filtered_df.columns if "time" in col.lower()]
            if time_cols:
                filtered_df["open_time"] = filtered_df[time_cols[0]]
            else:
                logger.error("No suitable time column found to create open_time")
                return self.create_empty_dataframe()

        return TimestampedDataFrame(filtered_df)

    def fetch(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch data for a specific time range from the Binance Vision API.

        This method implements the DataClientInterface fetch method.
        It downloads data from Binance Vision API using daily data files and
        handles date boundaries correctly.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Time interval string (e.g., "1m", "1h")
            start_time: Start time for data retrieval (timezone-aware datetime)
            end_time: End time for data retrieval (timezone-aware datetime)
            **kwargs: Additional parameters (unused, for interface compatibility)

        Returns:
            DataFrame with data, where open_time is both a column and the index name

        Raises:
            Exception: If checksum verification fails, indicating data integrity issues

        Note:
            This client is optimized for historical data. For recent data (< 2 days old),
            use the RestDataClient as Vision API typically has a 24-48 hour delay.

            Timestamps in the returned data preserve their semantic meaning:
            - open_time represents the BEGINNING of each candle period
            - close_time represents the END of each candle period
        """
        # Validate parameters
        if not isinstance(symbol, str) or not symbol:
            logger.warning(f"Invalid symbol: {symbol}, using client symbol {self._symbol}")
            symbol = self._symbol
        elif symbol != self._symbol:
            logger.warning(f"Symbol mismatch: requested {symbol}, client configured for {self._symbol}. Using client configuration.")

        if not isinstance(interval, str) or not interval:
            logger.warning(f"Invalid interval: {interval}, using client interval {self._interval_str}")
            interval = self._interval_str
        elif interval != self._interval_str:
            logger.warning(
                f"Interval mismatch: requested {interval}, client configured for {self._interval_str}. Using client configuration."
            )

        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise ValueError("Start time and end time must be datetime objects")

        if start_time >= end_time:
            raise ValueError(f"Start time {start_time} must be before end time {end_time}")

        try:
            # Enforce consistent timezone for time boundaries
            start_time = start_time.astimezone(timezone.utc)
            end_time = end_time.astimezone(timezone.utc)

            # Calculate date range
            delta_days = (end_time - start_time).days + 1

            # Log if it's a large request
            if delta_days > LARGE_REQUEST_DAYS:
                logger.info(f"Processing a large date range of {delta_days} days with parallel downloads.")

            # Download data
            try:
                timestamped_df = self._download_data(start_time, end_time)

                # Convert to standard pandas DataFrame
                if hasattr(timestamped_df, "to_pandas"):
                    df = timestamped_df.to_pandas()
                else:
                    df = pd.DataFrame(timestamped_df)

                # Ensure open_time is properly handled
                return ensure_open_time_as_column(df)

            except Exception as e:
                if "Checksum verification failed" in str(e):
                    # Log but don't stop execution for checksum failures
                    logger.critical(f"Checksum verification issues detected: {e}")
                    logger.warning("Continuing despite checksum verification issues")

                    # Return the data we have, or empty dataframe if none
                    if "df" in locals() and df is not None and not df.empty:
                        logger.info(f"Returning {len(df)} rows despite checksum issues")
                        return df
                    logger.critical("No data available due to checksum verification failure")
                    raise RuntimeError(f"VISION API DATA INTEGRITY ERROR: {e!s}")
                logger.error(f"Error in _download_data: {e}")
                raise

        except Exception as e:
            # Check if this is a checksum error that needs to be propagated
            if "Checksum verification failed" in str(e) or "VISION API DATA INTEGRITY ERROR" in str(e):
                # This is critical and should be propagated to trigger failover
                raise

            # Check if the request is within the allowed delay window for Vision API
            current_time = datetime.now(timezone.utc)
            vision_delay = timedelta(hours=VISION_DATA_DELAY_HOURS)

            if end_time > (current_time - vision_delay):
                # This falls within the allowable delay window for Vision API
                logger.warning(
                    f"Expected data unavailability from Vision API (within {VISION_DATA_DELAY_HOURS}h delay window): {e}. "
                    f"Returning empty dataframe - caller (not this client) may attempt REST API fallback."
                )
                return self.create_empty_dataframe()
            # For historical data outside the delay window, this is a critical error
            logger.critical(
                f"CRITICAL ERROR fetching historical data from Vision API: {e}. "
                f"This data should be available in Vision API but could not be retrieved."
            )
            raise RuntimeError(f"Vision API failed to retrieve historical data: {e!s}")

    @staticmethod
    def fetch_multiple(
        symbols: list[str],
        start_time: datetime,
        end_time: datetime,
        interval: str = "1m",
        market_type: str | MarketType = MarketType.SPOT,
        max_workers: int | None = None,
    ) -> dict[str, TimestampedDataFrame]:
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
        if delta_days > LARGE_REQUEST_DAYS:
            logger.info(f"Processing a large date range of {delta_days} days for {len(symbols)} symbols with parallel downloads.")

        logger.info(f"Fetching data for {len(symbols)} symbols using {max_workers} parallel workers")

        results: dict[str, TimestampedDataFrame] = {}

        # Define worker function to download data for a single symbol
        def download_worker(symbol: str) -> tuple[str, TimestampedDataFrame]:
            try:
                with VisionDataClient(symbol=symbol, interval=interval, market_type=market_type) as client:
                    df = client.fetch(symbol, interval, start_time, end_time)
                return symbol, df
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
                # Propagate critical errors
                if "CRITICAL ERROR" in str(e) or "DATA INTEGRITY ERROR" in str(e):
                    logger.critical(f"Critical download failure for {symbol}: {e}")
                    raise

                # Return empty dataframe for non-critical errors
                logger.warning(f"Non-critical error for {symbol}, returning empty dataframe")
                client = VisionDataClient(symbol=symbol, interval=interval, market_type=market_type)
                empty_df = client.create_empty_dataframe()
                client.close()
                return symbol, empty_df

        # Use ThreadPoolExecutor to parallelize downloads across symbols
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {executor.submit(download_worker, symbol): symbol for symbol in symbols}

            # Process results as they complete
            for i, future in enumerate(as_completed(future_to_symbol)):
                symbol = future_to_symbol[future]
                try:
                    symbol_result, df = future.result()
                    results[symbol_result] = df
                    logger.info(f"Completed download for {symbol} ({i + 1}/{len(symbols)}): {len(df)} records")
                except Exception as e:
                    logger.error(f"Error processing result for {symbol}: {e}")
                    # Create empty dataframe for failed symbols
                    client = VisionDataClient(symbol=symbol, interval=interval, market_type=market_type)
                    results[symbol] = client.create_empty_dataframe()
                    client.close()

        # Check if all downloads failed (all results are empty dataframes)
        all_empty = all(df.empty for df in results.values()) if results else True
        if all_empty and symbols:
            logger.critical(f"CRITICAL ERROR: All {len(symbols)} symbols failed to download")
            raise RuntimeError("All symbol downloads failed. No data available from Vision API.")

        return results
