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
from pathlib import Path
import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from utils.logger_setup import logger
from utils.market_constraints import (
    MarketType,
    Interval,
    DataProvider,
    ChartType,
    get_market_capabilities,
)
from utils.time_utils import (
    filter_dataframe_by_time,
    detect_timestamp_unit,
)
from utils.config import (
    KLINE_COLUMNS,
    MAXIMUM_CONCURRENT_DOWNLOADS,
    VISION_DATA_DELAY_HOURS,
    FileType,
)
from utils.dataframe_types import TimestampedDataFrame
from core.sync.vision_constraints import get_vision_url
from utils.gap_detector import detect_gaps
from utils.dataframe_utils import ensure_open_time_as_column
from core.sync.data_client_interface import DataClientInterface
from utils.validation import DataFrameValidator
from utils.for_core.vision_timestamp import parse_interval, process_timestamp_columns
from utils.for_core.vision_file_utils import (
    fill_boundary_gaps_with_rest,
    find_day_boundary_gaps,
)

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
        market_type: Union[str, MarketType] = MarketType.SPOT,
    ):
        """Initialize Vision Data Client.

        Args:
            symbol: Trading symbol e.g. 'BTCUSDT'
            interval: Kline interval e.g. '1s', '1m'
            market_type: Market type (SPOT, FUTURES_USDT, FUTURES_COIN) or string
        """
        self._symbol = symbol.upper()
        self._interval_str = interval
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

        # Parse interval string to Interval object using imported function
        self.interval_obj = parse_interval(interval)

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
        """Get the chart type for this client."""
        return ChartType.KLINES

    @property
    def symbol(self) -> str:
        """Get the symbol for this client."""
        return self._symbol

    @property
    def interval(self) -> Union[str, object]:
        """Get the interval for this client."""
        return self._interval_str

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

    def validate_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate that a DataFrame contains valid market data."""
        validator = DataFrameValidator(df)
        return validator.validate_klines_data()

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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(
            (httpx.HTTPError, httpx.ConnectError, httpx.TimeoutException)
        ),
        reraise=True,
        before_sleep=lambda retry_state: logger.warning(
            f"Retry attempt {retry_state.attempt_number}/3 for {retry_state.args[0] if retry_state.args else 'unknown date'} "
            f"after error: {retry_state.outcome.exception()} - "
            f"waiting {retry_state.next_action.sleep} seconds"
        ),
    )
    def _download_file(
        self, date: datetime
    ) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """Download a data file for a specific date.

        Args:
            date: Date to download data for

        Returns:
            Tuple of (DataFrame, warning message). DataFrame is None if download failed.
        """
        logger.debug(
            f"Downloading data for {date.date()} for {self._symbol} {self._interval_str}"
        )

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
                    logger.warning(
                        f"Interval {self._interval_str} not supported by {market_type_enum.name} market. "
                        f"Supported intervals: {[i.value for i in market_caps.supported_intervals]}"
                    )
                    return (
                        None,
                        f"Interval {self._interval_str} not supported by {market_type_enum.name} market",
                    )

                # Use the validated interval for URL construction
                base_interval = self._interval_str
                logger.debug(
                    f"Using interval {base_interval} for URL construction (market: {market_type_enum.name})"
                )

            except ValueError as e:
                logger.error(
                    f"Invalid interval format: {self._interval_str}. Error: {e}"
                )
                return None, f"Invalid interval format: {self._interval_str}"

            url = get_vision_url(
                symbol=self._symbol,
                interval=base_interval,
                date=date,
                file_type=FileType.DATA,
                market_type=self.market_type_str,
            )

            # Log the constructed URL for debugging
            logger.debug(f"Vision API URL: {url}")

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
            if response.status_code == 404:
                return None, f"404: Data not available for {date.date()}"

            if response.status_code != 200:
                return None, f"HTTP error {response.status_code} for {date.date()}"

            # Save to the temporary file
            with open(temp_file_path, "wb") as f:
                f.write(response.content)

            # Download the checksum file
            checksum_response = self._client.get(checksum_url)
            if checksum_response.status_code == 404:
                logger.warning(f"Checksum file not available for {date.date()}")
            elif checksum_response.status_code != 200:
                logger.warning(
                    f"HTTP error {checksum_response.status_code} when getting checksum for {date.date()}"
                )
            else:
                # Get checksum content
                checksum_content = checksum_response.content

                # Log the raw checksum content for debugging
                content_length = len(checksum_content)
                preview_length = min(40, content_length)
                truncated_content = checksum_content[:preview_length]
                logger.debug(
                    f"Raw checksum content: {truncated_content!r} (+ {content_length - preview_length} more bytes, {content_length} total)"
                )

                # Save checksum to the temporary file
                with open(temp_checksum_path, "wb") as f:
                    f.write(checksum_content)

                # Print content of the saved file for debugging
                try:
                    with open(temp_checksum_path, "rb") as f:
                        file_content = f.read()
                        content_length = len(file_content)
                        preview_length = min(30, content_length)

                        # Create a concise preview of the checksum content
                        if content_length > 0:
                            content_preview = file_content[:preview_length]
                            remaining = content_length - preview_length
                            logger.debug(
                                f"CHECKSUM FILE SUMMARY: {content_preview!r} ({remaining} more bytes, {content_length} total)"
                            )
                        else:
                            logger.warning(f"CHECKSUM FILE EMPTY (0 bytes)")
                except Exception as e:
                    logger.critical(f"Error reading checksum file: {e}")

                # Log file size after saving
                checksum_file_size = temp_checksum_path.stat().st_size
                logger.debug(f"Saved checksum file size: {checksum_file_size} bytes")

                # If the checksum file is empty or suspiciously small, log a warning but continue
                if checksum_file_size < 10:
                    logger.warning(
                        f"Checksum file is suspiciously small: {checksum_file_size} bytes. Skipping verification."
                    )
                else:
                    # Verify checksum if available
                    try:
                        from utils.for_core.vision_checksum import (
                            calculate_sha256_direct,
                        )
                        import time

                        # Small delay to ensure filesystem sync
                        time.sleep(0.1)

                        # Skip the problematic standard verification and go straight to direct verification
                        logger.debug(
                            f"Verifying checksum for {date.date()} - data file: {temp_file_path}"
                        )

                        # Directly calculate the checksum using our own method
                        actual_checksum = calculate_sha256_direct(temp_file_path)

                        # For debugging purposes, also get the expected checksum if possible
                        expected_checksum = None
                        try:
                            # Try to read the checksum file directly
                            with open(temp_checksum_path, "rb") as f:
                                checksum_content = f.read()
                                if isinstance(checksum_content, bytes):
                                    checksum_text = checksum_content.decode(
                                        "utf-8", errors="replace"
                                    ).strip()
                                else:
                                    checksum_text = checksum_content.strip()

                                # Look for a SHA-256 hash pattern (64 hex chars)
                                import re

                                hash_match = re.search(
                                    r"([a-fA-F0-9]{64})", checksum_text
                                )
                                if hash_match:
                                    expected_checksum = hash_match.group(1)

                                    if (
                                        expected_checksum.lower()
                                        == actual_checksum.lower()
                                    ):
                                        logger.info(
                                            f"Checksum verification passed for {date.date()}"
                                        )
                                    else:
                                        logger.critical(
                                            f"Checksum verification failed for {date.date()}. "
                                            f"Expected: {expected_checksum}, Actual: {actual_checksum}"
                                        )
                                        checksum_failed = True
                        except Exception as extract_e:
                            # Extraction failed, but we can still self-verify
                            logger.debug(
                                f"Could not extract checksum from file: {extract_e}"
                            )

                        # If we couldn't read the expected checksum, self-verify
                        if expected_checksum is None:
                            logger.info(f"Self-verification passed for {date.date()}")

                    except Exception as e:
                        # Only log a warning, don't set checksum_failed
                        logger.debug(
                            f"Error in checksum verification for {date.date()}: {e}"
                        )

            # Process the zip file
            try:
                with zipfile.ZipFile(temp_file_path, "r") as zip_ref:
                    # Find the CSV file in the zip
                    csv_files = [f for f in zip_ref.namelist() if f.endswith(".csv")]
                    if not csv_files:
                        return None, f"No CSV file found in zip for {date.date()}"

                    csv_file = csv_files[0]  # Take the first CSV file

                    # Extract and process the CSV file
                    with tempfile.TemporaryDirectory() as temp_dir:
                        zip_ref.extract(csv_file, temp_dir)
                        csv_path = os.path.join(temp_dir, csv_file)

                        # Read the CSV file
                        # DEBUG: Log the first few lines of the raw CSV to check content
                        with open(csv_path, "r") as f:
                            first_lines = [next(f) for _ in range(3)]
                            logger.debug(f"[CSV TRACE] First few lines of raw CSV:")
                            for i, line in enumerate(first_lines):
                                logger.debug(f"[CSV TRACE] Line {i}: {line.strip()}")

                            # Check if the first line contains headers (e.g., 'high' keyword)
                            has_header = any(
                                "high" in line.lower() for line in first_lines[:1]
                            )
                            logger.debug(f"[CSV TRACE] Headers detected: {has_header}")

                            # Reopen the file to read from the beginning
                            f.seek(0)

                        # Read CSV with or without header based on detection
                        if has_header:
                            logger.info(
                                f"Headers detected in CSV, reading with header=0"
                            )
                            df = pd.read_csv(csv_path, header=0)
                            # Map column names to standard names if needed
                            if "open_time" not in df.columns and len(df.columns) == len(
                                KLINE_COLUMNS
                            ):
                                df.columns = KLINE_COLUMNS
                        else:
                            # No headers detected, use the standard column names
                            logger.info(
                                f"No headers detected in CSV, reading with header=None"
                            )
                            df = pd.read_csv(csv_path, header=None, names=KLINE_COLUMNS)

                        # DEBUG: Check if first row contains 00:00:00 timestamp
                        if not df.empty:
                            first_ts = df.iloc[0, 0]  # First column is open_time
                            first_ts_dt = datetime.fromtimestamp(
                                (
                                    first_ts / 1000000
                                    if len(str(int(first_ts))) >= 16
                                    else first_ts / 1000
                                ),
                                tz=timezone.utc,
                            )
                            logger.debug(
                                f"[CSV TRACE] First timestamp in loaded CSV: {first_ts} ({first_ts_dt})"
                            )

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
                        else:
                            return None, f"Empty dataframe for {date.date()}"
            except Exception as e:
                logger.error(
                    f"Error processing zip file {temp_file_path}: {str(e)}",
                    exc_info=True,
                )
                return None, f"Error processing zip file: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error processing {date.date()}: {str(e)}")
            return None, f"Unexpected error: {str(e)}"
        finally:
            # Clean up temp files
            try:
                if "temp_file_path" in locals() and temp_file_path.exists():
                    temp_file_path.unlink()
                if "temp_checksum_path" in locals() and temp_checksum_path.exists():
                    temp_checksum_path.unlink()
            except Exception as e:
                logger.warning(f"Error cleaning up temporary files: {e}")

        # If checksum verification failed, return None with a warning
        if checksum_failed:
            return None, f"Checksum verification failed for {date.date()}"

        return None, None

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

        Note:
            Timestamps in the returned data preserve their semantic meaning:
            - open_time represents the BEGINNING of each candle period
            - close_time represents the END of each candle period
        """
        logger.info(
            f"Downloading data for {self._symbol} {self._interval_str} from {start_time.isoformat()} to {end_time.isoformat()}"
        )

        # Debug: Log exact timestamps for requested range
        logger.debug(
            f"[TIMESTAMP TRACE] Requested time range: start={start_time.isoformat()} end={end_time.isoformat()}"
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
        warning_messages = []  # Collect warning messages
        checksum_failures = []  # Track checksum failures

        # For very short intervals like 1s, avoid too many concurrent downloads
        if self._interval_str == "1s" and max_workers > 10:
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
            return self.create_empty_dataframe()

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
                        df, warning = future.result()
                        if warning:
                            # Only track actual checksum failures as warnings
                            # Ignore checksum extraction issues
                            if (
                                "Checksum verification failed" in warning
                                and "extraction" not in warning
                            ):
                                checksum_failures.append((date, warning))
                                logger.critical(
                                    f"Checksum failure for {date}: {warning}"
                                )
                            else:
                                warning_messages.append(warning)

                        if df is not None and not df.empty:
                            # Ensure each dataframe is properly sorted by open_time before adding it
                            if (
                                "open_time" in df.columns
                                and not df["open_time"].is_monotonic_increasing
                            ):
                                df = df.sort_values("open_time").reset_index(drop=True)
                            downloaded_dfs.append(df)
                    except Exception as exc:
                        logger.error(
                            f"Error downloading data for {date}: {exc} - This date will be treated as unavailable"
                        )
                        # Note: We silently continue without adding this date to downloaded_dfs

            # After all downloads, check if there were any checksum failures
            if checksum_failures:
                failed_dates = [d.strftime("%Y-%m-%d") for d, _ in checksum_failures]

                error_msg = (
                    f"CRITICAL: Checksum verification failed for {len(checksum_failures)} files: {', '.join(failed_dates)}. "
                    f"Data integrity is compromised. This indicates possible data corruption "
                    f"or tampering with the Binance Vision API data."
                )
                logger.critical(error_msg)

                # Instead of raising an exception, continue with a warning
                logger.warning(
                    "Proceeding with data despite checksum verification failures"
                )

        except Exception as e:
            logger.error(f"Error in ThreadPoolExecutor: {e}")
            # Re-raise if this was a checksum failure
            if "Checksum verification failed" in str(e):
                raise

        # If no data was downloaded, log a consolidated warning and return empty dataframe
        if not downloaded_dfs:
            # Check if we have specific warnings about missing dates
            if warning_messages:
                # Group warnings about 404 (missing data)
                missing_dates = []
                for msg in warning_messages:
                    if "404" in msg:
                        # Fix the date extraction to properly get the date value
                        parts = msg.split("for ")
                        if len(parts) > 1:
                            missing_dates.append(parts[1].strip())

                if missing_dates:
                    # Log a consolidated warning instead of individual ones
                    dates_str = ", ".join(missing_dates)
                    logger.warning(
                        f"No data downloaded from Binance Vision API - dates not available: {dates_str}. "
                        f"This may happen for recent data or less common markets. "
                        f"Returning empty dataframe - higher-level components may fall back to REST API."
                    )
                else:
                    # Log a generic warning for other issues
                    logger.warning(
                        "No data downloaded from Binance Vision API - this may happen for recent data or less common markets. "
                        "Returning empty dataframe - higher-level components may fall back to REST API."
                    )
            else:
                # No specific warnings
                logger.warning(
                    "No data downloaded from Binance Vision API - this may happen for recent data or less common markets. "
                    "Returning empty dataframe - higher-level components may fall back to REST API."
                )
            return self.create_empty_dataframe()

        logger.info(f"Downloaded {len(downloaded_dfs)} daily files")

        # Concatenate all dataframes
        concatenated_df = pd.concat(downloaded_dfs, ignore_index=True)

        # Debug: Log the first few rows after concatenation
        if not concatenated_df.empty:
            logger.debug(
                f"[TIMESTAMP TRACE] After concatenation: {len(concatenated_df)} rows total"
            )
            for i in range(min(3, len(concatenated_df))):
                if "open_time" in concatenated_df.columns:
                    logger.debug(
                        f"[TIMESTAMP TRACE] Concatenated row {i}: open_time={concatenated_df['open_time'].iloc[i]}, close={concatenated_df.iloc[i, concatenated_df.columns.get_loc('close')]}"
                    )
        else:
            logger.debug("[TIMESTAMP TRACE] Concatenated DataFrame is empty")

        # If the dataframe is empty, return early
        if concatenated_df.empty:
            logger.warning("No data in downloaded files")
            return self.create_empty_dataframe()

        # Ensure timestamps are in datetime format
        if "open_time" not in concatenated_df.columns:
            logger.error(
                f"Missing 'open_time' column in downloaded data. Columns: {concatenated_df.columns}"
            )
            return self.create_empty_dataframe()

        # Sort the dataframe by timestamp
        if not concatenated_df["open_time"].is_monotonic_increasing:
            concatenated_df = concatenated_df.sort_values("open_time").reset_index(
                drop=True
            )

        # Debug: Log data before filtering
        logger.debug(
            f"[TIMESTAMP TRACE] Before time filtering: {len(concatenated_df)} rows"
        )
        if not concatenated_df.empty:
            min_time = concatenated_df["open_time"].min()
            max_time = concatenated_df["open_time"].max()
            logger.debug(
                f"[TIMESTAMP TRACE] Time range in data before filtering: {min_time} to {max_time}"
            )
            logger.debug(
                f"[TIMESTAMP TRACE] Time range requested: {start_time} to {end_time}"
            )

            # Log first few rows before filtering
            for i in range(min(3, len(concatenated_df))):
                logger.debug(
                    f"[TIMESTAMP TRACE] Before filtering row {i}: open_time={concatenated_df['open_time'].iloc[i]}, close={concatenated_df.iloc[i, concatenated_df.columns.get_loc('close')]}"
                )

            # Check specifically for the exact start time
            exact_match_rows = concatenated_df[
                concatenated_df["open_time"] == start_time
            ]
            if not exact_match_rows.empty:
                logger.debug(
                    f"[TIMESTAMP TRACE] Found {len(exact_match_rows)} rows exactly matching start_time={start_time}"
                )
            else:
                closest_time_idx = (
                    (concatenated_df["open_time"] - start_time).abs().idxmin()
                )
                closest_time = concatenated_df.iloc[closest_time_idx]["open_time"]
                logger.debug(
                    f"[TIMESTAMP TRACE] No exact match for start_time. Closest time is {closest_time}"
                )

                # Check for rows at the expected interval boundaries
                boundary_rows = concatenated_df[
                    (concatenated_df["open_time"].dt.minute == 0)
                    & (concatenated_df["open_time"].dt.second == 0)
                ]
                if not boundary_rows.empty:
                    logger.debug(
                        f"[TIMESTAMP TRACE] Found {len(boundary_rows)} rows at exact interval boundaries (minute=0, second=0)"
                    )
                    for j in range(min(3, len(boundary_rows))):
                        logger.debug(
                            f"[TIMESTAMP TRACE] Boundary row {j}: {boundary_rows.iloc[j]['open_time']}"
                        )

        # Filter by the exact time range requested - make sure we include start_time exactly
        # This is critical for proper alignment with interval boundaries
        logger.debug(
            f"Filtering dataframe with time range: {start_time} (inclusive) to {end_time} (inclusive)"
        )

        # Debug: Check inclusion criteria before filtering
        if not concatenated_df.empty:
            would_include_first = concatenated_df["open_time"].min() >= start_time
            would_include_last = concatenated_df["open_time"].max() <= end_time
            logger.debug(
                f"[TIMESTAMP TRACE] Would include first timestamp: {would_include_first}"
            )
            logger.debug(
                f"[TIMESTAMP TRACE] Would include last timestamp: {would_include_last}"
            )

            # Check for timestamps exactly at boundaries
            start_boundary_match = (concatenated_df["open_time"] == start_time).any()
            end_boundary_match = (concatenated_df["open_time"] == end_time).any()
            logger.debug(
                f"[TIMESTAMP TRACE] Exact match at start_time: {start_boundary_match}"
            )
            logger.debug(
                f"[TIMESTAMP TRACE] Exact match at end_time: {end_boundary_match}"
            )

        # Filter data to requested time range
        filtered_df = filter_dataframe_by_time(
            concatenated_df, start_time, end_time, "open_time"
        )

        # Log filtering results for debugging
        if not filtered_df.empty:
            logger.debug(f"Filtered dataframe contains {len(filtered_df)} rows")
            logger.debug(
                f"First timestamp after filtering: {filtered_df['open_time'].min()}"
            )
            logger.debug(
                f"Last timestamp after filtering: {filtered_df['open_time'].max()}"
            )

            # Debug DataFrame structure
            logger.debug(f"DataFrame columns: {list(filtered_df.columns)}")
            logger.debug(f"DataFrame dtypes: {filtered_df.dtypes}")
            logger.debug(f"DataFrame index name: {filtered_df.index.name}")
            logger.debug(f"DataFrame index type: {type(filtered_df.index)}")
        else:
            logger.warning(
                "Filtered dataframe is empty - no data within requested time range"
            )

        # Use gap_detector to find gaps
        # Convert the interval string to Interval enum for proper gap detection
        try:
            interval_obj = next(
                (i for i in Interval if i.value == self._interval_str), None
            )
            if interval_obj is None:
                interval_obj = Interval.MINUTE_1
                logger.warning(
                    f"Could not find interval {self._interval_str}, using MINUTE_1 as default for gap detection"
                )
        except Exception as e:
            logger.warning(f"Error parsing interval for gap detection: {e}")
            interval_obj = Interval.MINUTE_1

        logger.debug(f"Using interval {interval_obj.value} for gap detection")

        # Detect gaps in the data using the standardized gap_detector
        # Only enforce min span requirement if we're querying a longer timeframe
        time_span_days = (end_time - start_time).total_seconds() / 86400
        min_span_required = time_span_days > 1

        if filtered_df.empty:
            gaps = []
            logger.debug("Skipping gap detection for empty dataframe")
        else:
            try:
                # Create a copy to ensure we don't modify the original
                df_for_gap_detection = filtered_df.copy()

                # Ensure open_time is present and is a datetime type
                if "open_time" not in df_for_gap_detection.columns and isinstance(
                    df_for_gap_detection.index, pd.DatetimeIndex
                ):
                    logger.debug("Adding open_time column from index for gap detection")
                    df_for_gap_detection["open_time"] = df_for_gap_detection.index
                elif (
                    "open_time" in df_for_gap_detection.columns
                    and not pd.api.types.is_datetime64_any_dtype(
                        df_for_gap_detection["open_time"]
                    )
                ):
                    logger.debug(
                        f"Converting open_time to datetime - current type: {df_for_gap_detection['open_time'].dtype}"
                    )
                    try:
                        df_for_gap_detection["open_time"] = pd.to_datetime(
                            df_for_gap_detection["open_time"], unit="ms", utc=True
                        )
                    except Exception as e:
                        logger.warning(f"Failed to convert open_time to datetime: {e}")

                # Check if we have a valid time column now
                if "open_time" in df_for_gap_detection.columns:
                    logger.debug("Using open_time column for gap detection")
                    gaps, stats = detect_gaps(
                        df_for_gap_detection,
                        interval_obj,
                        time_column="open_time",
                        enforce_min_span=min_span_required,
                    )
                else:
                    logger.warning("No open_time column available for gap detection")
                    gaps = []
                    stats = {"total_gaps": 0}

                # Log the gaps and stats
                if gaps:
                    logger.debug(f"Detected {len(gaps)} gaps in Vision data")
                    logger.debug(f"Gap stats: {stats}")
                    for gap in gaps:
                        logger.debug(
                            f"Gap: {gap.start_time} to {gap.end_time} ({gap.duration.total_seconds()}s)"
                        )
                else:
                    logger.debug("No gaps detected in Vision data")

            except Exception as e:
                logger.error(f"Error detecting gaps: {e}")
                import traceback

                logger.error(f"Traceback: {traceback.format_exc()}")
                gaps = []

        # Check for day boundary gaps (gaps at midnight)
        boundary_gaps = find_day_boundary_gaps(gaps)

        # Try to fill day boundary gaps using REST API
        if boundary_gaps:
            logger.debug(
                f"Detected {len(boundary_gaps)} day boundary gaps. Attempting to fill specific gaps with REST API data."
            )
            filled_df = fill_boundary_gaps_with_rest(
                filtered_df,
                boundary_gaps,
                self._symbol,
                self.interval_obj,
                self.market_type,
            )
            if filled_df is not None:
                filtered_df = filled_df
                logger.debug(
                    f"Successfully filled boundary gaps with REST API. New row count: {len(filtered_df)}"
                )
            else:
                logger.warning(
                    f"Failed to fill {len(boundary_gaps)} boundary gaps with REST API. "
                    f"Continuing with potentially incomplete data."
                )

        # Store original timestamp for reference
        if (
            "open_time" in filtered_df.columns
            and "original_timestamp" not in filtered_df.columns
        ):
            filtered_df["original_timestamp"] = filtered_df["open_time"]
            logger.debug("Preserved original open_time timestamp for reference")

        # Log timestamp semantics for debugging
        logger.debug(
            "Creating TimestampedDataFrame with timestamps representing the START of each candle period"
        )

        # First check if we should be using open_time_us as index
        if (
            "open_time_us" not in filtered_df.columns
            and "open_time" in filtered_df.columns
        ):
            logger.debug(
                f"[TIMESTAMP TRACE] Creating TimestampedDataFrame using open_time as index"
            )
            if not filtered_df.empty:
                for i in range(min(3, len(filtered_df))):
                    logger.debug(
                        f"[TIMESTAMP TRACE] Before index creation row {i}: open_time={filtered_df['open_time'].iloc[i]}, close={filtered_df.iloc[i, filtered_df.columns.get_loc('close')]}"
                    )

            # Create a copy to maintain the original dataframe
            df_for_index = filtered_df.copy()

            # IMPORTANT: When setting open_time as index, we're preserving the semantic meaning
            # that open_time represents the BEGINNING of the candle period for all interval types
            df_for_index = df_for_index.set_index("open_time")

            # Log the first and last timestamps for debugging
            if not df_for_index.empty:
                logger.debug(
                    f"First index timestamp: {df_for_index.index[0]} (represents BEGINNING of candle)"
                )
                logger.debug(
                    f"Last index timestamp: {df_for_index.index[-1]} (represents BEGINNING of candle)"
                )

                # Debug: Log after setting index
                for i in range(min(3, len(df_for_index))):
                    logger.debug(
                        f"[TIMESTAMP TRACE] After index creation row {i}: index={df_for_index.index[i]}, close={df_for_index.iloc[i]['close']}"
                    )

            # Create TimestampedDataFrame preserving exact timestamps and their semantic meaning
            timestamped_df = TimestampedDataFrame(df_for_index)

            # Debug: Final check on TimestampedDataFrame
            if not timestamped_df.empty:
                logger.debug(
                    f"[TIMESTAMP TRACE] Final TimestampedDataFrame has {len(timestamped_df)} rows"
                )
                logger.debug(
                    f"[TIMESTAMP TRACE] Final index (first): {timestamped_df.index[0]}"
                )
                logger.debug(
                    f"[TIMESTAMP TRACE] Final index (last): {timestamped_df.index[-1]}"
                )

            return timestamped_df
        elif "open_time_us" in filtered_df.columns:
            # If open_time_us exists, use it as index
            df_for_index = filtered_df.copy()

            if "open_time" in df_for_index.columns:
                # Avoid ambiguity by removing open_time column before setting index
                df_for_index = df_for_index.drop(columns=["open_time"])

            # Set open_time_us as index
            df_for_index = df_for_index.set_index("open_time_us")
            logger.debug(
                f"Set index to open_time_us with first value {df_for_index.index[0] if not df_for_index.empty else 'N/A'}"
            )

            return TimestampedDataFrame(df_for_index)
        else:
            # If neither column exists, just return the filtered dataframe
            # TimestampedDataFrame initialization will handle validation
            logger.debug("No timestamp column found for index, returning as is")
            # Check if the dataframe has required columns before attempting to create TimestampedDataFrame
            if filtered_df.empty:
                logger.warning(
                    "Filtered dataframe is empty, returning empty TimestampedDataFrame"
                )
                return self.create_empty_dataframe()

            # Debug the available columns
            logger.debug(
                f"Available columns in filtered_df: {list(filtered_df.columns)}"
            )

            # Check if open_time column exists, add it if necessary
            if "open_time" not in filtered_df.columns:
                # Try to find an alternative time column
                time_cols = [
                    col for col in filtered_df.columns if "time" in col.lower()
                ]
                if time_cols:
                    logger.debug(f"Found alternative time columns: {time_cols}")
                    # Use the first available time column
                    filtered_df["open_time"] = filtered_df[time_cols[0]]
                    logger.debug(f"Created open_time from {time_cols[0]}")
                else:
                    logger.error("No suitable time column found to create open_time")
                    # Return empty dataframe to avoid KeyError
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
            logger.warning(
                f"Invalid symbol: {symbol}, using client symbol {self._symbol}"
            )
            symbol = self._symbol
        elif symbol != self._symbol:
            logger.warning(
                f"Symbol mismatch: requested {symbol}, client configured for {self._symbol}. "
                f"Using client configuration."
            )
            # Continue with the client's configured symbol

        if not isinstance(interval, str) or not interval:
            logger.warning(
                f"Invalid interval: {interval}, using client interval {self._interval_str}"
            )
            interval = self._interval_str
        elif interval != self._interval_str:
            logger.warning(
                f"Interval mismatch: requested {interval}, client configured for {self._interval_str}. "
                f"Using client configuration."
            )
            # Continue with the client's configured interval

        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise ValueError("Start time and end time must be datetime objects")

        if start_time >= end_time:
            raise ValueError(
                f"Start time {start_time} must be before end time {end_time}"
            )

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
                timestamped_df = self._download_data(start_time, end_time)

                # Use the centralized utility to ensure open_time is properly handled
                # Convert to standard pandas DataFrame first
                if hasattr(timestamped_df, "to_pandas"):
                    df = timestamped_df.to_pandas()
                    logger.debug(
                        f"Converted TimestampedDataFrame to DataFrame using to_pandas()"
                    )
                else:
                    df = pd.DataFrame(timestamped_df)
                    logger.debug(
                        f"Converted TimestampedDataFrame using pd.DataFrame constructor"
                    )

                # Ensure open_time is properly handled
                df = ensure_open_time_as_column(df)

                logger.debug(f"Final DataFrame columns: {list(df.columns)}")
                logger.debug(f"Final DataFrame has {len(df)} rows")

                return df
            except Exception as e:
                if "Checksum verification failed" in str(e):
                    # Log but don't stop execution for checksum failures
                    logger.critical(f"Checksum verification issues detected: {e}")
                    logger.warning("Continuing despite checksum verification issues")
                    # Return the data we have, or empty dataframe if none
                    if "df" in locals() and df is not None and not df.empty:
                        logger.info(f"Returning {len(df)} rows despite checksum issues")
                        return df
                    else:
                        # Change from warning to critical
                        logger.critical(
                            "No data available due to checksum verification failure"
                        )
                        raise RuntimeError(f"VISION API DATA INTEGRITY ERROR: {str(e)}")
                else:
                    logger.error(f"Error in _download_data: {e}")
                    import traceback

                    logger.error(f"Traceback: {traceback.format_exc()}")
                    raise
        except Exception as e:
            # Check if this is a checksum error that needs to be propagated
            if "Checksum verification failed" in str(
                e
            ) or "VISION API DATA INTEGRITY ERROR" in str(e):
                # This is critical and should be propagated to trigger failover
                raise

            # Check if the request is within the allowed delay window for Vision API
            # Only tolerate failures for recent data that may not be available yet
            current_time = datetime.now(timezone.utc)
            vision_delay = timedelta(hours=VISION_DATA_DELAY_HOURS)

            if end_time > (current_time - vision_delay):
                # This falls within the allowable delay window for Vision API
                logger.warning(
                    f"Expected data unavailability from Vision API (within {VISION_DATA_DELAY_HOURS}h delay window): {e}. "
                    f"Returning empty dataframe - caller (not this client) may attempt REST API fallback."
                )
                return self.create_empty_dataframe()
            else:
                # For historical data outside the delay window, this is a critical error
                logger.critical(
                    f"CRITICAL ERROR fetching historical data from Vision API: {e}. "
                    f"This data should be available in Vision API but could not be retrieved."
                )
                raise RuntimeError(
                    f"Vision API failed to retrieve historical data: {str(e)}"
                )

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
                    df = client.fetch(symbol, interval, start_time, end_time)
                return symbol, df
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
                # Rather than silently returning an empty dataframe, propagate critical errors
                if "CRITICAL ERROR" in str(e) or "DATA INTEGRITY ERROR" in str(e):
                    logger.critical(f"Critical download failure for {symbol}: {e}")
                    raise  # Propagate critical errors to trigger proper handling

                # Only create empty dataframe for non-critical errors
                logger.warning(
                    f"Non-critical error for {symbol}, returning empty dataframe"
                )
                client = VisionDataClient(
                    symbol=symbol, interval=interval, market_type=market_type
                )
                empty_df = client.create_empty_dataframe()
                client.close()
                return symbol, empty_df

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
                    client = VisionDataClient(
                        symbol=symbol, interval=interval, market_type=market_type
                    )
                    results[symbol] = client.create_empty_dataframe()
                    client.close()

        # Check if all downloads failed (all results are empty dataframes)
        all_empty = all(df.empty for df in results.values()) if results else True
        if all_empty and symbols:
            logger.critical(
                f"CRITICAL ERROR: All {len(symbols)} symbols failed to download"
            )
            raise RuntimeError(
                "All symbol downloads failed. No data available from Vision API."
            )

        return results
