#!/usr/bin/env python
"""Timeseries data processor for unified timestamp handling.

This module provides the TimeseriesDataProcessor class for consistent
timestamp handling across different APIs (REST, Vision) and different
timestamp formats (milliseconds vs microseconds).

# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
# Refactoring: Extract from time_utils.py for modularity
# polars-exception: extracted from existing pandas-based time_utils.py - migration is separate task
"""

from datetime import datetime

import numpy as np
import pandas as pd

from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.time.conversion import (
    TIMESTAMP_UNIT,
    detect_timestamp_unit,
    enforce_utc_timezone,
)

__all__ = [
    "TimeseriesDataProcessor",
]


class TimeseriesDataProcessor:
    """Unified processor for handling time series data with consistent timestamp handling.

    This class centralizes the timestamp handling logic to ensure consistent
    behavior across different APIs (REST, Vision) and different timestamp formats
    (milliseconds vs microseconds).
    """

    @staticmethod
    def detect_timestamp_unit(sample_ts: int | str) -> str:
        """Detect timestamp unit based on number of digits.

        Args:
            sample_ts: Sample timestamp value

        Returns:
            "us" for microseconds (16 digits)
            "ms" for milliseconds (13 digits)

        Raises:
            ValueError: If timestamp format is not recognized
        """
        # Use the global function for consistency
        return detect_timestamp_unit(sample_ts)

    @classmethod
    def process_kline_data(cls, raw_data: list[list], columns: list[str]) -> pd.DataFrame:
        """Process raw kline data into a standardized DataFrame.

        This method handles different timestamp formats consistently:
        - Automatically detects millisecond (13 digits) vs microsecond (16 digits) precision
        - Properly handles timezone awareness
        - Normalizes numeric columns
        - Removes duplicates and ensures chronological ordering
        - Preserves exact timestamps from raw data without any shifting

        Args:
            raw_data: List of kline data from an API
            columns: Column names for the data

        Returns:
            Processed DataFrame with standardized index and columns
        """
        if not raw_data:
            return pd.DataFrame()

        # Use provided column definitions
        df = pd.DataFrame(raw_data, columns=pd.Index(columns))

        # Detect timestamp unit from the first row
        timestamp_unit = TIMESTAMP_UNIT  # Default from config
        timestamp_multiplier = 1000  # Default milliseconds to microseconds conversion

        # Detect format based on open_time (first element in kline data)
        if len(raw_data) > 0:
            try:
                sample_ts = raw_data[0][0]  # First element is open_time
                logger.debug(f"Sample open_time: {sample_ts}")
                logger.debug(f"Number of digits: {len(str(int(sample_ts)))}")

                # Use the timestamp format detector
                detected_unit = cls.detect_timestamp_unit(sample_ts)

                if detected_unit == "us":
                    # Already in microseconds, no conversion needed
                    timestamp_unit = "us"
                    timestamp_multiplier = 1
                    logger.debug("Detected microsecond precision (16 digits)")
                else:
                    # Convert from milliseconds to microseconds
                    timestamp_unit = "us"
                    timestamp_multiplier = 1000
                    logger.debug("Detected millisecond precision (13 digits)")

            except (ValueError, TypeError) as e:
                logger.warning(f"Error detecting timestamp format: {e}")

        # Convert timestamps with appropriate precision
        for col in ["open_time", "close_time"]:
            if col in df.columns:
                # Convert to int64 first to ensure full precision
                df[col] = df[col].astype(np.int64)

                # Apply the detected multiplier
                if timestamp_multiplier > 1:
                    df[col] = df[col] * timestamp_multiplier

                # Convert to datetime with the appropriate unit
                df[col] = pd.to_datetime(df[col], unit=timestamp_unit, utc=True)

                # Log the exact timestamp values
                if len(raw_data) > 0:
                    logger.debug(f"Converted {col}: {df[col].iloc[0]}")
                    logger.debug(f"{col} microseconds: {df[col].iloc[0].microsecond}")

        # Convert numeric columns efficiently
        numeric_cols = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_asset_volume",
            "taker_buy_volume",
            "taker_buy_quote_volume",
        ]

        # Only convert columns that exist
        numeric_cols = [col for col in numeric_cols if col in df.columns]
        if numeric_cols:
            df[numeric_cols] = df[numeric_cols].astype(np.float64)

        # Convert count if it exists
        if "count" in df.columns:
            df["count"] = df["count"].astype(np.int32)

        # Check for duplicate timestamps and sort by open_time
        if "open_time" in df.columns:
            logger.debug(f"Shape before sorting and deduplication: {df.shape}")

            # First, sort by open_time to ensure chronological order
            df = df.sort_values("open_time")

            # Then check for duplicates and drop them if necessary
            if df.duplicated(subset=["open_time"]).any():
                duplicates_count = df.duplicated(subset=["open_time"]).sum()
                logger.debug(f"Found {duplicates_count} duplicate timestamps, keeping first occurrence")
                df = df.drop_duplicates(subset=["open_time"], keep="first")

            # Set the index to open_time for consistent behavior
            df = df.set_index("open_time")

            # If close_time doesn't exist in the raw data, we need to calculate it
            # but we should clearly log that we are doing this modification
            if "close_time" not in df.columns and "close_time" not in columns:
                logger.debug("close_time not in raw data, calculating it from interval")
                # Generate close_time based on the interval detected from consecutive timestamps
                if len(df) > 1:
                    # Estimate interval from first two timestamps
                    first_two_indices = df.index[:2]
                    estimated_interval = (first_two_indices[1] - first_two_indices[0]).total_seconds()
                    logger.debug(f"Estimated interval from timestamps: {estimated_interval} seconds")
                    # Calculate close_time based on this interval
                    df["close_time"] = df.index + pd.Timedelta(seconds=estimated_interval) - pd.Timedelta(microseconds=1)
                    logger.debug(f"Generated close_time using estimated interval: first value = {df['close_time'].iloc[0]}")
                else:
                    # Default fallback to 1 second if we only have one timestamp
                    logger.debug("Only one timestamp, using default 1-second interval for close_time")
                    df["close_time"] = df.index + pd.Timedelta(seconds=1) - pd.Timedelta(microseconds=1)

        logger.debug(f"Final DataFrame shape: {df.shape}")
        return df

    @classmethod
    def standardize_dataframe(cls, df: pd.DataFrame, canonical_index_name: str = "open_time") -> pd.DataFrame:
        """Standardize a DataFrame to ensure consistent structure.

        Args:
            df: DataFrame to standardize
            canonical_index_name: Name to use for the index

        Returns:
            Standardized DataFrame
        """
        if df.empty:
            return df

        # Ensure index has the canonical name
        if df.index.name != canonical_index_name:
            df.index.name = canonical_index_name

        # Ensure index is sorted
        if not df.index.is_monotonic_increasing:
            df = df.sort_index()

        # Remove duplicates from index if any exist
        if df.index.has_duplicates:
            df = df[~df.index.duplicated(keep="first")]

        # Ensure all datetimes are timezone-aware UTC
        if isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.DatetimeIndex(
                [enforce_utc_timezone(dt) for dt in df.index.to_pydatetime()],
                name=df.index.name,
            )

        # Also fix close_time if it exists
        if "close_time" in df.columns and isinstance(df["close_time"].iloc[0], datetime):
            df["close_time"] = df["close_time"].apply(enforce_utc_timezone)

        return df
