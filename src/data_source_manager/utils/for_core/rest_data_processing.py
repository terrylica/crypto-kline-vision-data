#!/usr/bin/env python
"""Utilities for processing REST API data responses.

This module provides common utilities for processing data from REST API responses including:
1. Data standardization and column mapping
2. Data type conversion and validation
3. DataFrame creation and manipulation
"""

import pandas as pd

from data_source_manager.utils.config import OUTPUT_DTYPES
from data_source_manager.utils.loguru_setup import logger

# Define the column names as a constant for REST API output
REST_OUTPUT_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
]


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to ensure consistent naming.

    Args:
        df: DataFrame to standardize

    Returns:
        DataFrame with standardized column names
    """
    # Define mappings for column name standardization
    column_mapping = {
        # Quote volume variants
        "quote_volume": "quote_asset_volume",
        "quote_vol": "quote_asset_volume",
        # Trade count variants
        "trades": "count",
        "number_of_trades": "count",
        # Taker buy base volume variants
        "taker_buy_base": "taker_buy_volume",
        "taker_buy_base_volume": "taker_buy_volume",
        "taker_buy_base_asset_volume": "taker_buy_volume",
        # Taker buy quote volume variants
        "taker_buy_quote": "taker_buy_quote_volume",
        "taker_buy_quote_asset_volume": "taker_buy_quote_volume",
        # Time field variants
        "time": "open_time",
        "timestamp": "open_time",
        "date": "open_time",
    }

    # Rename columns that need standardization
    for col in df.columns:
        if col.lower() in column_mapping:
            df = df.rename(columns={col: column_mapping[col.lower()]})

    return df


def process_kline_data(raw_data: list[list]) -> pd.DataFrame:
    """Process raw kline data into a structured DataFrame.

    Args:
        raw_data: Raw kline data from the API

    Returns:
        Processed DataFrame with standardized columns
    """
    # Create DataFrame from raw data
    df = pd.DataFrame(
        raw_data,
        columns=[
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
        ],
    )

    # Convert times to datetime
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    # Convert strings to floats
    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]:
        df[col] = df[col].astype(float)

    # Convert number of trades to integer
    df["number_of_trades"] = df["number_of_trades"].astype(int)

    # Drop the ignore column
    df = df.drop(columns=["ignore"])

    # Add extended columns based on existing data
    df = standardize_column_names(df)

    # Ensure we consistently return a DataFrame with open_time as a column, never as an index
    # This prevents downstream ambiguity
    if hasattr(df, "index") and hasattr(df.index, "name") and df.index.name == "open_time":
        logger.debug("Resetting index to ensure open_time is a column, not an index")
        df = df.reset_index()

    # Ensure there's only one open_time (column takes precedence over index)
    if hasattr(df, "index") and hasattr(df.index, "name") and df.index.name == "open_time" and "open_time" in df.columns:
        logger.debug("Resolving ambiguous open_time by keeping only the column version")
        df = df.reset_index(drop=True)

    return df


def create_empty_dataframe() -> pd.DataFrame:
    """Create an empty DataFrame with the correct structure for REST data.

    Returns:
        Empty DataFrame
    """
    # Create an empty DataFrame with the right columns and types
    df = pd.DataFrame(columns=REST_OUTPUT_COLUMNS)
    for col, dtype in OUTPUT_DTYPES.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    return df
