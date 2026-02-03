#!/usr/bin/env python
# Memory optimization: Uses Polars internally for efficient processing
# Public API returns pandas DataFrames for backward compatibility
# ADR: docs/adr/2026-01-30-claude-code-infrastructure.md
"""Utilities for processing REST API data responses.

This module provides common utilities for processing data from REST API responses including:
1. Data standardization and column mapping
2. Data type conversion and validation
3. DataFrame creation and manipulation

Internally uses Polars for efficient processing, converts to pandas at API boundary.
"""

import pandas as pd
import polars as pl

from data_source_manager.utils.config import OUTPUT_DTYPES

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


def _process_kline_data_polars(raw_data: list[list]) -> pl.DataFrame:
    """Process raw kline data using Polars (internal).

    Args:
        raw_data: Raw kline data from the API

    Returns:
        Polars DataFrame with processed data
    """
    # Define column names
    columns = [
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

    # Create Polars DataFrame - all processing in a single expression chain
    df = (
        pl.DataFrame(raw_data, schema=columns, orient="row")
        .drop("ignore")
        .with_columns([
            # Convert milliseconds to datetime
            pl.col("open_time").cast(pl.Int64).cast(pl.Datetime("ms", "UTC")),
            pl.col("close_time").cast(pl.Int64).cast(pl.Datetime("ms", "UTC")),
            # Convert strings to floats
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.col("quote_asset_volume").cast(pl.Float64),
            pl.col("taker_buy_base_asset_volume").cast(pl.Float64),
            pl.col("taker_buy_quote_asset_volume").cast(pl.Float64),
            # Convert number of trades to integer
            pl.col("number_of_trades").cast(pl.Int64),
        ])
        # Rename columns in a single operation (standardization)
        .rename({
            "number_of_trades": "count",
            "taker_buy_base_asset_volume": "taker_buy_volume",
            "taker_buy_quote_asset_volume": "taker_buy_quote_volume",
        })
    )

    return df


def process_kline_data(raw_data: list[list]) -> pd.DataFrame:
    """Process raw kline data into a structured DataFrame.

    Args:
        raw_data: Raw kline data from the API

    Returns:
        Processed DataFrame with standardized columns
    """
    # Use Polars internally for efficient processing
    df_pl = _process_kline_data_polars(raw_data)

    # Convert to pandas at API boundary
    df = df_pl.to_pandas()

    # Ensure open_time is timezone-aware (Polars to_pandas may lose tz info)
    if "open_time" in df.columns and df["open_time"].dt.tz is None:
        df["open_time"] = df["open_time"].dt.tz_localize("UTC")
    if "close_time" in df.columns and df["close_time"].dt.tz is None:
        df["close_time"] = df["close_time"].dt.tz_localize("UTC")

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
