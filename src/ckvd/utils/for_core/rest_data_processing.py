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

from ckvd.utils.config import OUTPUT_DTYPES

# Raw Binance kline column names as received from the API (12 columns including "ignore")
# MEMORY OPTIMIZATION (Round 8): Module-level constant avoids list recreation per REST batch parse
_RAW_KLINE_COLUMNS: list[str] = [
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

# Define the column names as a constant for REST API output
# MEMORY OPTIMIZATION (Round 9): Module-level constant avoids dict recreation per standardize_column_names() call.
_COLUMN_MAPPING: dict[str, str] = {
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
    # Rename columns that need standardization — batch into single rename()
    # call to avoid creating a new DataFrame per column (memory efficiency)
    rename_dict = {col: _COLUMN_MAPPING[col.lower()] for col in df.columns if col.lower() in _COLUMN_MAPPING}
    if rename_dict:
        df = df.rename(columns=rename_dict)

    return df


def _process_kline_data_polars(raw_data: list[list]) -> pl.DataFrame:
    """Process raw kline data using Polars (internal).

    Args:
        raw_data: Raw kline data from the API

    Returns:
        Polars DataFrame with processed data
    """
    # Columnar construction: transpose rows to columns via zip(*), avoiding Polars'
    # internal row-to-column transposition overhead.
    columns = list(zip(*raw_data, strict=False))
    col_dict = dict(zip(_RAW_KLINE_COLUMNS, columns, strict=False))
    del col_dict["ignore"]

    return (
        pl.DataFrame(col_dict)
        .with_columns(
            [
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
            ]
        )
        # Rename columns in a single operation (standardization)
        .rename(
            {
                "number_of_trades": "count",
                "taker_buy_base_asset_volume": "taker_buy_volume",
                "taker_buy_quote_asset_volume": "taker_buy_quote_volume",
            }
        )
    )


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


def _build_empty_rest_dataframe() -> pd.DataFrame:
    """Build a typed empty DataFrame with REST output structure (internal, called once)."""
    df = pd.DataFrame(columns=REST_OUTPUT_COLUMNS)
    dtypes_to_apply = {col: dtype for col, dtype in OUTPUT_DTYPES.items() if col in df.columns}
    if dtypes_to_apply:
        df = df.astype(dtypes_to_apply)
    return df


# MEMORY OPTIMIZATION (Round 9): Singleton avoids rebuilding identical empty DataFrame per error-path call.
_EMPTY_REST_DATAFRAME: pd.DataFrame = _build_empty_rest_dataframe()


def create_empty_dataframe() -> pd.DataFrame:
    """Create an empty DataFrame with the correct structure for REST data.

    Returns:
        Empty DataFrame (copy of cached singleton to prevent mutation)
    """
    return _EMPTY_REST_DATAFRAME.copy()
