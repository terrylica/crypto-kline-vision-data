#!/usr/bin/env python3
"""
Utility module for file handling with Binance Vision API data.

This module provides functions for downloading, processing, and handling files from
the Binance Vision API, including boundary gap filling and related operations.
"""

from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd

from core.providers.binance.rest_data_client import RestDataClient
from utils.gap_detector import Gap
from utils.logger_setup import logger
from utils.market_constraints import Interval, MarketType


def fill_boundary_gaps_with_rest(
    df: pd.DataFrame,
    boundary_gaps: List[Gap],
    symbol: str,
    interval_obj: Interval,
    market_type: MarketType,
) -> Optional[pd.DataFrame]:
    """Fill day boundary gaps using REST API data.

    This method is used to fill specific gaps that occur at day boundaries
    by fetching the missing data directly from the REST API.

    Args:
        df: DataFrame with Vision API data that has gaps
        boundary_gaps: List of Gap objects representing day boundary gaps
        symbol: Trading symbol (e.g., "BTCUSDT")
        interval_obj: Interval enum object
        market_type: Market type enum

    Returns:
        DataFrame with gaps filled, or None if filling failed
    """
    if not boundary_gaps:
        return df

    try:
        # Create a REST client with the same parameters
        rest_client = RestDataClient(
            market_type=market_type,
            symbol=symbol,
            interval=interval_obj,
        )

        # Create a list to hold the gap data we'll fetch
        gap_dfs = []
        gap_dfs.append(df)

        # For each gap, fetch the specific missing data
        for gap in boundary_gaps:
            # Add a small buffer around the gap to ensure we get the needed data
            # Use 50% of the interval duration as buffer
            interval_seconds = interval_obj.to_seconds()
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
                symbol,
                interval_obj.value,
                start_time=gap_start,
                end_time=gap_end,
            )

            if not gap_data.empty:
                # Check if we got data around midnight
                expected_midnight = gap.start_time + (gap.end_time - gap.start_time) / 2
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
                logger.warning("No data retrieved from REST API for gap")

        # If we have gap data, merge it with the original data
        if len(gap_dfs) > 1:  # More than just the original df
            # Concatenate all dataframes and remove duplicates
            merged_df = pd.concat(gap_dfs, ignore_index=True)
            merged_df = merged_df.drop_duplicates(subset=["open_time"], keep="first")
            merged_df = merged_df.sort_values("open_time").reset_index(drop=True)
            return merged_df

        # If we didn't add any gap data, return the original
        return df
    except Exception as e:
        logger.error(f"Error filling boundary gaps with REST API: {e}")
        return None


def find_day_boundary_gaps(gaps: List[Gap]) -> List[Gap]:
    """Find gaps that occur at day boundaries (midnight).

    Args:
        gaps: List of Gap objects

    Returns:
        List of Gap objects that occur at day boundaries
    """
    boundary_gaps = []
    try:
        boundary_gaps = [
            gap
            for gap in gaps
            if (
                gap.start_time.hour == 0
                and gap.start_time.minute == 0
                and gap.start_time.second == 0
            )
            or (
                gap.end_time.hour == 0
                and gap.end_time.minute == 0
                and gap.end_time.second == 0
            )
        ]
    except Exception as e:
        logger.error(f"Error checking for boundary gaps: {e}")

    return boundary_gaps
