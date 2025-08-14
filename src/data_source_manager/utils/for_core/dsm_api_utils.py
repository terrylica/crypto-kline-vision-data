#!/usr/bin/env python
"""Utility functions for DataSourceManager API operations."""

import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd

from data_source_manager.utils.config import VISION_DATA_DELAY_HOURS, create_empty_dataframe
from data_source_manager.utils.for_core.vision_constraints import is_date_too_fresh_for_vision
from data_source_manager.utils.loguru_setup import logger
from data_source_manager.utils.market_constraints import ChartType, Interval, MarketType
from data_source_manager.utils.time_utils import align_time_boundaries, filter_dataframe_by_time


def fetch_from_vision(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
    vision_client,
    chart_type: ChartType,
    use_cache: bool,
    save_to_cache_func=None,
    fs_handler=None,
) -> pd.DataFrame:
    """Fetch data from the Vision API.

    Args:
        symbol: Symbol to retrieve data for
        start_time: Start time for data retrieval
        end_time: End time for data retrieval
        interval: Time interval between data points
        vision_client: VisionDataClient instance
        chart_type: Type of chart data
        use_cache: Whether to use caching
        save_to_cache_func: Function to save data to cache
        fs_handler: FSSpecVisionHandler instance (optional)

    Returns:
        DataFrame with data from Vision API filtered to the requested time range

    Note:
        As a core part of the FCP mechanism, this method implements the Daily Pack Caching requirement:
        1. Regardless of the requested time range (start_time to end_time), the method expands
           the request to fetch full days of data from Vision API
        2. The complete daily data is cached to ensure consistent and complete availability
        3. Only the originally requested time range is returned to the caller

        This ensures that even if a request specifies a start time at the beginning, middle, or end
        of the day, the entire day's data is cached for future use.
    """
    logger.info(f"Fetching data from Vision API for {symbol} from {start_time} to {end_time}")

    try:
        # Get aligned boundaries to ensure complete data
        aligned_start, aligned_end = align_time_boundaries(start_time, end_time, interval)

        # For the FCP mechanism, we need to ensure that the full days' data is downloaded
        # and cached from Vision API, even if only a partial day is requested

        # Calculate full-day boundaries for Vision API data retrieval
        # Start from the beginning of the day for start_time
        vision_start = aligned_start.replace(hour=0, minute=0, second=0, microsecond=0)
        # End at the end of the day for end_time
        vision_end = aligned_end.replace(hour=23, minute=59, second=59, microsecond=999999)

        logger.debug(f"[FCP] Expanding Vision API request to full days: {vision_start} to {vision_end}")

        # If we have an FSSpecVisionHandler, use it for direct path mapping
        if fs_handler is not None:
            logger.debug("Using FSSpecVisionHandler for consistency in path mapping")
            # This indicates we can directly use the fs_handler for path operations
            # Note: The actual implementation would handle the Vision API downloads

        # Vision API has date-based files, fetch with chunking
        df = vision_client.fetch(
            symbol=symbol,
            interval=interval.value,
            start_time=vision_start,
            end_time=vision_end,
            chart_type=chart_type,
        )

        if df is not None and not df.empty:
            # Add debugging information about dataframe
            logger.debug(f"Vision API returned DataFrame with shape: {df.shape}")
            if hasattr(df, "index") and df.index is not None:
                logger.debug(f"DataFrame index name: {df.index.name}, type: {type(df.index).__name__}")

            # Add source information
            df["_data_source"] = "VISION"

            # Save the entire day's data to cache before filtering to the requested range
            if use_cache and save_to_cache_func is not None:
                logger.debug("[FCP] Caching full day's data from Vision API")
                save_to_cache_func(df, symbol, interval, source="VISION")

            # Filter the dataframe to the originally requested time range
            logger.debug(f"[FCP] Filtering Vision API data to originally requested range: {aligned_start} to {aligned_end}")
            filtered_df = filter_dataframe_by_time(df, aligned_start, aligned_end, "open_time")

            # Help with debugging
            logger.info(f"Retrieved {len(filtered_df)} records from Vision API (after filtering to requested range)")

            return filtered_df
        logger.info(f"Vision API returned no data for {symbol}")
        # Check if end_time is within the Vision API delay window using our centralized function
        if is_date_too_fresh_for_vision(end_time):
            logger.info(
                f"No data returned from Vision API - end_time {end_time.isoformat()} "
                f"is within the {VISION_DATA_DELAY_HOURS}h delay window. "
                f"This is expected for recent data. Trying REST API."
            )
        else:
            logger.info(
                f"No data returned from Vision API for {symbol} despite being outside "
                f"the {VISION_DATA_DELAY_HOURS}h delay window. "
                f"This is unexpected for historical data. Trying REST API as fallback."
            )
        return create_empty_dataframe()

    except Exception as e:
        # Sanitize error message to prevent binary data from causing rich formatting issues
        try:
            error_message = str(e)
            # Replace any non-printable characters to prevent rich markup errors
            safe_error_message = "".join(c if c.isprintable() else f"\\x{ord(c):02x}" for c in error_message)

            # Check if this is a critical error that should be propagated
            if "CRITICAL ERROR" in safe_error_message or "DATA INTEGRITY ERROR" in safe_error_message:
                logger.critical(f"Vision API critical error: {safe_error_message}")
                raise  # Re-raise to trigger failover

            # Check if the request is within the allowed delay window for Vision API using our centralized function
            if is_date_too_fresh_for_vision(end_time):
                # This falls within the allowable delay window for Vision API
                logger.warning(
                    f"Error fetching recent data from Vision API (within {VISION_DATA_DELAY_HOURS}h delay window): {safe_error_message}"
                )
                return create_empty_dataframe()

            # For historical data outside the delay window, log critical error
            logger.critical(f"Vision API failed to retrieve historical data: {safe_error_message}")
            logger.critical(f"Error type: {type(e).__name__}")

            # More controlled traceback handling
            tb_string = traceback.format_exc()
            # Sanitize the traceback too
            safe_tb = "".join(c if c.isprintable() else f"\\x{ord(c):02x}" for c in tb_string)
            tb_lines = safe_tb.splitlines()

            logger.critical("Traceback summary:")
            for line in tb_lines[:3]:  # Just log first few lines
                logger.critical(line)
            logger.critical("...")
            for line in tb_lines[-3:]:  # And last few lines
                logger.critical(line)

            # Propagate the error to trigger failover
            raise RuntimeError(f"CRITICAL: Vision API failed to retrieve historical data: {safe_error_message}")

        except Exception as nested_error:
            # If even our error handling fails, log a simpler message
            logger.critical(f"Vision API error occurred (details unavailable): {type(e).__name__}")
            logger.critical(f"Error handling also failed: {type(nested_error).__name__}")

            # Propagate the error to trigger failover
            raise RuntimeError("CRITICAL: Vision API error could not be handled properly") from e


def fetch_from_rest(
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    interval: Interval,
    rest_client,
    chart_type: ChartType,
) -> pd.DataFrame:
    """Fetch data from REST API with chunking.

    Args:
        symbol: Symbol to retrieve data for
        start_time: Start time for data retrieval
        end_time: End time for data retrieval
        interval: Time interval between data points
        rest_client: RestDataClient instance
        chart_type: Type of chart data

    Returns:
        DataFrame with data from REST API

    Raises:
        RuntimeError: When REST API fails to retrieve data. As this is the final
                      data source in the FCP chain, failures here represent
                      complete failure of all data sources.
    """
    logger.info(f"Fetching data from REST API for {symbol} from {start_time} to {end_time}")

    try:
        # Get aligned boundaries to ensure complete data
        aligned_start, aligned_end = align_time_boundaries(start_time, end_time, interval)
        logger.debug(f"Complete data range after alignment: {aligned_start} to {aligned_end}")

        # REST API has limits, so get data with chunking
        df = rest_client.fetch(
            symbol=symbol,
            interval=interval.value,
            start_time=aligned_start,
            end_time=aligned_end,
            chart_type=chart_type,
        )

        if df.empty:
            logger.critical(f"REST API returned no data for {symbol}")
            raise RuntimeError(f"CRITICAL: REST API returned no data for {symbol}")

        # Add source information
        df["_data_source"] = "REST"

        # Help with debugging
        logger.info(f"Retrieved {len(df)} records from REST API")

        return df
    except Exception as e:
        # Sanitize error message to prevent binary data from causing rich formatting issues
        try:
            error_message = str(e)
            # Replace any non-printable characters to prevent rich markup errors
            safe_error_message = "".join(c if c.isprintable() else f"\\x{ord(c):02x}" for c in error_message)

            logger.critical(f"Error in fetch_from_rest: {safe_error_message}")
            logger.critical(f"Error type: {type(e).__name__}")

            # More controlled traceback handling
            tb_string = traceback.format_exc()
            # Sanitize the traceback
            safe_tb = "".join(c if c.isprintable() else f"\\x{ord(c):02x}" for c in tb_string)
            tb_lines = safe_tb.splitlines()

            logger.critical("Traceback summary:")
            for line in tb_lines[:3]:  # Just log first few lines to avoid binary data
                logger.critical(line)
            logger.critical("...")
            for line in tb_lines[-3:]:  # And last few lines
                logger.critical(line)

            # This is the final fallback in the FCP chain, so raise an error
            # to indicate complete failure of all sources
            raise RuntimeError(f"CRITICAL: REST API fallback failed: {safe_error_message}")

        except Exception as nested_error:
            # If even our error handling fails, log a simpler message
            logger.critical(f"REST API critical error: {type(e).__name__}")
            logger.critical(f"Error handling also failed: {type(nested_error).__name__}")

            # Propagate the error
            raise RuntimeError("CRITICAL: REST API error could not be handled properly") from e


def create_client_if_needed(
    client,
    client_class,
    symbol: str | None = None,
    interval: str | None = None,
    market_type: MarketType | None = None,
    retry_count: int | None = None,
    chart_type: ChartType | None = None,
    cache_dir: Path | None = None,
):
    """Create or reconfigure client if needed.

    Args:
        client: Existing client to reuse or reconfigure
        client_class: Client class to instantiate
        symbol: Symbol for the client
        interval: Interval for the client
        market_type: Market type for the client
        retry_count: Number of retries for the client
        chart_type: Chart type for the client
        cache_dir: Cache directory for the client

    Returns:
        New or reconfigured client
    """
    # Check if this is a RestDataClient
    if client_class.__name__ == "RestDataClient":
        logger.debug(f"Setting up {client_class.__name__} with market_type={market_type}, retry_count={retry_count}")

        # Only create a new client if needed
        if client is None or not isinstance(client, client_class):
            # Rest client needs at least market_type
            if market_type is None:
                raise ValueError("market_type is required for creating a new RestDataClient")

            # Pass retry_count if provided
            kwargs = {}
            if retry_count is not None:
                kwargs["retry_count"] = retry_count

            return client_class(market_type=market_type, **kwargs)
        # Already have a client, check if it needs reconfiguration
        if market_type is not None and client.market_type != market_type:
            logger.debug(f"Reconfiguring RestDataClient with market_type={market_type}")
            # Need a new client for different market type
            kwargs = {}
            if retry_count is not None:
                kwargs["retry_count"] = retry_count
            return client_class(market_type=market_type, **kwargs)
        # Same market type, can reuse
        return client

    # Check if this is a VisionDataClient
    if client_class.__name__ == "VisionDataClient":
        logger.debug(
            f"Setting up {client_class.__name__} with symbol={symbol}, interval={interval}, "
            f"market_type={market_type}, chart_type={chart_type}"
        )

        # Vision client needs symbol, interval, and market_type
        if symbol is None or interval is None or market_type is None:
            raise ValueError("symbol, interval, and market_type are required for creating a new VisionDataClient")

        # Only create a new client if needed
        if client is None or not isinstance(client, client_class):
            # Create new client with required params
            kwargs = {}
            if chart_type is not None:
                kwargs["chart_type"] = chart_type
            if cache_dir is not None:
                kwargs["cache_dir"] = cache_dir

            return client_class(symbol=symbol, interval=interval, market_type=market_type, **kwargs)
        # Already have a client, check if it needs reconfiguration
        if client.symbol != symbol or client.interval != interval or client.market_type_str != str(market_type).lower():
            logger.debug(
                f"Reconfiguring VisionDataClient with new parameters: symbol={symbol}, interval={interval}, market_type={market_type}"
            )
            # Need a new client for different parameters
            kwargs = {}
            if chart_type is not None:
                kwargs["chart_type"] = chart_type
            if cache_dir is not None:
                kwargs["cache_dir"] = cache_dir

            return client_class(symbol=symbol, interval=interval, market_type=market_type, **kwargs)
        # Same parameters, can reuse
        return client
    # Unknown client type
    raise ValueError(f"Unsupported client class: {client_class.__name__}")
