#!/usr/bin/env python
from utils.logger_setup import get_logger

logger = get_logger(__name__, "INFO", show_path=False, rich_tracebacks=True)

import re
from datetime import datetime, timezone, timedelta
import aiohttp
import pandas as pd
from .market_constraints import (
    MarketType,
    Interval,
    get_market_capabilities,
    is_interval_supported,
    get_endpoint_url,
    get_minimum_interval,
)


class DataValidation:
    """Validation utilities for Binance data loading."""

    @classmethod
    def validate_interval(cls, interval: str, market_type: str = "spot") -> None:
        """Validate the interval parameter for a specific market type."""
        try:
            market = MarketType[market_type.upper()]
            interval_enum = Interval(interval)
            if not is_interval_supported(market, interval_enum):
                capabilities = get_market_capabilities(market)
                min_interval = get_minimum_interval(market).value
                supported = sorted(i.value for i in capabilities.supported_intervals)
                raise ValueError(
                    f"Invalid interval '{interval}' for {market.name}. "
                    f"Minimum interval: {min_interval}. "
                    f"Supported intervals: {', '.join(supported)}"
                )
        except (KeyError, ValueError) as e:
            if isinstance(e, KeyError) or "MarketType" in str(e):
                valid_types = [m.name for m in MarketType]
                raise ValueError(
                    f"Invalid market type: {market_type}. Must be one of: {', '.join(valid_types)}"
                )
            raise

    @classmethod
    def validate_market_type(cls, market_type: str) -> None:
        """Validate the market type parameter."""
        try:
            MarketType[market_type.upper()]
        except KeyError:
            valid_types = [m.name for m in MarketType]
            raise ValueError(
                f"Invalid market type: {market_type}. Must be one of: {', '.join(valid_types)}"
            )

    @classmethod
    def validate_symbol_format(cls, symbol: str, market_type: str = "spot") -> None:
        """Validate the symbol format for spot market."""
        try:
            market = MarketType[market_type.upper()]
            capabilities = get_market_capabilities(market)

            pattern = re.compile(r"^[A-Z0-9]{3,}$")
            if not pattern.match(symbol):
                raise ValueError(
                    f"Invalid symbol format for {market.name}: {symbol}. "
                    f"Must follow format: {capabilities.symbol_format}"
                )
        except KeyError:
            valid_types = [m.name for m in MarketType]
            raise ValueError(
                f"Invalid market type: {market_type}. Must be one of: {', '.join(valid_types)}"
            )

    @classmethod
    def validate_dates(cls, start_date: datetime, end_date: datetime) -> None:
        """Validate date parameters."""
        if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
            raise ValueError("Dates must be datetime objects")

        if start_date.tzinfo is None or end_date.tzinfo is None:
            raise ValueError("Dates must have timezone")

        if start_date >= end_date:
            raise ValueError(
                f"Start time ({start_date}) is after end time ({end_date})"
            )

        max_days = 1000
        if (datetime.now(timezone.utc) - start_date).days > max_days:
            raise ValueError("Start date cannot be more than 1000 days old")

    @classmethod
    async def validate_symbol_exists(
        cls, session: aiohttp.ClientSession, symbol: str, market_type: str = "spot"
    ) -> None:
        """Validate that the symbol exists by attempting to fetch a single kline."""
        try:
            market = MarketType[market_type.upper()]
            endpoint_url = get_endpoint_url(market)
        except KeyError:
            valid_types = [m.name for m in MarketType]
            raise ValueError(
                f"Invalid market type: {market_type}. Must be one of: {', '.join(valid_types)}"
            )

        # Check if symbol exists by attempting to fetch a single kline
        now = datetime.now(timezone.utc)
        start_ts = int((now - timedelta(minutes=1)).timestamp() * 1000)
        end_ts = int(now.timestamp() * 1000)

        params = {
            "symbol": symbol,
            "interval": get_minimum_interval(market).value,
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": 1,
        }

        try:
            async with session.get(endpoint_url, params=params) as response:
                response.raise_for_status()
        except aiohttp.ClientError as e:
            if "400" in str(e):  # Bad Request usually means invalid symbol
                raise ValueError(f"Symbol not found: {symbol}")
            raise  # Re-raise other errors

    @staticmethod
    def validate_time_window(start_time: datetime, end_time: datetime) -> None:
        """Validate time window for data retrieval.

        Args:
            start_time: Start time of the window
            end_time: End time of the window

        Raises:
            ValueError: If time window is invalid
        """
        # Ensure times are in UTC
        if start_time.tzinfo is None or end_time.tzinfo is None:
            raise ValueError("Timestamps must be timezone-aware (UTC)")

        # Check time window order
        if start_time >= end_time:
            raise ValueError(
                f"Start time ({start_time}) is after end time ({end_time})"
            )

        # Validate that end time is not in the future
        current_time = datetime.now(timezone.utc)
        if end_time > current_time:
            raise ValueError(f"End time ({end_time}) is in the future")

        # Check reasonable time range (e.g., not too far in the past)
        max_days_back = 1000  # Binance typically keeps data for about 1000 days
        if (datetime.now(timezone.utc) - start_time).days > max_days_back:
            raise ValueError(
                f"Start time cannot be more than {max_days_back} days in the past"
            )

        # Check minimum window size
        if (end_time - start_time).total_seconds() < 1:
            raise ValueError("Time window must be at least 1 second")


class DataFrameValidator:
    """Centralized validator for DataFrame structure and content.

    This class consolidates validation logic that was previously scattered across multiple files,
    providing a unified interface for verifying DataFrame integrity in terms of:
    1. Structure (columns, types, index requirements)
    2. Content (value ranges, relationships between columns)
    3. Time-related properties (monotonicity, completeness)
    """

    # Standard output format specification reused across components
    STANDARD_DTYPES = {
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "float64",
        "close_time": "int64",
        "quote_volume": "float64",
        "trades": "int64",
        "taker_buy_volume": "float64",
        "taker_buy_quote_volume": "float64",
    }

    @classmethod
    def validate_structure(cls, df: pd.DataFrame, allow_empty: bool = True) -> None:
        """Validate DataFrame structure (columns, types, index).

        Args:
            df: DataFrame to validate
            allow_empty: Whether empty DataFrames are valid

        Raises:
            ValueError: If DataFrame structure is invalid
        """
        if df is None:
            raise ValueError("DataFrame is None")

        # Empty DataFrame handling
        if df.empty:
            if not allow_empty:
                raise ValueError("DataFrame is empty when non-empty data is required")

            # Check that empty DataFrame has correct columns and index
            required_columns = set(cls.STANDARD_DTYPES.keys())
            if not all(col in df.columns for col in required_columns):
                raise ValueError("Empty DataFrame missing required columns")

            if not isinstance(df.index, pd.DatetimeIndex):
                raise ValueError("DataFrame index must be DatetimeIndex")

            if df.index.tz != timezone.utc:
                raise ValueError("DataFrame index timezone must be UTC")

            return

        # Index validation
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("Index must be DatetimeIndex")

        if df.index.tz != timezone.utc:
            raise ValueError("Index timezone must be UTC")

        if df.index.name != "open_time":
            raise ValueError("Index name must be 'open_time'")

        # Column validation
        required_columns = set(cls.STANDARD_DTYPES.keys())
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Data type validation
        for col, expected_type in cls.STANDARD_DTYPES.items():
            if str(df[col].dtype) != expected_type:
                raise ValueError(
                    f"Column {col} has wrong type: {df[col].dtype}, expected {expected_type}"
                )

    @classmethod
    def validate_content(cls, df: pd.DataFrame) -> None:
        """Validate DataFrame content (value ranges, relationships).

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If DataFrame content is invalid
        """
        if df.empty:
            return

        # Value range checks
        if (df["high"] < df["low"]).any():
            raise ValueError("Found high price less than low price")

        if (df["volume"] < 0).any():
            raise ValueError("Found negative volume")

        if (df["trades"] < 0).any():
            raise ValueError("Found negative trade count")

    @classmethod
    def validate_time_properties(cls, df: pd.DataFrame) -> None:
        """Validate DataFrame time-related properties.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If DataFrame time properties are invalid
        """
        if df.empty:
            return

        # Check index is monotonically increasing (no duplicates, sorted)
        if not df.index.is_monotonic_increasing:
            raise ValueError("Index is not monotonically increasing")

        if df.index.has_duplicates:
            raise ValueError("Index contains duplicate timestamps")

    @classmethod
    def validate_dataframe(
        cls,
        df: pd.DataFrame,
        allow_empty: bool = True,
        check_structure: bool = True,
        check_content: bool = True,
        check_time: bool = True,
    ) -> None:
        """Complete DataFrame validation.

        Args:
            df: DataFrame to validate
            allow_empty: Whether empty DataFrames are valid
            check_structure: Whether to check structure
            check_content: Whether to check content
            check_time: Whether to check time properties

        Raises:
            ValueError: If DataFrame is invalid
        """
        if check_structure:
            cls.validate_structure(df, allow_empty)

        if check_content:
            cls.validate_content(df)

        if check_time:
            cls.validate_time_properties(df)
