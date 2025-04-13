#!/usr/bin/env python
"""Interface for data clients."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Tuple, Union
import pandas as pd

from utils.market_constraints import DataProvider, ChartType


class DataClientInterface(ABC):
    """Interface for data clients to implement.

    This abstract base class defines the common interface that all data clients
    must implement to be used with the DataSourceManager.

    Implementation Guidelines:
    1. All concrete implementations should properly validate parameters
    2. Error handling should follow consistent patterns across implementations
    3. Return types should maintain consistent structure (column names, types)
    4. Method names should follow the interface exactly as defined
    5. Optional parameters should be handled gracefully with sensible defaults
    """

    @property
    @abstractmethod
    def provider(self) -> DataProvider:
        """Get the data provider.

        Returns:
            The data provider (e.g., BINANCE)
        """

    @property
    @abstractmethod
    def chart_type(self) -> ChartType:
        """Get the chart type.

        Returns:
            The chart type (e.g., KLINES, FUNDING_RATE)
        """

    @property
    @abstractmethod
    def symbol(self) -> str:
        """Get the symbol.

        Returns:
            The trading pair symbol
        """

    @property
    @abstractmethod
    def interval(self) -> Union[str, object]:
        """Get the interval.

        Returns:
            The time interval as a string or compatible object with string representation
        """

    @abstractmethod
    def fetch(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch data from the data source.

        All implementations should validate input parameters and provide
        appropriate error handling for invalid inputs. If the implementation
        allows for default values (e.g., using instance properties when
        parameters are empty), this should be clearly documented.

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Time interval as string (e.g., "1m", "1h")
            start_time: Start time as timezone-aware datetime
            end_time: End time as timezone-aware datetime
            **kwargs: Additional parameters specific to the implementation

        Returns:
            DataFrame with the fetched data, properly formatted according to
            the chart type (klines, funding rate, etc.)

        Raises:
            ValueError: If input parameters are invalid
            RuntimeError: If data cannot be fetched due to service issues
        """

    @abstractmethod
    def is_data_available(self, start_time: datetime, end_time: datetime) -> bool:
        """Check if data is available for the specified time range.

        This method is used to determine if the specific data source can
        provide data for the given time range. It should consider factors
        such as exchange launch date, recent data availability, and any
        other source-specific constraints.

        Args:
            start_time: Start time as timezone-aware datetime
            end_time: End time as timezone-aware datetime

        Returns:
            True if data is available, False otherwise
        """

    @abstractmethod
    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct structure.

        This method should return an empty DataFrame with the proper column
        structure, data types, and index configuration appropriate for the
        specific chart type (klines, funding rate, etc.).

        Returns:
            Empty DataFrame with the correct structure
        """

    @abstractmethod
    def validate_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate that a DataFrame contains valid data.

        Implementations should check for structural correctness, data types,
        required columns, and any chart-type specific validations. This may
        use shared validation utilities to maintain consistency.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, error_message)
        """

    @abstractmethod
    def close(self) -> None:
        """Close the client and release resources.

        This method should properly clean up any open connections or resources
        to prevent memory leaks or resource exhaustion.
        """
