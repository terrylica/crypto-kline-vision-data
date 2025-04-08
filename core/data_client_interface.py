#!/usr/bin/env python
"""Abstract base class for data clients that all provider-specific clients must implement."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Tuple, Union
import pandas as pd

from utils.market_constraints import MarketType, ChartType, Interval, DataProvider


class DataClientInterface(ABC):
    """Abstract base class for data clients that all provider-specific clients must implement."""

    @property
    @abstractmethod
    def provider(self) -> DataProvider:
        """Get the data provider for this client."""

    @property
    @abstractmethod
    def market_type(self) -> MarketType:
        """Get the market type for this client."""

    @property
    @abstractmethod
    def chart_type(self) -> ChartType:
        """Get the chart type for this client."""

    @property
    @abstractmethod
    def symbol(self) -> str:
        """Get the trading symbol for this client."""

    @property
    @abstractmethod
    def interval(self) -> Union[str, Interval]:
        """Get the interval for this client."""

    @abstractmethod
    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch data for the configured parameters.

        Args:
            start_time: Start time
            end_time: End time
            **kwargs: Additional provider-specific parameters

        Returns:
            DataFrame with market data
        """

    @abstractmethod
    async def is_data_available(self, start_time: datetime, end_time: datetime) -> bool:
        """Check if data is available for the specified time range.

        Args:
            start_time: Start time
            end_time: End time

        Returns:
            True if data is available, False otherwise
        """

    @abstractmethod
    def create_empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct structure for this data type.

        Returns:
            Empty DataFrame with correct columns and types
        """

    @abstractmethod
    async def validate_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """Validate that a DataFrame contains valid data for this data type.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, error_message)
        """

    @abstractmethod
    async def __aenter__(self):
        """Async context manager entry."""

    @abstractmethod
    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        """Async context manager exit."""
