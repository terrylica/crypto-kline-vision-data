#!/usr/bin/env python
"""Test the config module functionality."""

import pandas as pd
from datetime import timezone

from utils.config import create_empty_dataframe, CANONICAL_INDEX_NAME
from utils.market_constraints import ChartType


class TestEmptyDataFrameCreation:
    """Tests for the centralized empty DataFrame creation functions."""

    def test_create_empty_dataframe_with_klines_default(self):
        """Test creating empty KLINES DataFrame by default."""
        df = create_empty_dataframe()

        # Check basic structure
        assert isinstance(df, pd.DataFrame)
        assert df.empty
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index.name == CANONICAL_INDEX_NAME
        assert df.index.tzinfo == timezone.utc

        # Check klines columns are present
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns

        # Check funding rate columns are not present
        assert "funding_rate" not in df.columns

    def test_create_empty_dataframe_with_klines_explicit(self):
        """Test creating empty KLINES DataFrame explicitly."""
        df = create_empty_dataframe(ChartType.KLINES)

        # Check basic structure
        assert isinstance(df, pd.DataFrame)
        assert df.empty
        assert isinstance(df.index, pd.DatetimeIndex)

        # Check klines columns are present
        assert "open" in df.columns
        assert "high" in df.columns
        assert "low" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns

        # Check funding rate columns are not present
        assert "funding_rate" not in df.columns

    def test_create_empty_dataframe_with_funding_rate(self):
        """Test creating empty FUNDING_RATE DataFrame."""
        df = create_empty_dataframe(ChartType.FUNDING_RATE)

        # Check basic structure
        assert isinstance(df, pd.DataFrame)
        assert df.empty
        assert isinstance(df.index, pd.DatetimeIndex)
        assert df.index.name == CANONICAL_INDEX_NAME

        # Check funding rate columns are present
        assert "contracts" in df.columns
        assert "funding_interval" in df.columns
        assert "funding_rate" in df.columns

        # Check klines columns are not present
        assert "open" not in df.columns
        assert "high" not in df.columns

    def test_create_empty_dataframe_with_string_chart_type(self):
        """Test creating empty DataFrame with string chart type."""
        df = create_empty_dataframe("FUNDING_RATE")

        # Check it correctly created a funding rate DataFrame
        assert "contracts" in df.columns
        assert "funding_interval" in df.columns
        assert "funding_rate" in df.columns

        # Check klines columns are not present
        assert "open" not in df.columns

    def test_create_empty_dataframe_with_invalid_chart_type(self):
        """Test creating empty DataFrame with invalid chart type falls back to KLINES."""
        df = create_empty_dataframe("INVALID_TYPE")

        # Should default to KLINES
        assert "open" in df.columns
        assert "high" in df.columns
        assert "funding_rate" not in df.columns
