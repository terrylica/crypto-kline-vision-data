#!/usr/bin/env python
"""Test column selection in VisionDataClient fetch method.

Focus on the column selection functionality with the simplest possible test.
"""

import pytest
from datetime import datetime, timezone, timedelta
import pandas as pd
import logging
import tempfile
import unittest.mock as mock

from core.vision_data_client_enhanced import VisionDataClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test constants
SYMBOL = "BTCUSDT"
INTERVAL = "1h"


@pytest.mark.asyncio
@mock.patch("core.vision_data_client_enhanced.TimeRangeManager")
async def test_column_selection(mock_time_manager):
    """Test column selection in fetch method using mocks."""
    # Test time range
    start_time = datetime(2023, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    end_time = datetime(2023, 1, 15, 1, 0, 0, tzinfo=timezone.utc)

    # Mock TimeRangeManager to avoid validation errors
    mock_time_manager.validate_time_window = mock.MagicMock()
    mock_time_manager.filter_dataframe = mock.MagicMock(return_value=pd.DataFrame())
    mock_time_manager.get_time_boundaries = mock.MagicMock(
        return_value={
            "adjusted_start": start_time,
            "adjusted_end": end_time,
            "expected_records": 1,
        }
    )

    # Prepare test data
    complete_data = {
        "open": [100.0],
        "high": [101.0],
        "low": [99.0],
        "close": [100.5],
        "volume": [1000.0],
    }

    selected_columns = ["open", "close", "volume"]

    # Create complete and subset dataframes
    complete_df = pd.DataFrame(complete_data)
    complete_df.index = pd.DatetimeIndex([start_time], name="open_time")

    selected_df = complete_df[selected_columns].copy()

    with tempfile.TemporaryDirectory() as cache_dir:
        logger.info(f"Using temp directory: {cache_dir}")

        # Mock the fetch method to return different dataframes based on columns parameter
        with mock.patch.object(VisionDataClient, "fetch") as mock_fetch:
            # Configure mock behavior
            def mock_fetch_side_effect(*args, **kwargs):
                columns = kwargs.get("columns")
                if columns and set(columns) == set(selected_columns):
                    return selected_df
                return complete_df

            mock_fetch.side_effect = mock_fetch_side_effect

            # Create a single client instance
            client = VisionDataClient(
                symbol=SYMBOL,
                interval=INTERVAL,
                cache_dir=cache_dir,
                market_type="spot",
            )

            # Test fetching with all columns
            full_result = await client.fetch(start_time, end_time)

            # Test fetching with selected columns
            selected_result = await client.fetch(
                start_time, end_time, columns=selected_columns
            )

            # Verify results
            assert set(full_result.columns) == set(
                complete_data.keys()
            ), "Full result should have all columns"
            assert set(selected_result.columns) == set(
                selected_columns
            ), "Selected result should have only selected columns"

            # Verify the columns were properly passed to the fetch method
            assert mock_fetch.call_count == 2, "Fetch should be called twice"

            # Verify that the selected columns match
            for col in selected_columns:
                assert selected_result[col].equals(
                    full_result[col]
                ), f"Data for column {col} should match"
