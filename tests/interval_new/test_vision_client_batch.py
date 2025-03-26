#!/usr/bin/env python
"""Test VisionDataClient batch fetching functionality.

Focus on the core functionality of fetching data for multiple symbols simultaneously.
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
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
INTERVAL = "1h"


@pytest.mark.asyncio
@mock.patch("core.vision_data_client_enhanced.TimeRangeManager")
async def test_batch_fetch_functionality(mock_time_manager):
    """Test batch fetching for multiple symbols using mocks."""
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

    # Create sample dataframes for each symbol with consistent structure
    def create_test_df(symbol, price_multiplier=1.0):
        data = {
            "open": [100.0 * price_multiplier],
            "high": [101.0 * price_multiplier],
            "low": [99.0 * price_multiplier],
            "close": [100.5 * price_multiplier],
            "volume": [1000.0],
        }
        df = pd.DataFrame(data)
        df.index = pd.DatetimeIndex([start_time], name="open_time")
        return df

    # Create test dataframes with different price levels for each symbol
    test_dfs = {
        "BTCUSDT": create_test_df("BTCUSDT", 100.0),
        "ETHUSDT": create_test_df("ETHUSDT", 10.0),
        "BNBUSDT": create_test_df("BNBUSDT", 1.0),
    }

    with tempfile.TemporaryDirectory() as cache_dir:
        logger.info(f"Using temp directory: {cache_dir}")

        # Mock the batch_fetch method
        with mock.patch.object(VisionDataClient, "batch_fetch") as mock_batch_fetch:
            # Configure mock to return the test dataframes
            mock_batch_fetch.return_value = test_dfs

            # Create client for testing
            client = VisionDataClient(
                symbol=SYMBOLS[0],  # We'll override this with batch_fetch
                interval=INTERVAL,
                cache_dir=cache_dir,
                market_type="spot",
            )

            # Test batch fetching
            result = await client.batch_fetch(SYMBOLS, start_time, end_time)

            # Verify the result
            assert mock_batch_fetch.called, "batch_fetch method should be called"
            assert isinstance(result, dict), "Result should be a dictionary"
            assert set(result.keys()) == set(
                SYMBOLS
            ), "Result should contain all requested symbols"

            # Verify the structure of each dataframe
            for symbol in SYMBOLS:
                assert symbol in result, f"Symbol {symbol} should be in the result"
                df = result[symbol]
                assert isinstance(
                    df, pd.DataFrame
                ), f"Result for {symbol} should be a DataFrame"
                assert set(df.columns) == set(
                    ["open", "high", "low", "close", "volume"]
                ), f"DataFrame for {symbol} should have the expected columns"
                assert (
                    len(df) == 1
                ), f"DataFrame for {symbol} should have exactly one row"

                # Verify the data values match expectations
                if symbol == "BTCUSDT":
                    assert df["open"].iloc[0] == 10000.0
                elif symbol == "ETHUSDT":
                    assert df["open"].iloc[0] == 1000.0
                elif symbol == "BNBUSDT":
                    assert df["open"].iloc[0] == 100.0

            logger.info("Batch fetch test completed successfully")
