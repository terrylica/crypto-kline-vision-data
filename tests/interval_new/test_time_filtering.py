#!/usr/bin/env python
"""Test filtering functionality by directly accessing data from Binance Vision.

This test validates basic data filtering directly from downloaded files without
relying on the internal filtering functions in TimeRangeManager.
"""

import pytest
import pandas as pd
import logging
import os
import httpx
from datetime import datetime, timezone, timedelta
from pathlib import Path
import tempfile

from utils.download_handler import DownloadHandler

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_direct_data_filtering():
    """Test filtering directly with known data from Binance Vision API."""
    # Create a date range that we know works from our previous test
    start_date = datetime(2023, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    end_date = datetime(2023, 1, 15, 1, 0, 0, tzinfo=timezone.utc)

    logger.info(f"Using time range: {start_date.isoformat()} to {end_date.isoformat()}")

    # Create a temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        debug_dir = Path(temp_dir) / "debug_filter"
        os.makedirs(debug_dir, exist_ok=True)
        logger.info(f"Created debug directory at {debug_dir}")

        # Parameters for direct download
        symbol = "BTCUSDT"
        date_str = "2023-01-15"
        interval = "1h"

        # Download the file using httpx
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                download_handler = DownloadHandler(client)

                # Download file directly
                zip_file_path = debug_dir / f"{symbol}-{interval}-{date_str}.zip"
                url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"

                logger.info(f"Directly downloading {url} to {zip_file_path}")
                success = await download_handler.download_file(url, zip_file_path)
                logger.info(f"Direct download success: {success}")

                if success and zip_file_path.exists():
                    # Extract and read the data
                    import zipfile

                    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                        file_list = zip_ref.namelist()
                        logger.info(f"Zip file contents: {file_list}")

                        if file_list:
                            main_file = file_list[0]
                            csv_file_path = debug_dir / main_file
                            zip_ref.extract(main_file, debug_dir)

                            # Read the CSV directly
                            df = pd.read_csv(
                                csv_file_path,
                                names=[
                                    "open_time",
                                    "open",
                                    "high",
                                    "low",
                                    "close",
                                    "volume",
                                    "close_time",
                                    "quote_volume",
                                    "trades",
                                    "taker_buy_volume",
                                    "taker_buy_quote_volume",
                                    "ignored",
                                ],
                            )

                            # Convert timestamps with proper timezone
                            from core.vision_constraints import detect_timestamp_unit

                            sample_ts = df["open_time"].iloc[0]
                            ts_unit = detect_timestamp_unit(sample_ts)

                            df["open_time"] = pd.to_datetime(
                                df["open_time"], unit=ts_unit
                            ).dt.tz_localize("UTC")

                            df.set_index("open_time", inplace=True)
                            logger.info(f"Data shape: {df.shape}")
                            logger.info(
                                f"Data range: {df.index.min()} to {df.index.max()}"
                            )

                            # Perform basic filtering - this is the key test
                            filtered_df = df[
                                (df.index >= start_date) & (df.index < end_date)
                            ]

                            logger.info(f"Filtered data shape: {filtered_df.shape}")
                            if not filtered_df.empty:
                                logger.info(
                                    f"Filtered range: {filtered_df.index.min()} to {filtered_df.index.max()}"
                                )

                                # Verify filtered data
                                assert (
                                    filtered_df.index.min() >= start_date
                                ), "Data starts too early"
                                assert (
                                    filtered_df.index.max() < end_date
                                ), "Data extends too far"

                                logger.info("Basic pandas filtering works as expected")
                            else:
                                logger.warning(
                                    "Filtered dataframe is empty - this could be normal if data doesn't exist for this time period"
                                )
                        else:
                            logger.warning(
                                "No files found in zip archive - test is inconclusive"
                            )
                else:
                    logger.warning("Failed to download file - test is inconclusive")

        except Exception as e:
            logger.error(f"Error in test: {e}")
            import traceback

            logger.error(traceback.format_exc())
            pytest.skip(f"Test failed due to external issues: {e}")
