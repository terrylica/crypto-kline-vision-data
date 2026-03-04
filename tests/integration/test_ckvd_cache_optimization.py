#!/usr/bin/env python
"""Integration tests for CryptoKlineVisionData cache optimization."""

import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pendulum
import polars as pl

from ckvd.core.sync.crypto_kline_vision_data import CryptoKlineVisionData
from ckvd.utils.config import FEATURE_FLAGS
from ckvd.utils.loguru_setup import logger
from ckvd.utils.market_constraints import ChartType, DataProvider, Interval, MarketType

logger.configure_level("DEBUG")


class TestDsmCacheUtils(unittest.TestCase):
    """Unit tests for the cache optimization functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.symbol = "BTCUSDT"
        self.provider = DataProvider.BINANCE
        self.chart_type = ChartType.KLINES
        self.market_type = MarketType.SPOT
        self.interval = Interval("5m")

        # Create a mock cache manager
        self.cache_manager = MagicMock()

        # Time range for the tests
        self.start_time = pendulum.datetime(2025, 4, 13, 15, 35, 0, tz="UTC")
        self.end_time = pendulum.datetime(2025, 4, 14, 15, 30, 0, tz="UTC")

    def test_cache_optimization_incomplete_days(self):
        """
        TEST_CASE_ID:CACHE-OPT-001 - Critical Optimization Test (Unit Test)

        Test the optimization that prevents unnecessary API calls when:
        1. A day is incomplete (less than 90% of total records)
        2. BUT all required records for the specified time range are present

        This test validates the business requirement that partial days shouldn't be
        refetched when they already contain all data needed for the requested time range.
        """
        # Create a temporary directory for the test
        import shutil
        import tempfile
        from pathlib import Path

        # Import cache utilities with correct package prefix
        from ckvd.utils.for_core.ckvd_cache_utils import get_from_cache, save_to_cache

        test_cache_dir = Path(tempfile.mkdtemp())

        # Initialize feature flag tracking before try block to avoid UnboundLocalError
        original_flag_value = FEATURE_FLAGS.get("OPTIMIZE_CACHE_PARTIAL_DAYS", True)

        try:
            # Create test data for two days
            # Day 1: 50% complete but has all records for the requested time range
            day1 = pendulum.datetime(2025, 4, 13, 0, 0, 0, tz="UTC")
            day1_records = []

            # Create only records from 12:00 to 23:55 (half a day - incomplete)
            for i in range(12 * 12):  # 12 hours * 12 5-min intervals
                timestamp = day1.add(hours=12).add(minutes=i * 5)
                day1_records.append(
                    {
                        "open_time": timestamp,
                        "close_time": timestamp.add(minutes=5).subtract(microseconds=1),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.5,
                        "volume": 100.0,
                        "quote_asset_volume": 10050.0,
                        "count": 100,
                        "taker_buy_volume": 50.0,
                        "taker_buy_quote_volume": 5025.0,
                        "ignore": 0,
                    }
                )

            day1_df = pd.DataFrame(day1_records)

            # Day 2: 50% complete but has all records for the requested time range
            day2 = pendulum.datetime(2025, 4, 14, 0, 0, 0, tz="UTC")
            day2_records = []

            # Create only records from 00:00 to 15:55
            for i in range(16 * 12):  # 16 hours * 12 5-min intervals
                timestamp = day2.add(minutes=i * 5)
                day2_records.append(
                    {
                        "open_time": timestamp,
                        "close_time": timestamp.add(minutes=5).subtract(microseconds=1),
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.5,
                        "volume": 100.0,
                        "quote_asset_volume": 10050.0,
                        "count": 100,
                        "taker_buy_volume": 50.0,
                        "taker_buy_quote_volume": 5025.0,
                        "ignore": 0,
                    }
                )

            day2_df = pd.DataFrame(day2_records)

            # Save the test data to cache files
            # Save day1 data to cache
            save_to_cache(
                df=day1_df,
                symbol=self.symbol,
                interval=self.interval,
                market_type=self.market_type,
                cache_dir=test_cache_dir,
                chart_type=self.chart_type,
                provider=self.provider,
            )

            # Save day2 data to cache
            save_to_cache(
                df=day2_df,
                symbol=self.symbol,
                interval=self.interval,
                market_type=self.market_type,
                cache_dir=test_cache_dir,
                chart_type=self.chart_type,
                provider=self.provider,
            )

            # Make sure the feature flag is enabled for this test
            FEATURE_FLAGS["OPTIMIZE_CACHE_PARTIAL_DAYS"] = True

            # Request data from 13th 15:35 to 14th 15:30 (our test case)
            # Day 1 should have all records needed for 15:35-23:55
            # Day 2 should have all records needed for 00:00-15:30
            df, missing_ranges = get_from_cache(
                symbol=self.symbol,
                start_time=self.start_time,
                end_time=self.end_time,
                interval=self.interval,
                cache_dir=test_cache_dir,
                provider=self.provider,
                chart_type=self.chart_type,
                market_type=self.market_type,
            )

            # Verify results
            # 1. No missing ranges should be reported
            self.assertEqual(
                len(missing_ranges),
                0,
                "Expected no missing ranges when cache has all required records",
            )

            # 2. Verify we got a complete dataset back
            # The number of 5-min intervals between 2025-04-13 15:35 and 2025-04-14 15:30
            expected_intervals = int((self.end_time - self.start_time).total_seconds() / self.interval.to_seconds()) + 1
            self.assertEqual(
                len(df),
                expected_intervals,
                f"Expected {expected_intervals} records in the result DataFrame",
            )
        finally:
            # Clean up the temporary directory
            shutil.rmtree(test_cache_dir)
            # Restore the original feature flag value
            FEATURE_FLAGS["OPTIMIZE_CACHE_PARTIAL_DAYS"] = original_flag_value


class TestDsmCacheOptimization(unittest.TestCase):
    """
    Integration tests for the CryptoKlineVisionData cache optimization feature.

    Tests end-to-end FCP flow with cache data, verifying that complete cache coverage
    prevents unnecessary API calls, and partial cache data triggers REST fallback.
    """

    def setUp(self):
        """Set up test environment."""
        # Create a test cache directory
        self.cache_dir = Path("./test_cache")
        self.cache_dir.mkdir(exist_ok=True, parents=True)

        # Create mock cache manager and clients
        self.cache_manager = MagicMock()
        self.vision_client = MagicMock()
        self.rest_client = MagicMock()

        # Test parameters
        self.symbol = "BTCUSDT"
        self.market_type = MarketType.SPOT
        self.interval = Interval("5m")
        self.provider = DataProvider.BINANCE
        self.chart_type = ChartType.KLINES

        # Time range
        self.start_time = pendulum.datetime(2025, 4, 13, 15, 35, 0, tz="UTC")
        self.end_time = pendulum.datetime(2025, 4, 14, 15, 30, 0, tz="UTC")

    def tearDown(self):
        """Clean up after tests."""
        # Remove the test cache directory
        import shutil

        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)

    def _make_cache_lazyframe(self, records: list[dict]) -> pl.LazyFrame:
        """Convert test records to a Polars LazyFrame suitable for get_cache_lazyframes mock."""
        df = pd.DataFrame(records)
        return pl.from_pandas(df).lazy()

    def test_optimization_enabled(self):
        """Test with complete cache coverage - should not make API calls."""
        # Mock the API calls to track if they're made
        api_called = {"vision": False, "rest": False}

        def mock_vision_get(*_args, **_kwargs):
            api_called["vision"] = True
            return pd.DataFrame()

        def mock_rest_get(*_args, **_kwargs):
            api_called["rest"] = True
            return pd.DataFrame()

        # Create a complete dataset for the time range (without _data_source — pipeline adds it)
        all_records = []
        current_time = self.start_time
        while current_time <= self.end_time:
            all_records.append(
                {
                    "open_time": current_time,
                    "close_time": current_time.add(minutes=5).subtract(microseconds=1),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 100.0,
                    "quote_asset_volume": 10050.0,
                    "count": 100,
                    "taker_buy_volume": 50.0,
                    "taker_buy_quote_volume": 5025.0,
                    "ignore": 0,
                }
            )
            current_time = current_time.add(minutes=5)

        cache_lf = self._make_cache_lazyframe(all_records)

        # Patch get_cache_lazyframes (FCP Step 1 now uses this utility directly)
        with (
            patch("ckvd.utils.for_core.ckvd_cache_utils.get_cache_lazyframes", return_value=[cache_lf]),
            patch.object(CryptoKlineVisionData, "_fetch_from_vision", side_effect=mock_vision_get),
            patch.object(CryptoKlineVisionData, "_fetch_from_rest", side_effect=mock_rest_get),
        ):
            ckvd = CryptoKlineVisionData(provider=DataProvider.BINANCE, cache_dir=self.cache_dir)

            df = ckvd.get_data(
                symbol=self.symbol,
                start_time=self.start_time,
                end_time=self.end_time,
                interval=self.interval,
                chart_type=self.chart_type,
                include_source_info=True,
            )

            # Verify no API calls were made (cache had full coverage)
            self.assertFalse(api_called["vision"], "Vision API should not have been called")
            self.assertFalse(api_called["rest"], "REST API should not have been called")

            # Verify we got data back
            self.assertGreater(len(df), 0, "Expected non-empty result from cache")

            # Verify all records are from cache
            self.assertTrue(all(df["_data_source"] == "CACHE"), "All records should be from cache")

    def test_partial_cache_triggers_rest(self):
        """Test with partial cache data - should trigger REST fallback for missing ranges."""
        # Mock the API calls to track if they're made
        api_called = {"vision": False, "rest": False}

        # Create incomplete data (only 10 records out of ~287 needed)
        incomplete_records = []
        current_time = self.start_time
        for _ in range(10):
            incomplete_records.append(
                {
                    "open_time": current_time,
                    "close_time": current_time.add(minutes=5).subtract(microseconds=1),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 100.0,
                    "quote_asset_volume": 10050.0,
                    "count": 100,
                    "taker_buy_volume": 50.0,
                    "taker_buy_quote_volume": 5025.0,
                    "ignore": 0,
                }
            )
            current_time = current_time.add(minutes=5)

        cache_lf = self._make_cache_lazyframe(incomplete_records)

        # Create REST result data for the missing range
        rest_records = []
        rest_start = self.start_time.add(minutes=50)
        current_time = rest_start
        while current_time <= self.end_time:
            rest_records.append(
                {
                    "open_time": current_time,
                    "close_time": current_time.add(minutes=5).subtract(microseconds=1),
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.5,
                    "volume": 110.0,
                    "quote_asset_volume": 11050.0,
                    "count": 110,
                    "taker_buy_volume": 55.0,
                    "taker_buy_quote_volume": 5525.0,
                    "ignore": 0,
                }
            )
            current_time = current_time.add(minutes=5)
        rest_df = pd.DataFrame(rest_records)
        rest_df["_data_source"] = "REST"

        def mock_vision_get(*_args, **_kwargs):
            api_called["vision"] = True
            return pd.DataFrame()

        def mock_rest_get(*_args, **_kwargs):
            api_called["rest"] = True
            return rest_df

        # Patch get_cache_lazyframes with partial data — FCP will detect gaps
        with (
            patch("ckvd.utils.for_core.ckvd_cache_utils.get_cache_lazyframes", return_value=[cache_lf]),
            patch.object(CryptoKlineVisionData, "_fetch_from_vision", side_effect=mock_vision_get),
            patch.object(CryptoKlineVisionData, "_fetch_from_rest", side_effect=mock_rest_get),
        ):
            ckvd = CryptoKlineVisionData(provider=DataProvider.BINANCE, cache_dir=self.cache_dir)

            df = ckvd.get_data(
                symbol=self.symbol,
                start_time=self.start_time,
                end_time=self.end_time,
                interval=self.interval,
                chart_type=self.chart_type,
                include_source_info=True,
            )

            # Verify REST API was called to fill gaps
            self.assertTrue(api_called["rest"], "REST API should have been called")

            # Verify the result contains data from both sources
            self.assertIn("_data_source", df.columns, "Missing data source column")
            self.assertIn("CACHE", df["_data_source"].to_numpy(), "Missing cache data")
            self.assertIn("REST", df["_data_source"].to_numpy(), "Missing REST data")


if __name__ == "__main__":
    unittest.main()
