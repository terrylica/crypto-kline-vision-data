#!/usr/bin/env python
"""Test timestamp semantics preservation in Vision API timestamp processing."""

from datetime import datetime, timezone, timedelta
import unittest
import pandas as pd

from utils.market_constraints import Interval
from utils.time_utils import filter_dataframe_by_time
from utils.config import KLINE_COLUMNS
from utils.for_core.vision_timestamp import process_timestamp_columns


class TestTimestampSemantics(unittest.TestCase):
    """Test timestamp semantics preservation across all interval types."""

    def test_timestamp_semantics_across_intervals(self):
        """Test timestamp semantics preservation across all interval types."""
        # Test all intervals defined in market_constraints.py
        intervals_to_test = [interval for interval in Interval]

        for interval in intervals_to_test:
            with self.subTest(interval=interval.value):
                # Create test data with 2025 (microsecond) timestamps
                # Start is interval-boundary aligned
                start_time = datetime(2025, 3, 15, 0, 0, 0, tzinfo=timezone.utc)

                # Calculate expected second timestamp based on interval
                interval_seconds = interval.to_seconds()
                second_timestamp = start_time + timedelta(seconds=interval_seconds)

                # Create raw data with open_time at START of period
                raw_data = [
                    # First candle: 00:00:00 - 00:00:59 for 1s, 00:00:00 - 00:00:59.999 for 1m, etc.
                    [
                        int(start_time.timestamp() * 1000000),
                        100.0,
                        101.0,
                        99.0,
                        100.5,
                        10.0,
                        int(
                            (
                                start_time + timedelta(seconds=interval_seconds - 0.001)
                            ).timestamp()
                            * 1000000
                        ),
                        1000.0,
                        10,
                        5.0,
                        500.0,
                        0,
                    ],
                    # Second candle: starts exactly at second_timestamp
                    [
                        int(second_timestamp.timestamp() * 1000000),
                        100.5,
                        102.0,
                        100.0,
                        101.0,
                        20.0,
                        int(
                            (
                                second_timestamp
                                + timedelta(seconds=interval_seconds - 0.001)
                            ).timestamp()
                            * 1000000
                        ),
                        2000.0,
                        20,
                        10.0,
                        1000.0,
                        0,
                    ],
                ]

                # Create DataFrame with column names
                df = pd.DataFrame(raw_data, columns=KLINE_COLUMNS)

                # Process the timestamps using the utility function
                processed_df = process_timestamp_columns(df, interval.value)

                # Verify that timestamps preserve their semantic meaning
                # First timestamp should match start_time exactly (beginning of period)
                self.assertEqual(
                    processed_df["open_time"].iloc[0].timestamp(),
                    start_time.timestamp(),
                    f"First open_time incorrect for {interval.value}",
                )

                # Second timestamp should match second_timestamp exactly
                self.assertEqual(
                    processed_df["open_time"].iloc[1].timestamp(),
                    second_timestamp.timestamp(),
                    f"Second open_time incorrect for {interval.value}",
                )

                # Filter by time and verify we don't lose the first record
                filtered_df = filter_dataframe_by_time(
                    processed_df,
                    start_time,
                    start_time + timedelta(seconds=interval_seconds * 2),
                )
                self.assertEqual(
                    len(filtered_df),
                    2,
                    f"Time filtering lost records for {interval.value}",
                )

    def test_real_world_timestamps(self):
        """Test timestamp semantics with realistic 2025 data format."""
        # Create sample data in the 2025 Vision API format (microsecond timestamps)
        raw_data = [
            # First candle: 2025-03-15 00:00:00 - 00:00:59.999999
            [
                1741996800000000,
                83983.19,
                84052.93,
                83983.19,
                84045.49,
                21.71669,
                1741996859999999,
                1824732.53,
                2993,
                10.49778,
                881995.95,
                0,
            ],
            # Second candle: 2025-03-15 00:01:00 - 00:01:59.999999
            [
                1741996860000000,
                84045.49,
                84045.49,
                83964.57,
                83971.29,
                7.41994,
                1741996919999999,
                623260.91,
                1804,
                1.19858,
                100661.29,
                0,
            ],
        ]

        # Create DataFrame with column names
        df = pd.DataFrame(raw_data, columns=KLINE_COLUMNS)

        # Process the timestamps using the utility function
        processed_df = process_timestamp_columns(df, "1m")

        # Check that open_time is correctly interpreted as period start
        first_candle_start = datetime(2025, 3, 15, 0, 0, 0, tzinfo=timezone.utc)
        second_candle_start = datetime(2025, 3, 15, 0, 1, 0, tzinfo=timezone.utc)

        # Verify first candle
        self.assertEqual(
            processed_df["open_time"].iloc[0].timestamp(),
            first_candle_start.timestamp(),
            "First candle open_time should be 2025-03-15 00:00:00",
        )

        # Verify second candle
        self.assertEqual(
            processed_df["open_time"].iloc[1].timestamp(),
            second_candle_start.timestamp(),
            "Second candle open_time should be 2025-03-15 00:01:00",
        )

        # Filter by exact times and verify both candles are included
        filtered_df = filter_dataframe_by_time(
            processed_df,
            first_candle_start,
            second_candle_start + timedelta(seconds=59.999999),
        )
        self.assertEqual(
            len(filtered_df), 2, "Time filtering should include both candles"
        )


if __name__ == "__main__":
    unittest.main()
