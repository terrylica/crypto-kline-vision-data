#!/usr/bin/env python
"""Gap Patcher Utility for Binance Data Services.

This utility provides functions to identify and patch day boundary timestamp gaps in market data.
It focuses specifically on the common pattern where 00:00:00 timestamps are missing at day boundaries.
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
from typing import List, Tuple, Dict, Any
import logging
import argparse
from pathlib import Path

# Project imports
from utils.logger_setup import logger
from utils.market_constraints import Interval, MarketType
from utils.time_utils import (
    get_interval_seconds,
)
from core.sync.data_source_manager import DataSourceManager
from core.sync.vision_data_client import VisionDataClient
from core.sync.rest_data_client import RestDataClient


class GapPatcher:
    """Utility class for identifying and patching timestamp gaps in market data."""

    def __init__(self, debug_mode: bool = False):
        """Initialize the gap patcher.

        Args:
            debug_mode: Enable verbose logging for debugging
        """
        self.debug_mode = debug_mode
        if debug_mode:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

    def find_day_boundary_gaps(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Find gaps at day boundaries in a DataFrame.

        Args:
            df: DataFrame with open_time column

        Returns:
            List of dictionaries with gap details
        """
        if df.empty:
            logger.warning("Empty DataFrame provided, no gaps to find")
            return []

        # Ensure DataFrame is sorted by open_time
        df = df.sort_values("open_time").reset_index(drop=True)

        # Calculate time differences between consecutive rows
        df["time_diff"] = df["open_time"].diff().dt.total_seconds()

        # Find gaps where time difference is greater than expected
        # We use a threshold of 90 seconds to catch the typical 1-minute interval gaps
        gap_threshold = 90  # seconds
        potential_gaps = df[df["time_diff"] > gap_threshold].copy()

        # Filter for day boundary gaps only (previous timestamp's day != current timestamp's day)
        day_boundary_gaps = []

        for idx, row in potential_gaps.iterrows():
            prev_idx = idx - 1
            if prev_idx >= 0:
                prev_time = df.loc[prev_idx, "open_time"]
                curr_time = row["open_time"]

                # Check if this gap crosses a day boundary
                is_day_boundary = prev_time.day != curr_time.day

                if is_day_boundary:
                    gap_info = {
                        "prev_time": prev_time,
                        "curr_time": curr_time,
                        "gap_seconds": row["time_diff"],
                        "prev_idx": prev_idx,
                        "curr_idx": idx,
                        "is_month_boundary": prev_time.month != curr_time.month,
                        "is_year_boundary": prev_time.year != curr_time.year,
                    }
                    day_boundary_gaps.append(gap_info)

                    boundary_type = "day"
                    if gap_info["is_year_boundary"]:
                        boundary_type = "year"
                    elif gap_info["is_month_boundary"]:
                        boundary_type = "month"

                    logger.debug(
                        f"Found {boundary_type} boundary gap: {prev_time} → {curr_time} ({row['time_diff']:.1f}s)"
                    )

        return day_boundary_gaps

    def interpolate_missing_timestamps(
        self, df: pd.DataFrame, interval: Interval
    ) -> pd.DataFrame:
        """Interpolate missing timestamps in a DataFrame, focusing on day boundaries.

        Args:
            df: DataFrame with market data
            interval: Time interval between data points

        Returns:
            DataFrame with interpolated values at day boundaries
        """
        if df.empty:
            logger.warning("Empty DataFrame provided, nothing to interpolate")
            return df

        # Find day boundary gaps
        gaps = self.find_day_boundary_gaps(df)

        if not gaps:
            logger.info("No day boundary gaps found in the data")
            return df

        logger.info(f"Found {len(gaps)} day boundary gaps to patch")

        # Get expected interval in seconds
        interval_seconds = get_interval_seconds(interval)

        # Create a new DataFrame to hold all the interpolated data
        patched_data = []

        # For each gap, generate the missing row(s)
        for gap in gaps:
            prev_time = gap["prev_time"]
            curr_time = gap["curr_time"]

            # Prepare the midnight timestamp (00:00:00)
            if prev_time.hour == 23 and prev_time.minute == 59:
                # Create midnight timestamp for the next day
                midnight = datetime(
                    curr_time.year,
                    curr_time.month,
                    curr_time.day,
                    0,
                    0,
                    0,
                    tzinfo=timezone.utc,
                )

                # Get the previous and next rows for interpolation
                prev_row = df.loc[gap["prev_idx"]].to_dict()
                next_row = df.loc[gap["curr_idx"]].to_dict()

                # Create a new interpolated row for midnight
                interpolated_row = prev_row.copy()
                interpolated_row["open_time"] = midnight

                # Simple linear interpolation for numeric values
                for col in ["open", "high", "low", "close", "volume"]:
                    if col in prev_row and col in next_row:
                        weight = (midnight - prev_time).total_seconds() / (
                            curr_time - prev_time
                        ).total_seconds()
                        interpolated_row[col] = prev_row[col] + weight * (
                            next_row[col] - prev_row[col]
                        )

                # Add the interpolated row to our patched data
                patched_data.append(interpolated_row)

                logger.debug(f"Created interpolated row at {midnight}")

        # If no rows were created, return the original DataFrame
        if not patched_data:
            return df

        # Create a DataFrame from the patched data
        patched_df = pd.DataFrame(patched_data)

        # Combine the original and patched DataFrames
        combined_df = pd.concat([df, patched_df], ignore_index=True)

        # Sort by open_time and reset index
        combined_df = combined_df.sort_values("open_time").reset_index(drop=True)

        # Remove duplicate timestamps if any
        combined_df = combined_df.drop_duplicates(subset=["open_time"])

        logger.info(f"Added {len(patched_data)} interpolated rows at day boundaries")

        return combined_df

    def patch_dataset(
        self,
        symbol: str,
        interval: Interval,
        start_time: datetime,
        end_time: datetime,
        market_type: MarketType = MarketType.SPOT,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """Retrieve and patch a dataset for the specified time range.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            start_time: Start time
            end_time: End time
            market_type: Market type
            use_cache: Whether to use cached data

        Returns:
            Patched DataFrame
        """
        logger.info(
            f"Patching dataset for {symbol} {interval.value} from {start_time} to {end_time}"
        )

        # Create a DataSourceManager to get the data
        dsm = DataSourceManager(market_type=market_type, use_cache=use_cache)

        # Get data from the manager
        df = dsm.get_data(
            symbol=symbol, interval=interval, start_time=start_time, end_time=end_time
        )

        if df.empty:
            logger.warning("No data retrieved from DataSourceManager")
            return df

        logger.info(f"Retrieved {len(df)} rows from DataSourceManager")

        # Patch day boundary gaps
        patched_df = self.interpolate_missing_timestamps(df, interval)

        # Verify that all gaps are fixed
        remaining_gaps = self.find_day_boundary_gaps(patched_df)
        if remaining_gaps:
            logger.warning(
                f"There are still {len(remaining_gaps)} day boundary gaps after patching"
            )
        else:
            logger.info("All day boundary gaps have been successfully patched")

        return patched_df

    def compare_raw_and_patched(
        self,
        symbol: str,
        interval: Interval,
        start_time: datetime,
        end_time: datetime,
        market_type: MarketType = MarketType.SPOT,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Compare raw Vision data, raw REST data, and patched data.

        Args:
            symbol: Trading pair symbol
            interval: Time interval
            start_time: Start time
            end_time: End time
            market_type: Market type

        Returns:
            Tuple of (vision_df, rest_df, patched_df)
        """
        logger.info(f"Comparing data sources for {symbol} {interval.value}")

        # Get data from Vision API
        vision_client = VisionDataClient(
            symbol=symbol, interval=interval.value, market_type=market_type
        )
        vision_df = vision_client.fetch(start_time, end_time)
        logger.info(f"Retrieved {len(vision_df)} rows from Vision API")

        # Get data from REST API
        rest_client = RestDataClient(
            market_type=market_type, symbol=symbol, interval=interval
        )
        rest_df = rest_client.fetch(symbol, interval, start_time, end_time)
        logger.info(f"Retrieved {len(rest_df)} rows from REST API")

        # Patch the Vision data
        patched_df = self.interpolate_missing_timestamps(vision_df, interval)
        logger.info(f"Patched Vision data now has {len(patched_df)} rows")

        # Compare the datasets
        vision_gaps = self.find_day_boundary_gaps(vision_df)
        rest_gaps = self.find_day_boundary_gaps(rest_df)
        patched_gaps = self.find_day_boundary_gaps(patched_df)

        logger.info(f"Vision API data has {len(vision_gaps)} day boundary gaps")
        logger.info(f"REST API data has {len(rest_gaps)} day boundary gaps")
        logger.info(f"Patched data has {len(patched_gaps)} day boundary gaps")

        # Detect if rest data has timestamps not present in Vision
        if not rest_df.empty and not vision_df.empty:
            rest_times = set(rest_df["open_time"])
            vision_times = set(vision_df["open_time"])
            patched_times = set(patched_df["open_time"])

            rest_only_times = rest_times - vision_times
            patched_added_times = patched_times - vision_times

            logger.info(
                f"REST API has {len(rest_only_times)} timestamps not present in Vision API data"
            )
            logger.info(f"Patched data added {len(patched_added_times)} timestamps")

            # List timestamps added by patching that are also in REST data (validation)
            valid_patches = patched_added_times.intersection(rest_times)
            if valid_patches:
                logger.info(
                    f"Patching added {len(valid_patches)} timestamps that match REST API data"
                )
                for ts in sorted(valid_patches):
                    logger.debug(f"Validated patched timestamp: {ts}")

        return vision_df, rest_df, patched_df


def main():
    """Run the gap patcher tool."""
    parser = argparse.ArgumentParser(description="Binance Data Services Gap Patcher")
    parser.add_argument(
        "--symbol", type=str, default="BTCUSDT", help="Trading pair symbol"
    )
    parser.add_argument("--interval", type=str, default="1m", help="Kline interval")
    parser.add_argument(
        "--market",
        type=str,
        default="spot",
        help="Market type (spot, futures_usdt, futures_coin)",
    )
    parser.add_argument(
        "--start-date", type=str, help="Start date in YYYY-MM-DD format"
    )
    parser.add_argument("--end-date", type=str, help="End date in YYYY-MM-DD format")
    parser.add_argument(
        "--days",
        type=int,
        default=5,
        help="Number of days to analyze if start/end not specified",
    )
    parser.add_argument(
        "--year-transition",
        action="store_true",
        help="Analyze the 2024-2025 transition",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--save-csv", action="store_true", help="Save results to CSV files"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./gap_patcher_results",
        help="Output directory for CSV files",
    )
    args = parser.parse_args()

    # Setup market type
    market_type_map = {
        "spot": MarketType.SPOT,
        "futures_usdt": MarketType.FUTURES_USDT,
        "futures_coin": MarketType.FUTURES_COIN,
    }
    market_type = market_type_map.get(args.market.lower(), MarketType.SPOT)

    # Parse interval
    interval_map = {
        "1s": Interval.SECOND_1,
        "1m": Interval.MINUTE_1,
        "3m": Interval.MINUTE_3,
        "5m": Interval.MINUTE_5,
        "15m": Interval.MINUTE_15,
        "30m": Interval.MINUTE_30,
        "1h": Interval.HOUR_1,
        "2h": Interval.HOUR_2,
        "4h": Interval.HOUR_4,
        "6h": Interval.HOUR_6,
        "8h": Interval.HOUR_8,
        "12h": Interval.HOUR_12,
        "1d": Interval.DAY_1,
        "3d": Interval.DAY_3,
        "1w": Interval.WEEK_1,
        "1M": Interval.MONTH_1,
    }
    interval = interval_map.get(args.interval, Interval.MINUTE_1)

    # Setup time range
    end_time = datetime.now(timezone.utc)
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        end_time = datetime(
            end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc
        )

    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        start_time = datetime(
            start_date.year,
            start_date.month,
            start_date.day,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        )
    else:
        start_time = end_time - timedelta(days=args.days)

    # Handle year transition request
    if args.year_transition:
        start_time = datetime(2024, 12, 30, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 2, 23, 59, 59, tzinfo=timezone.utc)

    # Create the patcher
    patcher = GapPatcher(debug_mode=args.debug)

    # Compare data sources
    vision_df, rest_df, patched_df = patcher.compare_raw_and_patched(
        args.symbol, interval, start_time, end_time, market_type
    )

    if args.save_csv:
        # Create output directory if it doesn't exist
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Prepare base filename
        base_filename = f"{args.symbol}_{interval.value}_{start_time.strftime('%Y%m%d')}_to_{end_time.strftime('%Y%m%d')}"

        # Save the dataframes
        if not vision_df.empty:
            vision_df.to_csv(output_dir / f"{base_filename}_vision.csv", index=False)
            logger.info(
                f"Saved Vision API data to {output_dir}/{base_filename}_vision.csv"
            )

        if not rest_df.empty:
            rest_df.to_csv(output_dir / f"{base_filename}_rest.csv", index=False)
            logger.info(f"Saved REST API data to {output_dir}/{base_filename}_rest.csv")

        if not patched_df.empty:
            patched_df.to_csv(output_dir / f"{base_filename}_patched.csv", index=False)
            logger.info(
                f"Saved patched data to {output_dir}/{base_filename}_patched.csv"
            )


if __name__ == "__main__":
    main()
