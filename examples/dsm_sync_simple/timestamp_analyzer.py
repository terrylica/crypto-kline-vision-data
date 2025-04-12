#!/usr/bin/env python3
"""
Timestamp analyzer for diagnosing timestamp conversion issues.

This script tests various timestamp handling functions to identify
where timestamp format changes or validation might be causing gaps.
"""

from pathlib import Path
import pandas as pd
import logging
import argparse
from datetime import datetime, timezone
import json

from utils.logger_setup import logger
from rich import print
from utils.time_utils import milliseconds_to_datetime

# Set up detailed logging
logger.setLevel(logging.DEBUG)
# Add file handler to capture all logs
log_dir = Path("./logs/timestamp_analyzer")
log_dir.mkdir(parents=True, exist_ok=True)
file_handler = logging.FileHandler(
    log_dir / f"timestamp_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

# Create a results directory for output
results_dir = Path("./logs/timestamp_analyzer/results")
results_dir.mkdir(parents=True, exist_ok=True)


def test_timestamp_conversion(timestamps):
    """
    Test timestamp conversion function on various formats.

    This tests the milliseconds_to_datetime function from utils.time_utils
    which is likely used in multiple places in the codebase.
    """
    logger.info("Testing timestamp conversion function")
    results = {}

    for ts_type, ts_value in timestamps.items():
        try:
            logger.info(f"Converting {ts_type} timestamp: {ts_value}")
            dt = milliseconds_to_datetime(ts_value)
            logger.info(f"Result: {dt} (type: {type(dt)})")
            results[ts_type] = {
                "original": ts_value,
                "converted": str(dt),
                "success": True,
            }
        except Exception as e:
            logger.error(f"Error converting {ts_type} timestamp {ts_value}: {str(e)}")
            results[ts_type] = {"original": ts_value, "error": str(e), "success": False}

    return results


def extract_timestamp_code_paths():
    """
    Search for files in the codebase that handle timestamps.

    This function would ideally use grep or similar to find all
    timestamp handling code, but here we'll focus on known
    problematic areas.
    """
    logger.info("Identifying timestamp handling code paths")

    # These are the known files that handle timestamps
    key_files = [
        "utils/time_utils.py",
        "core/sync/vision_data_client.py",
        "core/sync/rest_data_client.py",
        "core/sync/data_source_manager.py",
        "core/sync/cache_manager.py",
    ]

    # In a more comprehensive version, we would analyze the code
    # but for now, we'll just log the known files
    for file_path in key_files:
        full_path = Path("../../" + file_path)
        if full_path.exists():
            logger.info(f"Found timestamp handling in: {file_path}")
        else:
            logger.warning(f"Could not find file: {file_path}")

    return key_files


def analyze_raw_data_timestamps(csv_file):
    """
    Analyze timestamps in raw data CSV files from Vision API.

    Args:
        csv_file: Path to CSV file
    """
    logger.info(f"Analyzing timestamps in raw file: {csv_file}")

    try:
        df = pd.read_csv(csv_file)

        # Check if this is a Binance data file with the expected format
        if len(df.columns) >= 1:
            # First column should be open time
            timestamps = df.iloc[:, 0].values

            logger.info(f"Found {len(timestamps)} timestamps in the file")

            # Check timestamp format
            first_ts = timestamps[0]
            last_ts = timestamps[-1]

            logger.info(f"First timestamp: {first_ts} ({len(str(first_ts))} digits)")
            logger.info(f"Last timestamp: {last_ts} ({len(str(last_ts))} digits)")

            # Check if timestamps are in ms or μs
            ts_format = "milliseconds" if len(str(first_ts)) <= 13 else "microseconds"
            logger.info(f"Detected timestamp format: {ts_format}")

            # Calculate interval between timestamps
            if len(timestamps) > 1:
                intervals = []
                for i in range(1, min(10, len(timestamps))):
                    interval = timestamps[i] - timestamps[i - 1]
                    intervals.append(interval)

                avg_interval = sum(intervals) / len(intervals)
                logger.info(f"Average interval between timestamps: {avg_interval}")

                # Normalize to seconds for comparison
                if ts_format == "milliseconds":
                    avg_interval_sec = avg_interval / 1000
                else:
                    avg_interval_sec = avg_interval / 1000000

                logger.info(f"Average interval in seconds: {avg_interval_sec}")

            return {
                "filename": csv_file,
                "num_timestamps": len(timestamps),
                "first_timestamp": str(first_ts),
                "last_timestamp": str(last_ts),
                "format": ts_format,
                "avg_interval_raw": (
                    float(avg_interval) if "avg_interval" in locals() else None
                ),
                "avg_interval_sec": (
                    float(avg_interval_sec) if "avg_interval_sec" in locals() else None
                ),
            }

    except Exception as e:
        logger.error(f"Error analyzing file {csv_file}: {str(e)}")
        return {"filename": csv_file, "error": str(e)}


def simulate_data_merge(file1, file2):
    """
    Simulate merging data from two CSV files to check for timestamp continuity.

    This simulates what happens when the DataSourceManager merges data from
    different days or sources.

    Args:
        file1: Path to first CSV file
        file2: Path to second CSV file
    """
    logger.info(f"Simulating data merge between {file1} and {file2}")

    try:
        # Read CSVs
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)

        logger.info(f"File 1: {len(df1)} rows, File 2: {len(df2)} rows")

        # Get timestamp columns (first column)
        ts1 = df1.iloc[:, 0].values
        ts2 = df2.iloc[:, 0].values

        # Check timestamp formats
        ts1_format = "milliseconds" if len(str(ts1[0])) <= 13 else "microseconds"
        ts2_format = "milliseconds" if len(str(ts2[0])) <= 13 else "microseconds"

        logger.info(f"File 1 timestamp format: {ts1_format}")
        logger.info(f"File 2 timestamp format: {ts2_format}")

        # Check if we have different formats
        if ts1_format != ts2_format:
            logger.warning(f"Timestamp format mismatch: {ts1_format} vs {ts2_format}")

            # Normalize timestamps to same format (use microseconds)
            if ts1_format == "milliseconds":
                # Convert to microseconds
                ts1 = [t * 1000 for t in ts1]
                logger.info(f"Converted File 1 timestamps to microseconds")

            if ts2_format == "milliseconds":
                # Convert to microseconds
                ts2 = [t * 1000 for t in ts2]
                logger.info(f"Converted File 2 timestamps to microseconds")

        # Find the last timestamp from file 1 and first from file 2
        last_ts1 = ts1[-1]
        first_ts2 = ts2[0]

        # Calculate the expected interval based on earlier timestamps
        if len(ts1) > 1:
            expected_interval = ts1[1] - ts1[0]
        else:
            expected_interval = (
                ts2[1] - ts2[0] if len(ts2) > 1 else 60000
            )  # Default to 1 minute

        logger.info(f"Expected interval: {expected_interval}")

        # Check the gap between files
        actual_gap = first_ts2 - last_ts1
        logger.info(f"Gap between files: {actual_gap}")

        # Check if gap is as expected (1 interval)
        if abs(actual_gap - expected_interval) < 1000:  # Allow small tolerance
            logger.info("Gap between files is as expected (1 interval)")
        else:
            # Calculate how many intervals are missing
            missing_intervals = round(actual_gap / expected_interval) - 1
            logger.warning(f"Unexpected gap: {missing_intervals} intervals missing")

        # Create merged dataset to test timestamp continuity
        # Create actual DataFrames with Binance-like schema
        df1_clean = pd.DataFrame(
            {
                "open_time": [
                    datetime.fromtimestamp(
                        ts / 1000 if len(str(ts)) > 13 else ts / 1000, tz=timezone.utc
                    )
                    for ts in ts1
                ],
                "dummy": [1] * len(ts1),
            }
        )

        df2_clean = pd.DataFrame(
            {
                "open_time": [
                    datetime.fromtimestamp(
                        ts / 1000 if len(str(ts)) > 13 else ts / 1000, tz=timezone.utc
                    )
                    for ts in ts2
                ],
                "dummy": [2] * len(ts2),
            }
        )

        # Merge and sort
        merged_df = pd.concat([df1_clean, df2_clean])
        merged_df = merged_df.sort_values("open_time").reset_index(drop=True)

        # Check for duplicates
        dupes = merged_df.duplicated(subset=["open_time"], keep=False)
        if dupes.any():
            logger.warning(f"Found {dupes.sum()} duplicate timestamps")

        # Check for gaps
        merged_df["time_diff"] = merged_df["open_time"].diff().dt.total_seconds()
        gaps = merged_df[
            merged_df["time_diff"] != 60
        ].dropna()  # Assuming 1-minute interval

        if not gaps.empty:
            logger.warning(f"Found {len(gaps)} gaps in merged dataset")
            # Log first few gaps
            for idx, row in gaps.head(5).iterrows():
                prev_idx = idx - 1
                prev_time = merged_df.loc[prev_idx, "open_time"]
                logger.warning(
                    f"Gap: {prev_time} -> {row['open_time']} ({row['time_diff']}s)"
                )
        else:
            logger.info("No gaps found in merged dataset")

        # Save merged data for analysis
        output_file = (
            results_dir / f"merged_simulation_{Path(file1).stem}_{Path(file2).stem}.csv"
        )
        merged_df.to_csv(output_file)
        logger.info(f"Saved merged simulation to {output_file}")

        return {
            "file1": file1,
            "file2": file2,
            "file1_format": ts1_format,
            "file2_format": ts2_format,
            "gap_value": float(actual_gap),
            "expected_interval": float(expected_interval),
            "missing_intervals": (
                missing_intervals if "missing_intervals" in locals() else 0
            ),
            "duplicate_timestamps": int(dupes.sum()) if "dupes" in locals() else 0,
            "num_gaps": len(gaps) if "gaps" in locals() else 0,
        }

    except Exception as e:
        logger.error(f"Error simulating merge: {str(e)}")
        return {"file1": file1, "file2": file2, "error": str(e)}


def test_various_timestamp_formats():
    """Test the system against various timestamp formats."""
    logger.info("Testing various timestamp formats")

    # Test cases - different timestamp formats
    test_timestamps = {
        "ms_2024": 1735689540000,  # 2024-12-31 23:59:00 UTC (ms)
        "ms_2025": 1735689600000,  # 2025-01-01 00:00:00 UTC (ms)
        "us_2024": 1735689540000000,  # 2024-12-31 23:59:00 UTC (μs)
        "us_2025": 1735689600000000,  # 2025-01-01 00:00:00 UTC (μs)
        "second": 1735689600,  # 2025-01-01 00:00:00 UTC (s)
        "iso_str": "2025-01-01T00:00:00Z",
        "iso_tz": "2025-01-01T00:00:00+00:00",
    }

    # Test conversion functions
    conversion_results = test_timestamp_conversion(test_timestamps)

    # Create test dataframes with different timestamp formats
    ms_data = []
    us_data = []

    # Generate test data around the 2024-2025 transition
    base_time_ms = 1735689540000  # 2024-12-31 23:59:00 UTC

    # Generate 10 entries - 5 before midnight, 5 after
    for i in range(-5, 5):
        ms_timestamp = base_time_ms + (i * 60000)  # 1-minute intervals
        us_timestamp = ms_timestamp * 1000

        ms_data.append(ms_timestamp)
        us_data.append(us_timestamp)

    # Create dataframes
    ms_df = pd.DataFrame({"timestamp": ms_data, "value": range(len(ms_data))})
    us_df = pd.DataFrame({"timestamp": us_data, "value": range(len(us_data))})

    # Save test data
    ms_file = results_dir / "test_ms_timestamps.csv"
    us_file = results_dir / "test_us_timestamps.csv"
    mixed_file = results_dir / "test_mixed_timestamps.csv"

    ms_df.to_csv(ms_file, index=False)
    us_df.to_csv(us_file, index=False)

    # Create mixed format test file (ms before 2025, μs after)
    mixed_data = []
    for i in range(10):
        timestamp = base_time_ms + (i * 60000)  # 1-minute intervals
        # Convert to microseconds after 2025-01-01
        if timestamp >= 1735689600000:  # 2025-01-01 00:00:00
            timestamp *= 1000
        mixed_data.append(timestamp)

    mixed_df = pd.DataFrame({"timestamp": mixed_data, "value": range(len(mixed_data))})
    mixed_df.to_csv(mixed_file, index=False)

    # Analyze the test files
    ms_analysis = analyze_raw_data_timestamps(ms_file)
    us_analysis = analyze_raw_data_timestamps(us_file)
    mixed_analysis = analyze_raw_data_timestamps(mixed_file)

    # Simulate merging different formats
    merge_ms_us = simulate_data_merge(ms_file, us_file)
    merge_mixed = simulate_data_merge(ms_file, mixed_file)

    # Combine all results
    results = {
        "conversion_tests": conversion_results,
        "file_analysis": {
            "milliseconds": ms_analysis,
            "microseconds": us_analysis,
            "mixed": mixed_analysis,
        },
        "merge_tests": {"ms_to_us": merge_ms_us, "ms_to_mixed": merge_mixed},
    }

    # Save results
    results_file = results_dir / "timestamp_tests.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Saved test results to {results_file}")

    return results


def main():
    """Run the timestamp analyzer."""
    parser = argparse.ArgumentParser(description="Analyze timestamp handling")
    parser.add_argument(
        "--csv-dir", type=str, help="Directory containing CSV files to analyze"
    )
    parser.add_argument(
        "--test-formats", action="store_true", help="Test various timestamp formats"
    )
    args = parser.parse_args()

    logger.info("Starting timestamp analyzer")

    # Always run the code path extraction
    timestamp_code_paths = extract_timestamp_code_paths()

    # Test various timestamp formats
    if args.test_formats:
        logger.info("Running timestamp format tests")
        test_various_timestamp_formats()

    # Analyze CSV files if a directory is provided
    if args.csv_dir:
        csv_dir = Path(args.csv_dir)
        if csv_dir.exists() and csv_dir.is_dir():
            logger.info(f"Analyzing CSV files in {csv_dir}")

            # Find all CSV files
            csv_files = list(csv_dir.glob("*.csv"))
            logger.info(f"Found {len(csv_files)} CSV files")

            # Analyze each file
            results = []
            for csv_file in csv_files:
                analysis = analyze_raw_data_timestamps(csv_file)
                results.append(analysis)

            # Save results
            results_file = results_dir / "csv_analysis_results.json"
            with open(results_file, "w") as f:
                json.dump(results, f, indent=2)

            logger.info(f"Saved CSV analysis results to {results_file}")

            # If we have multiple files, try to simulate merging neighboring files
            if len(csv_files) > 1:
                sorted_files = sorted(csv_files)
                merge_results = []

                for i in range(len(sorted_files) - 1):
                    file1 = sorted_files[i]
                    file2 = sorted_files[i + 1]

                    merge_result = simulate_data_merge(file1, file2)
                    merge_results.append(merge_result)

                # Save merge results
                merge_results_file = results_dir / "merge_simulation_results.json"
                with open(merge_results_file, "w") as f:
                    json.dump(merge_results, f, indent=2)

                logger.info(f"Saved merge simulation results to {merge_results_file}")
        else:
            logger.error(f"CSV directory not found: {args.csv_dir}")

    logger.info("Timestamp analysis complete. Check logs for details.")
    print(
        f"[bold green]Timestamp analysis complete. Results saved to {results_dir}[/bold green]"
    )


if __name__ == "__main__":
    main()
