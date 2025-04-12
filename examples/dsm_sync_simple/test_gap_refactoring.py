#!/usr/bin/env python3
"""
Test script for the refactored gap detection in gap_detector.py

This script tests the basic functionality of the gap_detector utility.
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
from pathlib import Path
import argparse

from utils.logger_setup import logger
from rich import print
from utils.market_constraints import Interval
from utils.gap_detector import detect_gaps, Gap, format_gaps_for_display


def test_gap_detector_direct():
    """Test gap_detector directly with sample data"""
    print("[bold blue]Testing gap_detector with sample data[/bold blue]")

    # Create sample dataframe with a known gap
    now = datetime.now(timezone.utc)
    dates = [now + timedelta(minutes=i) for i in range(10)]  # 10 consecutive minutes
    # Add a gap by removing some records
    del dates[5:7]  # Remove 2 records to create a gap

    # Create dataframe
    df = pd.DataFrame({"open_time": dates, "value": range(len(dates))})

    # Detect gaps
    gaps, stats = detect_gaps(
        df,
        Interval.MINUTE_1,
        time_column="open_time",
        gap_threshold=0.3,
        enforce_min_span=False,  # Don't enforce min span for this small test
    )

    # Display results
    print(f"Gap detection results: Found {len(gaps)} gaps")
    for i, gap in enumerate(gaps):
        print(
            f"Gap {i+1}: {gap.start_time} → {gap.end_time}, duration: {gap.duration}, missing: {gap.missing_points}"
        )

    # Format gaps for display
    gap_df = format_gaps_for_display(gaps)
    if not gap_df.empty:
        print("\nGap details:")
        print(gap_df)

    # Check if gap detection worked correctly
    if len(gaps) == 1 and gaps[0].missing_points == 2:
        print(
            "[bold green]✓ Gap detection test passed - correctly identified 1 gap with 2 missing points[/bold green]"
        )
    else:
        print(
            "[bold red]✗ Gap detection test failed - expected 1 gap with 2 missing points[/bold red]"
        )


def main():
    """Main function to run all tests"""
    # Configure logger
    logger.use_rich(True)
    logger.setLevel("INFO")

    print("[bold cyan]===== Gap Detection Test =====[/bold cyan]")
    test_gap_detector_direct()
    print("[bold cyan]===== Tests Complete =====[/bold cyan]")


if __name__ == "__main__":
    main()
