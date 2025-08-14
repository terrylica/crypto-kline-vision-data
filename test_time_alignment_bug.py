#!/usr/bin/env python3
"""Test script to reproduce the time alignment bug causing start_time > end_time."""

from datetime import datetime, timedelta

import pytz

from data_source_manager.utils.market_constraints import Interval
from data_source_manager.utils.time_utils import align_time_boundaries


def test_time_alignment_bug():
    """Test the time alignment issue that causes start_time > end_time."""

    print("🔍 Testing Time Alignment Bug")
    print("=" * 50)

    # Reproduce the exact scenario from the error
    end_time = datetime.now(pytz.UTC) - timedelta(minutes=30)
    start_time = end_time - timedelta(minutes=30)

    print(f"Original time range: {start_time} to {end_time}")
    print(f"Duration: {(end_time - start_time).total_seconds()} seconds")
    print()

    # Test different intervals
    intervals_to_test = [Interval.MINUTE_1, Interval.MINUTE_5, Interval.MINUTE_15, Interval.HOUR_1, Interval.HOUR_2, Interval.SECOND_1]

    for interval in intervals_to_test:
        print(f"Testing {interval.value} interval:")
        try:
            aligned_start, aligned_end = align_time_boundaries(start_time, end_time, interval)

            if aligned_start > aligned_end:
                print(f"  ❌ BUG FOUND: aligned_start ({aligned_start}) > aligned_end ({aligned_end})")
                print(f"     Time difference: {(aligned_start - aligned_end).total_seconds()} seconds")
            else:
                print(f"  ✅ OK: {aligned_start} to {aligned_end}")
                print(f"     Duration: {(aligned_end - aligned_start).total_seconds()} seconds")

        except Exception as e:
            print(f"  ❌ ERROR: {type(e).__name__}: {e}")
        print()


if __name__ == "__main__":
    test_time_alignment_bug()
