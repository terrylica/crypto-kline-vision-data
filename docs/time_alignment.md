# Time Alignment in Data Services

## Overview

This document explains the standardized approach to time alignment in the data services codebase. Time alignment is critical for ensuring consistent handling of time boundaries across different components of the system.

## Core Principles

1. **Start time is inclusive** - Records at the start time are included in results
2. **End time is exclusive** - Records at the end time are NOT included in results
3. **Timestamps with microseconds are floored** - All units smaller than the interval are removed
4. **Consistent behavior across components** - All components use the same utilities for time handling

## Centralized Utility Functions

To enforce consistency, we've implemented the following utility functions in `utils/time_alignment.py`:

### `adjust_time_window(start_time, end_time, interval)`

This function adjusts input timestamps to ensure consistent handling:

- Rounds start and end times down to interval boundaries
- Removes incomplete intervals at the end of the range
- Returns a tuple of `(adjusted_start, adjusted_end)`

### `get_time_boundaries(start_time, end_time, interval)`

This comprehensive function provides all necessary time boundary information:

- Calls `adjust_time_window` to get properly aligned timestamps
- Calculates millisecond versions for API calls
- Calculates expected record counts
- Returns a dictionary with all relevant boundary information

### `filter_time_range(df, start_time, end_time)`

This function filters a DataFrame to a specific time range:

- Implements the inclusive start, exclusive end boundary behavior
- Handles timezone conversion
- Provides detailed debug logs for understanding the filtering operation

## Time Boundary Rules

The system follows these rules when handling time boundaries:

1. **Record at time X belongs to interval X** - A record with timestamp X belongs to the interval starting at X
2. **Intervals are aligned to seconds** - For 1-second intervals, timestamps are aligned to whole seconds
3. **Time windowing logic is consistent** - All components use the same window calculation

## Example

For a time window from `2025-03-17 08:37:25.528448` to `2025-03-17 08:37:30.056345` with 1-second intervals:

- Adjusted start: `2025-03-17 08:37:25.000000` (rounded DOWN from 25.528448)
- Adjusted end: `2025-03-17 08:37:30.000000` (rounded DOWN from 30.056345)
- Expected records: 5 seconds (25, 26, 27, 28, 29), NOT including 30

## Usage in Codebase

The unified time boundary utilities are used in:

1. **DataSourceManager** - For fetching data with adjusted time windows
2. **Market Data Client** - For calculating chunk boundaries
3. **Vision Data Client** - For filtering and validating time ranges
4. **Test Suite** - For verifying time boundary behavior

## Testing

A dedicated test suite `tests/test_time_alignment_utils.py` verifies the behavior of the time alignment utilities, ensuring consistent behavior across various edge cases.
