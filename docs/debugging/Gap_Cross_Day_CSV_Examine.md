# Cross-Day Boundary Gap Analysis in Binance Raw CSV Files

## Overview

This document analyzes the gap issue reported at the day boundary between `2025-04-10 23:59:00` and `2025-04-11 00:01:00` in the DataSourceManager. The goal is to determine if the gap exists in the raw data files from Binance Vision API or if it's introduced during processing.

## Investigation Method

We downloaded and examined the raw CSV files for both days:

- `BTCUSDT-1m-2025-04-10.zip`
- `BTCUSDT-1m-2025-04-11.zip`

We then extracted and analyzed the last records of April 10 and first records of April 11 to identify any gaps.

## Raw Data Analysis

### Last 5 records from April 10 file 01

```csv
1744329300000000,79656.00000000,79656.00000000,79578.77000000,79652.00000000,10.29756000,1744329359999999,819802.09166970,2453,4.78062000,380563.88600160,0
1744329360000000,79651.99000000,79652.00000000,79585.38000000,79610.85000000,3.43106000,1744329419999999,273153.50873460,1688,1.42884000,113745.31175000,0
1744329420000000,79610.85000000,79638.01000000,79585.38000000,79631.93000000,7.64511000,1744329479999999,608582.83272620,1250,5.25319000,418183.44004100,0
1744329480000000,79631.93000000,79704.80000000,79631.93000000,79682.51000000,11.54221000,1744329539999999,919732.31519340,1822,6.21178000,494920.19281140,0
1744329540000000,79682.51000000,79682.51000000,79585.43000000,79607.30000000,9.47174000,1744329599999999,754193.67292690,2207,3.14044000,250010.29256440,0
```

The last record from April 10 has timestamp `1744329540000000` which translates to `2025-04-10 23:59:00 UTC`.

### First 5 records from April 11 file 02

```csv
1744329600000000,79607.30000000,79672.67000000,79575.72000000,79608.70000000,22.20316000,1744329659999999,1767870.26783440,2974,7.03810000,560393.48291640,0
1744329660000000,79608.69000000,79615.25000000,79551.28000000,79551.28000000,7.09766000,1744329719999999,564760.52715540,1748,1.53399000,122078.34152880,0
1744329720000000,79551.28000000,79553.91000000,79529.18000000,79530.57000000,7.60546000,1744329779999999,604985.29578720,1436,1.75697000,139757.45878870,0
1744329780000000,79530.57000000,79587.52000000,79499.11000000,79503.93000000,75.55190000,1744329839999999,6008976.92800840,2858,17.74487000,1411389.76337930,0
1744329840000000,79503.93000000,79601.06000000,79453.93000000,79571.78000000,68.28371000,1744329899999999,5428417.12671010,4866,17.69043000,1407059.76129240,0
```

The first record from April 11 has timestamp `1744329600000000` which translates to `2025-04-11 00:00:00 UTC`.

### Timestamp Sequence Analysis

The sequence of timestamps at the day boundary:

- `2025-04-10 23:59:00 UTC` (last record from April 10)
- `2025-04-11 00:00:00 UTC` (first record from April 11)
- `2025-04-11 00:01:00 UTC` (second record from April 11)

## Findings

**Conclusion**: There is **no gap** in the raw CSV data files from Binance Vision API at the day boundary.

The data shows a continuous sequence of 1-minute candles:

- 23:59:00 (from April 10 file)
- 00:00:00 (from April 11 file)
- 00:01:00 (from April 11 file)

## Identified Code Issue

The issue appears to be in the `VisionDataClient` class when merging data from multiple daily files. Here's the problematic flow:

1. In `VisionDataClient._download_file()`, the code properly downloads and processes each day's file
2. The `VisionDataClient._download_data()` method detects if a file has certain critical timestamps:

   ```python
   # Check for boundary timestamps in the data
   has_23_59 = (df["open_time"] == boundary_times[0]).any()
   has_00_00 = (df["open_time"] == boundary_times[1]).any()
   has_00_01 = (df["open_time"] == boundary_times[2]).any()
   ```

3. The issue occurs in the day boundary gap detection logic:

   ```python
   # Check for day boundary transition gap (23:XX -> 00:XX/01:XX)
   if (
       prev_row["hour"] == 23
       and curr_row["hour"] in [0, 1]
       and curr_row["time_diff"] > expected_interval * 1.5
   ):
       logger.warning(
           f"Day boundary gap detected at index {i}: "
           f"{prev_row['open_time']} -> {curr_row['open_time']} "
           f"({curr_row['time_diff']}s, expected {expected_interval}s)"
       )
   ```

4. The gap detection logic correctly identifies the file transition point, but fails to realize that the "missing 00:00:00" timestamp actually exists in the April 11 file.

5. The log showed:

   ```ba
   File for 2025-04-10 has 00:00 record: False
   File for 2025-04-11 has 00:00 record: False
   ```

   This suggests that each file check is only looking for records belonging to that day's date, instead of checking the next day file for the 00:00:00 record.

## Potential Issues in Processing

1. **File Merging Logic**: When the `VisionDataClient` merges data from multiple files, it calculates time differences after the merge. This causes it to detect a "gap" at day boundaries even when the data is complete, because it's not recognizing that the 00:00:00 record from the next day should connect seamlessly with the 23:59:00 record.

2. **Day Boundary Check Logic**: The `_fix_day_boundary_gaps` function in `DataSourceManager` looks specifically for:

   ```python
   if (
       prev_time.hour == 23
       and prev_time.minute >= 59
       and curr_time.hour == 0
       and curr_time.minute >= 1
   ):
   ```

   This is looking for 23:59 → 00:01 gaps, assuming 00:00 is missing. But in the raw data, 00:00 exists.

3. **Data Source Prioritization**: The DataSourceManager prioritizes different data sources (cache → Vision → REST). Since the Vision API reports a gap that doesn't actually exist, it may cause unnecessary fallback to REST API data when the Vision data is actually complete.

## Recommendations

1. **Fix `VisionDataClient` File Merging**: Modify the file merging logic to properly handle day boundary transitions. When checking for gaps, consider that adjacent files contain the boundary records:

   ```python
   # Modify the day boundary detection to check specifically for 23:59 → 00:01 gaps
   # where 00:00 is missing, rather than using a general time difference check
   ```

2. **Update Gap Detection Logic**: Rather than using the `time_diff` > threshold check, specifically look for missing 00:00:00 timestamp after merging:

   ```python
   # After merging files, check each day boundary if 00:00:00 record exists
   for date in dates[:-1]:  # Skip the last date
       midnight = datetime(date.year, date.month, date.day+1, 0, 0, 0, tzinfo=timezone.utc)
       if not ((df['open_time'] - midnight).abs().min() < timedelta(seconds=1)).any():
           # Missing midnight record, interpolate
   ```

3. **Add Validation Step**: Add a validation step that prints the full sequence of records at day boundaries to verify merging:

   ```python
   # Display records around day boundaries for verification
   for i in range(1, len(df)):
       if df.iloc[i-1]['open_time'].day != df.iloc[i]['open_time'].day:
           logger.debug(f"Day boundary: {df.iloc[i-1]['open_time']} → {df.iloc[i]['open_time']}")
   ```

## Next Steps

1. **Fix the `VisionDataClient` File Merging Logic**: Update the code to properly combine data across day boundaries without incorrectly detecting gaps.

2. **Update the `_fix_day_boundary_gaps` Method**: Ensure this method only interpolates records when truly missing, not when records exist in different files.

3. **Add Specific Tests**: Add tests that specifically validate day boundary transitions with real data from multiple days.

4. **Consider File Pre-Check**: Before reporting a gap, check if the next day's file explicitly contains the 00:00:00 record.

By addressing these issues, the DataSourceManager should be able to handle day boundary transitions correctly and avoid unnecessary fallbacks to the REST API.
