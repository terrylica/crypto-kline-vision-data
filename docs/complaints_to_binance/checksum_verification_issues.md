# Binance Data Checksum Verification Issues Report

## Summary

We have identified a systematic issue with checksum verification for historical data files from Binance's public data repository. Our investigation shows that while the data content appears valid, there are consistent checksum mismatches that correlate with file modification dates.

## Key Findings

1. **1-Second Data Files Modified in April 2023**

   - Example file: `BTCUSDT-1s-2022-12-01.zip`
   - Expected checksum: `b0badded6508aefec4359938215a1991a976196eaab2dba14d9602ae4bbae1b6`
   - Actual checksum: `d782820021eb67f558cfd3909075baa7cbebfa954d3328e158e65b1cceedd006`
   - Last modified: Thu, 13 Apr 2023 10:23:51 GMT
   - File size: 3,595,694 bytes
   - Line count: 86,400 (correct for 1-second data)

2. **Corresponding 1-Minute Data Remains Unchanged**
   - Example file: `BTCUSDT-1m-2022-12-01.zip`
   - Checksum matches expected value
   - Last modified: Fri, 02 Dec 2022 02:50:54 GMT (original creation date)
   - File size: 74,249 bytes
   - Line count: 1,440 (correct for 1-minute data)

## Data Integrity Verification

Our manual verification shows:

1. All files contain the correct number of data points
2. Timestamps are properly sequential
3. Data values are consistent between 1-second and 1-minute files
4. ZIP file integrity is maintained

## Issue Pattern

The checksum mismatches follow a clear pattern:

1. Only affects 1-second data files
2. Files were modified on April 13, 2023
3. Data content appears valid despite checksum mismatches
4. Corresponding 1-minute data files remain unaffected

## Request

We kindly request:

1. Confirmation whether the April 2023 modifications were intentional
2. If intentional, updated checksum files to match the current data
3. Documentation of any data reprocessing or modifications

## Contact Information

For further discussion or clarification:
Terry Li <terry@eonlabs.com>
Director of Operations, Eon Labs Ltd.
