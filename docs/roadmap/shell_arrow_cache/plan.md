# Shell Arrow Cache Population Plan

## Goal

Create a robust shell script to populate the local cache with historical market data from Binance Vision API for specified symbols and intervals. This will use direct file operations and a synchronous approach for maximum reliability.

## Current State Understanding

- We need to access the Binance Vision API for downloadable zipped data files
- The cache is stored in a hierarchical structure: `{cache_dir}/{provider}/{chart_type}/{symbol}/{interval}/{date}.arrow`
- We have a list of symbols in `spot_synchronal.csv` with their earliest available dates and supported intervals

## Implementation Status

We have implemented a complete solution for building the cache using the following components:

1. **Python Script (`scripts/arrow_cache/cache_builder_sync.py`)**

   - Parses symbol information from the CSV file
   - Supports filtering by symbol, interval, and date range
   - Uses direct file system operations for all cache reads/writes
   - Provides detailed progress reporting and error handling
   - Completely synchronous to avoid any hanging issues
   - Uses standard Python libraries and PyArrow for file operations
   - Leverages ThreadPoolExecutor for controlled concurrency
   - Implements robust checksum verification and failure tracking

2. **Shell Wrapper (`scripts/arrow_cache/cache_builder.sh`)**

   - Provides a user-friendly command-line interface
   - Supports both test and production modes
   - Handles default parameter configuration
   - Manages logging and error reporting
   - Supports flexible parameter overrides
   - Includes options for controlling checksum verification

3. **Checksum Failure Management (`scripts/arrow_cache/view_checksum_failures.sh`)**

   - View and manage checksum failures
   - Generate summary statistics
   - Filter failures by symbol or interval
   - Retry failed downloads
   - Clear and backup the failures registry

4. **Implementation Features**
   - Robust error handling at multiple levels
   - Direct file system operations for maximum reliability
   - Progress tracking and reporting
   - Support for both direct symbol specification and CSV file input
   - Graceful shutdown with signal handling
   - Comprehensive data integrity verification
   - Flexible checksum failure handling

> **Note**: We have completely removed the asynchronous implementation (`cache_builder.py`) from the codebase, standardizing on the synchronous version to prevent confusion and ensure consistency.

## Checksum Verification

### Overview

For data integrity, our implementation includes robust checksum verification of downloaded data files from Binance Vision API:

1. **Download Process with Checksums**

   - For each date and symbol/interval combination:
     - Download the data ZIP file
     - Download the corresponding checksum file (.CHECKSUM)
     - Verify the ZIP file integrity by comparing its SHA-256 hash with the expected checksum

2. **Checksum Verification Implementation**

   - Use `calculate_sha256(file_path)` to compute SHA-256 checksum of downloaded files
   - Compare with expected checksum from .CHECKSUM file
   - Log detailed information about checksum verification process and any failures

3. **Handling Checksum Failures**

   - Log detailed error information including expected vs. actual checksum
   - Create a dedicated "checksum_failures.log" file to track all checksum mismatches
   - Implement a JSON-based registry of checksum failures for programmatic access
   - Add a "skip-checksum" option for users who need to proceed despite checksum mismatches
   - Add a "retry-failed-checksums" option to attempt re-downloading files with checksum mismatches

4. **Checksum Failure Registry**
   - Store structured information about each failure:
     ```json
     {
       "symbol": "BTCUSDT",
       "interval": "5m",
       "date": "2024-01-01",
       "expected_checksum": "expected_hash_value",
       "actual_checksum": "actual_hash_value",
       "timestamp": "2025-04-11T12:34:56Z",
       "action_taken": "skipped|cached_anyway|retried_success|retried_failed"
     }
     ```
   - Allow reviewing and managing checksum failures through helper scripts

### Usage Examples

For testing with default parameters (3 symbols, 5m interval, recent data):

```bash
./scripts/arrow_cache/cache_builder.sh
```

For specific symbols and intervals:

```bash
./scripts/arrow_cache/cache_builder.sh --symbols BTCUSDT,ETHUSDT --intervals 1m,5m --start-date 2024-01-01
```

For production mode with all symbols and intervals:

```bash
./scripts/arrow_cache/cache_builder.sh --mode production --start-date 2023-01-01
```

With checksum failure handling options:

```bash
# Skip checksum verification entirely
./scripts/arrow_cache/cache_builder.sh --symbols BTCUSDT --skip-checksum

# Retry previously failed checksums
./scripts/arrow_cache/cache_builder.sh --retry-failed-checksums

# Proceed even on checksum failures (but still log them)
./scripts/arrow_cache/cache_builder.sh --symbols BTCUSDT --proceed-on-checksum-failure
```

View checksum failure report:

```bash
./scripts/arrow_cache/view_checksum_failures.sh
```

## Project Plan Status

### 1. Initial Test Phase (✅ Complete)

- ✅ Create the synchronous Python script (`scripts/arrow_cache/cache_builder_sync.py`)
- ✅ Test with limited symbols and date ranges
- ✅ Verify direct file operations work correctly
- ✅ Implement controlled concurrency with ThreadPoolExecutor

### 2. Shell Wrapper (✅ Complete)

- ✅ Create a shell script wrapper
- ✅ Add parameter handling
- ✅ Add logging and error reporting
- ✅ Implement test and production modes

### 3. Checksum Verification Improvements (✅ Complete)

- ✅ Implement dedicated checksum verification function
- ✅ Create checksum failure registry
- ✅ Add checksum failure log
- ✅ Implement command-line options for checksum handling
- ✅ Create helper script for viewing and managing checksum failures

### 4. Final Integration (✅ Complete)

- ✅ Test with real production data
- ✅ Verify cache directory structure
- ✅ Ensure proper error handling
- ✅ Implement graceful shutdown

### 5. Code Cleanup (✅ Complete)

- ✅ Remove the async implementation (`cache_builder.py`)
- ✅ Update documentation to reflect the standardization on synchronous approach
- ✅ Update README.md with implementation note
- ✅ Ensure consistent code style and conventions

## Future Enhancements

Based on our experience with the current implementation, we have identified several areas for future enhancement:

### 1. Incremental Update Mode

- [ ] Implement an incremental update mode that only downloads missing or modified data
- [ ] Add date range detection to automatically identify gaps in the cache
- [ ] Create a periodic update scheduler for automated maintenance
- [ ] Implement a "force-update" option to re-download data regardless of cache status

### 2. Cache Metadata and Indexing

- [ ] Create a metadata index for the cache (`cache_index.json`)
- [ ] Store information about each cached file including:
  - Last update timestamp
  - Number of records
  - Checksum information
  - File size
  - Symbol/interval statistics
- [ ] Implement functions to query the metadata without loading full cache files
- [ ] Add cache validation tools to verify integrity of the entire cache

### 3. Performance Optimizations

- [ ] Implement dynamic concurrency adjustment based on system capabilities
- [ ] Add memory usage monitoring to prevent out-of-memory conditions
- [ ] Implement adaptive retry mechanisms with exponential backoff
- [ ] Add bandwidth throttling options for rate-limited environments
- [ ] Optimize file operations with batch processing

### 4. User Interface Improvements

- [ ] Create a basic web interface for cache management
- [ ] Implement real-time progress monitoring
- [ ] Add visualization of cache coverage and completeness
- [ ] Create interactive reports of cache statistics
- [ ] Implement email/notification alerts for failed downloads

### 5. Advanced Data Quality Features

- [ ] Implement anomaly detection in cached data
- [ ] Add data validation beyond checksums (e.g., range checks, consistency validations)
- [ ] Create comparison tools to cross-reference data from multiple sources
- [ ] Add support for additional data integrity checks (e.g., CRC, digital signatures)
- [ ] Implement automatic detection and handling of data quality issues

### 6. Integration Enhancements

- [ ] Create API endpoints for programmatic access to the cache
- [ ] Implement integration with common data analysis frameworks
- [ ] Add support for additional file formats (Parquet, CSV, JSON)
- [ ] Create plugins for popular trading platforms
- [ ] Implement distributed cache building across multiple nodes

## Implementation Timeline

| Phase | Features                       | Timeline |
| ----- | ------------------------------ | -------- |
| 1     | Incremental Update Mode        | Q2 2025  |
| 2     | Cache Metadata and Indexing    | Q3 2025  |
| 3     | Performance Optimizations      | Q3 2025  |
| 4     | User Interface Improvements    | Q4 2025  |
| 5     | Advanced Data Quality Features | Q1 2026  |
| 6     | Integration Enhancements       | Q2 2026  |
