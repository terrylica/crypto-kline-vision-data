# Utility Modules

This directory contains consolidated utility modules used throughout the codebase, providing standardized functionality for various common operations.

## Overview

- **time_utils.py**: Centralized time-related utilities, including timezone handling, interval calculations, and time boundary alignment.
- **validation.py**: Core validation classes (DataValidation, DataFrameValidator) for ensuring data integrity and validation.
- **validation_utils.py**: [DEPRECATED] Legacy validation utilities - use validation.py instead.
- **network_utils.py**: Unified network functionality, including HTTP client creation, file downloads with retry logic, and standardized API request handling.
- **deprecation_rules.py**: Provides utilities for handling and enforcing function deprecation.

## Consolidation Strategy

The utility modules in this directory are part of a consolidation effort to reduce code duplication and standardize functionality across the codebase. The approach involves:

1. Moving related functions from various modules into consolidated utility modules
2. Adding appropriate deprecation warnings to original functions
3. Updating imports and usage across the codebase
4. Ensuring comprehensive test coverage for the consolidated utilities

## Time Utilities (`time_utils.py`)

Time-related utilities consolidated from:

- `TimeRangeManager` in `api_boundary_validator.py`
- `TimeRangeManager` in other modules
- Timezone utilities from various modules

Key functions:

- `enforce_utc_timezone`: Ensures datetime objects are timezone-aware and in UTC
- `get_interval_*`: Functions for interval calculations
- `align_time_boundaries`: Aligns time boundaries based on interval

## Validation Modules (`validation.py`)

Primary validation classes consolidating functionality from legacy validation modules:

Key classes:

- `DataValidation`: Provides static methods for validating data integrity
  - `validate_time_window`: Validates time windows for market data operations
  - `validate_time_range`: Normalizes and validates time range parameters
  - `validate_dates`: Validates that datetimes are proper and timezone-aware
  - `validate_symbol_format`: Validates trading pair symbol format
- `DataFrameValidator`: Provides static methods for validating and formatting DataFrames
  - `validate_dataframe`: Validates DataFrame structure and integrity
  - `format_dataframe`: Formats DataFrames to ensure consistent structure

## Validation Utilities (`validation_utils.py`) [DEPRECATED]

Legacy validation utilities that have been migrated to `validation.py`:

- `validate_dataframe` → `DataFrameValidator.validate_dataframe`
- `format_dataframe` → `DataFrameValidator.format_dataframe`
- `validate_time_window` → `DataValidation.validate_time_window`
- `validate_time_range` → `DataValidation.validate_time_range`
- `validate_dates` → `DataValidation.validate_dates`
- `ApiValidator` → Use direct API service modules
- `DataValidator` → Use `DataFrameValidator` and service-specific validation

## Network Utilities (`network_utils.py`)

Network utilities consolidated from:

- `http_client_factory.py`
- `download_handler.py`
- Network-related code from various modules

Key functions and classes:

- HTTP client factories (`create_client`, `create_aiohttp_client`, `create_httpx_client`)
- `DownloadHandler`: Handles file downloads with retry logic and progress monitoring
- `download_files_concurrently`: Downloads multiple files with controlled parallelism
- `make_api_request`: Makes API requests with automatic retry and error handling

## Usage Guidelines

- Prefer using these consolidated utilities rather than implementation-specific utilities
- When modifying these utilities, ensure thorough test coverage
- Consider the impact on dependent modules when making changes

## Migration Path

The codebase is in the process of migrating to these consolidated utilities. The original functions remain available temporarily with deprecation warnings to ensure a smooth transition.

## Migration from validation_utils.py

We're gradually migrating away from `validation_utils.py` to the more modern class-based approach in `validation.py`. Here's our progress:

### Completed Steps

- Added deprecation warnings to all functions in `validation_utils.py`
- Implemented equivalent functionality in `DataValidation` and `DataFrameValidator` classes
- Fixed the duplicate `is_data_likely_available` method in `DataValidation`
- Updated `rest_data_client.py` to use `DataValidation.enforce_utc_timestamp` instead of `enforce_utc_timezone`
- Ensured backward compatibility during the transition period
- Verified all tests pass with the new implementation

### Remaining Work

- Continue monitoring for direct usage of `validation_utils.py` in the codebase
- Update any remaining code that imports directly from `validation_utils.py`
- Update documentation references to point to `validation.py` instead
- After a suitable deprecation period, remove `validation_utils.py` entirely

### Migration Guide for Developers

When migrating code from `validation_utils.py` to `validation.py`, use this mapping:

| Old (validation_utils.py)                     | New (validation.py)                                   |
| --------------------------------------------- | ----------------------------------------------------- |
| `validate_dates()`                            | `DataValidation.validate_dates()`                     |
| `validate_interval()`                         | `DataValidation.validate_interval()`                  |
| `validate_symbol()`                           | `DataValidation.validate_symbol_format()`             |
| `validate_dataframe()`                        | `DataFrameValidator.validate_dataframe()`             |
| `format_dataframe()`                          | `DataFrameValidator.format_dataframe()`               |
| `calculate_checksum()`                        | `DataValidation.calculate_checksum()`                 |
| `is_data_likely_available()`                  | `DataValidation.is_data_likely_available()`           |
| `validate_dataframe_time_boundaries()`        | `DataValidation.validate_dataframe_time_boundaries()` |
| `enforce_utc_timezone()` (from time_utils.py) | `DataValidation.enforce_utc_timestamp()`              |
