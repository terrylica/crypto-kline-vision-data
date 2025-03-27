# Utility Modules

This directory contains consolidated utility modules used throughout the codebase, providing standardized functionality for various common operations.

## Overview

- **time_utils.py**: Centralized time-related utilities, including date validation, timezone handling, interval calculations, and time boundary alignment.
- **validation_utils.py**: Consolidated validation utilities for ensuring data integrity, including DataFrame validation, API boundary validation, and cache validation.
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
- `validate_time_window`: Validates time windows for market data operations
- `get_interval_*`: Functions for interval calculations
- `align_time_boundaries`: Aligns time boundaries based on interval

## Validation Utilities (`validation_utils.py`)

Validation utilities consolidated from:

- `api_boundary_validator.py`
- `cache_validator.py`
- `validation.py`
- Other validation logic

Key functions:

- `validate_dataframe`: Validates DataFrame structure and integrity
- `format_dataframe`: Formats DataFrames to ensure consistent structure
- `validate_cache_integrity`: Validates cache file integrity
- `calculate_checksum`: Calculates SHA-256 checksums for files

Key classes:

- `ApiValidator`: For API-specific validations
- `DataValidator`: For comprehensive data validations

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
