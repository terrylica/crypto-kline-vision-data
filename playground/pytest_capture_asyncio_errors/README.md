# Enhanced Asyncio Error Reporting

This directory contains tests and tools for enhanced error reporting in pytest, specifically focusing on asyncio-related errors.

## Features

### 1. Enhanced Error Summary

The error summary has been improved to show full file paths, function names, and error types without truncation. This makes it easier to identify and fix issues in your asyncio tests.

### 2. Detailed Failure Table

A comprehensive failure summary is provided with:

- Complete file paths
- Full function names
- Function definition line numbers
- Error line numbers
- Complete error types and messages

## Usage

Run tests with the `-e` flag to enable the enhanced error summary:

```bash
./scripts/op/run_tests_parallel.sh -e tests/your_test_file.py
```

### How It Works

The enhanced error reporting works by:

1. Setting the `COLUMNS` environment variable to a larger value (200) to prevent truncation in the pytest-pretty summary table
2. Running tests with `PYTHONASYNCIOEBUG=1` to enable more detailed asyncio error reporting
3. Using a custom Python script to extract and display detailed failure information

## Common Asyncio Errors

- **Task was destroyed but it is pending**: This occurs when you create a task but don't properly await it
- **Task exception was never retrieved**: This happens when an exception occurs in a task that isn't properly handled
- **Coroutine was never awaited**: This happens when you forget to await a coroutine

## Recommendations

1. Always await coroutines and asyncio tasks
2. Use proper exception handling for asyncio tasks
3. Clean up tasks properly to avoid pending task warnings
