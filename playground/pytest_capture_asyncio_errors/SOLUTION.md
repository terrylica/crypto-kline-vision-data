# Error Summary Table Truncation Solution

## Problem

The pytest-pretty plugin's "Summary of Failures" table was truncating important information:

- File paths were getting cut off (showing only "playground...")
- Function names were truncated
- Error types were truncated

This made it difficult to identify and fix errors without scrolling back through the entire test output.

## Root Cause

The truncation issue was caused by terminal width constraints. The pytest-pretty plugin uses the terminal width (COLUMNS environment variable) to determine how wide to make the table columns. When the terminal width is insufficient, it truncates the content.

## Solution

We implemented a two-part solution:

### 1. Set COLUMNS Environment Variable

We modified the `run_tests_parallel.sh` script to set a large value for the COLUMNS environment variable:

```bash
# Set COLUMNS to a very wide value to prevent truncation in the summary table
export COLUMNS=200

# Pass COLUMNS to ensure wide tables in the output
TEMP_CMD="PYTHONUNBUFFERED=1 FORCE_COLOR=1 PYTHONASYNCIOEBUG=1 COLUMNS=${COLUMNS} $PYTEST_CMD --color=yes"
```

This ensures that the pytest-pretty plugin has enough space to display full file paths, function names, and error types without truncation.

### 2. Added a Custom Detailed Failure Summary

For more complex scenarios, we created a separate Python script (`detailed_failure_summary.py`) that can extract and display comprehensive error information:

```python
# Run with our custom script for detailed error info
python "${SCRIPT_DIR}/detailed_failure_summary.py" "$TEST_PATH"
```

This script uses multiple approaches to extract detailed failure information and display it in a clean, readable format.

## Results

The improved error summary now shows:

- Complete file paths (`playground/pytest_capture_asyncio_errors/test_asyncio_errors.py`)
- Full function names (`test_asyncio_with_exception`)
- Line numbers for both function definitions and error locations
- Complete error types (`AssertionError`, `ValueError`, `RuntimeError`)

## Future Improvements

1. The COLUMNS setting could be made configurable
2. Additional error context could be added to the summary
3. Specialized handling for common asyncio errors could be implemented

## References

- [pytest-pretty GitHub Repository](https://github.com/samuelcolvin/pytest-pretty)
- [GitHub Actions recommendation for wide tables](https://github.com/samuelcolvin/pytest-pretty#usage-with-github-actions)
