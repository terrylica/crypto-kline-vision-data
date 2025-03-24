#!/bin/bash
# Run tests in parallel mode with 8 workers
#
# DESCRIPTION:
#   This script runs pytest tests in parallel mode using pytest-xdist to speed up
#   test execution. It automatically uses 8 worker processes for parallelization.
#   Use this script when you want to run tests faster by utilizing multiple CPU cores.
#
# USAGE:
#   ./scripts/run_tests_parallel.sh [test_path] [log_level] [additional_pytest_args]
#
# ARGUMENTS:
#   test_path: (Optional) Path to specific test file or directory to run
#              Default: tests/interval_1s
#              Examples: tests/, tests/interval_1s/, tests/test_specific.py
#
#   log_level: (Optional) Controls verbosity of test output
#              Default: INFO
#              Options: DEBUG (most verbose), INFO (normal), WARNING, ERROR (least verbose)
#              - Use DEBUG when you need to see detailed logging information
#              - Use INFO for standard test output
#              - Use WARNING or ERROR to reduce output noise
#
#   additional_pytest_args: (Optional) Any extra pytest arguments
#                          These are passed directly to pytest
#
# EXAMPLES:
#   # Run all tests in the tests/interval_1s directory with standard logging:
#   ./scripts/run_tests_parallel.sh
#
#   # Run all tests with normal output:
#   ./scripts/run_tests_parallel.sh tests/
#
#   # Run 1-second interval tests with very verbose output (useful for debugging):
#   ./scripts/run_tests_parallel.sh tests/interval_1s/ DEBUG
#
#   # Run a specific test file with shorter tracebacks:
#   ./scripts/run_tests_parallel.sh tests/test_file.py --tb=short
#
#   # Run tests matching a specific pattern:
#   ./scripts/run_tests_parallel.sh tests/ INFO -k "test_pattern"
#
#   # Run tests with specific markers:
#   ./scripts/run_tests_parallel.sh tests/ INFO -m "real"

set -e

# Simple script configuration
SCRIPT_DIR=$(dirname "$0")
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

# Set default values
TEST_PATH=${1:-tests/interval_1s}  # Default to interval_1s tests if not specified
LOG_LEVEL=${2:-INFO}               # Default to INFO log level
shift 2 2>/dev/null || shift $# 2>/dev/null || true
ADDITIONAL_ARGS="$* -n8"           # Always run with 8 parallel workers

# Install pytest-xdist if not installed
if ! python -c "import pytest; import xdist" 2>/dev/null; then
    echo "Installing pytest-xdist for parallel testing..."
    pip install pytest-xdist
fi

# Display basic info
echo "Running parallel tests (using 8 worker processes)"
echo "Test path: $TEST_PATH"
echo "Log level: $LOG_LEVEL (higher = more detailed output)"
echo "Additional args: $ADDITIONAL_ARGS"
echo "---------------------------------------------------"

# Construct and run the pytest command
# -vv: Increases verbosity
# --log-cli-level: Controls logging detail level
# --asyncio-mode=auto: Manages asyncio behavior
# -n8: Runs tests in 8 parallel processes
PYTEST_CMD="PYTHONPATH=${PROJECT_ROOT} pytest \"${TEST_PATH}\" -vv --log-cli-level=${LOG_LEVEL} --asyncio-mode=auto ${ADDITIONAL_ARGS}"
echo "Running: $PYTEST_CMD"
echo "---------------------------------------------------"

# Run the command
eval "$PYTEST_CMD"

PYTEST_EXIT_CODE=$?

if [ $PYTEST_EXIT_CODE -eq 0 ]; then
  echo "Tests completed successfully!"
else
  echo "Tests failed with exit code $PYTEST_EXIT_CODE"
fi

exit $PYTEST_EXIT_CODE 