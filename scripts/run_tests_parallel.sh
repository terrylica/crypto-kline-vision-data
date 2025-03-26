#!/bin/bash
# Run tests in parallel mode with 8 workers
#
# DESCRIPTION:
#   This script runs pytest tests in parallel using pytest-xdist to accelerate
#   test execution. It enhances testing efficiency through several key features:
#
#   - Parallel Test Execution: Employs pytest-xdist to run tests concurrently,
#     significantly reducing test times, especially for extensive test suites. By
#     default, it uses 8 worker processes, which can be adjusted via additional
#     pytest arguments.
#   - Interactive Test Selection: Offers an interactive mode, allowing users to
#     select specific test directories or files from a menu, providing focused
#     testing capabilities.
#   - Comprehensive Test Path Discovery: Automatically identifies test files and
#     directories, including both Git-tracked and newly added, untracked files,
#     ensuring all relevant tests are available for selection in interactive mode.
#   - Flexible Logging: Enables users to control the verbosity of test output
#     using a log level argument, facilitating detailed debugging or minimizing
#     output for cleaner test runs.
#   - Custom Pytest Arguments: Supports the inclusion of extra pytest command-line
#     arguments, allowing advanced users to further customize test execution behavior.
#   - asyncio Configuration:  Configures asyncio loop scope to 'function'
#     (`asyncio_default_fixture_loop_scope=function`) to prevent pytest-asyncio
#     deprecation warnings and ensure consistent behavior for asynchronous tests.
#
# USAGE:
#   ./scripts/run_tests_parallel.sh [options] [test_path] [log_level] [additional_pytest_args]
#   ./scripts/run_tests_parallel.sh -i|--interactive [log_level] [additional_pytest_args]
#
# OPTIONS:
#   -i, --interactive:  Enable interactive test selection mode. Presents a menu
#                       of test directories and files for selection, useful for
#                       focused testing.
#   -h, --help:         Show detailed help message and exit. Displays comprehensive
#                       usage instructions, options, arguments, and examples.
#
# ARGUMENTS:
#   test_path: (Optional) Path to a specific test file or directory.
#              Default: tests/ (runs all tests in the tests directory if no path is provided).
#              Examples: tests/, tests/time_boundary/, tests/test_specific.py
#              If -i or --interactive is used, this argument is ignored, and the
#              test path is selected interactively.
#
#   log_level: (Optional) Controls verbosity of test output.
#              Default: INFO (standard level of detail).
#              Options: ${CYAN}DEBUG${NC} (most verbose),
#                       ${CYAN}INFO${NC} (normal output),
#                       ${CYAN}WARNING${NC} (reduced output),
#                       ${CYAN}ERROR${NC} (least verbose).
#              Use DEBUG for detailed logs, INFO for standard test progress, and
#              WARNING or ERROR to minimize output noise, especially in CI environments.
#
#   additional_pytest_args: (Optional)  Extra arguments to pass directly to pytest.
#                           Examples: --tb=short (shorter tracebacks), -k "pattern"
#                           (run tests matching a pattern), -m "marker" (run tests
#                           with specific markers), -n4 (reduce parallel workers to 4).
#
# EXAMPLES:
#   # 1. Run all tests in the tests/ directory with standard logging (default):
#   ./scripts/run_tests_parallel.sh
#
#   # 2. Run tests interactively to select specific tests:
#   ./scripts/run_tests_parallel.sh -i
#
#   # 3. Run tests in a specific subdirectory (e.g., time_boundary) with default log level:
#   ./scripts/run_tests_parallel.sh tests/time_boundary
#
#   # 4. Display the full help message:
#   ./scripts/run_tests_parallel.sh -h
#
#   # 5. Run all tests with DEBUG log level for verbose output:
#   ./scripts/run_tests_parallel.sh tests/ DEBUG
#
#   # 6. Run tests with specific markers and WARNING log level:
#   ./scripts/run_tests_parallel.sh tests/ WARNING -m "real"
#
#   # 7. Run tests interactively with DEBUG log level and filter by test name:
#   ./scripts/run_tests_parallel.sh -i DEBUG -k "some_test"
#
#   # 8. Run all tests with reduced parallel workers (e.g., 4) and short tracebacks:
#   ./scripts/run_tests_parallel.sh tests/ --tb=short -n4
#
# BEST PRACTICES and NOTES:
#   - Interactive Test Selection:  The interactive mode smartly detects both
#     Git-tracked and untracked test files in the 'tests/' directory, ensuring
#     comprehensive test discovery for selection.
#   - asyncio Configuration: The script automatically configures
#     `asyncio_default_fixture_loop_scope=function` via pytest command-line option,
#     preventing pytest-asyncio deprecation warnings and ensuring consistent
#     async test behavior, independent of `pytest.ini` settings.
#   - Log Level Flexibility: Leverage different log levels (DEBUG, INFO, WARNING, ERROR)
#     to control output verbosity, aiding in detailed debugging or cleaner routine runs.
#   - Parallel Execution Efficiency: Parallel testing with `-n8` significantly
#     reduces test execution time. Adjust the worker count (`-n`) based on your
#     system's CPU cores and resources for optimal performance.
#   - Error Handling: The script includes basic error handling for pytest-xdist
#     installation and provides clear messages on test completion status (success/failure).
#
# LICENSE:
#   This script is provided as is, without warranty. Use it at your own risk.

set -e

# Define colors for better formatting
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Simple script configuration
SCRIPT_DIR=$(dirname "$0")
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

# Function to display help based on verbosity level
show_help() {
  local verbosity=$1

  # Always show the header
  echo -e "${BOLD}${BLUE}======================================================${NC}"
  echo -e "${BOLD}${GREEN}            PYTEST PARALLEL TEST RUNNER              ${NC}"
  echo -e "${BOLD}${BLUE}======================================================${NC}"
  
  if [[ "$verbosity" == "minimal" ]]; then
    # Minimal help - just the basics for normal operation
    echo -e "${YELLOW}Run:${NC} ${CYAN}./scripts/run_tests_parallel.sh -h${NC} ${YELLOW}for full help${NC}"
    echo -e ""
  else
    # Full help information
    echo -e "${YELLOW}Usage:${NC} ./scripts/run_tests_parallel.sh [options] [test_path] [log_level] [additional_pytest_args]"
    echo -e ""
    echo -e "${YELLOW}Options:${NC}"
    echo -e "  ${GREEN}-i, --interactive${NC} : Select tests interactively"
    echo -e "  ${GREEN}-h, --help${NC}        : Show this detailed help"
    echo -e ""
    echo -e "${YELLOW}Arguments:${NC}"
    echo -e "  ${GREEN}test_path${NC}            : Path to test file/directory (default: ${CYAN}tests/${NC})"
    echo -e "  ${GREEN}log_level${NC}            : Verbosity level (${CYAN}DEBUG${NC}|${CYAN}INFO${NC}|${CYAN}WARNING${NC}|${CYAN}ERROR${NC}) (default: ${CYAN}INFO${NC})"
    echo -e "  ${GREEN}additional_pytest_args${NC}: Extra arguments passed to pytest"
    echo -e ""
    echo -e "${YELLOW}Examples:${NC}"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh${NC}                  : Run all tests"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh -i${NC}               : Interactive mode"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh tests/cache${NC}      : Run specific tests"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh tests/ DEBUG${NC}     : With debug logging"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh tests/ INFO -k test${NC}: Filter by test name"
  fi
  
  echo -e "${BOLD}${BLUE}======================================================${NC}"
  echo -e ""
}

# Function to get all test paths (tracked and untracked)
get_test_paths() {
  # Array to hold all paths
  local all_dirs=()
  local all_files=()
  
  # First get Git-tracked files
  while IFS= read -r file; do
    if [[ "$file" == tests/* && "$file" == *test_*.py ]]; then
      all_files+=("$file")
      dir=$(dirname "$file")
      all_dirs+=("$dir")
    fi
  done < <(git ls-files "tests/**/*" | sort)
  
  # Then find all test files in the filesystem, including untracked ones
  # but exclude __pycache__ directories
  while IFS= read -r file; do
    if [[ "$file" == *test_*.py && "$file" != *__pycache__* ]]; then
      # Check if file is already in array
      if ! [[ " ${all_files[*]} " =~ " ${file} " ]]; then
        all_files+=("$file")
        dir=$(dirname "$file")
        all_dirs+=("$dir")
      fi
    fi
  done < <(find tests -type f -name "test_*.py" | sort)
  
  # Add the base tests directory
  all_dirs+=("tests")
  
  # Remove duplicates from directories
  all_dirs=($(printf '%s\n' "${all_dirs[@]}" | sort -u))
  
  # Return all paths as an array
  printf '%s\n' "${all_dirs[@]}" "${all_files[@]}"
}

# Check if help is requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  show_help "full"
  exit 0
else
  # Show minimal help at the beginning
  show_help "minimal"
fi

# Check for interactive mode
INTERACTIVE=false
if [[ "$1" == "-i" || "$1" == "--interactive" ]]; then
  INTERACTIVE=true
  shift
fi

# Set default values
if $INTERACTIVE; then
  echo "Scanning for test directories and files..."
  
  # Get all test paths
  readarray -t ALL_TEST_PATHS < <(get_test_paths)
  
  # Display options with numbers
  echo "Available test paths:"
  for i in "${!ALL_TEST_PATHS[@]}"; do
    printf "%3d) %s\n" $((i+1)) "${ALL_TEST_PATHS[$i]}"
  done
  
  # Custom path option
  CUSTOM_PATH_INDEX=$((${#ALL_TEST_PATHS[@]}+1))
  EXIT_INDEX=$((${#ALL_TEST_PATHS[@]}+2))
  printf "%3d) %s\n" $CUSTOM_PATH_INDEX "Custom Path"
  printf "%3d) %s\n" $EXIT_INDEX "Exit"
  
  # Get user selection
  while true; do
    read -p "Select a number: " selection_num
    
    if [[ "$selection_num" == "$EXIT_INDEX" ]]; then
      echo "Exiting..."
      exit 0
    elif [[ "$selection_num" == "$CUSTOM_PATH_INDEX" ]]; then
      read -p "Enter custom test path: " TEST_PATH
      break
    elif [[ "$selection_num" -ge 1 && "$selection_num" -le "${#ALL_TEST_PATHS[@]}" ]]; then
      TEST_PATH="${ALL_TEST_PATHS[$((selection_num-1))]}"
      break
    else
      echo "Invalid selection. Please try again."
    fi
  done
  
  LOG_LEVEL=${1:-INFO}
  shift 1 2>/dev/null || shift $# 2>/dev/null || true
else
  TEST_PATH=${1:-tests/}  # Default to tests/ directory if not specified
  LOG_LEVEL=${2:-INFO}               # Default to INFO log level
  shift 2 2>/dev/null || shift $# 2>/dev/null || true
fi

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
# -o asyncio_default_fixture_loop_scope=function: Sets fixture loop scope to function via ini option
# -n8: Runs tests in 8 parallel processes
PYTEST_CMD="PYTHONPATH=${PROJECT_ROOT} pytest \"${TEST_PATH}\" -vv --log-cli-level=${LOG_LEVEL} --asyncio-mode=auto -o asyncio_default_fixture_loop_scope=function ${ADDITIONAL_ARGS}"
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