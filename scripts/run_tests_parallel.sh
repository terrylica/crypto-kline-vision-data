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
#   - Sequential Execution Option: Provides an option to run tests sequentially
#     (without parallelism) for debugging complex test interactions or when
#     troubleshooting race conditions.
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
#   - Error Summary: Collects and displays a summary of all errors and warnings,
#     even from passing tests, to help identify issues that don't cause test failures.
#   - Enhanced Asyncio Error Detection: Provides specialized detection and reporting
#     for common asyncio issues like task destruction errors, with severity scoring
#     to prioritize the most critical problems.
#   - Profiling Capabilities: Supports performance profiling via pytest-profiling
#     with options to generate line-by-line profiling data (.prof files) and
#     SVG visualizations of the call graph.
#
# USAGE:
#   ./scripts/run_tests_parallel.sh [options] [test_path] [log_level] [additional_pytest_args]
#   ./scripts/run_tests_parallel.sh -i|--interactive [log_level] [additional_pytest_args]
#
# OPTIONS:
#   -i, --interactive:  Enable interactive test selection mode. Presents a menu
#                       of test directories and files for selection, useful for
#                       focused testing.
#   -s, --sequential:   Run tests sequentially (without parallelism). Useful for
#                       debugging race conditions or complex test interactions.
#   -h, --help:         Show detailed help message and exit. Displays comprehensive
#                       usage instructions, options, arguments, and examples.
#   -e, --error-summary: Generate a summary of all errors and warnings at the end,
#                        even from passing tests. Includes severity scoring (1-10)
#                        to prioritize critical issues.
#   -a, --analyze-log FILE: Analyze an existing log file for errors and warnings
#                        without running tests. Useful for post-run analysis.
#   -p, --profile:      Enable profiling and generate .prof files for performance
#                       analysis. Forces sequential mode for accurate results.
#   -g, --profile-svg:  Generate SVG visualizations of the profile (requires graphviz).
#                       Must be used with -p/--profile option.
#   -c, --clear:        Clear the screen before test execution.
#                       In interactive mode, clears after test selection. 
#                       In non-interactive mode, clears before displaying test information.
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
#              Options: DEBUG (most verbose),
#                       INFO (normal output),
#                       WARNING (reduced output),
#                       ERROR (least verbose).
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
#   # 9. Run tests and show a summary of all errors, even from passing tests:
#   ./scripts/run_tests_parallel.sh -e
#
#   # 10. Analyze errors in a previously generated log file:
#   ./scripts/run_tests_parallel.sh -a test_output.log
#
#   # 11. Run asyncio-heavy tests with error summary for better debugging:
#   ./scripts/run_tests_parallel.sh tests/time_boundary -e
#
#   # 12. Run tests sequentially (without parallelism) for debugging:
#   ./scripts/run_tests_parallel.sh -s
#
#   # 13. Run specific tests sequentially with error summary:
#   ./scripts/run_tests_parallel.sh -s -e tests/time_boundary
#
# BEST PRACTICES and NOTES:
#   - Interactive Test Selection:  The interactive mode smartly detects both
#     Git-tracked and untracked test files in the 'tests/' directory, ensuring
#     comprehensive test discovery for selection.
#   - Sequential vs Parallel: Use sequential mode (-s) when debugging test interactions
#     or race conditions that might be masked by parallel execution. Use parallel mode
#     (default) for faster execution in CI/CD pipelines and routine test runs.
#   - asyncio Configuration: The script automatically configures
#     `asyncio_default_fixture_loop_scope=function` via pytest command-line option,
#     preventing pytest-asyncio deprecation warnings and ensuring consistent
#     async test behavior, independent of `pytest.ini` settings. This is critical
#     for proper cleanup of asyncio resources between tests.
#   - Log Level Flexibility: Leverage different log levels (DEBUG, INFO, WARNING, ERROR)
#     to control output verbosity, aiding in detailed debugging or cleaner routine runs.
#   - Parallel Execution Efficiency: Parallel testing with `-n8` significantly
#     reduces test execution time. Adjust the worker count (`-n`) based on your
#     system's CPU cores and resources for optimal performance.
#   - Error Handling: The script includes basic error handling for pytest-xdist
#     installation and provides clear messages on test completion status (success/failure).
#   - Error Summary: Use the -e/--error-summary option to see all errors and warnings
#     logged during tests, even from passing tests, helping identify hidden issues.
#   - Error Severity Scoring: The error summary includes a severity score (1-10) for
#     each error type, with asyncio task destruction (9/10) and connection errors (8/10)
#     being the most critical to address.
#   - Asyncio Troubleshooting: For tests with asyncio issues, the script provides
#     targeted diagnostics that help identify tasks that weren't properly awaited
#     or were destroyed while still pending.
#   - Profiling Capabilities: Supports performance profiling via pytest-profiling
#     with options to generate line-by-line profiling data (.prof files) and
#     SVG visualizations of the call graph.
#
# LICENSE:
#   This script is provided as is, without warranty. Use it at your own risk.

# Source the enhanced error extraction script for better asyncio error reporting
if [[ -f "$(dirname "$0")/enhanced_extract_errors.sh" ]]; then
  source "$(dirname "$0")/enhanced_extract_errors.sh"
else
  # Define a simple fallback function if the enhanced error extraction script doesn't exist
  enhanced_extract_errors() {
    echo "Using simple error extraction (enhanced_extract_errors.sh not found)"
    extract_errors "$1"
  }
fi

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

# Create a temp file for error logs
ERROR_LOG_FILE=$(mktemp)
trap 'rm -f "$ERROR_LOG_FILE"' EXIT

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
    echo -e "  ${GREEN}-s, --sequential${NC}  : Run tests sequentially (not in parallel)"
    echo -e "  ${GREEN}-e, --error-summary${NC}: Show summary of all errors and warnings"
    echo -e "  ${GREEN}-a, --analyze-log FILE${NC}: Analyze an existing log file for errors and warnings"
    echo -e "  ${GREEN}-p, --profile${NC}    : Enable profiling and save .prof files"
    echo -e "  ${GREEN}-g, --profile-svg${NC}: Generate SVG visual profiles (requires graphviz)"
    echo -e "  ${GREEN}-c, --clear${NC}      : Clear screen before test execution"
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
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh -s${NC}               : Sequential mode"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh -e${NC}               : Show all errors"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh tests/cache${NC}      : Run specific tests"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh tests/ DEBUG${NC}     : With debug logging"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh tests/ INFO -k test${NC}: Filter by test name"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh -a test_output.log${NC} : Analyze existing log"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh -e -n4${NC}           : Error summary with 4 workers"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh -s -e${NC}            : Sequential with error summary"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh -p tests/intervals${NC}: Profile tests in intervals directory"
    echo -e "  ${CYAN}./scripts/run_tests_parallel.sh -p -g${NC}            : Profile with SVG visualization"
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

# Function to extract and summarize errors from test output
extract_errors() {
  local log_file=$1
  
  echo -e "\n${BOLD}${BLUE}======================================================${NC}"
  echo -e "${BOLD}${RED}                  ERROR SUMMARY                       ${NC}"
  echo -e "${BOLD}${BLUE}======================================================${NC}"
  echo -e "${YELLOW}All errors and warnings, even from passing tests:${NC}\n"
  
  # Initialize counters and data structures
  local error_count=0
  local warning_count=0
  local current_test=""
  local has_errors=false
  local json_errors=()
  
  # Define severity scores for different error types
  local -A severity_scores=(
    ["asyncio_task_destroyed"]=9
    ["asyncio_task_cancelled"]=7
    ["logging_error"]=6
    ["connection_error"]=8
    ["timeout_error"]=7
    ["assertion_error"]=5
    ["standard_error"]=4
    ["warning"]=2
  )
  
  # Define a section for special errors with better structure
  local special_errors=""
  local has_special_errors=false
  local special_error_count=0
  local in_task_error=false
  
  # Map to track asyncio errors by test
  local -A asyncio_errors_by_test
  local current_asyncio_error=""
  
  # First, check for any asyncio task destruction errors, even without context
  if grep "Task was destroyed but it is pending" "$log_file" > /dev/null; then
    # Extract all task destruction error contexts - collect all related lines
    local task_errors=$(grep -A 2 -B 2 "Task was destroyed but it is pending" "$log_file")
    
    # Try to identify which test case this error belongs to
    while IFS= read -r line; do
      if [[ "$line" =~ tests/.*::test_.* ]]; then
        # Extract test name - handles both normal and parameterized tests
        if [[ "$line" =~ tests/[^\ ]+::test_[^\ \[\]]+ ]]; then
          local test_name=$(echo "$line" | grep -o "tests/[^[:space:]]*::test_[^[:space:]^[^]]*")
          # For parameterized tests, try to extract the parameter too
          if [[ "$line" =~ tests/[^\ ]+::test_[^\ \[\]]+\[[^\]]+\] ]]; then
            local param=$(echo "$line" | grep -o "\[[^]]*\]")
            test_name="${test_name}${param}"
          fi
          
          # Store in the map
          if [[ -n "${asyncio_errors_by_test[$test_name]}" ]]; then
            asyncio_errors_by_test[$test_name]="${asyncio_errors_by_test[$test_name]}\n${task_errors}"
          else
            asyncio_errors_by_test[$test_name]="$task_errors"
          fi
        fi
      fi
    done < <(grep -B 10 "Task was destroyed but it is pending" "$log_file")
    
    has_special_errors=true
    special_error_count=$(grep -c "Task was destroyed but it is pending" "$log_file")
    special_errors+="${RED}${BOLD}Found ${special_error_count} asyncio task destruction errors (Severity: ${severity_scores["asyncio_task_destroyed"]}/10)${NC}\n"
    
    # If we couldn't associate errors with specific tests
    if [[ ${#asyncio_errors_by_test[@]} -eq 0 ]]; then
      special_errors+="${RED}${task_errors}${NC}\n\n"
    fi
    
    # Mark these lines as already processed
    in_task_error=true
  fi
  
  # Check for task cancellation errors with test context
  if grep -A 10 -B 3 "Task .* was cancelled" "$log_file" > /tmp/task_cancel_errors.txt; then
    if [[ -s /tmp/task_cancel_errors.txt ]]; then
      has_special_errors=true
      local cancel_count=$(grep -c "Task .* was cancelled" "$log_file")
      special_error_count=$((special_error_count + cancel_count))
      special_errors+="${RED}${BOLD}Task cancellation error detected (${cancel_count} occurrences) (Severity: ${severity_scores["asyncio_task_cancelled"]}/10)${NC}\n"
      
      # Try to associate these errors with test cases
      while IFS= read -r line; do
        if [[ "$line" =~ tests/.*::test_.* ]]; then
          # Extract test name as before
          if [[ "$line" =~ tests/[^\ ]+::test_[^\ \[\]]+ ]]; then
            local test_name=$(echo "$line" | grep -o "tests/[^[:space:]]*::test_[^[:space:]^[^]]*")
            if [[ "$line" =~ tests/[^\ ]+::test_[^\ \[\]]+\[[^\]]+\] ]]; then
              local param=$(echo "$line" | grep -o "\[[^]]*\]")
              test_name="${test_name}${param}"
            fi
            current_asyncio_error="$test_name"
          fi
        elif [[ "$line" == *"Task .* was cancelled"* ]]; then
          if [[ -n "$current_asyncio_error" ]]; then
            if [[ -n "${asyncio_errors_by_test[$current_asyncio_error]}" ]]; then
              asyncio_errors_by_test[$current_asyncio_error]="${asyncio_errors_by_test[$current_asyncio_error]}\n$line"
            else
              asyncio_errors_by_test[$current_asyncio_error]="$line"
            fi
          fi
        fi
      done < <(grep -B 10 "Task .* was cancelled" "$log_file")
      
      # If we couldn't associate errors with specific tests
      if [[ ${#asyncio_errors_by_test[@]} -eq 0 ]]; then
        special_errors+="${RED}$(cat /tmp/task_cancel_errors.txt)${NC}\n\n"
      fi
    fi
  fi
  
  # Check for logging errors that often accompany asyncio issues
  if grep -A 10 -B 3 "Logging error" "$log_file" > /tmp/logging_errors.txt; then
    if [[ -s /tmp/logging_errors.txt ]]; then
      has_special_errors=true
      local logging_count=$(grep -c "Logging error" "$log_file")
      special_error_count=$((special_error_count + logging_count))
      special_errors+="${RED}${BOLD}Logging error detected (${logging_count} occurrences, possibly related to asyncio) (Severity: ${severity_scores["logging_error"]}/10)${NC}\n"
      special_errors+="${RED}$(cat /tmp/logging_errors.txt)${NC}\n\n"
    fi
  fi
  
  # Check for JSON structured logs
  if grep -E '^\s*\{.*"level":("|)?(debug|info|warning|error|critical)("|)?.*\}' "$log_file" > /tmp/json_logs.txt; then
    if [[ -s /tmp/json_logs.txt ]]; then
      # Extract errors and warnings from JSON logs
      grep -E '^\s*\{.*"level":("|)?(error|critical)("|)?.*\}' /tmp/json_logs.txt > /tmp/json_errors.txt
      grep -E '^\s*\{.*"level":("|)?warning("|)?.*\}' /tmp/json_logs.txt > /tmp/json_warnings.txt
      
      local json_error_count=$(wc -l < /tmp/json_errors.txt)
      local json_warning_count=$(wc -l < /tmp/json_warnings.txt)
      
      if [[ $json_error_count -gt 0 ]]; then
        echo -e "${YELLOW}Found ${json_error_count} structured JSON log errors:${NC}"
        while IFS= read -r line; do
          echo -e "  ${RED}$line${NC}"
          ((error_count++))
        done < /tmp/json_errors.txt
        echo -e ""
      fi
      
      if [[ $json_warning_count -gt 0 ]]; then
        echo -e "${YELLOW}Found ${json_warning_count} structured JSON log warnings:${NC}"
        while IFS= read -r line; do
          echo -e "  ${YELLOW}$line${NC}"
          ((warning_count++))
        done < /tmp/json_warnings.txt
        echo -e ""
      fi
    fi
  fi
  
  # Process the log file line by line for regular errors and warnings
  while IFS= read -r line; do
    # Remove ANSI color codes for pattern matching
    local clean_line=$(echo "$line" | sed -r "s/\x1B\[([0-9]{1,3}(;[0-9]{1,3})*)?[mGK]//g")
    
    # Skip processing if we've already caught this in our special error section
    if [[ "$clean_line" == *"Task was destroyed but it is pending"* || 
          "$clean_line" == *"Logging error"* ||
          "$clean_line" == *"Task .* was cancelled"* ]]; then
      continue
    fi
    
    # Skip the traceback for special errors we've already captured
    if [[ "$in_task_error" == "true" && 
          ("$clean_line" == *"Traceback"* || 
           "$clean_line" == *"File "* || 
           "$clean_line" == *"ValueError:"* || 
           "$clean_line" == *"Call stack:"* ||
           "$clean_line" == *"Message:"* ||
           "$clean_line" == *"Arguments:"* ||
           "$clean_line" == *"task:"* ||
           "$clean_line" == *"coro="* ||
           "$clean_line" =~ ^[[:space:]]+.*) ]]; then
      # Check for end of error block
      if [[ "$clean_line" == *"Arguments: ()"* ]]; then
        in_task_error=false
      fi
      continue
    fi
    
    # Check if this is a test case header line
    if [[ "$clean_line" =~ tests/.*::test_.* ]]; then
      # Extract test name - handles both normal and parameterized tests
      if [[ "$clean_line" =~ tests/[^\ ]+::test_[^\ \[\]]+ ]]; then
        current_test=$(echo "$clean_line" | grep -o "tests/[^[:space:]]*::test_[^[:space:]^[^]]*")
        # For parameterized tests, try to extract the parameter too
        if [[ "$clean_line" =~ tests/[^\ ]+::test_[^\ \[\]]+\[[^\]]+\] ]]; then
          local param=$(echo "$clean_line" | grep -o "\[[^]]*\]")
          current_test="${current_test}${param}"
        fi
        has_errors=false
      fi
    fi
    
    # Extract ERROR and WARNING lines
    if [[ "$clean_line" == *"ERROR "* || "$clean_line" == *"CRITICAL "* ]]; then
      if [[ "$has_errors" == "false" && "$current_test" != "" ]]; then
        echo -e "${BOLD}${current_test}:${NC}"
        has_errors=true
      fi
      
      # Determine severity based on content
      local severity=${severity_scores["standard_error"]}
      if [[ "$clean_line" == *"Connection"*"Error"* || "$clean_line" == *"ConnectionError"* ]]; then
        severity=${severity_scores["connection_error"]}
        echo -e "  ${RED}$clean_line ${YELLOW}(Severity: $severity/10)${NC}"
      elif [[ "$clean_line" == *"Timeout"*"Error"* || "$clean_line" == *"TimeoutError"* ]]; then
        severity=${severity_scores["timeout_error"]}
        echo -e "  ${RED}$clean_line ${YELLOW}(Severity: $severity/10)${NC}"
      elif [[ "$clean_line" == *"AssertionError"* ]]; then
        severity=${severity_scores["assertion_error"]}
        echo -e "  ${RED}$clean_line ${YELLOW}(Severity: $severity/10)${NC}"
      else
        echo -e "  ${RED}$clean_line ${YELLOW}(Severity: $severity/10)${NC}"
      fi
      ((error_count++))
    elif [[ "$clean_line" == *"WARNING "* ]]; then
      if [[ "$has_errors" == "false" && "$current_test" != "" ]]; then
        echo -e "${BOLD}${current_test}:${NC}"
        has_errors=true
      fi
      echo -e "  ${YELLOW}$clean_line ${YELLOW}(Severity: ${severity_scores["warning"]}/10)${NC}"
      ((warning_count++))
    fi
  done < "$log_file"
  
  # Output the asyncio errors by test 
  if [[ ${#asyncio_errors_by_test[@]} -gt 0 ]]; then
    echo -e "\n${BOLD}${RED}Asyncio errors by test case:${NC}"
    for test_name in "${!asyncio_errors_by_test[@]}"; do
      echo -e "${BOLD}$test_name:${NC}"
      echo -e "${RED}${asyncio_errors_by_test[$test_name]}${NC}"
      echo -e ""
    done
  fi
  
  # Add special errors section if any were found
  if [[ "$has_special_errors" == "true" && -z ${#asyncio_errors_by_test[@]} ]]; then
    echo -e "\n${BOLD}${RED}Special errors detected:${NC}"
    echo -e "$special_errors"
  fi
  
  # Add special errors to total count
  error_count=$((error_count + special_error_count))
  
  echo -e "\n${BOLD}Total: $error_count errors, $warning_count warnings${NC}"
  echo -e "${BOLD}${BLUE}======================================================${NC}"
  
  # Clean up temp files
  rm -f /tmp/asyncio_errors.txt /tmp/task_cancel_errors.txt /tmp/logging_errors.txt /tmp/json_logs.txt /tmp/json_errors.txt /tmp/json_warnings.txt
}

# Check if help is requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  show_help "full"
  exit 0
fi

# Initialize variables
INTERACTIVE=false
ERROR_SUMMARY=false
ANALYZE_LOG_FILE=""
SEQUENTIAL=false
PROFILE=false
PROFILE_SVG=false
CLEAR_AFTER_SELECTION=false
PYTEST_OPTIONS=()  # New array to collect pytest options

# Parse options
while [[ $# -gt 0 && "$1" == -* ]]; do
  case "$1" in
    -i|--interactive)
      INTERACTIVE=true
      shift
      ;;
    -s|--sequential)
      SEQUENTIAL=true
      shift
      ;;
    -e|--error-summary)
      ERROR_SUMMARY=true
      shift
      ;;
    -a|--analyze-log)
      if [[ -z "$2" || "$2" == -* ]]; then
        echo "Error: --analyze-log requires a file path argument"
        show_help "full"
        exit 1
      fi
      ANALYZE_LOG_FILE="$2"
      shift 2
      ;;
    -p|--profile)
      PROFILE=true
      shift
      ;;
    -g|--profile-svg)
      PROFILE_SVG=true
      shift
      ;;
    -c|--clear)
      CLEAR_AFTER_SELECTION=true
      shift
      ;;
    -h|--help)
      show_help "full"
      exit 0
      ;;
    *)
      # Instead of erroring, collect this as a pytest option
      PYTEST_OPTIONS+=("$1")
      shift
      ;;
  esac
done

# If analyzing an existing log file, just do that and exit
if [[ -n "$ANALYZE_LOG_FILE" ]]; then
  if [[ ! -f "$ANALYZE_LOG_FILE" ]]; then
    echo "Error: File not found: $ANALYZE_LOG_FILE"
    exit 1
  fi
  echo "Analyzing log file: $ANALYZE_LOG_FILE"
  enhanced_extract_errors "$ANALYZE_LOG_FILE"
  exit 0
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
  
  # Clear the screen if requested
  if $CLEAR_AFTER_SELECTION; then
    clear
  fi
  
  LOG_LEVEL=${1:-INFO}
  shift 1 2>/dev/null || shift $# 2>/dev/null || true
else
  TEST_PATH=${1:-tests/}  # Default to tests/ directory if not specified
  
  # Skip log level check if we have no more arguments
  if [[ $# -gt 1 ]]; then
    # Only set LOG_LEVEL from the second argument if it doesn't start with a dash
    if [[ "${2:0:1}" != "-" ]]; then
      LOG_LEVEL=${2}
      shift
    else
      LOG_LEVEL=INFO
    fi
  else
    LOG_LEVEL=INFO
  fi
  
  shift 1 2>/dev/null || shift $# 2>/dev/null || true
fi

# Configure worker count or sequential mode
ADDITIONAL_ARGS=()

# Add any collected pytest options
for opt in "${PYTEST_OPTIONS[@]}"; do
  ADDITIONAL_ARGS+=("$opt")
done

# Add remaining arguments
for arg in "$@"; do
  ADDITIONAL_ARGS+=("$arg")
done

# Format the arguments for display and command usage
ADDITIONAL_ARGS_STR="${ADDITIONAL_ARGS[*]}"

if $SEQUENTIAL; then
  # No -n8 for sequential mode
  WORKER_INFO="sequentially (no parallel workers)"
else
  # Add -n8 for parallel mode if -n wasn't already specified
  if [[ ! "$ADDITIONAL_ARGS_STR" =~ -n[0-9]+ ]]; then
    ADDITIONAL_ARGS+=("-n8")  # Default: 8 parallel workers
    WORKER_INFO="using 8 worker processes"
  else
    WORKER_INFO="using parallel workers"
  fi
fi

# Install pytest-xdist if not installed and we're using parallel mode
if ! $SEQUENTIAL && ! python -c "import pytest; import xdist" 2>/dev/null; then
    echo "Installing pytest-xdist for parallel testing..."
    pip install pytest-xdist
fi

# Install pytest-profiling if not installed and we're using profiling
if $PROFILE && ! python -c "import pytest_profiling" 2>/dev/null; then
    echo "Installing pytest-profiling for profiling..."
    pip install pytest-profiling
fi

# Install gprof2dot if needed for SVG visualization
if $PROFILE_SVG && ! python -c "import gprof2dot" 2>/dev/null; then
    echo "Installing gprof2dot for SVG visualization..."
    pip install gprof2dot
fi

# Update ADDITIONAL_ARGS_STR after possible changes
ADDITIONAL_ARGS_STR="${ADDITIONAL_ARGS[*]}"

# Add profiling options
if $PROFILE; then
  # Create prof directory if it doesn't exist
  mkdir -p "${PROJECT_ROOT}/prof"
  
  # Add profiling plugin options
  ADDITIONAL_ARGS+=("--profile")
  
  # Force sequential mode if profiling is enabled
  # Running profiling in parallel often gives unreliable results
  SEQUENTIAL=true
  
  if $PROFILE_SVG; then
    ADDITIONAL_ARGS+=("--profile-svg")
  fi
fi

# Display basic info
# Clear the screen if requested in non-interactive mode
if $CLEAR_AFTER_SELECTION && ! $INTERACTIVE; then
  clear
fi

echo "Running tests $WORKER_INFO"
echo "Test path: $TEST_PATH"
echo "Log level: $LOG_LEVEL (higher = more detailed output)"
if $ERROR_SUMMARY; then
  echo "Error summary: Enabled (will show all errors after tests complete)"
fi
if $SEQUENTIAL; then
  echo "Mode: Sequential (easier debugging)"
fi
if $PROFILE; then
  echo "Profiling: Enabled (generating .prof files in ${PROJECT_ROOT}/prof/)"
  if $PROFILE_SVG; then
    echo "SVG Visualization: Enabled (generating .svg files in ${PROJECT_ROOT}/prof/)"
  fi
fi
echo "Additional args: $ADDITIONAL_ARGS_STR"
echo "---------------------------------------------------"

# Construct the additional args string with proper spacing
ADDITIONAL_ARGS_CMD=""
for arg in "${ADDITIONAL_ARGS[@]}"; do
  ADDITIONAL_ARGS_CMD+=" $arg"
done

# Construct and run the pytest command
# Move all pytest.ini settings to command line flags:
# -vv: Increases verbosity
# -p no:timer: Disable the pytest-timer plugin since we use --durations
# -o testpaths=tests: Define test paths
# -o python_files=test_*.py: Define pattern for test files 
# --asyncio-mode=auto: Manages asyncio behavior
# -o asyncio_default_fixture_loop_scope=function: Sets fixture loop scope
# -n8: Runs tests in 8 parallel processes
# --durations=10: Show execution time for top 10 slowest tests
# -o markers: Define markers (we register but don't apply them)
# -o filterwarnings: Configure warning filters
# --showlocals and -rA flags for better error context
PYTEST_CMD="PYTHONPATH=${PROJECT_ROOT} pytest \"${TEST_PATH}\" \
  -p no:timer \
  -vv \
  -o testpaths=tests \
  -o python_files=test_*.py \
  --asyncio-mode=auto \
  -o asyncio_default_fixture_loop_scope=function \
  --durations=10 \
  -o 'markers=real: mark tests that run against real data/resources rather than mocks
integration: mark tests that integrate with external services' \
  -o 'filterwarnings=ignore::ResourceWarning' \
  --showlocals \
  -rA${ADDITIONAL_ARGS_CMD}"
echo "Running: $PYTEST_CMD"
echo "---------------------------------------------------"

# Run the command and capture the output
if $ERROR_SUMMARY; then
  # Create a temporary file for logging if not already set
  if [ -z "$ERROR_LOG_FILE" ]; then
    ERROR_LOG_FILE=$(mktemp)
    trap 'rm -f "$ERROR_LOG_FILE"' EXIT
  fi
  
  echo "Running with error summary enabled (log file: $ERROR_LOG_FILE)"
  
  # Set COLUMNS to a very wide value to prevent truncation in the summary table
  export COLUMNS=200
  
  # Force color output with --color=yes and capture both stdout and stderr
  # Set PYTHONUNBUFFERED=1 to prevent output buffering
  # Set FORCE_COLOR=1 to ensure terminal colors are preserved through the pipe
  # Pass COLUMNS to ensure wide tables in the output
  TEMP_CMD="PYTHONUNBUFFERED=1 FORCE_COLOR=1 PYTHONASYNCIOEBUG=1 COLUMNS=${COLUMNS} $PYTEST_CMD --color=yes"
  echo "Executing: $TEMP_CMD"
  
  # Use eval with direct output redirection to tee
  eval "$TEMP_CMD" 2>&1 | tee "$ERROR_LOG_FILE"
  # Capture exit code from first command in the pipe (bash-specific)
  PYTEST_EXIT_CODE=${PIPESTATUS[0]}
  
  echo "Pytest finished with exit code: $PYTEST_EXIT_CODE"
  
  # Add custom detailed failure summary if asyncio tests are involved
  if [[ "$TEST_PATH" == *"asyncio"* ]]; then
    echo -e "\n${BOLD}${GREEN}==================================================================${NC}"
    echo -e "${BOLD}${BLUE}                 DETAILED FAILURE SUMMARY                       ${NC}"
    echo -e "${BOLD}${GREEN}==================================================================${NC}"
    
    echo "Generating a comprehensive error summary with full file paths..."
    echo ""
    
    # Call the Python script directly with the test path
    python "${SCRIPT_DIR}/detailed_failure_summary.py" "$TEST_PATH"
  fi
  
  # Process and display error summary
  enhanced_extract_errors "$ERROR_LOG_FILE"
  
  # Run a custom command to get a full failure table without truncation if needed
  if [[ "$TEST_PATH" == *test_asyncio_errors.py ]]; then
    echo -e "\n${BOLD}${BLUE}======================================================${NC}"
    echo -e "${BOLD}${GREEN}        COMPREHENSIVE FAILURE SUMMARY (FULL PATHS)     ${NC}"
    echo -e "${BOLD}${BLUE}======================================================${NC}"
    
    # Use our dedicated Python script for improved failure summary
    python "${SCRIPT_DIR}/detailed_failure_summary.py" "$TEST_PATH"
  fi
  
else
  # Run pytest without capturing output but still set COLUMNS to avoid truncation
  export COLUMNS=200
  eval "COLUMNS=${COLUMNS} $PYTEST_CMD"
  PYTEST_EXIT_CODE=$?
fi

# Check if exit code is defined and not empty
if [[ -n "$PYTEST_EXIT_CODE" ]]; then
  # Display profiling summary if profiling was enabled
  if $PROFILE; then
    echo -e "\n${BOLD}${BLUE}======================================================${NC}"
    echo -e "${BOLD}${GREEN}                 PROFILING SUMMARY                    ${NC}"
    echo -e "${BOLD}${BLUE}======================================================${NC}"
    
    PROF_DIR="${PROJECT_ROOT}/prof"
    PROF_COUNT=$(ls -1 "${PROF_DIR}"/*.prof 2>/dev/null | wc -l)
    
    if [ "$PROF_COUNT" -gt 0 ]; then
      echo -e "${GREEN}${PROF_COUNT} profile files generated in ${PROF_DIR}${NC}"
      
      # List the profile files
      echo -e "\n${YELLOW}Profile files:${NC}"
      ls -lh "${PROF_DIR}"/*.prof | awk '{print $9, "("$5")"}'
      
      if $PROFILE_SVG; then
        SVG_COUNT=$(ls -1 "${PROF_DIR}"/*.svg 2>/dev/null | wc -l)
        if [ "$SVG_COUNT" -gt 0 ]; then
          echo -e "\n${YELLOW}SVG visualizations:${NC}"
          ls -lh "${PROF_DIR}"/*.svg | awk '{print $9, "("$5")"}'
        fi
      fi
      
      echo -e "\n${YELLOW}To analyze a specific profile, use:${NC}"
      echo -e "  python -m pstats ${PROF_DIR}/<profile_file>"
      echo -e "  or"
      echo -e "  gprof2dot -f pstats ${PROF_DIR}/<profile_file> | dot -Tsvg -o ${PROF_DIR}/<output_file>.svg"
    else
      echo -e "${RED}No profile files generated. Make sure tests are actually running.${NC}"
    fi
    
    echo -e "${BOLD}${BLUE}======================================================${NC}"
  fi

  if [ $PYTEST_EXIT_CODE -eq 0 ]; then
    echo "Tests completed successfully!"
  else
    echo "Tests failed with exit code $PYTEST_EXIT_CODE"
  fi
  exit $PYTEST_EXIT_CODE
else
  echo "Warning: No exit code was captured"
  exit 1
fi 