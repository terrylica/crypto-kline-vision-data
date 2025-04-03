#!/bin/bash

# Enhanced error extraction script
# This script provides improved categorization and display of error messages from pytest output

# Define colors for better formatting
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Enhanced error extraction function with categorization
enhanced_extract_errors() {
    local logfile="$1"
    
    if [ ! -f "$logfile" ]; then
        echo -e "${RED}Error: Log file $logfile does not exist${NC}"
        return 1
    fi
  
  # Initialize counters
    local asyncio_errors=0
    local assertion_errors=0
    local test_failures=0
    local other_exceptions=0
    local total_errors=0
    
    # Initialize arrays for error details
    declare -a asyncio_error_details=()
    declare -a assertion_error_details=()
    declare -a test_failure_details=()
    declare -a other_exception_details=()
    
    echo -e "\n${BOLD}${BLUE}=== Error Summary ===${NC}\n"
    
    # Extract asyncio-specific errors
    if grep -q "asyncio.exceptions" "$logfile" || grep -q "Task was destroyed but it is pending" "$logfile"; then
        asyncio_errors=$(grep -c "Task was destroyed but it is pending" "$logfile")
        asyncio_errors=$((asyncio_errors + $(grep -c "asyncio.exceptions" "$logfile")))
        
        # Get test details for asyncio errors
        while read -r line; do
            # Extract test name from line
            test_name=$(echo "$line" | sed -n 's/.*\(test_[a-zA-Z0-9_]*\).*/\1/p')
            if [ -n "$test_name" ]; then
                asyncio_error_details+=("$test_name")
            fi
        done < <(grep -B 5 "Task was destroyed but it is pending" "$logfile" | grep "test_")
    fi
    
    # Extract assertion errors
    if grep -q "AssertionError" "$logfile"; then
        assertion_errors=$(grep -c "AssertionError" "$logfile")
        
        # Get test details for assertion errors
        while read -r line; do
            # Extract test name from line
            test_name=$(echo "$line" | sed -n 's/.*\(test_[a-zA-Z0-9_]*\).*/\1/p')
            if [ -n "$test_name" ]; then
                assertion_error_details+=("$test_name")
            fi
        done < <(grep -B 5 "AssertionError" "$logfile" | grep "test_")
    fi
    
    # Extract test failures (FAILED)
    if grep -q "FAILED " "$logfile"; then
        test_failures=$(grep -c "FAILED " "$logfile")
        
        # Get test details for test failures
        while read -r line; do
            test_failure_details+=("$line")
        done < <(grep "FAILED " "$logfile" | sed 's/FAILED//g' | sed 's/\[.*\]//g' | tr -s ' ')
    fi
    
    # Extract other exceptions
    if grep -q "Exception:" "$logfile" || grep -q "Error:" "$logfile"; then
        other_exceptions=$(grep -c -E "Exception:|Error:" "$logfile")
        
        # Get test details for other exceptions
        while read -r line; do
            # Extract test name from line
            test_name=$(echo "$line" | sed -n 's/.*\(test_[a-zA-Z0-9_]*\).*/\1/p')
            if [ -n "$test_name" ]; then
                other_exception_details+=("$test_name")
            fi
        done < <(grep -B 5 -E "Exception:|Error:" "$logfile" | grep "test_")
    fi
    
    # Calculate total errors
    total_errors=$((asyncio_errors + assertion_errors + other_exceptions))
    
    # Output error summary with categories
    echo -e "${BOLD}${YELLOW}Error Summary:${NC}\n"
    
    if [ $asyncio_errors -gt 0 ]; then
        echo -e "${RED}Asyncio-Specific Errors: $asyncio_errors${NC}"
        for detail in "${asyncio_error_details[@]}"; do
            echo -e "  - ${YELLOW}$detail${NC}"
        done
        echo ""
    fi
    
    if [ $assertion_errors -gt 0 ]; then
        echo -e "${RED}Assertion Errors: $assertion_errors${NC}"
        for detail in "${assertion_error_details[@]}"; do
            echo -e "  - ${YELLOW}$detail${NC}"
        done
        echo ""
    fi
    
    if [ ${#test_failure_details[@]} -gt 0 ]; then
        echo -e "${RED}Test Failures:${NC}"
        for detail in "${test_failure_details[@]}"; do
            echo -e "  - ${YELLOW}$detail${NC}"
        done
        echo ""
    fi
    
    if [ $other_exceptions -gt 0 ]; then
        echo -e "${RED}Other Exceptions: $other_exceptions${NC}"
        for detail in "${other_exception_details[@]}"; do
            echo -e "  - ${YELLOW}$detail${NC}"
        done
        echo ""
    fi
    
    # Output total errors
    echo -e "${BOLD}${RED}Total Errors: $total_errors${NC}\n"
    
    # Find specific error types and include them in the summary
    if grep -q "Task was destroyed but it is pending" "$logfile"; then
        echo -e "${MAGENTA}Detected: Asyncio task was created but not awaited${NC}"
        echo -e "${CYAN}Fix: Use 'await asyncio.create_task(...)' or store the task and await it later${NC}"
        echo ""
    fi
    
    if grep -q "coroutine .* was never awaited" "$logfile"; then
        echo -e "${MAGENTA}Detected: Coroutine not awaited${NC}"
        echo -e "${CYAN}Fix: Add 'await' before coroutine calls${NC}"
        echo ""
    fi
    
    # Display all failures in condensed form with line numbers
    if [ $total_errors -gt 0 ]; then
        echo -e "${BOLD}${BLUE}Condensed Error Highlights:${NC}\n"
        grep -n "FAILED " "$logfile" | head -10
        echo ""
    fi
    
    # Display extra information about the test run
    total_tests=$(grep -c "collected " "$logfile" | cut -d " " -f 2)
    passed_tests=$(grep " passed" "$logfile" | grep -v "warnings" | head -1 | awk '{print $1}')
    
    echo -e "${BOLD}${GREEN}Test Summary:${NC}"
    echo -e "Total Tests: $total_tests"
    echo -e "Passed: $passed_tests"
    echo -e "Failed: $test_failures"
    
    # Return total errors for exit status
    return $total_errors
}

# Export the function so it can be used in other scripts
export -f enhanced_extract_errors
