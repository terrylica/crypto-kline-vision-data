#!/bin/bash

# Enhanced error extraction script for asyncio error reporting
# This script provides improved categorization and display of error messages from pytest output
# with particular focus on asyncio-related errors

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
    
    echo -e "\n${BOLD}${BLUE}======================================================${NC}"
    echo -e "${BOLD}${RED}                  ERROR SUMMARY                       ${NC}"
    echo -e "${BOLD}${BLUE}======================================================${NC}"
    echo -e "${YELLOW}All errors and warnings, even from passing tests:${NC}\n"
    
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
    
    # 1. SECTION: ASYNCIO-SPECIFIC ERRORS
    echo -e "${BOLD}${RED}ASYNCIO ERRORS:${NC}\n"
    
    if [ $asyncio_errors -gt 0 ]; then
        echo -e "${RED}Found ${asyncio_errors} asyncio-specific errors${NC}"
        for detail in "${asyncio_error_details[@]}"; do
            echo -e "  - ${YELLOW}$detail${NC}"
        done
        
        # Show details of asyncio errors
        if grep -q "Task was destroyed but it is pending" "$logfile"; then
            echo -e "\n${MAGENTA}Task Destruction Errors:${NC}"
            grep -A 2 -B 2 "Task was destroyed but it is pending" "$logfile" | grep -v "^--$" | sed 's/^/    /'
            echo -e "${CYAN}Fix: Use 'await asyncio.create_task(...)' or store the task and await it later${NC}"
        fi
        
        if grep -q "Task exception was never retrieved" "$logfile"; then
            echo -e "\n${MAGENTA}Task Exception Never Retrieved:${NC}"
            grep -A 5 -B 2 "Task exception was never retrieved" "$logfile" | grep -v "^--$" | head -10 | sed 's/^/    /'
            echo -e "${CYAN}Fix: Handle exceptions in tasks with try/except or ensure tasks are properly awaited${NC}"
        fi
        
        echo ""
    else
        echo -e "${GREEN}No asyncio-specific errors detected${NC}\n"
    fi
    
    # 2. SECTION: TEST FAILURES
    echo -e "${BOLD}${RED}TEST FAILURES:${NC}\n"
    
    if [ ${#test_failure_details[@]} -gt 0 ]; then
        echo -e "${RED}Found ${test_failures} test failures${NC}"
        for detail in "${test_failure_details[@]}"; do
            echo -e "  - ${YELLOW}$detail${NC}"
        done
        echo ""
    else
        echo -e "${GREEN}No test failures detected${NC}\n"
    fi
    
    # 3. SECTION: ASSERTION ERRORS
    echo -e "${BOLD}${RED}ASSERTION ERRORS:${NC}\n"
    
    if [ $assertion_errors -gt 0 ]; then
        echo -e "${RED}Found ${assertion_errors} assertion errors${NC}"
        
        # Group by test if possible
        if grep -A 5 -B 5 "AssertionError" "$logfile" > /tmp/assertion_errors.txt; then
            if [[ -s /tmp/assertion_errors.txt ]]; then
                # Clean up and display relevant parts
                cat /tmp/assertion_errors.txt | grep -v "^--$" | head -20 | sed 's/^/    /'
                rm -f /tmp/assertion_errors.txt
            fi
        fi
        echo ""
    else
        echo -e "${GREEN}No assertion errors detected${NC}\n"
    fi
    
    # 4. SECTION: OTHER EXCEPTIONS
    echo -e "${BOLD}${RED}OTHER EXCEPTIONS:${NC}\n"
    
    if [ $other_exceptions -gt 0 ]; then
        echo -e "${RED}Found ${other_exceptions} other exceptions${NC}"
        for exception in "ValueError" "TypeError" "KeyError" "RuntimeError" "ImportError"; do
            if grep -A 3 -B 3 "$exception:" "$logfile" > "/tmp/${exception}_errors.txt"; then
                if [[ -s "/tmp/${exception}_errors.txt" ]]; then
                    local exception_count=$(grep -c "$exception:" "$logfile")
                    echo -e "  ${YELLOW}${exception}: ${exception_count}${NC}"
                    cat "/tmp/${exception}_errors.txt" | grep -v "^--$" | head -10 | sed 's/^/    /'
                    rm -f "/tmp/${exception}_errors.txt"
                fi
            fi
        done
        echo ""
    else
        echo -e "${GREEN}No other exceptions detected${NC}\n"
    fi
    
    # 5. NEW SECTION: IMPROVED DETAILED FAILURE SUMMARY
    # This provides a more detailed table without truncation issues
    echo -e "${BOLD}${BLUE}DETAILED FAILURE SUMMARY:${NC}\n"
    
    # Create a file to store our detailed failure data
    local detailed_failures_file="/tmp/detailed_failures.txt"
    > "$detailed_failures_file"
    
    # Extract detailed failure information - looking for file paths followed by error lines
    if grep -B 5 -A 5 "FAILED" "$logfile" > /tmp/failed_sections.txt; then
        while read -r line; do
            # Look for test file paths
            if [[ "$line" =~ [a-zA-Z0-9_/]+\.py::test_[a-zA-Z0-9_]+ ]]; then
                # Extract the file path and function name
                local file_path=$(echo "$line" | sed -n 's/.*\([a-zA-Z0-9_/]\+\.py\).*/\1/p')
                local function_name=$(echo "$line" | sed -n 's/.*::\(test_[a-zA-Z0-9_]\+\).*/\1/p')
                
                # Look ahead for function line and error line information
                local function_line=""
                local error_line=""
                local error_type=""
                
                # Search nearby lines for more details
                grep -A 10 "$file_path::$function_name" "$logfile" | while read -r detail_line; do
                    # Look for function line
                    if [[ "$detail_line" =~ [0-9]+:\ +$function_name ]]; then
                        function_line=$(echo "$detail_line" | sed -n 's/.*\([0-9]\+\):.*'$function_name'.*/\1/p')
                    fi
                    
                    # Look for error line
                    if [[ "$detail_line" =~ [a-zA-Z0-9_/]+\.py:[0-9]+ ]]; then
                        error_line=$(echo "$detail_line" | sed -n 's/.*\.py:\([0-9]\+\).*/\1/p')
                    fi
                    
                    # Look for error type
                    if [[ "$detail_line" =~ E\ +[A-Za-z]+Error: ]]; then
                        error_type=$(echo "$detail_line" | sed -n 's/.*E\ \+\([A-Za-z]\+Error\):.*/\1/p')
                    fi
                    
                    # If we have all the data, add it to our file
                    if [[ -n "$file_path" && -n "$function_name" && -n "$function_line" && -n "$error_line" && -n "$error_type" ]]; then
                        echo "$file_path|$function_name|$function_line|$error_line|$error_type" >> "$detailed_failures_file"
                        break
                    fi
                done
            fi
        done < /tmp/failed_sections.txt
    fi
    
    # Process and display the detailed failure summary in a structured format
    if [[ -s "$detailed_failures_file" ]]; then
        # Sort and remove duplicates
        sort -u "$detailed_failures_file" > "/tmp/sorted_failures.txt"
        
        # Calculate column widths based on content
        local max_file_width=20
        local max_func_width=20
        local max_error_width=20
        
        # Find the maximum width for each column
        while IFS='|' read -r file func func_line error_line error_type; do
            [[ ${#file} -gt $max_file_width ]] && max_file_width=${#file}
            [[ ${#func} -gt $max_func_width ]] && max_func_width=${#func}
            [[ ${#error_type} -gt $max_error_width ]] && max_error_width=${#error_type}
        done < "/tmp/sorted_failures.txt"
        
        # Add some padding
        max_file_width=$((max_file_width + 2))
        max_func_width=$((max_func_width + 2))
        max_error_width=$((max_error_width + 2))
        
        # Create header line
        local header_line="+"
        local header_sep="+"
        local header_titles="|"
        
        # File column
        for ((i=0; i<max_file_width; i++)); do header_line+="="; done
        header_line+="+"
        for ((i=0; i<max_file_width; i++)); do header_sep+="-"; done
        header_sep+="+"
        header_titles+=" File"
        for ((i=0; i<max_file_width-6; i++)); do header_titles+=" "; done
        header_titles+="|"
        
        # Function column
        for ((i=0; i<max_func_width; i++)); do header_line+="="; done
        header_line+="+"
        for ((i=0; i<max_func_width; i++)); do header_sep+="-"; done
        header_sep+="+"
        header_titles+=" Function"
        for ((i=0; i<max_func_width-10; i++)); do header_titles+=" "; done
        header_titles+="|"
        
        # Function Line column
        header_line+="============+"
        header_sep+="------------+"
        header_titles+=" Func Line |"
        
        # Error Line column
        header_line+="===========+"
        header_sep+="-----------+"
        header_titles+=" Error Line|"
        
        # Error Type column
        for ((i=0; i<max_error_width; i++)); do header_line+="="; done
        header_line+="+"
        for ((i=0; i<max_error_width; i++)); do header_sep+="-"; done
        header_sep+="+"
        header_titles+=" Error Type"
        for ((i=0; i<max_error_width-12; i++)); do header_titles+=" "; done
        header_titles+="|"
        
        # Print the detailed table
        echo -e "$header_line"
        echo -e "$header_titles"
        echo -e "$header_sep"
        
        # Print each failure record
        while IFS='|' read -r file func func_line error_line error_type; do
            local line="|"
            
            # File column
            line+=" $file"
            for ((i=0; i<max_file_width-${#file}-1; i++)); do line+=" "; done
            line+="|"
            
            # Function column
            line+=" $func"
            for ((i=0; i<max_func_width-${#func}-1; i++)); do line+=" "; done
            line+="|"
            
            # Function Line column
            line+=" $func_line"
            for ((i=0; i<10-${#func_line}; i++)); do line+=" "; done
            line+="|"
            
            # Error Line column
            line+=" $error_line"
            for ((i=0; i<9-${#error_line}; i++)); do line+=" "; done
            line+="|"
            
            # Error Type column
            line+=" $error_type"
            for ((i=0; i<max_error_width-${#error_type}-1; i++)); do line+=" "; done
            line+="|"
            
            echo -e "$line"
        done < "/tmp/sorted_failures.txt"
        
        echo -e "$header_sep"
    else
        echo -e "${GREEN}No detailed failure information available${NC}\n"
    fi
    
    # Display total errors and error summary
    echo -e "${BOLD}${BLUE}ERROR SUMMARY:${NC}\n"
    echo -e "Total Errors: ${total_errors}"
    echo -e "Asyncio Errors: ${asyncio_errors}"
    echo -e "Assertion Errors: ${assertion_errors}"
    echo -e "Other Exceptions: ${other_exceptions}"
    echo -e "Test Failures: ${test_failures}"
    
    # Find specific error types and include them in the summary
    echo -e "\n${BOLD}${BLUE}RECOMMENDATIONS:${NC}\n"
    
    if grep -q "Task was destroyed but it is pending" "$logfile"; then
        echo -e "${MAGENTA}✗ Asyncio task was created but not awaited${NC}"
        echo -e "${CYAN}✓ Fix: Use 'await asyncio.create_task(...)' or store the task and await it later${NC}"
    fi
    
    if grep -q "coroutine .* was never awaited" "$logfile"; then
        echo -e "${MAGENTA}✗ Coroutine not awaited${NC}"
        echo -e "${CYAN}✓ Fix: Add 'await' before coroutine calls${NC}"
    fi
    
    if grep -q "Task exception was never retrieved" "$logfile"; then
        echo -e "${MAGENTA}✗ Task exception not handled${NC}"
        echo -e "${CYAN}✓ Fix: Use try/except blocks when awaiting tasks${NC}"
    fi
    
    echo -e "\n${BOLD}${BLUE}======================================================${NC}"
    
    # Clean up temp files
    rm -f /tmp/sorted_failures.txt /tmp/detailed_failures.txt /tmp/failed_sections.txt
    
    # Return total errors for exit status
    return $total_errors
}

# Export the function so it can be used in other scripts
export -f enhanced_extract_errors 